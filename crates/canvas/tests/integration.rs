//! Integration tests: the projection over the real query path —
//! `fetch_export` against a system with an installed bridge, the summary
//! derived from the fetched export, and the no-bridge timeout.
//!
//! Every zenoh test runs on its own [`state_report::StateKey::scoped`]
//! island key: default peer discovery puts all sessions in one mesh, so
//! tests that shared the production key answered each other's queries
//! under a parallel test runner. Island keys make the locks unnecessary.

use canvas::{SnapshotSummary, StateError, fetch_export_on};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::prelude::*;
use trouper::schema::{FieldDef, FieldTy, Schema, SchemaDef, SchemaKind};
use trouper::state_report::{ReportState, StateReported, StateReporter};

#[derive(Deserialize)]
struct Work {
    #[allow(dead_code)] // payload shape; the demo never reads it back
    n: i64,
}

impl Schema for Work {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Work".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

#[derive(Deserialize)]
struct WorkDone {
    #[allow(dead_code)] // payload shape; nobody reads it back
    n: i64,
}

impl Schema for WorkDone {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "WorkDone".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
struct Worker {
    total: i64,
}

impl EventSourcedActor for Worker {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<Work>()
            .emits::<WorkDone>()
            .kind(ActorKind::EventSourced)
    }
    fn restore(_args: &serde_json::Value) -> Self {
        Self::default()
    }
    fn apply(&mut self, event: &Event) {
        self.total += event.payload["n"].as_i64().unwrap_or(0);
    }
}

impl CommandHandler<Work> for Worker {
    fn handle(&self, cmd: Work, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        vec![Event::new(WorkDone::schema_id(), json!({ "n": cmd.n }))]
    }
}

/// Builds a live system: one journaled worker that has done `total` units
/// of work, plus the state reporter with the bridge installed on `key`.
async fn serving_system(
    key: state_report::StateKey,
    total: i64,
) -> (ActorSystem, state_report::zenoh::Session) {
    let system = ActorSystem::new(SystemConfig::production());
    system.register_schema::<Work>();
    system.register_schema::<WorkDone>();
    system.register_schema::<ReportState>();
    system.register_schema::<StateReported>();

    trouper::builder::spawn_es_builder::<Worker>(&system)
        .at(ActorPath::new("worker"))
        .args(json!({}))
        .handles::<Work>()
        .emits::<WorkDone>()
        .start();
    trouper::builder::spawn_es_builder::<StateReporter>(&system)
        .at(ActorPath::new("state/reporter"))
        .args(json!({}))
        .handles::<ReportState>()
        .emits::<StateReported>()
        .start();

    for n in 1..=total {
        system
            .send(system.envelope(
                Work::schema_id(),
                ActorPath::new("worker"),
                json!({ "n": n }),
            ))
            .await
            .expect("work delivered");
    }
    wait_for(|| async {
        system
            .inbox_cursor(&ActorPath::new("worker"))
            .map(|c| c.as_u64())
            == Some(total as u64)
    })
    .await;

    let session = state_report::install_on(key, system.clone(), ActorPath::new("state/reporter"))
        .await
        .expect("bridge installed");
    (system, session)
}

/// Polls `cond` until true (5s budget).
async fn wait_for<F, Fut>(cond: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..2_500 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("condition never became true within 5s");
}

#[tokio::test(flavor = "multi_thread")]
async fn fetched_export_projects_to_a_summary() {
    // Given a serving system: one ES worker with 4 units done, the
    // reporter, and both ReportState schemas — plus the runtime's own
    // Fact@1, auto-registered on first spawn (5 schemas, 2 actors, 2 ES).
    let key = state_report::StateKey::scoped("canvas-summary");
    let (_system, session) = serving_system(key.clone(), 4).await;

    // When fetching through the projection seam and summarizing.
    let export = fetch_export_on(key).await.expect("fetch");
    let summary = SnapshotSummary::of(&export);

    // Then the summary counts the live topology.
    assert_eq!(summary.schemas, 5);
    assert_eq!(summary.actors, 2);
    assert_eq!(summary.es, 2);
    assert_eq!(summary.service, 0);
    assert_eq!(summary.partitions, 0);
    assert_eq!(summary.rules, 0);

    // Teardown: leave the mesh gracefully.
    session.close().await.expect("bridge session closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_without_any_bridge_times_out() {
    // Given a fresh island no bridge serves.
    let key = state_report::StateKey::scoped("canvas-timeout");

    // When fetching.
    let result = fetch_export_on(key).await;

    // Then the projection reports the timeout as "nothing answered".
    match result {
        Err(StateError::Timeout) => {}
        other => panic!("expected StateError::Timeout, got {other:?}"),
    }
}
