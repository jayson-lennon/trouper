//! Integration tests: the projection over the real query path —
//! `fetch_export` against a system with an installed bridge, the summary
//! derived from the fetched export, and the no-bridge timeout.

use actor_runtime::actor::{CommandHandler, EventSourcedActor};
use actor_runtime::prelude::*;
use actor_runtime::schema::{FieldDef, FieldTy, Schema, SchemaDef, SchemaKind};
use actor_runtime::state_report::{ReportState, StateReported, StateReporter};
use canvas::{SnapshotSummary, StateError, fetch_export};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

/// Zenoh discovery is machine-wide, so concurrent tests (including ones
/// from the state-report suite) could answer each other's queries —
/// serialize everything that touches the shared state key.
static ZENOH_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
/// of work, plus the state reporter with the bridge installed.
async fn serving_system(total: i64) -> Arc<ActorSystem> {
    let system = Arc::new(ActorSystem::new(SystemConfig::production()));
    system.register_schema::<Work>();
    system.register_schema::<WorkDone>();
    system.register_schema::<ReportState>();
    system.register_schema::<StateReported>();

    actor_runtime::builder::spawn_es_builder::<Worker>(&system)
        .at(ActorPath::new("worker"))
        .args(json!({}))
        .handles::<Work>()
        .emits::<WorkDone>()
        .start();
    actor_runtime::builder::spawn_es_builder::<StateReporter>(&system)
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

    state_report::install(system.clone(), ActorPath::new("state/reporter"))
        .await
        .expect("bridge installed");
    system
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
    let _zenoh = ZENOH_LOCK.lock().await;
    // Given a serving system: one ES worker with 4 units done, the
    // reporter, and both ReportState schemas — plus the runtime's own
    // Fact@1, auto-registered on first spawn (5 schemas, 2 actors, 2 ES).
    let _system = serving_system(4).await;

    // When fetching through the projection seam and summarizing.
    let export = fetch_export().await.expect("fetch");
    let summary = SnapshotSummary::of(&export);

    // Then the summary counts the live topology.
    assert_eq!(summary.schemas, 5);
    assert_eq!(summary.actors, 2);
    assert_eq!(summary.es, 2);
    assert_eq!(summary.service, 0);
    assert_eq!(summary.partitions, 0);
    assert_eq!(summary.rules, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_without_any_bridge_times_out() {
    let _zenoh = ZENOH_LOCK.lock().await;
    // Given NO system on this network has a bridge installed.

    // When fetching.
    let result = fetch_export().await;

    // Then the projection reports the timeout as "nothing answered".
    match result {
        Err(StateError::Timeout) => {}
        other => panic!("expected StateError::Timeout, got {other:?}"),
    }
}
