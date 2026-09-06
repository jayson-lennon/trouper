//! Integration tests: the bridge serves real queries over real zenoh
//! sessions (`Config::default()`), `fetch` decodes a fresh export, and
//! `install` refuses to start without a reporter.

use actor_runtime::actor::{CommandHandler, EventSourcedActor};
use actor_runtime::prelude::*;
use actor_runtime::schema::{FieldDef, FieldTy, Schema, SchemaDef, SchemaKind};
use actor_runtime::state_report::{ReportState, StateReported, StateReporter};
use actor_runtime::system::SystemExport;
use serde::{Deserialize, Serialize};
use serde_json::json;
use state_report::{STATE_KEY, StateBridgeError, fetch, install};
use std::sync::Arc;
use std::time::Duration;

/// Zenoh discovery is machine-wide, so concurrent tests in this binary
/// could answer each other's queries — serialize everything that touches
/// the shared state key.
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
/// of work, plus the state reporter.
async fn reporting_system(total: i64) -> Arc<ActorSystem> {
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

/// The worker's exported `total`, from a raw export document.
fn worker_total(export: &SystemExport) -> i64 {
    export
        .actors
        .iter()
        .find(|a| a.path.as_str() == "worker")
        .and_then(|a| a.state.as_ref())
        .and_then(|s| s["total"].as_i64())
        .expect("worker exported with state")
}

#[tokio::test(flavor = "multi_thread")]
async fn installed_bridge_answers_a_second_sessions_query() {
    let _zenoh = ZENOH_LOCK.lock().await;
    // Given a reporting system whose bridge is installed.
    let system = reporting_system(3).await;
    let _session = install(system.clone(), ActorPath::new("state/reporter"))
        .await
        .expect("bridge installed");

    // When a SECOND zenoh session queries the state key (retrying through
    // peer-discovery warm-up).
    let client = zenoh::open(zenoh::Config::default())
        .await
        .expect("session");
    let mut payload = None;
    for _ in 0..6 {
        let replies = client.get(STATE_KEY).timeout(Duration::from_secs(1)).await;
        let Ok(replies) = replies else { continue };
        while let Ok(reply) = replies.recv_async().await {
            let Ok(sample) = reply.result() else { continue };
            let Ok(text) = sample.payload().try_to_string() else {
                continue;
            };
            payload = Some(text.to_string());
            break;
        }
        if payload.is_some() {
            break;
        }
    }
    let payload = payload.expect("a second session received a reply");

    // Then the payload decodes into the system's export with fresh content.
    let export: SystemExport = serde_json::from_str(&payload).expect("decodable export");
    assert_eq!(worker_total(&export), 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_returns_a_decodable_export() {
    let _zenoh = ZENOH_LOCK.lock().await;
    // Given a reporting system whose bridge is installed.
    let system = reporting_system(5).await;
    let _session = install(system.clone(), ActorPath::new("state/reporter"))
        .await
        .expect("bridge installed");

    // When fetching (the retry budget absorbs discovery warm-up).
    let export = fetch().await.expect("fetch");

    // Then the export reflects the live system.
    assert_eq!(worker_total(&export), 15);
    assert!(
        export
            .actors
            .iter()
            .any(|a| a.path.as_str() == "state/reporter")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn successive_fetches_observe_advancing_state() {
    let _zenoh = ZENOH_LOCK.lock().await;
    // Given a reporting system whose bridge is installed, fetched once.
    let system = reporting_system(1).await;
    let _session = install(system.clone(), ActorPath::new("state/reporter"))
        .await
        .expect("bridge installed");
    let first = fetch().await.expect("first fetch");
    assert_eq!(worker_total(&first), 1);

    // When more work happens and the state is fetched again.
    system
        .send(system.envelope(
            Work::schema_id(),
            ActorPath::new("worker"),
            json!({ "n": 9 }),
        ))
        .await
        .expect("work delivered");
    wait_for(|| async {
        system
            .inbox_cursor(&ActorPath::new("worker"))
            .map(|c| c.as_u64())
            == Some(2)
    })
    .await;
    let second = fetch().await.expect("second fetch");

    // Then the second reply carries the NEW state, not the first fetch's.
    assert_eq!(worker_total(&second), 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn install_rejects_unknown_reporter_path() {
    let _zenoh = ZENOH_LOCK.lock().await;
    // Given a live system with NO actor at the reporter path.
    let system = reporting_system(0).await;

    // When installing the bridge at a path nothing occupies.
    let result = install(system, ActorPath::new("nowhere")).await;

    // Then the install fails with NoReporter naming the path.
    match result {
        Err(StateBridgeError::NoReporter(path)) => {
            assert_eq!(path.as_str(), "nowhere");
        }
        other => panic!("expected NoReporter, got {other:?}"),
    }
}
