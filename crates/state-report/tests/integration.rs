//! Integration tests: the bridge serves real queries over real zenoh
//! sessions (`Config::default()`), `fetch` decodes a fresh export, and
//! `install` refuses to start without a reporter.
//!
//! Every zenoh test runs on its own [`StateKey::scoped`] island key:
//! default peer discovery puts all sessions in one mesh, so tests that
//! shared the production key answered each other's queries under a
//! parallel test runner. Island keys make the locks unnecessary.

use actor_runtime::actor::{CommandHandler, EventSourcedActor};
use actor_runtime::prelude::*;
use actor_runtime::schema::{FieldDef, FieldTy, Schema, SchemaDef, SchemaKind};
use actor_runtime::state_report::{ReportState, StateReported, StateReporter};
use actor_runtime::system::SystemExport;
use serde::{Deserialize, Serialize};
use serde_json::json;
use state_report::{StateBridgeError, StateKey, fetch_on, install, install_on};
use std::sync::Arc;
use std::time::Duration;

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
    // Given a reporting system whose bridge is installed on its own
    // island key.
    let key = StateKey::scoped("bridge-answers-second-session");
    let system = reporting_system(3).await;
    let _session = install_on(
        key.clone(),
        system.clone(),
        ActorPath::new("state/reporter"),
    )
    .await
    .expect("bridge installed");

    // When a SECOND zenoh session queries the island key. Zenoh closes a
    // query immediately when no queryable is discovered yet, so the retry
    // loop needs real backoff between attempts to ride out peer discovery
    // (~20 tries × 250ms ≈ 5s worst case).
    let client = zenoh::open(zenoh::Config::default())
        .await
        .expect("session");
    let mut payload = None;
    for _ in 0..20 {
        if let Ok(replies) = client
            .get(key.as_str())
            .timeout(Duration::from_secs(1))
            .await
            && let Ok(reply) = replies.recv_async().await
            && let Ok(sample) = reply.result()
            && let Ok(text) = sample.payload().try_to_string()
        {
            payload = Some(text.to_string());
        }
        if payload.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let payload = payload.expect("a second session received a reply");

    // Then the payload decodes into the system's export with fresh content.
    let export: SystemExport = serde_json::from_str(&payload).expect("decodable export");
    assert_eq!(worker_total(&export), 6);

    // Teardown: both sessions close gracefully so peers see clean leaves,
    // not vanished transports.
    client.close().await.expect("client session closed");
    _session.close().await.expect("bridge session closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_returns_a_decodable_export() {
    // Given a reporting system whose bridge is installed on its own
    // island key.
    let key = StateKey::scoped("fetch-decodes");
    let system = reporting_system(5).await;
    let _session = install_on(
        key.clone(),
        system.clone(),
        ActorPath::new("state/reporter"),
    )
    .await
    .expect("bridge installed");

    // When fetching (the retry budget absorbs discovery warm-up).
    let export = fetch_on(key).await.expect("fetch");

    // Then the export reflects the live system.
    assert_eq!(worker_total(&export), 15);
    assert!(
        export
            .actors
            .iter()
            .any(|a| a.path.as_str() == "state/reporter")
    );

    // Teardown: leave the mesh gracefully.
    _session.close().await.expect("bridge session closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn successive_fetches_observe_advancing_state() {
    // Given a reporting system whose bridge is installed on its own
    // island key, fetched once.
    let key = StateKey::scoped("fetch-advances");
    let system = reporting_system(1).await;
    let _session = install_on(
        key.clone(),
        system.clone(),
        ActorPath::new("state/reporter"),
    )
    .await
    .expect("bridge installed");
    let first = fetch_on(key.clone()).await.expect("first fetch");
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
    let second = fetch_on(key).await.expect("second fetch");

    // Then the second reply carries the NEW state, not the first fetch's.
    assert_eq!(worker_total(&second), 10);

    // Teardown: leave the mesh gracefully.
    _session.close().await.expect("bridge session closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn install_rejects_unknown_reporter_path() {
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

// ----- the control plane over the wire --------------------------------------

use state_report::{
    Blueprints, ControlCommand, ControlKey, ControlReply, ControlRequest, ControlRouter,
    PoolBlueprint, ScalePoolCmd, install_control_on, send_command_on,
};

/// A wire-test command that parses `{"text": …}` before echoing — the
/// decode-before-effect shape the malformed-args test leans on.
struct Echo;

impl ControlCommand for Echo {
    fn name(&self) -> &'static str {
        "Echo"
    }

    async fn execute(
        &self,
        _system: &Arc<actor_runtime::system::ActorSystem>,
        args: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let text = args
            .get("text")
            .and_then(|t| t.as_str())
            .ok_or_else(|| "args must be {\"text\": <string>}".to_string())?;
        Ok(json!({ "echoed": text }))
    }
}

/// Installs a control bridge serving `Echo` on its own island key.
async fn echo_bridge(scope: &str) -> (ControlKey, zenoh::Session) {
    let key = ControlKey::scoped(scope);
    let system = Arc::new(actor_runtime::system::ActorSystem::new(
        SystemConfig::production(),
    ));
    let session = install_control_on(key.clone(), system, ControlRouter::new().with(Echo))
        .await
        .expect("control bridge installed");
    (key, session)
}

#[tokio::test(flavor = "multi_thread")]
async fn control_bridge_serves_a_wire_round_trip() {
    // Given a control bridge installed on its own island key.
    let (key, bridge) = echo_bridge("wire-round-trip").await;

    // When a second zenoh session sends a command through the client
    // seam. `send_command_on` is one-shot by contract, so a cold mesh
    // needs the same discovery warm-up the state tests ride out: the
    // first attempt(s) scout for the bridge, later ones land.
    let mut reply = None;
    for _ in 0..20 {
        if let Ok(r) = send_command_on(
            key.clone(),
            ControlRequest {
                command: "Echo".into(),
                args: json!({ "text": "over the wire" }),
            },
        )
        .await
        {
            reply = Some(r);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let reply = reply.expect("a reply within the discovery warm-up");

    // Then the reply is the command's own result — the full wire path
    // (payload attach, bridge decode, dispatch, reply decode) works.
    assert_eq!(
        reply.result(),
        Some(json!({ "echoed": "over the wire" })),
        "the wire round trip carries the command result"
    );

    // Teardown: leave the mesh gracefully.
    bridge.close().await.expect("bridge session closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn control_bridge_replies_an_error_for_unknown_commands_over_the_wire() {
    // Given a control bridge with Echo registered.
    let (key, bridge) = echo_bridge("wire-unknown").await;

    // When sending a command name nothing registered.
    let mut reply = None;
    for _ in 0..20 {
        if let Ok(r) = send_command_on(key.clone(), ControlRequest::bare("Nope")).await {
            reply = Some(r);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let reply = reply.expect("an error reply within the discovery warm-up");

    // Then the bridge answers (never silence) with the unknown-command
    // error.
    match reply {
        ControlReply::Err { error } => {
            assert!(
                error.contains("no such command") && error.contains("Nope"),
                "expected the unknown command named, got: {error}"
            );
        }
        other => panic!("expected an Err reply, got {other:?}"),
    }

    // Teardown.
    bridge.close().await.expect("bridge session closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn control_bridge_replies_an_error_for_malformed_envelopes() {
    // Given a control bridge on its own island key.
    let (key, bridge) = echo_bridge("wire-malformed").await;

    // When a raw zenoh query arrives whose payload is not a
    // ControlRequest at all (the envelope is the transport's to decode).
    let client = zenoh::open(zenoh::Config::default())
        .await
        .expect("session");
    let mut raw_reply = None;
    for _ in 0..20 {
        if let Ok(replies) = client
            .get(key.as_str())
            .payload(b"this is not a control envelope".as_slice())
            .timeout(Duration::from_secs(1))
            .await
            && let Ok(reply) = replies.recv_async().await
            && let Ok(sample) = reply.result()
            && let Ok(text) = sample.payload().try_to_string()
        {
            raw_reply = Some(text.to_string());
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let raw_reply = raw_reply.expect("the bridge answered the malformed envelope");

    // Then the reply is a legible error document — silence on a control
    // channel would read as success.
    let reply: ControlReply = serde_json::from_str(&raw_reply).expect("the error document decodes");
    match reply {
        ControlReply::Err { error } => {
            assert!(
                error.contains("malformed request"),
                "expected a malformed-request error, got: {error}"
            );
        }
        other => panic!("expected an Err reply, got {other:?}"),
    }

    // And the malformed envelope did not wedge the bridge: the next
    // well-formed command still round-trips.
    let mut still_alive = None;
    for _ in 0..20 {
        if let Ok(r) = send_command_on(key.clone(), ControlRequest::bare("Echo")).await {
            still_alive = Some(r);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    match still_alive.expect("the bridge survived a malformed envelope") {
        ControlReply::Err { error } => {
            // Echo with Null args is a decode failure — but an error
            // REPLY from the command proves the serving loop is alive.
            assert!(
                error.contains("text"),
                "expected Echo's own decode error, got: {error}"
            );
        }
        other => panic!("expected a reply, got {other:?}"),
    }

    // Teardown.
    client.close().await.expect("client session closed");
    bridge.close().await.expect("bridge session closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn control_bridge_scales_pools_over_the_wire() {
    // Given a system where a plain actor holds the public path and a
    // control bridge serves ScalePool for it.
    let key = ControlKey::scoped("wire-scale");
    let system = Arc::new(actor_runtime::system::ActorSystem::new(
        SystemConfig::production(),
    ));
    actor_runtime::builder::spawn_es_builder::<Worker>(&system)
        .at(ActorPath::new("api"))
        .args(json!({}))
        .handles::<Work>()
        .emits::<WorkDone>()
        .start();
    let blueprints = Blueprints::new().with_kind(
        "api",
        PoolBlueprint {
            public: ActorPath::new("api"),
            algo: actor_runtime::pool::PoolAlgo::RoundRobin,
            parent: None,
            seed: 42,
            args: Some(json!({})),
            factory: Arc::new(|system, path, args| {
                actor_runtime::builder::spawn_es_builder::<Worker>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .handles::<Work>()
                    .emits::<WorkDone>()
                    .start();
            }),
        },
    );
    let bridge = install_control_on(
        key.clone(),
        system.clone(),
        ControlRouter::new().with(ScalePoolCmd::new(blueprints)),
    )
    .await
    .expect("control bridge installed");

    // When scaling the pool from a second session, riding out discovery.
    let mut reply = None;
    for _ in 0..20 {
        if let Ok(r) = send_command_on(
            key.clone(),
            ControlRequest {
                command: "ScalePool".into(),
                args: json!({ "kind": "api", "workers": 3 }),
            },
        )
        .await
        {
            reply = Some(r);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Then the takeover is confirmed over the wire…
    let reply = reply.expect("a scale reply within the discovery warm-up");
    assert_eq!(
        reply.result(),
        Some(json!({
            "kind": "api",
            "public": "api",
            "workers": 3,
            "algo": "round-robin",
        })),
        "ScalePool's summary crosses the wire"
    );

    // …and the system actually has the pool: work sent to the public
    // path reaches the three workers (the takeover drained the plain
    // holder — the export's pools section is the source of truth).
    let pools = system.export().await.pools;
    assert_eq!(pools.len(), 1, "exactly the scaled pool exists");
    assert_eq!(pools[0].path.as_str(), "api");
    assert_eq!(pools[0].workers.len(), 3, "three worker slots");

    // Teardown.
    bridge.close().await.expect("bridge session closed");
}
