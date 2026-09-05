//! End-to-end demo of the actor-runtime: the whole scenario the runtime
//! was designed for, run in one pass.
//!
//! Cast:
//! - `Inventory` — an event-sourced actor (ReserveStock / Restock
//!   commands; StockReserved / StockRejected events; a fold into state).
//! - `AuditLog` — a service actor subscribing to `inventory.events`,
//!   appending each event to an in-memory sink (impure by design).
//! - `tally` — a foreign actor registered from a pure JSON schema with
//!   JSON state and closures for decision + fold (no Rust types).
//!
//! Scenario beats: dynamic add → commit → topic fan-out → crash with a
//! pending queue (restart + redelivery exactly once) → snapshot fast
//! path → topic re-consume → ask (reply + timeout) → remove (cascade) →
//! `export()` printing declared vs observed edges and live ES state.

use actor_runtime::actor::{
    CommandHandler, EventSourcedActor, MsgHandler, ServiceActor, TypedEsAdapter,
    TypedServiceAdapter,
};
use actor_runtime::kernel::SnapshotPolicy;
use actor_runtime::prelude::*;
use actor_runtime::registry::RegistryError;
use error_stack::Report;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// The Inventory event-sourced actor
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ReserveStock {
    sku: String,
    qty: i64,
}

impl Schema for ReserveStock {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "ReserveStock".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![
                FieldDef::required("sku", FieldTy::Str),
                FieldDef::required("qty", FieldTy::Int),
            ],
            description: Some("Ask the warehouse to hold stock".into()),
        }
    }
}

#[derive(Deserialize)]
struct Restock {
    sku: String,
    qty: i64,
}

impl Schema for Restock {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Restock".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![
                FieldDef::required("sku", FieldTy::Str),
                FieldDef::required("qty", FieldTy::Int),
            ],
            description: Some("Add stock back".into()),
        }
    }
}

/// A command whose handler panics (crash → restart → redelivery demo).
#[derive(Deserialize)]
struct Explode {
    #[allow(dead_code)] // payload shape; the handler panics before reading
    why: String,
}

impl Schema for Explode {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Explode".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("why", FieldTy::Str)],
            description: Some("Injected handler panic".into()),
        }
    }
}

#[derive(Deserialize)]
struct StockReserved {
    #[allow(dead_code)] // folded via raw payload
    sku: String,
    qty: i64,
}

impl Schema for StockReserved {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "StockReserved".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("sku", FieldTy::Str),
                FieldDef::required("qty", FieldTy::Int),
            ],
            description: None,
        }
    }
}

#[derive(Deserialize)]
struct StockRejected {
    sku: String,
    qty: i64,
}

impl Schema for StockRejected {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "StockRejected".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("sku", FieldTy::Str),
                FieldDef::required("qty", FieldTy::Int),
            ],
            description: None,
        }
    }
}

#[derive(Deserialize)]
struct Restocked {
    #[allow(dead_code)] // folded via raw payload
    sku: String,
    #[allow(dead_code)] // folded via raw payload
    qty: i64,
}

impl Schema for Restocked {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Restocked".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("sku", FieldTy::Str),
                FieldDef::required("qty", FieldTy::Int),
            ],
            description: None,
        }
    }
}

// ---------------------------------------------------------------------------
// The Inventory state + pure decision functions
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Default)]
struct Inventory {
    total: i64,
    reserved: i64,
}

impl EventSourcedActor for Inventory {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<ReserveStock>()
            .handles::<Restock>()
            .handles::<Explode>()
            .emits::<StockReserved>()
            .emits::<StockRejected>()
            .emits_on_topic(Topic::new("inventory.events"))
            .kind(ActorKind::EventSourced)
    }

    fn restore(_args: &serde_json::Value) -> Self {
        Self::default()
    }

    /// THE mutation — used for live application AND replay.
    fn apply(&mut self, event: &Event) {
        match event.schema.as_str() {
            "StockReserved@1" => {
                self.reserved += event.payload["qty"].as_i64().unwrap_or(0);
            }
            "Restocked@1" => {
                self.total += event.payload["qty"].as_i64().unwrap_or(0);
            }
            // Rejections are recorded facts, not stock.
            "StockRejected@1" => {}
            _ => {}
        }
    }
}

impl CommandHandler<ReserveStock> for Inventory {
    /// Pure decision: no I/O, no mutation, no await. Returns facts.
    fn handle(&self, cmd: ReserveStock, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        if self.total - self.reserved >= cmd.qty {
            vec![Event::new(
                StockReserved::schema_id(),
                json!({ "sku": cmd.sku, "qty": cmd.qty }),
            )]
        } else {
            vec![Event::new(
                StockRejected::schema_id(),
                json!({ "sku": cmd.sku, "qty": cmd.qty, "available": self.total - self.reserved }),
            )]
        }
    }
}

impl CommandHandler<Restock> for Inventory {
    fn handle(&self, cmd: Restock, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        vec![Event::new(
            Restocked::schema_id(),
            json!({ "sku": cmd.sku, "qty": cmd.qty }),
        )]
    }
}

impl CommandHandler<Explode> for Inventory {
    fn handle(&self, _cmd: Explode, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        panic!("injected warehouse failure");
    }
}

// ---------------------------------------------------------------------------
// The AuditLog service actor (impure by design: appends to a sink)
// ---------------------------------------------------------------------------

static AUDIT: std::sync::OnceLock<std::sync::Mutex<Vec<String>>> = std::sync::OnceLock::new();

fn audit_log() -> &'static std::sync::Mutex<Vec<String>> {
    AUDIT.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

struct AuditLog;

impl ServiceActor for AuditLog {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<StockReserved>()
            .handles::<StockRejected>()
            .handles::<Probe>()
            .kind(ActorKind::Service)
    }

    async fn start(_args: &serde_json::Value) -> Result<Self, Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<StockReserved> for AuditLog {
    async fn handle(&mut self, msg: StockReserved, _ctx: &mut MsgCtx<'_>) {
        audit_log()
            .lock()
            .expect("audit lock")
            .push(format!("reserved sku={} qty={}", msg.sku, msg.qty));
    }
}

impl MsgHandler<StockRejected> for AuditLog {
    async fn handle(&mut self, msg: StockRejected, _ctx: &mut MsgCtx<'_>) {
        audit_log()
            .lock()
            .expect("audit lock")
            .push(format!("rejected sku={} qty={}", msg.sku, msg.qty));
    }
}

impl MsgHandler<Probe> for AuditLog {
    async fn handle(&mut self, msg: Probe, ctx: &mut MsgCtx<'_>) {
        // The ask demo's callee: reply inline (demonstrates the slot path).
        ctx.core
            .reply(ProbeOk::schema_id(), json!({ "echo": msg.body }));
    }
}

#[derive(Deserialize)]
struct Probe {
    body: String,
}

impl Schema for Probe {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Probe".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("body", FieldTy::Str)],
            description: Some("Ask target that replies".into()),
        }
    }
}

#[derive(Deserialize)]
struct ProbeOk {
    #[allow(dead_code)] // printed via raw JSON
    echo: String,
}

impl Schema for ProbeOk {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "ProbeOk".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("echo", FieldTy::Str)],
            description: None,
        }
    }
}

// ---------------------------------------------------------------------------
// The driver
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let system = Arc::new(ActorSystem::new(SystemConfig::production()));

    // -- 1. Dynamic add: register schemas, spawn the actors ----------------
    {
        let _ = ReserveStock::schema_id();
        // Typed registration: each Schema impl registers itself.
        let _ = (
            system.register_schema::<ReserveStock>(),
            system.register_schema::<Restock>(),
            system.register_schema::<Explode>(),
            system.register_schema::<StockReserved>(),
            system.register_schema::<StockRejected>(),
            system.register_schema::<Restocked>(),
            system.register_schema::<Probe>(),
            system.register_schema::<ProbeOk>(),
            system.register_schema::<RunAsks>(),
        );
    }

    let foreign_schema = system
        .register_schema_json(json!({
            "name": "TallyAdd", "version": 1, "kind": "command",
            "fields": [{ "name": "delta", "ty": "int" }]
        }))
        .expect("valid");

    println!("== 1. dynamic add ==");
    system.spawn_es::<Inventory, _>(
        Path::new("warehouse"),
        &json!({}),
        SpawnOpts {
            snapshot: SnapshotPolicy::EveryN(3),
            ..SpawnOpts::default()
        },
        || {
            vec![
                Arc::new(TypedEsAdapter::<Inventory, ReserveStock>::new::<ReserveStock>()),
                Arc::new(TypedEsAdapter::<Inventory, Restock>::new::<Restock>()),
                Arc::new(TypedEsAdapter::<Inventory, Explode>::new::<Explode>()),
            ]
        },
    );
    system.spawn_service::<AuditLog, _>(
        Path::new("auditor"),
        &json!({}),
        SpawnOpts::default(),
        || {
            vec![
                Arc::new(TypedServiceAdapter::<AuditLog, StockReserved>::new::<
                    StockReserved,
                >()),
                Arc::new(TypedServiceAdapter::<AuditLog, StockRejected>::new::<
                    StockRejected,
                >()),
                Arc::new(TypedServiceAdapter::<AuditLog, Probe>::new::<Probe>()),
            ]
        },
    );
    let tally_schema = foreign_schema.clone();
    system.spawn_es_foreign(
        Path::new("tally"),
        foreign_schema.clone(),
        json!({ "total": 0 }),
        Arc::new(move |_state, cmd, _ctx| {
            vec![Event::new(
                tally_schema.clone(),
                json!({ "delta": cmd["delta"].as_i64().unwrap_or(0) }),
            )]
        }),
        Arc::new(|state: &mut serde_json::Value, ev: &Event| {
            state["total"] = json!(
                state["total"].as_i64().unwrap_or(0) + ev.payload["delta"].as_i64().unwrap_or(0)
            );
        }),
        SpawnOpts::default(),
    );
    println!("   spawned: warehouse (ES), auditor (service), tally (foreign)");

    // -- 2. Commit some commands; events fan out to the topic -------------
    println!("== 2. commands → events → topic ==");
    let topic = Topic::new("inventory.events");
    system
        .subscribe(&Path::new("auditor"), &topic, None)
        .expect("subscribe");
    for qty in [5, 4] {
        system
            .send(system.envelope(
                ReserveStock::schema_id(),
                Path::new("warehouse"),
                json!({ "sku": "widget", "qty": qty }),
            ))
            .await
            .expect("delivered");
    }
    system
        .send(system.envelope(foreign_schema, Path::new("tally"), json!({ "delta": 9 })))
        .await
        .expect("delivered");
    wait(|| async { system.es_state(&Path::new("tally")).await == Some(json!({ "total": 9 })) })
        .await;
    println!(
        "   tap facts: {:?}",
        system
            .tap_facts()
            .iter()
            .map(|f| format!("{:?}", f.kind)
                .split('{')
                .next()
                .unwrap()
                .to_owned())
            .collect::<Vec<_>>()
    );
    eprintln!("   dead letters: {:?}", system.dead_letter_reasons().await);
    wait(|| async { !audit_log().lock().expect("lock").is_empty() }).await;
    println!(
        "   audit log: {:?}",
        audit_log().lock().expect("lock").clone()
    );

    // -- 3. Crash → restart → redelivery (at-least-once) -------------------
    println!("== 3. crash with a pending queue ==");
    system
        .send(system.envelope(
            Explode::schema_id(),
            Path::new("warehouse"),
            json!({ "why": "demo" }),
        ))
        .await
        .expect("enqueued");
    // Queued behind the poison message; processed after the restart.
    system
        .send(system.envelope(
            Restock::schema_id(),
            Path::new("warehouse"),
            json!({ "sku": "widget", "qty": 7 }),
        ))
        .await
        .expect("enqueued");
    wait(|| async {
        system
            .tap_facts()
            .iter()
            .any(|f| matches!(&f.kind, actor_runtime::tap::FactKind::Failed { path, .. } if path == &Path::new("warehouse")))
    })
    .await;
    println!("   warehouse crashed mid-message (Failed fact emitted)");

    // Restart: same path, inbox intact, state rebuilt from the journal,
    // and the HEAD message (the poison) redelivered exactly once — it
    // panics again, which is the contract: redelivery is at-least-once
    // and the failed command produced no events.
    system
        .restart_es(&Path::new("warehouse"), &json!({}))
        .await
        .expect("restart");
    wait(|| async {
        system
            .tap_facts()
            .iter()
            .filter(|f| matches!(&f.kind, actor_runtime::tap::FactKind::Failed { path, .. } if path == &Path::new("warehouse")))
            .count()
            >= 2
    })
    .await;
    let state = system
        .es_state(&Path::new("warehouse"))
        .await
        .expect("state");
    println!("   redelivered poison panicked again (at-least-once); state: {state}");

    // Flush the poison out of the queue (stop → DLQ — the pending
    // restock is undeliverable while the actor is down), then re-add at
    // the SAME path: identity is the path, so senders never care.
    system.stop(&Path::new("warehouse")).await;
    let dead_letters = system.dead_letter_count().await;
    println!(
        "   stopped; undelivered flushed to the DLQ ({} total)",
        dead_letters
    );
    system.spawn_es::<Inventory, _>(
        Path::new("warehouse"),
        &json!({}),
        SpawnOpts {
            snapshot: SnapshotPolicy::EveryN(3),
            ..SpawnOpts::default()
        },
        || {
            vec![
                Arc::new(TypedEsAdapter::<Inventory, ReserveStock>::new::<ReserveStock>()),
                Arc::new(TypedEsAdapter::<Inventory, Restock>::new::<Restock>()),
                Arc::new(TypedEsAdapter::<Inventory, Explode>::new::<Explode>()),
            ]
        },
    );
    system
        .send(system.envelope(
            Restock::schema_id(),
            Path::new("warehouse"),
            json!({ "sku": "widget", "qty": 7 }),
        ))
        .await
        .expect("delivered to the re-added path");
    wait(|| async {
        system
            .es_state(&Path::new("warehouse"))
            .await
            .and_then(|s| s["total"].as_i64())
            == Some(7)
    })
    .await;
    let state = system
        .es_state(&Path::new("warehouse"))
        .await
        .expect("state");
    println!("   re-added at the same path; fresh restock delivered: {state}");

    // -- 4. Snapshot fast path ---------------------------------------------
    println!("== 4. snapshot fast path ==");
    // Two more commits land (4 events total → past EveryN(3): a
    // snapshot exists in the journal).
    for qty in [1, 2] {
        system
            .send(system.envelope(
                Restock::schema_id(),
                Path::new("warehouse"),
                json!({ "sku": "widget", "qty": qty }),
            ))
            .await
            .expect("delivered");
    }
    wait(|| async {
        system
            .inbox_cursor(&Path::new("warehouse"))
            .map(|c| c.as_u64())
            == Some(3)
    })
    .await;

    // Crash on a poison command, then restart. The rebuild loads the
    // LAST SNAPSHOT and applies only the journal tail (fast path); the
    // state proves it: the folded total survived the crash.
    system
        .send(system.envelope(
            Explode::schema_id(),
            Path::new("warehouse"),
            json!({ "why": "snapshot demo" }),
        ))
        .await
        .expect("enqueued");
    wait(|| async {
        system
            .tap_facts()
            .iter()
            .filter(|f| matches!(&f.kind, actor_runtime::tap::FactKind::Failed { path, .. } if path == &Path::new("warehouse")))
            .count()
            >= 3
    })
    .await;
    system
        .restart_es(&Path::new("warehouse"), &json!({}))
        .await
        .expect("restart");
    let state = system
        .es_state(&Path::new("warehouse"))
        .await
        .expect("state");
    println!("   state after snapshot-anchored restart: {state} (folded total survived)");

    // The poison was redelivered (it panicked again — at-least-once);
    // flush it through stop → DLQ, and re-add at the same path so the
    // next beats start clean.
    system.stop(&Path::new("warehouse")).await;
    system.spawn_es::<Inventory, _>(
        Path::new("warehouse"),
        &json!({}),
        SpawnOpts::default(),
        || {
            vec![
                Arc::new(TypedEsAdapter::<Inventory, ReserveStock>::new::<ReserveStock>()),
                Arc::new(TypedEsAdapter::<Inventory, Restock>::new::<Restock>()),
                Arc::new(TypedEsAdapter::<Inventory, Explode>::new::<Explode>()),
            ]
        },
    );
    println!(
        "   poison flushed ({} dead letters); warehouse re-added for the next beats",
        system.dead_letter_count().await
    );

    // -- 5. Topic re-consume (cursor reset; at-least-once) ------------------
    println!("== 5. topic re-consume ==");
    let (floor, _) = system.topic_range(&topic).expect("topic live");
    system
        .reset_topic_cursor(&Path::new("auditor"), &topic, floor)
        .expect("reset");
    let before = audit_log().lock().expect("lock").len();
    // A fresh publish triggers the pump pass that also serves the reset
    // cursor: the retained backlog is re-delivered (at-least-once), in
    // log order, ahead of the new event.
    system
        .send(system.envelope(
            Restock::schema_id(),
            Path::new("warehouse"),
            json!({ "sku": "widget", "qty": 1 }),
        ))
        .await
        .expect("delivered");
    wait(|| async {
        system
            .inbox_cursor(&Path::new("warehouse"))
            .map(|c| c.as_u64())
            == Some(1)
    })
    .await;
    wait(|| async { audit_log().lock().expect("lock").len() > before }).await;
    println!(
        "   audit log after re-consume: {:?}",
        audit_log().lock().expect("lock").clone()
    );

    // -- 6. Ask: reply + timeout -------------------------------------------
    println!("== 6. ask (reply and timeout) ==");
    system.spawn_service::<Asker, _>(Path::new("asker"), &json!({}), SpawnOpts::default(), || {
        vec![Arc::new(TypedServiceAdapter::<Asker, RunAsks>::new::<
            RunAsks,
        >())]
    });
    bind_ask_results(&system);
    system
        .send(system.envelope(RunAsks::schema_id(), Path::new("asker"), json!({})))
        .await
        .expect("enqueued");
    wait(|| async { ask_results().lock().expect("lock").len() >= 2 }).await;
    for line in ask_results().lock().expect("lock").iter() {
        println!("   {line}");
    }

    // -- 7. Remove + export -------------------------------------------------
    println!("== 7. remove + export ==");
    system.stop(&Path::new("tally")).await;
    let export = system.export().await;
    println!("{}", serde_json::to_string_pretty(&export).expect("json"));

    let declared = export.declared_edges.len();
    let observed = export.observed_edges.len();
    println!("-- declared edges: {declared}, observed edges: {observed}");
    println!(
        "-- warehouse live state: {:?}",
        export
            .actors
            .iter()
            .find(|a| a.path == Path::new("warehouse"))
            .and_then(|a| a.state.clone())
    );
    println!("demo complete");
}

// ---------------------------------------------------------------------------
// The Asker service actor: demonstrates ctx.ask (reply + timeout)
// ---------------------------------------------------------------------------

static ASK_RESULTS: std::sync::OnceLock<std::sync::Mutex<Vec<String>>> = std::sync::OnceLock::new();

fn ask_results() -> &'static std::sync::Mutex<Vec<String>> {
    ASK_RESULTS.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

fn bind_ask_results(system: &Arc<ActorSystem>) {
    // Detached watcher: mirrors settled asks into the results list.
    let system = system.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_millis(5));
            let facts = system.tap_facts();
            let mut results = ask_results().lock().expect("lock");
            for fact in facts {
                if let actor_runtime::tap::FactKind::AskSettled { outcome, .. } = &fact.kind {
                    let line = format!("ask settled: {outcome:?}");
                    if !results.contains(&line) {
                        results.push(line);
                    }
                }
            }
            if results.len() >= 2 {
                break;
            }
        }
    });
}

#[derive(Deserialize)]
struct RunAsks {}

impl Schema for RunAsks {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "RunAsks".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![],
            description: Some("Runs both ask flavors".into()),
        }
    }
}

struct Asker;

impl ServiceActor for Asker {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<RunAsks>()
            .kind(ActorKind::Service)
    }
    async fn start(_args: &serde_json::Value) -> Result<Self, Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<RunAsks> for Asker {
    async fn handle(&mut self, _msg: RunAsks, ctx: &mut MsgCtx<'_>) {
        // Inline ask that replies in time.
        let reply = ctx
            .ask(
                actor_runtime::envelope::Address::Path(Path::new("auditor")),
                Probe::schema_id(),
                json!({ "body": "hello" }),
                Duration::from_secs(2),
            )
            .await;
        let line = match reply {
            Ok(value) => format!("ask replied: {value}"),
            Err(err) => format!("ask failed: {err:?}"),
        };
        ask_results().lock().expect("lock").push(line);

        // Ask a destination that never replies — the mandatory timeout
        // produces an AskSettled(Timeout) fact.
        let outcome = ctx
            .ask(
                actor_runtime::envelope::Address::Path(Path::new("nobody")),
                Probe::schema_id(),
                json!({ "body": "anyone there?" }),
                Duration::from_millis(80),
            )
            .await;
        let line = match outcome {
            Ok(value) => format!("unexpected reply: {value}"),
            Err(err) => format!("ask timed out as designed: {:?}", err.current_context()),
        };
        ask_results().lock().expect("lock").push(line);
    }
}

/// Polls `cond` until true (2s budget) — demo pacing helper.
async fn wait<F, Fut>(cond: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..1_000 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("demo condition never became true");
}
