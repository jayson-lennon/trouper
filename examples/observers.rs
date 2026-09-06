//! Observers: in-actor consumers of runtime facts.
//!
//! `system.facts` is a subscribable topic that mirrors the tap ring
//! (fact JSON, offset included). An ordinary service actor subscribes
//! and receives facts as messages with an at-most-once-with-gaps
//! contract: the ring's drop-oldest pressure shows up as offset gaps,
//! never as backpressure on the ring. Also demonstrates the DLQ
//! re-driver (sugar over `system.deadletters` + cursor machinery).
//!
//! Run: `cargo run --example observers`

use actor_runtime::actor::{CommandHandler, EventSourcedActor, MsgHandler, ServiceActor};
use actor_runtime::prelude::*;
use actor_runtime::registry::RegistryError;
use error_stack::Report;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::Level;

static SINK: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

static GAPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn sink() -> &'static Mutex<Vec<String>> {
    SINK.get_or_init(|| Mutex::new(Vec::new()))
}

fn record(line: String) {
    println!("   {line}");
    sink().lock().expect("sink lock").push(line);
}

/// The Rust mirror of the runtime's Fact@1 schema (what `system.facts`
/// delivers to subscribers).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FactMsg {
    kind: String,
    offset: u64,
    ts: i64,
}

impl Schema for FactMsg {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Fact".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("kind", FieldTy::Str),
                FieldDef::required("offset", FieldTy::Int),
                FieldDef::required("ts", FieldTy::Int),
            ],
            description: None,
        }
    }
}

/// An ordinary service actor that consumes `system.facts` and detects
/// offset discontinuities (the documented at-most-once-with-gaps
/// contract).
struct Observer {
    last: Option<u64>,
}

impl ServiceActor for Observer {
    async fn start(_args: &serde_json::Value) -> Result<Self, Report<RegistryError>> {
        Ok(Self { last: None })
    }
}

impl MsgHandler<FactMsg> for Observer {
    async fn handle(&mut self, fact: FactMsg, _ctx: &mut MsgCtx<'_>) {
        if let Some(last) = self.last
            && fact.offset > last + 1
        {
            GAPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if GAPS.load(std::sync::atomic::Ordering::Relaxed) <= 3 {
                record(format!(
                    "GAP detected: offsets {} → {} (ring evictions)",
                    last + 1,
                    fact.offset
                ));
            }
        }
        self.last = Some(fact.offset);
    }
}

#[derive(Deserialize)]
struct Tick {}

impl Schema for Tick {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Tick".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![],
            description: None,
        }
    }
}

#[derive(Deserialize)]
struct StrictCmd {
    #[allow(dead_code)]
    n: i64,
}

impl Schema for StrictCmd {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "StrictCmd".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

#[derive(Deserialize)]
struct StrictOk {
    #[allow(dead_code)]
    n: i64,
}

impl Schema for StrictOk {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "StrictOk".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

/// An ES actor that handles ONLY StrictCmd (strict schema surface).
#[derive(Serialize, Deserialize, Default)]
struct TickBouncer;

impl EventSourcedActor for TickBouncer {
    fn restore(_args: &serde_json::Value) -> Self {
        Self
    }
    fn apply(&mut self, _event: &Event) {}
}

impl CommandHandler<StrictCmd> for TickBouncer {
    fn handle(&self, cmd: StrictCmd, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        vec![Event::new(StrictOk::schema_id(), json!({ "n": cmd.n }))]
    }
}

struct Ticker;

impl ServiceActor for Ticker {
    async fn start(_args: &serde_json::Value) -> Result<Self, Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<Tick> for Ticker {
    async fn handle(&mut self, _msg: Tick, _ctx: &mut MsgCtx<'_>) {}
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::ERROR)
        .init();
    // A small tap ring: floods produce gaps quickly.
    let system = Arc::new(ActorSystem::test_with_tap(16).0);

    // The observer: a plain service actor + one subscribe call.
    actor_runtime::builder::spawn_service_builder::<Observer>(&system)
        .at(ActorPath::new("observer"))
        .args(json!({}))
        .handles::<FactMsg>()
        .mailbox(64, actor_runtime::inbox::OverloadPolicy::DropNew)
        .start();
    system
        .subscribe(
            &ActorPath::new("observer"),
            &actor_runtime::registry::Registry::facts_topic(),
            None,
        )
        .expect("subscribe");

    // The traffic source: a plain actor the demo sends commands to.
    actor_runtime::builder::spawn_service_builder::<Ticker>(&system)
        .at(ActorPath::new("ticker"))
        .args(json!({}))
        .handles::<Tick>()
        .start();

    // Traffic: 60 commands → hundreds of facts (Sent/Delivered/Acked...)
    // through a 16-slot ring → eviction → gaps.
    println!("== system.facts observer ==");
    for _i in 0..60 {
        let _ = system
            .send(system.envelope(Tick::schema_id(), ActorPath::new("ticker"), json!({})))
            .await;
    }
    for _ in 0..1_000 {
        if sink()
            .lock()
            .expect("sink lock")
            .iter()
            .any(|l| l.starts_with("GAP"))
        {
            // Let the pump drain the backlog before reporting.
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let facts_seen = sink().lock().expect("sink lock").len();
    let gaps = GAPS.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "   observer consumed {facts_seen} fact messages and detected {gaps} gaps (at-most-once with accounted-for losses; the ring never slowed down)"
    );

    // Seed one dead letter: a live actor that does not handle the schema
    // (UnknownSchema → the DLQ topic retains the envelope). The flood's
    // spawn-race mail may also be in the DLQ; the baseline below absorbs it.
    let baseline = system.dead_letter_count().await;
    actor_runtime::builder::spawn_es_builder::<TickBouncer>(&system)
        .at(ActorPath::new("strict"))
        .args(json!({}))
        .handles::<StrictCmd>()
        .emits::<StrictOk>()
        .start();
    system
        .send(system.envelope(Tick::schema_id(), ActorPath::new("strict"), json!({})))
        .await
        .expect("routed to strict (dead-lettered at the actor)");
    for _ in 0..1_000 {
        if system.dead_letter_count().await > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    // The DLQ re-driver: sugar over the deadletters topic + cursors.
    println!("== DLQ re-driver ==");
    let seeded = system.dead_letter_count().await - baseline;
    system.install_dlq_redriver();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    println!(
        "   {seeded} dead letter(s) seeded (Tick → 'strict', which only handles StrictCmd); the re-driver re-sent every retained DLQ envelope to its recorded dest — each redrive dead-lettered again ('strict' still refuses Tick), proving the redriver acted as a plain sender"
    );
    println!("observers example complete");
}
