//! Observation and the dead-letter queue.
//!
//! The tap ring is the SOLE observation surface: every waist crossing
//! (send, deliver, ack, spawn, stop, ask, fail, escalate, dead-letter)
//! appends a fact to a bounded, drop-oldest ring the host reads directly
//! (`tap_facts`). Actors observe EVENTS only by declaring `.handles` on
//! them — there is no facts feed to subscribe to.
//!
//! Dead letters are retained in memory WITH their envelopes and are
//! host-managed: `drain_dead_letters` hands them over for inspection or
//! deliberate resend; the runtime never redrives automatically.
//!
//! Run: `cargo run --example observers`

use error_stack::Report;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::OnceLock;
use tracing::Level;
use trouper::actor::{CommandHandler, EventSourcedActor, MsgHandler, ServiceActor};
use trouper::prelude::*;
use trouper::registry::RegistryError;
use trouper::tap::FactKind;

static SINK: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

fn sink() -> &'static Mutex<Vec<String>> {
    SINK.get_or_init(|| Mutex::new(Vec::new()))
}

fn record(line: String) {
    println!("   {line}");
    sink().lock().push(line);
}

#[derive(Command, Deserialize)]
struct Tick {}

#[derive(Command, Deserialize)]
struct StrictCmd {
    #[allow(dead_code)]
    n: i64,
}

#[derive(Event, Serialize, Deserialize)]
struct StrictOk {
    #[allow(dead_code)]
    n: i64,
}

/// An ES actor that handles ONLY StrictCmd (strict schema surface).
#[derive(Serialize, Deserialize, Default)]
struct TickBouncer;

impl EventSourcedActor for TickBouncer {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<StrictCmd>()
            .emits::<StrictOk>()
            .kind(ActorKind::EventSourced)
    }
    fn restore(_args: &Json) -> Self {
        Self
    }
    fn apply(&mut self, _event: &Event) {}
}

impl CommandHandler<StrictCmd> for TickBouncer {
    fn handle(&self, cmd: StrictCmd, _ctx: &mut CmdCtx<'_>) -> Events {
        Events::one(StrictOk { n: cmd.n })
    }
}

struct Ticker;

impl ServiceActor for Ticker {
    async fn start(_args: &Json) -> Result<Self, Report<RegistryError>> {
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
    let system = ActorSystem::new(SystemConfig::production());

    // The traffic source: a plain actor the demo sends commands to.
    trouper::builder::spawn_service_builder::<Ticker>(&system)
        .at(ActorPath::new("ticker"))
        .args(json!({}))
        .handles::<Tick>()
        .start();

    // Traffic: 60 commands produce Sent/Delivered/Acked facts on the tap.
    println!("== tap observation ==");
    for _i in 0..60 {
        let _ = system
            .send(system.envelope(Tick::schema_id(), ActorPath::new("ticker"), json!({})))
            .await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let facts = system.tap_facts();
    let sent = facts
        .iter()
        .filter(|f| matches!(f.kind, FactKind::Sent { .. }))
        .count();
    let delivered = facts
        .iter()
        .filter(|f| matches!(f.kind, FactKind::Delivered { .. }))
        .count();
    let acked = facts
        .iter()
        .filter(|f| matches!(f.kind, FactKind::Acked { .. }))
        .count();
    record(format!(
        "tap recorded {} facts ({sent} Sent, {delivered} Delivered, {acked} Acked) for 60 commands",
        facts.len()
    ));

    // Seed one dead letter: a live actor that does not handle the schema
    // (UnknownSchema → the DLQ retains the envelope).
    println!("== DLQ drain ==");
    let baseline = system.dead_letter_count().await;
    trouper::builder::spawn_es_builder::<TickBouncer>(&system)
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
        if system.dead_letter_count().await > baseline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    // Host-managed drain: inspect the retained envelopes, then decide.
    let drained = system.drain_dead_letters();
    for letter in &drained {
        record(format!(
            "drained: {} → {:?} ({:?}) — envelope schema {}",
            letter.dest, letter.reason, letter.detail, letter.envelope.schema,
        ));
    }
    println!(
        "   {} dead letter(s) drained (Tick → 'strict', which only handles StrictCmd); the queue is empty now ({} retained) — the host chooses whether to resend an envelope deliberately",
        drained.len(),
        system.dead_letter_count().await
    );
    println!("observers example complete");
}
