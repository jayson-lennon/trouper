//! The emit contract: declared events flow, undeclared events are
//! dropped BEFORE journal append (DeadLettered(UndeclaredEvent)
//! observation + a tracing error). Journals only ever contain declared
//! schemas, so `fold(journal) == state` holds even for a decision that
//! mixes both.
//!
//! Run: `cargo run --example emit_contract`

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::Level;
use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::observe::{ObservationHandler, ObservationKind};
use trouper::prelude::*;

#[derive(Command, Serialize, Deserialize, Clone)]
struct Ping {
    n: i64,
}

#[derive(Event, Serialize, Deserialize, Clone)]
struct Ponged {
    #[allow(dead_code)]
    n: i64,
}

/// NEVER declared in any manifest — the counter emits it anyway.
#[derive(Event, Serialize, Deserialize, Clone)]
#[schema(description = "Emitted but never declared (the bug)")]
struct SecretPing {
    #[allow(dead_code)]
    n: i64,
}

#[derive(Serialize, Deserialize, Default)]
struct Counter {
    seen: i64,
}

impl EventSourcedActor for Counter {
    fn manifest() -> ActorManifest {
        // ONLY Ponged is declared — SecretPing is the contract break.
        ActorManifest::new()
            .handles::<Ping>()
            .emits::<Ponged>()
            .kind(ActorKind::EventSourced)
    }

    fn restore(_args: &Json) -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "Ponged" {
            self.seen += event.payload_json()["n"].as_i64().unwrap_or(0);
        }
    }
}

impl CommandHandler<Ping> for Counter {
    /// A MIXED decision: one declared event, one undeclared. The declared
    /// half lands; the undeclared half is dropped pre-append — the step
    /// is not failed, and state stays fold-consistent.
    fn handle(&self, cmd: Ping, _ctx: &mut CmdCtx<'_>) -> Events {
        let mut ev = Events::new();
        ev.push_event(Ponged { n: cmd.n }); // declared: journals + fans out
        ev.push_event(SecretPing { n: cmd.n }); // undeclared: dropped pre-append
        ev
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::ERROR)
        .init();
    let system = ActorSystem::new(SystemConfig::production());
    // Opt-in observation: count DeadLettered(UndeclaredEvent) drops in a
    // handler-owned log.
    let undeclared_drops: Arc<parking_lot::Mutex<usize>> = Arc::default();
    system.set_observation({
        let undeclared_drops = undeclared_drops.clone();
        let handler: ObservationHandler = Arc::new(move |observation| {
            if let ObservationKind::DeadLettered {
                reason: trouper::kernel::DeadLetterReason::UndeclaredEvent,
                ..
            } = &observation.kind
            {
                *undeclared_drops.lock() += 1;
            }
        });
        handler
    });

    trouper::builder::spawn_es_builder::<Counter>(&system)
        .at(ActorPath::new("counter"))
        .args(json!({}))
        .handles::<Ping>()
        .emits::<Ponged>()
        .start();

    println!("== emit contract ==");
    for n in 1..=3 {
        system
            .send(system.envelope(
                Ping::schema_id(),
                ActorPath::new("counter"),
                json!({ "n": n }),
            ))
            .await
            .expect("delivered");
    }
    wait(|| async { *undeclared_drops.lock() >= 3 }).await;

    let state = system
        .es_state(&ActorPath::new("counter"))
        .await
        .expect("state");
    println!("   declared events applied (seen = 1+2+3): {state:?}");
    let undeclared = *undeclared_drops.lock();
    println!(
        "   undeclared emits dropped pre-append: {undeclared} DeadLettered(UndeclaredEvent) observations (+ a tracing::error! each)"
    );
    println!("   journals contain only declared schemas — replay stays fold-consistent");
    println!("emit_contract example complete");
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
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    panic!("demo condition never became true");
}
