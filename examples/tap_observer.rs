//! Watching the wire: opt-in runtime observation.
//!
//! Every boundary crossing (send, deliver, ack, spawn, stop, ask, …) can
//! be OBSERVED by a handler you install. With no handler installed — the
//! default — nothing is constructed: observation costs nothing until you
//! ask for it, and uncaptured observations are gone (there is no
//! history; the DLQ is the only after-the-fact artifact).
//!
//! The handler rules (see `trouper::observe`):
//! - **Never call back into the system** from the handler — it runs on
//!   the message path. Fan observations out into your own channel and
//!   process them elsewhere (that is exactly what this demo does).
//! - **Keep it fast**: observation cost is paid by the message path.
//! - A panicking handler is isolated; the message keeps flowing.
//!
//! This demo installs a handler that pushes each observation into an
//! owned std channel; a drain thread pretty-prints the Sent → Delivered
//! → Acked flow for a few commands, then the handler is REMOVED
//! (runtime toggle) and the same traffic flows unobserved.
//!
//! Run: `cargo run --example tap_observer`

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::observe::{ObservationHandler, ObservationKind};
use trouper::prelude::*;

#[derive(Command, Serialize, Deserialize, Clone)]
struct Add {
    n: i64,
}

#[derive(Event, Serialize, Deserialize, Clone)]
struct Added {
    n: i64,
}

#[derive(Serialize, Deserialize, Default)]
struct Counter {
    total: i64,
}

impl EventSourcedActor for Counter {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<Add>()
            .emits::<Added>()
            .kind(ActorKind::EventSourced)
    }
    fn restore(_args: &Json) -> Self {
        Self::default()
    }
    fn apply(&mut self, event: &Event) {
        self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
    }
}

impl CommandHandler<Add> for Counter {
    fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> Events {
        Events::one(Added { n: cmd.n })
    }
}

#[tokio::main]
async fn main() {
    let system = ActorSystem::new(SystemConfig::production());
    system.register_schema::<Add>();
    system.register_schema::<Added>();

    // The entity the demo talks to.
    trouper::builder::spawn_es_builder::<Counter>(&system)
        .at(ActorPath::new("counter"))
        .args(json!({}))
        .start();

    // The handler: push every observation into an owned channel. The
    // handler itself does almost nothing (a channel send) — the WORK
    // happens on the drain thread, off the message path.
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let drainer = std::thread::spawn(move || {
        for line in rx {
            println!("  {line}");
        }
    });
    system.set_observation({
        let tx = tx.clone();
        let handler: ObservationHandler = Arc::new(move |observation| {
            let line = match &observation.kind {
                ObservationKind::Sent { from, dest, .. } => format!(
                    "Sent      {} -> {dest}",
                    from.as_ref()
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| "system".into())
                ),
                ObservationKind::Delivered { to, .. } => format!("Delivered -> {to}"),
                ObservationKind::Acked { to, .. } => format!("Acked     {to} (committed)"),
                _ => return,
            };
            let _ = tx.send(line);
        });
        handler
    });

    println!("== observation ON: 3 commands, Sent -> Delivered -> Acked ==");
    for n in 1..=3 {
        system
            .tell(ActorPath::new("counter"), Add { n })
            .await
            .expect("committed");
    }
    // Let the drain thread catch up, then TOGGLE OFF at runtime: the
    // same traffic flows again, but nothing is constructed any more.
    std::thread::sleep(std::time::Duration::from_millis(100));
    system.clear_observation();
    drop(tx); // the drain thread ends when the channel closes
    let _ = drainer.join();

    println!("== observation OFF: 3 more commands, nothing observed ==");
    for n in 4..=6 {
        system
            .tell(ActorPath::new("counter"), Add { n })
            .await
            .expect("committed");
    }
    let seen = system
        .es_state(&ActorPath::new("counter"))
        .await
        .and_then(|s| s["total"].as_i64())
        .unwrap_or(0);
    println!("counter total = {seen} (all six committed; only three were observed)");

    println!("tap_observer example complete");
}
