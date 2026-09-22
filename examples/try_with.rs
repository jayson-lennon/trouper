//! Typed ZERO-COPY reads, non-blocking pair: `try_with_es_state` and
//! `try_with_projector_state`.
//!
//! The `try_` reads run a sync closure over the LIVE state under its
//! lock — no serialize, no clone, no decode — and never block: the lock
//! is taken with `try_lock`, so a read that races the fold (microseconds)
//! reads `None` and the caller keeps its previous frame. That is the
//! render-loop pattern: a GUI thread reads projection state per frame
//! without awaiting anything.
//!
//! `None` is "no `<A>` state at this path right now" — no live entry
//! (unknown or cold path), a wrong type (this demo reads the counter as a
//! different type), or a busy lock.
//!
//! Run: `cargo run --example try_with`
//!
//! The awaiting twins (including the projector cold-wake) are demoed in
//! `examples/with.rs`.

use serde::{Deserialize, Serialize};
use serde_json::json;
use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::prelude::*;

// ---- A journaled counter (the entity whose state we read) -------------

#[derive(Command, Debug, Deserialize, Serialize)]
#[schema(description = "Add to the counter.")]
struct Add {
    n: i64,
}

#[derive(Event, Debug, Serialize, Deserialize)]
struct Added {
    n: i64,
}
// NOTE: no shard key on this fact, and none is needed — this demo uses a
// STANDALONE projector, which receives broadcast copies by schema alone.
// A shard key only matters for projector SETS (per-key `public/<key>`
// paths, activated on demand): there, the registry REJECTS the set at
// install unless one consumed schema declares the key field with
// `#[schema(shard_key)]`. See examples/with.rs for that mode.

#[derive(Default, Debug, Serialize, Deserialize)]
struct Counter {
    total: i64,
}
impl EventSourcedActor for Counter {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::EventSourced)
    }
    fn restore(_args: &Json) -> Self {
        Self::default()
    }
    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "Added" {
            self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
        }
    }
}
impl CommandHandler<Add> for Counter {
    fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> Events {
        Events::one(Added { n: cmd.n })
    }
}

/// A DIFFERENT entity type — never spawned. Stand-in for "the host asks
/// for the wrong type at a live path" (and for foreign actors, whose JSON
/// state misses the typed seam the same way).
#[derive(Default, Debug, Serialize, Deserialize)]
struct Other {
    #[allow(dead_code)] // never live; the downcast target's shape only
    n: i64,
}
impl EventSourcedActor for Other {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::EventSourced)
    }
    fn apply(&mut self, _event: &Event) {}
}

// ---- A read model (the projector whose fold we read) -------------------

/// Folds `Added` facts into a running total and a bell that rings at 30.
#[derive(Default, Debug, Serialize, Deserialize)]
struct Totals {
    total: i64,
    bell: bool,
}
impl Projector for Totals {
    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "Added" {
            self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
            self.bell = self.total >= 30;
        }
    }
}

#[tokio::main]
async fn main() {
    let (system, _clock) = ActorSystem::test();
    system.register_schema::<Add>();
    system.register_schema::<Added>();

    let counter = trouper::builder::spawn_es_builder::<Counter>(&system)
        .at(ActorPath::new("counter"))
        .args(json!({}))
        .handles::<Add>()
        .emits::<Added>()
        .start();

    // A STANDALONE projector: one read model at a fixed path, folding the
    // broadcast copies of its consumed schema. No key derivation, no wake
    // path (that's why its builder has no `passivate_after`) — and no
    // shard key needed on the fact, unlike the projector SET demoed in
    // examples/with.rs.
    let projector = trouper::builder::spawn_projector_builder::<Totals>(&system)
        .at(ActorPath::new("totals"))
        .args(json!({}))
        .consumes::<Added>()
        .start();

    // ---- Frame reads over the ENTITY -----------------------------------
    for n in [10, 20, 30] {
        system
            .send(system.envelope(Add::schema_id(), counter.clone(), json!({ "n": n })))
            .await
            .expect("delivered");
    }

    // The typed frame read: the closure sees the LIVE fold, so the value
    // it returns needs no serialize + decode round-trip.
    let mut frames = 0;
    let mut last: Option<i64> = None;
    while last != Some(60) {
        // NEVER blocks: if the fold holds the lock right now, this frame
        // keeps the previous value (None on the very first frame).
        if let Some(total) = system.try_with_es_state::<Counter, _>(&counter, |c| c.total) {
            last = Some(total);
        }
        frames += 1;
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(frames < 5_000, "demo never converged");
    }
    println!("counter frame read : total={last:?} after {frames} try_ frames");

    // ---- Frame reads over the PROJECTOR --------------------------------
    // The projector folds a broadcast copy of every Added fact its
    // .consumes() declaration attracts. Same never-blocking read.
    let mut seen_bell = false;
    for _ in 0..5_000 {
        if system.try_with_projector_state::<Totals, _>(&projector, |t| t.bell) == Some(true) {
            seen_bell = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    println!("projector frame read: bell rang={seen_bell} (fold >= 30)");

    // ---- None semantics, demonstrated, not asserted ---------------------
    // 1. An unknown path has no live state entry.
    let ghost = system.try_with_es_state::<Counter, _>(&ActorPath::new("ghost"), |c| c.total);
    println!("unknown path       : {ghost:?} (no live entry)");

    // 2. The state exists but is NOT the requested type — a miss, never a
    //    panic (foreign actors' JSON state misses the same way).
    let wrong = system.try_with_es_state::<Other, _>(&counter, |o| o.n);
    println!("wrong type         : {wrong:?} (Other is not Counter)");

    println!("try_with example complete");
}
