//! Typed ZERO-COPY reads, awaiting pair: `with_es_state` and
//! `with_projector_state`.
//!
//! The `with_` reads run a sync closure over the LIVE state under its
//! lock — no serialize, no clone, no decode — awaiting the lock instead
//! of missing it (the non-blocking twins are demoed in
//! `examples/try_with.rs`).
//!
//! Wake semantics are the point of this demo: `with_projector_state` is
//! the COMPLETE read, exactly like its JSON twin `projector_state` — a
//! cold, set-owned projector is woken, catch-up is awaited (bounded by
//! the `CaughtUp` tap fact), and the fold is read typed. `with_es_state`
//! is the PEEK: a cold or passivated entity reads `None` — entities have
//! no wake path.
//!
//! Run: `cargo run --example with`

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::prelude::*;

// ---- A journaled counter (the entity whose state we read) -------------

#[derive(Debug, Deserialize, Serialize)]
struct Add {
    n: i64,
}
impl Schema for Add {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Add".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: Some("Add to the counter.".into()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Added {
    key: String,
    n: i64,
}
impl Schema for Added {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Added".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("key", FieldTy::Str).as_shard_key(),
                FieldDef::required("n", FieldTy::Int),
            ],
            description: None,
        }
    }
}

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
        if event.schema.as_str() == "Added@1" {
            self.total += event.payload["n"].as_i64().unwrap_or(0);
        }
    }
}
impl CommandHandler<Add> for Counter {
    fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> Events {
        Events::one(Added {
            key: "k".to_owned(),
            n: cmd.n,
        })
    }
}

// ---- The read model: one projector per counter key ---------------------

/// The per-key read model: how many adds it saw.
#[derive(Default, Debug, Serialize, Deserialize)]
struct Seen {
    count: u64,
}
impl Projector for Seen {
    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "Added@1" {
            self.count += 1;
        }
    }
}

#[tokio::main]
async fn main() {
    let (system, clock) = ActorSystem::test();
    system.register_schema::<Add>();
    system.register_schema::<Added>();

    // ---- A standalone entity: with_es_state reads it live --------------
    let counter = trouper::builder::spawn_es_builder::<Counter>(&system)
        .at(ActorPath::new("counter"))
        .args(json!({}))
        .handles::<Add>()
        .emits::<Added>()
        .start();
    system
        .send(system.envelope(Add::schema_id(), counter.clone(), json!({ "n": 41 })))
        .await
        .expect("delivered");
    while system
        .with_es_state::<Counter, _>(&counter, |c| c.total)
        .await
        != Some(41)
    {
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    println!("with_es_state  : total=Some(41) read typed, live");

    // ---- The peek rule: a COLD entity reads None (no wake path) --------
    // Stop the entity (passivation's teardown does the same): the state
    // entry leaves memory, the journal stays durable.
    system.stop(&counter).await;
    let cold = system
        .with_es_state::<Counter, _>(&counter, |c| c.total)
        .await;
    println!("with_es_state  : after stop, cold entity = {cold:?} (peek, never wakes)");

    // ---- A projector SET over the counter's facts ----------------------
    // Per-key read models under `seen`: `seen/<key>`, keyed by the
    // `Added` fact's shard-key field (chat_log demos the multi-key story;
    // this demo pins one key).
    let spec = ProjectorSetSpec {
        public: ActorPath::new("seen"),
        system: system.clone(),
        factory: Arc::new(|system, path, args| {
            spawn_projector_builder::<Seen>(system)
                .at(path.clone())
                .args(args.clone())
                .consumes::<Added>()
                .start();
        }),
        key_field: "key".to_owned(),
        args_template: None,
        opts: SpawnOpts {
            passivation: Some(trouper::system::Passivation {
                idle_for: Duration::from_millis(200),
            }),
            ..SpawnOpts::default()
        },
        consumed: vec![Added::schema_id()],
    };
    system.install_projector_set(spec).expect("install");

    // The counter's command already recorded one `Added` fact for key
    // "k". The broadcast copy ACTIVATES the projector `seen/k` (a
    // declared consumption is a delivery obligation) and the wake + fold
    // happens there.
    let warm = system
        .projector_state(&ActorPath::new("seen/k"))
        .await
        .expect("activated by the broadcast");
    println!("projector_state: warm JSON fold: {warm:?}");

    // ...then passivate it, and read it COLD through the typed twin: the
    // wake + bounded CaughtUp wait runs exactly as for the JSON read, but
    // the answer comes back as `Seen` — no serialize, no decode.
    clock.advance(Duration::from_millis(500));
    tokio::time::sleep(Duration::from_millis(200)).await;
    let cold_fold = system
        .with_projector_state::<Seen, _>(&ActorPath::new("seen/k"), |s| s.count)
        .await
        .expect("cold set-owned projector woken and read typed");
    println!("with_projector : woken cold projector, fold count = {cold_fold} (typed, zero-copy)");

    // And the wake budget miss: an ACTIVATED but never-fed projector — a
    // path the set owns (so the factory wakes it) whose consumed schema
    // has NO stored facts — still reaches CaughtUp (an empty history is a
    // complete fold), so the honest `None` case here is a key that maps
    // outside every set. Demonstrated with a path no set owns.
    let nowhere = system
        .with_projector_state::<Seen, _>(&ActorPath::new("nowhere/k"), |s| s.count)
        .await;
    println!("with_projector : unknown path = {nowhere:?} (no set owns it)");

    println!("with example complete");
}
