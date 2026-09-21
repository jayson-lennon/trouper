//! Projector SETS: per-key read models that passivate when idle and
//! WAKE on the next broadcast of a consumed fact.
//!
//! One projector per chat (`chats/<chat_id>`), each folding only its
//! key's `Chatted` facts — recorded by the `ChatRoom` ENTITY (the
//! journal is the source of truth; the projectors are views over it).
//! A declared consumption with a shard key is a delivery obligation:
//! every broadcast copy of `Chatted` resolves `chats/<chat_id>` and
//! activates the owning projector on demand — exactly like a told
//! command to a partition set. Passivation lives in the set's `opts`
//! (a standalone projector has no wake path, so its builder has no
//! `passivate_after`).
//!
//! Also demonstrated: `rebuild_projector` (stop → purge → re-activate
//! → catch-up), which equals a from-scratch fold.
//!
//! Run: `cargo run --example chat_log`

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::prelude::*;

// ---- The source of truth: a journaled chat room --------------------

/// The room's command: say something.
#[derive(Command, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "Say something in one chat room.")]
struct Say {
    #[schema(shard_key)]
    chat_id: String,
    text: String,
}

/// The decision fact — what the projector set consumes.
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "One chat message (a fact, not a command).")]
struct Chatted {
    #[schema(shard_key)]
    chat_id: String,
    text: String,
}

/// One chat-room entity: journals every message as a `Chatted` fact.
#[derive(Default, Debug, Serialize, Deserialize)]
struct ChatRoom {
    messages: u64,
}
impl CommandHandler<Say> for ChatRoom {
    fn handle(&self, cmd: Say, _ctx: &mut CmdCtx<'_>) -> Events {
        Events::one(Chatted {
            chat_id: cmd.chat_id,
            text: cmd.text,
        })
    }
}
impl EventSourcedActor for ChatRoom {
    fn restore(_args: &Json) -> Self {
        Self::default()
    }
    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "Chatted@1" {
            self.messages += 1;
        }
    }
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles_id(Say::schema_id())
            .emits_id(Chatted::schema_id())
    }
}

// ---- The read model: one projector per chat ------------------------

/// The per-chat read model: message count + the transcript.
#[derive(Default, Debug, Serialize, Deserialize)]
struct ChatLog {
    messages: u64,
    transcript: Vec<String>,
}
impl Projector for ChatLog {
    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "Chatted@1" {
            self.messages += 1;
            if let Some(text) = event.payload["text"].as_str() {
                self.transcript.push(text.to_owned());
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let (system, clock) = ActorSystem::test();
    system.register_schema::<Say>();
    system.register_schema::<Chatted>();

    // The source: per-key chat-room ENTITIES under `rooms` (commands).
    let rooms = trouper::pool::PartitionSpec {
        public: ActorPath::new("rooms"),
        system: system.clone(),
        factory: Arc::new(|system, path, args| {
            spawn_es_builder::<ChatRoom>(system)
                .at(path.clone())
                .args(args.clone())
                .handles::<Say>()
                .emits::<Chatted>()
                .start();
        }),
        key_field: "chat_id".to_owned(),
        args_template: None,
        opts: SpawnOpts::default(),
    };
    system.install_partition_set(rooms).expect("install");

    // ---- Write through the entity; the fact fans out to the view ---
    async fn say(system: &ActorSystem, chat_id: &str, text: &str) {
        system
            .send(system.envelope(
                Say::schema_id(),
                ActorPath::new("rooms"),
                json!({ "chat_id": chat_id, "text": text }),
            ))
            .await
            .expect("command routed");
    }
    say(&system, "rust", "goodbye borrow checker").await;
    say(&system, "rust", "hello traits").await;
    say(&system, "k8s", "pod restarting").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The views: per-key `chats/<chat_id>` projectors under `chats`
    // (facts), activated on demand, evicted after 200ms idle.
    let spec = ProjectorSetSpec {
        public: ActorPath::new("chats"),
        system: system.clone(),
        factory: Arc::new(|system, path, args| {
            // Fire-and-forget: the arm is synchronous; catch-up (and its
            // CaughtUp fact) continues in the background.
            spawn_projector_builder::<ChatLog>(system)
                .at(path.clone())
                .args(args.clone())
                .consumes::<Chatted>()
                .start();
        }),
        key_field: "chat_id".to_owned(),
        args_template: None,
        opts: SpawnOpts {
            passivation: Some(trouper::system::Passivation {
                idle_for: Duration::from_millis(200),
            }),
            ..SpawnOpts::default()
        },
        consumed: vec![Chatted::schema_id()],
    };
    // Projector installed after the `say` calls to demonstrate how the projections will "catch up"
    // to previously emitted events.
    system.install_projector_set(spec).expect("install");

    let rust = system
        .projector_state(&ActorPath::new("chats/rust"))
        .await
        .expect("rust");
    println!("chats/rust   : {rust:?}");
    let k8s = system
        .projector_state(&ActorPath::new("chats/k8s"))
        .await
        .expect("k8s");
    println!("chats/k8s    : {k8s:?}");
    // Each projector folded ONLY its key's facts.
    println!(
        "chats/other  : {:?} (never activated — no key \"other\" fact)",
        system
            .es_state(&ActorPath::new("chats/other"))
            .await
            .is_some()
    );

    // ---- Idle passivation frees the fold; the broadcast wakes it ---
    clock.advance(Duration::from_millis(500));
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!(
        "chats/rust after idle: live={} (fold evicted, journal durable)",
        system
            .es_state(&ActorPath::new("chats/rust"))
            .await
            .is_some()
    );

    say(&system, "rust", "back from the dead").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let rust = system
        .projector_state(&ActorPath::new("chats/rust"))
        .await
        .expect("woken");
    println!("chats/rust woken + gap-filled: {rust:?}");

    // ---- Rebuild = stop + purge + re-activate + catch-up -----------
    // The re-fold equals a from-scratch fold (same scan, same apply).
    system
        .rebuild_projector(&ActorPath::new("chats/rust"))
        .await
        .expect("rebuild");
    let rust = system
        .projector_state(&ActorPath::new("chats/rust"))
        .await
        .expect("rebuilt");
    println!("chats/rust rebuilt (fresh fold): {rust:?}");
}
