//! The GUI workflow: a NON-ASYNC main thread driving a chat window off
//! projector reads — the "click → spinner → content" protocol end to end.
//!
//! The main thread here is a plain synchronous event loop (the shape of
//! every real UI toolkit: no runtime, no futures, no `.await`). It talks
//! to the actor system two ways, and the split is the whole point:
//!
//! - **Peek** (`try_with_projector_state`): sync, never blocks, never
//!   wakes anything. The render loop calls it EVERY frame; `None` means
//!   "not ready right now" and the UI keeps drawing whatever it has
//!   (the spinner). This is the only actor-system call on the main thread.
//! - **Wake** (`with_projector_state`): async; wakes a cold set-owned
//!   projector, waits bounded for its catch-up, reads the complete fold.
//!   This NEVER runs on the main thread — a background runtime thread
//!   runs it per "open chat" click and posts the outcome back as a UI
//!   event, exactly like any background fetch in a GUI.
//!
//! The protocol, concretely:
//! 1. click `OpenChat` → main posts a command to the background thread
//!    (it does NOT peek-and-hope: a cold projector stays cold under a
//!    peek forever — peeks have no wake path).
//! 2. main flips the view to `Loading`; every frame it draws the spinner.
//! 3. the background thread ensures the room exists, seeds history for a
//!    fresh room, then awaits `with_projector_state` — the projector
//!    activates, gap-fills from the journal, and the read returns only
//!    once the fold is COMPLETE (a `CaughtUp`-gated read, so the first
//!    draw can never be a partial chat).
//! 4. `ChatReady` lands in the UI event queue → next frame draws content.
//!    From then on the render loop peeks per frame: new `Say`s sent from
//!    the GUI appear within a frame or two, without any completion
//!    tracking (fire-and-forget into the system; the projector folds
//!    whenever the fact arrives).
//! 5. a failed wake (unknown path / missed catch-up budget) posts
//!    `ChatFailed` → an error state, never an infinite spinner.
//!
//! The set's passivation window is 400ms, so this demo's chat IS the
//! passivate/reactivate cycle in miniature: let it idle past the window
//! and the next open re-wakes from the journal (same spinner, complete
//! fold again).
//!
//! Run: `cargo run --example gui_chat`

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use trouper::actor::{ActorKind, ActorPath, CommandHandler, EventSourcedActor, Projector};
use trouper::context::CmdCtx;
use trouper::json::Json;
use trouper::schema::{ActorManifest, Command, Event};
use trouper::system::SpawnOpts;
use trouper::{builder, prelude::*};

// ---- The domain: rooms record chats; projectors render them ----------

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

/// The journaled room (the source of truth).
#[derive(Default, Debug, Serialize, Deserialize)]
struct ChatRoom;
impl EventSourcedActor for ChatRoom {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<Say>()
            .emits::<Chatted>()
            .kind(ActorKind::EventSourced)
    }
    fn restore(_args: &Json) -> Self {
        Self
    }
    fn apply(&mut self, _event: &Event) {}
}
impl CommandHandler<Say> for ChatRoom {
    fn handle(&self, cmd: Say, _ctx: &mut CmdCtx<'_>) -> trouper::envelope::Events {
        trouper::envelope::Events::one(Chatted {
            chat_id: cmd.chat_id,
            text: cmd.text,
        })
    }
}

/// The read model the GUI draws: one projector per chat under `chats/`.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct ChatLog {
    messages: Vec<String>,
}
impl Projector for ChatLog {
    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "Chatted" {
            let text = event.payload_json()["text"]
                .as_str()
                .unwrap_or("(undecodable)")
                .to_owned();
            self.messages.push(text);
        }
    }
}

// ---- The UI: plain synchronous types, no runtime anywhere ------------

/// What the user does.
#[derive(Debug)]
enum UiEvent {
    /// Click "load chat".
    OpenChat(String),
    /// Type + hit send.
    SendChat { chat_id: String, text: String },
}

/// What the background thread posts back.
#[derive(Debug)]
enum BgEvent {
    /// The wake read completed: the fold is complete, draw it.
    ChatReady {
        chat_id: String,
        messages: Vec<String>,
    },
    /// The wake failed (no set owns the path / catch-up budget missed).
    ChatFailed { chat_id: String },
}

/// What one chat's panel shows.
#[derive(Debug)]
enum Panel {
    /// The spinner (drawn while `BgEvent::ChatReady` is in flight).
    Loading,
    /// Content + the live fold, refreshed by peeks every frame.
    Loaded { messages: Vec<String> },
    /// The wake failed; the UI says so instead of spinning forever.
    Failed,
}

fn draw(panel: &Panel, frame: u64) {
    match panel {
        Panel::Loading => {
            let spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'][frame as usize % 10];
            println!("   {spinner} loading chat…");
        }
        Panel::Loaded { messages } => {
            println!("   ┌─ chat ─────────────────────────");
            for m in messages {
                println!("   │ {m}");
            }
            println!("   └──────────── {} message(s)", messages.len());
        }
        Panel::Failed => println!("   ✗ chat could not be loaded"),
    }
}

fn main() {
    // The actor system lives on a background thread with its own runtime;
    // the main thread gets only sync handles: a CLONE of the system (for
    // peeks — ActorSystem is an Arc bundle, Clone + Send + Sync) and two
    // channels (for commands + completion events).
    let (system, ui_tx, bg_rx) = background_actor_thread_start();

    // The user's view state lives ON the main thread. `open_chat` is
    // Option<String> (which chat is open, if any); `panel` is the view.
    let mut panel: Option<Panel> = None;
    let mut open_chat: Option<String> = None;
    let mut frame: u64 = 0;

    // A scripted user: click, wait out the load, send two messages, idle
    // past passivation, click again (the re-wake path). In a real app
    // these arrive from the window system; the loop shape is identical.
    let script: Vec<(u64, UiEvent)> = vec![
        (1, UiEvent::OpenChat("general".into())),
        (
            12,
            UiEvent::SendChat {
                chat_id: "general".into(),
                text: "hello from the GUI thread".into(),
            },
        ),
        (
            16,
            UiEvent::SendChat {
                chat_id: "general".into(),
                text: "peeks pick this up — no completion tracking".into(),
            },
        ),
        (40, UiEvent::OpenChat("general".into())),
    ];

    // THE MAIN LOOP: synchronous, ~30fps. Per frame: read the script,
    // drain background completions, then draw by PEEKING the projector.
    for (at_frame, event) in script {
        while frame < at_frame {
            tick(&system, &bg_rx, &mut panel, &mut open_chat, frame);
            frame += 1;
            std::thread::sleep(Duration::from_millis(33));
        }
        // Optimistic view update on the way out (the GUI's own state):
        // a click flips the panel to the spinner; sends need nothing.
        if let UiEvent::OpenChat(chat_id) = &event
            && open_chat.as_deref() != Some(chat_id)
        {
            open_chat.replace(chat_id.clone());
            panel = Some(Panel::Loading);
        }
        ui_tx.send(event).expect("background thread alive");
    }
    // A few trailing frames so the last state is visible (the wake runs
    // on the background thread — the UI waits for its event, it does not
    // poll for it).
    for _ in 0..30 {
        tick(&system, &bg_rx, &mut panel, &mut open_chat, frame);
        frame += 1;
        std::thread::sleep(Duration::from_millis(33));
    }
    drop(ui_tx);
}

/// One frame: apply scripted input, absorb background completions, draw.
///
/// `draw` never awaits and never wakes: `try_with_projector_state` is a
/// pure peek (`None` = keep what we have), and state transitions come
/// only from posted `BgEvent`s — the GUI-thread contract.
fn tick(
    system: &ActorSystem,
    bg_rx: &mpsc::Receiver<BgEvent>,
    panel: &mut Option<Panel>,
    open_chat: &mut Option<String>,
    frame: u64,
) {
    // Drain everything the background thread posted since last frame.
    while let Ok(event) = bg_rx.try_recv() {
        match event {
            BgEvent::ChatReady { chat_id, messages } => {
                println!("◆ [{chat_id}] ready — complete fold, frame {frame}");
                *panel = Some(Panel::Loaded { messages });
            }
            BgEvent::ChatFailed { chat_id } => {
                println!("◆ [{chat_id}] wake failed");
                *panel = Some(Panel::Failed);
            }
        }
    }
    print!("frame {frame:3}: ");
    // The peek: the ONLY actor-system call on this thread. Lock busy /
    // still cold ⇒ None ⇒ redraw the current panel (spinner or content).
    if let (Some(chat_id), Some(Panel::Loaded { messages })) =
        (open_chat.as_deref(), panel.as_mut())
        && let Some(live) = system.try_with_projector_state::<ChatLog, _>(
            &ActorPath::new(format!("chats/{chat_id}")),
            |log| log.messages.clone(),
        )
    {
        *messages = live; // the fold moved: new messages show up here
    }
    // Draw: closed = nothing open; otherwise the panel's current state.
    match panel {
        Some(p) => draw(p, frame),
        None => println!("   [ no chat open ]"),
    }
}

/// Boots the background thread (runtime + system) and returns the main
/// thread's sync handles: an `ActorSystem` clone for peeks, the command
/// sender, and the completion receiver.
fn background_actor_thread_start() -> (ActorSystem, mpsc::Sender<UiEvent>, mpsc::Receiver<BgEvent>)
{
    let (ui_tx, ui_rx) = mpsc::channel::<UiEvent>();
    let (bg_tx, bg_rx) = mpsc::channel::<BgEvent>();
    // ONE construction runtime, dropped immediately: it only exists to
    // build the system handle. The BACKGROUND thread's runtime owns
    // everything the system runs from here on.
    let system = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("construction rt")
        .block_on(async { ActorSystem::new(SystemConfig::production()) });
    let peek_clone = system.clone();
    std::thread::spawn(move || background_actor_thread(system, ui_rx, bg_tx));
    (peek_clone, ui_tx, bg_rx)
}

/// The background thread: owns the runtime, the system, and every await.
///
/// Commands from the UI are handled one at a time; the wake
/// (`with_projector_state`) runs HERE — it activates the cold projector,
/// waits bounded for catch-up, and only then posts the complete fold.
fn background_actor_thread(
    system: ActorSystem,
    ui_rx: mpsc::Receiver<UiEvent>,
    bg_tx: mpsc::Sender<BgEvent>,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("bg runtime");
    rt.block_on(async move {
        system.register_schema::<Say>();
        system.register_schema::<Chatted>();

        // The projector SET: per-key read models `chats/<chat_id>` with a
        // short passivation window — the re-wake cycle is part of the demo.
        let set = ProjectorSetSpec {
            public: ActorPath::new("chats"),
            system: system.clone(),
            factory: Arc::new(|system, path, args| {
                builder::spawn_projector_builder::<ChatLog>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .consumes::<Chatted>()
                    .start();
            }),
            key_field: "chat_id".to_owned(),
            args_template: None,
            opts: SpawnOpts {
                passivation: Some(trouper::system::Passivation {
                    idle_for: Duration::from_millis(400),
                }),
                ..SpawnOpts::default()
            },
            consumed: vec![Chatted::schema_id()],
        };
        system.install_projector_set(set).expect("install set");

        let mut spawned_rooms: Vec<String> = Vec::new();
        // A std mpsc receiver cannot be awaited: bridge it with a blocking
        // thread pool task so the runtime stays free for the system's work.
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<UiEvent>(64);
        std::thread::spawn(move || {
            while let Ok(cmd) = ui_rx.recv() {
                if cmd_tx.blocking_send(cmd).is_err() {
                    break;
                }
            }
        });

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                UiEvent::OpenChat(chat_id) => {
                    // First open of a room spawns the journaled entity and
                    // seeds a little history (so "load" has something to
                    // load). Reopen after passivation skips this — the
                    // journal is the source of truth.
                    if !spawned_rooms.contains(&chat_id) {
                        spawned_rooms.push(chat_id.clone());
                        builder::spawn_es_builder::<ChatRoom>(&system)
                            .at(ActorPath::new(format!("rooms/{chat_id}")))
                            .args(json!({}))
                            .handles::<Say>()
                            .emits::<Chatted>()
                            .start();
                        for text in ["seeding: welcome to #general", "seeding: older message"] {
                            system
                                .send(system.envelope(
                                    Say::schema_id(),
                                    ActorPath::new(format!("rooms/{chat_id}")),
                                    json!({ "chat_id": chat_id, "text": text }),
                                ))
                                .await
                                .expect("seed delivered");
                        }
                    }
                    // Let the seed broadcasts land first: each copy
                    // resolves `chats/<key>` and ACTIVATES the projector
                    // (a declared consumption is a delivery obligation).
                    // Activating via the fact flow (not a parallel wake)
                    // keeps exactly one factory run per cold start.
                    let room = ActorPath::new(format!("rooms/{chat_id}"));
                    for _ in 0..5_000 {
                        if system
                            .with_es_state::<ChatRoom, _>(&room, |_: &ChatRoom| ())
                            .await
                            .is_some()
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    // THE WAKE — off the UI thread by construction. This
                    // read is CaughtUp-gated: it returns the COMPLETE fold
                    // or None (set does not own the path / budget missed).
                    let path = ActorPath::new(format!("chats/{chat_id}"));
                    let fold = system
                        .with_projector_state::<ChatLog, _>(&path, |log| log.messages.clone())
                        .await;
                    match fold {
                        Some(messages) => {
                            bg_tx.send(BgEvent::ChatReady { chat_id, messages }).ok();
                        }
                        None => {
                            bg_tx.send(BgEvent::ChatFailed { chat_id }).ok();
                        }
                    }
                }
                UiEvent::SendChat { chat_id, text } => {
                    // Fire-and-forget into the system: the entity folds,
                    // records `Chatted`, the broadcast copy wakes/folds the
                    // projector, and the GUI's per-frame peeks pick it up.
                    system
                        .send(system.envelope(
                            Say::schema_id(),
                            ActorPath::new(format!("rooms/{chat_id}")),
                            json!({ "chat_id": chat_id, "text": text }),
                        ))
                        .await
                        .expect("send delivered");
                }
            }
        }
    });
}
