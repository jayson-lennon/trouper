//! The minimal useful actor: a file-saver.
//!
//! The whole actor is ~15 lines (`FileSaver` below): a service actor with
//! a `start` and ONE typed message handler. No `manifest()` override (the
//! builder supplies the surface), no registration calls (the builder
//! registers every declared schema), no hand-serialized payloads (the
//! typed `system.tell`/`system.ask` do serde).
//!
//! Demonstrates both flows:
//! - `tell` — fire-and-forget (the handler's `ctx.reply` is silently
//!   dropped: nobody asked);
//! - `ask` — save & confirm (the lease-backed reply comes back).
//!
//! A state-report bridge is installed, so a second shell can reflect the
//! system:
//!
//! ```text
//! shell 1: cargo run --example file_save
//! shell 2: cargo run -p canvas
//! ```

use actor_runtime::actor::{MsgHandler, ServiceActor};
use actor_runtime::prelude::*;
use actor_runtime::registry::RegistryError;
use actor_runtime::state_report::{ReportState, StateReported, StateReporter};
use actor_runtime::system::ActorSystem;
use error_stack::Report;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tracing::Level;

// ---------- the minimal actor ---------------------------------------------

/// The command: save these contents to this path.
#[derive(Serialize, Deserialize)]
struct SaveFile {
    path: String,
    contents: String,
}

impl Schema for SaveFile {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "SaveFile".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![
                FieldDef::required("path", FieldTy::Str),
                FieldDef::required("contents", FieldTy::Str),
            ],
            description: None,
        }
    }
}

/// The ack: the save attempt's outcome.
#[derive(Serialize, Deserialize)]
struct SaveAck {
    ok: bool,
    bytes: i64,
}

impl Schema for SaveAck {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "SaveAck".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("ok", FieldTy::Bool),
                FieldDef::required("bytes", FieldTy::Int),
            ],
            description: None,
        }
    }
}

/// THE minimal actor: ~15 lines. The default `manifest()` and the
/// builder's auto-registration mean `start` + one handler is the entire
/// contract; the write is I/O, so this is the SERVICE tier (an ES actor's
/// `handle` is sync on `&self` — no I/O is physically possible there).
struct FileSaver;

impl ServiceActor for FileSaver {
    async fn start(_args: &serde_json::Value) -> Result<Self, Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<SaveFile> for FileSaver {
    async fn handle(&mut self, msg: SaveFile, ctx: &mut MsgCtx<'_>) {
        let bytes = tokio::fs::write(&msg.path, &msg.contents)
            .await
            .map(|_| msg.contents.len());
        let (ok, n) = (bytes.is_ok(), bytes.unwrap_or(0) as i64);
        // A reply without an ask is dropped silently (no reply_to) — a
        // tell's handler may reply harmlessly.
        ctx.core
            .reply(SaveAck::schema_id(), json!({ "ok": ok, "bytes": n }));
    }
}

// ---------- the demo -------------------------------------------------------

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

/// Runs the demo: tell flow, ask flow, error path, then serves state
/// queries until ctrl-c.
///
/// # Errors
///
/// Propagates bridge installation failures (zenoh session/queryable).
async fn run_demo() -> Result<(), state_report::StateBridgeError> {
    let system = Arc::new(ActorSystem::new(SystemConfig::production()));

    // Spawn: the builder declares the whole surface (and registers the
    // schemas — no caller-side register_schema anywhere in this file).
    let saver = actor_runtime::builder::spawn_service_builder::<FileSaver>(&system)
        .at(ActorPath::new("fs.saver"))
        .args(json!({}))
        .handles::<SaveFile>()
        .emits::<SaveAck>()
        .start();
    wait(|| async { system.inbox_cursor(&saver).is_some() }).await;

    // The state reporter the bridge serves every query through.
    actor_runtime::builder::spawn_es_builder::<StateReporter>(&system)
        .at(ActorPath::new("state/reporter"))
        .args(json!({}))
        .handles::<ReportState>()
        .emits::<StateReported>()
        .start();
    wait(|| async {
        system
            .inbox_cursor(&ActorPath::new("state/reporter"))
            .is_some()
    })
    .await;

    // --- tell: fire-and-forget, confirmed only by looking at the disk ---
    let tell_path = std::env::temp_dir().join("actor-canvas-file-save-tell.txt");
    system
        .tell(
            saver.clone(),
            SaveFile {
                path: tell_path.display().to_string(),
                contents: "saved via tell (fire-and-forget)".to_owned(),
            },
        )
        .await
        .expect("told");
    wait(|| async { tokio::fs::metadata(&tell_path).await.is_ok() }).await;
    println!("tell: wrote {}", tell_path.display());

    // --- ask: save & confirm, the reply rides the lease back ---
    let ask_path = std::env::temp_dir().join("actor-canvas-file-save-ask.txt");
    let ack = system
        .ask(
            saver.clone(),
            SaveFile {
                path: ask_path.display().to_string(),
                contents: "saved via ask (save & confirm)".to_owned(),
            },
            Duration::from_secs(5),
        )
        .await
        .expect("acked");
    assert!(
        ack["ok"] == true,
        "the ask ack must confirm the save: {ack}"
    );
    println!("ask: ack {ack}");

    // --- error path: an unresolvable destination fails fast ---
    let ghost = system
        .ask(
            ActorPath::new("fs.ghost"),
            SaveFile {
                path: String::new(),
                contents: String::new(),
            },
            Duration::from_millis(500),
        )
        .await;
    match ghost {
        Err(e) => println!("ask to fs.ghost failed fast (as it should): {e}"),
        Ok(_) => panic!("an ask to a missing actor must not succeed"),
    }

    // --- what the export will show (auto-registration proof) ---
    let export = system.export().await;
    let actor = export
        .actors
        .iter()
        .find(|a| a.path == saver)
        .expect("fs.saver exported");
    let schemas = actor
        .manifest
        .handles
        .iter()
        .chain(actor.manifest.emits.iter())
        .map(|id| id.as_str().to_owned())
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "export: fs.saver ({:?}) knows [{schemas}]; {} schema defs registered",
        actor.kind,
        export.schemas.len()
    );

    // Keep the bridge session alive — dropping it closes the queryable.
    let _session = state_report::install(system.clone(), ActorPath::new("state/reporter")).await?;
    println!(
        "serving state on the actor-runtime/state key — run `cargo run -p canvas` (or the GUI) now (ctrl-c to stop)"
    );
    tokio::signal::ctrl_c()
        .await
        .expect("ctrl-c handler installs");
    println!("bye");
    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    if let Err(e) = run_demo().await {
        eprintln!("file_save: {e}");
        std::process::exit(1);
    }
}
