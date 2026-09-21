//! The minimal useful actor: a file-saver.
//!
//! The runtime-facing surface is a `start` and ONE typed handler (no
//! `manifest()` override — the builder supplies it; no registration calls —
//! the builder registers every declared schema; no hand-serialized payloads
//! — the typed `system.tell`/`system.ask` do serde). The DOMAIN logic is a
//! plain `save` method over an injected `FileStore`, testable with `MemFs`
//! and no runtime at all.
//!
//! Demonstrates the full outcome grammar:
//! - `tell` — fire-and-forget (the handler's `ctx.reply` is silently
//!   dropped: nobody asked);
//! - `ask` — save & confirm (the lease-backed reply comes back);
//! - failed ask — a `SaveFailed` REPLY names the reason (point-to-point);
//! - failed tell — the same failure PUBLISHED as an event, observed by
//!   the audit subscriber that declared `.handles::<SaveFailed>()` (a
//!   tell has no asker: the reply channel is gone, the fact channel is
//!   not).
//!
//! Run it: `cargo run --example file_save`

use error_stack::Report;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tracing::Level;
use trouper::actor::{MsgHandler, ServiceActor};
use trouper::prelude::*;
use trouper::registry::RegistryError;
use trouper::system::ActorSystem;

// ---------- the domain seam -------------------------------------------------

type FileWriteFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<usize>> + Send + 'a>>;

/// The store seam: the domain logic depends on THIS, not on the real
/// filesystem — so it tests with [`MemFs`] and zero runtime (the same DI
/// taste as the runtime's `RuntimeView`/`AskPort` seams).
trait FileStore: Send + Sync + 'static {
    /// Writes `contents` to `path`; returns the byte count written.
    fn write<'a>(&'a self, path: &'a std::path::Path, contents: &'a [u8]) -> FileWriteFuture<'a>;
}

/// Production store: the real async filesystem.
#[derive(Default)]
struct RealFs;

impl FileStore for RealFs {
    fn write<'a>(&'a self, path: &'a std::path::Path, contents: &'a [u8]) -> FileWriteFuture<'a> {
        Box::pin(async move {
            tokio::fs::write(path, contents)
                .await
                .map(|_| contents.len())
        })
    }
}

// ---------- the minimal actor ---------------------------------------------

/// The command: save these contents to this path.
///
/// Rich domain types on both fields. `PathBuf` serializes as a JSON
/// string; `Vec<u8>` has no native JSON form, so serde picks its array-of-
/// numbers representation — which is what [`FieldTy::Json`] exists for
/// ("arbitrary JSON; the escape hatch for payloads a tooling consumer need
/// not inspect deeply").
#[derive(Command, Serialize, Deserialize)]
struct SaveFile {
    path: std::path::PathBuf,
    contents: Vec<u8>,
}

/// The ack: how many bytes were written. A save that did NOT succeed
/// never produces this — it produces [`SaveFailed`] instead — so there is
/// no `ok` flag to lie: the schema of the reply IS the outcome.
#[derive(Event, Serialize, Deserialize, Debug)]
struct SaveAck {
    bytes: i64,
}

/// A failed save, as the domain names it: the `io::ErrorKind` reason.
/// Not a schema — the adapter renders it into [`SaveFailed`].
#[derive(Debug)]
struct SaveError {
    kind: std::io::ErrorKind,
}

/// The failure fact: published (and replied to an asker) whenever a
/// save is declined. This is the recorded decision —
/// domain outcomes (including failures) are facts, not ok-flags.
#[derive(Event, Serialize, Deserialize, Clone)]
struct SaveFailed {
    path: String,
    reason: String,
}

/// THE minimal actor. The domain logic is `save` — a plain method on a
/// plain struct, testable with `MemFs` and no runtime. The `MsgHandler`
/// impl is just the integration shim between that method and the runtime:
/// success replies the asker; failure replies AND publishes the fact (a
/// failed tell is otherwise invisible — nobody asked, so no reply goes
/// anywhere).
struct FileSaver<S> {
    store: S,
}

impl<S: FileStore> FileSaver<S> {
    /// The domain operation: write these contents to this path, answer
    /// in bytes. Takes refs — the caller keeps its command.
    async fn save(
        &mut self,
        path: &std::path::Path,
        contents: &[u8],
    ) -> Result<SaveAck, SaveError> {
        let bytes = self
            .store
            .write(path, contents)
            .await
            .map_err(|e| SaveError { kind: e.kind() })?;
        Ok(SaveAck {
            bytes: bytes as i64,
        })
    }
}

impl<S: FileStore + Default> ServiceActor for FileSaver<S> {
    async fn start(_args: &Json) -> Result<Self, Report<RegistryError>> {
        Ok(Self {
            store: S::default(),
        })
    }
}

impl<S: FileStore + Default> MsgHandler<SaveFile> for FileSaver<S> {
    async fn handle(&mut self, msg: SaveFile, ctx: &mut MsgCtx<'_>) {
        match self.save(&msg.path, &msg.contents).await {
            Ok(ack) => {
                // Point-to-point: to the asker when asked, silently
                // dropped on a tell.
                ctx.reply(ack);
            }
            Err(e) => {
                let fact = SaveFailed {
                    path: msg.path.display().to_string(),
                    reason: format!("{:?}", e.kind),
                };
                ctx.reply(fact.clone());
                // The fact channel: observers (and tellers' audits) see it.
                ctx.publish(&fact);
            }
        }
    }
}

// ---------- the demo -------------------------------------------------------

/// The audit sink: every published `SaveFailed` fact lands here. A static
/// because the fact travels through the runtime to another ACTOR — the
/// main flow only polls it (broadcast copies are at-least-once per
/// delivery; never assume ordering with the reply).
static AUDIT: parking_lot::Mutex<Vec<String>> = parking_lot::Mutex::new(Vec::new());

/// The audit subscriber: an ordinary service actor that handles
/// `SaveFailed` — the observer side of the pattern.
struct SaveAudit;

impl ServiceActor for SaveAudit {
    async fn start(_args: &Json) -> Result<Self, Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<SaveFailed> for SaveAudit {
    async fn handle(&mut self, fact: SaveFailed, _ctx: &mut MsgCtx<'_>) {
        AUDIT
            .lock()
            .push(format!("{} ({})", fact.path, fact.reason));
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
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    panic!("demo condition never became true");
}

/// Runs the demo: tell flow, ask flow, failure paths.
///
/// # Errors
///
/// Propagates runtime errors (spawn failures, ask rejections).
async fn run_demo() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::new(SystemConfig::production());

    // Spawn: the builder declares the whole surface (and registers the
    // schemas — no caller-side register_schema anywhere in this file).
    let saver = trouper::builder::spawn_service_builder::<FileSaver<RealFs>>(&system)
        .at(ActorPath::new("fs.saver"))
        .args(json!({}))
        .handles::<SaveFile>()
        .emits::<SaveAck>()
        .emits::<SaveFailed>()
        .start();
    wait(|| async { system.inbox_cursor(&saver).is_some() }).await;

    // The audit subscriber: sees every SaveFailed fact on fs.events.
    let audit = trouper::builder::spawn_service_builder::<SaveAudit>(&system)
        .at(ActorPath::new("fs.audit"))
        .args(json!({}))
        .handles::<SaveFailed>()
        .start();
    wait(|| async { system.inbox_cursor(&audit).is_some() }).await;

    // --- tell: fire-and-forget, confirmed only by looking at the disk ---
    let tell_path = std::env::temp_dir().join("trouper-sdk-file-save-tell.txt");
    system
        .tell(
            saver.clone(),
            SaveFile {
                path: tell_path.clone(),
                contents: b"saved via tell (fire-and-forget)".to_vec(),
            },
        )
        .await
        .expect("told");
    wait(|| async { tokio::fs::metadata(&tell_path).await.is_ok() }).await;
    println!("tell: wrote {}", tell_path.display());

    // --- ask: save & confirm, the reply rides the lease back ---
    let ask_path = std::env::temp_dir().join("trouper-sdk-file-save-ask.txt");
    let ack = system
        .ask(
            saver.clone(),
            SaveFile {
                path: ask_path.clone(),
                contents: b"saved via ask (save & confirm)".to_vec(),
            },
            Duration::from_secs(5),
        )
        .await
        .expect("acked");
    assert_eq!(
        ack["bytes"], 30,
        "the ask ack must report the written bytes: {ack:?}"
    );
    println!("ask: ack {ack:?}");

    // --- failure, asked: the reply names the reason (point-to-point) ---
    // A path under a directory that does not exist: the domain method
    // declines with NotFound, and the adapter replies it as SaveFailed.
    let fail_ask_path = std::env::temp_dir().join("no-such-dir-deny/fail-ask.txt");
    let failed = system
        .ask(
            saver.clone(),
            SaveFile {
                path: fail_ask_path,
                contents: b"will fail".to_vec(),
            },
            Duration::from_secs(5),
        )
        .await;
    match failed {
        Ok(reply) if reply["reason"] == "NotFound" => {
            println!("failed ask replied the reason: {reply:?}");
        }
        other => panic!("expected a SaveFailed reply, got {other:?}"),
    }

    // --- failure, told: NOBODY asked — only the audit sees it ---
    let fail_tell_path = std::env::temp_dir().join("no-such-dir-deny/fail-tell.txt");
    system
        .tell(
            saver.clone(),
            SaveFile {
                path: fail_tell_path,
                contents: b"will fail, invisibly".to_vec(),
            },
        )
        .await
        .expect("told");
    wait(|| async { !AUDIT.lock().is_empty() }).await;
    for fact in AUDIT.lock().iter() {
        println!("audit saw the failed tell: {fact}");
    }

    // --- error path: an unresolvable destination fails fast ---
    let ghost = system
        .ask(
            ActorPath::new("fs.ghost"),
            SaveFile {
                path: std::path::PathBuf::new(),
                contents: Vec::new(),
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

    println!("demo complete");
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Test store: an in-memory map with injectable write denials.
    ///
    /// Denials are process-global (`DENIED`): a spawned actor owns its own
    /// `MemFs` instance, so tests seed the global list to make THE ACTOR'S
    /// store deny a path.
    #[derive(Default)]
    struct MemFs {
        files: parking_lot::Mutex<std::collections::HashMap<std::path::PathBuf, Vec<u8>>>,
    }

    /// Paths every `MemFs` must refuse (test fixture).
    static DENIED: parking_lot::Mutex<Vec<std::path::PathBuf>> =
        parking_lot::Mutex::new(Vec::new());

    impl MemFs {
        fn new() -> Self {
            Self::default()
        }

        /// Marks `path` so writes to it fail with `PermissionDenied`.
        fn deny(path: &std::path::Path) {
            DENIED.lock().push(path.to_owned());
        }

        /// What a path's contents ended up as (test assertions).
        fn written(&self, path: &std::path::Path) -> Option<Vec<u8>> {
            self.files.lock().get(path).cloned()
        }
    }

    impl FileStore for MemFs {
        fn write<'a>(
            &'a self,
            path: &'a std::path::Path,
            contents: &'a [u8],
        ) -> FileWriteFuture<'a> {
            Box::pin(async move {
                if DENIED.lock().iter().any(|p| p == path) {
                    return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
                }
                self.files.lock().insert(path.to_owned(), contents.to_vec());
                Ok(contents.len())
            })
        }
    }

    /// The domain method under test — no runtime, no disk, deterministic.
    fn saver() -> FileSaver<MemFs> {
        FileSaver {
            store: MemFs::new(),
        }
    }

    #[tokio::test]
    async fn save_writes_contents_and_answers_in_bytes() {
        // Given a file saver over an in-memory store.
        let mut f = saver();
        let path = std::path::PathBuf::from("/virtual/doc.txt");

        // When saving.
        let ack = f.save(&path, b"hello facts").await.expect("saved");

        // Then the ack counts the bytes and the store holds the contents.
        assert_eq!(ack.bytes, 11);
        assert_eq!(
            f.store.written(&path).as_deref(),
            Some(b"hello facts".as_slice())
        );
    }

    #[tokio::test]
    async fn save_declines_a_denied_path_with_the_io_reason() {
        // Given a store that denies one path.
        let mut f = saver();
        let path = std::path::PathBuf::from("/virtual/locked.txt");
        MemFs::deny(&path);

        // When saving to the denied path.
        let err = f.save(&path, b"nope").await.expect_err("denied");

        // Then the domain error names the io reason.
        assert_eq!(err.kind, std::io::ErrorKind::PermissionDenied);
    }

    // ---- adapter tests: the shim over a REAL ActorSystem ----

    /// Spawns `FileSaver<MemFs>` + `SaveAudit` on a fresh test system; the
    /// audit handles the `SaveFailed` event.
    async fn demo_system() -> (ActorSystem, ActorPath, ActorPath) {
        let system = ActorSystem::new(SystemConfig::production());
        let saver = trouper::builder::spawn_service_builder::<FileSaver<MemFs>>(&system)
            .at(ActorPath::new("test.saver"))
            .args(json!({}))
            .handles::<SaveFile>()
            .emits::<SaveAck>()
            .emits::<SaveFailed>()
            .start();
        let audit = trouper::builder::spawn_service_builder::<SaveAudit>(&system)
            .at(ActorPath::new("test.audit"))
            .args(json!({}))
            .handles::<SaveFailed>()
            .start();
        wait(|| async {
            system.inbox_cursor(&saver).is_some() && system.inbox_cursor(&audit).is_some()
        })
        .await;
        (system, saver, audit)
    }

    #[tokio::test]
    async fn adapter_replies_the_asker_on_success() {
        // Given the actor spawned on a real system.
        let (system, saver, _audit) = demo_system().await;
        let path = std::env::temp_dir().join(format!("adapter-ok-{}.txt", std::process::id()));

        // When asking it to save.
        let ack = system
            .ask(
                saver,
                SaveFile {
                    path,
                    contents: b"via ask".to_vec(),
                },
                Duration::from_secs(5),
            )
            .await
            .expect("acked");

        // Then the SaveAck reply reports the written bytes — a failed
        // save would have replied SaveFailed instead.
        assert_eq!(ack["bytes"], 7);
    }

    #[tokio::test]
    async fn adapter_replies_save_failed_when_the_write_is_denied() {
        // Given the actor on a real system and a globally denied path.
        let (system, saver, _audit) = demo_system().await;
        let path = std::env::temp_dir().join("adapter-denied.txt");
        MemFs::deny(&path);

        // When asking it to save to the denied path.
        let reply = system
            .ask(
                saver,
                SaveFile {
                    path: path.clone(),
                    contents: b"x".to_vec(),
                },
                Duration::from_secs(5),
            )
            .await
            .expect("the failure is a REPLY, not an ask error");

        // Then the reply names the path and the reason.
        assert_eq!(reply["reason"], "PermissionDenied");
        assert_eq!(reply["path"], path.display().to_string());
    }

    #[tokio::test]
    async fn a_failed_tell_is_observable_through_the_published_fact() {
        // Given the actor + audit on a real system. The AUDIT sink is
        // process-global and tests run concurrently — so wait for GROWTH
        // past a baseline rather than a non-empty sink, and use a path
        // unique to this test.
        let (system, saver, _audit) = demo_system().await;
        let baseline = AUDIT.lock().len();
        let path = std::env::temp_dir().join("adapter-tell-denied.txt");
        MemFs::deny(&path);

        // When telling (no asker: the reply channel is gone).
        system
            .tell(
                saver,
                SaveFile {
                    path,
                    contents: b"x".to_vec(),
                },
            )
            .await
            .expect("told");

        // Then the failure still surfaces — as a published fact at the
        // audit subscriber. Never a broadcast reply; an event publish.
        wait(|| async { AUDIT.lock().len() > baseline }).await;
        let seen = AUDIT.lock();
        assert!(
            seen.iter()
                .any(|f| f.contains("adapter-tell-denied.txt") && f.contains("PermissionDenied")),
            "audit must observe the failed tell: {seen:?}"
        );
    }
}
