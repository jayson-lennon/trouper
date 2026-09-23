//! A router is a stateless actor on the hot path — a composition, not a
//! runtime feature.
//!
//! `DocumentSaved` is a FACT: the sender publishes it, and every actor
//! that declared `.handles::<DocumentSaved>()` gets one copy. Two very
//! different consumers receive identical copies from that single
//! declaration:
//! - `ParseRouter` treats the copy as WORK: it translates domains
//!   (`DocumentSaved` → `ParseDocument`) and forwards via
//!   `ctx.send_to_any` — exactly one parser gets the job.
//! - `MetricsRecorder` treats the copy as NEWS: it counts and moves on.
//!
//! Nothing in the fabric knows which is which. "News vs work" lives in
//! the handler, where it belongs.
//!
//! Run: `cargo run --example router`

use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use trouper::actor::{MsgHandler, ServiceActor};
use trouper::prelude::*;
use trouper::registry::RegistryError;

static LINES: std::sync::OnceLock<Arc<Mutex<Vec<String>>>> = std::sync::OnceLock::new();

fn log(line: impl Into<String>) {
    let lines = LINES.get_or_init(|| Arc::new(Mutex::new(Vec::new())));
    let line = line.into();
    println!("{line}");
    lines.lock().expect("lines lock").push(line);
}

/// The fact: a document was saved somewhere.
#[derive(Event, Debug, Serialize, Deserialize, Clone)]
#[schema(description = "A document was saved.")]
struct DocumentSaved {
    uri: String,
}

/// The command the router issues into the parser domain.
#[derive(Command, Debug, Serialize, Deserialize, Clone)]
#[schema(description = "Parse this document.")]
struct ParseDocument {
    uri: String,
}

/// The parser's completion fact.
#[derive(Event, Debug, Serialize, Deserialize, Clone)]
#[schema(description = "A document was parsed.")]
struct DocumentParsed {
    uri: String,
}

/// The router: stateless, on the hot path. It receives the published
/// fact like any other handler and TRANSLATES — one `send_to_any` per
/// fact, so exactly one parser is chosen by the route table.
struct ParseRouter;

impl ServiceActor for ParseRouter {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }

    async fn start(_args: &Json) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<DocumentSaved> for ParseRouter {
    async fn handle(&mut self, msg: DocumentSaved, ctx: &mut MsgCtx<'_>) {
        log(format!("[router] translating {} into parse work", msg.uri));
        ctx.send_to_any(ParseDocument { uri: msg.uri });
    }
}

/// A parser: one of several, all declaring `ParseDocument`. Each
/// `send_to_any` reaches exactly one of them, rotating.
struct Parser {
    id: &'static str,
}

impl Parser {
    fn new(id: u32) -> Self {
        Self {
            id: Box::leak(format!("parser-{id}").into_boxed_str()),
        }
    }
}

impl ServiceActor for Parser {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }

    async fn start(args: &Json) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self::new(args["id"].as_u64().unwrap_or(0) as u32))
    }
}

impl MsgHandler<ParseDocument> for Parser {
    async fn handle(&mut self, msg: ParseDocument, ctx: &mut MsgCtx<'_>) {
        log(format!("[{}] parsed {}", self.id, msg.uri));
        ctx.publish(&DocumentParsed { uri: msg.uri });
    }
}

/// The second consumer of the SAME published fact: pure news. It
/// never sees parse commands — those went to ONE parser each.
struct MetricsRecorder;

impl ServiceActor for MetricsRecorder {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }

    async fn start(_args: &Json) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<DocumentSaved> for MetricsRecorder {
    async fn handle(&mut self, msg: DocumentSaved, _ctx: &mut MsgCtx<'_>) {
        log(format!("[metrics] counted saved: {}", msg.uri));
    }
}

#[tokio::main]
async fn main() {
    let system = ActorSystem::new(SystemConfig::production());

    spawn_service_builder::<ParseRouter>(&system)
        .at(ActorPath::new("parse.router"))
        .handles::<DocumentSaved>()
        .emits::<ParseDocument>()
        .start();
    spawn_service_builder::<MetricsRecorder>(&system)
        .at(ActorPath::new("metrics"))
        .handles::<DocumentSaved>()
        .start();
    for id in 0..2_u32 {
        trouper::builder::spawn_service_builder::<Parser>(&system)
            .at(ActorPath::new(format!("parse.worker-{id}")))
            .args(serde_json::json!({ "id": id }))
            .handles::<ParseDocument>()
            .start();
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // The emitter: a saver that publishes its fact from INSIDE a
    // handler (the ctx outbox path, flushed post-ack by the kernel).
    struct Saver;
    impl ServiceActor for Saver {
        fn manifest() -> ActorManifest {
            ActorManifest::new().kind(ActorKind::Service)
        }
        async fn start(_args: &Json) -> Result<Self, error_stack::Report<RegistryError>> {
            Ok(Self)
        }
    }
    impl MsgHandler<SaveCmd> for Saver {
        async fn handle(&mut self, msg: SaveCmd, ctx: &mut MsgCtx<'_>) {
            ctx.publish(&DocumentSaved {
                uri: msg.uri.clone(),
            });
        }
    }
    #[derive(Command, Debug, Serialize, Deserialize, Clone)]
    struct SaveCmd {
        uri: String,
    }
    trouper::builder::spawn_service_builder::<Saver>(&system)
        .at(ActorPath::new("saver"))
        .handles::<SaveCmd>()
        .emits::<DocumentSaved>()
        .start();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Three saves: each publishes one fact; each fact fans out to the
    // router (work) and metrics (news).
    for uri in ["doc-a", "doc-b", "doc-c"] {
        system
            .tell(ActorPath::new("saver"), SaveCmd { uri: uri.into() })
            .await
            .expect("delivered");
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    println!("--- router report ---");
    let lines = LINES
        .get()
        .map(|l| l.lock().expect("lines lock").clone())
        .unwrap_or_default();
    let translates = lines.iter().filter(|l| l.contains("[router]")).count();
    let counted = lines.iter().filter(|l| l.contains("[metrics]")).count();
    let parsed: Vec<&String> = lines.iter().filter(|l| l.contains("parsed doc-")).collect();
    let parsing_workers: std::collections::BTreeSet<&str> = lines
        .iter()
        .filter(|l| l.contains("parsed doc-"))
        .map(|l| l.split(']').next().unwrap_or(""))
        .collect();
    println!("  facts published: 3");
    println!("  copies translated by the router (work): {translates}");
    println!("  copies counted by metrics (news): {counted}");
    println!("  parse jobs executed: {}", parsed.len());
    println!("  workers that parsed: {parsing_workers:?} (one job each, rotating)");
    assert_eq!(translates, 3);
    assert_eq!(counted, 3, "same copies, different meaning: news");
    assert_eq!(parsed.len(), 3, "exactly one parser per fact");
    assert_eq!(parsing_workers.len(), 2, "rotation spread the work");
    println!(
        "\nThe router is a plain actor on the hot path: it translates domains and\n\
         forwards with send_to_any. The fabric never learned what \"work\" means."
    );
}
