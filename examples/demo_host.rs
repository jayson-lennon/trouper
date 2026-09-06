//! The demo host: a small but complete system whose topology touches
//! every export section — an ES actor, a service actor, a pool, a
//! partition set with activated entities, a topic subscriber, and a tee
//! rule — served to canvas clients over loopback TCP.
//!
//! Manual two-shell run (the example occupies the shell it runs in):
//!
//! ```text
//! shell 1: cargo run --example demo_host
//! shell 2: cargo run -p canvas -- --connect 127.0.0.1:7667
//! ```
//!
//! The host keeps running (it IS the server) until interrupted.
//!
//! Run: `cargo run --example demo_host`

use actor_runtime::actor::{CommandHandler, EventSourcedActor, MsgHandler, ServiceActor};
use actor_runtime::prelude::*;
use actor_runtime::registry::RegistryError;
use actor_runtime::system::ActorSystem;
use error_stack::Report;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::Level;

/// Where the demo host serves canvas clients.
pub const DEMO_ADDR: &str = "127.0.0.1:7667";

// --- the demo traffic ----------------------------------------------------

#[derive(Deserialize)]
struct Work {
    #[allow(dead_code)] // the pool's kernel reads it (JSON side)
    n: i64,
}

impl Schema for Work {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Work".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

#[derive(Deserialize)]
struct WorkDone {
    #[allow(dead_code)]
    n: i64,
}

impl Schema for WorkDone {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "WorkDone".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

/// A pool worker (ES): consumes `Work`, emits `WorkDone`.
#[derive(Serialize, Deserialize, Default)]
struct Worker {
    total: i64,
}

impl EventSourcedActor for Worker {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<Work>()
            .emits::<WorkDone>()
            .kind(ActorKind::EventSourced)
    }

    fn restore(_args: &serde_json::Value) -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &Event) {
        self.total += event.payload["n"].as_i64().unwrap_or(0);
    }
}

impl CommandHandler<Work> for Worker {
    fn handle(&self, cmd: Work, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        vec![Event::new(WorkDone::schema_id(), json!({ "n": cmd.n }))]
    }
}

// --- partition set -------------------------------------------------------

#[derive(Deserialize)]
struct KeyedAdd {
    #[allow(dead_code)] // the kernel reads it as the shard key (JSON side)
    account: String,
    n: i64,
}

impl Schema for KeyedAdd {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "KeyedAdd".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![
                FieldDef::required("account", FieldTy::Str).as_shard_key(),
                FieldDef::required("n", FieldTy::Int),
            ],
            description: None,
        }
    }
}

#[derive(Deserialize)]
struct Added {
    #[allow(dead_code)]
    n: i64,
}

impl Schema for Added {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Added".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

/// A partition entity (ES account with a balance).
#[derive(Serialize, Deserialize, Default)]
struct Account {
    balance: i64,
}

impl EventSourcedActor for Account {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<KeyedAdd>()
            .emits::<Added>()
            .kind(ActorKind::EventSourced)
    }

    fn restore(_args: &serde_json::Value) -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &Event) {
        self.balance += event.payload["n"].as_i64().unwrap_or(0);
    }
}

impl CommandHandler<KeyedAdd> for Account {
    fn handle(&self, cmd: KeyedAdd, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        vec![Event::new(Added::schema_id(), json!({ "n": cmd.n }))]
    }
}

// --- service actor: the topic subscriber ---------------------------------

/// The Rust mirror of the runtime's Fact@1 schema (what `system.facts`
/// delivers to subscribers).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FactMsg {
    #[allow(dead_code)]
    kind: String,
    #[allow(dead_code)]
    offset: u64,
    #[allow(dead_code)]
    ts: i64,
}

impl Schema for FactMsg {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Fact".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("kind", FieldTy::Str),
                FieldDef::required("offset", FieldTy::Int),
                FieldDef::required("ts", FieldTy::Int),
            ],
            description: None,
        }
    }
}

/// A plain service actor that counts consumed facts (the topic
/// subscriber whose consumption proves the topic edge exists).
struct FactCounter {
    consumed: u64,
}

impl ServiceActor for FactCounter {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<FactMsg>()
            .kind(ActorKind::Service)
    }

    async fn start(_args: &serde_json::Value) -> Result<Self, Report<RegistryError>> {
        Ok(Self { consumed: 0 })
    }
}

impl MsgHandler<FactMsg> for FactCounter {
    async fn handle(&mut self, _fact: FactMsg, _ctx: &mut MsgCtx<'_>) {
        self.consumed += 1;
    }
}

// --- the host -------------------------------------------------------------

/// Builds the demo system and runs its warm-up traffic until settled.
///
/// Sections produced (for the export / canvas snapshot):
/// - schemas: every demo message type;
/// - actors: `watchdog` (service) + pool workers + partition entities;
/// - pools: `api` (2 round-robin workers behind `api-parent`);
/// - partitions: `accounts` (shard key `account`, 2 activated entities);
/// - declared/observed edges: from the manifests above;
/// - rules: one tee rule mirroring `Work` traffic to `watchdog`.
pub async fn build_demo_system() -> Arc<ActorSystem> {
    let system = Arc::new(ActorSystem::new(SystemConfig::production()));

    // Schemas for everything the demo sends (and the facts topic mirror).
    system.register_schema::<Work>();
    system.register_schema::<WorkDone>();
    system.register_schema::<KeyedAdd>();
    system.register_schema::<Added>();
    system.register_schema::<FactMsg>();

    // 1. The topic subscriber: a service actor on `system.facts`.
    actor_runtime::builder::spawn_service_builder::<FactCounter>(&system)
        .at(ActorPath::new("watchdog"))
        .args(json!({}))
        .handles::<FactMsg>()
        .mailbox(64, actor_runtime::inbox::OverloadPolicy::DropNew)
        .start();
    system
        .subscribe(
            &ActorPath::new("watchdog"),
            &actor_runtime::registry::Registry::facts_topic(),
            None,
        )
        .expect("subscribe watchdog to system.facts");

    // 2. The pool: `api` — senders keep addressing `api`; the kernel
    //    picks one of 2 workers round-robin (workers are children of
    //    `api-parent` for escalation).
    system
        .install_pool(actor_runtime::pool::PoolSpec {
            public: ActorPath::new("api"),
            workers: 2,
            algo: actor_runtime::pool::PoolAlgo::RoundRobin,
            factory: Arc::new(|system, path, args| {
                actor_runtime::builder::spawn_es_builder::<Worker>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .handles::<Work>()
                    .emits::<WorkDone>()
                    .start();
            }),
            args: Some(json!({})),
            parent: Some(ActorPath::new("api-parent")),
            seed: 42,
        })
        .await
        .expect("pool install");

    // 3. The tee rule: mirrors `Work` traffic to `watchdog`... which
    //    cannot consume `Work` (it handles `FactMsg`) — so instead the
    //    rule's declared shape is exercised by the export (rules: 1) and
    //    the watchdog's real food is the facts topic.
    system.install_rule(actor_runtime::pool::Rule {
        source: None,
        schema: Some(Work::schema_id()),
        dest: Some(ActorPath::new("api")),
        action: actor_runtime::pool::RuleAction::Tee(ActorPath::new("watchdog")),
    });

    // 4. The partition set: `accounts`, shard key `account`.
    system
        .install_partition_set(actor_runtime::pool::PartitionSpec {
            public: ActorPath::new("accounts"),
            system: system.clone(),
            factory: Arc::new(|system, path, args| {
                actor_runtime::builder::spawn_es_builder::<Account>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .handles::<KeyedAdd>()
                    .emits::<Added>()
                    .start();
            }),
            key_field: "account".to_owned(),
            args_template: None,
            opts: SpawnOpts::default(),
        })
        .expect("partition install");

    // 5. Warm-up traffic: pool sends, keyed adds (activating 2
    //    entities), until each flow settles.
    for i in 0..6 {
        system
            .send(system.envelope(Work::schema_id(), ActorPath::new("api"), json!({ "n": i })))
            .await
            .expect("delivered to a worker");
    }
    for (account, amount) in [("acme", 10), ("globex", 20), ("acme", 5)] {
        system
            .send(system.envelope(
                KeyedAdd::schema_id(),
                ActorPath::new("accounts"),
                json!({ "account": account, "n": amount }),
            ))
            .await
            .expect("delivered to the derived entity");
    }
    wait(|| async {
        system
            .es_state(&ActorPath::new("accounts/acme"))
            .await
            .and_then(|s| s["balance"].as_i64())
            == Some(15)
            && system
                .es_state(&ActorPath::new("accounts/globex"))
                .await
                .and_then(|s| s["balance"].as_i64())
                == Some(20)
    })
    .await;
    let served = |system: &ActorSystem| {
        system
            .tap_facts()
            .iter()
            .filter(|f| {
                matches!(&f.kind, actor_runtime::tap::FactKind::Delivered { to, .. }
                    if to.as_str().starts_with("api/worker-"))
            })
            .count()
    };
    wait(|| async { served(&system) >= 6 }).await;
    system
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

/// Runs the host: prints what it built, then serves canvas clients
/// forever (the serve call IS the host's main loop).
///
/// # Errors
///
/// Propagates listener failures from [`canvas_server::serve`].
pub async fn run_demo_system(addr: SocketAddr) -> std::io::Result<()> {
    let system = build_demo_system().await;
    println!("demo system ready:");
    println!("  pool       api       2 workers (round-robin)");
    println!("  partition  accounts  key=account, entities acme+globex");
    println!("  subscriber watchdog  on system.facts (service actor)");
    println!("  rule       tee       Work@api -> watchdog");
    println!("serving canvas clients on {addr} (ctrl-c to stop)");
    canvas_server::serve(system, addr).await
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::ERROR)
        .init();
    let addr: SocketAddr = DEMO_ADDR.parse().expect("valid demo addr");
    if let Err(e) = run_demo_system(addr).await {
        eprintln!("demo_host: {e}");
        std::process::exit(1);
    }
}
