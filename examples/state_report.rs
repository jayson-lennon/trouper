//! A small but complete actor system whose topology touches every export
//! section — an ES pool, a partition set with activated entities, a
//! service-actor topic subscriber, and a tee rule — that answers state
//! queries over zenoh until interrupted.
//!
//! Manual two-shell run (the example occupies the shell it runs in):
//!
//! ```text
//! shell 1: cargo run --example state_report
//! shell 2: cargo run -p canvas
//! ```
//!
//! Shell 1 keeps running (it IS the answering system) until ctrl-c; each
//! `canvas` invocation in shell 2 prints that instant's export.

use error_stack::Report;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::Level;
use trouper::actor::{CommandHandler, EventSourcedActor, MsgHandler, ServiceActor};
use trouper::prelude::*;
use trouper::registry::RegistryError;
use trouper::state_report::{ReportState, StateReported, StateReporter};
use trouper::system::ActorSystem;

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
    async fn start(_args: &serde_json::Value) -> Result<Self, Report<RegistryError>> {
        Ok(Self { consumed: 0 })
    }
}

impl MsgHandler<FactMsg> for FactCounter {
    async fn handle(&mut self, _fact: FactMsg, _ctx: &mut MsgCtx<'_>) {
        self.consumed += 1;
    }
}

// --- the demo system -------------------------------------------------------

/// Builds the demo system — demo topology, state reporter, warm-up
/// traffic — and returns once everything has settled.
///
/// Sections produced (for the export the bridge will serve):
/// - schemas: every demo message type plus the reporting pair;
/// - actors: `watchdog` (service) + pool workers + partition entities +
///   `state/reporter`;
/// - pools: `api` (2 round-robin workers behind `api-parent`);
/// - partitions: `accounts` (shard key `account`, 2 activated entities);
/// - declared/observed edges: from the manifests above;
/// - rules: one tee rule mirroring `Work` traffic to `watchdog`.
pub async fn build_demo_system() -> ActorSystem {
    let system = ActorSystem::new(SystemConfig::production());

    // The partition spec below validates against the schema table AT
    // INSTALL TIME — but the Account builder registers KeyedAdd only when
    // its factory spawns the first entity (after traffic arrives). The
    // shard-key def must therefore be an agreed fact up front.
    system.register_schema::<KeyedAdd>();

    // 1. The topic subscriber: a service actor on `system.facts`.
    trouper::builder::spawn_service_builder::<FactCounter>(&system)
        .at(ActorPath::new("watchdog"))
        .args(json!({}))
        .handles::<FactMsg>()
        .mailbox(64, trouper::inbox::OverloadPolicy::DropNew)
        .start();
    system
        .subscribe(
            &ActorPath::new("watchdog"),
            &trouper::registry::Registry::facts_topic(),
            None,
        )
        .expect("subscribe watchdog to system.facts");

    // 2. The pool: `api` — senders keep addressing `api`; the kernel
    //    picks one of 2 workers round-robin (workers are children of
    //    `api-parent` for escalation).
    system
        .install_pool(trouper::pool::PoolSpec {
            public: ActorPath::new("api"),
            workers: 2,
            algo: trouper::pool::PoolAlgo::RoundRobin,
            factory: Arc::new(|system, path, args| {
                trouper::builder::spawn_es_builder::<Worker>(system)
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
    system.install_rule(trouper::pool::Rule {
        source: None,
        schema: Some(Work::schema_id()),
        dest: Some(ActorPath::new("api")),
        action: trouper::pool::RuleAction::Tee(ActorPath::new("watchdog")),
    });

    // 4. The partition set: `accounts`, shard key `account`.
    system
        .install_partition_set(trouper::pool::PartitionSpec {
            public: ActorPath::new("accounts"),
            system: system.clone(),
            factory: Arc::new(|system, path, args| {
                trouper::builder::spawn_es_builder::<Account>(system)
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

    // 5. The state reporter: the journaled recorder the bridge serves
    //    every state query through.
    trouper::builder::spawn_es_builder::<StateReporter>(&system)
        .at(ActorPath::new("state/reporter"))
        .args(json!({}))
        .handles::<ReportState>()
        .emits::<StateReported>()
        .start();

    // 6. Warm-up traffic: pool sends, keyed adds (activating 2
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
                matches!(&f.kind, trouper::tap::FactKind::Delivered { to, .. }
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

/// Runs the demo: installs the state bridge, prints what was built, then
/// stays alive answering state queries until ctrl-c.
///
/// # Errors
///
/// Propagates bridge installation failures (zenoh session/queryable).
pub async fn run_demo_system() -> Result<(), state_report::StateBridgeError> {
    let system = build_demo_system().await;
    let _session = state_report::install(system.clone(), ActorPath::new("state/reporter")).await?;
    println!("demo system ready:");
    println!("  pool       api       2 workers (round-robin)");
    println!("  partition  accounts  key=account, entities acme+globex");
    println!("  subscriber watchdog  on system.facts (service actor)");
    println!("  rule       tee       Work@api -> watchdog");
    println!("  reporter   state/reporter (journaled StateReported)");
    println!("answering state queries on the trouper/state key (ctrl-c to stop)");
    tokio::signal::ctrl_c()
        .await
        .expect("ctrl-c handler installs");
    println!("bye");
    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    if let Err(e) = run_demo_system().await {
        eprintln!("state_report: {e}");
        std::process::exit(1);
    }
}
