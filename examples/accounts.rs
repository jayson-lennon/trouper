//! The account demo: two outcome channels, both observable.
//!
//! A supervised event-sourced `Account` answers every command with a
//! *total* decision:
//! - A valid `Withdraw` journals `Withdrawn` (balance drops).
//! - An overdraft is a DOMAIN outcome: it journals `WithdrawFailed`
//!   (requested + balance at the time) — a fact as real as the
//!   withdrawal. No crash, no error channel; the event stream is the
//!   answer, and any projection can subscribe to it.
//! - The `Poison` command is a TECHNICAL failure: the handler panics,
//!   `catch_unwind` marks the actor crashed, and supervision restarts
//!   it — journal replay restores the balance (including the declines),
//!   and pending/next commands succeed against the rebuilt state.
//!
//! A facts observer (subscription filter on `system.facts`) prints the
//! crash/restart slice; `tracing::error!` from the kernel is the
//! developer channel. The journal printout at the end shows every
//! journaled entry, `WithdrawFailed` included.
//!
//! Run: `cargo run --example accounts`

use actor_runtime::actor::{CommandHandler, EventSourcedActor, MsgHandler, ServiceActor};
use actor_runtime::prelude::*;
use actor_runtime::registry::RegistryError;
use actor_runtime::tap::FactKind;
use error_stack::Report;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, OnceLock};
use tracing::Level;

// -- Commands -------------------------------------------------------------

#[derive(Deserialize)]
struct Deposit {
    n: i64,
}

impl Schema for Deposit {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Deposit".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

#[derive(Deserialize)]
struct Withdraw {
    n: i64,
}

impl Schema for Withdraw {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Withdraw".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

/// The technical-failure trigger: panics in the handler ONCE — a
/// transient fault. (A poison that panics every time is a permanent
/// fault: supervision would burn the budget and stop the actor.)
#[derive(Deserialize)]
struct Poison {
    n: i64,
}

impl Schema for Poison {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Poison".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

// -- Events ---------------------------------------------------------------

#[derive(Deserialize)]
struct Deposited {
    #[allow(dead_code)]
    n: i64,
}

impl Schema for Deposited {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Deposited".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

#[derive(Deserialize)]
struct Withdrawn {
    #[allow(dead_code)]
    n: i64,
}

impl Schema for Withdrawn {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Withdrawn".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

/// The domain rejection, journaled like any event: a fact about the
/// world ("a decline happened"), not an error report.
#[derive(Deserialize)]
struct WithdrawFailed {
    #[allow(dead_code)]
    requested: i64,
    #[allow(dead_code)]
    balance: i64,
}

impl Schema for WithdrawFailed {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "WithdrawFailed".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("requested", FieldTy::Int),
                FieldDef::required("balance", FieldTy::Int),
            ],
            description: None,
        }
    }
}

// -- The account ----------------------------------------------------------

#[derive(Serialize, Deserialize, Default)]
struct Account {
    balance: i64,
}

impl EventSourcedActor for Account {
    fn restore(_args: &serde_json::Value) -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &Event) {
        match event.schema.name() {
            "Deposited" => self.balance += event.payload["n"].as_i64().unwrap_or(0),
            "Withdrawn" => self.balance -= event.payload["n"].as_i64().unwrap_or(0),
            "WithdrawFailed" => {} // a decline changes nothing
            _ => {}
        }
    }
}

impl CommandHandler<Deposit> for Account {
    fn handle(&self, cmd: Deposit, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        vec![Event::new(Deposited::schema_id(), json!({ "n": cmd.n }))]
    }
}

impl CommandHandler<Withdraw> for Account {
    fn handle(&self, cmd: Withdraw, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        if self.balance >= cmd.n {
            vec![Event::new(Withdrawn::schema_id(), json!({ "n": cmd.n }))]
        } else {
            // Domain rejection as an event: total decision, journaled,
            // replayed, observable. (The empty-vec alternative is the
            // silent form; the event makes the decline a fact.)
            vec![Event::new(
                WithdrawFailed::schema_id(),
                json!({ "requested": cmd.n, "balance": self.balance }),
            )]
        }
    }
}

impl CommandHandler<Poison> for Account {
    fn handle(&self, cmd: Poison, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        // Technical failure: panic on the FIRST sight (n = 1). The crash
        // is transient — after the supervised restart, the redelivered
        // command passes (the fault is gone) and processing continues.
        static POISONED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if cmd.n == 1 && !POISONED.swap(true, std::sync::atomic::Ordering::SeqCst) {
            panic!("poison command: simulated technical fault (transient)");
        }
        vec![]
    }
}

// -- The facts observer ----------------------------------------------------

static LINES: OnceLock<std::sync::Mutex<Vec<String>>> = OnceLock::new();

fn lines() -> &'static std::sync::Mutex<Vec<String>> {
    LINES.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

fn tell(line: String) {
    println!("   {line}");
    lines().lock().expect("lines lock").push(line);
}

/// The Rust mirror of the runtime's `Fact@1` schema (what `system.facts`
/// delivers to subscribers).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FactMsg {
    kind: String,
    offset: u64,
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

/// A story observer: subscribed to `system.facts` with a filter that only
/// admits `failed` and `spawned` facts — the crash/restart slice.
struct StoryObserver;

impl ServiceActor for StoryObserver {
    async fn start(_args: &serde_json::Value) -> Result<Self, Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<FactMsg> for StoryObserver {
    async fn handle(&mut self, fact: FactMsg, ctx: &mut MsgCtx<'_>) {
        if ctx.core.self_path.as_str() == "story2" {
            tell(format!(
                "spawn fact seen (offset {}) — initial spawn or supervised restart",
                fact.offset
            ));
        } else {
            tell(format!(
                "CRASH fact seen (offset {}) — handler panicked",
                fact.offset
            ));
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::ERROR)
        .init();
    let system = Arc::new(ActorSystem::new(SystemConfig::production()));

    // The story observer: one subscription, filtered to failed/spawned.
    actor_runtime::builder::spawn_service_builder::<StoryObserver>(&system)
        .at(ActorPath::new("story"))
        .args(json!({}))
        .handles::<FactMsg>()
        .mailbox(64, actor_runtime::inbox::OverloadPolicy::DropNew)
        .start();
    system
        .subscribe_filtered(
            &ActorPath::new("story"),
            &actor_runtime::registry::Registry::facts_topic(),
            None,
            actor_runtime::topics::SubscriptionFilter {
                kind: Some("failed".into()),
                ..actor_runtime::topics::SubscriptionFilter::default()
            },
        )
        .expect("subscribe");
    add_spawn_filter(&system);
    println!("== account demo ==");
    println!("   observers subscribed: failures ('story') + restarts ('story2')");

    // The account lives under supervision (restart budget 3 per 10s).
    let account = ActorPath::new("account");
    let spec = actor_runtime::supervision::ChildSpec {
        path: account.clone(),
        parent: None,
        restart: actor_runtime::supervision::RestartPolicy::Permanent,
        budget: actor_runtime::supervision::RestartBudget::per(
            3,
            std::time::Duration::from_secs(10),
        ),
        backoff: actor_runtime::supervision::Backoff {
            base: std::time::Duration::from_millis(20),
            max: std::time::Duration::from_millis(80),
            factor: 2.0,
        },
        args: json!({}),
        spawn: Arc::new(
            |sys: &Arc<ActorSystem>, path: &ActorPath, args: &serde_json::Value| {
                actor_runtime::builder::spawn_es_builder::<Account>(sys)
                    .at(path.clone())
                    .args(args.clone())
                    .handles::<Deposit>()
                    .handles::<Withdraw>()
                    .handles::<Poison>()
                    .emits::<Deposited>()
                    .emits::<Withdrawn>()
                    .emits::<WithdrawFailed>()
                    .start();
            },
        ),
    };
    system.spawn_child(spec);
    wait(|| async { system.inbox_cursor(&account).is_some() }).await;
    println!("   account spawned under supervision at '{account}'");

    // -- 1. The happy path --------------------------------------------------
    println!("== 1. success ==");
    send(&system, Deposit::schema_id(), 100).await;
    send(&system, Withdraw::schema_id(), 30).await;
    wait_for_balance(&system, &account, 70).await;
    println!("   deposited 100, withdrew 30 → balance 70");

    // -- 2. The domain rejection -------------------------------------------
    println!("== 2. withdraw-failed (domain outcome, journaled) ==");
    send(&system, Withdraw::schema_id(), 500).await;
    wait_for_journal_len(&system, &account, 3).await;
    let state = system.es_state(&account).await.expect("live");
    println!(
        "   withdraw 500 declined → balance still {} (decline journaled: WithdrawFailed)",
        state["balance"]
    );

    // -- 3. The technical failure ------------------------------------------
    println!("== 3. poison (transient panic → supervision) ==");
    // The poison panics ONCE; the supervised restart redelivers it, it
    // passes, and the queued deposit behind it also lands (at-least-once
    // through the crash: the cursor never acked the un-acked poison).
    send(&system, Poison::schema_id(), 1).await;
    send(&system, Deposit::schema_id(), 10).await;
    wait(|| async {
        system
            .tap_facts()
            .iter()
            .any(|f| matches!(&f.kind, FactKind::Failed { path, .. } if *path == account))
    })
    .await;
    println!("   handler panicked → Failed fact → supervision restarts");

    // -- 4. Life after restart ----------------------------------------------
    println!("== 4. after restart ==");
    // The rebuilt actor replays the journal (balance 70: declines change
    // nothing), the redelivered poison passes, the queued deposit lands.
    wait_for_balance(&system, &account, 80).await;
    let restarted = system
        .tap_facts()
        .iter()
        .any(|f| matches!(&f.kind, FactKind::Spawned { restart: true, .. }));
    assert!(restarted, "supervision emitted Spawned{{restart:true}}");
    println!("   replay + redelivery → balance 80 (the crash never lost state or mail)");

    // -- 5. The journal tells the whole story -------------------------------
    println!("== 5. journal ==");
    let schemas: Vec<String> = system
        .journal_schemas(&account)
        .iter()
        .map(|s| s.to_string())
        .collect();
    println!("   {schemas:?}");
    println!("   (WithdrawFailed is a first-class journal entry — replay reproduces it)");
    println!("accounts example complete");
}

/// Adds a SECOND filtered subscription admitting spawned facts (a second
/// StoryObserver instance on the same path is illegal, so this installs a
/// second observer "story2").
fn add_spawn_filter(system: &Arc<ActorSystem>) {
    actor_runtime::builder::spawn_service_builder::<StoryObserver>(&system.clone())
        .at(ActorPath::new("story2"))
        .args(json!({}))
        .handles::<FactMsg>()
        .mailbox(64, actor_runtime::inbox::OverloadPolicy::DropNew)
        .start();
    system
        .subscribe_filtered(
            &ActorPath::new("story2"),
            &actor_runtime::registry::Registry::facts_topic(),
            None,
            actor_runtime::topics::SubscriptionFilter {
                kind: Some("spawned".into()),
                ..actor_runtime::topics::SubscriptionFilter::default()
            },
        )
        .expect("subscribe");
}

/// Sends one command with an int payload `{"n": n}` (Poison uses n as its
/// transient-fault marker: n = 1 panics the first time only).
async fn send(system: &Arc<ActorSystem>, schema: SchemaId, n: i64) {
    system
        .send(system.envelope(schema, ActorPath::new("account"), json!({ "n": n })))
        .await
        .expect("delivered");
}

/// Polls until the account's live balance is `want` (5s budget).
async fn wait_for_balance(system: &Arc<ActorSystem>, path: &ActorPath, want: i64) {
    wait(|| async {
        system
            .es_state(path)
            .await
            .and_then(|s| s["balance"].as_i64())
            == Some(want)
    })
    .await;
}

/// Polls until the journal holds `want` entries (5s budget).
async fn wait_for_journal_len(system: &Arc<ActorSystem>, path: &ActorPath, want: usize) {
    wait(|| async { system.journal_schemas(path).len() >= want }).await;
}

/// Polls `cond` until true (5s budget) — demo pacing helper.
async fn wait<F, Fut>(cond: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..2_500 {
        if cond().await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    panic!("demo condition never became true");
}
