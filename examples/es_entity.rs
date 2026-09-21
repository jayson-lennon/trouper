//! Event-sourced entities under a partition set: transparent
//! passivation and re-activation, plus one uniform receive surface.
//!
//! Senders address the SET path (`accts`); the kernel extracts the
//! schema-declared shard key, derives `accts/<key>`, and activates the
//! entity on demand from the shared factory. After an idle window the
//! entity PASSIVATES (slot gone, journal durable); the next command to
//! the set re-activates it and journal replay rebuilds the balance —
//! all invisible to senders, who never learn entities exist.
//!
//! The same factory declares the entity's handled schemas: commands
//! arrive via tells to the set, AND a PUBLISHED event reaches every
//! live entity that declared it — one declaration, every transport.
//!
//! Run: `cargo run --example es_entity`

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::prelude::*;

/// The set's command: an int amount plus the STRING shard key. The
/// kernel reads `account` per envelope (declared via `as_shard_key`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountCmd {
    account: String,
    delta: i64,
}
impl Schema for AccountCmd {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "AccountCmd".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![
                FieldDef::required("account", FieldTy::Str).as_shard_key(),
                FieldDef::required("delta", FieldTy::Int),
            ],
            description: Some("Adjust one account's balance.".into()),
        }
    }
}

/// The decision event: the only thing the journal stores.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountAdjusted {
    delta: i64,
}
impl Schema for AccountAdjusted {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "AccountAdjusted".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("delta", FieldTy::Int)],
            description: None,
        }
    }
}

/// An idle-hour fact entities declare: every LIVE entity receives a
/// copy when it is published (a published copy dispatches exactly like
/// a told one — same declaration, same dispatch table).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MarketBell {
    ring: u32,
}
impl Schema for MarketBell {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "MarketBell".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("ring", FieldTy::Int)],
            description: Some("A market-wide announcement.".into()),
        }
    }
}

/// The event-sourced entity: pure decisions, fold-on-apply, keyed
/// genesis (each entity seeds from its own `key`).
#[derive(Serialize, Deserialize, Default)]
struct Account {
    balance: i64,
}

impl EventSourcedActor for Account {
    fn restore(args: &serde_json::Value) -> Self {
        Account { balance: 0 }.seeded(args)
    }

    fn apply(&mut self, event: &Event) {
        if event.schema == AccountAdjusted::schema_id() {
            self.balance += event.payload["delta"].as_i64().unwrap_or(0);
        }
    }
}

impl Account {
    fn seeded(mut self, args: &serde_json::Value) -> Self {
        if let Some(opening) = args["opening"].as_i64() {
            self.balance = opening;
        }
        self
    }
}

impl CommandHandler<AccountCmd> for Account {
    fn handle(&self, cmd: AccountCmd, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        vec![Event::new(
            AccountAdjusted::schema_id(),
            json!({ "delta": cmd.delta }),
        )]
    }
}

impl CommandHandler<MarketBell> for Account {
    fn handle(&self, bell: MarketBell, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        // A published event is still just an input: the entity decides
        // and journals its response like any other message.
        vec![Event::new(
            AccountAdjusted::schema_id(),
            json!({ "delta": bell.ring as i64 }),
        )]
    }
}

#[tokio::main]
async fn main() {
    let system = ActorSystem::new(SystemConfig::production());
    system.register_schema::<AccountCmd>();
    system.register_schema::<AccountAdjusted>();
    system.register_schema::<MarketBell>();

    // The set: senders address `accts`; entities spawn from the factory
    // with 60ms idle passivation (fast enough to watch happen).
    let spec = trouper::pool::PartitionSpec {
        public: ActorPath::new("accts"),
        system: system.clone(),
        factory: Arc::new(|system, path, args| {
            trouper::builder::spawn_es_builder::<Account>(system)
                .at(path.clone())
                .args(args.clone())
                .handles::<AccountCmd>()
                .handles::<MarketBell>()
                .emits::<AccountAdjusted>()
                .passivate_after(Duration::from_millis(60))
                .start();
        }),
        key_field: "account".to_owned(),
        // Genesis template: every entity opens at 100, plus its key.
        args_template: Some(json!({ "opening": 100 })),
        opts: SpawnOpts::default(),
    };
    system
        .install_partition_set(spec)
        .expect("partition set installs");

    // 1. Activate two distinct entities with commands to the SET path.
    for (acct, delta) in [("alice", 50), ("bob", -20)] {
        system
            .tell(
                ActorPath::new("accts"),
                AccountCmd {
                    account: acct.into(),
                    delta,
                },
            )
            .await
            .expect("delivered");
    }
    wait(|| async {
        system
            .es_state(&ActorPath::new("accts/alice"))
            .await
            .and_then(|s| s["balance"].as_i64())
            == Some(150)
            && system
                .es_state(&ActorPath::new("accts/bob"))
                .await
                .and_then(|s| s["balance"].as_i64())
                == Some(80)
    })
    .await;
    let alice = system
        .es_state(&ActorPath::new("accts/alice"))
        .await
        .expect("alice live");
    let bob = system
        .es_state(&ActorPath::new("accts/bob"))
        .await
        .expect("bob live");
    println!("after commands:  alice = {alice}, bob = {bob}");
    println!("  (separate entities, separate journals, one public path)");

    // 2. Let them idle out: passivation is the KERNEL's call (60ms).
    //    Observable as a Stopped{Passivated} fact, not a lost slot.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let passivated = system.tap_facts().iter().any(|f| {
        matches!(
            &f.kind,
            trouper::tap::FactKind::Stopped {
                path,
                reason: trouper::actor::StopReason::Passivated,
            } if path.as_str() == "accts/alice"
        )
    });
    println!("after 60ms idle: alice passivated (Stopped fact) = {passivated}");
    assert!(passivated, "the idle entity was passivated");
    let alice_journal = system.journal_schemas(&ActorPath::new("accts/alice"));
    println!(
        "  ...but her journal survives ({} Adjusted event)",
        alice_journal.len()
    );

    // 3. Re-address the same key: transparent re-activation, journal
    //    replay restores the balance.
    system
        .tell(
            ActorPath::new("accts"),
            AccountCmd {
                account: "alice".into(),
                delta: 10,
            },
        )
        .await
        .expect("delivered");
    wait(|| async { system.journal_schemas(&ActorPath::new("accts/alice")).len() == 2 }).await;
    let alice2 = system
        .es_state(&ActorPath::new("accts/alice"))
        .await
        .expect("reactivated");
    println!("after re-activation: alice = {alice2} (replay rebuilt 150, then +10)");

    // 4. A PUBLISHED event: every LIVE entity that declared MarketBell
    //    receives one copy and journals its decision — dispatched
    //    through the SAME entry table as a told command. Alice was
    //    re-activated at step 3 (inside her idle window); a passivated
    //    entity receives nothing (publish never activates, never
    //    phantom-delivers).
    system.publish(&MarketBell { ring: 5 }).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let alice3 = system
        .es_state(&ActorPath::new("accts/alice"))
        .await
        .expect("alice still live");
    println!("after MarketBell(5): alice = {alice3} (published copy journaled)");
    assert_eq!(
        alice3["balance"], 165,
        "the published event's decision applied"
    );

    println!(
        "\nEntities are lazy, keyed, and durable: the set activates them on\n demand, passivation is invisible, and .handles covers every transport."
    );
}

/// Polls a condition for up to 2s (demo pacing).
async fn wait<F, Fut>(mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..1_000 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("condition never became true");
}
