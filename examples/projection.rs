//! The projector: an event-sourced READ MODEL that folds FACTS from
//! other actors into its own state — with catch-up.
//!
//! `Balances` is a global projector: ONE actor holding every account's
//! balance. It declares `.consumes::<AccountAdjusted>()` — the entity
//! events from the `es_entity` pattern — and the runtime:
//!
//! 1. arms the projector (slot + routes) BEFORE its loop runs,
//! 2. re-records every folded fact into the projector's OWN journal
//!    (the journal IS the checkpoint),
//! 3. scans the store for what the journal lacks and seeds the gap,
//! 4. then goes live — facts published during seeding queue up and
//!    fold AFTER history (no gap, no duplicate).
//!
//! `start_and_catchup().await` returns only when the fold is complete
//! (`CaughtUp` records the seeded count on the tap). Reads split:
//! `projector_state` is the complete fold; `es_state` is a raw peek
//! that never wakes anything.
//!
//! Run: `cargo run --example projection`

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::prelude::*;

// ---- The entity side (the es_entity pattern, compressed) -----------

/// The entity's command.
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

/// The entity's decision event — the fact the projector consumes.
/// (Carrying `account` in the fact is what makes a GLOBAL fold
/// possible: the projector reads the key from the payload.)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountAdjusted {
    account: String,
    delta: i64,
}
impl Schema for AccountAdjusted {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "AccountAdjusted".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("account", FieldTy::Str),
                FieldDef::required("delta", FieldTy::Int),
            ],
            description: None,
        }
    }
}

/// One account entity: folds its own adjustments.
#[derive(Default, Debug, Serialize, Deserialize)]
struct Account {
    balance: i64,
}
impl CommandHandler<AccountCmd> for Account {
    fn handle(&self, cmd: AccountCmd, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        vec![Event::new(
            AccountAdjusted::schema_id(),
            json!({ "account": cmd.account, "delta": cmd.delta }),
        )]
    }
}

impl EventSourcedActor for Account {
    fn restore(_args: &serde_json::Value) -> Self {
        Self::default()
    }
    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "AccountAdjusted@1" {
            self.balance += event.payload["delta"].as_i64().unwrap_or(0);
        }
    }
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles_id(AccountCmd::schema_id())
            .emits_id(AccountAdjusted::schema_id())
    }
}

// ---- The projector (THIS example's subject) ------------------------

/// The read model: every account's balance, folded from the FACTS the
/// entities recorded. `Projector: Default` — genesis is `default()`
/// and catch-up re-folds history into it.
#[derive(Default, Debug, Serialize, Deserialize)]
struct Balances {
    by_account: HashMap<String, i64>,
}
impl Projector for Balances {
    /// THE fold. The same code path folds history (catch-up), live
    /// broadcast copies, and restart replay.
    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() != "AccountAdjusted@1" {
            return;
        }
        let name = event.payload["account"].as_str().unwrap_or("?");
        let delta = event.payload["delta"].as_i64().unwrap_or(0);
        *self.by_account.entry(name.to_owned()).or_default() += delta;
    }
}

#[tokio::main]
async fn main() {
    let (system, _clock) = ActorSystem::test();
    system.register_schema::<AccountCmd>();
    system.register_schema::<AccountAdjusted>();

    // The entity partition set: commands to `accts` derive per-key
    // entities that record `AccountAdjusted` facts.
    let partition = trouper::pool::PartitionSpec {
        public: ActorPath::new("accts"),
        system: system.clone(),
        factory: Arc::new(|system, path, args| {
            spawn_es_builder::<Account>(system)
                .at(path.clone())
                .args(args.clone())
                .handles::<AccountCmd>()
                .emits::<AccountAdjusted>()
                .start();
        }),
        key_field: "account".to_owned(),
        args_template: None,
        opts: SpawnOpts::default(),
    };
    system.install_partition_set(partition).expect("install");

    // ---- Entities record facts (the write side) --------------------
    for (name, delta) in [("alice", 100), ("bob", 40), ("alice", -30), ("carol", 7)] {
        system
            .send(system.envelope(
                AccountCmd::schema_id(),
                ActorPath::new("accts"),
                json!({ "account": name, "delta": delta }),
            ))
            .await
            .expect("command routed");
    }

    // ---- The projector comes up LATE and catches up ----------------
    // (No retained log would lose this history; the store's journals
    // are the log, the projector's own journal is its checkpoint.)
    let balances = spawn_projector_builder::<Balances>(&system)
        .at(ActorPath::new("proj/balances"))
        .consumes::<AccountAdjusted>()
        .start_and_catchup()
        .await;

    let fold = system.projector_state(&balances).await.expect("fold");
    println!("balances after catch-up: {fold}");

    // ---- Live facts fold too, exactly once -------------------------
    system
        .send(system.envelope(
            AccountCmd::schema_id(),
            ActorPath::new("accts"),
            json!({ "account": "bob", "delta": 10 }),
        ))
        .await
        .expect("command routed");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let fold = system.projector_state(&balances).await.expect("fold");
    println!("balances after live fact: {fold}");

    // bob's live fact folded ONCE — the projector re-recorded it into
    // its journal as a checkpoint, and a restart would NOT re-seed it.
    system.stop(&balances).await;
    let again = spawn_projector_builder::<Balances>(&system)
        .at(balances.clone())
        .consumes::<AccountAdjusted>()
        .start_and_catchup()
        .await;
    let fold = system.projector_state(&again).await.expect("fold");
    println!("balances after restart (no double-fold): {fold}");

    // The raw peek: in-memory only, never wakes anything.
    println!(
        "es_state peek: {:?}",
        system.es_state(&again).await.is_some()
    );
}
