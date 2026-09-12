//! Partition sets: senders address the SET path; the kernel extracts the
//! schema-declared shard key from the payload, derives the entity path
//! (`set/key`), and activates the entity on demand from the shared
//! factory. Same key → same entity, structurally.
//!
//! Run: `cargo run --example partitions`

use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::prelude::*;
use trouper::tap::FactKind;
use trouper::types::DeadLetterReason;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::Level;

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
            // The `account` field is the shard key — the kernel reads it
            // per envelope to derive the entity path.
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::ERROR)
        .init();
    let system = Arc::new(ActorSystem::new(SystemConfig::production()));

    // The partition spec below validates against the schema table AT
    // INSTALL TIME — but the Account builder registers KeyedAdd only when
    // its factory spawns the first entity (after traffic arrives). The
    // shard-key def must therefore be an agreed fact up front.
    system.register_schema::<KeyedAdd>();

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
    println!("== partition set 'accounts' (key field: account) ==");

    // Distinct keys activate DISTINCT entities (own journals).
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
    })
    .await;
    wait(|| async {
        system
            .es_state(&ActorPath::new("accounts/globex"))
            .await
            .and_then(|s| s["balance"].as_i64())
            == Some(20)
    })
    .await;
    println!("   accounts/acme   balance 15 (10 + 5: same key → same entity)");
    println!("   accounts/globex balance 20 (its own entity + journal)");

    // A command without its shard key is dead-lettered, NOT activated.
    let before = system.dead_letter_count().await;
    system
        .send(system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("accounts"),
            json!({ "n": 99 }),
        ))
        .await
        .expect("routed (the dead-letter happens at the entity step)");
    wait(|| async { system.dead_letter_count().await > before }).await;
    let missing = system
        .tap_facts()
        .iter()
        .filter(|f| {
            matches!(&f.kind, FactKind::DeadLettered { reason, .. }
                if *reason == DeadLetterReason::ShardKeyMissing)
        })
        .count();
    println!(
        "   keyless command dead-lettered ({missing} ShardKeyMissing fact); no entity activated"
    );

    let export = system.export().await;
    for partition in &export.partitions {
        println!(
            "   export: set {} key_field={} entities={:?}",
            partition.path, partition.key_field, partition.entities
        );
    }
    println!("partitions example complete");
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
