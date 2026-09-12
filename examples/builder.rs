//! The builder API: typed and foreign spawns where each type is said
//! exactly once.
//!
//! - Typed: `spawn_es_builder::<Inventory>()` with `.handles::<C>()` /
//!   `.emits::<E>()` / `.emits_on_topic()` / `.snapshot()` / `.mailbox()`.
//!   The builder constructs the erased adapters internally.
//! - Foreign: `spawn_foreign()` with named `handle`/`apply` closures —
//!   the same event-sourced contract with no Rust actor types.
//!
//! Run: `cargo run --example builder`

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::Level;
use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::prelude::*;
use trouper::system::SnapshotCadence;

// -- A typed event-sourced actor -------------------------------------------

#[derive(Deserialize)]
struct Restock {
    sku: String,
    qty: i64,
}

impl Schema for Restock {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Restock".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![
                FieldDef::required("sku", FieldTy::Str),
                FieldDef::required("qty", FieldTy::Int),
            ],
            description: Some("Add stock".into()),
        }
    }
}

#[derive(Deserialize)]
struct Restocked {
    #[allow(dead_code)] // folded via raw payload
    sku: String,
    #[allow(dead_code)] // folded via raw payload
    qty: i64,
}

impl Schema for Restocked {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Restocked".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("sku", FieldTy::Str),
                FieldDef::required("qty", FieldTy::Int),
            ],
            description: None,
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
struct Inventory {
    total: i64,
}

impl EventSourcedActor for Inventory {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::EventSourced)
    }

    fn restore(_args: &serde_json::Value) -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "Restocked@1" {
            self.total += event.payload["qty"].as_i64().unwrap_or(0);
        }
    }
}

impl CommandHandler<Restock> for Inventory {
    fn handle(&self, cmd: Restock, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        vec![Event::new(
            Restocked::schema_id(),
            json!({ "sku": cmd.sku, "qty": cmd.qty }),
        )]
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::ERROR)
        .init();
    let system = ActorSystem::new(SystemConfig::production());

    // -- Typed spawn: the builder wires schema edges + adapters ------------
    println!("== typed builder ==");
    let warehouse = trouper::builder::spawn_es_builder::<Inventory>(&system)
        .at(ActorPath::new("warehouse"))
        .args(json!({}))
        .handles::<Restock>()
        .emits::<Restocked>()
        .emits_on_topic(Topic::new("inventory.events"))
        .snapshot(SnapshotCadence::Messages(50))
        .mailbox(64, trouper::inbox::OverloadPolicy::Block)
        .start();
    println!("   spawned {warehouse}");

    system
        .send(system.envelope(
            Restock::schema_id(),
            ActorPath::new("warehouse"),
            json!({ "sku": "widget", "qty": 5 }),
        ))
        .await
        .expect("delivered");
    wait(|| async {
        system
            .es_state(&ActorPath::new("warehouse"))
            .await
            .and_then(|s| s["total"].as_i64())
            == Some(5)
    })
    .await;
    println!(
        "   state after one restock: {}",
        system
            .es_state(&ActorPath::new("warehouse"))
            .await
            .expect("state")
    );

    // -- Foreign spawn: named handle/apply closures, JSON only -------------
    println!("== foreign builder ==");
    let tally = trouper::builder::spawn_foreign(&system)
        .at(ActorPath::new("tally"))
        .schema(json!({
            "name": "TallyAdd", "version": 1, "kind": "command",
            "fields": [{ "name": "delta", "ty": "int" }]
        }))
        .args(json!({ "total": 0 }))
        .handle(Arc::new(|_state, cmd, _ctx| {
            vec![Event::new(
                SchemaId::new("TallyAdded", 1),
                json!({ "delta": cmd["delta"].as_i64().unwrap_or(0) }),
            )]
        }))
        .apply(Arc::new(|state: &mut serde_json::Value, ev: &Event| {
            state["total"] = json!(
                state["total"].as_i64().unwrap_or(0) + ev.payload["delta"].as_i64().unwrap_or(0)
            );
        }))
        .emits_id(SchemaId::new("TallyAdded", 1))
        .start()
        .expect("foreign spawn");
    println!("   spawned {tally}");

    system
        .send(system.envelope(
            SchemaId::new("TallyAdd", 1),
            ActorPath::new("tally"),
            json!({ "delta": 9 }),
        ))
        .await
        .expect("delivered");
    wait(|| async {
        system.es_state(&ActorPath::new("tally")).await == Some(json!({ "total": 9 }))
    })
    .await;
    println!(
        "   foreign state after one add: {}",
        system
            .es_state(&ActorPath::new("tally"))
            .await
            .expect("state")
    );

    // Both spawns went through the same kernel funnel — one export shows
    // the declared edges for both.
    let export = system.export().await;
    println!(
        "   declared edges: {}; warehouse emits enforced from the builder's .emits()",
        export.declared_edges.len()
    );
    println!("builder example complete");
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
