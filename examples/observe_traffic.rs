//! Observing message traffic: `.observes` vs topic announcements.
//!
//! Two tools, two questions:
//! - **Announcement** ("who wants to hear about this?"): the sender
//!   publishes an event to a topic; subscribers get copies. Here:
//!   `fulfillment` publishes `Shipped` to `orders`; `billing`
//!   subscribes.
//! - **Observation** ("what actually flowed?"): an actor declares
//!   `.observes::<M>()` and receives a copy of every routed delivery
//!   of `M` — commands, asks, whatever traffic exists — without the
//!   sender knowing or caring. Here: `auditor` watches every `Ship`
//!   command, including the schema-addressed one.
//!
//! Run: `cargo run --example observe_traffic`

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, Mutex};
use trouper::actor::{MsgHandler, ServiceActor};
use trouper::prelude::*;
use trouper::registry::{Registry, RegistryError};

static LINES: std::sync::OnceLock<Mutex<Vec<String>>> = std::sync::OnceLock::new();

fn log(line: String) {
    println!("  {line}");
    LINES.get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("lines lock")
        .push(line);
}

/// A command: `fulfillment` handles it (point-to-point work).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ship {
    order: String,
}

impl Schema for Ship {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Ship".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("order", FieldTy::Str)],
            description: Some("Ship an order (command traffic).".into()),
        }
    }
}

/// An event: the announcement that shipping happened (published).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Shipped {
    order: String,
}

impl Schema for Shipped {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Shipped".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("order", FieldTy::Str)],
            description: Some("An order was shipped (announcement).".into()),
        }
    }
}

/// The primary: handles Ship commands and ANNOUNCES the outcome.
struct Fulfillment;

impl ServiceActor for Fulfillment {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }

    async fn start(
        _args: &serde_json::Value,
    ) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<Ship> for Fulfillment {
    async fn handle(&mut self, msg: Ship, ctx: &mut MsgCtx<'_>) {
        log(format!("[fulfillment] shipped {}", msg.order));
        // The announcement: whoever cares subscribes to `orders`.
        ctx.publish(Topic::new("orders"), &Shipped { order: msg.order });
    }
}

/// The announcement consumer: billing subscribes to the topic.
struct Billing;

impl ServiceActor for Billing {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<Shipped>()
            .kind(ActorKind::Service)
    }

    async fn start(
        _args: &serde_json::Value,
    ) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<Shipped> for Billing {
    async fn handle(&mut self, msg: Shipped, _ctx: &mut MsgCtx<'_>) {
        log(format!("[billing] invoiced {} (from the announcement)", msg.order));
    }
}

/// The traffic observer: the auditor sees every Ship COMMAND that
/// flows — no sender cooperation needed.
struct Auditor;

impl ServiceActor for Auditor {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }

    async fn start(
        _args: &serde_json::Value,
    ) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<Ship> for Auditor {
    async fn handle(&mut self, msg: Ship, _ctx: &mut MsgCtx<'_>) {
        log(format!("[auditor] recorded Ship({}) traffic", msg.order));
    }
}

#[tokio::main]
async fn main() {
    let system = ActorSystem::new(SystemConfig::production());

    // The primary (handles Ship), the auditor (observes Ship traffic),
    // and billing (subscribes to the Shipped announcement).
    spawn_service_builder::<Fulfillment>(&system)
        .at(ActorPath::new("fulfillment"))
        .handles::<Ship>()
        .start();
    spawn_service_builder::<Auditor>(&system)
        .at(ActorPath::new("auditor"))
        .observes::<Ship>()
        .start();
    let billing = spawn_service_builder::<Billing>(&system)
        .at(ActorPath::new("billing"))
        .handles::<Shipped>()
        .start();
    system
        .subscribe(&billing, &Topic::new("orders"), None)
        .expect("billing subscribes to orders");

    // 1. A path-addressed Ship command (the usual dispatch).
    system
        .tell(
            ActorPath::new("fulfillment"),
            Ship {
                order: "ord-1".into(),
            },
        )
        .await
        .expect("delivered");

    // 2. A schema-addressed Ship (the kernel picks the handler) — the
    //    auditor sees this too, though nobody announced it.
    let env = Envelope::json(
        Ship::schema_id(),
        Address::Schema(Ship::schema_id()),
        json!({ "order": "ord-2" }),
        TraceCtx::root(),
    );
    system.send(env).await.expect("schema delivery");

    // Let the topic pumps drain, then tell the story.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    println!("--- traffic report ---");
    let lines = LINES.get().map(|l| l.lock().expect("lines lock").clone()).unwrap_or_default();
    let ships = lines.iter().filter(|l| l.contains("auditor")).count();
    let announcements = lines.iter().filter(|l| l.contains("billing")).count();
    println!("  Ship commands observed by the auditor: {ships}");
    println!("  Shipped announcements invoiced by billing: {announcements}");
    assert_eq!(ships, 2, "the auditor saw BOTH Ship commands");
    assert_eq!(announcements, 2, "billing invoiced both announcements");
    println!("\nobserve = copies of the traffic; topics = the announcements.");
}
