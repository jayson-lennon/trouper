//! Event announcements vs command dispatch: `publish`/`subscribe` vs
//! `handles`/`tell`.
//!
//! Two transports, two questions:
//! - **Commands** ("who should DO this?"): `handles::<M>()` + `tell`/
//!   schema-addressed send. Point-to-point; a second handler for the
//!   same schema makes the route round-robin.
//! - **Events** ("who wants to HEAR about this?"): the sender calls
//!   `ctx.publish(&msg)` (or `system.publish(&msg)` from outside);
//!   every actor that declared `.subscribe::<M>()` receives a copy —
//!   insertion order, no round-robin. Zero subscribers ⇒ no-op: events
//!   are news, not work orders.
//!
//! The two declaration tables are disjoint: `billing` handles `Shipped`
//! AND subscribes to it (both deliver), while `auditor` subscribes to
//! `Ship` without handling it (never a dispatch target — the command
//! goes only to `fulfillment`).
//!
//! Run: `cargo run --example observe_traffic`

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

/// A command: fulfillment SHOULD ship the order.
#[derive(Debug, Serialize, Deserialize)]
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
            description: Some("Fulfillment should ship the order.".into()),
        }
    }
}

/// An event: the order WAS shipped (announcement).
#[derive(Debug, Serialize, Deserialize)]
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
        // The announcement: every subscriber of the EVENT gets a copy.
        ctx.publish(&Shipped { order: msg.order });
    }
}

/// The announcement consumer: billing handles AND subscribes to
/// `Shipped` — the tables are independent, so it receives both the
/// broadcasts and (point-to-point) sends.
struct Billing;

impl ServiceActor for Billing {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
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

/// The audit subscriber: subscribes to the Ship COMMAND without
/// handling it — it observes traffic and is never a dispatch target.
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

    // The primary (handles Ship), the auditor (subscribes to Ship
    // traffic), and billing (subscribes to the Shipped announcement).
    spawn_service_builder::<Fulfillment>(&system)
        .at(ActorPath::new("fulfillment"))
        .handles::<Ship>()
        .start();
    spawn_service_builder::<Auditor>(&system)
        .at(ActorPath::new("auditor"))
        .subscribe::<Ship>()
        .start();
    spawn_service_builder::<Billing>(&system)
        .at(ActorPath::new("billing"))
        .subscribe::<Shipped>()
        .start();

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

    // 2. A schema-addressed Ship (the kernel picks the handler). The
    //    command still routes ONLY to fulfillment — subscribers are
    //    never dispatch targets.
    let env = Envelope::json(
        Ship::schema_id(),
        Address::Schema(Ship::schema_id()),
        serde_json::json!({ "order": "ord-2" }),
        TraceCtx::root(),
    );
    system.send(env).await.expect("schema delivery");

    // 3. A direct announcement from outside the system (no actor in
    //    the middle): billing gets it, fulfillment does not.
    system
        .publish(&Shipped {
            order: "ord-3".into(),
        })
        .await;

    // Let the broadcasts drain, then tell the story.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    println!("--- traffic report ---");
    let lines = LINES
        .get()
        .map(|l| l.lock().expect("lines lock").clone())
        .unwrap_or_default();
    let shipped_by_fulfillment = lines
        .iter()
        .filter(|l| l.contains("fulfillment"))
        .count();
    let ships_seen_by_auditor = lines.iter().filter(|l| l.contains("auditor")).count();
    let announcements = lines.iter().filter(|l| l.contains("billing")).count();
    println!("  Ship commands dispatched to fulfillment: {shipped_by_fulfillment}");
    println!("  Ship commands observed by the auditor: {ships_seen_by_auditor}");
    println!("  Shipped announcements invoiced by billing: {announcements}");
    assert_eq!(shipped_by_fulfillment, 2, "commands went to the handler");
    assert_eq!(ships_seen_by_auditor, 2, "the auditor saw BOTH Ship events");
    assert_eq!(
        announcements, 3,
        "billing invoiced both shipments + the direct announcement"
    );
    println!("\npublish/subscribe = events to every subscriber; handles/tell = commands to one handler.");
}
