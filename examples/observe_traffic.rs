//! One declaration, three sender verbs: `tell`, `send_to_any`, `publish`.
//!
//! The sender chooses topology; the receiver just processes. `Auditor`
//! declares `.handles::<Ship>()` — the one receive declaration — and
//! CANNOT tell whether a copy arrived as a tell, a one-of send, or a
//! publish fan-out:
//! - **`tell`** ("give this to that actor"): one copy to one path.
//! - **`send_to_any`** ("give this to ONE of the willing"): one copy to
//!   one handler of the schema, round-robin.
//! - **`publish`** ("announce this"): one copy to EVERY handler of the
//!   schema. Zero handlers ⇒ silent no-op: news, not work orders.
//!
//! Here `fulfillment` tells, a schema-addressed send races nothing (the
//! route table picks), and a `Shipped` announcement fans out to every
//! declarant — `billing` (work: invoice it) and the second observer
//! (news: record it) both receive copies from the SAME declaration.
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
#[derive(Command, Debug, Serialize, Deserialize)]
#[schema(description = "Fulfillment should ship the order.")]
struct Ship {
    order: String,
}

/// An event: the order WAS shipped (announcement).
#[derive(Event, Debug, Serialize, Deserialize)]
#[schema(description = "An order was shipped (announcement).")]
struct Shipped {
    order: String,
}

/// The primary: handles Ship commands and ANNOUNCES the outcome.
struct Fulfillment;

impl ServiceActor for Fulfillment {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }

    async fn start(_args: &Json) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<Ship> for Fulfillment {
    async fn handle(&mut self, msg: Ship, ctx: &mut MsgCtx<'_>) {
        log(format!("[fulfillment] shipped {}", msg.order));
        // The announcement: every handler of the EVENT gets a copy.
        ctx.publish(&Shipped { order: msg.order });
    }
}

/// The work consumer: billing declared `.handles::<Shipped>()` — a
/// published copy is indistinguishable from a sent one, and it treats
/// the announcement as WORK (invoice it).
struct Billing;

impl ServiceActor for Billing {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }

    async fn start(_args: &Json) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<Shipped> for Billing {
    async fn handle(&mut self, msg: Shipped, _ctx: &mut MsgCtx<'_>) {
        log(format!(
            "[billing] invoiced {} (from the announcement)",
            msg.order
        ));
    }
}

/// A second observer: the SAME declaration, different meaning — this
/// one treats the announcement as NEWS. News and work differ only at
/// the handler; the fabric carries both identically.
struct Auditor;

impl ServiceActor for Auditor {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }

    async fn start(_args: &Json) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<Shipped> for Auditor {
    async fn handle(&mut self, msg: Shipped, _ctx: &mut MsgCtx<'_>) {
        log(format!("[auditor] recorded Shipped({})", msg.order));
    }
}

#[tokio::main]
async fn main() {
    let system = ActorSystem::new(SystemConfig::production());

    // The primary (handles Ship), billing (work on Shipped), and a
    // second observer (news on Shipped) — all the same declaration.
    spawn_service_builder::<Fulfillment>(&system)
        .at(ActorPath::new("fulfillment"))
        .handles::<Ship>()
        .emits::<Shipped>()
        .start();
    spawn_service_builder::<Billing>(&system)
        .at(ActorPath::new("billing"))
        .handles::<Shipped>()
        .start();
    spawn_service_builder::<Auditor>(&system)
        .at(ActorPath::new("auditor"))
        .handles::<Shipped>()
        .start();

    // 1. A path-addressed Ship (the usual dispatch).
    system
        .tell(
            ActorPath::new("fulfillment"),
            Ship {
                order: "ord-1".into(),
            },
        )
        .await
        .expect("delivered");

    // 2. A schema-addressed Ship (the route table picks the handler).
    let env = Envelope::json(
        Ship::schema_id(),
        Address::Schema(Ship::schema_id()),
        serde_json::json!({ "order": "ord-2" }),
        TraceCtx::root(),
    );
    system.send(env).await.expect("schema delivery");

    // 3. A direct announcement from outside the system (no actor in
    //    the middle): billing AND the auditor each get one copy.
    system
        .publish(&Shipped {
            order: "ord-3".into(),
        })
        .await;

    // Let the deliveries drain, then tell the story.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    println!("--- traffic report ---");
    let lines = LINES
        .get()
        .map(|l| l.lock().expect("lines lock").clone())
        .unwrap_or_default();
    let shipped_by_fulfillment = lines.iter().filter(|l| l.contains("fulfillment")).count();
    let announcements = lines.iter().filter(|l| l.contains("billing")).count();
    let news = lines.iter().filter(|l| l.contains("auditor")).count();
    println!("  Ship commands dispatched to fulfillment: {shipped_by_fulfillment}");
    println!("  Shipped copies invoiced by billing (work): {announcements}");
    println!("  Shipped copies recorded by the auditor (news): {news}");
    assert_eq!(shipped_by_fulfillment, 2, "commands went to the handler");
    assert_eq!(
        announcements, 3,
        "billing invoiced both shipment announcements + the direct one"
    );
    assert_eq!(news, 3, "the auditor saw every Shipped copy too");
    println!(
        "\nOne declaration (.handles); tell/send_to_any/publish are sender choices the\nreceiver cannot observe."
    );
}
