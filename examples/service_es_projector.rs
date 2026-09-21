//! Service → ES → projector: the same impure edge, now with HISTORY.
//!
//! Identical workflow to `service_projector.rs` — one difference: the
//! service TELLS its state change to a tiny ES LOGGER entity instead of
//! (only) publishing. The entity journals the fact properly, so the
//! full history exists in the store BEFORE any projector is spawned.
//!
//! The projector consumes the entity's recorded event and gets:
//! - full historical replay (catch-up folds everything ever logged);
//! - restart durability from its own re-records (same as always);
//! - one total fold order (ingest_seq) over entity-recorded facts.
//!
//! This is the ATM-shaped composition: service = impure edge, entity =
//! durable fact record, projector = view. The rule of thumb: if the
//! history of a fact matters, an ES actor must be at (or behind) its
//! origin — a projector can only replay what was ever persisted.
//!
//! Run: `cargo run --example service_es_projector`

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use trouper::actor::{CommandHandler, EventSourcedActor, MsgHandler, ServiceActor};
use trouper::prelude::*;
use trouper::registry::RegistryError;

// ---- The impure edge (same pool as service_projector.rs) -----------

#[derive(Command, Debug, Serialize, Deserialize)]
#[schema(description = "Take a connection from the pool.")]
struct Checkout;

#[derive(Command, Debug, Serialize, Deserialize)]
#[schema(description = "Return a connection to the pool.")]
struct Checkin;

/// The service TELLS the pool log entity (an ES actor) about each
/// transition. No publish here — the ENTITY records the fact, and its
/// journal is the durable origin. (It could ALSO publish; the projector
/// below subscribes to the recorded event either way.)
struct PoolService;

impl ServiceActor for PoolService {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }
    async fn start(_args: &Json) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self)
    }
}

static TAKEN: AtomicU64 = AtomicU64::new(0);
static RETURNED: AtomicU64 = AtomicU64::new(0);

impl MsgHandler<Checkout> for PoolService {
    async fn handle(&mut self, _msg: Checkout, ctx: &mut MsgCtx<'_>) {
        let taken = TAKEN.fetch_add(1, Ordering::SeqCst) + 1;
        let _returned = RETURNED.load(Ordering::SeqCst);
        println!("[pool] checkout #{taken}");
        // The durable hop: tell the LOGGER ENTITY its command; the entity
        // records the PoolEvent fact (append-before-ack) — that journal
        // entry IS the durable history.
        ctx.send(Address::Path(ActorPath::new("pool.log")), &Checkout, None);
    }
}

impl MsgHandler<Checkin> for PoolService {
    async fn handle(&mut self, _msg: Checkin, ctx: &mut MsgCtx<'_>) {
        let returned = RETURNED.fetch_add(1, Ordering::SeqCst) + 1;
        let _taken = TAKEN.load(Ordering::SeqCst);
        println!("[pool] checkin #{returned}");
        ctx.send(Address::Path(ActorPath::new("pool.log")), &Checkin, None);
    }
}

// ---- The durable origin: a tiny ES logger --------------------------

/// The fact, as recorded by the ENTITY (kind: Event — it is a fact
/// about the world the logger observed and journaled).
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "A pool transition, journaled.")]
struct PoolEvent {
    taken: u64,
    returned: u64,
}

/// The logger entity: handle the command, record the fact. Its journal
/// (append-before-ack) is what makes history exist.
#[derive(Debug, Serialize, Deserialize, Default)]
struct PoolLog;
impl CommandHandler<Checkout> for PoolLog {
    fn handle(&self, _cmd: Checkout, _ctx: &mut CmdCtx<'_>) -> Events {
        let mut ev = Events::new();
        ev.push_event(PoolEvent {
            taken: TAKEN.load(Ordering::SeqCst),
            returned: RETURNED.load(Ordering::SeqCst),
        });
        ev
    }
}
impl CommandHandler<Checkin> for PoolLog {
    fn handle(&self, _cmd: Checkin, _ctx: &mut CmdCtx<'_>) -> Events {
        let mut ev = Events::new();
        ev.push_event(PoolEvent {
            taken: TAKEN.load(Ordering::SeqCst),
            returned: RETURNED.load(Ordering::SeqCst),
        });
        ev
    }
}
impl EventSourcedActor for PoolLog {
    fn restore(_args: &Json) -> Self {
        Self
    }
    fn apply(&mut self, _event: &Event) {
        // Counters live in the service; the log's fold is trivial. The
        // journal entries ARE the product here.
    }
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles_id(Checkout::schema_id())
            .handles_id(Checkin::schema_id())
            .emits_id(PoolEvent::schema_id())
    }
}

// ---- The read model: now historically complete ---------------------

#[derive(Default, Debug, Serialize, Deserialize)]
struct PoolView {
    taken: u64,
    returned: u64,
}
impl Projector for PoolView {
    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "PoolEvent@1" {
            self.taken = event.payload["taken"].as_u64().unwrap_or(self.taken);
            self.returned = event.payload["returned"].as_u64().unwrap_or(self.returned);
        }
    }
}

#[tokio::main]
async fn main() {
    let system = ActorSystem::new(SystemConfig::production());
    system.register_schema::<Checkout>();
    system.register_schema::<Checkin>();
    system.register_schema::<PoolEvent>();

    // The durable origin: the logger entity (here: a standalone ES actor
    // under a fixed path — a partition set would do for many pools).
    spawn_es_builder::<PoolLog>(&system)
        .at(ActorPath::new("pool.log"))
        .args(json!({}))
        .handles::<Checkout>()
        .handles::<Checkin>()
        .start();

    // The service: TELLS the logger (tell = the durable hop).
    spawn_service_builder::<PoolService>(&system)
        .at(ActorPath::new("pool"))
        .handles::<Checkout>()
        .handles::<Checkin>()
        .emits::<Checkout>() // what the service SENDS onward (the durable hop)
        .emits::<Checkin>()
        .start();

    // ---- Traffic BEFORE the projector exists — and this time it survives.
    for _ in 0..3 {
        system
            .tell(ActorPath::new("pool"), Checkout)
            .await
            .expect("told");
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("\n3 checkouts happened BEFORE the projector went live — journaled by the entity.");

    // ---- The projector spawns LATE and catches up over full history.
    // The scan finds every PoolEvent in the logger's journal (they are
    // origin-recorded), seeds the projector's journal, folds them.
    spawn_projector_builder::<PoolView>(&system)
        .at(ActorPath::new("proj/pool"))
        .consumes::<PoolEvent>()
        .start_and_catchup()
        .await; // returns after catch-up — which replayed ALL history

    let view = system
        .projector_state(&ActorPath::new("proj/pool"))
        .await
        .expect("live projector");
    println!("pool view right after spawn: {view:?}");
    println!(
        "  ^ taken=3, returned=0: HISTORY PRESENT. The entity's journal\n    was the origin of truth; catch-up folded all of it.\n"
    );

    // ---- Live tail continues.
    system
        .tell(ActorPath::new("pool"), Checkout)
        .await
        .expect("told");
    system
        .tell(ActorPath::new("pool"), Checkin)
        .await
        .expect("told");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let view = system
        .projector_state(&ActorPath::new("proj/pool"))
        .await
        .expect("live projector");
    println!("pool view after live traffic: {view:?}\n");

    // ---- Restart: replay from the projector's own journal.
    system.stop(&ActorPath::new("proj/pool")).await;
    spawn_projector_builder::<PoolView>(&system)
        .at(ActorPath::new("proj/pool"))
        .consumes::<PoolEvent>()
        .start_and_catchup()
        .await;
    let view = system
        .projector_state(&ActorPath::new("proj/pool"))
        .await
        .expect("restarted projector");
    println!("pool view after restart: {view:?}");
    println!(
        "  ^ taken=4, returned=1 survive restart (own-journal replay),\n    and the 3 pre-spawn events are IN there too — unlike\n    service_projector, where history before the projector is gone."
    );

    println!(
        "\nTHE RULE: history requires an ES actor at (or behind) the origin.\n\
         Service → projector = live only; Service → ES → projector = live + all history."
    );
}
