//! Service → projector: the impure edge feeding a read model directly.
//!
//! A SERVICE actor (async handlers, I/O, no journal) publishes facts with
//! `ctx.publish`; a PROJECTOR declares `.consumes::<Fact>()` and folds
//! them into a read model. No entity, no journal, no command — the
//! service IS the origin of the facts.
//!
//! THE HISTORY CONTRACT (read this twice): a service actor has NO
//! JOURNAL. Its published facts are fire-and-forget on the wire:
//! - published BEFORE the projector declared consumption → zero handlers
//!   → silent no-op → GONE FOREVER;
//! - published AFTER → re-recorded in the projector's journal, which
//!   makes them durable from then on (restarts replay them).
//!
//! So a projector over service facts is eventually-complete from its
//! spawn moment, never historically complete. Catch-up can only replay
//! what was ever persisted — which is nothing until the projector first
//! went live. If you need history, see `service_es_projector.rs`:
//! journal the origin as an ES event, then project over THAT.
//!
//! Run: `cargo run --example service_projector`

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use trouper::actor::{MsgHandler, Projector, ServiceActor};
use trouper::prelude::*;
use trouper::registry::RegistryError;

// ---- The impure edge: a connection-pool service --------------------

/// The fact the pool publishes when a connection is taken or returned.
/// kind: Event — it is news for whomever handles it. The service is the
/// ORIGIN: there is no entity journaling this anywhere (that is the
/// point of this example).
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "The pool's live counters changed.")]
struct PoolDrained {
    taken: u64,
    returned: u64,
}

/// The service: real work happens in an async handler (here: a global
/// atomic stands in for the actual pool). It ANNOUNCES its state change
/// as a fact — it never answers asks about it, never stores it anywhere
/// durable.
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
        let returned = RETURNED.load(Ordering::SeqCst);
        println!("[pool] checkout #{taken}");
        ctx.publish(&PoolDrained { taken, returned });
    }
}

impl MsgHandler<Checkin> for PoolService {
    async fn handle(&mut self, _msg: Checkin, ctx: &mut MsgCtx<'_>) {
        let returned = RETURNED.fetch_add(1, Ordering::SeqCst) + 1;
        let taken = TAKEN.load(Ordering::SeqCst);
        println!("[pool] checkin #{returned}");
        ctx.publish(&PoolDrained { taken, returned });
    }
}

#[derive(Command, Debug, Serialize, Deserialize, Clone)]
#[schema(description = "Take a connection from the pool.")]
struct Checkout;

#[derive(Command, Debug, Serialize, Deserialize, Clone)]
#[schema(description = "Return a connection to the pool.")]
struct Checkin;

// ---- The read model: live pool counters ----------------------------

/// The projection: current taken/returned. It can NEVER know how many
/// checkouts happened before it went live — those facts were not
/// persisted by anyone.
#[derive(Default, Debug, Serialize, Deserialize)]
struct PoolView {
    taken: u64,
    returned: u64,
}
impl Projector for PoolView {
    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "PoolDrained" {
            self.taken = event.payload_json()["taken"].as_u64().unwrap_or(self.taken);
            self.returned = event.payload_json()["returned"].as_u64().unwrap_or(self.returned);
        }
    }
}

#[tokio::main]
async fn main() {
    let system = ActorSystem::new(SystemConfig::production());
    system.register_schema::<Checkout>();
    system.register_schema::<Checkin>();
    system.register_schema::<PoolDrained>();

    // ---- The HISTORY LOSS, demonstrated ----------------------------
    // Traffic BEFORE the projector exists: the publishes have zero
    // handlers → silent no-op. Not lost by accident — lost by contract.
    spawn_service_builder::<PoolService>(&system)
        .at(ActorPath::new("pool"))
        .handles::<Checkout>()
        .handles::<Checkin>()
        .emits::<PoolDrained>() // the outbound declaration, enforced at flush
        .start();

    for _ in 0..3 {
        system
            .tell(ActorPath::new("pool"), Checkout)
            .await
            .expect("told");
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("\n3 checkouts happened BEFORE the projector went live.");

    // The projector comes up NOW. Its journal is empty; the scan finds
    // nothing (the service never journaled anything); catch-up folds
    // zero facts. CaughtUp{seeded: 0} is the proof.
    spawn_projector_builder::<PoolView>(&system)
        .at(ActorPath::new("proj/pool"))
        .consumes::<PoolDrained>()
        .start_and_catchup()
        .await; // returns after catch-up — which had nothing to replay

    let view = system
        .projector_state(&ActorPath::new("proj/pool"))
        .await
        .expect("live projector");
    println!("pool view right after spawn: {view:?}");
    println!("  ^ taken=0, returned=0: the 3 pre-spawn checkouts are GONE\n");

    // ---- Live tail: everything from here on arrives and STICKS -----
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
    println!("pool view after live traffic: {view:?}");
    println!("  ^ taken=4, returned=1: only POST-spawn facts are known\n");

    // ---- Restarts work — over the projector's OWN journal ----------
    // The re-records made post-spawn facts durable: stop the projector,
    // re-run its builder, and its journal replays taken=4/returned=1.
    // (What a restart can NOT resurrect: the 3 pre-spawn checkouts.
    // They were never persisted by anyone.)
    system.stop(&ActorPath::new("proj/pool")).await;
    spawn_projector_builder::<PoolView>(&system)
        .at(ActorPath::new("proj/pool"))
        .consumes::<PoolDrained>()
        .start_and_catchup()
        .await;
    let view = system
        .projector_state(&ActorPath::new("proj/pool"))
        .await
        .expect("restarted projector");
    println!("pool view after restart: {view:?}");
    println!("  ^ taken=4, returned=1 survive — replayed from the projector's own journal");

    println!(
        "\nTHE RULE: service facts are wire-only until a consumer re-records them.\n\
         A projector over service facts sees only what happened after it went live.\n\
         Need history? Put an ES actor at the origin (see service_es_projector)."
    );
}
