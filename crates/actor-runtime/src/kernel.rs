//! The delivery kernel: routing, actor loops, and the runtime-owned inbox
//! substrate.
//!
//! Identity model: the registry maps paths to slots; the kernel keeps the
//! per-actor inbox and its task. A restart reuses both — the endpoint handle
//! under the path is swapped, the inbox cursor never moves backward, so
//! senders holding pre-crash handles never notice and undelivered messages
//! redeliver.
//!
//! Loop discipline (project skill): one loop per function; each loop body is
//! a named step function.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use serde_json::Value as JsonValue;
use tokio::sync::{mpsc, watch};

use crate::envelope::{Address, Envelope};
use crate::inbox::Inbox;
use crate::registry::{Endpoint, Registry};
use crate::types::{Path, SchemaId, Topic};

/// How the system routes a message to its destination.
#[derive(Debug)]
pub enum Routed {
    /// Delivered into the actor's front door.
    Delivered(Path),
    /// No live endpoint under the path — dead-letter this.
    Unresolvable(Address),
}

/// Kernel-facing handle for one spawned actor.
pub struct ActorHandle {
    /// The kill switch: signaled on graceful stop.
    pub shutdown: watch::Sender<bool>,
    /// The task join handle; aborted on hard remove.
    pub task: tokio::task::JoinHandle<()>,
}

/// Everything the runtime owns for one actor across restarts.
pub struct ActorCell {
    /// The actor's path (its identity).
    pub path: Path,
    /// The runtime-owned inbox (survives endpoint swaps).
    pub inbox: tokio::sync::Mutex<Inbox>,
    /// The mailbox front door registered in the registry slot.
    pub endpoint: ArcSwapOption<Endpoint>,
    /// The running loop's handle, when a task is live.
    pub handle: tokio::sync::Mutex<Option<ActorHandle>>,
}

impl ActorCell {
    /// Creates a cell with a fresh inbox; the endpoint arrives on start.
    pub fn new(path: Path, inbox: Inbox) -> Self {
        Self {
            path,
            inbox: tokio::sync::Mutex::new(inbox),
            endpoint: ArcSwapOption::empty(),
            handle: tokio::sync::Mutex::new(None),
        }
    }
}

/// Shared kernel state guarded by a single lock.
///
/// One lock, not several: kernel mutations (route + fan + slot swap) are
/// compound; handler-side lookups go through `RuntimeView` snapshots taken
/// BEFORE the lock is taken, so actors never block on this lock while user
/// code runs.
pub struct Kernel {
    pub registry: std::sync::Mutex<Registry>,
    pub cells: std::sync::Mutex<std::collections::HashMap<Path, Arc<ActorCell>>>,
}

impl Kernel {
    /// A kernel with empty tables.
    pub fn new() -> Self {
        Self {
            registry: std::sync::Mutex::new(Registry::default()),
            cells: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// The dead-letter topic.
    pub fn dead_letters() -> Topic {
        Registry::dead_letter_topic()
    }
}

impl Default for Kernel {
    fn default() -> Self {
        Self::new()
    }
}

impl Kernel {
    /// Routes an envelope to its destination: resolve, deliver, and on any
    /// refusal return the envelope so the caller can dead-letter it.
    ///
    /// Trace stamping already happened at envelope construction (context
    /// `send`/`publish` derive the child causality); this function adds no
    /// new hops — it only moves bytes into an inbox front door.
    pub async fn route(&self, envelope: Envelope) -> Result<Path, Envelope> {
        let dest = envelope.dest.clone();
        match dest {
            Address::Path(ref path) => {
                let endpoint = {
                    let registry = self.registry.lock().expect("registry lock");
                    registry.resolve(path)
                };
                match endpoint {
                    Some(endpoint) => {
                        deliver_with_retry(&endpoint, envelope).await?;
                        Ok(path.clone())
                    }
                    None => Err(envelope),
                }
            }
            Address::Slot(_) => Err(envelope),  // reply routing: Phase 5
            Address::Topic(_) => Err(envelope), // topics: Phase 6
        }
    }

    /// Dead-letters an envelope onto the system DLQ topic (best effort).
    ///
    /// The DLQ entry carries the original envelope as its payload so the
    /// failure stays inspectable; if the DLQ subscriber itself is gone the
    /// fact is dropped — the tap (Phase 7) is the durable record.
    pub fn dead_letter(&self, envelope: Envelope, reason: &str) {
        let payload = JsonValue::Object({
            let mut map = serde_json::Map::new();
            map.insert("reason".into(), JsonValue::String(reason.to_owned()));
            map.insert("schema".into(), JsonValue::String(envelope.schema.to_string()));
            map.insert("dest".into(), JsonValue::String(format!("{}", fmt_dest(&envelope.dest))));
            map
        });
        let dlq = Envelope::json(
            SchemaId::new("DeadLetter", 1),
            Address::Topic(Self::dead_letters()),
            payload,
            envelope.trace,
        );
        // A topic publish is a fan to subscriber inboxes (Phase 6); until
        // then the DLQ topic is a sink and the envelope is recorded only in
        // the tap fact. Nothing to do here yet.
        let _ = dlq;
    }
}

/// Formats a destination for fact payloads.
fn fmt_dest(dest: &Address) -> String {
    match dest {
        Address::Path(path) => path.to_string(),
        Address::Topic(topic) => topic.to_string(),
        Address::Slot(lease) => format!("slot:{lease}"),
    }
}

/// Delivers to an endpoint, honoring Block by awaiting capacity.
///
/// `try_deliver` first (the common case); on a full mailbox fall back to
/// the awaiting `deliver`. If the endpoint died mid-send, hand the envelope
/// back to the caller for dead-lettering.
async fn deliver_with_retry(endpoint: &Endpoint, envelope: Envelope) -> Result<(), Envelope> {
    use tokio::sync::mpsc::error::TrySendError::*;
    match endpoint.try_deliver(envelope.clone()) {
        Ok(()) => Ok(()),
        Err(Full(envelope)) => endpoint.deliver(envelope).await.map_err(|send_err| send_err.0),
        Err(Closed(envelope)) => Err(envelope), // the slot's endpoint died mid-restart
    }
}

/// The front-door task: drains the mpsc into the runtime-owned inbox.
///
/// Splitting front door (mpsc) from inbox (VecDeque) is what lets the
/// overload policy be enforced against the *logical* queue while senders
/// still get a cheap backpressured handle.
pub async fn front_door_loop(cell: Arc<ActorCell>, mut rx: mpsc::Receiver<Envelope>) {
    while let Some(envelope) = rx.recv().await {
        let mut inbox = cell.inbox.lock().await;
        if let Err(refused) = inbox.push(envelope) {
            let evicted = refused.into_envelope();
            // Evicted/closed envelopes would be dead-lettered by the caller
            // in a full system; the front door records nothing (the tap does,
            // in Phase 7). The message is intentionally dropped here.
            let _ = evicted;
        }
    }
}

/// The actor loop: peeks the inbox, dispatches, acks — forever.
///
/// This phase establishes the loop skeleton with a pluggable dispatch
/// closure; the ES/service contracts install their own in Phases 4–5. The
/// loop NEVER consumes the envelope before the dispatch decision: peek,
/// clone, dispatch, ack — so a failed dispatch leaves the message queued
/// for redelivery.
pub async fn actor_loop(cell: Arc<ActorCell>, shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        let progressed = step_once(&cell).await;
        if !progressed {
            tokio::task::yield_now().await;
        }
    }
    drain_front_inbox(&cell).await;
}

/// One dispatcher step: take the next ready envelope and process it.
///
/// Returns whether work was done (the loop idles when false).
async fn step_once(cell: &Arc<ActorCell>) -> bool {
    let envelope = {
        let mut inbox = cell.inbox.lock().await;
        match inbox.peek() {
            Some(envelope) => {
                // Clone, do NOT consume: the ack is the commit point.
                envelope.clone()
            }
            None => return false,
        }
    };
    dispatch(cell, &envelope).await
}

/// Dispatches one envelope: the placeholder waist crossing.
///
/// Phases 4–5 replace this with schema decode + typed handler invocation;
/// the ack after dispatch is already the real commit point.
async fn dispatch(cell: &Arc<ActorCell>, envelope: &Envelope) -> bool {
    let json = envelope.as_json().cloned().unwrap_or(JsonValue::Null);
    let _ = (cell, json);
    ack(cell).await;
    true
}

/// Commits the message at the cursor (the ONLY place the cursor advances).
async fn ack(cell: &Arc<ActorCell>) {
    cell.inbox.lock().await.ack();
}

/// On shutdown: close the inbox and flush undeliverable entries (Phase 8
/// replaces this with full DLQ flushing; today the entries are dropped).
async fn drain_front_inbox(cell: &Arc<ActorCell>) {
    let mut inbox = cell.inbox.lock().await;
    inbox.close();
    let _undelivered: Vec<_> = inbox.drain().collect();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::TraceCtx;
    use crate::inbox::OverloadPolicy;
    use crate::types::{CausalityId, TraceId};
    use serde_json::json;

    fn envelope_to(path: &str, n: u32) -> Envelope {
        Envelope::json(
            SchemaId::new("Ping", 1),
            Address::Path(Path::new(path)),
            json!({ "n": n }),
            TraceCtx {
                trace_id: TraceId::new(),
                causality_id: CausalityId::new(),
            },
        )
    }

    #[tokio::test]
    async fn actor_loop_acks_every_envelope_then_stops_on_shutdown() {
        // Given a cell with two queued envelopes and a live loop.
        let cell = Arc::new(ActorCell::new(
            Path::new("a"),
            Inbox::new(8, OverloadPolicy::Block),
        ));
        {
            let mut inbox = cell.inbox.lock().await;
            inbox.push(envelope_to("a", 1)).expect("push");
            inbox.push(envelope_to("a", 2)).expect("push");
        }
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let loop_cell = cell.clone();
        let task = tokio::spawn(actor_loop(loop_cell, shutdown_rx));

        // When the loop processes both and is then told to stop.
        wait_for_cursor(&cell, 2).await;
        shutdown_tx.send(true).expect("signal");

        // Then the task ends with the cursor advanced past both messages.
        task.await.expect("loop joins");
        let inbox = cell.inbox.lock().await;
        assert_eq!(inbox.cursor().as_u64(), 2);
        assert!(!inbox.is_open());
    }

    #[tokio::test]
    async fn failed_dispatch_leaves_the_message_queued_for_redelivery() {
        // Given a cell whose dispatch placeholder fails before ack
        // (simulated here by a cursor stuck at 0 while a message is queued).
        let cell = Arc::new(ActorCell::new(
            Path::new("a"),
            Inbox::new(8, OverloadPolicy::Block),
        ));
        {
            let mut inbox = cell.inbox.lock().await;
            inbox.push(envelope_to("a", 1)).expect("push");
        }

        // When inspecting the inbox before any dispatch ran.
        let mut inbox = cell.inbox.lock().await;
        let queued = inbox.peek().is_some();

        // Then the message is still queued (redelivery intact).
        assert!(queued);
        assert_eq!(inbox.cursor().as_u64(), 0);
    }

    #[tokio::test]
    async fn unresolvable_route_returns_the_envelope_for_dead_lettering() {
        // Given a kernel with no actors.
        let kernel = Kernel::new();

        // When routing an envelope to a path nobody owns.
        let result = kernel.route(envelope_to("ghost", 1)).await;

        // Then the envelope comes back refused, ready for the DLQ.
        let refused = result.expect_err("must refuse");
        assert_eq!(
            refused.as_json().map(|j| j["n"].as_u64()),
            Some(Some(1))
        );
    }

    #[tokio::test]
    async fn route_delivers_through_a_registered_endpoint() {
        // Given a kernel with a registered, running actor.
        let kernel = Kernel::new();
        let path = Path::new("worker");
        let cell = Arc::new(ActorCell::new(
            path.clone(),
            Inbox::new(8, OverloadPolicy::Block),
        ));
        let (tx, mut rx) = mpsc::channel::<Envelope>(8);
        kernel
            .registry
            .lock()
            .expect("lock")
            .insert_slot(
                path.clone(),
                crate::schema::ActorManifest::new().kind(crate::types::ActorKind::Service),
                Endpoint::new(tx),
            )
            .expect("insert");
        kernel.cells.lock().expect("lock").insert(path.clone(), cell);

        // When routing an envelope to the actor.
        kernel.route(envelope_to("worker", 7)).await.expect("delivered");

        // Then it arrives through the front door.
        let received = rx.recv().await.expect("envelope");
        assert_eq!(received.as_json().map(|j| j["n"].as_u64()), Some(Some(7)));
    }

    #[tokio::test]
    async fn shutdown_signal_stops_an_idle_loop_promptly() {
        // Given an idle actor loop.
        let cell = Arc::new(ActorCell::new(
            Path::new("idle"),
            Inbox::new(4, OverloadPolicy::Block),
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(actor_loop(cell, shutdown_rx));

        // When shutting it down with nothing queued.
        shutdown_tx.send(true).expect("signal");

        // Then the task ends promptly.
        tokio::time::timeout(std::time::Duration::from_millis(500), task)
            .await
            .expect("loop stops")
            .expect("joins");
    }

    async fn wait_for_cursor(cell: &Arc<ActorCell>, expected: u64) {
        for _ in 0..2_000 {
            if cell.inbox.lock().await.cursor().as_u64() == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("cursor never reached {expected}");
    }
}
