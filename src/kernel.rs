//! The delivery kernel: actor cells, front doors, routing, and the ES
//! atomic step.
//!
//! Identity model: the registry maps paths to slots; the system keeps the
//! per-actor cell (inbox + endpoint + handle). A restart swaps the endpoint
//! under the path — the inbox and its cursor persist, so senders holding
//! pre-crash handles never notice and undelivered messages redeliver.
//!
//! The atomic step (spec, exact order): peek → find entry → build ctx →
//! catch_unwind dispatch → journal.append → inbox.ack → apply → outbox
//! flush → emit fan-out → maybe snapshot. NO user code runs after ack;
//! steps after it are infallible kernel code, so the append+ack pair is
//! atomic in practice. A panicking handler leaves nothing appended, nothing
//! acked: the message stays queued for redelivery and the poisoned state is
//! never reused (restart rebuilds from the journal).
//!
//! Loop discipline (project skill): one loop per function; loop bodies are
//! named step functions.

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use serde_json::Value as JsonValue;
use tokio::sync::{Notify, mpsc, watch};

use serde::{Deserialize, Serialize};

use crate::actor::{ActorPath, CommandEntry, DynEsActor, DynServiceActor, MsgEntry};
use crate::context::{CmdCtx, Outbox, RuntimeView};
use crate::envelope::{Address, Envelope, TraceCtx};
use crate::inbox::Inbox;
use crate::journal::{JournalEntry, JournalError};
use crate::registry::{Endpoint, Registry};
use crate::schema::SchemaId;

/// An envelope the runtime could not deliver or decode.
///
/// Kept inspectable — dropped messages must stay observable, never silently
/// vanish. `drain_dead_letters` hands retained envelopes to the host; the
/// tap facts are the live observation surface.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    /// The undeliverable payload's schema.
    pub schema: SchemaId,
    /// Where it was headed.
    pub dest: Address,
    /// Why it died.
    pub reason: crate::kernel::DeadLetterReason,
    /// A human-readable detail line (context beyond the reason).
    pub detail: String,
    /// The trace of the hop that failed.
    pub trace: TraceCtx,
    /// The full envelope, retained for host inspection / deliberate resend.
    pub envelope: Envelope,
}

/// An ask lifecycle event (tap facts from Phase 7 read these).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AskOutcome {
    /// The callee replied in time.
    Replied,
    /// The timeout elapsed with no reply.
    Timeout,
    /// The ask failed outright (callee dead, slot lost).
    Failed,
}

#[derive(Debug, Clone)]
pub(crate) struct AskFact {
    /// Whether this opened or settled an ask.
    #[allow(dead_code)] // ledger completeness; only `outcome` is asserted
    pub(crate) opened: bool,
    /// The settled outcome (None while open).
    #[allow(dead_code)] // asserted by tests, projected via FactKind in prod
    pub(crate) outcome: Option<AskOutcome>,
    /// The callee's address.
    #[allow(dead_code)] // ledger completeness
    pub(crate) dest: Address,
    /// The ask's trace.
    #[allow(dead_code)] // ledger completeness
    pub(crate) trace: TraceCtx,
}

/// Actor tables beyond the registry: cells, journals, live ES state,
/// command entries, snapshot policies, crashes, and dead letters.
///
/// Guarded by one lock — these mutate together (spawn inserts into every
/// table; restart swaps state + endpoint as one observation).
pub(crate) struct KernelState {
    pub(crate) cells: HashMap<ActorPath, Arc<ActorCell>>,
    /// Where every actor's journal lives (the in-memory store by
    /// default; a test/backend store rides the same trait).
    pub(crate) journal_store: Arc<dyn crate::journal::JournalStore>,
    /// Per-path snapshot-cadence bookkeeping (the time anchor is policy
    /// state, not storage — it stays kernel-side, off the store).
    pub(crate) snapshot_cadence_ms: HashMap<ActorPath, Option<u64>>,
    pub(crate) es_state: HashMap<ActorPath, Arc<tokio::sync::Mutex<Box<dyn DynEsActor>>>>,
    pub(crate) entries: HashMap<ActorPath, Vec<Arc<dyn CommandEntry>>>,
    pub(crate) snapshot_policy: HashMap<ActorPath, crate::actor::SnapshotCadence>,
    /// Live service instances (service actors are not journaled).
    pub(crate) services: HashMap<ActorPath, Arc<tokio::sync::Mutex<Box<dyn DynServiceActor>>>>,
    /// Reply-slot leases (the mechanism half of reply addresses).
    pub(crate) replies: crate::reply::ReplyTable,
    /// Ask lifecycle facts (the tap consumes these in Phase 7).
    pub(crate) ask_facts: Vec<AskFact>,
    /// Per-actor async message dispatch entries.
    pub(crate) msg_entries: HashMap<ActorPath, Vec<Arc<dyn MsgEntry>>>,
    /// Spawn args (genesis rebuild needs them at restart time).
    pub(crate) genesis_args: HashMap<ActorPath, JsonValue>,
    /// Paths whose loop died to a handler panic (awaiting supervision).
    pub(crate) crashed: HashSet<ActorPath>,
    /// Envelopes that could not be delivered or decoded.
    pub(crate) dead_letters: Vec<DeadLetter>,
    /// The global observation ring (drop-oldest).
    pub(crate) tap: crate::tap::TapRing,
    /// Supervised children: path → spec.
    pub(crate) specs: HashMap<ActorPath, crate::supervision::ActorSpec>,
    /// Sliding-window failure records: path → window.
    pub(crate) failures: HashMap<ActorPath, crate::supervision::FailureWindow>,
    /// Per-actor backpressure watermarks: path → (high watermark, fired).
    /// `fired` latches the up-crossing (down-crossings re-arm it), so a
    /// sustained overload produces ONE fact, not one per message.
    pub(crate) watermarks: HashMap<ActorPath, (u64, bool)>,
    /// Per-actor passivation config: path → idle window. The companion
    /// `last_work_ms` map carries the injected-clock stamp of the last
    /// completed step (kernel-side bookkeeping, off the journal store).
    pub(crate) passivation: HashMap<ActorPath, crate::system::Passivation>,
    /// Injected-clock millis of each actor's last completed message step.
    pub(crate) last_work_ms: HashMap<ActorPath, u64>,
    /// The graceful-shutdown barrier: set by the sweep, read on every
    /// route (deliveries dead-letter with `ShuttingDown`), by partition
    /// activation (refused), by the supervision engines (suspended), and
    /// by passivation (stands down).
    pub(crate) shutting_down: std::sync::atomic::AtomicBool,
}

impl Default for KernelState {
    fn default() -> Self {
        Self::with_tap_capacity(4096)
    }
}

impl KernelState {
    /// A fresh state with a tap ring of the given capacity.
    pub fn with_tap_capacity(tap_capacity: usize) -> Self {
        Self {
            cells: HashMap::new(),
            journal_store: Arc::new(crate::journal::InMemoryJournalStore::new()),
            snapshot_cadence_ms: HashMap::new(),
            es_state: HashMap::new(),
            entries: HashMap::new(),
            snapshot_policy: HashMap::new(),
            services: HashMap::new(),
            replies: crate::reply::ReplyTable::default(),
            ask_facts: Vec::new(),
            msg_entries: HashMap::new(),
            genesis_args: HashMap::new(),
            crashed: HashSet::new(),
            dead_letters: Vec::new(),
            tap: crate::tap::TapRing::new(tap_capacity),
            specs: HashMap::new(),
            failures: HashMap::new(),
            watermarks: HashMap::new(),
            passivation: HashMap::new(),
            last_work_ms: HashMap::new(),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl KernelState {
    /// Records one fact to the tap ring: the sole observation surface
    /// (drop-oldest; gaps appear when the RING drops). Fact offsets make
    /// any loss visible.
    pub fn record_fact(&mut self, ts: crate::clock::Timestamp, kind: crate::tap::FactKind) {
        self.tap.push(ts, kind);
    }
}

/// Kernel-facing handle for one running actor loop.
pub(crate) struct ActorHandle {
    /// The kill switch: signaled on graceful stop.
    pub(crate) shutdown: watch::Sender<bool>,
    /// The task join handle; aborted on hard remove.
    pub(crate) task: Option<tokio::task::JoinHandle<()>>,
}

/// Everything the runtime owns for one actor across restarts.
pub(crate) struct ActorCell {
    /// The actor's path (its identity).
    pub(crate) path: ActorPath,
    /// The runtime-owned inbox (survives endpoint swaps).
    pub(crate) inbox: tokio::sync::Mutex<Inbox>,
    /// The running loop's handle, when a task is live.
    pub(crate) handle: tokio::sync::Mutex<Option<ActorHandle>>,
    /// Wakes the actor loop when work arrives (latency optimization; the
    /// loop's poll backstop is the correctness guarantee).
    pub(crate) work: Arc<Notify>,
    /// Whether the actor's `on_stop` hook has run. Claimed by whichever
    /// caller reaches the graceful exit FIRST (the loop's self-stop/
    /// passivation exit, or the external stop after joining the task) —
    /// the hook runs exactly once per actor lifetime, never twice.
    pub(crate) on_stop_done: std::sync::atomic::AtomicBool,
}

impl ActorCell {
    /// Creates a cell with a fresh inbox; the endpoint arrives on start.
    pub fn new(path: ActorPath, inbox: Inbox) -> Self {
        Self {
            path,
            inbox: tokio::sync::Mutex::new(inbox),
            handle: tokio::sync::Mutex::new(None),
            work: Arc::new(Notify::new()),
            on_stop_done: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Claims the right to run `on_stop`: `true` = this caller runs the
    /// hook; `false` = someone else already ran it.
    pub(crate) fn claim_on_stop(&self) -> bool {
        !self
            .on_stop_done
            .swap(true, std::sync::atomic::Ordering::SeqCst)
    }
}

/// Everything one running ES loop needs; cloned per spawn/restart.
#[derive(Clone)]
pub(crate) struct EsLoop {
    /// The actor's path.
    pub(crate) path: ActorPath,
    /// The actor's cell (inbox + front door).
    pub(crate) cell: Arc<ActorCell>,
    /// The shared routing table.
    pub(crate) registry: Arc<Mutex<Registry>>,
    /// The shared actor tables.
    pub(crate) kernel: Arc<Mutex<KernelState>>,
    /// The read-only view handed to handler contexts.
    pub(crate) view: Arc<dyn RuntimeView>,
    /// The injected clock (lease expiries, deterministic tests).
    pub(crate) clock: crate::clock::ClockService,
}

impl EsLoop {
    /// Spawns the front door + the ES loop for this actor, returning the
    /// loop task's join handle (the external stop and the shutdown sweep
    /// join it: a bounded wait for the current message, then teardown).
    pub fn start_tracked(
        self,
        rx: mpsc::Receiver<Envelope>,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let front_cell = self.cell.clone();
        let front_kernel = self.kernel.clone();
        tokio::spawn(front_door_loop(front_cell, front_kernel, rx));
        tokio::spawn(es_actor_loop(self, shutdown))
    }
}

/// Routes an envelope through the registry to its destination.
///
/// Returns the delivered path, or the envelope back for dead-lettering
/// when the destination does not resolve.
pub(crate) async fn route(
    registry: &Mutex<Registry>,
    kernel: &Mutex<KernelState>,
    envelope: Envelope,
) -> Result<ActorPath, Envelope> {
    route_inner(registry, kernel, envelope).await
}

async fn route_inner(
    registry: &Mutex<Registry>,
    kernel: &Mutex<KernelState>,
    envelope: Envelope,
) -> Result<ActorPath, Envelope> {
    // SHUTDOWN BARRIER: once the sweep starts, nothing new is accepted —
    // every delivery dead-letters with `ShuttingDown` (observably, never
    // silently). Activation is refused inside resolve_partition.
    if kernel
        .lock()
        .shutting_down
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        dead_letter(
            kernel,
            &envelope,
            crate::kernel::DeadLetterReason::ShuttingDown,
            "graceful shutdown sweep in progress",
        );
        return Err(envelope);
    }
    let dest = envelope.dest.clone();
    match dest {
        Address::Path(ref path) => {
            // RULES first: a matching rule places an observer relative to
            // the flow (Tee copies with a linked causality; Inline
            // interposes the observer in the primary's place).
            let (delivery, primary_dest) = {
                let reg = registry.lock();
                apply_rules(&reg, &envelope, path.clone())
            };
            if let Some(tee) = delivery {
                // Tee: deliver the copy BEFORE the primary (same position
                // in the flow, at-most-once). The copy carries a NEW
                // causality id under the ORIGINAL's trace id — the Sent
                // fact keeps the original trace so the two deliveries
                // link observably without looking like a two-hop chain.
                let (mut copy, tee_dest, origin_trace) = tee;
                copy.trace.trace_id = origin_trace.trace_id;
                copy.trace.causality_id = crate::envelope::CausalityId::new();
                let endpoint = {
                    let reg = registry.lock();
                    reg.resolve(&tee_dest)
                };
                if let Some(endpoint) = endpoint {
                    let _ = deliver_with_retry(&endpoint, copy).await;
                    let mut kernel_table = kernel.lock();
                    kernel_table.record_fact(
                        origin_trace.causality_id.as_millis_ts(),
                        crate::tap::FactKind::Sent {
                            from: envelope.from.clone(),
                            dest: Address::Path(tee_dest.clone()),
                            schema: envelope.schema.clone(),
                            trace: origin_trace,
                        },
                    );
                }
                // No tee endpoint → the copy is silently dropped: a tee is
                // best-effort by contract (never blocks the primary flow).
            }
            let path = match primary_dest {
                Some(interposed) => {
                    // Inline: the envelope's primary delivery goes to the
                    // interposer, which owns forwarding.
                    interposed
                }
                None => path.clone(),
            };
            // PARTITION SETS: a public set path resolves to ONE entity,
            // derived from the payload's shard key (activated on demand).
            let path = match resolve_partition(registry, kernel, &envelope, path.clone()).await {
                Ok(Some(entity)) => entity,
                Ok(None) => path,
                Err(missing_key_envelope) => {
                    // Key absent/unextractable: dead-letter, no activation.
                    dead_letter(
                        kernel,
                        &missing_key_envelope,
                        crate::kernel::DeadLetterReason::ShardKeyMissing,
                        "partition command without its shard key",
                    );
                    return Ok(path);
                }
            };
            let endpoint = {
                let registry = registry.lock();
                registry.resolve(&path)
            };
            let endpoint = match endpoint {
                Some(endpoint) => endpoint,
                None => return Err(envelope),
            };
            // The activation fast-path can lose a race against the
            // entity's passivation drain (the inbox closes between
            // `resolve_partition` and this delivery): the refused
            // envelope RETRIES the whole arm once — this time the entity
            // is absent, so it is re-activated from the factory and the
            // journal replays. Converges: the second attempt cannot hit
            // a second drain (a just-activated actor is not idle).
            if deliver_with_retry(&endpoint, envelope.clone())
                .await
                .is_err()
            {
                let retry_path =
                    match resolve_partition(registry, kernel, &envelope, path.clone()).await {
                        Ok(Some(entity)) => entity,
                        _ => path.clone(),
                    };
                let retry_endpoint = {
                    let registry = registry.lock();
                    registry.resolve(&retry_path)
                };
                let Some(retry_endpoint) = retry_endpoint else {
                    return Err(envelope);
                };
                deliver_with_retry(&retry_endpoint, envelope.clone()).await?;
                return Ok(retry_path);
            }
            {
                let mut kernel = kernel.lock();
                kernel.record_fact(
                    envelope.trace.causality_id.as_millis_ts(),
                    crate::tap::FactKind::Sent {
                        from: envelope.from.clone(),
                        dest: Address::Path(path.clone()),
                        schema: envelope.schema.clone(),
                        trace: envelope.trace,
                    },
                );
            }
            Ok(path)
        }
        Address::Slot(_) => Err(envelope), // reply routing: ctx only
        Address::Schema(ref schema) => {
            // Schema-addressed send: the route table picks the handler
            // (rotating when several actors handle the same schema).
            let (target, endpoint) = {
                let mut registry = registry.lock();
                match registry.route(schema) {
                    Some(target) => {
                        let endpoint = registry.resolve(&target);
                        (target, endpoint)
                    }
                    None => return Err(envelope),
                }
            };
            let Some(endpoint) = endpoint else {
                return Err(envelope);
            };
            deliver_with_retry(&endpoint, envelope.clone()).await?;
            {
                let mut kernel = kernel.lock();
                kernel.record_fact(
                    envelope.trace.causality_id.as_millis_ts(),
                    crate::tap::FactKind::Sent {
                        from: envelope.from.clone(),
                        dest: Address::Schema(schema.clone()),
                        schema: schema.clone(),
                        trace: envelope.trace,
                    },
                );
            }
            Ok(target)
        }
    }
}

/// Resolves a partition-set destination to its entity path.
///
/// `Ok(None)` = the dest is not a partition set (fall through). `Ok(Some)`
/// = the entity path (activated on demand if absent). `Err(envelope)` =
/// the command lacked its shard key — dead-lettered, NEVER activated.
///
/// Determinism is structural: the entity path is `public/key`, so the same
/// key always reaches the same entity and journal. Activation is
/// check-then-insert under the registry lock: the loser of a concurrent
/// same-key race delivers to the winner's entity.
async fn resolve_partition(
    registry: &Mutex<Registry>,
    kernel: &Mutex<KernelState>,
    envelope: &Envelope,
    dest: ActorPath,
) -> Result<Option<ActorPath>, Envelope> {
    let spec = {
        let reg = registry.lock();
        let Some(spec) = reg.partitions.get(&dest) else {
            return Ok(None);
        };
        spec.clone()
    };
    // Schema-aware key extraction from the payload (schema lock scoped).
    let key = {
        let reg = registry.lock();
        let payload = envelope.as_json().cloned().unwrap_or(JsonValue::Null);
        match reg.schema(&envelope.schema) {
            Some(def) => crate::pool::extract_shard_key(def, &spec.key_field, &payload),
            None => payload
                .get(&spec.key_field)
                .and_then(|v| v.as_str().map(str::to_owned)),
        }
    };
    let Some(key) = key else {
        // The schema declares the key required; arriving here is a
        // contract break (or an unregistered foreign sender).
        return Err(envelope.clone());
    };
    // Determinism is structural: same key → same derived path.
    let entity_path = ActorPath::new(format!("{}/{}", dest, key).as_str());
    // Fast path: the entity is already live.
    if registry.lock().lookup(&entity_path).is_some() {
        return Ok(Some(entity_path));
    }
    // The sweep disables activation: nothing new may start mid-shutdown.
    if kernel
        .lock()
        .shutting_down
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        return Err(envelope.clone());
    } // ACTIVATE: spawn the entity from the shared factory. The factory's
    // spawn registers the entity's slot; a concurrent same-key send is
    // serialized by the registry lock inside the spawn, and the loser of
    // a race delivers to the winner's entity (same derived path).
    (spec.factory)(&spec.system, &entity_path, &spec.entity_args(&key));
    Ok(Some(entity_path))
}

/// Applies the first matching router rule to a path-addressed envelope.
///
/// Returns `(tee, inline_dest)`:
/// - `tee`: `Some((copy, observer, origin_trace))` when a Tee rule matched —
///   the copy carries a NEW causality id whose trace links to the original
///   (two deliveries of one message never look like a chain of two hops).
/// - `inline_dest`: `Some(interposer)` when an Inline rule matched — the
///   primary envelope is delivered to the interposer in the original's place.
///
/// First match wins (declaration order is priority order).
fn apply_rules(
    registry: &Registry,
    envelope: &Envelope,
    dest: ActorPath,
) -> (
    Option<(Envelope, ActorPath, crate::envelope::TraceCtx)>,
    Option<ActorPath>,
) {
    let mut tee = None;
    let mut inline = None;
    for rule in &registry.rules {
        if let Some(src) = &rule.source
            && envelope.from.as_ref() != Some(src)
        {
            continue;
        }
        if let Some(schema) = &rule.schema
            && envelope.schema != *schema
        {
            continue;
        }
        if let Some(rule_dest) = &rule.dest
            && *rule_dest != dest
        {
            continue;
        }
        match &rule.action {
            crate::pool::RuleAction::Tee(observer) => {
                let origin_trace = envelope.trace;
                let mut trace = origin_trace;
                // New causality for the copy, same trace id: a fresh cause
                // INSIDE the original's trace, never a chain of two hops.
                trace.causality_id = crate::envelope::CausalityId::new();
                let Some(payload) = envelope.as_json().cloned() else {
                    return (tee, inline); // typed payload: not teeable at the waist
                };
                let copy = Envelope::json(
                    envelope.schema.clone(),
                    Address::Path(observer.clone()),
                    payload,
                    trace,
                )
                .from(
                    envelope
                        .from
                        .clone()
                        .unwrap_or_else(|| ActorPath::new("anonymous")),
                );
                tee = Some((copy, observer.clone(), origin_trace));
            }
            crate::pool::RuleAction::Inline(interposer) => {
                inline = Some(interposer.clone());
            }
        }
        break; // first match wins
    }
    (tee, inline)
}

/// Dead-letters an envelope into the kernel's inspectable record.
///
/// `detail` is the human-readable elaboration (e.g. the decode error);
/// `reason` is the typed category.
pub(crate) fn dead_letter(
    kernel: &Mutex<KernelState>,
    envelope: &Envelope,
    reason: crate::kernel::DeadLetterReason,
    detail: &str,
) {
    let mut kernel = kernel.lock();
    kernel.dead_letters.push(DeadLetter {
        schema: envelope.schema.clone(),
        dest: envelope.dest.clone(),
        reason: reason.clone(),
        detail: detail.to_owned(),
        trace: envelope.trace,
        envelope: envelope.clone(),
    });
    kernel.record_fact(
        envelope.trace.causality_id.as_millis_ts(),
        crate::tap::FactKind::DeadLettered {
            dest: envelope.dest.clone(),
            schema: envelope.schema.clone(),
            reason,
            trace: envelope.trace,
        },
    );
}

/// Delivers to an endpoint, honoring Block by awaiting capacity.
async fn deliver_with_retry(endpoint: &Endpoint, envelope: Envelope) -> Result<(), Envelope> {
    use tokio::sync::mpsc::error::TrySendError::*;
    match endpoint.try_deliver(envelope.clone()) {
        Ok(()) => Ok(()),
        Err(Full(envelope)) => endpoint
            .deliver(envelope)
            .await
            .map_err(|send_err| send_err.0),
        Err(Closed(envelope)) => Err(envelope), // the slot's endpoint died mid-restart
    }
}

/// The front-door task: drains the mpsc into the runtime-owned inbox.
///
/// Only DropOld/DropNew refusals land here (the mpsc already backpressures
/// Block); refused/evicted messages are dead-lettered — never lost silently.
pub(crate) async fn front_door_loop(
    cell: Arc<ActorCell>,
    kernel: Arc<Mutex<KernelState>>,
    mut rx: mpsc::Receiver<Envelope>,
) {
    while let Some(envelope) = rx.recv().await {
        let accepted = {
            let mut inbox = cell.inbox.lock().await;
            match inbox.push(envelope.clone()) {
                Ok(_) => true,
                Err(refused) => {
                    if !refused.queued_anyway() {
                        dead_letter(
                            &kernel,
                            &envelope,
                            crate::kernel::DeadLetterReason::InboxRefused,
                            "inbox refused (overload/closed)",
                        );
                    } else {
                        let evicted = refused.into_envelope();
                        dead_letter(
                            &kernel,
                            &evicted,
                            crate::kernel::DeadLetterReason::InboxRefused,
                            "inbox evicted oldest (DropOld)",
                        );
                    }
                    false
                }
            }
        };
        // WATERMARK CHECK (rate-limited): fires on the UP-crossing only;
        // the latch re-arms when the depth falls back to/below the mark.
        let depth = cell.inbox.lock().await.len() as u64;
        let mut kernel_state = kernel.lock();
        if let Some((wm, fired)) = kernel_state.watermarks.get_mut(&cell.path) {
            if depth > *wm && !*fired {
                *fired = true;
                kernel_state.record_fact(
                    envelope.trace.causality_id.as_millis_ts(),
                    crate::tap::FactKind::Backpressured {
                        path: cell.path.clone(),
                        depth,
                    },
                );
            } else if depth <= *wm && *fired {
                *fired = false;
            }
        }
        drop(kernel_state);
        if accepted {
            cell.work.notify_one();
        }
    }
}

/// The ES actor loop: the atomic step, forever, until shutdown or crash.
///
/// Idles with a notify + short poll backstop; the poll is deliberate — it
/// bounds wakeup latency without lost-wakeup races.
pub(crate) async fn es_actor_loop(loop_ctx: EsLoop, mut shutdown: watch::Receiver<bool>) {
    // SPAWN-TIME RECOVERY: a re-activated entity (partition re-spawn,
    // or any spawn onto a journaled path) replays its journal before the
    // first step — passivation is lossless for the ES tier. A fresh
    // genesis spawn has no journal; the store answers None.
    recover_at_boot(&loop_ctx).await;
    loop {
        if *shutdown.borrow_and_update() {
            break;
        }
        match step_es(&loop_ctx).await {
            Step::Work => {
                stamp_work(&loop_ctx);
                continue;
            }
            Step::Idle => {
                // Idle window: the time-based snapshot cadence is checked
                // here (the 20ms poll arm below is the wake), never
                // mid-step — snapshots stay BETWEEN messages.
                maybe_snapshot_on_idle(&loop_ctx).await;
                maybe_passivate(&loop_ctx).await;
            }
            Step::Crashed => break, // supervisor (Phase 8) takes over
            Step::Stop => {
                loop_ctx
                    .graceful_exit(crate::actor::StopReason::Normal)
                    .await;
                break;
            }
        }
        let notified = loop_ctx.cell.work.notified();
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }
    }
    drain_inbox_on_stop(&loop_ctx).await;
}

/// Boot-time journal recovery for an ES actor: rebuilds the live state
/// from the store's replay (snapshot + tail) when this path has a
/// journal. Runs BEFORE the first step; no command is processed
/// unrecovered.
async fn recover_at_boot(ctx: &EsLoop) {
    let replay = {
        let store = ctx.kernel.lock().journal_store.clone();
        match store.load(&ctx.path).await {
            Ok(r) => r,
            Err(_) => return,
        }
    };
    let Some(r) = replay else { return };
    let (snapshot_state, tail) = (
        r.snapshot.as_ref().and_then(|e| match e {
            JournalEntry::Snapshot { state, .. } => Some(state.clone()),
            _ => None,
        }),
        r.tail.clone(),
    );
    if snapshot_state.is_none() && tail.is_empty() {
        return;
    }
    let old = {
        let kernel = ctx.kernel.lock();
        kernel.es_state.get(&ctx.path).cloned()
    };
    let Some(old) = old else { return };
    let genesis_args = {
        let kernel = ctx.kernel.lock();
        kernel
            .genesis_args
            .get(&ctx.path)
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}))
    };
    let fresh = {
        let old = old.lock().await;
        old.rebuild(&genesis_args, snapshot_state, &tail).ok()
    };
    if let Some(fresh) = fresh {
        let mut kernel = ctx.kernel.lock();
        kernel
            .es_state
            .insert(ctx.path.clone(), Arc::new(tokio::sync::Mutex::new(fresh)));
    }
}

/// What one atomic step concluded.
enum Step {
    /// A message was committed (or dead-lettered); loop continues.
    Work,
    /// No message ready; the loop may idle.
    Idle,
    /// The handler panicked; the loop must stop (state is poisoned).
    Crashed,
    /// The handler recorded `stop_self` and the message committed; the
    /// loop must exit through the GRACEFUL path (on_stop + teardown).
    Stop,
}

/// THE ATOMIC STEP — spec order, no deviations:
/// peek → find entry → build ctx → catch_unwind dispatch (decide only)
/// → journal.append → inbox.ack → apply → outbox flush → emit fan-out
/// → maybe snapshot.
async fn step_es(ctx: &EsLoop) -> Step {
    // 1. PEEK (clone, never consume: the ack is the commit point).
    let envelope = {
        let mut inbox = ctx.cell.inbox.lock().await;
        inbox.peek().cloned()
    };
    let Some(envelope) = envelope else {
        return Step::Idle;
    };
    {
        let mut kernel = ctx.kernel.lock();
        kernel.record_fact(
            envelope.trace.causality_id.as_millis_ts(),
            crate::tap::FactKind::Delivered {
                to: ctx.path.clone(),
                schema: envelope.schema.clone(),
                trace: envelope.trace,
            },
        );
    }

    // 2. FIND the command entry for this schema.
    let entry = {
        let kernel = ctx.kernel.lock();
        kernel.entries.get(&ctx.path).and_then(|entries| {
            entries
                .iter()
                .find(|e| e.schema() == envelope.schema)
                .cloned()
        })
    };
    let Some(entry) = entry else {
        // Unknown schema: dead-letter and ADVANCE the cursor (the message
        // can never be handled; redelivering it would be futile).
        dead_letter(
            &ctx.kernel,
            &envelope,
            crate::kernel::DeadLetterReason::UnknownSchema,
            "no entry for this schema",
        );
        ctx.cell.inbox.lock().await.ack();
        return Step::Work;
    };

    // 3+4. Build ctx, dispatch under catch_unwind. DECIDE ONLY: no state
    // mutation, no journal write, no ack inside the handler.
    let mut outbox = Outbox::new();
    let dispatch_result = {
        let state = ctx.state().await;
        let mut state = state.lock().await;
        let payload = envelope
            .as_json()
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let mut cmd_ctx = CmdCtx::new(
            &ctx.path,
            &envelope.trace,
            envelope.reply_to.as_ref(),
            ctx.view.as_ref(),
            &mut outbox,
        );

        std::panic::catch_unwind(AssertUnwindSafe(|| {
            entry.dispatch(state.as_mut(), &payload, &mut cmd_ctx)
        }))
    };

    let events = match dispatch_result {
        Ok(Ok(events)) => events,
        Ok(Err(report)) => {
            // Decode failure: dead-letter and advance (kernel bug only if
            // the schema registry and adapter disagree).
            let reason = format!("{report}");
            dead_letter(
                &ctx.kernel,
                &envelope,
                crate::kernel::DeadLetterReason::Decode,
                &reason,
            );
            ctx.cell.inbox.lock().await.ack();
            return Step::Work;
        }
        Err(poison) => {
            // PANIC: nothing appended, nothing acked, outbox discarded.
            // The state may be poisoned — mark crashed and stop; the
            // supervisor rebuilds from the journal (never reuses `state`).
            {
                let mut kernel = ctx.kernel.lock();
                kernel.crashed.insert(ctx.path.clone());
                kernel.record_fact(
                    envelope.trace.causality_id.as_millis_ts(),
                    crate::tap::FactKind::Failed {
                        path: ctx.path.clone(),
                        error: "handler panic".to_owned(),
                    },
                );
            }
            let _ = poison;
            return Step::Crashed;
        }
    };

    // 4.5 EMIT FILTER (declaration enforcement, PRE-append). The declared
    // surface is the only surface: events whose schema the actor never
    // declared are dropped here — never journalled, never applied — with a
    // DeadLettered fact + tracing error as the observable record. The step
    // CONTINUES with the declared remainder: dropping is a state-consistent
    // outcome (apply runs per appended event), while failing the step would
    // burn restart budget on a static condition redelivery can never heal.
    let declared = {
        let registry = ctx.registry.lock();
        registry
            .lookup(&ctx.path)
            .map(|info| info.manifest.emits)
            .unwrap_or_default()
    };
    let events: Vec<crate::envelope::Event> = events
        .into_iter()
        .filter(|event| {
            let is_declared = declared.contains(&event.schema);
            if !is_declared {
                tracing::error!(
                    actor = %ctx.path,
                    schema = %event.schema,
                    "undeclared event dropped before journal append"
                );
                // The dropped EVENT is what died: it is dead-lettered as an
                // envelope addressed back to the emitting actor (same trace,
                // so the drop stays causally linked to the command).
                let dropped = Envelope::json(
                    event.schema.clone(),
                    crate::envelope::Address::Path(ctx.path.clone()),
                    event.payload.clone(),
                    envelope.trace,
                )
                .from(ctx.path.clone());
                dead_letter(
                    &ctx.kernel,
                    &dropped,
                    crate::kernel::DeadLetterReason::UndeclaredEvent,
                    "emitted undeclared schema (dropped before journal append)",
                );
            }
            is_declared
        })
        .collect();

    // 5. JOURNAL APPEND (durable record first). The store is awaited
    // OUTSIDE the kernel sync guard — a write-through backend gets
    // "never ack what isn't journaled"; a failure aborts the step BEFORE
    // the ack (the message stays queued; supervision treats it as a crash).
    // NOTE: the declared/undeclared `envelope` binding above was consumed
    // by the filter; the events own their traces now.
    let seqs = {
        let store = ctx.kernel.lock().journal_store.clone();
        let seqs = store.append(&ctx.path, &events).await.map_err(|report| {
            tracing::error!(actor = %ctx.path, error = ?report, "journal append failed");
            let mut kernel = ctx.kernel.lock();
            kernel.crashed.insert(ctx.path.clone());
        });
        match seqs {
            Ok(seqs) => seqs,
            Err(()) => return Step::Crashed,
        }
    };

    // 6. ACK (the commit point: this message will never redeliver).
    ctx.cell.inbox.lock().await.ack();
    {
        let mut kernel = ctx.kernel.lock();
        kernel.record_fact(
            envelope.trace.causality_id.as_millis_ts(),
            crate::tap::FactKind::Acked {
                to: ctx.path.clone(),
                schema: envelope.schema.clone(),
                trace: envelope.trace,
            },
        );
    }

    // 7. APPLY (the same fold replay uses; state may now lag the journal
    // only if the process dies before this line — rebuild covers that).
    {
        let state = ctx.state().await;
        let mut state = state.lock().await;
        for event in &events {
            state.apply_erased(event);
        }
    }

    // 8. OUTBOX FLUSH (deferred sends/replies, causality-linked). A
    // StopSelf intent concludes the step with Step::Stop — sends recorded
    // before it have already flushed (in-order).
    let stop_self = flush_outbox(ctx, outbox).await;

    // 9. EMIT FAN-OUT (recorded, declared facts broadcast to every actor
    // that declared .handles — the named step exists so the order never
    // changes).
    fan_out_emits(ctx, &events).await;

    // 10. MAYBE SNAPSHOT (policy-gated, BETWEEN messages).
    maybe_snapshot(ctx, seqs).await;

    if stop_self { Step::Stop } else { Step::Work }
}

impl EsLoop {
    /// The live state shell.
    async fn state(&self) -> Arc<tokio::sync::Mutex<Box<dyn DynEsActor>>> {
        let kernel = self.kernel.lock();
        kernel
            .es_state
            .get(&self.path)
            .cloned()
            .expect("es state present for a running loop")
    }

    /// The loop's own graceful exit: run the actor's `on_stop` hook (the
    /// instance is still alive here — NOT a crash), then hand the table
    /// teardown to the system facade. The system race is safe: the
    /// teardown is idempotent (see `ActorSystemCore::teardown_tables`).
    ///
    /// The external `stop()` path never calls this — it joins the loop
    /// task instead. Calling it from inside the loop is exactly what makes
    /// self-stop and passivation deadlock-free: no task joins itself.
    pub(crate) async fn graceful_exit(&self, reason: crate::actor::StopReason) {
        let claimed = self.cell.claim_on_stop();
        if claimed {
            run_on_stop(self).await;
        }
        let system = crate::system::ActorSystemCore::loop_facade(self);
        system.teardown_tables(&self.path, reason).await;
    }
}

/// Runs the actor's `on_stop` hook, if its tier has a live instance.
///
/// Service actors: async hook with a real `MsgCtx` (sends/publishes
/// recorded there are flushed after the hook returns). ES actors: sync
/// hook over the final folded state. A poisoned (crashed) instance never
/// reaches this — the loop breaks before the graceful path, and the
/// external stop path only hooks when the instance survives.
pub(crate) async fn run_on_stop(ctx: &EsLoop) {
    // Service tier first: the live instance is behind its own mutex, the
    // hook gets a MsgCtx over a throwaway outbox flushed on completion.
    // STANDS DOWN during the shutdown sweep: the sweep's parallel drain
    // runs every hook itself (through the same claim), so a loop still
    // alive under the barrier must not double-run or hang on a dead port.
    let service = {
        let kernel = ctx.kernel.lock();
        if kernel
            .shutting_down
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        kernel.services.get(&ctx.path).cloned()
    };
    if let Some(service) = service {
        let ask_port = KernelAskPort {
            registry: ctx.registry.clone(),
            kernel: ctx.kernel.clone(),
            clock: ctx.clock.clone(),
        };
        let mut outbox = Outbox::new();
        let trace = crate::envelope::TraceCtx::root();
        {
            let mut svc = service.lock().await;
            let mut msg_ctx = crate::context::MsgCtx::new(
                &ctx.path,
                &trace,
                None,
                ctx.view.as_ref(),
                &mut outbox,
                Some(&ask_port),
            );
            svc.on_stop_erased(&mut msg_ctx).await;
        }
        let _ = flush_outbox(ctx, outbox).await;
        return;
    }
    // ES tier: sync hook over the folded state.
    let state = {
        let kernel = ctx.kernel.lock();
        kernel.es_state.get(&ctx.path).cloned()
    };
    if let Some(state) = state {
        let state = state.lock().await;
        state.on_stop_es();
    }
}

/// The kernel's ask port: opens leases, routes request envelopes,
/// records ask facts. Handed to service contexts at dispatch time.
#[derive(Clone)]
pub(crate) struct KernelAskPort {
    /// The shared routing table.
    pub(crate) registry: Arc<Mutex<Registry>>,
    /// The shared actor tables (reply leases + ask facts live here).
    pub(crate) kernel: Arc<Mutex<KernelState>>,
    /// The clock for lease expiries.
    pub(crate) clock: crate::clock::ClockService,
}

impl crate::context::AskPort for KernelAskPort {
    fn ask_channel(
        &self,
        dest: Address,
        schema: SchemaId,
        payload: JsonValue,
        ttl: std::time::Duration,
    ) -> std::pin::Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (
                            crate::reply::LeaseId,
                            tokio::sync::oneshot::Receiver<JsonValue>,
                        ),
                        error_stack::Report<crate::context::AskError>,
                    >,
                > + Send,
        >,
    > {
        let registry = self.registry.clone();
        let kernel = self.kernel.clone();
        let clock = self.clock.clone();
        Box::pin(async move {
            // ENTITIES DO NOT ANSWER ASKS: a request aimed at an
            // event-sourced actor fails fast with the named error —
            // before any lease opens, before any AskOpened fact — so the
            // asker learns the contract violation immediately instead of
            // burning its mandatory timeout. Consumers of entities LISTEN
            // FOR THE FACT (a declared `.handles` on the entity's emits).
            if let Address::Path(path) = &dest
                && registry.lock().lookup(path).map(|i| i.kind)
                    == Some(crate::actor::ActorKind::EventSourced)
            {
                return Err(error_stack::Report::new(
                    crate::context::AskError::Unresolved(
                        "event-sourced actors do not answer asks — listen for the fact".to_owned(),
                    ),
                ));
            }
            // Resolve the destination FIRST: an unresolvable ask fails fast.
            let endpoint = {
                let Address::Path(path) = &dest else {
                    return Err(error_stack::Report::new(
                        crate::context::AskError::Unresolved(format!("{dest:?}")),
                    ));
                };
                registry.lock().resolve(path)
            };
            let Some(endpoint) = endpoint else {
                return Err(error_stack::Report::new(
                    crate::context::AskError::Unresolved(format!("{dest:?}")),
                ));
            };

            // Open the lease and route the request envelope.
            let now = clock.now();
            let (lease, receiver) = {
                let mut kernel = kernel.lock();
                let trace = crate::envelope::TraceCtx::root();
                let (lease, receiver) = kernel.replies.open(ttl, now);
                kernel.ask_facts.push(AskFact {
                    opened: true,
                    outcome: None,
                    dest: dest.clone(),
                    trace,
                });
                kernel.record_fact(
                    now,
                    crate::tap::FactKind::AskOpened {
                        from: ActorPath::new("anonymous"),
                        dest: dest.clone(),
                        trace,
                    },
                );
                (lease, receiver)
            };
            let trace = crate::envelope::TraceCtx::root();
            let envelope =
                Envelope::json(schema, dest.clone(), payload, trace).reply_to(Address::Slot(lease));
            match deliver_with_retry(&endpoint, envelope).await {
                Ok(()) => Ok((lease, receiver)),
                Err(_) => Err(error_stack::Report::new(
                    crate::context::AskError::Unresolved(format!("{dest:?}")),
                )),
            }
        })
    }

    fn ask_settled(
        &self,
        lease: crate::reply::LeaseId,
        dest: Address,
        outcome: AskOutcome,
        trace: TraceCtx,
    ) {
        let mut kernel = self.kernel.lock();
        kernel.ask_facts.push(AskFact {
            opened: false,
            outcome: Some(outcome.clone()),
            dest: dest.clone(),
            trace,
        });
        kernel.record_fact(
            trace.causality_id.as_millis_ts(),
            crate::tap::FactKind::AskSettled { outcome, trace },
        );
        let _ = dest;
        // Drop the lease: settled (consumed) or timed out (late replies
        // land nowhere). The reply's `complete` already removed it on the
        // Replied path; removal here is idempotent.
        kernel.replies.cancel(&lease);
    }
}

/// Broadcasts one published event to EVERY handler of its schema.
///
/// Publish is a sender verb over the one route table: every actor that
/// declared `.handles::<M>()` receives exactly one copy, in registration
/// order — no round-robin, no dead-lettering. A handler whose endpoint is
/// closed (stopped or mid-restart) is skipped silently: events are news,
/// not work orders, and one gone reader never fails the others. Zero
/// handlers ⇒ a silent no-op (one Sent fact, no deliveries, no DLQ).
///
/// Because handlers declared `.handles`, the dispatch entry exists on
/// their side too — a published copy dispatches exactly like a told one.
///
/// Lock discipline: one acquisition to snapshot the (path, endpoint)
/// pairs, deliveries outside the locks. The route cursor is untouched:
/// publishes never disturb one-of send rotation.
pub(crate) async fn broadcast(
    registry: &Mutex<Registry>,
    kernel: &Mutex<KernelState>,
    schema: SchemaId,
    envelope: Envelope,
) {
    let targets: Vec<(ActorPath, Arc<Endpoint>)> = {
        let reg = registry.lock();
        reg.handlers_of(&schema)
            .into_iter()
            .filter_map(|path| reg.resolve(&path).map(|endpoint| (path, endpoint)))
            .collect()
    };
    // ONE Sent fact per publish (not per delivery): the broadcast itself is
    // the observable event — a zero-subscriber publish still happened.
    {
        let mut kernel = kernel.lock();
        kernel.record_fact(
            envelope.trace.causality_id.as_millis_ts(),
            crate::tap::FactKind::Sent {
                from: envelope.from.clone(),
                dest: crate::envelope::Address::Schema(schema.clone()),
                schema: schema.clone(),
                trace: envelope.trace.clone(),
            },
        );
    }
    for (_path, endpoint) in &targets {
        // Block backpressure: a full inbox stalls the publisher (loss is
        // unrepresentable; sizing mailboxes is the spawner's call). A
        // closed endpoint (restart in flight) skips this one delivery.
        let _ = deliver_with_retry(endpoint, envelope.clone()).await;
    }
}

/// Resolves one reply: a slot goes straight to the asker's oneshot (the
/// mechanism); a path routes an ordinary envelope through the registry
/// (the durable name).
async fn resolve_reply(
    kernel: &Mutex<KernelState>,
    registry: &Mutex<Registry>,
    to: Address,
    schema: SchemaId,
    payload: JsonValue,
    trace: TraceCtx,
) {
    match to {
        Address::Slot(lease) => {
            // Mechanism: complete the lease if it is still live; a dead
            // (expired/pruned) slot just drops the reply — the asker is
            // gone, and the ask timed out on its side already.
            kernel.lock().replies.complete(&lease, payload);
        }
        Address::Schema(_) => {
            // A schema-addressed reply is an ordinary routed send (the
            // route table picks a handler).
            let envelope = Envelope::json(schema, to, payload, trace);
            if let Err(undeliverable) = route(registry, kernel, envelope).await {
                dead_letter(
                    kernel,
                    &undeliverable,
                    crate::kernel::DeadLetterReason::Unresolvable,
                    "reply destination unresolved",
                );
            }
        }
        Address::Path(path) => {
            // Durable name: an ordinary envelope (any actor may have moved
            // on; unresolvable replies dead-letter like any send).
            let envelope = Envelope::json(schema, Address::Path(path.clone()), payload, trace);
            if let Err(undeliverable) = route(registry, kernel, envelope).await {
                dead_letter(
                    kernel,
                    &undeliverable,
                    crate::kernel::DeadLetterReason::Unresolvable,
                    "reply destination unresolved",
                );
            }
        }
    }
}

/// Fans the committed, declared events out as ordinary messages: every
/// actor that declared `.handles` for the event's schema receives exactly
/// one copy (the same `broadcast` path `ctx.publish` uses). Position in
/// the atomic order is AFTER ack — replay never re-runs this (a restart
/// must not duplicate broadcasts). A recorded fact with zero handlers is
/// a silent no-op (one Sent fact, no deliveries, no DLQ); an entity that
/// `.handles` its own fact legally delivers a copy to itself.
async fn fan_out_emits(ctx: &EsLoop, events: &[crate::envelope::Event]) {
    let cause = crate::envelope::TraceCtx::root();
    for event in events {
        let envelope = Envelope::json(
            event.schema.clone(),
            crate::envelope::Address::Schema(event.schema.clone()),
            event.payload.clone(),
            cause,
        )
        .from(ctx.path.clone());
        broadcast(&ctx.registry, &ctx.kernel, event.schema.clone(), envelope).await;
    }
}

/// The ONE flush-time gate for outbound actor messages: every intent an
/// actor recorded must declare its schema in the actor's `.emits`.
/// Undeclared intents drop at flush — dead-lettered as `UndeclaredEmit`
/// with the envelope retained, plus a tracing error — and never route.
/// The declared remainder dispatches normally, in order.
///
/// The gate sits at flush time (not record time) so there is exactly one
/// choke point next to the routing it guards; observably the message
/// leaves the actor at send time, so nothing else may see the intent.
/// Returns `true` when the outbox carried `StopSelf` (the caller's step
/// concludes with `Step::Stop`).
async fn flush_outbox(ctx: &EsLoop, mut outbox: Outbox) -> bool {
    let declared = {
        let registry = ctx.registry.lock();
        registry
            .lookup(&ctx.path)
            .map(|info| info.manifest.emits)
            .unwrap_or_default()
    };
    let mut stop_self = false;
    for intent in outbox.drain() {
        // THE GATE: every outbound message declares itself. StopSelf is
        // not a message — it passes untouched.
        let gated = match intent.emitted_schema() {
            Some(schema) if !declared.contains(schema) => {
                tracing::error!(
                    actor = %ctx.path,
                    schema = %schema,
                    "undeclared emit dropped at flush (add .emits::<M>() at the spawn site)"
                );
                dead_letter_schema(ctx, &intent, schema);
                continue;
            }
            _ => intent,
        };
        match gated {
            crate::context::Intent::Send(envelope) => {
                if let Err(undeliverable) = route(&ctx.registry, &ctx.kernel, envelope).await {
                    dead_letter(
                        &ctx.kernel,
                        &undeliverable,
                        crate::kernel::DeadLetterReason::Unresolvable,
                        "destination unresolved",
                    );
                }
            }
            crate::context::Intent::Broadcast(envelope) => {
                broadcast(
                    &ctx.registry,
                    &ctx.kernel,
                    envelope.schema.clone(),
                    envelope,
                )
                .await;
            }
            crate::context::Intent::Reply {
                to,
                schema,
                payload,
                trace,
            } => {
                resolve_reply(&ctx.kernel, &ctx.registry, to, schema, payload, trace).await;
            }
            crate::context::Intent::StopSelf => stop_self = true,
        }
    }
    stop_self
}

/// Dead-letters one gated-out intent: the intent's MESSAGE is what died,
/// rebuilt as an envelope addressed to its own destination (same trace, so
/// the drop stays causally linked to the command that caused it).
fn dead_letter_schema(ctx: &EsLoop, intent: &crate::context::Intent, schema: &SchemaId) {
    let envelope = match intent {
        crate::context::Intent::Send(envelope) | crate::context::Intent::Broadcast(envelope) => {
            envelope.clone()
        }
        crate::context::Intent::Reply {
            to, payload, trace, ..
        } => Envelope::json(schema.clone(), to.clone(), payload.clone(), *trace),
        crate::context::Intent::StopSelf => return,
    };
    dead_letter(
        &ctx.kernel,
        &envelope,
        crate::kernel::DeadLetterReason::UndeclaredEmit,
        "undeclared emit (add .emits::<M>() at the spawn site)",
    );
}

/// Takes a between-messages snapshot if the cadence asks for one: a
/// message-count cadence snapshots when the last committed event landed on
/// an n-boundary; a time cadence is checked on the idle path instead.
async fn maybe_snapshot(ctx: &EsLoop, seqs: Vec<crate::journal::SeqNo>) {
    let cadence = {
        let kernel = ctx.kernel.lock();
        kernel
            .snapshot_policy
            .get(&ctx.path)
            .copied()
            .unwrap_or_default()
    };
    let crate::actor::SnapshotCadence::Messages(n) = cadence else {
        return;
    };
    if n == 0 || seqs.is_empty() {
        return;
    }
    let last = *seqs.last().expect("non-empty");
    // Snapshot when the last committed event landed on an n-boundary.
    if last.as_u64() % n != n - 1 {
        return;
    }
    snapshot_now(ctx, last).await;
}

/// Writes one snapshot of the live state at `seq` (the shared tail of both
/// cadence checks; always BETWEEN messages, never mid-step).
async fn snapshot_now(ctx: &EsLoop, last: crate::journal::SeqNo) {
    // Capture the blob under the state lock, then DROP the guard before
    // the store await (the erased state shell is not Send; no lock may be
    // held across the store call).
    let captured = {
        let state = ctx.state().await;
        let state = state.lock().await;
        state.capture_erased().ok()
    };
    if let Some(blob) = captured {
        let now = ctx.clock.now();
        let store = ctx.kernel.lock().journal_store.clone();
        if store
            .append_snapshot(&ctx.path, last, blob, now.as_millis())
            .await
            .is_ok()
        {
            // The time-cadence anchor is kernel-side policy bookkeeping.
            let mut kernel = ctx.kernel.lock();
            kernel
                .snapshot_cadence_ms
                .insert(ctx.path.clone(), Some(now.as_millis()));
            kernel.record_fact(
                now,
                crate::tap::FactKind::SnapshotTaken {
                    path: ctx.path.clone(),
                    seq: last,
                },
            );
        }
    }
}

/// The time-cadence half of snapshotting: checked on the ES loop's idle
/// path (the 20ms poll arm is the wake), so an actor that never receives
/// another message still snapshots when the interval elapses. BETWEEN
/// messages by construction — the idle check runs after a full step drained
/// the inbox.
async fn maybe_snapshot_on_idle(ctx: &EsLoop) {
    let cadence = {
        let kernel = ctx.kernel.lock();
        kernel
            .snapshot_policy
            .get(&ctx.path)
            .copied()
            .unwrap_or_default()
    };
    let crate::actor::SnapshotCadence::Time(interval) = cadence else {
        return;
    };
    // Anchor + last committed seq: the store's replay (load reflects
    // buffered state), read WITHOUT holding the kernel sync guard across
    // the await.
    let replay = {
        let store = ctx.kernel.lock().journal_store.clone();
        match store.load(&ctx.path).await {
            Ok(replay) => replay,
            Err(_) => return,
        }
    };
    // An empty journal never snapshots: the anchor seq would be
    // "genesis", and a later restore would wrongly skip event seq 0.
    let Some(r) = replay else { return };
    if r.tail.is_empty() && r.snapshot.is_none() {
        return;
    }
    let (anchor_ms, now_ms) = {
        let kernel = ctx.kernel.lock();
        (
            kernel.snapshot_cadence_ms.get(&ctx.path).copied().flatten(),
            ctx.clock.now().as_millis(),
        )
    };
    let Some(anchor) = anchor_ms else {
        return; // unanchored: not due (safe default)
    };
    if now_ms.saturating_sub(anchor) < interval.as_millis() as u64 {
        return;
    }
    // Anchor at the LAST COMMITTED EVENT's seq: seqs are 0-based, so the
    // first post-snapshot seq is snap_seq + 1 (or 0 without a snapshot),
    // and the tail's length lands on the last one.
    let last = match r.tail.len() {
        0 => match r.snapshot.as_ref() {
            Some(snap) => snap.seq(),
            None => return,
        },
        tail_len => {
            let base = r
                .snapshot
                .as_ref()
                .map(|s| s.seq().as_u64() + 1)
                .unwrap_or(0);
            crate::journal::SeqNo::new(base + tail_len as u64 - 1)
        }
    };
    snapshot_now(ctx, last).await;
}

/// Closes the inbox on stop; Phase 8 flushes undelivered entries to the DLQ.
async fn drain_inbox_on_stop(ctx: &EsLoop) {
    let mut inbox = ctx.cell.inbox.lock().await;
    inbox.close();
    inbox.reopen();
    drop(inbox);
    // Entries stay queued with the door OPEN: a crash-restart resumes
    // redelivery from the cursor, while an external stop's teardown (or
    // the sweep) closes the door and flushes the rest to the DLQ.
}

/// Stamps the injected-clock time of a completed message step. Only a
/// COMPLETED step (acked/committed) resets the passivation timer — a
/// message merely enqueued does not.
fn stamp_work(ctx: &EsLoop) {
    let now = ctx.clock.now().as_millis();
    let mut kernel = ctx.kernel.lock();
    kernel.last_work_ms.insert(ctx.path.clone(), now);
}

/// The idle arm's passivation check: when the actor's declared idle
/// window has elapsed with no completed step, passivate — close the
/// door FIRST (front door refuses new pushes to the DLQ), drain what is
/// already queued through the normal step path (bounded by inbox
/// capacity), then exit gracefully with `StopReason::Passivated`.
///
/// Suspended during the shutdown sweep: the sweep owns termination.
async fn maybe_passivate(ctx: &EsLoop) {
    let (idle_for, last_work_ms) = {
        let kernel = ctx.kernel.lock();
        // The sweep owns termination: passivation stands down.
        if kernel
            .shutting_down
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        match kernel.passivation.get(&ctx.path) {
            Some(p) => (p.idle_for, kernel.last_work_ms.get(&ctx.path).copied()),
            None => return,
        }
    };
    let now = ctx.clock.now().as_millis();
    let Some(last) = last_work_ms else {
        return;
    };
    if now.saturating_sub(last) < idle_for.as_millis() as u64 {
        return;
    }
    // CLOSE THE DOOR first: pushes now refuse to the DLQ, but entries
    // already inside stay processable (peek/ack still work on a closed
    // inbox). Then drain: process what's queued — the ES step is sync,
    // so this is bounded by capacity.
    {
        let mut inbox = ctx.cell.inbox.lock().await;
        inbox.close();
    }
    loop {
        let has_mail = {
            let mut inbox = ctx.cell.inbox.lock().await;
            inbox.peek().is_some()
        };
        if !has_mail {
            break;
        }
        match step_es(ctx).await {
            Step::Work => continue,
            // A poison message mid-drain: leave the crash for supervision
            // (which restarts; the next idle re-passivates — converges).
            _ => break,
        }
    }
    ctx.graceful_exit(crate::actor::StopReason::Passivated)
        .await;
}
/// The service actor loop: pop → decode → dispatch (async, impure) →
/// drop the message. No journal, no cursor — service actors are at-most-once
/// by design (Phase 8 adds supervision around this loop).
pub(crate) async fn service_actor_loop(loop_ctx: ServiceLoop, mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            break;
        }
        match step_service(&loop_ctx).await {
            Step::Work => {
                stamp_work(&loop_ctx.es);
                continue;
            }
            Step::Idle => {
                // Idle window: passivation is checked here (the 20ms
                // poll arm below is the wake), never mid-step.
                maybe_passivate(&loop_ctx.es).await;
            }
            Step::Crashed => break,
            Step::Stop => {
                // Self-termination: the loop owns its graceful exit (hook
                // + tables); no task joins itself, so this cannot deadlock.
                loop_ctx
                    .es
                    .graceful_exit(crate::actor::StopReason::Normal)
                    .await;
                break;
            }
        }
        let notified = loop_ctx.es.cell.work.notified();
        tokio::select! {
            _ = shutdown.changed() => {}
            _ = notified => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }
    }
    drain_inbox_on_stop(&loop_ctx.es).await;
}

/// Everything one running service loop needs.
#[derive(Clone)]
pub(crate) struct ServiceLoop {
    /// The ES-shaped plumbing the service loop shares (routing, cell).
    pub(crate) es: EsLoop,
}

/// One service step: peek → decode (sync) → dispatch (async) → ack.
///
/// Service messages are consumed on HANDOFF (ack before dispatch): there is
/// no journal to replay from, so redelivery after a crash would re-run
/// side effects — at-most-once semantics are the honest contract here.
async fn step_service(ctx: &ServiceLoop) -> Step {
    // 1. PEEK the envelope.
    let envelope = {
        let mut inbox = ctx.es.cell.inbox.lock().await;
        inbox.peek().cloned()
    };
    let Some(envelope) = envelope else {
        return Step::Idle;
    };

    {
        let mut kernel = ctx.es.kernel.lock();
        kernel.record_fact(
            envelope.trace.causality_id.as_millis_ts(),
            crate::tap::FactKind::Delivered {
                to: ctx.es.path.clone(),
                schema: envelope.schema.clone(),
                trace: envelope.trace,
            },
        );
    }

    // 2. FIND the message entry.
    let entry = {
        let kernel = ctx.es.kernel.lock();
        kernel.msg_entries.get(&ctx.es.path).and_then(|entries| {
            entries
                .iter()
                .find(|e| e.schema() == envelope.schema)
                .cloned()
        })
    };
    let Some(entry) = entry else {
        dead_letter(
            &ctx.es.kernel,
            &envelope,
            crate::kernel::DeadLetterReason::UnknownSchema,
            "no entry for this schema",
        );
        ctx.es.cell.inbox.lock().await.ack();
        return Step::Work;
    };

    // 3. DECODE (sync — decode failures dead-letter cleanly).
    let payload = envelope
        .as_json()
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let decoded = match entry.decode(&payload) {
        Ok(msg) => msg,
        Err(report) => {
            let reason = format!("{report}");
            dead_letter(
                &ctx.es.kernel,
                &envelope,
                crate::kernel::DeadLetterReason::Decode,
                &reason,
            );
            ctx.es.cell.inbox.lock().await.ack();
            return Step::Work;
        }
    };

    // 4. CONSUME (ack) — at-most-once handoff to the handler.
    ctx.es.cell.inbox.lock().await.ack();

    // 5. DISPATCH (async, impure) — spawned so handler panics surface as a
    // JoinHandle error instead of tearing down the loop task itself; the
    // loop awaits the handle, so one actor still processes one message at
    // a time (its inbox serializes).
    let path = ctx.es.path.clone();
    let service = {
        let kernel = ctx.es.kernel.lock();
        kernel
            .services
            .get(&path)
            .cloned()
            .expect("service present for a running loop")
    };
    let view = ctx.es.view.clone();
    let entry = entry.clone();
    let ask_port = KernelAskPort {
        registry: ctx.es.registry.clone(),
        kernel: ctx.es.kernel.clone(),
        clock: ctx.es.clock.clone(),
    };
    let (outbox_tx, outbox_rx) = tokio::sync::oneshot::channel();
    let trace = envelope.trace;
    let reply_to = envelope.reply_to.clone();
    let handle = tokio::spawn(async move {
        let mut outbox = Outbox::new();
        let mut msg_ctx = crate::context::MsgCtx::new(
            &path,
            &trace,
            reply_to.as_ref(),
            view.as_ref(),
            &mut outbox,
            Some(&ask_port),
        );
        let mut service = service.lock().await;
        entry
            .dispatch(service.as_mut(), decoded, &mut msg_ctx)
            .await;
        let _ = outbox_tx.send(outbox);
    });
    let outbox = if handle.await.is_err() {
        // Handler panicked: mark crashed (supervision restarts via `start`).
        let mut kernel = ctx.es.kernel.lock();
        kernel.crashed.insert(ctx.es.path.clone());
        return Step::Crashed;
    } else {
        outbox_rx.await.unwrap_or_default()
    };

    // 6. FLUSH deferred effects from the handler. A StopSelf intent
    // concludes the step with Step::Stop — sends recorded before it have
    // already flushed (in-order).
    let stop_self = flush_outbox(&ctx.es, outbox).await;

    if stop_self { Step::Stop } else { Step::Work }
}

/// Restarts a crashed ES actor (spec algorithm):
/// 1. rebuild state from the last snapshot (fast path) or genesis args,
/// 2. apply the journal tail after the snapshot seq (replay: apply only —
///    handlers never re-run, effects never re-fire),
/// 3. swap the slot endpoint to a fresh loop; the inbox and its cursor
///    persist, so pending messages (including the one that crashed the
///    actor) redeliver exactly once from where the cursor stopped.
///
/// The supervisor (Phase 8) wraps this with policy/budget/backoff checks.
///
/// # Errors
///
/// Propagates rebuild failures (a corrupt snapshot or undecodable state).
pub(crate) async fn restart_es(
    ctx: &EsLoop,
    genesis_args: &JsonValue,
) -> Result<(), error_stack::Report<JournalError>> {
    // Replay input comes from the STORE (load reflects buffered state).
    // A supervised child ALWAYS rebuilds: a crash before the first append
    // (store never saw the path) restarts from genesis, exactly like an
    // empty journal would.
    let replay = {
        let store = ctx.kernel.lock().journal_store.clone();
        store
            .load(&ctx.path)
            .await?
            .unwrap_or(crate::journal::Replay {
                snapshot: None,
                tail: Vec::new(),
            })
    };
    let (snapshot, tail) = (replay.snapshot, replay.tail);

    // Rebuild through the erased shell: the OLD instance is dropped, the
    // fresh one starts from snapshot-or-genesis plus the replay tail.
    let snapshot_state = match snapshot {
        Some(JournalEntry::Snapshot { state, .. }) => Some(state),
        _ => None,
    };
    // Lock order: resolve the state Arc WITHOUT holding the kernel's sync
    // guard across the await (a sync guard held across .await can deadlock
    // tasks that need the sync lock to make progress).
    let old = {
        let kernel = ctx.kernel.lock();
        kernel
            .es_state
            .get(&ctx.path)
            .cloned()
            .expect("es state present at restart")
    };
    let fresh = {
        let old = old.lock().await;
        old.rebuild(genesis_args, snapshot_state, &tail)?
    };
    // Fresh instance swapped in; the poisoned one is gone.
    {
        let mut kernel = ctx.kernel.lock();
        kernel
            .es_state
            .insert(ctx.path.clone(), Arc::new(tokio::sync::Mutex::new(fresh)));
        kernel.crashed.remove(&ctx.path);
        kernel.record_fact(
            ctx.clock.now(),
            crate::tap::FactKind::Spawned {
                path: ctx.path.clone(),
                kind: crate::actor::ActorKind::EventSourced,
                restart: true,
            },
        );
    }

    // Fresh endpoint behind the SAME path: senders holding pre-crash
    // clones never notice (identity = path; slots are swapped, not dropped).
    let (tx, rx) = mpsc::channel(capacity_hint());
    let endpoint = Endpoint::new(tx);
    {
        let mut registry = ctx.registry.lock();
        registry
            .swap_endpoint(&ctx.path, endpoint)
            .expect("slot exists at restart");
    }

    // Redelivery: the cursor never moved; the crash-loop left the inbox
    // open-and-queued. Reopen and run a fresh loop over the SAME cell.
    {
        let mut inbox = ctx.cell.inbox.lock().await;
        inbox.reopen();
    }
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(front_door_loop(ctx.cell.clone(), ctx.kernel.clone(), rx));
    tokio::spawn(es_actor_loop(ctx.clone(), shutdown_rx));
    Ok(())
}

/// The front-door capacity for restarted endpoints.
fn capacity_hint() -> usize {
    64
}

/// The supervision engine: one task per supervised child, awaiting the
/// child's crash, then applying the spec — policy → budget → backoff →
/// restart, or stop + escalate.
pub async fn supervise_child(
    system: crate::system::ActorSystem,
    spec: crate::supervision::ActorSpec,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        // Wait for this child to crash (or the system to shut down).
        let mut crashed_seen = false;
        let watch = async {
            loop {
                // Sweep suspension: a graceful shutdown that spawns
                // restarts fights itself — stand down until the watcher
                // exits.
                if system
                    .kernel
                    .lock()
                    .shutting_down
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    continue;
                }
                let crashed = {
                    let kernel = system.kernel.lock();
                    kernel.crashed.contains(&spec.path)
                };
                if crashed {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        };
        tokio::select! {
            _ = watch => { crashed_seen = true; }
            _ = shutdown.changed() => {
                if *shutdown.borrow() { return; }
            }
        }
        let _ = crashed_seen;

        // The child crashed. Interpret the spec.
        let now = system.clock.now().as_millis();
        let policy = spec.restart;
        if policy == crate::supervision::RestartPolicy::Never {
            escalate(
                &system,
                &spec,
                "policy Never (crashed)",
                crate::actor::StopReason::Crashed,
            )
            .await;
            return;
        }

        // Budget: record the failure first, then check the window.
        let budget_exhausted = {
            let mut kernel = system.kernel.lock();
            let window = kernel.failures.entry(spec.path.clone()).or_default();
            window.record(now);
            window.prune(now, &spec.budget);
            window.exhausted(now, &spec.budget)
        };
        if budget_exhausted {
            escalate(
                &system,
                &spec,
                "restart budget exhausted",
                crate::actor::StopReason::Escalated,
            )
            .await;
            return;
        }

        // Backoff, then restart. The restart mechanism follows the
        // child's contract: an EventSourced child recovers through
        // `restart_es` (journal rebuild + endpoint swap + inbox reopen
        // with pending mail intact); a Service child restarts through
        // the spec's spawn closure (a fresh start — no journal).
        let consecutive = {
            let kernel = system.kernel.lock();
            kernel
                .failures
                .get(&spec.path)
                .map(|w| w.count(now, &spec.budget))
                .unwrap_or(1) as u32
        };
        let delay = spec.backoff.delay(consecutive);
        tokio::time::sleep(delay).await;
        let is_es_child = system_is_es_child(&system, &spec.path);
        if is_es_child {
            // Journal-anchored recovery: rebuild from snapshot-or-genesis,
            // apply the tail, swap the endpoint under the SAME path, and
            // reopen the inbox (redelivery resumes from the cursor).
            let ctx = crate::kernel::EsLoop {
                path: spec.path.clone(),
                cell: {
                    let kernel = system.kernel.lock();
                    kernel
                        .cells
                        .get(&spec.path)
                        .cloned()
                        .expect("crashed ES child keeps its cell for redelivery")
                },
                registry: system.registry.clone(),
                kernel: system.kernel.clone(),
                view: system.view.clone(),
                clock: system.clock.clone(),
            };
            let genesis_args = {
                let kernel = system.kernel.lock();
                kernel
                    .genesis_args
                    .get(&spec.path)
                    .cloned()
                    .unwrap_or(JsonValue::Object(serde_json::Map::new()))
            };
            restart_es(&ctx, &genesis_args)
                .await
                .expect("supervised ES child restart");
            // restart_es already cleared the crash flag.
        } else {
            // Service child: the spawn closure does a FULL spawn (slot
            // insert included); clear the dead instance's slot first so
            // the path is free.
            {
                let mut registry = system.registry.lock();
                let _ = registry.remove_slot(&spec.path);
            }
            (spec.spawn)(&system, &spec.path, &spec.args);
            // The fresh instance is running (the spawn closure re-runs the
            // loop); clear the stale crash flag so the next wait observes a
            // NEW crash, not the one we just handled.
            {
                let mut kernel = system.kernel.lock();
                kernel.crashed.remove(&spec.path);
            }
        }
    }
}

/// Whether `path` is a journaled (EventSourced) child: the engine must
/// recover it through `restart_es` rather than a fresh spawn.
fn system_is_es_child(system: &crate::system::ActorSystem, path: &ActorPath) -> bool {
    let kernel = system.kernel.lock();
    kernel.es_state.contains_key(path)
}

/// Stops the child (slot + crash record) and escalates a control message
/// to the parent (or the system record when parentless). Emits the
/// Escalated fact.
async fn escalate(
    system: &crate::system::ActorSystem,
    spec: &crate::supervision::ActorSpec,
    reason: &str,
    stop_reason: crate::actor::StopReason,
) {
    {
        let mut kernel = system.kernel.lock();
        kernel.crashed.remove(&spec.path);
        // The child is gone: record the stop WITH its typed reason, then
        // the escalation.
        kernel.record_fact(
            system.clock.now(),
            crate::tap::FactKind::Stopped {
                path: spec.path.clone(),
                reason: stop_reason,
            },
        );
        kernel.record_fact(
            system.clock.now(),
            crate::tap::FactKind::Escalated {
                path: spec.path.clone(),
                reason: reason.to_owned(),
            },
        );
    }
    // Remove the child's slot (its identity leaves the registry; the
    // graceful-stop cascade arrives with the stop API in Phase 9).
    {
        let mut registry = system.registry.lock();
        let _ = registry.remove_slot(&spec.path);
    }
    let message = spec.escalation_message(reason);
    if let Some(parent) = &spec.parent {
        system
            .send(system.envelope(
                crate::schema::SchemaId::new("Escalated", 1),
                parent.clone(),
                message,
            ))
            .await
            .ok();
    }
}

/// Why an envelope was dead-lettered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeadLetterReason {
    /// No slot or route resolved for the destination.
    Unresolvable,
    /// No handler is registered for the payload's schema.
    UnknownSchema,
    /// The payload did not decode against its registered schema.
    Decode,
    /// The destination inbox refused the envelope (overload/closed).
    InboxRefused,
    /// The actor was stopped with undelivered inbox entries.
    StoppedWithMail,
    /// The system is in its graceful shutdown sweep; the barrier refuses
    /// all new deliveries.
    ShuttingDown,
    /// The actor emitted an event whose schema it never declared (the
    /// pre-journal filter: the journal guard).
    UndeclaredEvent,
    /// The actor recorded an outbound effect whose schema it never
    /// declared in `.emits` (the flush-time gate: the wire guard).
    UndeclaredEmit,
    /// A partition-set command arrived without its shard key.
    ShardKeyMissing,
}
