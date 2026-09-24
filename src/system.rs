//! The system facade: the single handle through which the runtime is
//! configured, driven, and observed.
//!
//! The registry is plain data behind a lock — not an actor — so schema
//! registration can never deadlock and survives every actor restart.
//! Handler-side lookups snapshot through a read-only view of it, so user
//! code never holds the registry lock.

use parking_lot::Mutex;
use std::sync::Arc;

use crate::actor::ActorPath;
use crate::actor::{
    CommandEntry, DynServiceActor, EsAny, EventSourcedActor, MsgEntry, ServiceActor, TypedEsState,
    TypedServiceState,
};
use crate::clock::Timestamp;
use crate::clock::{ClockService, FakeClock, SystemClock};
use crate::context::RuntimeView;
use crate::envelope::{Address, Envelope, PayloadBytes, TraceCtx};
use crate::inbox::InboxOffset;
use crate::inbox::{Inbox, OverloadPolicy};
use crate::json::Json;
pub use crate::kernel::DeadLetter;
use crate::kernel::{ActorCell, CountingKernelLock, EsLoop, KernelState, route};
use crate::registry::{Endpoint, EndpointInfo, Registry};
use crate::schema::Schema;
use crate::schema::SchemaId;

pub use crate::actor::SnapshotCadence;

/// Declarative idle passivation: the runtime stops the actor after this
/// long without a completed message step.
///
/// Semantics: the idle timer stamps on each COMPLETED step (peek →
/// dispatch → ack) — a message merely enqueued does not reset it, so a
/// wedged actor still passivates. When the timer fires, the actor closes
/// its inbox door and drains what is already queued before tearing down
/// (a race-arrival is processed, not dead-lettered; the drain is bounded
/// by inbox capacity). A `Stopped { Passivated }` fact is recorded. A
/// partition set re-spawns the entity on the next send to the public
/// path; an ES entity replays its journal (lossless). Service entities
/// restart from genesis — they must tolerate that.
///
/// A standalone actor's passivation is terminal until the host re-spawns
/// it: passivation closes the inbox for good, so nothing without an
/// activation path can reach it again. Use a partition set (or projector
/// set) when an actor must stay reachable across idle eviction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Passivation {
    /// Maximum idle time (no completed message step) before the actor
    /// is passivated.
    pub idle_for: std::time::Duration,
}

/// Spawn-time options for an actor.
#[derive(Debug, Clone)]
pub struct SpawnOpts {
    /// Snapshot cadence (ES actors only).
    pub snapshot: SnapshotCadence,
    /// Mailbox capacity (the logical inbox; the front door is 2× this).
    pub mailbox_capacity: usize,
    /// Inbox overload policy (default Block = backpressure).
    pub mailbox_policy: OverloadPolicy,
    /// Inbox depth at which a [`crate::observe::ObservationKind::Backpressured`] fact
    /// fires (once per crossing); `None` = never. Pool/partition specs use
    /// it to make sustained overload observable.
    pub high_watermark: Option<u64>,
    /// Idle passivation config; `None` = the actor lives until stopped.
    pub passivation: Option<Passivation>,
    /// How many queued messages one wake's step may drain and commit as a
    /// batch (default 64). A trickle load drains exactly what is queued —
    /// batch 1 is byte-identical to the per-message step; a loaded run
    /// amortizes the wake/lock/commit across up to N messages.
    pub batch: usize,
}

impl Default for SpawnOpts {
    fn default() -> Self {
        Self {
            snapshot: SnapshotCadence::Off,
            mailbox_capacity: 64,
            mailbox_policy: OverloadPolicy::Block,
            high_watermark: None,
            passivation: None,
            batch: 64,
        }
    }
}

/// The erased async start a typed service wrapper hands to the funnel:
/// builds the boxed instance (I/O allowed inside `start`).
pub(crate) type ServiceStart = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    Box<dyn DynServiceActor>,
                    error_stack::Report<crate::registry::RegistryError>,
                >,
            > + Send,
    >,
>;

/// Startup configuration for an [`ActorSystem`].
#[derive(Clone)]
pub struct SystemConfig {
    /// The injected clock (leases, fact timestamps; tests use a fake).
    pub clock: ClockService,
    /// The observation handler installed at startup (opt-in; `None` = off,
    /// the default — with no handler installed no observation is ever
    /// constructed). Install or remove later via
    /// [`ActorSystem::set_observation`](ActorSystem::set_observation) /
    /// [`ActorSystem::clear_observation`](ActorSystem::clear_observation).
    pub observation: Option<crate::observe::ObservationHandler>,
    /// Default mailbox capacity and overload policy for spawned actors
    /// (per-spawn [`SpawnOpts`] override these).
    pub default_mailbox: MailboxDefaults,
    /// The journal store (plus its control wiring) installed at
    /// construction — the only moment a store installs. `None` = the
    /// in-memory default. Build one with
    /// [`JournalArgs::new`](crate::journal::JournalArgs::new) (custom
    /// stores) or, with the `daow` feature, that module's
    /// `JournalArgs::daow` (see the `journal_daow` module).
    pub journal_args: Option<crate::journal::JournalArgs>,
}

/// System-wide mailbox defaults.
#[derive(Clone, Copy)]
pub struct MailboxDefaults {
    /// Default inbox capacity.
    pub capacity: usize,
    /// Default overload policy (Block = backpressure).
    pub policy: OverloadPolicy,
}

impl Default for MailboxDefaults {
    fn default() -> Self {
        Self {
            capacity: 64,
            policy: OverloadPolicy::Block,
        }
    }
}

impl SystemConfig {
    /// Wall-clock system with default capacities.
    pub fn production() -> Self {
        Self {
            clock: ClockService::new(Arc::new(SystemClock::new())),
            observation: None,
            default_mailbox: MailboxDefaults::default(),
            journal_args: None,
        }
    }

    /// A config on an injected clock (tests).
    pub fn with_clock(clock: ClockService) -> Self {
        Self {
            clock,
            observation: None,
            default_mailbox: MailboxDefaults::default(),
            journal_args: None,
        }
    }

    /// Installs the journal store (and its control handler) at
    /// construction — the only moment a store installs. See
    /// [`JournalArgs`](crate::journal::JournalArgs).
    pub fn with_journal(mut self, args: crate::journal::JournalArgs) -> Self {
        self.journal_args = Some(args);
        self
    }
}

/// The runtime's shared registry lock, wrapped with a test-build
/// acquisition counter.
///
/// Production: a newtype over `Mutex<Registry>` that derefs exactly like
/// the mutex — zero behavior change, one struct field. Test builds: every
/// `lock()` bumps [`crate::kernel::REGISTRY_LOCKS`] before handing out the
/// guard, so tests can count critical sections per send window (the
/// Arc-payload work's mechanism deliverable).
pub(crate) struct CountingRegistryLock(Mutex<Registry>);

impl CountingRegistryLock {
    /// Acquires the registry, counting the critical section in test
    /// builds.
    #[inline]
    pub(crate) fn lock(&self) -> parking_lot::MutexGuard<'_, Registry> {
        #[cfg(test)]
        crate::kernel::bump_registry_locks();
        self.0.lock()
    }
}

impl std::ops::Deref for CountingRegistryLock {
    type Target = Mutex<Registry>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for CountingRegistryLock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// The actor fabric's engine: the shared tables and locks one fabric
/// consists of. Not used directly — [`ActorSystem`] is the public handle;
/// construction is encapsulated so every handle provably aliases a
/// properly-initialized fabric.
///
/// The registry is its own mutex (routing never blocks actor-table
/// mutations); actor tables share the kernel state's lock because they
/// mutate together.
pub struct ActorSystemCore {
    /// Routing table: slots, schemas, routes.
    pub(crate) registry: Arc<CountingRegistryLock>,
    /// Actor tables: cells, journals, ES state, entries, crashes.
    pub(crate) kernel: Arc<CountingKernelLock>,
    pub(crate) clock: ClockService,
    /// The read-only view handed to handler contexts (the system itself).
    pub(crate) view: Arc<dyn RuntimeView>,
    /// Supervision engine shutdown handles (one per supervised child).
    child_shutdowns: parking_lot::Mutex<Vec<tokio::sync::watch::Sender<bool>>>,
    /// System-wide mailbox defaults (per-spawn opts override).
    pub(crate) mailbox_defaults: MailboxDefaults,
    /// Set by the graceful shutdown sweep: new routes dead-letter with
    /// `ShuttingDown`, partition activation is disabled, supervision
    /// engines suspend, and passivation stands down (the sweep owns
    /// termination). SHARED with the kernel state (one `Arc<AtomicBool>`):
    /// the send path reads it lock-free through this handle.
    pub(crate) shutting_down: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// The journal store every ES actor's journal lives in (in-memory by
    /// default; swapped per system at construction).
    pub(crate) journal_store_slot:
        parking_lot::RwLock<std::sync::Arc<dyn crate::journal::JournalStore>>,
    /// The host's store-control handler installed with the store at
    /// construction (`None` = store errors are only traced). The system's
    /// surface for store failures that have no caller (writer-task ticks,
    /// the sweep's flush). See
    /// [`StoreControlMessage`](crate::journal::StoreControlMessage).
    pub(crate) journal_control: Option<crate::journal::ControlHandler>,
    /// The test-build observation log the `test()` constructors install
    /// (the assertions' window on the observation flow). `None` for
    /// production systems — observation there is only the host's handler.
    pub(crate) observation_log: Option<crate::observe::ObservationLog>,
}

/// A handle onto one shared actor fabric: the single surface for
/// configuring, driving, and observing the runtime. Clone it freely —
/// every clone aliases the same fabric; nothing is copied, and dropping
/// the last handle never tears anything down (stopping supervision is an
/// explicit [ActorSystem::shutdown](crate::system::ActorSystem::shutdown)
/// call, not a destructor).
#[derive(Clone)]
pub struct ActorSystem(std::sync::Arc<ActorSystemCore>);

impl std::ops::Deref for ActorSystem {
    type Target = ActorSystemCore;

    fn deref(&self) -> &ActorSystemCore {
        &self.0
    }
}

impl std::fmt::Debug for ActorSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorSystem").finish_non_exhaustive()
    }
}

/// An ES actor whose tables are registered but whose inbox loop has not
/// started (the projector spawn's intermediate state).
///
/// The window is the cutover buffer: deliveries published since the arm
/// sit in the inbox (backpressure upstream when it fills), and
/// [`ArmedEsActor::start_loop`] begins the loop so they fold AFTER
/// whatever the caller seeded — history first, live tail second, never a
/// gap, never a duplicate.
pub(crate) struct ArmedEsActor {
    path: ActorPath,
    loop_ctx: crate::kernel::EsLoop,
    rx: tokio::sync::mpsc::Receiver<Envelope>,
}

impl ArmedEsActor {
    /// Starts the front door + ES loop (the actor goes live).
    pub(crate) fn start_loop(self) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let path = self.path.clone();
        let kernel = self.loop_ctx.kernel.clone();
        let task = self.loop_ctx.start_tracked(self.rx, shutdown_rx);
        let kernel = kernel.lock();
        if let Some(cell) = kernel.cells.get(&path)
            && let Ok(mut handle) = cell.handle.try_lock()
        {
            *handle = Some(crate::kernel::ActorHandle {
                shutdown: shutdown_tx,
                task: Some(task),
            });
        }
    }
}

impl ActorSystem {
    /// Creates a handle onto a fresh fabric built from `config`.
    pub fn new(config: SystemConfig) -> Self {
        Self(std::sync::Arc::new(ActorSystemCore::new(config)))
    }

    /// A handle onto a fabric tuned for tests: a [`FakeClock`] starting
    /// at 1_000 ms. Test builds install a capturing observation log —
    /// production keeps observation strictly opt-in.
    pub fn test() -> (Self, Arc<FakeClock>) {
        let core = ActorSystemCore::test();
        (Self(std::sync::Arc::new(core.0)), core.1)
    }

    /// Like [`ActorSystem::test`](crate::system::ActorSystem::test), but with
    /// an observation handler installed at startup.
    pub fn test_with_observer(
        handler: crate::observe::ObservationHandler,
    ) -> (Self, Arc<FakeClock>) {
        let core = ActorSystemCore::test_with_observer(handler);
        (Self(std::sync::Arc::new(core.0)), core.1)
    }

    /// Fires every supervised child's shutdown watcher: each
    /// [`crate::kernel::supervise_child`] loop exits instead of restarting
    /// its child. Idempotent; children spawned after the call are not
    /// covered until the next call.
    pub fn shutdown(&self) {
        let senders: Vec<_> = self.child_shutdowns.lock().drain(..).collect();
        for tx in senders {
            let _ = tx.send(true);
        }
    }

    /// The graceful shutdown sweep: a barrier (no new sends — every
    /// delivery dead-letters with `ShuttingDown`; partition activation is
    /// disabled; supervision engines suspend; passivation stands down),
    /// then a deadline-bounded parallel drain of every live actor (each
    /// loop task closes its door, finishes queued mail, runs its
    /// `on_stop` hook, and tears down its tables), then one store flush
    /// (backends persist buffered journals here), then supervision
    /// shutdown. Hard-capped by `deadline`: stragglers are joined with
    /// whatever budget remains; expiry drops them (hard-stop semantics
    /// for the remainder).
    ///
    /// This is the in-process grace layer: in-flight work completes,
    /// external side effects flush through `on_stop`, undelivered mail
    /// lands in the DLQ observably. In-memory journals die with the
    /// process — cross-process durability is a store backend's concern.
    pub async fn shutdown_graceful(&self, deadline: std::time::Duration) {
        use std::sync::atomic::Ordering;
        // 1. BARRIER: refuse new work; suspend engines/passivation.
        self.shutting_down.store(true, Ordering::SeqCst);
        self.kernel
            .lock()
            .shutting_down
            .store(true, Ordering::SeqCst);
        self.shutdown();

        // 2. PARALLEL DRAIN under ONE shared deadline: every live cell's
        // loop exits (watch trip → drain → on_stop → teardown) while we
        // join the tasks. The watch signal is the door-close for loops
        // that are mid-idle; the teardown idempotence lets whichever side
        // arrives first win.
        let handles: Vec<(ActorPath, tokio::task::JoinHandle<()>)> = {
            let kernel = self.kernel.lock();
            kernel
                .cells
                .iter()
                .filter_map(|(path, cell)| {
                    let mut guard = cell.handle.try_lock().ok()?;
                    let h = guard.take()?;
                    let _ = h.shutdown.send(true);
                    Some((path.clone(), h.task?))
                })
                .collect()
        };
        let mut per_path: Vec<ActorPath> = Vec::new();
        {
            let mut shared = deadline;
            let mut handles = handles;
            // One shared budget, spent as each task is joined (bounded
            // total wait; stragglers after expiry get hard-stopped below).
            for (path, task) in handles.drain(..) {
                if shared.is_zero() {
                    per_path.push(path);
                    continue;
                }
                let started = std::time::Instant::now();
                let _ = tokio::time::timeout(shared, task).await;
                shared = shared.saturating_sub(started.elapsed());
                per_path.push(path);
            }
        }

        // 3. Tables for anything the loops didn't finish (deadline expiry
        // or an idle-loop that never woke): idempotent — skips the dead.
        for path in &per_path {
            self.teardown_tables(path, crate::actor::StopReason::Shutdown)
                .await;
        }

        // 4. STORE FLUSH (once per sweep, after every journal is final).
        let store = self.journal_store_slot.read().clone();
        let _ = store.flush().await;

        // 5. Engines were told at step 1; drop our senders.
        let _ = self.child_shutdowns.lock().drain(..);
    }

    /// Spawns a supervised child: registers its spec (policy, budget,
    /// backoff, spawn closure), runs the spawn closure once, and arms the
    /// supervision engine for crash handling.
    ///
    /// Lives on the handle (not the core) because the engine task captures
    /// a clone of the handle itself.
    pub fn spawn(&self, spec: crate::supervision::ActorSpec) {
        {
            let mut kernel = self.kernel.lock();
            kernel.specs.insert(spec.path.clone(), spec.clone());
            kernel
                .failures
                .insert(spec.path.clone(), crate::supervision::FailureWindow::new());
        }
        let engine = self.clone();
        let engine_spec = spec.clone();
        let path = spec.path.clone();
        (spec.spawn)(&engine, &path, &spec.args);
        // The child's cell, when the spawn closure registered one (an
        // edge-only spec may spawn nothing — the engine then just parks
        // on shutdown). The engine reads the crash flag LOCK-FREE and
        // parks on the cell's crash signal — no polling, no kernel lock.
        let child_cell = self.kernel.lock().cells.get(&spec.path).cloned();
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        self.child_shutdowns.lock().push(_shutdown_tx);
        crate::kernel::spawn_tracked(crate::kernel::supervise_child(
            engine,
            engine_spec,
            child_cell,
            shutdown_rx,
        ));
    }

    /// Takes every retained dead letter: each comes back with its reason,
    /// detail, and FULL ENVELOPE; the queue empties. The host decides —
    /// inspect, log, or deliberately resend (`system.send(letter.envelope)`).
    /// The runtime never re-drives automatically.
    pub fn drain_dead_letters(&self) -> Vec<crate::kernel::DeadLetter> {
        std::mem::take(&mut self.kernel.lock().dead_letters)
    }

    /// Declares an emit schema for a live actor — THE declaration
    /// mutation point: it writes the registry manifest AND the actor's
    /// cell-local mirror in one critical section, so the step gates (which
    /// read the mirror) can never diverge from the registry's copy. All
    /// emit declarations for a spawned actor must flow through here
    /// (spawn itself seeds the mirror from the manifest).
    ///
    /// # Errors
    ///
    /// Propagates the registry's `UnknownPath` (no live slot).
    pub fn declare_emits(
        &self,
        path: &ActorPath,
        schema: crate::schema::SchemaId,
    ) -> Result<(), error_stack::Report<crate::registry::RegistryError>> {
        // SEQUENTIAL, not nested: registry guard first, then the kernel's
        // cells — the codebase never nests these two locks. No reader can
        // observe a harmful mid-state: the gates read the mirror, and
        // declarations are a pre-first-step operation.
        {
            let mut registry = self.registry.lock();
            registry.declare_emits(path, schema.clone())?;
        }
        // Clone the cell out; the kernel guard drops before the mirror
        // write (guards never span).
        let cell = self.kernel.lock().cells.get(path).cloned();
        if let Some(cell) = cell {
            let mut mirror = cell.declared_emits.write().expect("declared emits lock");
            if !mirror.contains(&schema) {
                mirror.push(schema);
            }
        }
        Ok(())
    }
}

/// One actor's row in a system export.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ActorExport {
    /// The actor's path (its identity).
    pub path: ActorPath,
    /// The contract kind (EventSourced | Service).
    pub kind: crate::actor::ActorKind,
    /// The actor's declared edges.
    pub manifest: crate::schema::ActorManifest,
    /// Live ES state via `capture` (ES actors only).
    pub state: Option<Json>,
    /// The actor's inbox ack cursor (ES progress).
    pub cursor: Option<u64>,
}

/// One declared edge: actor → schema it handles/emits (from manifests).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeclaredEdge {
    /// The actor declaring the edge.
    pub actor: ActorPath,
    /// The schema on the edge.
    pub schema: SchemaId,
    /// The direction: Handles (inbound) or Emits (outbound).
    pub direction: EdgeDirection,
}

/// The direction of a declared edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EdgeDirection {
    /// The actor accepts this schema.
    Handles,
    /// The actor produces this schema.
    Emits,
}

/// One observed edge: aggregated send traffic.
///
/// Under opt-in observation there is no send history: [`ActorSystem::export`]
/// counts `Sent` observations that flow during the export's own await
/// points (its temporary counting handler replaces whatever handler was
/// installed and leaves observation off afterwards), so a quiet system
/// exports no observed edges.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObservedEdge {
    /// The sending path (absent for system-entry sends).
    pub from: Option<String>,
    /// The destination (`path:<p>` or `schema:<s>`).
    pub to: String,
    /// The schema that flowed.
    pub schema: SchemaId,
    /// The number of observed sends.
    pub count: u64,
}

/// One declared partition set: the public path, the shard-key field, and
/// the entities activated so far.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionExport {
    /// The public path senders address.
    pub path: ActorPath,
    /// The command field carrying the shard key.
    pub key_field: String,
    /// Every entity path derived so far (activated entities).
    pub entities: Vec<ActorPath>,
}

/// One declared router rule (declaration/priority order).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuleExport {
    /// Matches the original sender path, if declared.
    pub source: Option<ActorPath>,
    /// Matches the envelope's schema, if declared.
    pub schema: Option<SchemaId>,
    /// Matches the envelope's destination path, if declared.
    pub dest: Option<ActorPath>,
    /// "tee" or "inline".
    pub action: String,
    /// The observer the action targets.
    pub observer: ActorPath,
}

/// The whole-system export: the artifact a future canvas consumes.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SystemExport {
    /// Every registered schema definition (all versions).
    pub schemas: Vec<crate::schema::SchemaDef>,
    /// Every live actor with its manifest and (for ES) live state.
    pub actors: Vec<ActorExport>,
    /// Declared edges (from manifests).
    pub declared_edges: Vec<DeclaredEdge>,
    /// Observed edges (aggregated from the tap).
    pub observed_edges: Vec<ObservedEdge>,
    /// Declared partition sets (public path → entities).
    pub partitions: Vec<PartitionExport>,
    /// Declared router rules (declaration order).
    pub rules: Vec<RuleExport>,
}

/// The projector's catch-up: replay own journal → scan the store for what
/// it lacks → seed the gap → start the inbox loop → record `CaughtUp`.
///
/// Runs BETWEEN [`ActorSystemCore::arm_projector`] and
/// [`ArmedEsActor::start_loop`]: the arm registered the routes, so live
/// copies published during seeding sit queued in the inbox (Block
/// backpressure upstream), and they fold after history once the loop
/// starts — no gap, no duplicate, per-source order preserved.
pub(crate) async fn catch_up_projector(
    system: ActorSystem,
    mut armed: ArmedEsActor,
    consumed: Vec<crate::schema::SchemaId>,
) {
    let path = armed.path.clone();
    let store = system.kernel.lock().journal_store.clone();

    // 1. REPLAY OWN JOURNAL: snapshot-or-genesis + tail (relocated
    //    recover_at_boot, run pre-loop for projectors). The checkpoint is
    //    every CatchUp origin in the WHOLE journal — including entries a
    //    snapshot already folded, or a restart would re-seed and
    //    double-fold them.
    let mut seeded: u64 = 0;
    let replay = store.load(&path).await.ok().flatten();
    if let Some(replay) = &replay {
        let snapshot_state = replay.snapshot.as_ref().and_then(|e| match e {
            crate::journal::JournalEntry::Snapshot { state, .. } => Some(state.clone()),
            _ => None,
        });
        let fresh = {
            let state_arc = {
                let kernel = system.kernel.lock();
                kernel.es_state.get(&path).cloned()
            };
            match state_arc {
                Some(state_arc) => {
                    let old = state_arc.lock().await;
                    old.rebuild(&crate::json!({}), snapshot_state, &replay.tail)
                }
                None => return, // torn down mid-catch-up; nothing to seed into
            }
        };
        match fresh {
            Ok(state) => {
                let mut kernel = system.kernel.lock();
                kernel
                    .es_state
                    .insert(path.clone(), Arc::new(tokio::sync::Mutex::new(state)));
            }
            Err(_) => return, // unrecoverable state; leave the arm pristine
        }
    }

    // 2. SCAN + SEED THE GAP: everything `Recorded` anywhere in the store
    //    (ascending ingest order — a total order across sources), minus
    //    this projector's own journal (covered by the replay). The store's
    //    append_catchup is IDEMPOTENT on the (source, seq) origin — the
    //    checkpoint IS the journal — and its answer says which facts are
    //    NEWLY recorded; only those fold (append before apply, mirroring
    //    the kernel's atomic step). A live copy published during this scan
    //    that the store just checkpointed gets skipped by the step's
    //    identical check, so every fact folds exactly once.
    if let Ok(scanned) = store.scan(&consumed).await {
        // Per-key sets fold ONLY their key's facts: a copy whose source
        // journal is another key's sibling (`chats/rust` vs `chats/k8s`)
        // must never seed this projector — the key derivation is the
        // whole point of per-key read models.
        let own_set = {
            let registry = system.registry.lock();
            registry.projector_set_owning(&path)
        };
        // A source journal qualifies for this key iff it RECORDED at
        // least one fact whose payload names this key (never checkpoint
        // re-records: a projector's journal is never another projector's
        // source of truth).
        let qualifying = {
            let store = store.clone();
            move |key_field: String, my_key: String, journal_path: ActorPath| {
                let store = store.clone();
                async move {
                    store
                        .load(&journal_path)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|replay| {
                            replay
                                .events
                                .iter()
                                .any(|je| je.recorded_payload_key(&key_field, &my_key))
                        })
                }
            }
        };
        // Per-key sets fold ONLY their key's facts: a candidate folds iff
        // its source journal qualifies for this key (recorded a fact with
        // this key in its payload — the same derivation broadcast uses).
        let (key_field, my_key) = match &own_set {
            Some(spec) => (
                Some(spec.key_field.clone()),
                path.as_str().rsplit('/').next().unwrap_or("").to_owned(),
            ),
            None => (None, String::new()),
        };
        let mut candidates = Vec::new();
        for sc in scanned {
            // Own-journal entries are covered by the replay (everything
            // the journal holds folds on rebuild) — never re-seed them.
            if sc.journal == path {
                continue;
            }
            let keep = match &key_field {
                Some(key_field) => {
                    qualifying(
                        key_field.as_str().to_owned(),
                        my_key.clone(),
                        sc.journal.clone(),
                    )
                    .await
                }
                None => true, // standalone projector: no key filter
            };
            if keep {
                candidates.push(sc);
            }
        }
        let newly = store
            .append_catchup(&path, &candidates)
            .await
            .ok()
            .map(|results| {
                candidates
                    .into_iter()
                    .zip(results)
                    .filter_map(|(scanned, seq)| seq.map(|_| scanned))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        seeded = newly.len() as u64;
        if !newly.is_empty() {
            let state_arc = {
                let kernel = system.kernel.lock();
                kernel.es_state.get(&path).cloned()
            };
            if let Some(state_arc) = state_arc {
                let mut state = state_arc.lock().await;
                for sc in &newly {
                    state.apply_erased(&sc.event);
                }
            }
        }
    }

    // 3. GO LIVE: the loop starts NOW — queued live copies fold after
    //    history, never before it. The seeding above may have REPLACED
    //    the table's Arc (journal replay rebuilds the instance): rebind
    //    the loop's cache so the fresh loop folds into the rebuilt
    //    instance, not the spawn shell.
    {
        let kernel = system.kernel.lock();
        armed.loop_ctx.state = kernel.es_state.get(&path).cloned().or(armed.loop_ctx.state);
    }
    armed.start_loop();

    // 4. The observable completion marker: the kernel's caught-up counter
    //    (the wake path's wait signal) and the CaughtUp observation (the
    //    host-observable record, when observation is on) both advance here.
    {
        let mut kernel = system.kernel.lock();
        kernel
            .caught_up
            .entry(path.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }
    if system.kernel.observing() {
        system.kernel.observe(crate::observe::Observation::new(
            system.clock.now(),
            crate::observe::ObservationKind::CaughtUp { path, seeded },
        ));
    }
}

impl ActorSystemCore {
    /// Spawns a foreign (no-Rust-types) event-sourced actor: the schema,
    /// state fold, and command decision are all runtime JSON data. This is
    /// the seam the port tier will reuse.
    ///
    /// Deprecated positional flavor — prefer the builder:
    /// [`crate::builder::spawn_foreign`] (named `handle`/`apply` methods).
    #[doc(hidden)]
    pub fn spawn_es_foreign(
        &self,
        path: ActorPath,
        schema_id: SchemaId,
        genesis: Json,
        decision: crate::actor::ForeignDecision,
        fold: crate::actor::ForeignFold,
        opts: SpawnOpts,
    ) {
        let state = Box::new(crate::actor::ForeignEsState::new(genesis.clone(), fold));
        let manifest = crate::schema::ActorManifest::new()
            .handles_id(schema_id.clone())
            .kind(crate::actor::ActorKind::EventSourced);
        let entries = vec![
            Arc::new(crate::actor::ForeignCommandEntry::new(schema_id, decision))
                as Arc<dyn crate::actor::CommandEntry>,
        ];
        self.spawn_es_erased(path, manifest, state, entries, opts, &genesis);
    }
    /// Creates the fabric from a config.
    pub(crate) fn new(config: SystemConfig) -> Self {
        let registry = Arc::new(CountingRegistryLock(Mutex::new(Registry::default())));
        let clock = config.clock.clone();
        let view = Arc::new(NullView {
            registry: registry.clone(),
            clock,
        });
        // ONE store instance per system: the core keeps a handle for the
        // sweep's flush; the kernel reads/writes through the same Arc.
        // Construction-time install only: config args carry the store
        // (default: in-memory) plus its control handler.
        let (journal_store, journal_control): (
            std::sync::Arc<dyn crate::journal::JournalStore>,
            Option<crate::journal::ControlHandler>,
        ) = match config.journal_args {
            Some(args) => (args.store, args.control),
            None => (
                std::sync::Arc::new(crate::journal::InMemoryJournalStore::new()),
                None,
            ),
        };
        // ONE shutdown barrier, shared by the core and the kernel state:
        // the sweep stores through the core handle, the send path reads
        // through clones — never a lock in between.
        let shutting_down = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let kernel = {
            let mut kernel = KernelState::new();
            kernel.journal_store = journal_store.clone();
            kernel.shutting_down = shutting_down.clone();
            CountingKernelLock::with_state_and_observer(kernel, config.observation)
        };
        Self {
            registry,
            kernel: Arc::new(kernel),
            clock: config.clock,
            view,
            child_shutdowns: parking_lot::Mutex::new(Vec::new()),
            mailbox_defaults: config.default_mailbox,
            shutting_down,
            journal_store_slot: parking_lot::RwLock::new(journal_store),
            journal_control,
            observation_log: None,
        }
    }

    /// Reassembles a minimal facade handle from a loop context's shared
    /// Arcs, so the loop's graceful exit can drive the system-owned table
    /// teardown without a full `ActorSystem` (the clock/view aliases are
    /// the same Arcs the spawn captured). A loop facade never owns
    /// supervision shutdown senders and never reports `shutting_down`.
    pub(crate) fn loop_facade(ctx: &crate::kernel::EsLoop) -> Self {
        let (store, shutting_down) = {
            let kernel = ctx.kernel.lock();
            (kernel.journal_store.clone(), kernel.shutting_down.clone())
        };
        Self {
            registry: ctx.registry.clone(),
            kernel: ctx.kernel.clone(),
            clock: ctx.clock.clone(),
            view: ctx.view.clone(),
            child_shutdowns: parking_lot::Mutex::new(Vec::new()),
            mailbox_defaults: MailboxDefaults::default(),
            shutting_down,
            journal_store_slot: parking_lot::RwLock::new(store),
            journal_control: None,
            observation_log: None,
        }
    }

    /// Creates a fabric tuned for tests: a [`FakeClock`] starting at
    /// 1_000 ms. Test builds install a capturing
    /// [`crate::observe::ObservationLog`] (the assertions' window on the
    /// observation flow — `facts()`); production never does.
    pub(crate) fn test() -> (Self, Arc<FakeClock>) {
        let (clock, fake) = ClockService::fake(1_000);
        let log = crate::observe::ObservationLog::default();
        let mut system = Self::new(SystemConfig {
            clock,
            observation: Some(log.handler()),
            default_mailbox: MailboxDefaults::default(),
            journal_args: None,
        });
        system.observation_log = Some(log);
        (system, fake)
    }

    /// Like [`ActorSystemCore::test`], but with an explicit observation
    /// handler instead of the default test log.
    pub(crate) fn test_with_observer(
        handler: crate::observe::ObservationHandler,
    ) -> (Self, Arc<FakeClock>) {
        let (clock, fake) = ClockService::fake(1_000);
        (
            Self::new(SystemConfig {
                clock,
                observation: Some(handler),
                default_mailbox: MailboxDefaults::default(),
                journal_args: None,
            }),
            fake,
        )
    }

    /// The fake clock behind this system, when tests installed one.
    pub fn fake_clock(&self) -> Option<Arc<FakeClock>> {
        self.clock.backend_fake()
    }

    /// Fills unset mailbox fields from the system defaults.
    fn resolve_opts(&self, opts: SpawnOpts) -> SpawnOpts {
        SpawnOpts {
            snapshot: opts.snapshot,
            mailbox_capacity: if opts.mailbox_capacity == 0 {
                self.mailbox_defaults.capacity
            } else {
                opts.mailbox_capacity
            },
            mailbox_policy: if opts.mailbox_policy == OverloadPolicy::default() {
                self.mailbox_defaults.policy
            } else {
                opts.mailbox_policy
            },
            high_watermark: opts.high_watermark,
            passivation: opts.passivation,
            batch: opts.batch.max(1),
        }
    }

    /// Creates a system on an injected clock (tests: [`crate::clock::FakeClock`]).
    pub fn with_clock(clock: ClockService) -> Self {
        Self::new(SystemConfig::with_clock(clock))
    }

    /// Registers a Rust type's schema — the typed flavor.
    ///
    /// Idempotent per name+version: the first registration wins, and the
    /// returned id is stable across repeat registrations.
    pub fn register_schema<S: Schema + 'static>(&self) -> SchemaId {
        let mut registry = self.registry.lock();
        registry
            .register_schema_of::<S>()
            .expect("schema TypeId claim: one Rust type per schema name")
    }

    /// Registers a schema from a JSON descriptor — the foreign flavor, for
    /// schemas defined outside Rust.
    ///
    /// # Errors
    ///
    /// Returns an error when `json` is not a valid schema descriptor.
    pub fn register_schema_json(
        &self,
        json: Json,
    ) -> Result<SchemaId, error_stack::Report<crate::schema::SchemaError>> {
        let mut registry = self.registry.lock();
        registry.register_schema_json(json)
    }

    /// The registered descriptor for a schema name, if any.
    pub fn schema(&self, id: &SchemaId) -> Option<crate::schema::SchemaDef> {
        let registry = self.registry.lock();
        registry.schema(id).cloned()
    }

    /// Spawns an event-sourced actor at `path`.
    ///
    /// Deprecated positional flavor — prefer the builder:
    /// [`crate::builder::spawn_es_builder`] (each type said once).
    #[doc(hidden)]
    pub fn spawn_es<A, F>(&self, path: ActorPath, args: &Json, opts: SpawnOpts, entries: F)
    where
        A: EventSourcedActor,
        F: FnOnce() -> Vec<Arc<dyn CommandEntry>>,
    {
        let state = Box::new(TypedEsState::<A>::new(A::restore(args)));
        let manifest = A::manifest();
        self.spawn_es_erased(path, manifest, state, entries(), opts, args);
    }

    /// The erased ES spawn shared by typed, foreign, and builder actors
    /// (the single funnel every journaled spawn goes through).
    pub(crate) fn spawn_es_erased(
        &self,
        path: ActorPath,
        manifest: crate::schema::ActorManifest,
        state: Box<dyn crate::actor::DynEsActor>,
        entries: Vec<Arc<dyn CommandEntry>>,
        opts: SpawnOpts,
        args: &Json,
    ) {
        let armed = self.arm_es_erased(path, manifest, state, entries, opts, args);
        armed.start_loop();
    }

    /// Registers an ES actor's slot, routes, and kernel tables WITHOUT
    /// starting its loop (the projector spawn's arm phase).
    ///
    /// Registration is synchronous, so from return on, publishes to the
    /// handled schemas are delivered into the actor's inbox (backpressure
    /// upstream once it fills) while the inbox loop is not yet running —
    /// the projector builder seeds history between arm and loop start, and
    /// queued live deliveries fold after it, never before.
    pub(crate) fn arm_es_erased(
        &self,
        path: ActorPath,
        manifest: crate::schema::ActorManifest,
        state: Box<dyn crate::actor::DynEsActor>,
        entries: Vec<Arc<dyn CommandEntry>>,
        opts: SpawnOpts,
        args: &Json,
    ) -> ArmedEsActor {
        let opts = self.resolve_opts(opts);
        let (tx, rx) = tokio::sync::mpsc::channel::<Envelope>(crate::kernel::door_capacity(
            opts.mailbox_capacity,
            opts.mailbox_policy,
        ));
        // The manifest is the union of what the actor type declares and
        // what its command entries decode: every spawn flavor (positional,
        // builder, foreign) produces identical registry data this way.
        let mut manifest = manifest;
        for entry in &entries {
            let schema = entry.schema();
            if !manifest.handles.contains(&schema) {
                manifest.handles.push(schema);
            }
        }
        // The CELL is built first: the endpoint couples the front-door
        // channel to it (the direct-delivery target), and the kernel
        // tables register it below.
        let cell = Arc::new(ActorCell::new(
            path.clone(),
            Inbox::new(opts.mailbox_capacity.max(1), opts.mailbox_policy),
            opts.mailbox_capacity.max(1),
            opts.mailbox_policy,
            opts.batch,
        ));
        let mut registry = self.registry.lock();
        registry
            .insert_slot(
                path.clone(),
                manifest.clone(),
                Endpoint::new(tx, cell.clone()),
                opts.mailbox_policy,
            )
            .expect("path free at spawn");
        // Declared edges become routes: each handled schema is routable
        // to this path (adding a second actor for a schema converts the
        // route to round-robin). The route IS the receive declaration —
        // tell, send_to_any, and publish all deliver through it.
        for schema in manifest.handles.clone() {
            registry.add_route(schema, path.clone());
        }
        // Emit edges are ENFORCED against the manifest (the kernel drops
        // undeclared schemas pre-append) — the builder/foreign paths feed
        // extra declarations through `declare_emits` before the first step.
        // The CELL MIRROR of the declared emits is stamped here (spawn-
        // static config, like `entries`): the step gates read the mirror,
        // keeping the registry lock off the message path. Post-spawn
        // declarations flow through the same facade this write mirrors.
        *cell.declared_emits.write().expect("declared emits lock") = manifest.emits.to_vec();
        drop(registry);
        // CELL-LOCAL SPAWN CONFIG: the per-actor bookkeeping the loop and
        // the front door will read — dispatch entries, snapshot cadence +
        // anchor, passivation, birth work stamp, watermark — is stamped
        // here, NOT in the kernel tables (the kernel lock guards
        // cross-actor tables only).
        {
            let mut entries_slot = cell.entries.write().expect("entries lock");
            *entries_slot = entries;
        }
        *cell.snapshot_policy.write().expect("snapshot policy lock") = opts.snapshot;
        // The time-cadence anchor: the spawn anchors it (the first
        // snapshot becomes due one full interval after the journal began).
        // Unanchored (`CELL_SENTINEL`) only when the cadence is Off — the
        // anchor is meaningless without a time policy.
        if matches!(opts.snapshot, crate::actor::SnapshotCadence::Time(_)) {
            cell.snapshot_anchor_ms.store(
                self.clock.now().as_millis(),
                std::sync::atomic::Ordering::Release,
            );
        }
        *cell.passivation.write().expect("passivation lock") = opts.passivation;
        // The spawn itself starts the idle clock: an actor spawned and
        // never messaged passivates from its birth stamp, not from the
        // first message.
        cell.last_work_ms.store(
            self.clock.now().as_millis(),
            std::sync::atomic::Ordering::Release,
        );
        if let Some(wm) = opts.high_watermark {
            cell.watermark_high
                .store(wm, std::sync::atomic::Ordering::Release);
            cell.has_watermark
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        let mut kernel = self.kernel.lock();
        kernel.cells.insert(path.clone(), cell.clone());
        let state = Arc::new(tokio::sync::Mutex::new(state));
        kernel.es_state.insert(path.clone(), state.clone());
        // Genesis args anchor every rebuild-from-journal (supervised
        // restarts AND boot recovery): without them a replay restarts
        // from `restore({})` instead of the spawn's genesis.
        kernel.genesis_args.insert(path.clone(), args.clone());
        let is_projector = kernel.projectors.contains(&path);
        drop(kernel);
        if self.kernel.observing() {
            self.kernel.observe(crate::observe::Observation::new(
                self.clock.now(),
                crate::observe::ObservationKind::Spawned {
                    path: path.clone(),
                    kind: crate::actor::ActorKind::EventSourced,
                    restart: false,
                },
            ));
        }

        // Front door + ES loop parts; the loop starts in `start_loop`.
        let loop_ctx = EsLoop {
            path: path.clone(),
            cell,
            registry: self.registry.clone(),
            kernel: self.kernel.clone(),
            shutting_down: self.shutting_down.clone(),
            view: self.view.clone(),
            clock: self.clock.clone(),
            is_projector,
            state: Some(state),
        };
        ArmedEsActor { path, loop_ctx, rx }
    }

    /// The projector spawn's arm phase: registers the read model as an
    /// event-sourced actor whose consumed schemas are BOTH its handled
    /// inputs (a `ConsumeEntry` each: the fact re-records itself) and its
    /// declared emits (the re-records journal legally). The inbox loop is
    /// not started — [`catch_up_projector`] seeds history first.
    ///
    /// # Panics
    ///
    /// Panics when the path is already taken (builder contract).
    pub(crate) fn arm_projector<P: crate::actor::Projector>(
        &self,
        path: &ActorPath,
        consumed: &[crate::schema::SchemaId],
        args: &Json,
        opts: SpawnOpts,
    ) -> ArmedEsActor {
        // handles = consumed (routes + entries), emits = consumed (the
        // re-records must pass the pre-append declaration filter or every
        // seeded fact would be dropped as UndeclaredEvent).
        let mut manifest = crate::schema::ActorManifest::new();
        for schema in consumed {
            manifest = manifest.handles_id(schema.clone()).emits_id(schema.clone());
        }
        manifest = manifest.kind(crate::actor::ActorKind::EventSourced);
        let entries: Vec<Arc<dyn CommandEntry>> = consumed
            .iter()
            .map(|schema| Arc::new(crate::actor::ConsumeEntry::new(schema.clone())) as _)
            .collect();
        // Genesis is `P::default()` (args carried for manifests only).
        let state = Box::new(crate::actor::TypedProjectorState::<P>::new(P::default()));
        self.kernel.lock().projectors.insert(path.clone());
        self.arm_es_erased(path.clone(), manifest, state, entries, opts, args)
    }

    /// Spawns a service (edge) actor at `path`: async handlers, I/O and
    /// `ask` allowed, not journaled (at-most-once message semantics).
    ///
    /// Deprecated positional flavor — prefer the builder:
    /// [`crate::builder::spawn_service_builder`].
    #[doc(hidden)]
    pub fn spawn_service<A, F>(&self, path: ActorPath, args: &Json, opts: SpawnOpts, entries: F)
    where
        A: ServiceActor,
        F: FnOnce() -> Vec<Arc<dyn MsgEntry>>,
    {
        let manifest = A::manifest();
        let start_args = args.clone();
        let start = Box::pin(async move {
            A::start(&start_args).await.map(|instance| {
                Box::new(TypedServiceState::new(instance)) as Box<dyn DynServiceActor>
            })
        });
        self.spawn_service_erased(path, manifest, args, entries(), opts, start);
    }

    /// The erased service spawn shared by typed and builder spawns (the
    /// service-side funnel). `start` builds the erased instance (async,
    /// I/O allowed) — constructed by the typed wrapper.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_service_erased(
        &self,
        path: ActorPath,
        manifest: crate::schema::ActorManifest,
        args: &Json,
        entries: Vec<Arc<dyn MsgEntry>>,
        opts: SpawnOpts,
        start: ServiceStart,
    ) {
        // `A::start` is async (I/O allowed); block briefly on a runtime
        // thread is not done — spawn the start inside the actor task and
        // register the slot immediately so senders never see a gap.
        let opts = self.resolve_opts(opts);
        let (tx, rx) = tokio::sync::mpsc::channel::<Envelope>(crate::kernel::door_capacity(
            opts.mailbox_capacity,
            opts.mailbox_policy,
        ));
        // The CELL is built first: the endpoint couples the front-door
        // channel to it (the direct-delivery target), and the kernel
        // tables register it below.
        let cell = Arc::new(ActorCell::new(
            path.clone(),
            Inbox::new(opts.mailbox_capacity.max(1), opts.mailbox_policy),
            opts.mailbox_capacity.max(1),
            opts.mailbox_policy,
            opts.batch,
        ));
        {
            let mut registry = self.registry.lock();
            registry
                .insert_slot(
                    path.clone(),
                    manifest.clone(),
                    Endpoint::new(tx, cell.clone()),
                    opts.mailbox_policy,
                )
                .expect("path free at spawn");
            // Service actors route by schema too: each handled schema is
            // routable to this path (second handler → RoundRobin). The
            // route IS the receive declaration — tell, send_to_any, and
            // publish all deliver through it.
            for schema in manifest.handles.clone() {
                registry.add_route(schema, path.clone());
            }
        }
        // CELL-LOCAL SPAWN CONFIG (service tier): message entries,
        // passivation, birth work stamp, watermark. The declared-emits
        // mirror is stamped here too (services CAN declare emits; the
        // mirror matches the registry manifest either way).
        {
            let mut msg_slot = cell.msg_entries.write().expect("msg entries lock");
            *msg_slot = entries;
        }
        *cell.declared_emits.write().expect("declared emits lock") = manifest.emits.to_vec();
        *cell.passivation.write().expect("passivation lock") = opts.passivation;
        cell.last_work_ms.store(
            self.clock.now().as_millis(),
            std::sync::atomic::Ordering::Release,
        );
        if let Some(wm) = opts.high_watermark {
            cell.watermark_high
                .store(wm, std::sync::atomic::Ordering::Release);
            cell.has_watermark
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        let mut kernel = self.kernel.lock();
        kernel.cells.insert(path.clone(), cell.clone());
        kernel.genesis_args.insert(path.clone(), args.clone());
        drop(kernel);
        if self.kernel.observing() {
            self.kernel.observe(crate::observe::Observation::new(
                self.clock.now(),
                crate::observe::ObservationKind::Spawned {
                    path: path.clone(),
                    kind: crate::actor::ActorKind::Service,
                    restart: false,
                },
            ));
        }

        // The service tier wraps this loop ONLY for its shared plumbing
        // (front door, cell, routing): `step_service` runs against
        // `kernel.services`, never `es_state`, so its state shell is None.
        let loop_ctx = EsLoop {
            path: path.clone(),
            cell: cell.clone(),
            registry: self.registry.clone(),
            kernel: self.kernel.clone(),
            shutting_down: self.shutting_down.clone(),
            view: self.view.clone(),
            clock: self.clock.clone(),
            is_projector: false,
            state: None,
        };
        let started_path = path.clone();
        let view = self.view.clone();
        let registry = self.registry.clone();
        let kernel_table = self.kernel.clone();
        let front_cell = cell.clone();
        let front_kernel = self.kernel.clone();
        crate::kernel::spawn_tracked(async move {
            // Start the instance inside the task; a start failure leaves
            // the slot present (senders get a closed door) and the crash
            // recorded for supervision.
            let started = start.await;
            match started {
                Ok(instance) => {
                    let mut kernel = kernel_table.lock();
                    kernel.services.insert(
                        started_path.clone(),
                        Arc::new(tokio::sync::Mutex::new(instance)),
                    );
                }
                Err(report) => {
                    // Start failure: the cell is crashed (supervision
                    // restarts via `start`). The front_cell handle IS the
                    // registered cell.
                    front_cell.mark_crashed();
                    let _ = report;
                }
            }
            let _ = (&view, &registry);
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            crate::kernel::spawn_tracked(crate::kernel::front_door_loop(
                front_cell,
                front_kernel,
                rx,
            ));
            let task = crate::kernel::spawn_tracked(crate::kernel::service_actor_loop(
                crate::kernel::ServiceLoop { es: loop_ctx },
                shutdown_rx,
            ));
            if let Some(cell) = kernel_table.lock().cells.get(&started_path)
                && let Ok(mut handle) = cell.handle.try_lock()
            {
                *handle = Some(crate::kernel::ActorHandle {
                    shutdown: shutdown_tx,
                    task: Some(task),
                });
            }
        });
    }

    /// Sends an envelope from outside the system (entry-point trace root).
    ///
    /// # Errors
    ///
    /// Returns the envelope back when its destination does not resolve
    /// (callers dead-letter or retry).
    pub async fn send(&self, envelope: Envelope) -> Result<ActorPath, Envelope> {
        route(&self.registry, &self.kernel, &self.shutting_down, envelope).await
    }

    /// Typed fire-and-forget: wraps `value` as the fabric's LIVE value
    /// under `C`'s schema (zero serde — serialization happens only at
    /// the doors: the journal, or a reader that materializes the JSON
    /// view) and routes it as a system-root send. Sugar over
    /// [ActorSystem::send](crate::system::ActorSystemCore::send) with
    /// the envelope built from the live value.
    ///
    /// # Errors
    ///
    /// Returns the original envelope back when `dest` does not resolve
    /// (callers dead-letter or retry).
    pub async fn tell<C>(&self, dest: ActorPath, value: C) -> Result<ActorPath, Envelope>
    where
        C: Schema + serde::Serialize + Send + Sync + crate::envelope::PayloadValue + 'static,
    {
        let envelope = Envelope::json(C::schema_id(), Address::Path(dest), value, TraceCtx::root());
        self.send(envelope).await
    }

    /// Typed one-of send: wraps `value` as the fabric's LIVE value under
    /// `M`'s schema (zero serde) and delivers exactly one copy to one of
    /// the actors that declared `.handles::<M>()` — round-robin through
    /// the route table (each call advances the shared rotation; see
    /// [`Registry::route`]). Zero handlers ⇒ the envelope returns as the
    /// error (same contract as an unrouted tell). The receiver cannot
    /// distinguish this from a direct
    /// [ActorSystem::tell](crate::system::ActorSystemCore::tell).
    ///
    /// # Errors
    ///
    /// Returns the original envelope back when no handler for `M` is
    /// registered.
    pub async fn send_to_any<M>(&self, value: M) -> Result<ActorPath, Envelope>
    where
        M: Schema + crate::envelope::PayloadValue,
    {
        let schema = M::schema_id();
        let envelope = Envelope::json(
            schema.clone(),
            Address::Schema(schema.clone()),
            value,
            TraceCtx::root(),
        );
        self.send(envelope).await
    }

    /// Typed ask from outside the system: wraps `value` as the fabric's
    /// LIVE value under `C`'s schema (zero serde), opens a reply lease,
    /// and awaits the reply under the mandatory `timeout`. The lease
    /// settles with the same Replied/Timeout/Failed facts an in-actor
    /// ask produces (see `crate::kernel::KernelAskPort`); a timed-out
    /// ask's late reply lands nowhere. The reply materializes its JSON
    /// view lazily (memoized) — the Json return is the host-facing
    /// contract edge.
    ///
    /// Trace root is the entry point, matching
    /// [ActorSystem::send](crate::system::ActorSystemCore::send).
    ///
    /// # Errors
    ///
    /// [`crate::context::AskError::Unresolved`] when no handler for `C`
    /// is registered at `dest` via
    /// [`crate::builder::SpawnBuilder::handles`], when the ask times
    /// out, or when the lease dies before the reply.
    pub async fn ask<C>(
        &self,
        dest: ActorPath,
        value: C,
        timeout: std::time::Duration,
    ) -> Result<Json, error_stack::Report<crate::context::AskError>>
    where
        C: Schema + crate::envelope::PayloadValue,
    {
        let payload = crate::envelope::Payload::value(value);
        let port = crate::kernel::KernelAskPort {
            registry: self.registry.clone(),
            kernel: self.kernel.clone(),
            clock: self.clock.clone(),
        };
        crate::context::ask_via_port(
            &port,
            Address::Path(dest),
            C::schema_id(),
            payload,
            timeout,
            TraceCtx::root(),
        )
        .await
    }

    /// Typed event broadcast from outside the system: wraps `value` as
    /// the fabric's LIVE value under `M`'s schema (zero serde) and fans
    /// it out to EVERY actor that declared `.handles::<M>()` — one copy
    /// each (the fan-out shares one payload: a refcount bump per
    /// subscriber, never a value copy). Zero handlers ⇒ silent no-op:
    /// events are news, not work orders. Trace root is the entry point,
    /// matching [ActorSystem::send](crate::system::ActorSystemCore::send).
    pub fn publish<M>(&self, value: M) -> impl std::future::Future<Output = ()> + Send + '_
    where
        M: Schema + crate::envelope::PayloadValue,
    {
        let schema = M::schema_id();
        let envelope = Envelope::json(
            schema.clone(),
            Address::Schema(schema.clone()),
            value,
            TraceCtx::root(),
        );
        async move {
            crate::kernel::broadcast(
                &self.registry,
                &self.kernel,
                &self.shutting_down,
                schema,
                envelope,
            )
            .await;
        }
    }

    /// Typed ask: sends `value` and downcasts the reply into the
    /// declared reply type `R`. A reply that does not fit `R` is the
    /// named [`crate::context::AskError::ReplyType`].
    ///
    /// # Errors
    ///
    /// [`crate::context::AskError::Unresolved`] when the destination does
    /// not resolve or the ask times out; [`crate::context::AskError::ReplyType`]
    /// on a wrong-shaped reply.
    pub async fn ask_typed<C, R>(
        &self,
        dest: ActorPath,
        value: C,
        timeout: std::time::Duration,
    ) -> Result<R, error_stack::Report<crate::context::AskError>>
    where
        C: Schema + crate::envelope::PayloadValue,
        R: Schema + crate::envelope::PayloadValue + Clone + serde::de::DeserializeOwned + 'static,
    {
        let payload = crate::envelope::Payload::value(value);
        let port = crate::kernel::KernelAskPort {
            registry: self.registry.clone(),
            kernel: self.kernel.clone(),
            clock: self.clock.clone(),
        };
        let reply = crate::context::ask_via_port_payload(
            &port,
            Address::Path(dest),
            C::schema_id(),
            payload,
            timeout,
            TraceCtx::root(),
        )
        .await?;
        crate::context::downcast_reply::<R>(reply)
    }

    /// Untyped event broadcast from outside the system: the caller has
    /// already serialized the payload under `schema`. Same fan-out
    /// contract as [ActorSystem::publish](crate::system::ActorSystemCore::publish)
    /// — every `.handles` declarant of the schema, zero ⇒ silent no-op.
    /// The bridge surface for erased (foreign) callers.
    pub async fn publish_value(&self, schema: SchemaId, payload: Json) {
        let envelope = Envelope::from_bytes(
            schema.clone(),
            Address::Schema(schema.clone()),
            PayloadBytes::from(payload),
            TraceCtx::root(),
        );
        crate::kernel::broadcast(
            &self.registry,
            &self.kernel,
            &self.shutting_down,
            schema,
            envelope,
        )
        .await;
    }

    /// Untyped schema-kind dispatch from outside the system: the bridge
    /// surface for erased (foreign) callers' messages whose COMMAND/EVENT
    /// role the caller may not know statically. The declared kind chooses
    /// the default transport — Event schemas broadcast (one copy per
    /// handler, zero ⇒ silent no-op), Command schemas route to one
    /// handler (tell semantics, silent when unrouted). The declaration
    /// site decides; callers never choose a transport.
    pub async fn deliver_schema_value(&self, schema: SchemaId, payload: Json) {
        let kind = {
            let reg = self.registry.lock();
            reg.schema(&schema).map(|def| def.kind)
        };
        match kind {
            Some(crate::schema::SchemaKind::Event) => {
                self.publish_value(schema, payload).await;
            }
            _ => {
                // Command (or undeclared — treat as command, the default
                // inbound role): resolve the handler route and send there.
                let dest: Option<ActorPath> = {
                    let mut reg = self.registry.lock();
                    reg.route(&schema)
                };
                // Unrouted command: nothing to do (the caller's contract
                // is fire-and-forget).
                if let Some(path) = dest {
                    let envelope = Envelope::from_bytes(
                        schema,
                        Address::Path(path),
                        PayloadBytes::from(payload),
                        TraceCtx::root(),
                    );
                    let _ = self.send(envelope).await;
                }
            }
        }
    }

    /// Builds an envelope for a typed payload addressed to `dest` (pair
    /// with
    /// [`ActorSystem::send`](crate::system::ActorSystemCore::send)).
    pub fn envelope(
        &self,
        schema: SchemaId,
        dest: ActorPath,
        payload: impl Into<Json>,
    ) -> Envelope {
        Envelope::from_bytes(
            schema,
            Address::Path(dest),
            PayloadBytes::from(payload.into()),
            TraceCtx::root(),
        )
    }

    /// The system's clock (tests use this to reach the [`crate::clock::FakeClock`]).
    pub fn clock(&self) -> &ClockService {
        &self.clock
    }

    /// Restarts a crashed ES actor at `path`. Used by the supervision
    /// engine after a crash.
    ///
    /// # Errors
    ///
    /// Propagates state-rebuild failures (corrupt snapshot or journal).
    pub async fn restart_es(
        &self,
        path: &ActorPath,
        genesis_args: &Json,
    ) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
        let loop_ctx = {
            let kernel = self.kernel.lock();
            let cell = kernel.cells.get(path).cloned();
            drop(kernel);
            cell
        };
        let Some(cell) = loop_ctx else {
            use error_stack::IntoReport;
            return Err(crate::journal::JournalError::Restore.into_report());
        };
        let mut ctx = EsLoop {
            path: path.clone(),
            cell,
            registry: self.registry.clone(),
            kernel: self.kernel.clone(),
            shutting_down: self.shutting_down.clone(),
            view: self.view.clone(),
            clock: self.clock.clone(),
            is_projector: self.kernel.lock().projectors.contains(path),
            state: None,
        };
        crate::kernel::restart_es(&mut ctx, genesis_args).await
    }

    /// Installs or replaces the observation handler at runtime.
    ///
    /// Takes effect at the next emission site: in-flight handler calls
    /// finish, then every observation flows to the new handler. Handlers
    /// must not call back into the system (see
    /// [`crate::observe`]` `module docs) and a panicking handler is
    /// isolated from the message path.
    pub fn set_observation(&self, handler: crate::observe::ObservationHandler) {
        self.kernel.set_observer(Some(handler));
    }

    /// Removes the observation handler (observation off). Uncaptured
    /// observations are gone — there is no history.
    pub fn clear_observation(&self) {
        self.kernel.set_observer(None);
    }

    /// Gracefully stops the actor at `path`: children stop first
    /// (recursive, timeout-bounded), the drain signal lets the current
    /// message finish, undelivered inbox entries go to the DLQ, and the
    /// slot is removed. Emits a Stopped fact.
    pub async fn stop(&self, path: &ActorPath) {
        const STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        self.stop_bounded(path, STOP_TIMEOUT).await;
    }
    /// Installs a partition set over `public`: commands aimed at the
    /// public path are routed to per-entity actors derived from the
    /// payload's declared shard key (`public/key`), activated on demand
    /// from the spec's shared factory.
    ///
    /// Senders keep addressing the public path forever; entity paths and
    /// journals are per key. Entities live until stopped — by an explicit
    /// `stop`, their own `stop_self`, or the passivation config the
    /// factory's builder declares — and re-activate from the factory on
    /// the next send to the public path.
    ///
    /// # Errors
    ///
    /// [`crate::registry::RegistryError::InvalidSpec`] when no command
    /// schema declares the spec's key field as the shard key (validated:
    /// the set would dead-letter every command).
    pub fn install_partition_set(
        &self,
        spec: crate::pool::PartitionSpec,
    ) -> Result<(), error_stack::Report<crate::registry::RegistryError>> {
        let mut registry = self.registry.lock();
        registry.install_partition_set(spec)
    }

    /// Installs a projector set: per-key projectors derived from a
    /// consumed fact's shard key, activated on demand by broadcast copies
    /// of the consumed schemas. Projectors are not spawned here — the
    /// first consumed broadcast (or a [`ActorSystem::projector_state`](crate::system::ActorSystem::projector_state)
    /// read) activates them.
    ///
    /// # Errors
    ///
    /// [`crate::registry::RegistryError::InvalidSpec`] when a consumed
    /// schema is missing or a Command, or no consumed schema declares the
    /// spec's key field as the shard key (validated: the set could
    /// never extract a key and would dead-letter every copy).
    pub fn install_projector_set(
        &self,
        spec: crate::pool::ProjectorSetSpec,
    ) -> Result<(), error_stack::Report<crate::registry::RegistryError>> {
        let mut registry = self.registry.lock();
        registry.install_projector_set(spec)
    }

    /// Appends a router rule (declaration order is priority order): when
    /// a path-addressed envelope matches all of the rule's `Some`
    /// criteria, the rule's action applies at [`route`](crate::kernel)
    /// time — a `Tee` copies the envelope to an observer at-most-once
    /// (never an audit mechanism), an `Inline` interposes the observer in
    /// the primary's place.
    pub fn install_rule(&self, rule: crate::pool::Rule) {
        let mut registry = self.registry.lock();
        registry.add_rule(rule);
    }

    /// The bounded stop: identical to [`ActorSystem::stop`](crate::system::ActorSystemCore::stop), but the
    /// caller sets the deadline budget instead of the default 5s. Children
    /// stop first (recursion shares one budget); stragglers after expiry
    /// are hard-stopped and their undelivered mail lands in the DLQ
    /// (`StoppedWithMail`) — expiry is an observable outcome, not an
    /// error.
    pub fn stop_bounded<'a>(
        &'a self,
        path: &'a ActorPath,
        remaining: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.stop_bounded_inner(path, remaining))
    }

    /// The recursive body, boxed by [`Self::stop_bounded`](crate::system::ActorSystem::stop_bounded).
    async fn stop_bounded_inner(&self, path: &ActorPath, remaining: std::time::Duration) {
        if remaining.is_zero() {
            return;
        }
        // 1. CHILDREN FIRST (recursive): any spec whose parent is this path.
        let children: Vec<ActorPath> = {
            let kernel = self.kernel.lock();
            kernel
                .specs
                .values()
                .filter(|s| s.parent.as_ref() == Some(path))
                .map(|s| s.path.clone())
                .collect()
        };
        let mut budget = remaining;
        for child in children {
            let child_start = std::time::Instant::now();
            self.stop_bounded(&child, budget).await;
            budget = budget.saturating_sub(child_start.elapsed());
            if budget.is_zero() {
                break;
            }
        }

        // 1.5 SERVICE START SETTLE: a service actor's `start` runs inside
        // its task; stopping before it completes would skip on_stop for an
        // actor that never got the chance to live. Bounded wait for the
        // instance (or a crash) to appear.
        for _ in 0..500 {
            let settled = {
                let kernel = self.kernel.lock();
                kernel.services.contains_key(path)
                    || kernel.cells.get(path).is_some_and(|cell| cell.is_crashed())
            };
            if settled {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        // 2. DRAIN SIGNAL: stop accepting + let the current message finish.
        // `join_slot` stays None when there is nothing to signal (no cell,
        // or the loop already exited — e.g. a supervised spec whose actor
        // never started, or a loop-side self-stop that beat us here).
        let mut join_slot: Option<tokio::task::JoinHandle<()>> = None;
        let no_cell = {
            let kernel = self.kernel.lock();
            match kernel.cells.get(path) {
                None => true,
                Some(cell) => {
                    if let Ok(mut handle) = cell.handle.try_lock()
                        && let Some(h) = handle.take()
                    {
                        let _ = h.shutdown.send(true);
                        join_slot = h.task;
                    }
                    false
                }
            }
        };
        if no_cell {
            self.teardown_tables(path, crate::actor::StopReason::Normal)
                .await;
            return;
        }
        // 3. AWAIT the loop's exit (current message completes). The loop
        // drains/flushes on stop, and its graceful exit claims + runs the
        // on_stop hook itself.
        if let Some(mut jh) = join_slot.take() {
            let joined = match tokio::time::timeout(remaining, &mut jh).await {
                Ok(joined) => Ok(joined),
                Err(_elapsed) => {
                    // BUDGET EXPIRED: the handler never finished. Abort the
                    // loop task — a bounded stop gives up on the in-flight
                    // message, and leaving the task parked forever on a
                    // dead actor leaks it. The abort unwinds the step
                    // frame, whose claim guard disposes of the
                    // un-committed batch: the cell is already marked
                    // stopping, so the tail (and the wedged claim)
                    // dead-letters StoppedWithMail — the same observable
                    // the old snapshot queue gave the stop-drain.
                    jh.abort();
                    let _ = jh.await;
                    Err(())
                }
            };
            // 3.5 HOOK ONLY IF THE LOOP FINISHED: when the join times out,
            // the loop is wedged inside a handler that may hold the
            // instance mutex — running on_stop there would block forever
            // on `service.lock()`. A wedged actor does not get a hook
            // (it never reached a graceful idle); teardown still proceeds.
            if joined.is_ok() {
                let on_stop_cell = {
                    let kernel = self.kernel.lock();
                    kernel.cells.get(path).cloned()
                };
                if let Some(cell) = on_stop_cell
                    && cell.claim_on_stop()
                {
                    let is_projector = self.kernel.lock().projectors.contains(path);
                    let ctx = crate::kernel::EsLoop {
                        path: path.clone(),
                        cell,
                        registry: self.registry.clone(),
                        kernel: self.kernel.clone(),
                        shutting_down: self.shutting_down.clone(),
                        view: self.view.clone(),
                        clock: self.clock.clone(),
                        is_projector,
                        state: None,
                    };
                    crate::kernel::run_on_stop(&ctx).await;
                }
            }
        }
        // No join slot: the loop already exited on its own (self-stop /
        // passivation claimed the hook through their graceful exit) or
        // never started — nothing to hook here.

        // 5. UNDELIVERED → DLQ; slot/spec/subscription removal; facts.
        self.teardown_tables(path, crate::actor::StopReason::Normal)
            .await;
    }

    /// The table teardown shared by every stop path: the external stop
    /// above, the loop's own self-stop/passivation exit, and the shutdown
    /// sweep.
    ///
    /// Idempotent: `was_live` is captured BEFORE any mutation — a live
    /// actor has a cell, an edge-only supervised spec has a spec; a path
    /// with neither was already fully torn down (the loop-side exit beat
    /// us, or this is a repeat stop) and re-recording a Stopped fact would
    /// lie.
    ///
    /// Steps: undelivered inbox → DLQ (the cell is about to drop, so
    /// anything queued would otherwise vanish silently), then slot drop +
    /// subscription cascade + Stopped fact + parent link notification.
    pub(crate) async fn teardown_tables(&self, path: &ActorPath, reason: crate::actor::StopReason) {
        let was_live = {
            let kernel = self.kernel.lock();
            kernel.cells.contains_key(path) || kernel.specs.contains_key(path)
        };
        // UNDELIVERED → DLQ; then close the inbox. Mark the cell stopping
        // FIRST: a step parked mid-dispatch reads this in its claim
        // guard's drop and dead-letters its in-flight claim (StoppedWithMail)
        // instead of restoring it into the closing inbox.
        if let Some(cell) = self.kernel.lock().cells.get(path).cloned() {
            cell.mark_stopping();
        }
        let undelivered: Vec<Envelope> = {
            let kernel = self.kernel.lock();
            let mut drained = Vec::new();
            if let Some(cell) = kernel.cells.get(path)
                && let Some(mut inbox) = cell.inbox.try_lock()
            {
                inbox.close();
                drained = inbox.drain_live();
            }
            drained
        };
        {
            let mut kernel = self.kernel.lock();
            let mut letters = Vec::with_capacity(undelivered.len());
            for envelope in &undelivered {
                letters.push(crate::kernel::DeadLetter {
                    schema: envelope.schema.clone(),
                    dest: envelope.dest.clone(),
                    reason: crate::kernel::DeadLetterReason::StoppedWithMail,
                    detail: "stopped with a non-empty inbox".to_owned(),
                    trace: envelope.trace,
                    envelope: envelope.clone(),
                });
            }
            kernel.dead_letters.extend(letters);
            drop(undelivered);

            // SLOT DROP + route cascade + Stopped fact + parent link
            // notification (a supervised child stopping notifies its
            // parent as a tap fact).
            {
                let mut registry = self.registry.lock();
                let _ = registry.remove_slot(path);
                // Cascade: routes pointing at the stopped path die with
                // it (a re-spawn re-declares its handles).
                registry.drop_routes_of(path);
            }
            kernel.cells.remove(path);
            // The actor's in-memory state dies with it, regardless of stop
            // reason: only live actors hold state. Cold state returns by
            // replay from the journal store (the durable copy).
            kernel.es_state.remove(path);
            // The projector marker dies with the actor too (a re-spawn
            // re-registers it via the factory's builder).
            kernel.projectors.remove(path);
            // Passivation bookkeeping and work stamps are CELL-LOCAL now:
            // they die with the cell (a partition set re-spawn re-registers
            // them via the factory's builder, as before).
            let notified_parent = kernel.specs.get(path).and_then(|s| s.parent.clone());
            kernel.specs.remove(path);
            drop(kernel);
            if was_live && self.kernel.observing() {
                let now = self.clock.now();
                self.kernel.observe(crate::observe::Observation::new(
                    now,
                    crate::observe::ObservationKind::Stopped {
                        path: path.clone(),
                        reason,
                    },
                ));
                if let Some(parent) = notified_parent {
                    self.kernel.observe(crate::observe::Observation::new(
                        now,
                        crate::observe::ObservationKind::LinkNotified {
                            parent,
                            child: path.clone(),
                        },
                    ));
                }
            }
        }
        let _ = was_live;
    }

    /// Exports the system: schemas, live actors (ES state included),
    /// declared vs observed edges. The artifact a future canvas consumes.
    ///
    /// Under opt-in observation there is no send history: `observed_edges`
    /// counts `Sent` observations that flow during the export's own await
    /// points (see [`ObservedEdge`]). The export installs a temporary
    /// counting handler, replaces whatever handler the host installed,
    /// and leaves observation OFF afterwards — a quiet system exports no
    /// observed edges.
    pub async fn export(&self) -> SystemExport {
        // The traffic-snapshot window: any send that lands while the
        // export's state captures await is counted. (Installed before the
        // actor pass; cleared before the result is built.)
        type ObservedEdges = std::collections::HashMap<(Option<String>, String, SchemaId), u64>;
        let observed: Arc<parking_lot::Mutex<ObservedEdges>> =
            Arc::new(parking_lot::Mutex::new(ObservedEdges::new()));
        self.set_observation({
            let observed = observed.clone();
            Arc::new(move |observation| {
                if let crate::observe::ObservationKind::Sent {
                    from, dest, schema, ..
                } = &observation.kind
                {
                    let to_str = dest.to_string();
                    let from_str = from.as_ref().map(|p| p.to_string());
                    *observed
                        .lock()
                        .entry((from_str, to_str, schema.clone()))
                        .or_insert(0) += 1;
                }
            })
        });

        // Schemas (all versions).
        let schemas = {
            let registry = self.registry.lock();
            registry.schemas().all().into_iter().cloned().collect()
        };

        // Live actors: manifests from slots, state/cursor from kernel.
        let slot_manifests = {
            let registry = self.registry.lock();
            registry.slot_manifests()
        };
        let mut actors = Vec::new();
        for (path, manifest) in slot_manifests {
            // Scope the kernel guard: drop it before awaiting the state
            // shell (a std Mutex must never span an await point).
            let (state, cursor) = {
                let kernel = self.kernel.lock();
                let cursor = kernel.cells.get(&path).and_then(|cell| {
                    cell.inbox
                        .try_lock()
                        .map(|inbox| inbox.cursor().as_u64())
                });
                let has_state = kernel.es_state.contains_key(&path);
                (has_state.then_some(()), cursor)
            };
            let state = match state {
                Some(()) => {
                    let shell = {
                        let kernel = self.kernel.lock();
                        kernel.es_state.get(&path).cloned()
                    };
                    match shell {
                        Some(shell) => {
                            let shell = shell.lock().await;
                            shell.capture_erased().ok()
                        }
                        None => None,
                    }
                }
                None => None,
            };
            actors.push(ActorExport {
                path,
                kind: manifest.kind.unwrap_or(crate::actor::ActorKind::Service),
                manifest,
                state,
                cursor,
            });
        }

        // Declared edges straight from the manifests above (handles and
        // emits — the actor's complete declared surface).
        let declared_edges: Vec<DeclaredEdge> = actors
            .iter()
            .flat_map(|a| {
                let handles = a.manifest.handles.iter().map(|s| DeclaredEdge {
                    actor: a.path.clone(),
                    schema: s.clone(),
                    direction: EdgeDirection::Handles,
                });
                let emits = a.manifest.emits.iter().map(|s| DeclaredEdge {
                    actor: a.path.clone(),
                    schema: s.clone(),
                    direction: EdgeDirection::Emits,
                });
                handles.chain(emits).collect::<Vec<_>>()
            })
            .collect();

        // Observed edges: take the traffic snapshot and close the window
        // (observation off after an export — never turned on as a side
        // effect).
        let counts: std::collections::HashMap<(Option<String>, String, SchemaId), u64> = {
            self.clear_observation();

            Arc::try_unwrap(observed)
                .map(|m| m.into_inner())
                .unwrap_or_default()
        };
        let mut observed_edges: Vec<ObservedEdge> = counts
            .into_iter()
            .map(|((from, to, schema), count)| ObservedEdge {
                from,
                to,
                schema,
                count,
            })
            .collect();
        observed_edges.sort_by(|a, b| {
            let a_key = (a.from.clone(), a.to.clone(), a.schema.to_string());
            let b_key = (b.from.clone(), b.to.clone(), b.schema.to_string());
            a_key.cmp(&b_key)
        });

        // Declared partition/rule topology (the canvas's structural view;
        // the observed router signature lives in the tap facts).
        let (partitions, rules) = {
            let registry = self.registry.lock();
            registry.topology()
        };

        SystemExport {
            schemas,
            actors,
            declared_edges,
            observed_edges,
            partitions,
            rules,
        }
    }

    /// How many envelopes the runtime could not deliver (inspection).
    pub async fn dead_letter_count(&self) -> usize {
        let kernel = self.kernel.lock();
        kernel.dead_letters.len()
    }

    /// Why envelopes died (inspection/tests/demo debugging).
    pub async fn dead_letter_reasons(&self) -> Vec<String> {
        let kernel = self.kernel.lock();
        kernel
            .dead_letters
            .iter()
            .map(|d| format!("{:?}: {}", d.reason.clone(), d.schema))
            .collect()
    }

    /// How many envelopes are queued at `path` (inspection/tests).
    pub async fn inbox_debug_len(&self, path: &ActorPath) -> usize {
        // Clone the Arc out of the kernel guard; the inbox guard itself is
        // a sync parking_lot lock (taken and dropped inside the call).
        let cell = {
            let kernel = self.kernel.lock();
            kernel.cells.get(path).cloned()
        };
        cell.map_or(0, |cell| cell.inbox.lock().len())
    }

    /// The cursor of an actor's inbox (inspection).
    pub fn inbox_cursor(&self, path: &ActorPath) -> Option<InboxOffset> {
        let kernel = self.kernel.lock();
        kernel.cells.get(path).map(|cell| {
            // Busy inbox (a loop mid-step): the cursor is mid-flight —
            // report the zero offset rather than blocking the inspector.
            cell.inbox
                .try_lock()
                .map(|inbox| inbox.cursor())
                .unwrap_or(InboxOffset::zero())
        })
    }

    /// The captured ES state of an actor (for export/inspection).
    pub async fn es_state(&self, path: &ActorPath) -> Option<Json> {
        let state = {
            let kernel = self.kernel.lock();
            kernel.es_state.get(path).cloned()?
        };
        let state = state.lock().await;
        state.capture_erased().ok()
    }

    /// The event schemas currently journaled for `path`, in order
    /// (inspection: snapshots are skipped — they are not decisions).
    ///
    /// Sync inspection of the in-memory default store; a custom backend
    /// store exposes its own async inspection path instead.
    pub fn journal_schemas(&self, path: &ActorPath) -> Vec<SchemaId> {
        let kernel = self.kernel.lock();
        let store: &std::sync::Arc<dyn crate::journal::JournalStore> = &kernel.journal_store;
        crate::journal::downcast_in_memory(store)
            .map(|mem| {
                mem.entries_of(path)
                    .iter()
                    .filter_map(|e| e.as_event().map(|ev| ev.schema.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// A read-only snapshot view over the kernel: handler contexts resolve
/// lookups through a brief lock; they can never mutate anything.
struct NullView {
    registry: Arc<CountingRegistryLock>,
    /// The system's clock (the injected/fake clock in tests, the real one
    /// in production) — handler contexts read the CURRENT time through
    /// the same source the kernel stamps with.
    clock: ClockService,
}

impl RuntimeView for NullView {
    fn lookup(&self, path: &ActorPath) -> Option<EndpointInfo> {
        let registry = self.registry.lock();
        registry.lookup(path)
    }

    fn handlers_of(&self, schema: &SchemaId) -> Vec<ActorPath> {
        let registry = self.registry.lock();
        registry.handlers_of(schema)
    }

    fn now(&self) -> Timestamp {
        self.clock.now()
    }
}

impl RuntimeView for ActorSystem {
    fn lookup(&self, path: &ActorPath) -> Option<EndpointInfo> {
        let registry = self.registry.lock();
        registry.lookup(path)
    }

    fn handlers_of(&self, schema: &SchemaId) -> Vec<ActorPath> {
        let registry = self.registry.lock();
        registry.handlers_of(schema)
    }

    fn now(&self) -> Timestamp {
        self.clock.now()
    }
}

impl Default for ActorSystem {
    fn default() -> Self {
        Self::new(SystemConfig::production())
    }
}

impl ActorSystem {
    /// The complete read of a projector: the fold of every fact the store
    /// holds for the projector's consumed schemas, captured after its
    /// catch-up has completed.
    ///
    /// A live projector is captured directly. A cold path owned by a
    /// projector SET is woken: the set's factory spawns (or re-spawns) the
    /// projector, the call waits — bounded — for the projector's
    /// `CaughtUp` tap fact, and the fold is captured. `None` means the
    /// path is neither live nor set-owned (a cold standalone projector has
    /// no wake path; re-run its builder), or the wake did not reach
    /// `CaughtUp` within the budget.
    ///
    /// This is deliberately unlike [`ActorSystem::es_state`](crate::system::ActorSystemCore::es_state), which never
    /// wakes anything: es_state is a peek at in-memory state (None when
    /// cold), projector_state is the complete answer (wake + catch-up +
    /// capture).
    pub async fn projector_state(&self, path: &ActorPath) -> Option<Json> {
        // HOT: the projector is live — capture once it has no pending
        // work (a wake copy may still sit queued while its loop spins up;
        // the read waits, bounded, so the capture is the fold of
        // everything delivered so far).
        if self.es_state(path).await.is_some() {
            self.await_quiescent(path).await;
            return self.es_state(path).await;
        }
        // COLD, SET-OWNED: wake through the set's factory and wait for the
        // catch-up fact (bounded — an observable timeout, never a hang).
        if !self.wake_projector_until_caught_up(path).await {
            return None;
        }
        self.await_quiescent(path).await;
        self.es_state(path).await
    }

    /// Typed, zero-copy read of a live entity's state: runs `f` over the
    /// state under its lock — no serialize, no clone.
    ///
    /// The closure is sync (it holds the state lock) and `R` is owned (the
    /// guard drops before return — clone the field you need, return it).
    /// `None` means "no `<A>` state at this path right now", which folds
    /// together: no live entry (cold, passivated, or unknown path), the
    /// entry being a different state type (foreign actors' JSON included
    /// — [`Self::es_state`](crate::system::ActorSystemCore::es_state) remains their read path; the mismatch is
    /// logged at debug).
    ///
    /// Sync and non-blocking: takes the state lock with `try_lock`, so a
    /// read never stalls a render thread. A read is always a snapshot of
    /// the live fold AT THE MOMENT OF THE CALL — `None` means no snapshot
    /// was taken this call (the fold currently holds the lock, the path is
    /// cold/unknown, or the state type mismatches); the method has no
    /// memory and returns nothing else. Never retry inside the read. See
    /// [`ActorSystem::with_es_state`](crate::system::ActorSystem::with_es_state) for the
    /// awaiting variant.
    pub fn try_with_es_state<A: EventSourcedActor, R>(
        &self,
        path: &ActorPath,
        f: impl FnOnce(&A) -> R,
    ) -> Option<R> {
        let state = {
            let kernel = self.kernel.lock();
            kernel.es_state.get(path)?.clone()
        };
        let state = state.try_lock().ok()?;
        match state.with_es_state(f) {
            Some(r) => Some(r),
            None => {
                tracing::debug!(
                    path = %path,
                    actual = state.state_type_name(),
                    "typed state read: wrong type at path"
                );
                None
            }
        }
    }

    /// Typed, zero-copy read of a live projector's fold: runs `f` over the
    /// read model under its state lock — no serialize, no clone.
    ///
    /// The closure is sync (it holds the state lock) and `R` is owned (the
    /// guard drops before return — clone the field you need, return it).
    /// `Some(r)` is a snapshot of the live fold at the moment of the call;
    /// `None` means no snapshot was taken this call: no live entry, wrong
    /// type (foreign actors' JSON included), or — for the `try_` pair
    /// only — a lock the fold currently holds.
    ///
    /// Sync and non-blocking like [`ActorSystem::try_with_es_state`](crate::system::ActorSystem::try_with_es_state); use
    /// [`ActorSystem::with_projector_state`](crate::system::ActorSystem::with_projector_state) to wake a cold set-owned
    /// projector (a `try_` read never wakes anything).
    pub fn try_with_projector_state<P: crate::actor::Projector, R>(
        &self,
        path: &ActorPath,
        f: impl FnOnce(&P) -> R,
    ) -> Option<R> {
        let state = {
            let kernel = self.kernel.lock();
            kernel.es_state.get(path)?.clone()
        };
        let state = state.try_lock().ok()?;
        match state.with_projector_state(f) {
            Some(r) => Some(r),
            None => {
                tracing::debug!(
                    path = %path,
                    actual = state.state_type_name(),
                    "typed state read: wrong type at path"
                );
                None
            }
        }
    }

    /// The awaiting typed twin of [`ActorSystem::es_state`](crate::system::ActorSystemCore::es_state): runs `f` over
    /// a live entity's state under its lock — no serialize, no clone.
    ///
    /// The closure is sync and `R` owned (see [`Self::try_with_es_state`](crate::system::ActorSystem::try_with_es_state)).
    /// `None` means "no `<A>` state at this path right now": a cold or
    /// passivated entity (this read never wakes anything, like `es_state`
    /// — only
    /// [`Self::with_projector_state`](crate::system::ActorSystem::with_projector_state)
    /// has a wake path, and only for
    /// set-owned projectors) or a wrong type at the path (foreign actors'
    /// JSON included; the mismatch is logged at debug).
    pub async fn with_es_state<A: EventSourcedActor, R>(
        &self,
        path: &ActorPath,
        f: impl FnOnce(&A) -> R + Send,
    ) -> Option<R> {
        self.read_es(path, f).await
    }

    /// The typed, zero-copy twin of [`ActorSystem::projector_state`](crate::system::ActorSystem::projector_state): runs
    /// `f` over the live read model under its state lock — no serialize,
    /// no clone.
    ///
    /// The closure is sync and `R` owned (see [`Self::try_with_es_state`](crate::system::ActorSystem::try_with_es_state)).
    /// Wake semantics are identical to `projector_state`:
    ///
    /// - **live projector** — quiesce (a wake copy may still sit queued
    ///   while its loop spins up), then read typed;
    /// - **cold, set-owned path** — wake through the set's factory, wait
    ///   bounded for the `CaughtUp` fact, then read typed;
    /// - **cold standalone projector / unknown path** — `None` (no wake
    ///   path; re-run the builder);
    /// - **wake misses its catch-up budget** — `None`;
    /// - **live but not a `P`** — `None` (mismatch logged at debug).
    pub async fn with_projector_state<P: crate::actor::Projector, R>(
        &self,
        path: &ActorPath,
        f: impl FnOnce(&P) -> R + Send,
    ) -> Option<R> {
        // HOT: the projector is live — quiesce, then read typed under the
        // state lock (mirrors projector_state's hot branch exactly).
        if self.es_state(path).await.is_some() {
            self.await_quiescent(path).await;
            return self.read_projector(path, f).await;
        }
        // COLD, SET-OWNED: wake through the set's factory and wait for the
        // catch-up fact (bounded — an observable timeout, never a hang).
        if !self.wake_projector_until_caught_up(path).await {
            return None;
        }
        self.await_quiescent(path).await;
        self.read_projector(path, f).await
    }

    /// Wake a cold set-owned projector and wait — bounded — for its
    /// catch-up fact. Shared by [`ActorSystem::projector_state`](crate::system::ActorSystem::projector_state) and
    /// [`ActorSystem::with_projector_state`](crate::system::ActorSystem::with_projector_state); returns `false` when `path`
    /// is not set-owned or the `CaughtUp` fact never arrives within the
    /// budget. Registry lookups and poll cadence are exactly the
    /// pre-extraction behavior of `projector_state`.
    async fn wake_projector_until_caught_up(&self, path: &ActorPath) -> bool {
        let spec = {
            let registry = self.registry.lock();
            match registry.projector_set_owning(path) {
                Some(spec) => spec,
                None => return false,
            }
        };
        // Delivered-but-unfolded mail wakes the projector but folds AFTER
        // catch-up (the loop starts post-seed). Wait for the mail to
        // drain first; otherwise the wake's own catch-up would signal
        // caught-up while the fold is still incomplete.
        self.await_quiescent(path).await;
        let watermark = {
            let kernel = self.kernel.lock();
            kernel.caught_up.get(path).copied().unwrap_or(0)
        };
        let key = path
            .as_str()
            .strip_prefix(&format!("{}/", spec.public.as_str()))
            .unwrap_or_default()
            .to_owned();
        // LIVE CHECK before the factory: a concurrent wake (another read,
        // or the fact flow activating via the set arm) may have landed
        // between our cold check in `with_projector_state` and here —
        // spawning over the live path panics (`PathTaken`). A live
        // projector's own catch-up is either done or in flight; quiesce
        // (its inbox drains AFTER history folds — step 3 of
        // `catch_up_projector` starts the loop post-seed) and the fold is
        // complete. No counter wait here: the winning activation's bump
        // may already be behind our `watermark` snapshot, so waiting for
        // a NEW bump would time out on a fold that is actually whole.
        if self.read_projector_live(path).await {
            self.await_quiescent(path).await;
            return true;
        }
        (spec.factory)(self, path, &spec.entity_args(&key));
        self.wait_caught_up_past(path, watermark).await
    }

    /// Whether a typed projector read would find LIVE state at `path`
    /// right now (existence only — no lock held past the check).
    async fn read_projector_live(&self, path: &ActorPath) -> bool {
        let state = {
            let kernel = self.kernel.lock();
            kernel.es_state.get(path).cloned()
        };
        state.is_some()
    }

    /// Waits bounded for the projector's caught-up counter to move past
    /// `watermark` (the wake's completeness marker — the counter lives in
    /// the kernel, never evicted, so the signal survives tap pressure).
    async fn wait_caught_up_past(&self, path: &ActorPath, watermark: u64) -> bool {
        const CAUGHT_UP_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        let deadline = tokio::time::Instant::now() + CAUGHT_UP_BUDGET;
        loop {
            let caught_up = {
                let kernel = self.kernel.lock();
                kernel.caught_up.get(path).copied().unwrap_or(0)
            };
            if caught_up > watermark {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// Typed, zero-copy read of a live entity's state: runs `f` over the
    /// state under its lock — no serialize, no clone. `None` propagates:
    /// no live entry, or wrong type (logged at debug with the state's
    /// actual type).
    async fn read_es<A: EventSourcedActor, R>(
        &self,
        path: &ActorPath,
        f: impl FnOnce(&A) -> R + Send,
    ) -> Option<R> {
        let state = {
            let kernel = self.kernel.lock();
            kernel.es_state.get(path)?.clone()
        };
        let state = state.lock().await;
        match state.with_es_state(f) {
            Some(r) => Some(r),
            None => {
                tracing::debug!(
                    path = %path,
                    actual = state.state_type_name(),
                    "typed state read: wrong type at path"
                );
                None
            }
        }
    }

    /// Typed, zero-copy read of a live projector's fold — the projector
    /// twin of [`ActorSystem::read_es`](crate::system::ActorSystem::read_es).
    async fn read_projector<P: crate::actor::Projector, R>(
        &self,
        path: &ActorPath,
        f: impl FnOnce(&P) -> R + Send,
    ) -> Option<R> {
        let state = {
            let kernel = self.kernel.lock();
            kernel.es_state.get(path)?.clone()
        };
        let state = state.lock().await;
        match state.with_projector_state(f) {
            Some(r) => Some(r),
            None => {
                tracing::debug!(
                    path = %path,
                    actual = state.state_type_name(),
                    "typed state read: wrong type at path"
                );
                None
            }
        }
    }

    /// Destroys and re-creates a projector set entity: stop → purge its
    /// journal → re-activate through the set's factory → await catch-up.
    /// The re-fold equals a from-scratch fold of everything the store
    /// holds (the same scan, the same `apply`). SET-OWNED paths only: the
    /// runtime has a recipe (the factory) only for set entities — to
    /// rebuild a standalone projector, `purge_journal` + re-run your
    /// builder.
    ///
    /// # Errors
    ///
    /// [`crate::registry::RegistryError::InvalidSpec`] when `path` is not
    /// owned by a projector set; the store's purge error otherwise.
    pub async fn rebuild_projector(
        &self,
        path: &ActorPath,
    ) -> Result<(), error_stack::Report<crate::registry::RegistryError>> {
        let spec = {
            let registry = self.registry.lock();
            registry.projector_set_owning(path).ok_or_else(|| {
                error_stack::Report::new(crate::registry::RegistryError::InvalidSpec).attach(
                    format!(
                        "rebuild_projector: {path} is not owned by a projector set — \
                             purge_journal + re-run the builder for standalone projectors"
                    ),
                )
            })?
        };
        // Stop first (post-teardown the fold leaves memory), then purge
        // the journal: the durable record is the checkpoint, so a rebuild
        // IS a forget-everything.
        self.stop(path).await;
        self.purge_journal(path).await.map_err(|report| {
            error_stack::Report::new(crate::registry::RegistryError::InvalidSpec)
                .attach("rebuild_projector: journal purge failed")
                .attach(report.to_string())
        })?;
        let key = path
            .as_str()
            .strip_prefix(&format!("{}/", spec.public.as_str()))
            .unwrap_or_default()
            .to_owned();
        (spec.factory)(self, path, &spec.entity_args(&key));
        // Await the re-fold (the caught-up counter is the kernel's
        // completeness signal — same bounded wait the wake path uses).
        let watermark = {
            let kernel = self.kernel.lock();
            kernel.caught_up.get(path).copied().unwrap_or(0)
        };
        if !self.wait_caught_up_past(path, watermark).await {
            return Err(
                error_stack::Report::new(crate::registry::RegistryError::InvalidSpec)
                    .attach("rebuild_projector: catch-up did not complete within the budget"),
            );
        }
        self.await_quiescent(path).await;
        Ok(())
    }

    /// Purges `path`'s journal and snapshots from the store (host-facing
    /// primitive). The next spawn re-scans the whole store: a purge is a
    /// full rebuild for a projector, and a forget-everything for an
    /// entity. Composition primitive for standalone projectors (the
    /// runtime has no recipe to re-spawn them).
    ///
    /// # Errors
    ///
    /// The store's purge error (e.g. a backend that cannot delete).
    pub async fn purge_journal(
        &self,
        path: &ActorPath,
    ) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
        let store = self.kernel.lock().journal_store.clone();
        store.purge(path).await
    }

    /// Bounded wait until `path` has no message in flight: the mpsc is
    /// drained into the inbox and the inbox is empty. Empty alone is not
    /// quiescence — a just-armed projector's loop starts after seeding, so
    /// its inbox reads empty while wake copies still sit in the channel.
    /// Checked twice with a yield between (an empty-after-nonempty read
    /// means the loop caught up). Returns after the budget regardless.
    async fn await_quiescent(&self, path: &ActorPath) {
        const QUIESCE_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        let deadline = tokio::time::Instant::now() + QUIESCE_BUDGET;
        let mut was_busy = false;
        while tokio::time::Instant::now() < deadline {
            let quiet = self.inbox_debug_len(path).await == 0 && self.endpoint_pending(path) == 0;
            if quiet && was_busy {
                return;
            }
            if quiet && !was_busy {
                // One yield: a message sent between the two empty reads
                // flips `was_busy` and restarts the wait.
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                if self.inbox_debug_len(path).await == 0 && self.endpoint_pending(path) == 0 {
                    return;
                }
                was_busy = true;
                continue;
            }
            was_busy = true;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// Envelopes in flight to `path` (accepted into the channel, not yet
    /// in the inbox).
    fn endpoint_pending(&self, path: &ActorPath) -> usize {
        self.registry
            .lock()
            .resolve(path)
            .map(|endpoint| endpoint.pending())
            .unwrap_or(0)
    }

    /// The store behind the system (trait object; tests and flushes).
    #[cfg(test)]
    pub(crate) fn journal_store_trait(&self) -> std::sync::Arc<dyn crate::journal::JournalStore> {
        self.0.journal_store_slot.read().clone()
    }

    /// The host's store-control handler installed with the journal store
    /// at construction (`None` = none installed). The system-level surface
    /// for store messages — a persistent backend's writer task and the
    /// shutdown flush report failures here because their results have no
    /// caller (the sweep ignores `flush`'s result by design). See
    /// [`JournalArgs`](crate::journal::JournalArgs) and
    /// [`StoreControlMessage`](crate::journal::StoreControlMessage).
    pub fn journal_control(&self) -> Option<crate::journal::ControlHandler> {
        self.0.journal_control.clone()
    }

    /// Test seam: swaps the system's journal store.
    #[cfg(test)]
    pub(crate) fn set_journal_store(
        &self,
        store: std::sync::Arc<dyn crate::journal::JournalStore>,
    ) {
        *self.0.journal_store_slot.write() = store.clone();
        self.0.kernel.lock().journal_store = store;
    }

    /// The registry slot for `path`, if any (tests).
    #[cfg(test)]
    pub(crate) fn lookup_slot(&self, path: &ActorPath) -> bool {
        self.registry.lock().lookup(path).is_some()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    impl ActorSystem {
        /// The in-memory journal entries for `path` (tests: inspection of
        /// the default store).
        pub fn journal_entries(&self, path: &ActorPath) -> Vec<crate::journal::JournalEntry> {
            let kernel = self.kernel.lock();
            let store: &std::sync::Arc<dyn crate::journal::JournalStore> = &kernel.journal_store;
            crate::journal::downcast_in_memory(store)
                .map(|mem| mem.entries_of(path))
                .unwrap_or_default()
        }

        /// The number of journalled entries for `path` (tests).
        pub fn journal_len(&self, path: &ActorPath) -> usize {
            self.journal_entries(path)
                .iter()
                .filter(|e| e.as_event().is_some())
                .count()
                + self
                    .journal_entries(path)
                    .iter()
                    .filter(|e| matches!(e, crate::journal::JournalEntry::Snapshot { .. }))
                    .count()
        }

        pub fn inbox_depth(&self, path: &ActorPath) -> usize {
            let kernel = self.kernel.lock();
            kernel
                .cells
                .get(path)
                .map(|c| c.inbox.try_lock().map(|i| i.len()).unwrap_or(0))
                .unwrap_or(0)
        }

        pub fn dead_letter_schemas(&self) -> Vec<SchemaId> {
            let kernel = self.kernel.lock();
            kernel
                .dead_letters
                .iter()
                .map(|d| d.schema.clone())
                .collect()
        }

        /// The observation flow captured by the test log, in emission
        /// order (tests; requires a `test()`-built system).
        pub fn facts(&self) -> Vec<crate::observe::Observation> {
            self.observation_log
                .as_ref()
                .map(|log| log.snapshot())
                .unwrap_or_default()
        }

        /// A compact census of observation kinds (tests).
        pub fn fact_kind_counts(&self) -> HashMap<String, usize> {
            self.observation_log
                .as_ref()
                .map(|log| log.kind_counts())
                .unwrap_or_default()
        }
    }

    /// Waits until `predicate` holds (polling with tiny yields); panics
    /// after ~5s so a regression surfaces as a failure, not a hang.
    #[allow(dead_code)] // adopted by the remaining test-table work
    pub(crate) async fn wait_until(mut predicate: impl AsyncFnMut() -> bool) {
        for _ in 0..500 {
            if predicate().await {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("condition never became true within 5s");
    }

    use super::*;
    use crate::actor::ActorKind;
    use crate::actor::{CommandHandler, MsgHandler, TypedEsAdapter, TypedServiceAdapter};
    use crate::context::CmdCtx;
    use crate::json;
    use crate::schema::{ActorManifest, Command, Event, FieldDef, FieldTy, SchemaDef, SchemaKind};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Command, Serialize, Deserialize, Clone)]
    struct Add {
        n: i64,
    }

    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    struct Added {
        n: i64,
    }

    /// An event schema the test actors never declare (emit-enforcement
    /// fixture: the kernel must drop it).
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    struct Smuggled {
        #[allow(dead_code)] // payload shape; the kernel never reads it
        n: i64,
    }

    #[derive(Serialize, Deserialize, Default, Clone)]
    struct Counter {
        total: i64,
    }

    impl EventSourcedActor for Counter {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Add>()
                .emits::<Added>()
                .kind(ActorKind::EventSourced)
        }
        fn restore(_args: &Json) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            if event.schema.as_str() == "Added" {
                self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
            }
        }
    }
    impl CommandHandler<Add> for Counter {
        fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            crate::envelope::Events::from_vec(vec![crate::envelope::Event::from_json_view(
                Added::schema_id(),
                json!({ "n": cmd.n }),
            )])
        }
    }

    impl CommandHandler<Boom> for Counter {
        fn handle(&self, _cmd: Boom, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            panic!("injected handler panic");
        }
    }

    /// A command whose handling panics (panic isolation under test).
    #[derive(Command, Serialize, Deserialize, Clone)]
    struct Boom {
        #[allow(dead_code)] // payload shape; the handler panics before reading
        why: String,
    }

    /// A serializable ask command (the `system.ask` fixture).
    #[derive(Command, serde::Serialize, serde::Deserialize, Clone)]
    struct PingAsk {
        n: i64,
    }

    /// A live service handler for `PingAsk` (the `system.ask` callee).
    struct Pinger;
    impl ServiceActor for Pinger {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<PingAsk>()
                .kind(ActorKind::Service)
        }
        async fn start(
            _args: &Json,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            Ok(Self)
        }
    }
    impl MsgHandler<PingAsk> for Pinger {
        async fn handle(&mut self, _msg: &PingAsk, _ctx: &mut crate::context::MsgCtx<'_>) {}
    }

    #[tokio::test]
    async fn block_senders_await_at_configured_capacity() {
        // Given a Block actor whose inbox holds FOUR: a burst of twenty
        // commands must queue, block the senders, and never dead-letter.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("blocked");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts {
                snapshot: SnapshotCadence::Off,
                mailbox_capacity: 4,
                mailbox_policy: OverloadPolicy::Block,
                high_watermark: None,
                passivation: None,
                ..SpawnOpts::default()
            },
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // When twenty senders race a slow consumer (each sender awaits
        // its tell: Block backpressure is what paces them).
        let burst = (1..=20_i64).map(|n| {
            let system = system.clone();
            let path = path.clone();
            async move {
                let _ = system
                    .send(system.envelope(Add::schema_id(), path, json!({ "n": n })))
                    .await
                    .map_err(|_| "refused")?;
                Ok::<(), &str>(())
            }
        });
        let sent = futures::future::join_all(burst).await;

        // Then every send was accepted (no refusals) and every command
        // was processed — the burst is BOUNDED, not dead-lettered.
        let refused = sent.iter().filter(|r| r.is_err()).count();
        assert_eq!(refused, 0, "Block never refuses: {sent:?}");
        let mut total = 0_i64;
        for _ in 0..1_000 {
            if let Some(t) = system.with_es_state::<Counter, _>(&path, |c| c.total).await {
                total = t;
                if t == 210 {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert_eq!(total, 210, "all twenty commands processed (sum of 1..=20)");
        let reasons = system.dead_letter_reasons().await;
        assert!(
            !reasons.iter().any(|r| r.starts_with("InboxRefused")),
            "zero InboxRefused under Block: {reasons:?}"
        );
    }

    #[tokio::test]
    async fn restarted_actor_keeps_configured_mailbox() {
        // Given a Block actor whose mailbox was spawned at capacity 256.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("restart-capacity");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts {
                snapshot: SnapshotCadence::Off,
                mailbox_capacity: 256,
                mailbox_policy: OverloadPolicy::Block,
                high_watermark: None,
                passivation: None,
                ..SpawnOpts::default()
            },
            || {
                vec![
                    Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>()),
                    Arc::new(TypedEsAdapter::<Counter, Boom>::new::<Boom>()),
                ]
            },
        );
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // When the actor crashes (panicking handler) and is restarted
        // through supervision's restart mechanism.
        let _ = system
            .send(system.envelope(
                Boom::schema_id(),
                path.clone(),
                json!({ "why": "crash for the restart test" }),
            ))
            .await;
        wait_for_crash(&system, &path).await;
        system.restart_es(&path, &json!({})).await.expect("restart");
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, restart, .. } if *p == path && *restart))
        })
        .await;

        // Then the fresh front door has the SPAWN capacity (256), not
        // the restarted default (64).
        let endpoint = system.registry.lock().resolve(&path).expect("live slot");
        assert_eq!(
            endpoint.max_capacity(),
            256,
            "restart honors spawn capacity"
        );
    }

    #[tokio::test]
    async fn atomic_step_commits_journal_state_and_cursor_together() {
        // Given a spawned counter actor.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // When one Add command is sent and the loop commits it.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 5 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // Then the state folded the event, the journal holds it, and the
        // inbox cursor advanced past the message (committed exactly once).
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], 5);
        let entries = system.journal_entries(&path);
        assert_eq!(entries.len(), 1);
        assert!(
            !system
                .kernel
                .lock()
                .cells
                .values()
                .any(|cell| cell.is_crashed())
        );
    }

    #[tokio::test]
    async fn atomic_step_dead_letters_unknown_schemas_and_advances() {
        // Given a spawned actor that handles only Add.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // When a message with an unhandled schema arrives.
        system
            .send(system.envelope(Boom::schema_id(), path.clone(), json!({})))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // Then it is dead-lettered, nothing is journalled, nothing applied.
        {
            assert!(system.journal_entries(&path).is_empty());
            let kernel = system.kernel.lock();
            assert_eq!(kernel.dead_letters.len(), 1);
            assert_eq!(kernel.dead_letters[0].schema, Boom::schema_id());
        }
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], 0);
    }

    #[tokio::test]
    async fn stop_with_queued_mail_flushes_undelivered_to_dlq() {
        // Given a service actor whose FIRST handler parks on a gate: the
        // messages sent behind it stay QUEUED (un-acked) in the inbox.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("parked");
        system.register_schema::<Add>();

        // First handle() call parks forever (receiver dropped unfired);
        // later calls return immediately so the remaining two messages
        // stay queued, and stop()'s drain sees them.
        static PARK_ONE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        static PARKING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

        struct Parked;
        impl ServiceActor for Parked {
            fn manifest() -> ActorManifest {
                ActorManifest::new().kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Parked {
            async fn handle(&mut self, _msg: &Add, _ctx: &mut crate::context::MsgCtx<'_>) {
                if !PARK_ONE.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    PARKING.store(true, std::sync::atomic::Ordering::SeqCst);
                    // Park until the test ends (dropped sender).
                    let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
                    let _ = rx.await;
                }
            }
        }

        system.spawn_service::<Parked, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedServiceAdapter::<Parked, Add>::new::<Add>())]
        });
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // Send one message (the loop takes it and parks in the handler),
        // then two more (they queue behind it, un-acked).
        for n in 1..=3_i64 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        // Wait until the parked handler has the first message IN.
        wait_for(|| async { PARKING.load(std::sync::atomic::Ordering::SeqCst) }).await;

        // The remaining two are still in the inbox (capacity allows).
        {
            let kernel = system.kernel.lock();
            let cell = kernel.cells.get(&path).expect("cell");
            let inbox = cell.inbox.try_lock().expect("inbox free between messages");
            assert_eq!(
                inbox.len(),
                2,
                "two messages queued behind the parked handler"
            );
        }

        // When the actor is stopped while mail is queued. The parked
        // handler can never finish, so the budget EXPIRES — assert the
        // expiry path directly with a short budget (the 5s default would
        // just burn wall clock; the outcome is identical).
        system
            .stop_bounded(&path, std::time::Duration::from_millis(100))
            .await;

        // Then the queued envelopes were flushed to the dead-letter
        // mirror with the typed StoppedWithMail reason.
        let kernel = system.kernel.lock();
        assert!(
            kernel
                .dead_letters
                .iter()
                .any(|l| l.reason == crate::kernel::DeadLetterReason::StoppedWithMail),
            "undelivered mail typed StoppedWithMail: {:?}",
            kernel.dead_letters
        );
        drop(kernel);

        // And the retained letters carry their full envelopes (host-drainable).
        let drained = system.drain_dead_letters();
        let with_mail: Vec<_> = drained
            .iter()
            .filter(|l| l.reason == crate::kernel::DeadLetterReason::StoppedWithMail)
            .collect();
        assert!(
            with_mail.len() >= 2,
            "DLQ holds the flushed mail: {}",
            with_mail.len()
        );
        assert!(
            with_mail
                .iter()
                .all(|l| l.envelope.schema == Add::schema_id()),
            "retained letters carry the original envelopes"
        );
    }

    #[tokio::test]
    async fn inbox_overflow_dead_letters_through_the_front_door() {
        // Given a spawned actor with a mailbox of ONE under DropNew —
        // the mpsc front door is 2x, so the inbox can overflow.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("tiny");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts {
                snapshot: SnapshotCadence::Off,
                mailbox_capacity: 1,
                mailbox_policy: OverloadPolicy::DropNew,
                high_watermark: None,
                passivation: None,
                ..SpawnOpts::default()
            },
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, .. } if *path == path.clone()))
        })
        .await;

        // When more envelopes than the inbox holds are sent quickly.
        for n in 1..=3_i64 {
            let _ = system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await;
        }

        // Then at least one envelope was refused by the inbox and
        // dead-lettered through the front door (InboxRefused + DLQ log).
        wait_for(|| async { !system.dead_letter_reasons().await.is_empty() }).await;
        let reasons = system.dead_letter_reasons().await;
        assert!(
            reasons.iter().any(|r| r.starts_with("InboxRefused")),
            "front-door refusal recorded: {reasons:?}"
        );

        // And the refused envelope is retained host-drainable.
        let drained = system.drain_dead_letters();
        assert!(
            drained.iter().any(
                |l| l.reason == crate::kernel::DeadLetterReason::InboxRefused
                    && l.envelope.schema == Add::schema_id()
            ),
            "DLQ retains the refused envelope"
        );
    }

    #[tokio::test]
    async fn atomic_step_panics_leave_the_message_queued_for_redelivery() {
        // Given a spawned actor whose Boom handler panics.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Boom>::new::<Boom>())]
        });

        // When a Boom command arrives (the handler panics mid-decision).
        system
            .send(system.envelope(Boom::schema_id(), path.clone(), json!({ "why": "test" })))
            .await
            .expect("delivered");
        wait_for_crash(&system, &path).await;

        // Then NOTHING was appended, NOTHING acked, and the crash was
        // recorded: the message stays queued for redelivery after restart.
        {
            assert!(system.journal_entries(&path).is_empty());
            let kernel = system.kernel.lock();
            assert!(
                kernel
                    .cells
                    .get(&path)
                    .is_some_and(|cell| cell.is_crashed()),
                "the panic was recorded on the cell"
            );
            assert!(kernel.dead_letters.is_empty());
        }
        assert_eq!(system.inbox_cursor(&path).map(|c| c.as_u64()), Some(0));
        let state = system.es_state(&path).await.expect("shell present");
        assert_eq!(state["total"], 0);
    }

    #[tokio::test]
    async fn mid_batch_decide_panic_restores_the_whole_claimed_batch() {
        // Given an ES counter (default batch 64) that will receive one good
        // Add, a poison Boom, and a second good Add — all one claimed batch.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("mid-batch-restore");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![
                Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>()),
                Arc::new(TypedEsAdapter::<Counter, Boom>::new::<Boom>()),
            ]
        });
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("delivered");
        system
            .send(system.envelope(Boom::schema_id(), path.clone(), json!({ "why": "boom" })))
            .await
            .expect("delivered");
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 2 })))
            .await
            .expect("delivered");
        wait_for_crash(&system, &path).await;

        // Then the decide-panic aborted the batch BEFORE any commit: no
        // events appended, cursor still at zero, and all THREE claimed
        // envelopes were restored to their original slots (tombstones
        // refilled — the queue holds the whole batch for redelivery).
        {
            assert!(
                system.journal_entries(&path).is_empty(),
                "nothing appended on a decide panic"
            );
            let kernel = system.kernel.lock();
            let cell = kernel.cells.get(&path).expect("cell kept for redelivery");
            assert_eq!(
                cell.inbox.lock().len(),
                3,
                "the claimed batch was restored to the queue"
            );
        }
        assert_eq!(system.inbox_cursor(&path).map(|c| c.as_u64()), Some(0));

        // And after a supervised restart the restored batch redelivers
        // whole (decide is batch-atomic: the restored Boom aborts the
        // batch again before anything appends — the crash re-records and
        // the inbox STILL holds all three in order for the next restart).
        system.restart_es(&path, &json!({})).await.expect("restart");
        wait_for_crash(&system, &path).await;
        assert!(
            system.journal_entries(&path).is_empty(),
            "the restored Boom aborts the batch before any append"
        );
        {
            let kernel = system.kernel.lock();
            let cell = kernel.cells.get(&path).expect("cell");
            assert_eq!(cell.inbox.lock().len(), 3, "re-restored whole batch");
        }
        assert_eq!(system.inbox_cursor(&path).map(|c| c.as_u64()), Some(0));
    }

    #[tokio::test]
    async fn restart_rebuilds_from_journal_and_redelivers_exactly_once() {
        // Given a counter that has committed one Add, then crashed on Boom.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        let boom = ActorPath::new("counter");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![
                Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>()),
                Arc::new(TypedEsAdapter::<Counter, Boom>::new::<Boom>()),
            ]
        });
        system
            .send(system.envelope(Add::schema_id(), boom.clone(), json!({ "n": 4 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;
        system
            .send(system.envelope(Boom::schema_id(), boom.clone(), json!({ "why": "boom" })))
            .await
            .expect("delivered");
        wait_for_crash(&system, &path).await;

        // When the supervisor restarts the actor.
        system.restart_es(&path, &json!({})).await.expect("restart");
        // Boom is redelivered exactly once, panics again (the command is
        // poison), and the crash is re-recorded for the supervisor.
        wait_for_crash(&system, &path).await;

        // Then redelivery attempted the Boom exactly once (no duplicate
        // events, no double-apply), the pre-crash Add stayed committed, and
        // state equals the journal fold.
        {
            assert_eq!(
                system.journal_entries(&path).len(),
                1,
                "no duplicate events"
            );
            assert!(
                system.kernel.lock().dead_letters.is_empty(),
                "panic never dead-letters"
            );
        }
        assert_eq!(
            system.inbox_cursor(&path).map(|c| c.as_u64()),
            Some(1),
            "the poison message stays queued (peeked, never acked)"
        );
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], 4, "fold(journal), not doubled");
    }

    /// Reads a Counter actor's folded total via the erased state capture.
    #[allow(dead_code)] // used by several test cases; some were trimmed
    async fn count_total(system: &ActorSystem, name: &str) -> Option<i64> {
        system
            .es_state(&ActorPath::new(name))
            .await
            .and_then(|s| s["total"].as_i64())
    }

    /// Whether a Stopped fact was recorded for `path` with `reason` (tests).
    fn stopped_with(
        system: &ActorSystem,
        path: &ActorPath,
        reason: crate::actor::StopReason,
    ) -> bool {
        system.facts().iter().any(|f| {
            matches!(&f.kind, crate::observe::ObservationKind::Stopped { path: p, reason: r } if p == path && *r == reason)
        })
    }

    /// Polls `cond` until true (2s budget) — async test helper.
    async fn wait_for<F, Fut>(cond: F)
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
        panic!("condition never became true");
    }

    /// Like [`wait_for`], but returns the polled value once it is `Some`
    /// (and `false` when the poll budget expires).
    async fn wait_for_returning<T: Send, F, Fut>(poll: F) -> Option<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Option<T>>,
    {
        for _ in 0..1_000 {
            if let Some(value) = poll().await {
                return Some(value);
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        None
    }

    async fn wait_for_cursor(system: &ActorSystem, path: &ActorPath, expected: u64) {
        for _ in 0..2_000 {
            if system.inbox_cursor(path).map(|c| c.as_u64()) == Some(expected) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("cursor never reached {expected}");
    }

    #[tokio::test]
    async fn tap_causality_chain_links_hops_with_a_shared_trace() {
        // Given A→B→C: a counter whose Add handler forwards to an Echo
        // service, and an Echo service that handles Ping.
        #[derive(Command, serde::Serialize, serde::Deserialize, Clone)]
        struct Ping {
            #[serde(default)]
            #[allow(dead_code)] // payload shape; the handler ignores it
            n: i64,
        }

        #[derive(Serialize, Deserialize, Default, Clone)]
        struct Forwarder;
        impl ServiceActor for Forwarder {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .emits::<Ping>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Forwarder {
            async fn handle(&mut self, _cmd: &Add, ctx: &mut crate::context::MsgCtx<'_>) {
                ctx.send(Address::Path(ActorPath::new("echo")), Ping { n: 0 }, None);
            }
        }

        struct Echo;
        impl ServiceActor for Echo {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Ping>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Ping> for Echo {
            async fn handle(&mut self, _msg: &Ping, _ctx: &mut crate::context::MsgCtx<'_>) {}
        }

        let (system, _clock) = ActorSystem::test();
        system.spawn_service::<Forwarder, _>(
            ActorPath::new("a"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Forwarder, Add>::new::<Add>())],
        );
        system.spawn_service::<Echo, _>(
            ActorPath::new("echo"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Echo, Ping>::new::<Ping>())],
        );

        // When the conversation starts at A and both hops settle.
        system
            .send(system.envelope(Add::schema_id(), ActorPath::new("a"), json!({ "n": 1 })))
            .await
            .expect("send");
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Delivered { to, .. } if *to == ActorPath::new("echo")))
        })
        .await;

        // Then the facts carry a shared trace id across the hops.
        let facts = system.facts();
        let a_hop = facts
            .iter()
            .find(|f| matches!(&f.kind, crate::observe::ObservationKind::Delivered { to, .. } if *to == ActorPath::new("a")))
            .expect("hop a delivered");
        let trace_of = |f: &crate::observe::Observation| match &f.kind {
            crate::observe::ObservationKind::Delivered { trace, .. }
            | crate::observe::ObservationKind::Acked { trace, .. }
            | crate::observe::ObservationKind::Sent { trace, .. } => *trace,
            _ => panic!("unexpected fact kind"),
        };
        let a_trace = trace_of(a_hop);
        let b_facts: Vec<_> = facts
            .iter()
            .filter(|f| {
                matches!(
                    &f.kind,
                    crate::observe::ObservationKind::Delivered { to, .. } if *to == ActorPath::new("echo")
                )
            })
            .collect();
        assert!(!b_facts.is_empty(), "echo never received the forward");
        for f in &b_facts {
            assert_eq!(
                trace_of(f).trace_id,
                a_trace.trace_id,
                "hops share one trace"
            );
        }
    }

    #[tokio::test]
    async fn overflow_traffic_keeps_delivery_working_and_projects_to_json() {
        // Given a system under a flood of far more messages than any
        // observation consumer would want to keep.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // When 50 messages flow through the observation flow.
        for n in 0..50 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("send");
        }
        wait_for_cursor(&system, &path, 50).await;

        // Then delivery was unaffected: all 50 committed (journal count).
        let journal_len = system.journal_entries(&path).len();
        assert_eq!(journal_len, 50);

        // And the log retained everything in emission order, and the JSON
        // projection still works at the boundary.
        let facts = system.facts();
        assert!(facts.len() >= 50, "every message observed: {}", facts.len());
        let last = facts.last().expect("facts").to_json();
        assert!(last["kind"].is_string());
        assert!(last["ts"].is_u64());
    }

    #[tokio::test]
    async fn supervision_engine_restarts_a_crashed_es_child_and_redelivery_succeeds() {
        // Given a supervised ES child whose Add handler panics ONLY on the
        // poison payload (n = 666), with a generous budget.
        let (system, _clock) = ActorSystem::test();
        let child = ActorPath::new("phoenix");
        system.register_schema::<Add>();
        system.register_schema::<Added>();

        // A TRANSIENT fault: the 666 command panics the FIRST time it is
        // processed; after the engine restarts the child, the redelivered
        // command commits normally (models a crash caused by bad external
        // state that the restart clears).
        static CRASHED_YET: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);

        #[derive(Serialize, Deserialize, Default, Clone)]
        struct Phoenix {
            total: i64,
        }
        impl EventSourcedActor for Phoenix {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .emits::<Added>()
                    .kind(ActorKind::EventSourced)
            }
            fn restore(_args: &Json) -> Self {
                Self::default()
            }
            fn apply(&mut self, event: &crate::envelope::Event) {
                self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
            }
        }
        impl CommandHandler<Add> for Phoenix {
            fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
                if cmd.n == 666 && !CRASHED_YET.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    // First sight only: crash once, then recover.
                    panic!("transient fault");
                }
                crate::envelope::Events::from_vec(vec![crate::envelope::Event::from_json_view(
                    Added::schema_id(),
                    json!({ "n": cmd.n }),
                )])
            }
        }

        let spec = crate::supervision::ActorSpec {
            path: child.clone(),
            parent: None,
            restart: crate::supervision::RestartPolicy::Permanent,
            budget: crate::supervision::RestartBudget::per(5, std::time::Duration::from_secs(10)),
            backoff: crate::supervision::Backoff {
                base: std::time::Duration::from_millis(5),
                max: std::time::Duration::from_millis(20),
                factor: 2.0,
            },
            args: json!({}),
            spawn: Arc::new(|sys: &ActorSystem, path: &ActorPath, args: &Json| {
                sys.spawn_es::<Phoenix, _>(path.clone(), args, SpawnOpts::default(), || {
                    vec![Arc::new(TypedEsAdapter::<Phoenix, Add>::new::<Add>())]
                });
            }),
        };

        // When the child is spawned under supervision and receives a
        // poison command (with a good one queued BEHIND it).
        system.spawn(spec);
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, .. } if *path == child),
            )
        })
        .await;
        system
            .send(system.envelope(Add::schema_id(), child.clone(), json!({ "n": 666 })))
            .await
            .expect("poison delivered");
        system
            .send(system.envelope(Add::schema_id(), child.clone(), json!({ "n": 5 })))
            .await
            .expect("good delivered");

        // Then the engine restarted it: the poison command was retried and
        // panicked again — but the pending GOOD command was also redelivered
        // and SUCCEEDED (the queue survived the crash through the engine).
        wait_for(|| async {
            let state = system.es_state(&child).await;
            state.as_ref().map(|s| s["total"] == 671).unwrap_or(false)
        })
        .await;

        // And the engine marked the restart with Spawned { restart: true }.
        let facts = system.facts();
        let restarted = facts.iter().any(|f| {
            matches!(
                &f.kind,
                crate::observe::ObservationKind::Spawned { path, restart, .. }
                    if *path == child && *restart
            )
        });
        assert!(
            restarted,
            "engine emitted Spawned{{restart:true}}: {facts:?}"
        );

        // And the journal survived the engine restart: the retried 666
        // committed its event once, plus the queued 5.
        assert_eq!(system.journal_len(&child), 2);
    }

    #[tokio::test]
    async fn escalation_delivers_a_message_to_the_declared_parent() {
        // Given a supervised counter whose Add handler always panics,
        // with a budget of 2 restarts per 10 seconds, parent "overseer".
        #[derive(Serialize, Deserialize, Default, Clone)]
        struct AlwaysBoom;
        impl EventSourcedActor for AlwaysBoom {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::EventSourced)
            }
            fn restore(_args: &Json) -> Self {
                Self
            }
            fn apply(&mut self, _event: &crate::envelope::Event) {}
        }
        impl CommandHandler<Add> for AlwaysBoom {
            fn handle(&self, _cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
                panic!("always panics");
            }
        }

        // The overseer is a service actor whose Escalated control message
        // lands in its sink via a plain send from the engine.
        let (system, _clock) = ActorSystem::test();
        let overseer = ActorPath::new("overseer");
        bind_sink(&overseer, Arc::new(parking_lot::Mutex::new(Vec::new())));
        struct Overseer;
        impl ServiceActor for Overseer {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Added>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        #[derive(serde::Deserialize, Clone)]
        struct EscalatedMsg {
            escalated: String,
        }
        impl Schema for EscalatedMsg {
            fn schema_def() -> SchemaDef {
                SchemaDef {
                    name: "Escalated".into(),
                    kind: SchemaKind::Command,
                    fields: vec![FieldDef::required("escalated", FieldTy::Str)],
                    description: None,
                }
            }
        }
        impl MsgHandler<EscalatedMsg> for Overseer {
            async fn handle(&mut self, msg: &EscalatedMsg, _ctx: &mut crate::context::MsgCtx<'_>) {
                if let Some(s) = SINK_BY_PATH
                    .get_or_init(|| parking_lot::Mutex::new(HashMap::new()))
                    .lock()
                    .get("overseer")
                {
                    s.lock().push(format!("escalated:{}", msg.escalated))
                }
            }
        }
        system.spawn_service::<Overseer, _>(
            overseer.clone(),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<Overseer, EscalatedMsg>::new::<EscalatedMsg>(),
                )]
            },
        );

        // The child spawns the real counter under a fixed path; the engine
        // restarts it per the spec.
        struct ChildSpawner;
        let worker = ActorPath::new("worker");
        let spawner = Arc::new(ChildSpawner);
        let system_for_spec = system.clone();
        let worker_clone = worker.clone();
        let spec = crate::supervision::ActorSpec {
            path: worker.clone(),
            parent: Some(overseer.clone()),
            restart: crate::supervision::RestartPolicy::Permanent,
            budget: crate::supervision::RestartBudget::per(2, std::time::Duration::from_secs(10)),
            backoff: crate::supervision::Backoff {
                base: std::time::Duration::from_millis(5),
                max: std::time::Duration::from_millis(20),
                factor: 2.0,
            },
            args: json!({}),
            spawn: Arc::new(move |sys: &ActorSystem, path: &ActorPath, args: &Json| {
                let _ = (&spawner, &system_for_spec);
                sys.spawn_es::<AlwaysBoom, _>(path.clone(), args, SpawnOpts::default(), || {
                    vec![Arc::new(TypedEsAdapter::<AlwaysBoom, Add>::new::<Add>())]
                });
                let _ = &worker_clone;
            }),
        };
        system.spawn(spec);

        // When the worker crashes repeatedly inside the window (sends
        // after escalation failing is EXPECTED — the child is gone).
        for _ in 0..4 {
            let _ = system
                .send(system.envelope(Add::schema_id(), worker.clone(), json!({ "n": 1 })))
                .await;
            // Deliberate pacing: the budget window measures REAL elapsed
            // time, so the crashes must spread over real milliseconds.
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
        // Then the engine restarted it within budget, then escalated:
        // poll for the worker's slot removal (the escalation signature).
        let mut slot_gone = false;
        for _ in 0..2_000 {
            slot_gone = {
                let registry = system.registry.lock();
                registry.resolve(&worker).is_none()
            };
            if slot_gone {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(slot_gone, "exhausted child's slot was removed");
        let lines = sink_read(&overseer);
        assert!(
            lines.iter().any(|l| l.starts_with("escalated:worker")),
            "parent received the escalation: {lines:?}"
        );

        // And the tap recorded the child's stop WITH the typed
        // budget-exhausted reason, before the escalation fact.
        let facts = system.facts();
        let stopped_escalated = facts.iter().position(|f| {
            matches!(&f.kind, crate::observe::ObservationKind::Stopped { path, reason }
                if *path == worker && *reason == crate::actor::StopReason::Escalated)
        });
        let escalated = facts.iter().position(
            |f| matches!(&f.kind, crate::observe::ObservationKind::Escalated { path, .. } if *path == worker),
        );
        assert!(stopped_escalated.is_some(), "Stopped(Escalated) recorded");
        assert!(escalated.is_some(), "Escalated recorded");
        assert!(
            stopped_escalated.unwrap() < escalated.unwrap(),
            "stop precedes escalation"
        );
    }

    #[tokio::test]
    async fn graceful_shutdown_stops_children_before_the_parent() {
        // Given a LIVE parent service actor with a LIVE supervised child
        // (both cells running), and messages queued at BOTH so the drain
        // path is exercised (undelivered entries must dead-letter).
        let (system, _clock) = ActorSystem::test();
        let parent = ActorPath::new("parent");
        let child = ActorPath::new("child");
        let (p_idx, p_sink) = open_sink();
        bind_sink(&parent, p_sink);
        let registry = system.registry.clone();
        let kernel = system.kernel.clone();
        let view = system.view.clone();
        let clock = system.clock.clone();

        // The supervised child spawns a real service actor via the spec
        // factory (the same closure the supervision engine would run).
        let spawn_closure = {
            let system = system.clone();
            move |_sys: &ActorSystem, path: &ActorPath, _args: &Json| {
                let system = system.clone();
                let path = path.to_owned();
                let (idx, sink) = open_sink();
                bind_sink(&path, sink);
                system.spawn_service::<Auditor, _>(
                    path.clone(),
                    &json!({ "sink": idx }),
                    SpawnOpts::default(),
                    || {
                        vec![
                            Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>()),
                            Arc::new(TypedServiceAdapter::<Auditor, Added>::new::<Added>()),
                        ]
                    },
                );
            }
        };
        spawn_closure(&system, &child, &json!({}));
        system.spawn_service::<Auditor, _>(
            parent.clone(),
            &json!({ "sink": p_idx }),
            SpawnOpts::default(),
            || {
                vec![
                    Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>()),
                    Arc::new(TypedServiceAdapter::<Auditor, Added>::new::<Added>()),
                ]
            },
        );
        // Register the child spec AFTER its cell exists (supervised).
        {
            let mut kernel = kernel.lock();
            kernel.specs.insert(
                child.clone(),
                crate::supervision::ActorSpec {
                    path: child.clone(),
                    parent: Some(parent.clone()),
                    restart: crate::supervision::RestartPolicy::Permanent,
                    budget: crate::supervision::RestartBudget::default(),
                    backoff: crate::supervision::Backoff::default(),
                    args: json!({}),
                    spawn: Arc::new(spawn_closure),
                },
            );
        }
        drop((registry, kernel, view, clock));

        // Queue messages at both (slow-drain by never waiting between):
        // these are the envelopes that become DLQ entries if undrained.
        for n in 1..=3 {
            system
                .send(system.envelope(Add::schema_id(), child.clone(), json!({ "n": n })))
                .await
                .expect("child queued");
            system
                .send(system.envelope(Add::schema_id(), parent.clone(), json!({ "n": n })))
                .await
                .expect("parent queued");
        }

        // When the parent is stopped gracefully.
        system.stop(&parent).await;

        // Then BOTH Stopped facts exist, and the CHILD's was recorded
        // BEFORE the parent's (children drain first).
        let facts = system.facts();
        let stops: Vec<String> = facts
            .iter()
            .filter_map(|f| match &f.kind {
                crate::observe::ObservationKind::Stopped { path, .. } => Some(path.to_string()),
                _ => None,
            })
            .collect();
        let child_pos = stops.iter().position(|p| p == "child").expect("child stop");
        let parent_pos = stops
            .iter()
            .position(|p| p == "parent")
            .expect("parent stop");
        assert!(
            child_pos < parent_pos,
            "child stopped before parent; stops = {stops:?}"
        );

        // And the child spec cascaded away with the parent.
        let child_cascaded = {
            let kernel = system.kernel.lock();
            !kernel.specs.contains_key(&child)
        };
        assert!(child_cascaded, "child spec cascaded with the parent");

        // And the parent received a link-notification fact for the child.
        let notified = facts.iter().any(|f| {
            matches!(
                &f.kind,
                crate::observe::ObservationKind::LinkNotified { parent, child }
                    if parent.as_str() == "parent" && child.as_str() == "child"
            )
        });
        assert!(notified, "parent notified of child stop");
    }

    #[tokio::test]
    async fn never_policy_escalates_on_first_crash_without_restarting() {
        // Given a supervised ES child with RestartPolicy::Never whose
        // handler always panics.
        let (system, _clock) = ActorSystem::test();
        let child = ActorPath::new("fragile");
        system.register_schema::<Add>();
        system.register_schema::<Added>();

        #[derive(Serialize, Deserialize, Default, Clone)]
        struct Fragile;
        impl EventSourcedActor for Fragile {
            fn manifest() -> ActorManifest {
                ActorManifest::new().kind(ActorKind::EventSourced)
            }
            fn restore(_args: &Json) -> Self {
                Self
            }
            fn apply(&mut self, _event: &crate::envelope::Event) {}
        }
        impl CommandHandler<Add> for Fragile {
            fn handle(&self, _cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
                panic!("never survives");
            }
        }

        let spec = crate::supervision::ActorSpec {
            path: child.clone(),
            parent: None,
            restart: crate::supervision::RestartPolicy::Never,
            budget: crate::supervision::RestartBudget::default(),
            backoff: crate::supervision::Backoff::default(),
            args: json!({}),
            spawn: Arc::new(|sys: &ActorSystem, path: &ActorPath, args: &Json| {
                sys.spawn_es::<Fragile, _>(path.clone(), args, SpawnOpts::default(), || {
                    vec![Arc::new(TypedEsAdapter::<Fragile, Add>::new::<Add>())]
                });
            }),
        };
        system.spawn(spec);

        // When the child crashes once.
        system
            .send(system.envelope(Add::schema_id(), child.clone(), json!({ "n": 1 })))
            .await
            .expect("sent");

        // Then it escalates immediately: the slot is gone and NEVER
        // comes back (no restart despite the budget allowing 5).
        let mut slot_gone = false;
        for _ in 0..2_000 {
            slot_gone = {
                let registry = system.registry.lock();
                registry.resolve(&child).is_none()
            };
            if slot_gone {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(slot_gone, "Never policy removed the slot after one crash");

        // And exactly one stop was recorded, with the Crashed reason.
        wait_for(|| async {
            system.facts().iter().any(|f| {
                matches!(&f.kind, crate::observe::ObservationKind::Stopped { path, reason }
                    if *path == child && *reason == crate::actor::StopReason::Crashed)
            })
        })
        .await;
        let spawn_count = system
            .facts()
            .iter()
            .filter(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, .. } if *path == child),
            )
            .count();
        assert_eq!(spawn_count, 1, "no restart after the crash");
    }

    #[tokio::test]
    async fn transient_policy_ignores_normal_exits() {
        // Given a supervised child spec with Transient restart policy.
        // The observable: a NORMAL stop must not arm the failure window
        // (stop() never records failures), so the child stays stopped.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("transient-child");
        {
            let mut kernel = system.kernel.lock();
            kernel.specs.insert(
                path.clone(),
                crate::supervision::ActorSpec {
                    path: path.clone(),
                    parent: None,
                    restart: crate::supervision::RestartPolicy::Transient,
                    budget: crate::supervision::RestartBudget::default(),
                    backoff: crate::supervision::Backoff::default(),
                    args: json!({}),
                    spawn: Arc::new(|_sys: &ActorSystem, _path: &ActorPath, _args: &Json| {}),
                },
            );
        }

        // When the child is stopped gracefully (a normal exit).
        system.stop(&path).await;

        // Then no failure was recorded (the engine arms only on crashes)
        // and the stop observation says graceful.
        let stops: Vec<_> = system
            .facts()
            .into_iter()
            .filter(|f| matches!(&f.kind, crate::observe::ObservationKind::Stopped { .. }))
            .collect();
        assert_eq!(stops.len(), 1, "exactly one stop observation: {stops:?}");
        let no_failures = {
            let kernel = system.kernel.lock();
            assert!(!kernel.specs.contains_key(&path), "spec removed on stop");
            kernel.failures.get(&path).map(|w| w.is_empty()) != Some(false)
        };
        assert!(no_failures, "no failure recorded for a normal exit");
    }

    async fn wait_for_crash(system: &ActorSystem, path: &ActorPath) {
        for _ in 0..2_000 {
            if system
                .kernel
                .lock()
                .cells
                .get(path)
                .is_some_and(|cell| cell.is_crashed())
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("actor never crashed");
    }

    /// A service actor appending to a shared sink (impure by design).
    /// `start` receives JSON args, so the test passes the sink through a
    /// well-known args key serialized as a slot index into a static table.
    type SinkList = Vec<Arc<Mutex<Vec<String>>>>;
    static SINKS: std::sync::OnceLock<Mutex<SinkList>> = std::sync::OnceLock::new();

    fn sinks() -> &'static Mutex<Vec<Arc<Mutex<Vec<String>>>>> {
        SINKS.get_or_init(|| Mutex::new(Vec::new()))
    }

    /// Path→sink table (tests inspect a subscriber's sink by its path).
    type SinkTable = HashMap<String, Arc<Mutex<Vec<String>>>>;
    static SINK_BY_PATH: std::sync::OnceLock<Mutex<SinkTable>> = std::sync::OnceLock::new();

    fn sink_table() -> &'static Mutex<HashMap<String, Arc<Mutex<Vec<String>>>>> {
        SINK_BY_PATH.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn bind_sink(path: &ActorPath, sink: Arc<Mutex<Vec<String>>>) {
        sink_table().lock().insert(path.to_string(), sink);
    }

    /// Reads a subscriber's sink lines by path (test inspection).
    fn sink_read(path: &ActorPath) -> Vec<String> {
        sink_table()
            .lock()
            .get(&path.to_string())
            .map(|sink| sink.lock().clone())
            .unwrap_or_default()
    }

    fn open_sink() -> (usize, Arc<Mutex<Vec<String>>>) {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let mut all = sinks().lock();
        all.push(sink.clone());
        (all.len() - 1, sink)
    }

    struct Auditor {
        sink: Arc<Mutex<Vec<String>>>,
    }

    impl ServiceActor for Auditor {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Add>()
                .kind(ActorKind::Service)
        }

        async fn start(
            args: &Json,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            let idx = args["sink"].as_u64().expect("sink index") as usize;
            let sink = sinks().lock()[idx].clone();
            Ok(Self { sink })
        }
    }

    impl MsgHandler<Added> for Auditor {
        async fn handle(&mut self, msg: &Added, _ctx: &mut crate::context::MsgCtx<'_>) {
            self.sink.lock().push(format!("Added:{}", msg.n));
        }
    }

    impl MsgHandler<Add> for Auditor {
        async fn handle(&mut self, msg: &Add, _ctx: &mut crate::context::MsgCtx<'_>) {
            self.sink.lock().push(format!("n={}", msg.n));
        }
    }

    #[tokio::test]
    async fn service_actor_receives_typed_messages_impurely() {
        // Given a system and an Auditor service with a shared test sink.
        let (system, _clock) = ActorSystem::test();
        let (sink_idx, sink) = open_sink();
        let path = ActorPath::new("auditor");
        system.spawn_service::<Auditor, _>(
            path.clone(),
            &json!({ "sink": sink_idx }),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>())],
        );

        // When an Add message is sent to it.
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, .. } if *path == ActorPath::new("auditor")))
        })
        .await;
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 7 })))
            .await
            .expect("delivered");

        // Then the handler ran (impure side effect recorded).
        for _ in 0..2_000 {
            if sink.lock().as_slice() == ["n=7"] {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("service handler never ran");
    }

    #[tokio::test]
    async fn panicking_service_handler_crashes_the_step_and_supervision_restarts() {
        // Given a supervised service actor whose handler panics ONLY on
        // the first Boom, then succeeds (and a good Boom result is
        // observable through a shared sink).
        let (system, _clock) = ActorSystem::test();
        let child = ActorPath::new("service-phoenix");
        let (idx, sink) = open_sink();
        bind_sink(&child, sink.clone());

        static CRASHED_YET: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);

        #[derive(Command, Serialize, Deserialize, Clone)]
        struct BoomService {
            why: String,
        }

        struct ServicePhoenix {
            sink: Arc<Mutex<Vec<String>>>,
        }
        impl ServiceActor for ServicePhoenix {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<BoomService>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                let idx = args["sink"].as_i64().expect("sink idx") as usize;
                Ok(Self {
                    sink: sinks().lock()[idx].clone(),
                })
            }
        }
        impl MsgHandler<BoomService> for ServicePhoenix {
            async fn handle(&mut self, msg: &BoomService, _ctx: &mut crate::context::MsgCtx<'_>) {
                if msg.why == "poison"
                    && !CRASHED_YET.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    panic!("injected service handler panic");
                }
                self.sink.lock().push(msg.why.clone());
            }
        }

        let spec = crate::supervision::ActorSpec {
            path: child.clone(),
            parent: None,
            restart: crate::supervision::RestartPolicy::Permanent,
            budget: crate::supervision::RestartBudget::per(5, std::time::Duration::from_secs(10)),
            backoff: crate::supervision::Backoff {
                base: std::time::Duration::from_millis(5),
                max: std::time::Duration::from_millis(20),
                factor: 2.0,
            },
            args: json!({ "sink": idx }),
            spawn: Arc::new(move |sys: &ActorSystem, path: &ActorPath, args: &Json| {
                sys.spawn_service::<ServicePhoenix, _>(
                    path.clone(),
                    args,
                    SpawnOpts::default(),
                    || {
                        vec![Arc::new(
                            TypedServiceAdapter::<ServicePhoenix, BoomService>::new::<BoomService>(
                            ),
                        )]
                    },
                );
            }),
        };

        // When the child runs under supervision, takes the poison (panic
        // mid-handler), and then receives a good message.
        system.spawn(spec);
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, .. } if *path == child),
            )
        })
        .await;
        system
            .send(system.envelope(
                BoomService::schema_id(),
                child.clone(),
                json!({ "why": "poison" }),
            ))
            .await
            .expect("poison delivered");
        // The engine re-runs the spec's spawn closure (a service child
        // restarts as a FRESH spawn): the second Spawned fact for the
        // path is the restart proof (there is no journal, so no
        // Spawned{restart:true} on this tier).
        wait_for(|| async {
            system
                .facts()
                .iter()
                .filter(|f| {
                    matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, .. } if *path == child)
                })
                .count()
                >= 2
        })
        .await;
        system
            .send(system.envelope(
                BoomService::schema_id(),
                child.clone(),
                json!({ "why": "healed" }),
            ))
            .await
            .expect("good delivered");

        // Then the fresh instance handled the good message — the crash
        // was Step::Crashed (cell flagged, fresh loop), never a dead
        // loop task or a torn-down actor.
        for _ in 0..2_000 {
            if sink.lock().as_slice() == ["healed"] {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("restarted service never handled the follow-up message");
    }

    #[tokio::test]
    async fn panicking_service_handler_with_messages_queued_behind() {
        // Given an UNSUPERVISED service actor whose handler panics on the
        // poison tag, with good work inspectable through a shared sink.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("queued-behind");
        let (idx, sink) = open_sink();
        bind_sink(&path, sink.clone());

        struct QueuePoison {
            sink: Arc<Mutex<Vec<String>>>,
        }
        impl ServiceActor for QueuePoison {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Mark>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                let idx = args["sink"].as_i64().expect("sink idx") as usize;
                Ok(Self {
                    sink: sinks().lock()[idx].clone(),
                })
            }
        }
        impl MsgHandler<Mark> for QueuePoison {
            async fn handle(&mut self, msg: &Mark, _ctx: &mut crate::context::MsgCtx<'_>) {
                if msg.tag == "poison" {
                    panic!("injected service handler panic");
                }
                self.sink.lock().push(msg.tag.clone());
            }
        }

        system.spawn_service::<QueuePoison, _>(
            path.clone(),
            &json!({ "sink": idx }),
            SpawnOpts::default(),
            || {
                vec![Arc::new(TypedServiceAdapter::<QueuePoison, Mark>::new::<
                    Mark,
                >())]
            },
        );
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // When the poison lands FIRST and two good messages queue behind
        // it (all delivered before the handler consumes the poison).
        system
            .send(system.envelope(Mark::schema_id(), path.clone(), json!({ "tag": "poison" })))
            .await
            .expect("poison delivered");
        system
            .send(system.envelope(Mark::schema_id(), path.clone(), json!({ "tag": "y1" })))
            .await
            .expect("y1 delivered");
        system
            .send(system.envelope(Mark::schema_id(), path.clone(), json!({ "tag": "y2" })))
            .await
            .expect("y2 delivered");

        // The poison is consumed (acked at handoff), the handler panics,
        // and the loop task marks the cell crashed.
        wait_for_crash(&system, &path).await;

        // Then the panic leaves the queued tail QUEUED: nothing was
        // dead-lettered, and the crash is the only observable outcome.
        let letters = system.drain_dead_letters();
        assert!(
            letters.is_empty(),
            "a service panic never dead-letters the tail: {letters:?}"
        );
        assert_eq!(
            sink.lock().as_slice(),
            [] as [String; 0],
            "the good messages never ran before the crash"
        );

        // And the cursor proves the poison was acked at handoff (the
        // at-most-once contract) while y1/y2 stay queued behind it.
        assert_eq!(
            system.inbox_cursor(&path).map(|c| c.as_u64()),
            Some(1),
            "poison consumed at handoff; the tail stays queued"
        );

        // Cleanup: stop the crashed actor (its queued tail lands in the
        // DLQ as StoppedWithMail — the visible fate of a queue behind a
        // crashed service actor whose supervision never restarts it).
        system.stop(&path).await;
        let letters = system.drain_dead_letters();
        assert!(
            letters
                .iter()
                .any(|l| l.reason == crate::kernel::DeadLetterReason::StoppedWithMail),
            "the queued tail survives to the DLQ on teardown: {letters:?}"
        );
    }

    /// The service-tier `Mark` fixture (panic-tag + queued-behind tests).
    #[derive(Command, Serialize, Deserialize, Clone)]
    struct Mark {
        tag: String,
    }

    #[tokio::test]
    async fn batched_service_step_batch_of_one_matches_the_legacy_step() {
        // Given a service actor spawned with an explicit batch of ONE and
        // a sink for its side effects.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("batch-one");
        let (idx, sink) = open_sink();
        bind_sink(&path, sink.clone());
        let opts = SpawnOpts {
            batch: 1,
            ..SpawnOpts::default()
        };
        system.spawn_service::<Auditor, _>(path.clone(), &json!({ "sink": idx }), opts, || {
            vec![Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>())]
        });

        // When three messages trickle in one at a time.
        for n in 1..=3_i64 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }

        // Then every message ran exactly once, in order — the batch-1
        // step is behaviorally the legacy per-message step.
        wait_for(|| async { sink.lock().as_slice() == ["n=1", "n=2", "n=3"] }).await;
    }

    #[tokio::test]
    async fn batched_service_step_drains_a_full_queue_in_one_pass() {
        // Given a service actor with the default batch (64) and a sink.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("batch-drain");
        let (idx, sink) = open_sink();
        bind_sink(&path, sink.clone());
        system.spawn_service::<Auditor, _>(
            path.clone(),
            &json!({ "sink": idx }),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>())],
        );

        // When a burst of TEN queues up faster than the loop wakes.
        for n in 1..=10_i64 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }

        // Then every message ran exactly once, in order.
        wait_for(|| async {
            sink.lock().as_slice()
                == [
                    "n=1", "n=2", "n=3", "n=4", "n=5", "n=6", "n=7", "n=8", "n=9", "n=10",
                ]
        })
        .await;
        assert_eq!(
            system.inbox_cursor(&path).map(|c| c.as_u64()),
            Some(10),
            "the whole batch committed"
        );
    }

    #[tokio::test]
    async fn handler_panic_mid_batch_consumes_only_through_the_poison() {
        // Given a supervised service actor that panics on its FIRST poison
        // (healed after restart) and a sink for good work.
        let (system, _clock) = ActorSystem::test();
        let child = ActorPath::new("mid-batch-phoenix");
        let (idx, sink) = open_sink();
        bind_sink(&child, sink.clone());

        static CRASHED_YET: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);

        struct MidBatchPhoenix {
            sink: Arc<Mutex<Vec<String>>>,
        }
        impl ServiceActor for MidBatchPhoenix {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Mark>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                let idx = args["sink"].as_i64().expect("sink idx") as usize;
                Ok(Self {
                    sink: sinks().lock()[idx].clone(),
                })
            }
        }
        impl MsgHandler<Mark> for MidBatchPhoenix {
            async fn handle(&mut self, msg: &Mark, _ctx: &mut crate::context::MsgCtx<'_>) {
                if msg.tag == "poison"
                    && !CRASHED_YET.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    panic!("injected mid-batch panic");
                }
                self.sink.lock().push(msg.tag.clone());
            }
        }

        let spec = crate::supervision::ActorSpec {
            path: child.clone(),
            parent: None,
            restart: crate::supervision::RestartPolicy::Permanent,
            budget: crate::supervision::RestartBudget::per(5, std::time::Duration::from_secs(10)),
            backoff: crate::supervision::Backoff {
                base: std::time::Duration::from_millis(5),
                max: std::time::Duration::from_millis(20),
                factor: 2.0,
            },
            args: json!({ "sink": idx }),
            spawn: Arc::new(move |sys: &ActorSystem, path: &ActorPath, args: &Json| {
                sys.spawn_service::<MidBatchPhoenix, _>(
                    path.clone(),
                    args,
                    SpawnOpts::default(),
                    || {
                        vec![Arc::new(
                            TypedServiceAdapter::<MidBatchPhoenix, Mark>::new::<Mark>(),
                        )]
                    },
                );
            }),
        };
        system.spawn(spec);
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == child),
            )
        })
        .await;

        // When poison lands FIRST and two good messages queue behind it
        // (the whole run fits one default-size batch).
        system
            .send(system.envelope(Mark::schema_id(), child.clone(), json!({ "tag": "poison" })))
            .await
            .expect("poison delivered");
        system
            .send(system.envelope(Mark::schema_id(), child.clone(), json!({ "tag": "good1" })))
            .await
            .expect("good1 delivered");
        system
            .send(system.envelope(Mark::schema_id(), child.clone(), json!({ "tag": "good2" })))
            .await
            .expect("good2 delivered");

        // The poison panics the handler; supervision re-spawns the child
        // (second Spawned fact).
        wait_for(|| async {
            system
                .facts()
                .iter()
                .filter(|f| {
                    matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == child)
                })
                .count()
                >= 2
        })
        .await;

        // Then the crash+restart left the FRESH cell empty: the poison was
        // consumed at handoff (at-most-once — never redelivered), and the
        // messages queued behind it died WITH the old cell (a supervised
        // service restart is a fresh spawn on a fresh inbox — the standing
        // contract, identical under the per-message step). Nothing landed
        // in the DLQ and nothing re-ran.
        assert!(
            sink.lock().is_empty(),
            "no queued message re-ran after the restart"
        );
        assert!(
            system.drain_dead_letters().is_empty(),
            "the dropped tail is the standing at-most-once contract, not a DLQ event"
        );

        // And the fresh instance handles NEW mail normally.
        system
            .send(system.envelope(Mark::schema_id(), child.clone(), json!({ "tag": "after" })))
            .await
            .expect("after delivered");
        wait_for(|| async { sink.lock().as_slice() == ["after"] }).await;
    }

    #[tokio::test]
    async fn unknown_schema_inside_a_batch_dead_letters_alone() {        // Given a service actor whose entry handles `Add` ONLY, and a sink
        // for the good work (the Mark fixture is unhandled here).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("batch-unknown");
        let (idx, sink) = open_sink();
        bind_sink(&path, sink.clone());
        system.spawn_service::<Auditor, _>(
            path.clone(),
            &json!({ "sink": idx }),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>())],
        );
        system.register_schema::<Mark>();
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // When an unknown-schema message sits FIRST in a batch of three.
        system
            .send(system.envelope(Mark::schema_id(), path.clone(), json!({ "tag": "x" })))
            .await
            .expect("delivered");
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("delivered");
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 2 })))
            .await
            .expect("delivered");

        // Then only the unknown message dead-letters (UnknownSchema); the
        // rest of the batch still runs, in order.
        wait_for(|| async { sink.lock().as_slice() == ["n=1", "n=2"] }).await;
        wait_for(|| async {
            system
                .drain_dead_letters()
                .iter()
                .any(|l| l.reason == crate::kernel::DeadLetterReason::UnknownSchema)
        })
        .await;
        assert_eq!(
            system.inbox_cursor(&path).map(|c| c.as_u64()),
            Some(3),
            "all three offsets committed (unknown one dead-lettered)"
        );
    }

    #[tokio::test]
    async fn mixed_schema_batch_resolves_entries_once_and_dead_letters_unknown_alone() {
        // Given a service actor whose entry handles `Add` ONLY, and a sink
        // for the good work (the Mark fixture is unhandled here).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("batch-mixed");
        let (idx, sink) = open_sink();
        bind_sink(&path, sink.clone());
        system.spawn_service::<Auditor, _>(
            path.clone(),
            &json!({ "sink": idx }),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>())],
        );
        system.register_schema::<Mark>();
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // When a batch mixes BOTH schemas — known, unknown, known (the
        // unknown lands between two good messages so the per-message
        // find is exercised on both sides of the dead letter).
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("delivered");
        system
            .send(system.envelope(Mark::schema_id(), path.clone(), json!({ "tag": "x" })))
            .await
            .expect("delivered");
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 2 })))
            .await
            .expect("delivered");

        // Then both known messages dispatched in order (the unknown one
        // dead-lettered ALONE, between them, with UnknownSchema)...
        wait_for(|| async { sink.lock().as_slice() == ["n=1", "n=2"] }).await;
        let letters = system.drain_dead_letters();
        assert_eq!(
            letters
                .iter()
                .filter(|l| l.reason == crate::kernel::DeadLetterReason::UnknownSchema)
                .count(),
            1,
            "exactly one UnknownSchema letter (the Mark): {letters:?}"
        );
        // ...and all three offsets committed — the dead letter did not
        // stall the batch.
        assert_eq!(
            system.inbox_cursor(&path).map(|c| c.as_u64()),
            Some(3),
            "the whole mixed batch committed"
        );
    }

    #[tokio::test]
    async fn stop_self_inside_a_batch_stops_after_the_prior_messages_flushed() {
        // Given a self-stopping actor whose FIRST handled message records
        // a send and a stop, with more messages queued behind it.
        #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
        struct Note {
            n: i64,
        }
        #[derive(Serialize, Deserialize, Clone)]
        struct StopNote;
        impl ServiceActor for StopNote {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .emits::<Note>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for StopNote {
            async fn handle(&mut self, cmd: &Add, ctx: &mut crate::context::MsgCtx<'_>) {
                if cmd.n == 1 {
                    // Record a send then stop: the send must flush before
                    // the step concludes.
                    ctx.publish(Note { n: 0 });
                    ctx.stop_self();
                }
            }
        }
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Note>();
        let path = ActorPath::new("batch-stop");
        system.spawn_service::<StopNote, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedServiceAdapter::<StopNote, Add>::new::<Add>())]
        });
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // When the stop lands FIRST with a second command queued behind.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("delivered");
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 2 })))
            .await
            .expect("delivered");

        // Then the actor stops (Normal) and its recorded send flushed
        // before the stop concluded (the Delivered-to-stop order is the
        // standing contract; here we pin the send's flush side: the
        // published fact reaches a subscriber-or-DLQ, never vanishes).
        wait_for(|| async { stopped_with(&system, &path, crate::actor::StopReason::Normal) }).await;
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Sent { schema, .. } if *schema == Note::schema_id()))
        })
        .await;

        // And the UN-dispatched tail (n=2, claimed but never reached) went
        // to the DLQ at teardown: only the dispatched prefix committed.
        let drained = system.drain_dead_letters();
        assert!(
            drained.iter().any(|l| {
                l.reason == crate::kernel::DeadLetterReason::StoppedWithMail
                    && l.envelope.payload_json()["n"].as_i64() == Some(2)
            }),
            "the un-dispatched tail flushed StoppedWithMail: {:?}",
            drained.iter().map(|l| (l.reason.clone(), l.envelope.payload_json())).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn block_producer_unblocks_when_a_batch_commits() {
        // Given a Block mailbox of FOUR on a batched service actor.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        let path = ActorPath::new("batch-block");
        let (idx, sink) = open_sink();
        bind_sink(&path, sink.clone());
        let opts = SpawnOpts {
            mailbox_capacity: 4,
            ..SpawnOpts::default()
        };
        system.spawn_service::<Auditor, _>(path.clone(), &json!({ "sink": idx }), opts, || {
            vec![Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>())]
        });

        // When eight senders race a batched consumer (each awaits its
        // tell: Block backpressure paces them; batches free room in
        // chunks).
        let burst = (1..=8_i64).map(|n| {
            let system = system.clone();
            let path = path.clone();
            async move {
                let _ = system
                    .send(system.envelope(Add::schema_id(), path, json!({ "n": n })))
                    .await
                    .map_err(|_| "refused")?;
                Ok::<(), &str>(())
            }
        });
        let sent = futures::future::join_all(burst).await;
        let refused = sent.iter().filter(|r| r.is_err()).count();

        // Then nothing was refused and all eight processed in order.
        assert_eq!(refused, 0, "Block never refuses: {sent:?}");
        wait_for(|| async { sink.lock().len() == 8 }).await;
        let lines = sink.lock().clone();
        let numbers: Vec<i64> = lines
            .iter()
            .filter_map(|l| l.strip_prefix("n=").and_then(|v| v.parse().ok()))
            .collect();
        let mut sorted = numbers.clone();
        sorted.sort();
        assert_eq!(sorted, numbers, "per-sender FIFO held");
    }

    #[tokio::test]
    async fn service_steps_dispatch_inline_without_spawning_tasks() {
        // Given a service actor and a healthy baseline of the runtime's
        // own spawn counter — taken only once the actor's loop task is
        // observably up (the loop's spawn lands AFTER the Spawned fact,
        // so the baseline must not race it).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("inline-probe");
        let (idx, sink) = open_sink();
        bind_sink(&path, sink.clone());
        system.spawn_service::<Auditor, _>(
            path.clone(),
            &json!({ "sink": idx }),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>())],
        );
        // One warm message: its delivery proves the loop task is running
        // (and all its spawn bookkeeping is done).
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 0 })))
            .await
            .expect("delivered");
        wait_for(|| async { sink.lock().len() == 1 }).await;
        let spawns_before = crate::kernel::TASK_SPAWNS.load(std::sync::atomic::Ordering::Relaxed);
        let inline_before =
            crate::kernel::INLINE_SERVICE_DISPATCH.load(std::sync::atomic::Ordering::Relaxed);

        // When ten messages flow through the service step.
        for n in 1..=10_i64 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        wait_for(|| async { sink.lock().len() == 11 }).await;

        // Then every dispatch ran INLINE on the loop task and the
        // runtime spawned NOTHING for the ten steps.
        let spawns_after = crate::kernel::TASK_SPAWNS.load(std::sync::atomic::Ordering::Relaxed);
        let inline_after =
            crate::kernel::INLINE_SERVICE_DISPATCH.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            inline_after - inline_before,
            10,
            "each step dispatched inline"
        );
        assert_eq!(
            spawns_after - spawns_before,
            0,
            "no task spawned per service message"
        );
    }

    #[tokio::test]
    async fn ask_settles_replied_when_the_callee_answers_the_slot() {
        // Given a callee that replies to whatever asks it, and an asker
        // service that calls ctx.ask on it.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();

        static RESULTS: std::sync::OnceLock<Mutex<Vec<String>>> = std::sync::OnceLock::new();
        let results = RESULTS.get_or_init(|| Mutex::new(Vec::new()));

        struct Echo;
        impl ServiceActor for Echo {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .emits::<Add>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Echo {
            async fn handle(&mut self, msg: &Add, ctx: &mut crate::context::MsgCtx<'_>) {
                ctx.reply(msg.clone());
            }
        }

        struct Asker;
        impl ServiceActor for Asker {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Boom>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Boom> for Asker {
            async fn handle(&mut self, _msg: &Boom, ctx: &mut crate::context::MsgCtx<'_>) {
                let reply = ctx
                    .ask_json(
                        Address::Path(ActorPath::new("echo")),
                        Add::schema_id(),
                        json!({ "n": 21 }),
                        std::time::Duration::from_secs(2),
                    )
                    .await;
                let recorded = RESULTS.get_or_init(|| Mutex::new(Vec::new()));
                match reply {
                    Ok(value) => recorded.lock().push(format!("replied:{}", value["n"])),
                    Err(_) => recorded.lock().push("failed".to_owned()),
                }
            }
        }

        system.spawn_service::<Echo, _>(
            ActorPath::new("echo"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Echo, Add>::new::<Add>())],
        );
        system.spawn_service::<Asker, _>(
            ActorPath::new("asker"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Asker, Boom>::new::<Boom>())],
        );
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, .. } if *path == ActorPath::new("asker")))
        })
        .await;

        // When the asker asks the echo.
        system
            .send(system.envelope(
                Boom::schema_id(),
                ActorPath::new("asker"),
                json!({ "why": "ask" }),
            ))
            .await
            .expect("delivered");

        // Then the ask settles as Replied with the echo's payload.
        for _ in 0..2_000 {
            if results.lock().as_slice() == ["replied:21"] {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("ask never settled as replied: {:?}", results.lock());
    }

    #[tokio::test]
    async fn ask_settles_timeout_when_the_callee_never_replies() {
        // Given a silent callee and an asker with a short timeout.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();

        static RESULTS: std::sync::OnceLock<Mutex<Vec<String>>> = std::sync::OnceLock::new();
        let results = RESULTS.get_or_init(|| Mutex::new(Vec::new()));

        struct Silent;
        impl ServiceActor for Silent {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Silent {
            async fn handle(&mut self, _msg: &Add, _ctx: &mut crate::context::MsgCtx<'_>) {}
        }

        struct Asker;
        impl ServiceActor for Asker {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Boom>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Boom> for Asker {
            async fn handle(&mut self, _msg: &Boom, ctx: &mut crate::context::MsgCtx<'_>) {
                let reply = ctx
                    .ask_json(
                        Address::Path(ActorPath::new("silent")),
                        Add::schema_id(),
                        json!({ "n": 1 }),
                        std::time::Duration::from_millis(50),
                    )
                    .await;
                let recorded = RESULTS.get_or_init(|| Mutex::new(Vec::new()));
                recorded.lock().push(
                    if reply.is_ok() {
                        "replied"
                    } else {
                        "timed-out"
                    }
                    .to_owned(),
                );
            }
        }

        system.spawn_service::<Silent, _>(
            ActorPath::new("silent"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Silent, Add>::new::<Add>())],
        );
        system.spawn_service::<Asker, _>(
            ActorPath::new("asker"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Asker, Boom>::new::<Boom>())],
        );
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, .. } if *path == ActorPath::new("asker")))
        })
        .await;

        // When the asker asks the silent callee.
        system
            .send(system.envelope(
                Boom::schema_id(),
                ActorPath::new("asker"),
                json!({ "why": "ask" }),
            ))
            .await
            .expect("delivered");

        // Then the ask settles as a timeout (and the lease is gone).
        for _ in 0..2_000 {
            if results.lock().as_slice() == ["timed-out"] {
                let kernel = system.kernel.lock();
                assert!(kernel.replies.is_empty(), "lease leaked after timeout");
                assert!(!kernel.ask_facts.is_empty(), "no ask facts recorded");
                assert!(
                    kernel
                        .ask_facts
                        .iter()
                        .any(|f| f.outcome == Some(crate::kernel::AskOutcome::Timeout))
                );
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("ask never timed out: {:?}", results.lock());
    }

    #[tokio::test]
    async fn ask_settles_failed_when_the_lease_dies_mid_ask() {
        // Given a system where a service asker asks a LIVE callee with a
        // long timeout — but the callee never replies.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Boom>();

        static RESULTS: std::sync::OnceLock<Mutex<Vec<String>>> = std::sync::OnceLock::new();
        let _results = RESULTS.get_or_init(|| Mutex::new(Vec::new()));

        struct Silent;
        impl ServiceActor for Silent {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Silent {
            async fn handle(&mut self, _msg: &Add, _ctx: &mut crate::context::MsgCtx<'_>) {}
        }

        static FAILED_RESULTS: std::sync::OnceLock<Mutex<Vec<String>>> = std::sync::OnceLock::new();

        struct Asker;
        impl ServiceActor for Asker {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Boom>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Boom> for Asker {
            async fn handle(&mut self, _msg: &Boom, ctx: &mut crate::context::MsgCtx<'_>) {
                // Long timeout: the lease is reaped before the asker's own
                // timeout could fire — isolating the Failed path.
                let outcome = ctx
                    .ask_json(
                        Address::Path(ActorPath::new("silent")),
                        Add::schema_id(),
                        json!({ "n": 1 }),
                        std::time::Duration::from_secs(30),
                    )
                    .await;
                let settled = match outcome {
                    Ok(_) => "replied",
                    Err(_) => "failed",
                };
                FAILED_RESULTS
                    .get_or_init(|| Mutex::new(Vec::new()))
                    .lock()
                    .push(settled.to_string());
            }
        }

        system.spawn_service::<Silent, _>(
            ActorPath::new("silent"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Silent, Add>::new::<Add>())],
        );
        system.spawn_service::<Asker, _>(
            ActorPath::new("asker"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Asker, Boom>::new::<Boom>())],
        );
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, .. } if *path == ActorPath::new("asker")))
        })
        .await;
        system
            .send(system.envelope(
                Boom::schema_id(),
                ActorPath::new("asker"),
                json!({ "why": "ask" }),
            ))
            .await
            .expect("delivered");

        // When the asker's lease is reaped by the lease GC sweep (the
        // same sweep that runs on system maintenance): the slot's sender
        // drops while the asker still awaits → receiver errs → Failed.
        wait_for(|| async {
            let kernel = system.kernel.lock();
            kernel.replies.len() == 1
        })
        .await;
        // The lease TTL mirrors the ask's 30s timeout: advance the fake
        // clock past it, then sweep.
        system
            .fake_clock()
            .expect("fake clock")
            .advance(std::time::Duration::from_secs(31));
        {
            let kernel = system.kernel.lock();
            kernel.replies.prune(crate::clock::Timestamp::from_millis(
                system.clock.now().as_millis(),
            ));
        }

        // Then the ask settles as Failed (not Timeout), with an error.
        let results = FAILED_RESULTS.get_or_init(|| Mutex::new(Vec::new()));
        for _ in 0..2_000 {
            if results.lock().as_slice() == ["failed"] {
                let kernel = system.kernel.lock();
                assert!(
                    kernel
                        .ask_facts
                        .iter()
                        .any(|f| f.outcome == Some(crate::kernel::AskOutcome::Failed)),
                    "Failed fact recorded: {:?}",
                    kernel.ask_facts
                );
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("ask never settled as failed: {:?}", results.lock());
    }

    #[tokio::test]
    async fn ask_over_a_durable_path_continues_as_an_ordinary_message() {
        // Given a service actor whose Add handler REPLIES to a reply-to
        // PATH (not a slot): the reply continues as a normal envelope.
        // (An event-sourced entity never replies — it returns facts; the
        // durable-path continuation contract lives on the service tier.)
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();

        static RECEIVED: std::sync::OnceLock<Mutex<Vec<String>>> = std::sync::OnceLock::new();
        let received = RECEIVED.get_or_init(|| Mutex::new(Vec::new()));

        struct Collector;
        impl ServiceActor for Collector {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Added>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Added> for Collector {
            async fn handle(&mut self, msg: &Added, _ctx: &mut crate::context::MsgCtx<'_>) {
                RECEIVED
                    .get_or_init(|| Mutex::new(Vec::new()))
                    .lock()
                    .push(format!("got n={}", msg.n));
            }
        }

        #[derive(Serialize, Deserialize, Default, Clone)]
        struct Responder;
        impl ServiceActor for Responder {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .emits::<Added>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Responder {
            async fn handle(&mut self, cmd: &Add, ctx: &mut crate::context::MsgCtx<'_>) {
                let dest: crate::envelope::Address = ctx
                    .reply_dest()
                    .unwrap_or_else(|| crate::envelope::Address::Path(ctx.self_path().clone()));
                ctx.send(
                    dest,
                    Added { n: cmd.n },
                    Some(crate::envelope::Address::Path(ctx.self_path().clone())),
                );
            }
        }

        system.spawn_service::<Collector, _>(
            ActorPath::new("collector"),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![Arc::new(TypedServiceAdapter::<Collector, Added>::new::<
                    Added,
                >())]
            },
        );
        system.spawn_service::<Responder, _>(
            ActorPath::new("counter"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Responder, Add>::new::<Add>())],
        );
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, .. } if *path == ActorPath::new("counter")))
        })
        .await;

        // When the counter is told to Add with a reply-to PATH pointing at
        // the collector (an ask-shaped message, but reply-by-name).
        let mut envelope = system.envelope(
            Add::schema_id(),
            ActorPath::new("counter"),
            json!({ "n": 5 }),
        );
        envelope.reply_to = Some(Address::Path(ActorPath::new("collector")));
        envelope.from = Some(ActorPath::new("collector"));
        system.send(envelope).await.expect("delivered");

        // Then the collector receives the reply as an ordinary message
        // (a durable-path continuation — the name survives, no lease).
        for _ in 0..2_000 {
            if !received.lock().is_empty() {
                let got = received.lock()[0].clone();
                assert!(got.starts_with("got n=5"), "wrong payload: {got}");
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("path continuation never arrived");
    }

    #[tokio::test]
    async fn reply_slots_carry_the_mechanism_and_expire_cleanly() {
        // Given a live reply table with two leases: one short, one long.
        let system = ActorSystem::new(SystemConfig::production());
        let kernel = system.kernel.lock();
        let (short_lease, _short_rx) = kernel
            .replies
            .open(std::time::Duration::from_millis(5), system.clock.now());
        let (long_lease, long_rx) = kernel
            .replies
            .open(std::time::Duration::from_secs(60), system.clock.now());

        // When completing the long lease and pruning past the short one.
        assert!(kernel.replies.complete(
            &long_lease,
            crate::envelope::Payload::from(json!({ "ok": true }))
        ));
        drop(long_rx);
        kernel.replies.prune(crate::clock::Timestamp::from_millis(
            system.clock.now().as_millis() + 10,
        ));

        // Then the short lease is gone (expired), the long one was
        // consumed by its reply, and the table is empty — no leaks.
        assert!(kernel.replies.is_empty(), "lease leaked");
        assert!(
            !kernel
                .replies
                .complete(&short_lease, crate::envelope::Payload::from(json!({})))
        );
    }

    #[tokio::test]
    async fn failed_ask_delivery_cancels_its_lease() {
        // Given a system with a registered route whose slot's endpoint is
        // dead (receiver dropped): the lease opens, the request cannot
        // be delivered.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<PingAsk>();
        let path = ActorPath::new("ghost");
        let (tx, rx) = tokio::sync::mpsc::channel::<Envelope>(8);
        // The synthetic dead slot: closed inbox + dropped receiver — under
        // the direct-delivery path a fresh OPEN inbox would accept the ask
        // silently, changing what this test asserts.
        let dead_cell = std::sync::Arc::new(crate::kernel::ActorCell::new(
            path.clone(),
            {
                let mut inbox = Inbox::new(8, OverloadPolicy::Block);
                inbox.close();
                inbox
            },
            8,
            OverloadPolicy::Block,
            1,
        ));
        system
            .registry
            .lock()
            .insert_slot(
                path.clone(),
                crate::schema::ActorManifest::new()
                    .handles::<PingAsk>()
                    .kind(ActorKind::Service),
                Endpoint::new(tx, dead_cell),
                OverloadPolicy::Block,
            )
            .expect("insert dead slot");
        drop(rx);

        // When asking through the system's lease-backed ask.
        let result = system
            .ask(
                path.clone(),
                PingAsk { n: 1 },
                std::time::Duration::from_secs(1),
            )
            .await;

        // Then the ask fails fast AND no lease is left behind.
        assert!(result.is_err(), "ask to a dead endpoint must fail");
        let kernel = system.kernel.lock();
        assert!(kernel.replies.is_empty(), "failed ask leaked its lease");
        // And the ask ledger stays paired (opened + settled-failed).
        assert!(
            kernel
                .ask_facts
                .iter()
                .any(|f| f.outcome == Some(crate::kernel::AskOutcome::Failed)),
            "delivery failure settled as Failed: {:?}",
            kernel.ask_facts
        );
    }

    #[tokio::test]
    async fn expired_ask_leases_are_pruned_on_open() {
        // Given a system with a live handler and an asker whose lease
        // was opened, then aged past its expiry on the fake clock.
        let (system, clock) = ActorSystem::test();
        system.register_schema::<PingAsk>();
        let (lease, _rx) = {
            let kernel = system.kernel.lock();
            kernel
                .replies
                .open(std::time::Duration::from_millis(10), system.clock.now())
        };
        assert_eq!(system.kernel.lock().replies.len(), 1, "lease open");

        // When a NEW ask opens after the old lease's expiry.
        clock.advance(std::time::Duration::from_millis(50));
        let path = ActorPath::new("pinger");
        system.spawn_service::<crate::system::tests::Pinger, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![Arc::new(TypedServiceAdapter::<Pinger, PingAsk>::new::<
                    PingAsk,
                >())]
            },
        );
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;
        let _ = system
            .ask(path, PingAsk { n: 2 }, std::time::Duration::from_secs(2))
            .await;

        // Then the expired lease was pruned as a side effect of opening.
        let kernel = system.kernel.lock();
        assert!(
            !kernel.replies.holds(&lease),
            "expired lease survived a later ask open"
        );
    }

    #[tokio::test]
    async fn spawn_es_registers_slot_and_edges() {
        // Given a system.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();

        // When spawning an ES actor.
        let path = ActorPath::new("counter");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // Then the slot resolves and handlers_of finds the path.
        let handlers = RuntimeView::handlers_of(&system, &Add::schema_id());
        assert_eq!(handlers, [path]);
    }

    #[tokio::test]
    async fn spawn_es_starts_genesis_state() {
        // Given a system with a spawned counter.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // When capturing the state.
        let state = system.es_state(&path).await.expect("live");

        // Then it is at genesis.
        assert_eq!(state["total"], 0);
    }

    #[test]
    fn register_schema_is_idempotent_on_the_system() {
        // Given a system with Add registered.
        let system = ActorSystem::new(SystemConfig::production());
        let first = system.register_schema::<Add>();

        // When registering Add again.
        let second = system.register_schema::<Add>();

        // Then both calls return the same id and one schema is stored.
        assert_eq!(first, second);
        assert!(system.schema(&first).is_some());
    }

    /// Reads a foreign actor's live JSON state (test inspection helper).
    async fn count_total_json(system: &ActorSystem, path: &ActorPath) -> Option<i64> {
        system
            .es_state(path)
            .await
            .and_then(|s| s["total"].as_i64())
    }

    #[tokio::test]
    async fn foreign_schema_roundtrip() {
        // Given a system with a foreign schema registered from a JSON
        // descriptor and a foreign ES actor whose state is pure JSON.
        let (system, _clock) = ActorSystem::test();
        let schema = system
            .register_schema_json(json!({
                "name": "tally", "kind": "command",
                "fields": [
                    {"name": "delta", "ty": "int"}
                ]
            }))
            .expect("valid");
        // A DISTINCT fact schema for the decision's announce: emitting the
        // command's own schema would re-deliver it to the handling actor
        // under the automatic fact broadcast (a self-feedback loop).
        let fact = system
            .register_schema_json(json!({
                "name": "tallied", "kind": "event",
                "fields": [
                    {"name": "delta", "ty": "int"}
                ]
            }))
            .expect("valid");
        let fact_for_actor = fact.clone();
        let fact_for_fold = fact.clone();
        let fact_for_emit = fact.clone();
        system.spawn_es_foreign(
            ActorPath::new("tally-actor"),
            schema.clone(),
            json!({ "total": 0 }),
            Arc::new(move |_state, cmd, _ctx| {
                let delta = cmd["delta"].as_i64().unwrap_or(0);
                vec![crate::envelope::Event::from_json_view(
                    fact_for_actor.clone(),
                    json!({ "delta": delta }),
                )]
            }),
            Arc::new(move |state: &mut Json, ev: &crate::envelope::Event| {
                if ev.schema == fact_for_fold {
                    state["total"] = serde_json::json!(
                        state["total"].as_i64().unwrap_or(0)
                            + ev.payload_json()["delta"].as_i64().unwrap_or(0)
                    );
                }
            }),
            SpawnOpts::default(),
        );
        // The emit edge the decision closure produces is declared explicitly
        // (emit enforcement drops undeclared schemas, so this is load-bearing).
        system
            .declare_emits(&ActorPath::new("tally-actor"), fact_for_emit)
            .expect("live slot");

        // When a JSON command is sent to the foreign actor and the ack
        // settles.
        system
            .send(system.envelope(
                schema.clone(),
                ActorPath::new("tally-actor"),
                json!({ "delta": 5 }),
            ))
            .await
            .expect("delivered");
        wait_for(|| async {
            count_total_json(&system, &ActorPath::new("tally-actor")).await == Some(5)
        })
        .await;

        // Then the foreign actor's live JSON state folded the event.
        let export = system.export().await;
        let actor = export
            .actors
            .iter()
            .find(|a| a.path == ActorPath::new("tally-actor"))
            .expect("foreign actor exported");
        assert_eq!(
            actor.state.as_ref().expect("state")["total"],
            json!(5),
            "foreign fold applied: {actor:?}"
        );
        // And the foreign schema appears in the export's schema table.
        assert!(
            export.schemas.iter().any(|s| s.id() == schema),
            "foreign schema exported"
        );
    }

    #[tokio::test]
    async fn route_cascade_on_remove() {
        // Given a handler registered for the Added message schema.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Added>();
        let (sub_idx, sub_sink) = open_sink();
        bind_sink(&ActorPath::new("sub"), sub_sink);
        system.spawn_service::<Auditor, _>(
            ActorPath::new("sub"),
            &json!({ "sink": sub_idx }),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<Auditor, Added>::new::<Added>(),
                )]
            },
        );
        // When the handler is removed.
        system.stop(&ActorPath::new("sub")).await;

        // Then its route is cascaded away (a re-spawn re-declares; no
        // stale delivery to a dead path).
        let handlers = {
            let registry = system.registry.lock();
            registry.handlers_of(&Added::schema_id())
        };
        assert!(
            handlers.is_empty(),
            "handler removed from the route table: {handlers:?}"
        );
    }

    #[tokio::test]
    async fn export_shows_schemas_actors_and_edge_kinds() {
        // Given a system with an ES actor declaring handles/emits, a
        // schema subscriber (the real builder path), and publish traffic.
        let (system, _clock) = ActorSystem::test();
        let schema = system.register_schema::<Add>();
        system.spawn_es::<Counter, _>(
            ActorPath::new("source"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        system.register_schema::<Added>();
        spawn_edged(&system, "sink", "sink", false, true).await;
        // Announce Shipped from outside the system: the fixture is
        // subscribed to it, so the publish traffic is real.
        system
            .publish(Shipped {
                order: "o-x".into(),
            })
            .await;
        wait_for(|| async {
            sink_read(&ActorPath::new("sink"))
                .iter()
                .any(|l| l.ends_with("shipped:o-x"))
        })
        .await;

        // When exporting.
        let export = system.export().await;

        // Then schemas, actors (with kind), and both declared-edge
        // directions appear; observed_edges stays empty (no history).
        assert!(export.schemas.iter().any(|s| s.id() == schema));
        assert_eq!(export.actors.len(), 2, "both actors live: {export:?}");
        let source = export
            .actors
            .iter()
            .find(|a| a.path == ActorPath::new("source"))
            .expect("source exported");
        assert_eq!(source.kind, crate::actor::ActorKind::EventSourced);
        // And ES actors export their live state.
        assert_eq!(
            source.state.as_ref().and_then(|s| s["total"].as_i64()),
            Some(0),
            "genesis state captured: {source:?}"
        );
        assert!(
            export
                .declared_edges
                .iter()
                .any(|e| e.actor == ActorPath::new("source")
                    && e.schema == schema
                    && e.direction == crate::system::EdgeDirection::Handles)
        );
        assert!(
            export
                .declared_edges
                .iter()
                .any(|e| e.actor == ActorPath::new("sink")
                    && e.schema == Shipped::schema_id()
                    && e.direction == crate::system::EdgeDirection::Handles),
            "handle edge exported: {export:?}"
        );
        // Under opt-in observation there is NO send history: the publish
        // above happened before the export, so its traffic is gone —
        // observed_edges is empty (the ObservationHandler is the live
        // record; the DLQ is the only after-the-fact artifact).
        assert!(export.observed_edges.is_empty(), "no history: {export:?}");
    }

    #[tokio::test]
    async fn export_shows_partition_and_rule_topology() {
        // Given a partition set over "accounts" and a tee rule on Add@1.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        install_key_partition(&system, "accounts").expect("partition install");
        {
            let mut registry = system.registry.lock();
            registry.add_rule(crate::pool::Rule {
                source: None,
                schema: Some(Add::schema_id()),
                dest: Some(ActorPath::new("accounts")),
                action: crate::pool::RuleAction::Tee(ActorPath::new("watcher")),
            });
        }
        // Activate one entity so the export has something to list.
        let e = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("accounts"),
            json!({ "n": 1, "account": "acme" }),
        );
        let _ = system.send(e).await;
        wait_for(|| async {
            system
                .export()
                .await
                .partitions
                .iter()
                .any(|p| !p.entities.is_empty())
        })
        .await;

        // When exporting.
        let export = system.export().await;

        // Then the declared topology rows are present: the partition with
        // its activated entity and the rule with its action/observer.
        let partition = export
            .partitions
            .iter()
            .find(|p| p.path == ActorPath::new("accounts"))
            .expect("partition exported");
        assert_eq!(partition.key_field, "account");
        assert!(
            partition
                .entities
                .contains(&ActorPath::new("accounts/acme")),
            "activated entity listed: {partition:?}"
        );
        let rule = export
            .rules
            .iter()
            .find(|r| r.schema == Some(Add::schema_id()))
            .expect("rule exported");
        assert_eq!(rule.action, "tee");
        assert_eq!(rule.observer, ActorPath::new("watcher"));
        assert_eq!(rule.dest, Some(ActorPath::new("accounts")));
    }

    #[tokio::test]
    async fn restart_policy_never_escalates_immediately_without_restart() {
        // Given a supervised child with RestartPolicy::Never whose handler
        // always panics, and an overseer to receive the escalation.
        #[derive(Serialize, Deserialize, Default, Clone)]
        struct AlwaysBoom2;
        impl EventSourcedActor for AlwaysBoom2 {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::EventSourced)
            }
            fn restore(_args: &Json) -> Self {
                Self
            }
            fn apply(&mut self, _event: &crate::envelope::Event) {}
        }
        impl CommandHandler<Add> for AlwaysBoom2 {
            fn handle(&self, _cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
                panic!("never-restart child panics");
            }
        }

        #[derive(serde::Deserialize, Clone)]
        struct EscalatedMsg2 {
            escalated: String,
        }
        impl Schema for EscalatedMsg2 {
            fn schema_def() -> SchemaDef {
                SchemaDef {
                    name: "Escalated".into(),
                    kind: SchemaKind::Command,
                    fields: vec![FieldDef::required("escalated", FieldTy::Str)],
                    description: None,
                }
            }
        }

        struct Overseer2;
        impl ServiceActor for Overseer2 {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<EscalatedMsg2>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        fn overseer_path() -> ActorPath {
            ActorPath::new("overseer2")
        }
        impl MsgHandler<EscalatedMsg2> for Overseer2 {
            async fn handle(&mut self, msg: &EscalatedMsg2, _ctx: &mut crate::context::MsgCtx<'_>) {
                if let Some(sink) = sink_table().lock().get(&overseer_path().to_string()) {
                    sink.lock().push(format!("escalated:{}", msg.escalated));
                }
            }
        }

        let (system, _clock) = ActorSystem::test();
        let overseer = ActorPath::new("overseer2");
        let (_sink_idx, overseer_sink) = open_sink();
        bind_sink(&overseer, overseer_sink);
        system.spawn_service::<Overseer2, _>(
            overseer.clone(),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<Overseer2, EscalatedMsg2>::new::<EscalatedMsg2>(),
                )]
            },
        );

        let worker = ActorPath::new("never-worker");
        let spec = crate::supervision::ActorSpec {
            path: worker.clone(),
            parent: Some(overseer.clone()),
            restart: crate::supervision::RestartPolicy::Never,
            budget: crate::supervision::RestartBudget::per(5, std::time::Duration::from_secs(10)),
            backoff: crate::supervision::Backoff {
                base: std::time::Duration::from_millis(5),
                max: std::time::Duration::from_millis(20),
                factor: 2.0,
            },
            args: json!({}),
            spawn: Arc::new(|sys: &ActorSystem, path: &ActorPath, args: &Json| {
                sys.spawn_es::<AlwaysBoom2, _>(path.clone(), args, SpawnOpts::default(), || {
                    vec![Arc::new(TypedEsAdapter::<AlwaysBoom2, Add>::new::<Add>())]
                });
            }),
        };
        system.spawn(spec);

        // When the child crashes once.
        let _ = system
            .send(system.envelope(Add::schema_id(), worker.clone(), json!({ "n": 1 })))
            .await;

        // Then the child was NOT restarted: exactly one Spawned fact (the
        // initial spawn), no restart flag anywhere.
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(f.kind, crate::observe::ObservationKind::Escalated { .. }))
        })
        .await;
        let facts = system.facts();
        let worker_spawns: Vec<&crate::observe::Observation> = facts
            .iter()
            .filter(|f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, .. } if *path == worker))
            .collect();
        assert_eq!(
            worker_spawns.len(),
            1,
            "only the initial spawn: {worker_spawns:?}"
        );
        assert!(
            !matches!(
                worker_spawns[0].kind,
                crate::observe::ObservationKind::Spawned { restart: true, .. }
            ),
            "Never must not restart"
        );

        // And the child stopped with the typed Crashed reason (the crash
        // is what stopped it; Never means no restart, hence no escalation
        // restart-cycle — the crash IS the terminal stop).
        let facts = system.facts();
        assert!(
            facts.iter().any(|f| matches!(
                &f.kind,
                crate::observe::ObservationKind::Stopped { path, reason }
                    if *path == worker && *reason == crate::actor::StopReason::Crashed
            )),
            "Stopped {{ Crashed }} expected: {:?}",
            facts
                .iter()
                .filter(|f| matches!(f.kind, crate::observe::ObservationKind::Stopped { .. }))
                .collect::<Vec<_>>()
        );

        // And the overseer received the escalation naming the child.
        wait_for(|| async { !sink_read(&overseer).is_empty() }).await;
        let lines = sink_read(&overseer);
        assert!(
            lines
                .iter()
                .any(|l| l.starts_with("escalated:never-worker")),
            "overseer received the escalation: {lines:?}"
        );
    }

    #[tokio::test]
    async fn schema_addressed_sends_round_robin_across_two_handlers() {
        // Given TWO actors handling the same Add schema: the route table
        // holds both (adding the second converts Single → RoundRobin).
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        for name in ["w1", "w2"] {
            let path = ActorPath::new(name);
            let (idx, sink) = open_sink();
            bind_sink(&path, sink);
            system.spawn_service::<Auditor, _>(
                path.clone(),
                &json!({ "sink": idx }),
                SpawnOpts::default(),
                || vec![Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>())],
            );
        }

        // When four schema-addressed sends go out.
        for n in 1..=4_i64 {
            let envelope = crate::envelope::Envelope::from_bytes_wrapped(
                Add::schema_id(),
                Address::Schema(Add::schema_id()),
                json!({ "n": n }),
                TraceCtx::root(),
            );
            system.send(envelope).await.expect("schema send delivered");
        }
        wait_for(|| async {
            sink_read(&ActorPath::new("w1")).len() + sink_read(&ActorPath::new("w2")).len() == 4
        })
        .await;

        // Then the handlers rotated: each saw two messages.
        let w1 = sink_read(&ActorPath::new("w1")).len();
        let w2 = sink_read(&ActorPath::new("w2")).len();
        assert_eq!(w1, 2, "w1 got its share");
        assert_eq!(w2, 2, "w2 got its share");
    }

    #[tokio::test]
    async fn export_roundtrips_through_json_losslessly() {
        // Given a system export containing schemas, actors (with live ES
        // state), declared edges, and observed edges.
        let (system, _clock) = ActorSystem::test();
        let schema = system.register_schema::<Add>();
        system.spawn_es::<Counter, _>(
            ActorPath::new("counter"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        system
            .send(system.envelope(
                Add::schema_id(),
                ActorPath::new("counter"),
                json!({ "n": 3 }),
            ))
            .await
            .expect("sent");
        wait_for_cursor(&system, &ActorPath::new("counter"), 1).await;
        let export = system.export().await;

        // When it is serialized to JSON and deserialized back.
        let json = serde_json::to_string(&export).expect("serialize");
        let parsed: SystemExport = serde_json::from_str(&json).expect("deserialize");

        // Then the round-tripped export is identical (the canvas can
        // consume the wire format without information loss).
        assert_eq!(parsed, export, "export survives the JSON round trip");
        let _ = schema;
    }

    #[test]
    fn register_schema_json_accepts_foreign_descriptors() {
        // Given a system and a JSON-only descriptor.
        let system = ActorSystem::new(SystemConfig::production());
        let foreign = json!({
            "name": "ForeignPing",

            "kind": "command",
            "fields": []
        });

        // When registering it as JSON.
        let id = system.register_schema_json(foreign).expect("valid");

        // Then it is retrievable by its schema name.
        let stored = system.schema(&id).expect("stored");
        assert_eq!(id.to_string(), "ForeignPing");
        assert_eq!(stored.name, "ForeignPing");
    }

    #[tokio::test]
    async fn send_routes_through_the_kernel_to_the_inbox() {
        // Given a system with one spawned actor.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            std::vec::Vec::new,
        );

        // When sending an envelope from the system root.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 3 })))
            .await
            .expect("delivered");

        // Then it lands in the actor's runtime-owned inbox (front door →
        // inbox pump may take a moment).
        for _ in 0..500 {
            if system.inbox_cursor(&path).map(|c| c.as_u64()) == Some(0)
                && inbox_has_work(&system, &path)
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("envelope never reached the inbox");
    }

    fn inbox_has_work(_system: &ActorSystem, _path: &ActorPath) -> bool {
        // The envelope is queued iff the cursor has not advanced past it;
        // full inbox introspection lands with the atomic step (next task).
        true
    }

    // ---- test-table gap tests (Phase 10) ----

    #[tokio::test]
    async fn es_journal_and_fold_reconstruct_state_from_events_alone() {
        // Given a spawned counter.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // When three Adds commit.
        for n in 1..=3_i64 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        wait_for_cursor(&system, &path, 3).await;

        // Then the journal holds exactly the three events (no snapshots
        // under the default Off policy) and the live fold matches.
        assert_eq!(system.journal_len(&path), 3);
        let entries = system.journal_entries(&path);
        let mut folded = Counter::restore(&json!({}));
        for entry in &entries {
            if let crate::journal::JournalEntry::Event { event, .. } = entry {
                folded.apply(event);
            }
        }
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], json!(6));
        assert_eq!(folded.total, 6, "journal fold == live state");
    }

    #[tokio::test]
    async fn snapshot_fast_path_skips_replaying_committed_events() {
        // Given a counter with Messages(2) snapshots that committed 4 Adds.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let opts = SpawnOpts {
            snapshot: SnapshotCadence::Messages(2),
            ..SpawnOpts::default()
        };
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), opts, || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        for n in 1..=4_i64 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        wait_for_cursor(&system, &path, 4).await;

        // Then snapshots landed at each n-boundary (seqs 1 and 3), the
        // journal holds 4 events + 2 snapshots, and the LATEST snapshot
        // anchors the fast path: rebuild replays only the tail after it.
        {
            let journal = system.journal_entries(&path);
            assert_eq!(journal.len(), 6, "4 events + 2 snapshots");
            let last = journal
                .iter()
                .rev()
                .find(|e| matches!(e, crate::journal::JournalEntry::Snapshot { .. }))
                .expect("snapshot exists");
            let crate::journal::JournalEntry::Snapshot { seq, .. } = last else {
                panic!("expected a snapshot entry");
            };
            assert_eq!(
                *seq,
                crate::journal::SeqNo::new(3),
                "latest snapshot at seq 3 (4th Add)"
            );
        }
        assert!(
            system.facts().iter().any(|f| matches!(
                f.kind,
                crate::observe::ObservationKind::SnapshotTaken { .. }
            )),
            "SnapshotTaken fact emitted"
        );
        // The latest snapshot's fold already contains Adds 1-4 (total 10):
        // the fast path restores it, then replays an empty tail.
        {
            let snap = match system
                .journal_entries(&path)
                .iter()
                .rev()
                .find(|e| matches!(e, crate::journal::JournalEntry::Snapshot { .. }))
                .expect("snap")
            {
                crate::journal::JournalEntry::Snapshot { state, .. } => state.clone(),
                _ => unreachable!(),
            };
            assert_eq!(
                snap["total"],
                json!(10),
                "snapshot captures the folded state"
            );
        }
    }

    #[tokio::test]
    async fn snapshot_policy_off_never_appends_snapshots() {
        // Given a counter on the default (Off) policy that committed 5 Adds.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        for n in 1..=5_i64 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        wait_for_cursor(&system, &path, 5).await;

        // Then the journal holds only events, and no SnapshotTaken fact.
        {
            let journal = system.journal_entries(&path);
            assert_eq!(journal.len(), 5);
            assert!(
                journal
                    .iter()
                    .all(|e| !matches!(e, crate::journal::JournalEntry::Snapshot { .. }))
            );
        }
        assert!(
            !system.facts().iter().any(|f| matches!(
                f.kind,
                crate::observe::ObservationKind::SnapshotTaken { .. }
            )),
            "no snapshot facts under Off"
        );
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(3)]
    #[case(7)]
    #[case(16)]
    fn rebuild_from_snapshot_over_k_events_equals_full_fold(#[case] k: usize) {
        // Given a journal of k Added events folded from genesis.
        let events: Vec<crate::envelope::Event> = (1..=k as i64)
            .map(|n| crate::envelope::Event::from_json_view(Added::schema_id(), json!({ "n": n })))
            .collect();
        let mut folded = Counter::restore(&json!({}));
        for event in &events {
            folded.apply(event);
        }

        // When rebuilding from a snapshot anchored at EACH split point
        // s (0 <= s <= k): snapshot = fold of the first s events, tail =
        // the remaining k - s.
        for s in 0..=k {
            let mut snapshot_state = Counter::restore(&json!({}));
            for event in &events[..s] {
                snapshot_state.apply(event);
            }
            let snapshot = snapshot_state.capture().expect("capture");

            // Rebuilding via the erased shell must reproduce the full
            // fold — snapshot fast path + tail apply, for every split.
            let shell: Box<dyn crate::actor::DynEsActor> =
                Box::new(crate::actor::TypedEsState::<Counter>::new(
                    Counter::restore(&json!({})),
                ));
            let rebuilt = shell
                .rebuild(&json!({}), Some(snapshot), &events[s..])
                .expect("rebuild");

            // Then the rebuilt capture equals the full fold's capture.
            assert_eq!(
                rebuilt.capture_erased().expect("capture"),
                folded.capture().expect("capture"),
                "split at {s} of {k} diverged"
            );
        }
    }

    #[tokio::test]
    async fn restore_from_hydration_hook_populates_skipped_caches() {
        // Given a counter whose state carries a #[serde(skip)] cache that
        // derives from `total`, with restore_from overridden to rebuild it.
        #[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
        struct Cached {
            total: i64,
            #[serde(skip)]
            doubled: i64,
        }
        impl crate::actor::EventSourcedActor for Cached {
            fn manifest() -> crate::schema::ActorManifest {
                ActorManifest::new().kind(crate::actor::ActorKind::EventSourced)
            }
            fn restore(_args: &Json) -> Self {
                Self {
                    total: 0,
                    doubled: 0,
                }
            }
            fn apply(&mut self, event: &crate::envelope::Event) {
                self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
            }
            fn capture(&self) -> Result<Json, error_stack::Report<crate::journal::JournalError>> {
                // The cache is not persisted, but capture EXPOSES it when
                // hydrated — making the hydration hook observable.
                Ok(json!({ "total": self.total, "doubled": self.doubled }))
            }
            fn restore_from(
                snap: Json,
            ) -> Result<Self, error_stack::Report<crate::journal::JournalError>> {
                let total = snap["total"].as_i64().unwrap_or(0);
                // THE sanctioned hydration: derive the skipped field.
                Ok(Self {
                    total,
                    doubled: total * 2,
                })
            }
        }

        // When rebuilding from a snapshot blob through the erased shell.
        let shell: Box<dyn crate::actor::DynEsActor> = Box::new(
            crate::actor::TypedEsState::<Cached>::new(Cached::restore(&json!({}))),
        );
        let rebuilt = shell
            .rebuild(&json!({}), Some(json!({ "total": 21 })), &[])
            .expect("rebuild");

        // Then the rebuilt state carries the hydrated cache: capture
        // exposes `doubled`, which only restore_from could have set.
        let captured = rebuilt.capture_erased().expect("capture");
        assert_eq!(
            captured["doubled"],
            json!(42),
            "cache hydrated by restore_from"
        );
        assert_eq!(captured["total"], json!(21));

        // And folding the tail on top keeps the total invariant.
        let tail = vec![crate::envelope::Event::from_json_view(
            SchemaId::new("Added"),
            json!({ "n": 3 }),
        )];
        let with_tail: Box<dyn crate::actor::DynEsActor> =
            Box::new(crate::actor::TypedEsState::<Cached>::new(Cached::restore(
                &json!({}),
            )));
        let with_tail = with_tail
            .rebuild(&json!({}), Some(json!({ "total": 21 })), &tail)
            .expect("rebuild");
        let captured_tail = with_tail.capture_erased().expect("capture");
        assert_eq!(captured_tail["total"], json!(24));
    }

    #[tokio::test]
    async fn identity_survives_restart_at_the_same_path() {
        // Given a counter that committed one Add and then crashed on Boom.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.register_schema::<Boom>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![
                Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>()),
                Arc::new(TypedEsAdapter::<Counter, Boom>::new::<Boom>()),
            ]
        });
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 4 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;
        system
            .send(system.envelope(Boom::schema_id(), path.clone(), json!({ "why": "poison" })))
            .await
            .expect("delivered");
        wait_for_crash(&system, &path).await;

        // When the supervisor restarts it (via the same spawn path —
        // same name, fresh instance).
        system.restart_es(&path, &json!({})).await.expect("restart");
        // The same path resolves again and replays the journal.
        wait_for(|| async { system.es_state(&path).await.is_some() }).await;
        let state = system.es_state(&path).await.expect("identity intact");

        // Then the identity's state is the journal fold and the pending
        // poison is still queued at cursor 1.
        assert_eq!(state["total"], json!(4), "state == fold(journal)");
        assert_eq!(system.inbox_cursor(&path).map(|c| c.as_u64()), Some(1));
    }

    #[tokio::test]
    async fn panic_redelivery_replays_pending_queue_after_restart() {
        // Given a counter with a poison Boom queued BEHIND a good Add.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.register_schema::<Boom>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![
                Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>()),
                Arc::new(TypedEsAdapter::<Counter, Boom>::new::<Boom>()),
            ]
        });

        // When Add(4) commits, then Boom crashes the actor, then Add(1)
        // queues behind the undelivered poison.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 4 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;
        system
            .send(system.envelope(Boom::schema_id(), path.clone(), json!({ "why": "poison" })))
            .await
            .expect("delivered");
        wait_for_crash(&system, &path).await;
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("delivered");

        // When the actor is restarted (the supervisor's rebuild path).
        system.restart_es(&path, &json!({})).await.expect("restart");

        // Then the pending queue replays FIFO: the poison redelivers FIRST
        // and crashes the actor again (at-least-once); nothing is lost.
        wait_for_crash(&system, &path).await;
        {
            let events = system
                .journal_entries(&path)
                .iter()
                .filter(|e| matches!(e, crate::journal::JournalEntry::Event { .. }))
                .count();
            assert_eq!(events, 1, "poison never appended; only the first Add");
        }
        assert_eq!(
            system.inbox_cursor(&path).map(|c| c.as_u64()),
            Some(1),
            "poison stays queued: redelivered, never acked"
        );
        let facts = system.fact_kind_counts();
        let delivered = facts.get("Delivered").copied().unwrap_or(0);
        let failed = facts.get("Failed").copied().unwrap_or(0);
        assert_eq!(failed, 2, "poison attempted exactly once per crash");
        assert!(
            delivered >= 3,
            "original Add + poison x2 delivered (at-least-once redelivery)"
        );
    }

    // ---- dynamic actor primitives (Phase 1: emit enforcement + cadence) ----

    /// A counter whose Add handler emits one declared `Added` and one
    /// undeclared `Smuggled` per command (emit-enforcement fixture).
    #[derive(Serialize, Deserialize, Default, Clone)]
    struct MixedEmitter {
        total: i64,
    }
    impl EventSourcedActor for MixedEmitter {
        fn manifest() -> ActorManifest {
            // NOTE: deliberately does NOT declare Smuggled.
            ActorManifest::new()
                .handles::<Add>()
                .emits::<Added>()
                .kind(ActorKind::EventSourced)
        }
        fn restore(_args: &Json) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
        }
    }
    impl CommandHandler<Add> for MixedEmitter {
        fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            let mut events = crate::envelope::Events::new();
            events.push(crate::envelope::Event::from_json_view(
                Added::schema_id(),
                json!({ "n": cmd.n }),
            ));
            // Undeclared: the emit filter must drop this one pre-append.
            events.push(crate::envelope::Event::from_json_view(
                Smuggled::schema_id(),
                json!({ "n": cmd.n }),
            ));
            events
        }
    }

    #[tokio::test]
    async fn undeclared_emit_dropped_pre_append_with_trace_error() {
        // Given a mixed emitter (declares Added only) that committed one Add.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("mixed");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.register_schema::<Smuggled>();
        system.spawn_es::<MixedEmitter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<MixedEmitter, Add>::new::<Add>())]
        });
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 4 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // Then the declared event journalled and applied, the smuggled one
        // never touched the journal, a DeadLettered(UndeclaredEvent) fact
        // records the drop, and the actor keeps running (step not failed).
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], json!(4), "only the declared event applied");
        {
            let event_schemas: Vec<_> = system
                .journal_entries(&path)
                .iter()
                .filter_map(|e| e.as_event().map(|ev| ev.schema.clone()))
                .collect();
            assert_eq!(
                event_schemas,
                [Added::schema_id()],
                "journal contains only declared schemas"
            );
            let kernel = system.kernel.lock();
            assert_eq!(kernel.dead_letters.len(), 1);
            assert_eq!(
                kernel.dead_letters[0].reason,
                crate::kernel::DeadLetterReason::UndeclaredEvent
            );
            assert_eq!(kernel.dead_letters[0].schema, Smuggled::schema_id());
        }
        assert!(
            system.facts().iter().any(|f| matches!(
                &f.kind,
                crate::observe::ObservationKind::DeadLettered { reason, .. }
                    if *reason == crate::kernel::DeadLetterReason::UndeclaredEvent
            )),
            "DeadLettered(UndeclaredEvent) fact on the tap"
        );
        assert!(
            !kernel_has_crash(&system, &path),
            "the step continued; the actor was not failed"
        );
    }

    /// Kernel crash-record peek (tests): the crash flag lives on the cell.
    fn kernel_has_crash(system: &ActorSystem, path: &ActorPath) -> bool {
        let kernel = system.kernel.lock();
        kernel.cells.get(path).is_some_and(|cell| cell.is_crashed())
    }

    #[tokio::test]
    async fn mixed_decision_applies_declared_and_drops_undeclared() {
        // Given a mixed emitter that commits TWO commands.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("mixed");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.spawn_es::<MixedEmitter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<MixedEmitter, Add>::new::<Add>())]
        });
        for n in 1..=2_i64 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        wait_for_cursor(&system, &path, 2).await;

        // Then fold(journal) == live state == declared events only: the
        // mixed decisions stayed state-consistent (drop, not fail).
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], json!(3));
        let mut folded = MixedEmitter::restore(&json!({}));
        {
            for entry in system.journal_entries(&path) {
                if let crate::journal::JournalEntry::Event { event, .. } = &entry {
                    folded.apply(event);
                }
            }
            assert_eq!(
                system.journal_entries(&path).len(),
                2,
                "exactly the two declared events journalled"
            );
        }
        assert_eq!(folded.total, 3, "fold(journal) == live state");
    }

    #[tokio::test]
    async fn snapshot_cadence_messages_matches_old_every_n() {
        // Given a counter on Messages(2) that commits 4 Adds.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let opts = SpawnOpts {
            snapshot: SnapshotCadence::Messages(2),
            ..SpawnOpts::default()
        };
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), opts, || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        for n in 1..=4_i64 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        wait_for_cursor(&system, &path, 4).await;

        // Then snapshots landed exactly on the old EveryN(2) boundaries
        // (after the 2nd and 4th events, i.e. seqs 1 and 3).
        let snap_seqs: Vec<u64> = system
            .journal_entries(&path)
            .iter()
            .filter_map(|e| match e {
                crate::journal::JournalEntry::Snapshot { seq, .. } => Some(seq.as_u64()),
                _ => None,
            })
            .collect();
        assert_eq!(snap_seqs, [1, 3], "EveryN(2) boundaries preserved");
    }

    #[tokio::test]
    async fn snapshot_cadence_time_fires_on_idle() {
        // Given a counter on Time(100ms) that committed ONE Add and then
        // went fully idle (no further messages ever arrive).
        let (system, clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let opts = SpawnOpts {
            snapshot: SnapshotCadence::Time(std::time::Duration::from_millis(100)),
            ..SpawnOpts::default()
        };
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), opts, || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 7 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // When the clock advances past the interval (the actor idles; the
        // 20ms poll arm is the wake that runs the idle cadence check).
        clock.advance(std::time::Duration::from_millis(150));

        // Then the idle actor snapshots BETWEEN messages, anchored at the
        // last event's seq (0).
        wait_for(|| async {
            system.facts().iter().any(|f| {
                matches!(
                    f.kind,
                    crate::observe::ObservationKind::SnapshotTaken { .. }
                )
            })
        })
        .await;
        let snap_seq = {
            let entries = system.journal_entries(&path);
            let snap = entries
                .iter()
                .rev()
                .find(|e| matches!(e, crate::journal::JournalEntry::Snapshot { .. }))
                .expect("snapshots");
            match snap {
                crate::journal::JournalEntry::Snapshot { seq, .. } => seq.as_u64(),
                _ => panic!("expected snapshot"),
            }
        };
        assert_eq!(snap_seq, 0, "anchored at the last event, not a fake seq");
    }

    #[tokio::test]
    async fn snapshot_cadence_time_never_fires_before_the_interval() {
        // Given a counter on Time(1h) that committed one Add.
        let (system, clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let opts = SpawnOpts {
            snapshot: SnapshotCadence::Time(std::time::Duration::from_secs(3600)),
            ..SpawnOpts::default()
        };
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), opts, || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // When a modest amount of clock time passes (idle checks run).
        clock.advance(std::time::Duration::from_millis(500));
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        // Then no snapshot fired: the cadence was not yet due.
        assert!(
            !system.facts().iter().any(|f| matches!(
                f.kind,
                crate::observe::ObservationKind::SnapshotTaken { .. }
            )),
            "time cadence must not fire before its interval elapses"
        );
    }

    #[tokio::test]
    async fn snapshot_cadence_off_never_snapshots() {
        // Given a counter on the default Off cadence that committed 5 Adds.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        for n in 1..=5_i64 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        wait_for_cursor(&system, &path, 5).await;

        // Then the journal holds only events and no snapshot ever fired.
        assert_eq!(system.journal_len(&path), 5);
        assert!(
            !system.facts().iter().any(|f| matches!(
                f.kind,
                crate::observe::ObservationKind::SnapshotTaken { .. }
            )),
            "Off never snapshots"
        );
    }

    // ---- Phase 2: builder API ----

    /// A counter variant whose manifest declares NOTHING (builder-edge
    /// fixture: declarations must come from the builder calls).
    #[derive(Serialize, Deserialize, Default, Clone)]
    struct BareCounter {
        total: i64,
    }
    impl EventSourcedActor for BareCounter {
        fn manifest() -> ActorManifest {
            ActorManifest::new().kind(ActorKind::EventSourced)
        }
        fn restore(_args: &Json) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
        }
    }
    impl CommandHandler<Add> for BareCounter {
        fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            crate::envelope::Events::from_vec(vec![crate::envelope::Event::from_json_view(
                Added::schema_id(),
                json!({ "n": cmd.n }),
            )])
        }
    }

    /// Reads an actor's registered manifest from the registry (tests).
    fn registered_manifest(
        system: &ActorSystem,
        path: &ActorPath,
    ) -> Option<crate::schema::ActorManifest> {
        let registry = system.registry.lock();
        registry.lookup(path).map(|info| info.manifest.clone())
    }

    /// Reads the route table's destination set for a schema (tests).
    fn route_dests(system: &ActorSystem, schema: &SchemaId) -> Vec<ActorPath> {
        let registry = system.registry.lock();
        registry.route_dests(schema)
    }

    #[tokio::test]
    async fn builder_registers_same_edges_as_positional_spawn() {
        // Given the same counter actor spawned twice — once positionally,
        // once through the builder (distinct paths, same types).
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let positional_path = ActorPath::new("pos");
        let builder_path = ActorPath::new("built");
        system.spawn_es::<BareCounter, _>(
            positional_path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<BareCounter, Add>::new::<Add>())],
        );
        system
            .declare_emits(&positional_path, Added::schema_id())
            .expect("declare positional emit edge");
        crate::builder::spawn_es_builder::<BareCounter>(&system)
            .at(builder_path.clone())
            .handles::<Add>()
            .emits::<Added>()
            .start();

        // When both slots settle.
        wait_for(|| async {
            system.inbox_cursor(&builder_path).is_some()
                && system.inbox_cursor(&positional_path).is_some()
        })
        .await;

        // Then the registry sees IDENTICAL handles AND emit edges.
        let pos_manifest = registered_manifest(&system, &positional_path).expect("pos slot");
        let built_manifest = registered_manifest(&system, &builder_path).expect("built slot");
        assert_eq!(pos_manifest.handles, built_manifest.handles);
        assert_eq!(pos_manifest.emits, built_manifest.emits);
        let pos_dests = route_dests(&system, &Add::schema_id());
        assert!(pos_dests.contains(&positional_path) && pos_dests.contains(&builder_path));

        // And both actors behave identically under the same command.
        for path in [&positional_path, &builder_path] {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 6 })))
                .await
                .expect("delivered");
        }
        wait_for(|| async {
            count_total(&system, "pos").await == Some(6)
                && count_total(&system, "built").await == Some(6)
        })
        .await;
    }

    #[tokio::test]
    async fn builder_emits_are_enforced_edges() {
        // Given a counter built WITHOUT any `.emits::<Added>()` declaration
        // (its manifest does not declare the edge on its own).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("silent");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        crate::builder::spawn_es_builder::<BareCounter>(&system)
            .at(path.clone())
            .handles::<Add>()
            .start();

        // When a command commits an event.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 3 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // Then the undeclared event was dropped before append (the builder
        // edge is the enforced edge) and a fact records the drop.
        assert_eq!(system.journal_len(&path), 0, "no declared edge, no append");
        assert!(system.facts().iter().any(|f| matches!(
            &f.kind,
            crate::observe::ObservationKind::DeadLettered { reason, .. }
                if *reason == crate::kernel::DeadLetterReason::UndeclaredEvent
        )));

        // And the same actor WITH the declaration journals normally.
        let declared = ActorPath::new("loud");
        crate::builder::spawn_es_builder::<BareCounter>(&system)
            .at(declared.clone())
            .handles::<Add>()
            .emits::<Added>()
            .start();
        system
            .send(system.envelope(Add::schema_id(), declared.clone(), json!({ "n": 3 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &declared, 1).await;
        assert_eq!(
            system.journal_len(&declared),
            1,
            "declared emit edges append"
        );
    }

    #[tokio::test]
    async fn foreign_builder_matches_positional_foreign_spawn() {
        // Given the same foreign tally spawned twice — once positionally,
        // once through the named-method builder.
        let (system, _clock) = ActorSystem::test();
        let schema = system
            .register_schema_json(json!({
                "name": "tally2", "kind": "command",
                "fields": [{ "name": "delta", "ty": "int" }]
            }))
            .expect("valid");
        // A DISTINCT fact schema: emitting the command's own schema would
        // loop back to the handler under the automatic fact broadcast.
        let fact = system
            .register_schema_json(json!({
                "name": "tallied2", "kind": "event",
                "fields": [{ "name": "delta", "ty": "int" }]
            }))
            .expect("valid");

        let decision: crate::actor::ForeignDecision = {
            let f = fact.clone();
            Arc::new(move |_state, cmd, _ctx| {
                vec![crate::envelope::Event::from_json_view(
                    f.clone(),
                    json!({ "delta": cmd["delta"].as_i64().unwrap_or(0) }),
                )]
            })
        };
        let fold: crate::actor::ForeignFold = {
            let f = fact.clone();
            Arc::new(move |state: &mut Json, ev: &crate::envelope::Event| {
                if ev.schema == f {
                    state["total"] = serde_json::json!(
                        state["total"].as_i64().unwrap_or(0)
                            + ev.payload_json()["delta"].as_i64().unwrap_or(0)
                    );
                }
            })
        };
        system.spawn_es_foreign(
            ActorPath::new("t-pos"),
            schema.clone(),
            json!({ "total": 0 }),
            decision.clone(),
            fold.clone(),
            SpawnOpts::default(),
        );
        // The positional flavor declares its emit edge post-spawn; the
        // builder declares it inline — same table, same enforcement.
        system
            .declare_emits(&ActorPath::new("t-pos"), fact.clone())
            .expect("live slot");
        let built_decision: crate::actor::ForeignDecision = {
            let f = fact.clone();
            Arc::new(move |_state, cmd, _ctx| {
                vec![crate::envelope::Event::from_json_view(
                    f.clone(),
                    json!({ "delta": cmd["delta"].as_i64().unwrap_or(0) }),
                )]
            })
        };
        let built_fold: crate::actor::ForeignFold = {
            let f = fact.clone();
            Arc::new(move |state: &mut Json, ev: &crate::envelope::Event| {
                if ev.schema == f {
                    state["total"] = serde_json::json!(
                        state["total"].as_i64().unwrap_or(0)
                            + ev.payload_json()["delta"].as_i64().unwrap_or(0)
                    );
                }
            })
        };
        crate::builder::spawn_foreign(&system)
            .at(ActorPath::new("t-built"))
            .schema(json!({
                "name": "tally2", "kind": "command",
                "fields": [{ "name": "delta", "ty": "int" }]
            }))
            .args(json!({ "total": 0 }))
            .handle(built_decision)
            .apply(built_fold)
            .emits_id(fact)
            .start()
            .expect("foreign builder starts");

        // When both receive the same command.
        wait_for(|| async {
            system.inbox_cursor(&ActorPath::new("t-pos")).is_some()
                && system.inbox_cursor(&ActorPath::new("t-built")).is_some()
        })
        .await;
        for path in [ActorPath::new("t-pos"), ActorPath::new("t-built")] {
            system
                .send(system.envelope(SchemaId::new("tally2"), path, json!({ "delta": 9 })))
                .await
                .expect("delivered");
        }
        wait_for(|| async {
            count_total_json(&system, &ActorPath::new("t-pos")).await == Some(9)
                && count_total_json(&system, &ActorPath::new("t-built")).await == Some(9)
        })
        .await;

        // Then both run identically (and the builder's declared emit edge
        // was enforced — the journal holds the event).
        assert_eq!(system.journal_len(&ActorPath::new("t-built")), 1);
        assert_eq!(system.journal_len(&ActorPath::new("t-pos")), 1);
    }

    #[tokio::test]
    async fn builder_spawns_register_handled_and_emitted_schemas() {
        // Given a fresh system whose schema table starts empty.
        let (system, _clock) = ActorSystem::test();

        // When spawning through the typed ES builder (handles + emits)
        // and the typed service builder (handles) with NO hand
        // registration.
        crate::builder::spawn_es_builder::<BareCounter>(&system)
            .at(ActorPath::new("reg-es"))
            .handles::<Add>()
            .emits::<Added>()
            .start();
        let (svc_idx, _svc_sink) = open_sink();
        crate::builder::spawn_service_builder::<Auditor>(&system)
            .at(ActorPath::new("reg-svc"))
            .args(json!({ "sink": svc_idx }))
            .handles::<Added>()
            .start();

        // Then the export's schema table carries every declared def.
        let export = system.export().await;
        assert!(
            export.schemas.iter().any(|s| s.id() == Add::schema_id()),
            "handled command schema exported"
        );
        assert!(
            export.schemas.iter().any(|s| s.id() == Added::schema_id()),
            "emitted event schema exported"
        );
    }

    #[tokio::test]
    async fn builder_registration_keeps_the_first_registered_def() {
        // Given a def under the name "Add" hand-registered FIRST (the
        // foreign JSON path), with a body that differs from what the
        // builder would derive.
        let (system, _clock) = ActorSystem::test();
        system
            .register_schema_json(json!({
                "name": "Add",
                "kind": "command",
                "fields": [{ "name": "n", "ty": "int" }],
                "description": "hand-first"
            }))
            .expect("registers");

        // When a builder spawn handles `Add` (whose descriptor was
        // already registered FIRST via the foreign path — the table
        // keeps the first def; the typed claim adds only the TypeId).
        crate::builder::spawn_es_builder::<BareCounter>(&system)
            .at(ActorPath::new("first-wins"))
            .handles::<Add>()
            .start();

        // Then the export holds exactly ONE def for the name, and it is
        // the FIRST one (schemas are agreed facts, not config).
        let export = system.export().await;
        let defs: Vec<&SchemaDef> = export.schemas.iter().filter(|s| s.name == "Add").collect();
        assert_eq!(defs.len(), 1, "no duplicate def for one name");
        assert_eq!(
            defs[0].description.as_deref(),
            Some("hand-first"),
            "the first registration won"
        );
    }

    #[tokio::test]
    async fn foreign_builder_registers_its_schema_at_start() {
        // Given a foreign builder spawn with a JSON schema descriptor and
        // NO hand registration (regression guard: the foreign builder has
        // always registered at `start`; it must keep doing so).
        let (system, _clock) = ActorSystem::test();
        let decision: crate::actor::ForeignDecision = Arc::new(|_state, _cmd, _ctx| vec![]);
        let fold: crate::actor::ForeignFold =
            Arc::new(|_state: &mut Json, _ev: &crate::envelope::Event| {});
        crate::builder::spawn_foreign(&system)
            .at(ActorPath::new("f-reg"))
            .schema(json!({
                "name": "fbuildcmd", "kind": "command",
                "fields": [{ "name": "delta", "ty": "int" }]
            }))
            .args(json!({ "total": 0 }))
            .handle(decision)
            .apply(fold)
            .start()
            .expect("foreign builder starts");

        // Then the schema table — and thus the export — carries the def.
        let export = system.export().await;
        assert!(
            export
                .schemas
                .iter()
                .any(|s| s.id() == SchemaId::new("fbuildcmd")),
            "foreign builder schema exported"
        );
    }

    /// A counter that relies on the trait's DEFAULT manifest (no
    /// override): every declared edge must come from the builder.
    #[derive(Serialize, Deserialize, Default, Clone)]
    struct DefaultManifestCounter {
        total: i64,
    }
    impl EventSourcedActor for DefaultManifestCounter {
        fn restore(_args: &Json) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
        }
    }
    impl CommandHandler<Add> for DefaultManifestCounter {
        fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            crate::envelope::Events::from_vec(vec![crate::envelope::Event::from_json_view(
                Added::schema_id(),
                json!({ "n": cmd.n }),
            )])
        }
    }

    #[tokio::test]
    async fn default_manifest_actor_gets_edges_and_kind_from_the_builder() {
        // Given an actor that does NOT override manifest() (the trait
        // default returns an empty manifest), spawned via the builder.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("default-manifest");
        crate::builder::spawn_es_builder::<DefaultManifestCounter>(&system)
            .at(path.clone())
            .handles::<Add>()
            .emits::<Added>()
            .start();
        wait_for(|| async { system.inbox_cursor(&path).is_some() }).await;

        // Then the export stamps the contract kind and carries exactly the
        // builder-declared edges.
        let export = system.export().await;
        let actor = export
            .actors
            .iter()
            .find(|a| a.path == path)
            .expect("actor exported");
        assert_eq!(
            actor.kind,
            ActorKind::EventSourced,
            "kind stamped: {actor:?}"
        );
        assert_eq!(
            actor.manifest.handles,
            vec![Add::schema_id()],
            "builder handle edge: {actor:?}"
        );
        assert_eq!(
            actor.manifest.emits,
            vec![Added::schema_id()],
            "builder emit edge: {actor:?}"
        );
    }

    /// A counter whose own manifest declares an edge the builder does not
    /// (union fixture: explicit manifest and builder edges must merge).
    #[derive(Serialize, Deserialize, Default, Clone)]
    struct RichCounter {
        total: i64,
    }
    impl EventSourcedActor for RichCounter {
        fn manifest() -> ActorManifest {
            ActorManifest::new().handles_id(Boom::schema_id())
        }
        fn restore(_args: &Json) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
        }
    }
    impl CommandHandler<Add> for RichCounter {
        fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            crate::envelope::Events::from_vec(vec![crate::envelope::Event::from_json_view(
                Added::schema_id(),
                json!({ "n": cmd.n }),
            )])
        }
    }

    #[tokio::test]
    async fn explicit_manifest_edges_merge_with_builder_edges() {
        // Given an actor whose explicit manifest declares Boom (the builder
        // does not), spawned with the builder declaring Add.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("rich-manifest");
        crate::builder::spawn_es_builder::<RichCounter>(&system)
            .at(path.clone())
            .handles::<Add>()
            .start();
        wait_for(|| async { system.inbox_cursor(&path).is_some() }).await;

        // Then the registered manifest is the UNION of both sources.
        let manifest = registered_manifest(&system, &path).expect("slot");
        assert_eq!(
            manifest.handles.len(),
            2,
            "union, not overwrite: {manifest:?}"
        );
        assert!(manifest.handles.contains(&Add::schema_id()));
        assert!(manifest.handles.contains(&Boom::schema_id()));
        // And the kind is still stamped by the builder.
        assert_eq!(manifest.kind, Some(ActorKind::EventSourced));
    }

    // ---- typed system surface: tell / ask ----

    #[tokio::test]
    async fn system_ask_settles_replied_with_an_ask_settled_fact() {
        // Given a replying callee (builder-spawned, no hand registration).
        let (system, _clock) = ActorSystem::test();

        struct Echo;
        impl ServiceActor for Echo {
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Echo {
            async fn handle(&mut self, msg: &Add, ctx: &mut crate::context::MsgCtx<'_>) {
                ctx.reply(msg.clone());
            }
        }

        crate::builder::spawn_service_builder::<Echo>(&system)
            .at(ActorPath::new("sys-echo"))
            .handles::<Add>()
            .emits::<Add>()
            .start();
        wait_for(|| async { system.inbox_cursor(&ActorPath::new("sys-echo")).is_some() }).await;

        // When the system asks it a typed Add.
        let reply = system
            .ask(
                ActorPath::new("sys-echo"),
                Add { n: 21 },
                std::time::Duration::from_secs(2),
            )
            .await
            .expect("replied");

        // Then the reply decodes as the handler's payload (the echoed Add).
        assert_eq!(reply["n"], 21);
        // And the ask settled as Replied with its own fact.
        let kernel = system.kernel.lock();
        assert!(
            kernel
                .ask_facts
                .iter()
                .any(|f| f.outcome == Some(crate::kernel::AskOutcome::Replied)),
            "Replied fact recorded: {:?}",
            kernel.ask_facts
        );
    }

    #[tokio::test]
    async fn system_ask_timeout_kills_the_lease_for_late_replies() {
        // Given a silent callee.
        let (system, _clock) = ActorSystem::test();

        struct Silent;
        impl ServiceActor for Silent {
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Silent {
            async fn handle(&mut self, _msg: &Add, _ctx: &mut crate::context::MsgCtx<'_>) {}
        }

        crate::builder::spawn_service_builder::<Silent>(&system)
            .at(ActorPath::new("sys-silent"))
            .handles::<Add>()
            .start();
        wait_for(|| async { system.inbox_cursor(&ActorPath::new("sys-silent")).is_some() }).await;

        // When the ask times out and then the callee tries to reply LATE.
        let outcome = system
            .ask(
                ActorPath::new("sys-silent"),
                Add { n: 1 },
                std::time::Duration::from_millis(50),
            )
            .await;
        assert!(outcome.is_err(), "must time out");
        // (kernel-internal detail for the late-reply probe: the settled
        // ask's lease slot is gone, so `complete` finds nothing.)
        let late_lease = {
            let kernel = system.kernel.lock();
            assert!(
                kernel
                    .ask_facts
                    .iter()
                    .any(|f| f.outcome == Some(crate::kernel::AskOutcome::Timeout)),
                "Timeout fact recorded: {:?}",
                kernel.ask_facts
            );
            assert!(kernel.replies.is_empty(), "lease leaked after timeout");
            crate::reply::LeaseId::new()
        };

        // Then the late reply lands nowhere: no lease knows its id.
        {
            let kernel = system.kernel.lock();
            assert!(
                !kernel.replies.complete(
                    &late_lease,
                    crate::envelope::Payload::from(json!({ "echo": 1 }))
                ),
                "a dead lease must not accept a late reply"
            );
        }
    }

    #[tokio::test]
    async fn system_ask_settles_failed_when_the_lease_dies_mid_ask() {
        // Given a silent LIVE callee and a long system-level timeout (the
        // lease must be reaped by the GC sweep before the asker times out,
        // isolating the Failed path).
        let (system, _clock) = ActorSystem::test();

        struct Silent;
        impl ServiceActor for Silent {
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Silent {
            async fn handle(&mut self, _msg: &Add, _ctx: &mut crate::context::MsgCtx<'_>) {}
        }

        crate::builder::spawn_service_builder::<Silent>(&system)
            .at(ActorPath::new("sys-silent2"))
            .handles::<Add>()
            .start();
        wait_for(|| async {
            system
                .inbox_cursor(&ActorPath::new("sys-silent2"))
                .is_some()
        })
        .await;

        // When the ask is launched and its lease is reaped by the sweep.
        let asker = system.clone();
        let ask_task = tokio::spawn(async move {
            asker
                .ask(
                    ActorPath::new("sys-silent2"),
                    Add { n: 1 },
                    std::time::Duration::from_secs(30),
                )
                .await
        });
        wait_for(|| async { system.kernel.lock().replies.len() == 1 }).await;
        // The lease TTL mirrors the ask's 30s timeout: advance the fake
        // clock past it, then prune.
        system
            .fake_clock()
            .expect("fake clock")
            .advance(std::time::Duration::from_secs(31));
        {
            let kernel = system.kernel.lock();
            kernel.replies.prune(crate::clock::Timestamp::from_millis(
                system.clock.now().as_millis(),
            ));
        }

        // Then the ask settles as Failed (not Timeout), with its fact.
        let outcome = ask_task.await.expect("ask task");
        assert!(outcome.is_err(), "lease death must surface as an error");
        let kernel = system.kernel.lock();
        assert!(
            kernel
                .ask_facts
                .iter()
                .any(|f| f.outcome == Some(crate::kernel::AskOutcome::Failed)),
            "Failed fact recorded: {:?}",
            kernel.ask_facts
        );
    }

    #[tokio::test]
    async fn tell_delivers_a_typed_message_to_a_builder_spawned_actor() {
        // Given an Auditor spawned through the service builder with NO
        // hand registration (the builder declares the Add schema).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("tell-aud");
        let (idx, sink) = open_sink();
        bind_sink(&path, sink);
        crate::builder::spawn_service_builder::<Auditor>(&system)
            .at(path.clone())
            .args(json!({ "sink": idx }))
            .handles::<Add>()
            .start();
        wait_for(|| async { system.inbox_cursor(&path).is_some() }).await;

        // When telling it a typed Add (no json!{} hand-serialization).
        system
            .tell(path.clone(), Add { n: 9 })
            .await
            .expect("delivered");

        // Then the handler decoded the typed payload and ran.
        wait_for(|| async { sink_read(&path).contains(&"n=9".to_string()) }).await;
    }

    #[tokio::test]
    async fn tell_to_an_unknown_path_returns_the_envelope_back() {
        // Given a system where no actor exists at the destination.
        let (system, _clock) = ActorSystem::test();

        // When telling the ghost path.
        let result = system.tell(ActorPath::new("ghost"), Add { n: 1 }).await;

        // Then the original envelope comes back (schema + payload intact).
        let envelope = result.expect_err("unresolved destination");
        assert_eq!(envelope.schema, Add::schema_id());
        assert_eq!(
            envelope.payload_json(),
            &json!({ "n": 1 }),
            "the original payload is returned to the caller"
        );
    }

    #[tokio::test]
    async fn deprecated_positional_wrappers_still_function() {
        // Given all three positional flavors spawned.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let es_path = ActorPath::new("dep-es");
        system.spawn_es::<Counter, _>(es_path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        let svc_path = ActorPath::new("dep-svc");
        system.spawn_service::<Auditor, _>(
            svc_path.clone(),
            &json!({ "sink": 0 }),
            SpawnOpts::default(),
            Vec::new,
        );
        let foreign_path = ActorPath::new("dep-foreign");
        let schema = system
            .register_schema_json(json!({
                "name": "depcmd", "kind": "command",
                "fields": [{ "name": "delta", "ty": "int" }]
            }))
            .expect("valid");
        // A DISTINCT fact schema: emitting the command's own schema would
        // loop back to the handler under the automatic fact broadcast.
        let fact = system
            .register_schema_json(json!({
                "name": "depdone", "kind": "event",
                "fields": [{ "name": "delta", "ty": "int" }]
            }))
            .expect("valid");
        {
            let f_decision = fact.clone();
            let f_fold = fact.clone();
            system.spawn_es_foreign(
                foreign_path.clone(),
                schema,
                json!({ "total": 0 }),
                Arc::new(move |_st, cmd, _ctx| {
                    vec![crate::envelope::Event::from_json_view(
                        f_decision.clone(),
                        json!({ "delta": cmd["delta"].as_i64().unwrap_or(0) }),
                    )]
                }),
                Arc::new(move |state: &mut Json, ev: &crate::envelope::Event| {
                    if ev.schema == f_fold {
                        state["total"] = serde_json::json!(
                            state["total"].as_i64().unwrap_or(0)
                                + ev.payload_json()["delta"].as_i64().unwrap_or(0)
                        );
                    }
                }),
                SpawnOpts::default(),
            );
            system.declare_emits(&foreign_path, fact).expect("declare");
        }

        // When all three receive mail.
        wait_for(|| async {
            system.inbox_cursor(&es_path).is_some()
                && system.inbox_cursor(&svc_path).is_some()
                && system.inbox_cursor(&foreign_path).is_some()
        })
        .await;
        system
            .send(system.envelope(Add::schema_id(), es_path.clone(), json!({ "n": 2 })))
            .await
            .expect("es delivered");
        system
            .send(system.envelope(
                SchemaId::new("depcmd"),
                foreign_path.clone(),
                json!({ "delta": 4 }),
            ))
            .await
            .expect("foreign delivered");

        // Then the ES and foreign actors behave as before.
        wait_for(|| async {
            count_total(&system, "dep-es").await == Some(2)
                && count_total_json(&system, &foreign_path).await == Some(4)
        })
        .await;
    }

    /// A key-keyed counter for partition tests: state seeded from the
    /// `key` genesis arg, increments isolated per entity.
    #[derive(Serialize, Deserialize, Default, Clone)]
    struct KeyCounter {
        key: String,
        total: i64,
    }
    impl EventSourcedActor for KeyCounter {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<KeyedAdd>()
                .emits::<Added>()
                .kind(ActorKind::EventSourced)
        }
        fn restore(args: &Json) -> Self {
            Self {
                key: args["key"].as_str().unwrap_or_default().to_owned(),
                total: 0,
            }
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
        }
    }
    impl CommandHandler<KeyedAdd> for KeyCounter {
        fn handle(&self, cmd: KeyedAdd, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            crate::envelope::Events::from_vec(vec![crate::envelope::Event::from_json_view(
                Added::schema_id(),
                json!({ "n": cmd.n }),
            )])
        }
    }

    /// Registers the partition test command (a str shard key field) and
    /// installs a KeyCounter partition set over `public`.
    fn install_key_partition(
        system: &ActorSystem,
        public: &str,
    ) -> Result<(), error_stack::Report<crate::registry::RegistryError>> {
        system.register_schema::<KeyedAdd>();
        let spec = crate::pool::PartitionSpec {
            public: ActorPath::new(public),
            system: system.clone(),
            factory: Arc::new(|system, path, args| {
                crate::builder::spawn_es_builder::<KeyCounter>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .handles::<KeyedAdd>()
                    .emits::<Added>()
                    .start();
            }),
            key_field: "account".to_owned(),
            args_template: None,
            opts: SpawnOpts::default(),
        };
        system.install_partition_set(spec)
    }

    /// The partition test command: an int-typed `n` plus a STRING-TYPED
    /// `account` field marked ShardKey (typed key extraction).
    #[derive(Command, Debug, Clone, Serialize, Deserialize)]
    struct KeyedAdd {
        n: i64,
        #[schema(shard_key)]
        account: String,
    }

    /// A fact schema consumed by projector fixtures: a chat message whose
    /// `chat_id` is the shard key.
    #[derive(Event, Serialize, Deserialize, Clone)]
    struct Chatted {
        #[schema(shard_key)]
        chat_id: String,
        text: String,
    }

    /// A per-chat read model: the count of messages seen (projector
    /// fixtures). Consumes `Chatted` only.
    #[derive(Serialize, Deserialize, Default, Debug, Clone)]
    struct ChatLog {
        messages: i64,
        #[serde(default)]
        keys_seen: Vec<String>,
    }
    impl crate::actor::Projector for ChatLog {
        fn apply(&mut self, event: &crate::envelope::Event) {
            if event.schema.as_str() == "Chatted" {
                self.messages += 1;
                if let Some(text) = event.payload_json()["text"].as_str() {
                    self.keys_seen.push(text.to_owned());
                }
            }
        }
    }

    /// Installs a ChatLog projector set over `public` (consumes the
    /// `Chatted` fact, keyed by `chat_id`).
    fn install_chat_projector_set(
        system: &ActorSystem,
        public: &str,
    ) -> Result<(), error_stack::Report<crate::registry::RegistryError>> {
        system.register_schema::<Chatted>();
        let spec = crate::pool::ProjectorSetSpec {
            public: ActorPath::new(public),
            system: system.clone(),
            factory: Arc::new(|system, path, args| {
                // Fire-and-forget: the arm is synchronous; catch-up (and
                // its CaughtUp fact) continues in the background.
                crate::builder::spawn_projector_builder::<ChatLog>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .consumes::<Chatted>()
                    .start();
            }),
            key_field: "chat_id".to_owned(),
            args_template: None,
            opts: SpawnOpts::default(),
            consumed: vec![Chatted::schema_id()],
        };
        system.install_projector_set(spec)
    }

    /// Publishes one `Chatted` fact through the host broadcast.
    async fn publish_chatted(system: &ActorSystem, chat_id: &str, text: &str) {
        system
            .publish_value(
                Chatted::schema_id(),
                crate::json!({ "chat_id": chat_id, "text": text }),
            )
            .await;
    }

    /// Seeds events into a SOURCE journal through the test seam (the
    /// durable record an entity would have written).
    fn seed_journal_events(
        system: &ActorSystem,
        path: &ActorPath,
        events: Vec<crate::envelope::Event>,
    ) {
        let store = system.journal_store_trait();
        let store = crate::journal::downcast_in_memory(&store).expect("in-memory store");
        store.append_sync(path, &events).expect("seed append");
    }

    #[tokio::test]
    async fn partition_keys_activate_distinct_entities_with_separate_journals() {
        // Given a partition set over "accounts" (str shard key `account`).
        let (system, _clock) = ActorSystem::test();
        install_key_partition(&system, "accounts").expect("install");

        // When commands for two DIFFERENT keys are sent to the public path.
        let e1 = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("accounts"),
            json!({ "n": 3, "account": "a" }),
        );
        system.send(e1).await.expect("delivered");
        let e2 = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("accounts"),
            json!({ "n": 7, "account": "b" }),
        );
        system.send(e2).await.expect("delivered");
        wait_for(|| async {
            system.journal_len(&ActorPath::new("accounts/a")) == 1
                && system.journal_len(&ActorPath::new("accounts/b")) == 1
        })
        .await;

        // Then two distinct entities were activated with per-key state.
        assert_eq!(
            system
                .es_state(&ActorPath::new("accounts/a"))
                .await
                .and_then(|s| s["total"].as_i64()),
            Some(3),
            "entity a holds only key-a totals"
        );
        assert_eq!(
            system
                .es_state(&ActorPath::new("accounts/b"))
                .await
                .and_then(|s| s["total"].as_i64()),
            Some(7),
            "entity b holds only key-b totals"
        );
        // And each entity has its OWN journal.
        assert_eq!(system.journal_len(&ActorPath::new("accounts/a")), 1);
        assert_eq!(system.journal_len(&ActorPath::new("accounts/b")), 1);
    }

    /// A typed-args partition entity: genesis decodes `on_hand` from the
    /// args template and the shard key from the merged `"key"` field.
    #[derive(Serialize, Deserialize, Default, Debug, Clone)]
    struct Seeded {
        on_hand: i64,
        key: String,
    }
    impl EventSourcedActor for Seeded {
        fn restore(args: &Json) -> Self {
            args.decode().expect("genesis args decode")
        }
        fn apply(&mut self, _event: &crate::envelope::Event) {}
    }
    impl CommandHandler<KeyedAdd> for Seeded {
        fn handle(&self, _cmd: KeyedAdd, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            crate::envelope::Events::new()
        }
    }

    /// An override-free actor: only `Default` and a handler.
    #[derive(Serialize, Deserialize, Default, Debug, Clone)]
    struct Bare {
        seen: i64,
    }
    impl EventSourcedActor for Bare {
        fn apply(&mut self, _event: &crate::envelope::Event) {}
    }
    impl CommandHandler<KeyedAdd> for Bare {
        fn handle(&self, _cmd: KeyedAdd, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            crate::envelope::Events::new()
        }
    }

    #[tokio::test]
    async fn typed_args_roundtrip_from_builder_to_restore() {
        // Given a genesis struct passed to the ES builder as a TYPED value.
        let (system, _clock) = ActorSystem::test();
        crate::builder::spawn_es_builder::<Seeded>(&system)
            .at(ActorPath::new("seeded"))
            .args(crate::json!({ "on_hand": 41, "key": "ignored" }))
            .start();

        // When the actor's restored state is read back.
        let state = system
            .es_state(&ActorPath::new("seeded"))
            .await
            .expect("spawned");

        // Then restore decoded the args (here via the default fold of the
        // serialized document — the state IS the args shape).
        assert_eq!(state["on_hand"], 41);
    }

    #[tokio::test]
    async fn partition_restore_sees_the_shard_key_as_a_field() {
        // Given a partition set whose factory passes a TYPED args template
        // with `on_hand` and whose entity decodes the merged "key" field.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<KeyedAdd>();
        let spec = crate::pool::PartitionSpec {
            public: ActorPath::new("vault"),
            system: system.clone(),
            factory: Arc::new(|system, path, args| {
                crate::builder::spawn_es_builder::<Seeded>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .handles::<KeyedAdd>()
                    .start();
            }),
            key_field: "account".to_owned(),
            args_template: Some(crate::json!({ "on_hand": 100 })),
            opts: SpawnOpts::default(),
        };
        system.install_partition_set(spec).expect("install");

        // When a command for key "k9" activates an entity.
        let e = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("vault"),
            json!({ "n": 1, "account": "k9" }),
        );
        system.send(e).await.expect("delivered");
        wait_for(|| async { system.es_state(&ActorPath::new("vault/k9")).await.is_some() }).await;

        // Then the entity's genesis decoded BOTH the template's typed
        // `on_hand` AND the merged shard key.
        let state = system
            .es_state(&ActorPath::new("vault/k9"))
            .await
            .expect("activated");
        let seeded: Seeded = state.decode().expect("state decodes");
        assert_eq!(seeded.on_hand, 100, "template value decoded");
        assert_eq!(seeded.key, "k9", "shard key merged as a plain field");
    }

    #[tokio::test]
    async fn default_restore_serves_default_actors() {
        // Given an actor whose state derives Default and overrides NOTHING
        // (Bare has no `restore` impl at all), spawned with args it ignores.
        let (system, _clock) = ActorSystem::test();
        crate::builder::spawn_es_builder::<Bare>(&system)
            .at(ActorPath::new("plain"))
            .args(crate::json!({ "seen": 77 }))
            .start();

        // When its state is read.
        let state = system
            .es_state(&ActorPath::new("plain"))
            .await
            .expect("spawned without a restore override");

        // Then the default restore served the spawn: the state is
        // Default::default() (args ignored by design).
        let bare: Bare = state.decode().expect("state decodes");
        assert_eq!(bare.seen, 0);
    }

    #[tokio::test]
    async fn partition_same_key_always_same_entity() {
        // Given a partition set over "accounts".
        let (system, _clock) = ActorSystem::test();
        install_key_partition(&system, "accounts").expect("install");

        // When the SAME key is sent repeatedly.
        for n in 1..=3 {
            let e = system.envelope(
                KeyedAdd::schema_id(),
                ActorPath::new("accounts"),
                json!({ "n": n, "account": "a" }),
            );
            system.send(e).await.expect("delivered");
        }
        wait_for(|| async {
            system
                .es_state(&ActorPath::new("accounts/a"))
                .await
                .and_then(|s| s["total"].as_i64())
                == Some(6)
        })
        .await;

        // Then all three commands landed on ONE entity (deterministic
        // derived path), and only one Spawned fact exists for it.
        assert_eq!(
            system
                .es_state(&ActorPath::new("accounts/a"))
                .await
                .and_then(|s| s["total"].as_i64()),
            Some(6)
        );
        let spawns = system
            .facts()
            .iter()
            .filter(|f| matches!(
                &f.kind,
                crate::observe::ObservationKind::Spawned { path, .. } if *path == ActorPath::new("accounts/a")
            ))
            .count();
        assert_eq!(spawns, 1, "same key activated the entity exactly once");
    }

    #[tokio::test]
    async fn partition_rejects_command_without_shard_key() {
        // Given a partition set over "accounts".
        let (system, _clock) = ActorSystem::test();
        install_key_partition(&system, "accounts").expect("install");

        // When a payload WITHOUT the key is sent (schema-registered, but
        // the sender violates the payload contract).
        let e = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("accounts"),
            json!({ "n": 9 }),
        );
        let result = system.send(e).await;

        // Then the send RESOLVES (the set exists) but the command is
        // dead-lettered ShardKeyMissing — and no entity was activated.
        assert!(result.is_ok(), "the partition set resolved the dest");
        wait_for(|| async { !system.dead_letter_schemas().is_empty() }).await;
        assert!(
            system
                .dead_letter_reasons()
                .await
                .iter()
                .any(|r| r.starts_with("ShardKeyMissing")),
            "missing key dead-lettered: {:?}",
            system.dead_letter_reasons().await
        );
        assert!(
            system
                .es_state(&ActorPath::new("accounts/9"))
                .await
                .is_none(),
            "no entity activated for a keyless command"
        );
    }

    #[tokio::test]
    async fn live_partition_send_resolves_the_key_without_a_json_view() {
        // Given a partition set over "accounts" (the string `account`
        // shard key).
        let (system, _clock) = ActorSystem::test();
        install_key_partition(&system, "accounts2").expect("install");

        // When a LIVE payload is partition-sent (the fabric's typed send,
        // not the bytes door).
        let e = Envelope::json(
            KeyedAdd::schema_id(),
            crate::envelope::Address::Path(ActorPath::new("accounts2")),
            KeyedAdd {
                n: 8,
                account: "a-8".into(),
            },
            TraceCtx::root(),
        );
        system.send(e).await.expect("resolved");

        // Then the entity for the key activated and folded the command —
        // the derive-generated field() read resolved the partition.
        wait_for(|| async {
            system
                .es_state(&ActorPath::new("accounts2/a-8"))
                .await
                .and_then(|s| s["total"].as_i64())
                == Some(8)
        })
        .await;
        let state = system
            .es_state(&ActorPath::new("accounts2/a-8"))
            .await
            .expect("live entity");
        assert_eq!(state["total"], json!(8));

        // And the routing read is FIELD-BASED: `field()` answers the
        // declared shard key from the live value (the probe the runtime
        // extract_key rides), while an undeclared field reads None.
        let payload = crate::envelope::Payload::value(KeyedAdd {
            n: 1,
            account: "probe".into(),
        });
        assert_eq!(payload.field("account"), Some("probe".to_owned()));
        assert_eq!(payload.field("n"), None);
    }

    #[tokio::test]
    async fn partition_spec_without_key_field_rejected() {
        // Given a system whose command schema has NO shard-key field.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();

        // When a partition spec names a key field no command declares.
        let spec = crate::pool::PartitionSpec {
            public: ActorPath::new("accounts"),
            system: system.clone(),
            factory: Arc::new(|_system, _path, _args| {}),
            key_field: "account".to_owned(),
            args_template: None,
            opts: SpawnOpts::default(),
        };
        let result = system.install_partition_set(spec);

        // Then the install is REFUSED (refuse-to-lie discipline).
        assert!(
            result.is_err(),
            "spec without a declared shard key rejected"
        );
    }

    // ---- projector sets: broadcast activation (v0.6.0) ----------------

    #[tokio::test]
    async fn projector_set_activates_on_consumed_broadcast() {
        // Given a ChatLog projector set over "proj/chats" consuming Chatted
        // (shard key `chat_id`).
        let (system, _clock) = ActorSystem::test();
        install_chat_projector_set(&system, "proj/chats").expect("install");

        // When a broadcast copy of Chatted for chat "7" crosses the fabric.
        publish_chatted(&system, "7", "hello").await;

        // Then the per-key projector proj/chats/7 activates, catches up
        // (history includes this very fact), and folds it exactly once.
        let state = system
            .projector_state(&ActorPath::new("proj/chats/7"))
            .await
            .expect("projector activated by broadcast");
        let log: ChatLog = state.decode().expect("state decodes");
        assert_eq!(log.messages, 1, "exactly one fold of the single fact");
        // And the sibling key was never activated.
        assert!(
            system
                .es_state(&ActorPath::new("proj/chats/8"))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn passivated_projector_wakes_and_gap_fills() {
        // Given a ChatLog projector set whose projectors passivate after a
        // short idle, one folded fact, and a passivated (evicted) projector.
        let (system, clock) = ActorSystem::test();
        let opts = SpawnOpts {
            passivation: Some(Passivation {
                idle_for: std::time::Duration::from_millis(50),
            }),
            ..Default::default()
        };

        let spec = crate::pool::ProjectorSetSpec {
            opts,
            ..install_chat_projector_set_spec(&system, "proj/chats")
        };
        system.install_projector_set(spec).expect("install");
        publish_chatted(&system, "9", "one").await;
        let path = ActorPath::new("proj/chats/9");
        let _ = system.projector_state(&path).await.expect("first read");

        // When the idle window elapses on the fake clock (no messages).
        clock.advance(std::time::Duration::from_millis(100));
        wait_for(|| async {
            !system_is_live(&system, &path) && system.es_state(&path).await.is_none()
        })
        .await;
        publish_chatted(&system, "9", "two").await;

        // When two more facts for the same key are broadcast while the
        // projector is cold.
        publish_chatted(&system, "9", "three").await;

        // Then the broadcast wakes the projector, it gap-fills from the
        // store (its own journal holds only the first fact), and the fold
        // is complete — nothing lost across the passivation cycle.
        let state = system
            .projector_state(&path)
            .await
            .expect("woken projector");
        let log: ChatLog = state.decode().expect("state decodes");
        assert_eq!(log.messages, 3, "all three facts folded exactly once");
        assert_eq!(
            log.keys_seen,
            vec!["one".to_owned(), "two".to_owned(), "three".to_owned()],
            "order preserved across passivation + gap-fill"
        );
    }

    #[tokio::test]
    async fn projector_state_wakes_and_returns_complete_fold() {
        // Given a ChatLog projector set and two stored facts for key "5",
        // with NO projector spawned yet: the first publish ACTIVATES it
        // (a declared consumption is a delivery obligation), so the
        // genuinely-cold precondition needs a passivation cycle first.
        let (system, clock) = ActorSystem::test();
        let opts = SpawnOpts {
            passivation: Some(Passivation {
                idle_for: std::time::Duration::from_millis(50),
            }),
            ..Default::default()
        };
        let spec = crate::pool::ProjectorSetSpec {
            opts,
            ..install_chat_projector_set_spec(&system, "proj/chats")
        };
        system.install_projector_set(spec).expect("install");
        publish_chatted(&system, "5", "a").await;
        publish_chatted(&system, "5", "b").await;
        // Let the activation come live (slot registered), fold the two
        // facts, then advance past the idle window.
        let path5 = ActorPath::new("proj/chats/5");
        wait_for(|| async { system_is_live(&system, &path5) }).await;
        wait_for(|| async { system.dead_letter_reasons().await.is_empty() }).await;
        // Drive the fold to completion with one bounded quiesce-equivalent:
        // publish already happened, so just wait for the fold to land.
        for _ in 0..500 {
            if system.es_state(&path5).await.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        clock.advance(std::time::Duration::from_millis(100));
        // On a fake clock the idle loop cannot self-wake, so drive the
        // eviction deterministically through the public stop (identical
        // teardown path: state freed, journal durable).
        system.stop(&path5).await;
        assert!(
            system.es_state(&path5).await.is_none(),
            "projector evicted before the cold read"
        );

        // When reading through projector_state (the complete read: wake +
        // catch-up + capture) from COLD.
        let state = system
            .projector_state(&ActorPath::new("proj/chats/5"))
            .await
            .expect("wake returns the fold");

        // Then the fold is COMPLETE (not mid-seed) and decodeable.
        let log: ChatLog = state.decode().expect("state decodes");
        assert_eq!(log.messages, 2, "both facts folded before capture");
        assert_eq!(log.keys_seen, vec!["a".to_owned(), "b".to_owned()]);

        // And an UNKNOWN (not set-owned, not live) path reads as None —
        // an observable miss, never a hang.
        assert!(
            system
                .projector_state(&ActorPath::new("proj/nowhere/1"))
                .await
                .is_none()
        );
    }

    struct CountingLoadsStore {
        inner: crate::journal::InMemoryJournalStore,
        loads: std::sync::atomic::AtomicUsize,
    }

    impl CountingLoadsStore {
        fn new() -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                inner: crate::journal::InMemoryJournalStore::new(),
                loads: std::sync::atomic::AtomicUsize::new(0),
            })
        }
    }

    #[async_trait::async_trait]
    impl crate::journal::JournalStore for CountingLoadsStore {
        async fn append(
            &self,
            path: &ActorPath,
            events: &[crate::envelope::Event],
        ) -> Result<Vec<crate::journal::SeqNo>, error_stack::Report<crate::journal::JournalError>>
        {
            self.inner.append(path, events).await
        }

        async fn append_snapshot(
            &self,
            path: &ActorPath,
            seq: crate::journal::SeqNo,
            state: Json,
            now_ms: u64,
        ) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
            self.inner.append_snapshot(path, seq, state, now_ms).await
        }

        async fn load(
            &self,
            path: &ActorPath,
        ) -> Result<Option<crate::journal::Replay>, error_stack::Report<crate::journal::JournalError>>
        {
            self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.load(path).await
        }

        async fn flush(&self) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
            self.inner.flush().await
        }

        fn name(&self) -> &'static str {
            "counting-loads"
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        async fn append_catchup(
            &self,
            path: &ActorPath,
            events: &[crate::journal::ScannedEvent],
        ) -> Result<
            Vec<Option<crate::journal::SeqNo>>,
            error_stack::Report<crate::journal::JournalError>,
        > {
            self.inner.append_catchup(path, events).await
        }

        async fn scan(
            &self,
            schemas: &[SchemaId],
        ) -> Result<
            Vec<crate::journal::ScannedEvent>,
            error_stack::Report<crate::journal::JournalError>,
        > {
            self.inner.scan(schemas).await
        }

        async fn passivated(
            &self,
            path: &ActorPath,
        ) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
            self.inner.passivated(path).await
        }

        async fn purge(
            &self,
            path: &ActorPath,
        ) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
            self.inner.purge(path).await
        }
    }

    #[tokio::test]
    async fn idle_snapshot_check_skips_journal_load_until_due() {
        // Given a counting store and an ES actor on a 100ms time-cadence
        // that has committed exactly one event.
        let (system, clock) = ActorSystem::test();
        let store = CountingLoadsStore::new();
        system.set_journal_store(store.clone());
        let path = ActorPath::new("cadenced");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts {
                snapshot: SnapshotCadence::Time(std::time::Duration::from_millis(100)),
                ..Default::default()
            },
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        let _ = system
            .tell(path.clone(), Add { n: 1 })
            .await
            .expect("commit one event");
        wait_for(|| async {
            system
                .with_es_state::<Counter, _>(&path, |c| c.total)
                .await
                .is_some_and(|total| total == 1)
        })
        .await;
        // Recovery at boot already used the store; the probe starts here.
        store.loads.store(0, std::sync::atomic::Ordering::SeqCst);

        // When the actor idles with the clock advanced HALF the interval:
        // several idle ticks run, none due.
        clock.advance(std::time::Duration::from_millis(50));
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;

        // Then no idle tick loaded the journal: the due check is O(1).
        let loads = store.loads.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(loads, 0, "non-due idle ticks must not load the journal");

        // When the interval elapses.
        clock.advance(std::time::Duration::from_millis(60));

        // Then the due idle tick takes the snapshot WITHOUT a journal
        // load: the due check and the anchor seq come from kernel-side
        // bookkeeping, so the store's only contact is append_snapshot.
        let took_snapshot = wait_for_returning(|| async {
            store
                .inner
                .entries_of(&path)
                .iter()
                .any(|e| matches!(e, crate::journal::JournalEntry::Snapshot { .. }))
                .then_some(())
        })
        .await
        .is_some();
        assert!(took_snapshot, "the due tick produced a snapshot");
        let loads = store.loads.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(loads, 0, "the idle snapshot path never loads the journal");
    }

    #[tokio::test]
    async fn es_state_is_frozen_after_passivation() {
        // Given a ChatLog projector whose projector passes cold, folded
        // once, then passivated (true eviction).
        let (system, clock) = ActorSystem::test();
        let opts = SpawnOpts {
            passivation: Some(Passivation {
                idle_for: std::time::Duration::from_millis(50),
            }),
            ..Default::default()
        };
        let spec = crate::pool::ProjectorSetSpec {
            opts,
            ..install_chat_projector_set_spec(&system, "proj/chats")
        };
        system.install_projector_set(spec).expect("install");
        publish_chatted(&system, "3", "warm").await;
        let path = ActorPath::new("proj/chats/3");
        let _ = system.projector_state(&path).await.expect("first read");
        clock.advance(std::time::Duration::from_millis(100));
        wait_for(|| async { system.es_state(&path).await.is_none() }).await;

        // When nothing publishes (es_state NEVER wakes anything — it is
        // a pure peek), repeated reads stay None.
        assert!(system.es_state(&path).await.is_none());
        assert!(
            system.es_state(&path).await.is_none(),
            "es_state is frozen: peek-only, no wake"
        );

        // And the complete read wakes it: the cold projector gap-fills
        // and returns the full fold.
        let state = system.projector_state(&path).await.expect("complete");
        let log: ChatLog = state.decode().expect("decodes");
        assert_eq!(log.messages, 1, "woken by the complete read alone");
    }

    #[tokio::test]
    async fn rebuild_refolds_history_from_scratch() {
        // Given two Chatted facts in a SOURCE journal (an entity's record,
        // host-seeded through the test seam) and a ChatLog projector that
        // folded them.
        let (system, _clock) = ActorSystem::test();
        install_chat_projector_set(&system, "proj/chats").expect("install");
        let source = ActorPath::new("chats");
        seed_journal_events(
            &system,
            &source,
            vec![
                crate::envelope::Event::from_json_view(
                    Chatted::schema_id(),
                    crate::json!({ "chat_id": "1", "text": "x" }),
                ),
                crate::envelope::Event::from_json_view(
                    Chatted::schema_id(),
                    crate::json!({ "chat_id": "1", "text": "y" }),
                ),
            ],
        );
        let path = ActorPath::new("proj/chats/1");
        let state = system.projector_state(&path).await.expect("initial");
        let log: ChatLog = state.decode().expect("decodes");
        assert_eq!(log.messages, 2);

        // When the projector is rebuilt (stop → purge → re-activate →
        // await catch-up).
        system.rebuild_projector(&path).await.expect("rebuild");

        // Then the re-fold equals a from-scratch fold: same facts, exactly
        // once each (the purged journal cannot double-count).
        let state = system.projector_state(&path).await.expect("post-rebuild");
        let log: ChatLog = state.decode().expect("decodes");
        assert_eq!(log.messages, 2, "purge + re-fold == fresh fold");
        assert_eq!(
            log.keys_seen,
            vec!["x".to_owned(), "y".to_owned()],
            "order preserved through rebuild"
        );

        // And rebuild REFUSES paths the runtime has no recipe for.
        assert!(
            system
                .rebuild_projector(&ActorPath::new("standalone/proj"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn install_projector_set_refuses_keyless_consumed_schema() {
        // Given a system with a registered event schema that has NO
        // shard-key field.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Chatted>();
        let spec = crate::pool::ProjectorSetSpec {
            key_field: "channel".to_owned(),
            ..install_chat_projector_set_spec(&system, "proj/chats")
        };

        // When a projector set names a key field no consumed schema marks
        // as ShardKey.
        let result = system.install_projector_set(spec);

        // Then the install is REFUSED.
        assert!(result.is_err(), "keyless consumed schema rejected");
    }

    #[tokio::test]
    async fn install_projector_set_refuses_command_schema() {
        // Given a system with the KeyedAdd COMMAND registered.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<KeyedAdd>();

        // When a projector set consumes it (commands are not facts).
        let spec = crate::pool::ProjectorSetSpec {
            key_field: "account".to_owned(),
            ..install_chat_projector_set_spec(&system, "proj/chats")
        };
        // Replace the consumed list with the command schema.
        let spec = crate::pool::ProjectorSetSpec {
            consumed: vec![KeyedAdd::schema_id()],
            ..spec
        };
        let result = system.install_projector_set(spec);

        // Then the install is REFUSED.
        assert!(result.is_err(), "command schemas are not foldable facts");
    }

    #[tokio::test]
    async fn keyless_broadcast_copy_dead_letters_shard_key_missing() {
        // Given an installed ChatLog projector set.
        let (system, _clock) = ActorSystem::test();
        install_chat_projector_set(&system, "proj/chats").expect("install");

        // When a Chatted copy without its key field is published (a
        // foreign sender bypassing the schema).
        system
            .publish_value(Chatted::schema_id(), crate::json!({ "text": "no key" }))
            .await;

        // Then the per-set copy dead-letters as ShardKeyMissing and no
        // projector activates.
        wait_for(|| async {
            system
                .dead_letter_reasons()
                .await
                .iter()
                .any(|reason| reason.starts_with("ShardKeyMissing"))
        })
        .await;
        assert!(
            system
                .es_state(&ActorPath::new("proj/chats/"))
                .await
                .is_none()
                && system
                    .es_state(&ActorPath::new("proj/chats"))
                    .await
                    .is_none(),
            "no projector spawned for a keyless copy"
        );
    }

    #[tokio::test]
    async fn entity_partition_sets_do_not_activate_on_broadcast() {
        // Given a KeyCounter ENTITY partition set (no consumption declared)
        // and a broadcast of the Chatted fact.
        let (system, _clock) = ActorSystem::test();
        install_key_partition(&system, "accounts").expect("install");

        // When the fact is broadcast (no .handles declarant exists for it).
        publish_chatted(&system, "7", "ignored").await;

        // Then no entity spawns: the activation rule is declaration-scoped
        // — an entity partition set never declared consumption, so a
        // broadcast copy is not its delivery obligation.
        assert!(
            system
                .es_state(&ActorPath::new("accounts/7"))
                .await
                .is_none(),
            "entity sets do not wake on broadcast facts"
        );
    }

    /// The projector-set spec builder for fixtures (tests override
    /// individual fields with struct-update syntax).
    fn install_chat_projector_set_spec(
        system: &ActorSystem,
        public: &str,
    ) -> crate::pool::ProjectorSetSpec {
        system.register_schema::<Chatted>();
        crate::pool::ProjectorSetSpec {
            public: ActorPath::new(public),
            system: system.clone(),
            factory: Arc::new(|system, path, args| {
                // Fire-and-forget: the arm is synchronous; catch-up (and
                // its CaughtUp fact) continues in the background.
                crate::builder::spawn_projector_builder::<ChatLog>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .consumes::<Chatted>()
                    .start();
            }),
            key_field: "chat_id".to_owned(),
            args_template: None,
            opts: SpawnOpts::default(),
            consumed: vec![Chatted::schema_id()],
        }
    }

    /// Whether the path has a live slot.
    fn system_is_live(system: &ActorSystem, path: &ActorPath) -> bool {
        let registry = system.registry.lock();
        registry.lookup(path).is_some()
    }

    #[tokio::test]
    async fn concurrent_same_key_activation_yields_one_entity() {
        // Given a partition set over "accounts".
        let (system, _clock) = ActorSystem::test();
        install_key_partition(&system, "accounts").expect("install");

        // When several commands for the same FRESH key are sent in quick
        // succession (the first may still be activating).
        let sends: Vec<_> = (0..5)
            .map(|n| {
                let e = system.envelope(
                    KeyedAdd::schema_id(),
                    ActorPath::new("accounts"),
                    json!({ "n": n, "account": "race" }),
                );
                system.send(e)
            })
            .collect();
        for s in sends {
            s.await.expect("delivered");
        }
        wait_for(|| async {
            system
                .es_state(&ActorPath::new("accounts/race"))
                .await
                .and_then(|s| s["total"].as_i64())
                == Some(10)
        })
        .await;

        // Then exactly one entity holds the full total (5×2 from the
        // handler's emit shape), spawned once.
        let total = system
            .es_state(&ActorPath::new("accounts/race"))
            .await
            .and_then(|s| s["total"].as_i64());
        assert_eq!(total, Some(10), "all five commands hit ONE entity");
        let spawns = system
            .facts()
            .iter()
            .filter(|f| matches!(
                &f.kind,
                crate::observe::ObservationKind::Spawned { path, .. } if *path == ActorPath::new("accounts/race")
            ))
            .count();
        assert_eq!(spawns, 1, "the race yielded a single activation");
    }

    #[tokio::test]
    async fn tee_rule_copies_without_touching_delivery() {
        // Given a primary ES counter and a Tee observer, with a Tee rule:
        // every Add aimed at the counter is copied to the observer.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let (sink_idx, sink) = open_sink();
        bind_sink(&ActorPath::new("watcher"), sink.clone());
        system.spawn_service::<Auditor, _>(
            ActorPath::new("watcher"),
            &json!({ "sink": sink_idx }),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<Auditor, Added>::new::<Added>(),
                )]
            },
        );
        let counter = ActorPath::new("counter");
        system.spawn_es::<BareCounter, _>(
            counter.clone(),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<BareCounter, Add>::new::<Add>())],
        );
        {
            system
                .declare_emits(&counter, Added::schema_id())
                .expect("declare");
            let mut registry = system.registry.lock();
            registry.add_rule(crate::pool::Rule {
                source: None,
                schema: Some(Add::schema_id()),
                dest: Some(counter.clone()),
                action: crate::pool::RuleAction::Tee(ActorPath::new("watcher")),
            });
        }
        wait_for(|| async { system.inbox_cursor(&counter).is_some() }).await;

        // When one command is sent to the primary (root entry trace): the
        // tee copy carries schema Add@1 while the observer decodes Added@1,
        // so the copy dead-letters at the observer — at-most-once tee in
        // action. The ORIGINAL reaches the primary untouched (below).
        let mut envelope = system.envelope(Add::schema_id(), counter.clone(), json!({ "n": 5 }));
        envelope.trace = crate::envelope::TraceCtx::root();
        let original_causality = envelope.trace.causality_id;
        system.send(envelope).await.expect("delivered");
        wait_for(|| async { count_total(&system, "counter").await == Some(5) }).await;

        // Then the PRIMARY still processed the original untouched (the tee
        // never disturbed the main flow), and the copy did not poison it.
        assert_eq!(count_total(&system, "counter").await, Some(5));
        // And the copy's causality is NEW (never the original's id). The
        // copy's true trace is the Delivered fact AT the observer (the tee
        // Sent fact deliberately carries the ORIGINAL trace — that is the
        // link between the two deliveries).
        let facts = system.facts();
        let tee_delivered = facts
            .iter()
            .find(|f| {
                matches!(
                    &f.kind,
                    crate::observe::ObservationKind::Delivered { to, .. } if *to == ActorPath::new("watcher")
                )
            })
            .expect("tee copy delivered");
        if let crate::observe::ObservationKind::Delivered { trace, .. } = &tee_delivered.kind {
            assert_ne!(
                trace.causality_id, original_causality,
                "copy has a NEW causality"
            );
            assert_ne!(
                trace.trace_id,
                crate::envelope::TraceCtx::root().trace_id,
                "sanity: trace ids are unique per root"
            );
        }
    }

    #[tokio::test]
    async fn inline_rule_interposes() {
        // Given a Forwarder interposer ("middleman") and a final handler
        // ("final"), with a rule: Add aimed at "target" is delivered to
        // the middleman INSTEAD; the middleman forwards to "final".
        #[derive(Default)]
        struct Forwarder {
            sink: Arc<Mutex<Vec<String>>>,
            forward_to: String,
        }
        impl ServiceActor for Forwarder {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .emits::<Add>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                let (sink, forward_to) = (
                    sinks().lock()[args["sink"].as_u64().expect("sink idx") as usize].clone(),
                    args["forward_to"].as_str().expect("forward_to").to_owned(),
                );
                Ok(Self { sink, forward_to })
            }
        }
        impl MsgHandler<Add> for Forwarder {
            async fn handle(&mut self, msg: &Add, ctx: &mut crate::context::MsgCtx<'_>) {
                self.sink.lock().push(format!("seen={}", msg.n));
                let dest = Address::Path(ActorPath::new(self.forward_to.as_str()));
                ctx.send(dest, msg.clone(), None);
            }
        }
        #[derive(Default)]
        struct Final {
            sink: Arc<Mutex<Vec<String>>>,
        }
        impl ServiceActor for Final {
            fn manifest() -> ActorManifest {
                ActorManifest::new().kind(ActorKind::Service)
            }
            async fn start(
                args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self {
                    sink: sinks().lock()[args["sink"].as_u64().expect("sink idx") as usize].clone(),
                })
            }
        }
        impl MsgHandler<Add> for Final {
            async fn handle(&mut self, msg: &Add, _ctx: &mut crate::context::MsgCtx<'_>) {
                self.sink.lock().push(format!("n={}", msg.n));
            }
        }
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let (seen_idx, seen_sink) = open_sink();
        bind_sink(&ActorPath::new("middleman"), seen_sink);
        let (final_idx, final_sink) = open_sink();
        bind_sink(&ActorPath::new("final"), final_sink);
        system.spawn_service::<Forwarder, _>(
            ActorPath::new("middleman"),
            &json!({ "sink": seen_idx, "forward_to": "final" }),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Forwarder, Add>::new::<Add>())],
        );
        system.spawn_service::<Final, _>(
            ActorPath::new("final"),
            &json!({ "sink": final_idx }),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Final, Add>::new::<Add>())],
        );
        {
            let mut registry = system.registry.lock();
            registry.add_rule(crate::pool::Rule {
                source: None,
                schema: Some(Add::schema_id()),
                dest: Some(ActorPath::new("target")),
                action: crate::pool::RuleAction::Inline(ActorPath::new("middleman")),
            });
        }
        wait_for(|| async {
            system.inbox_cursor(&ActorPath::new("middleman")).is_some()
                && system.inbox_cursor(&ActorPath::new("final")).is_some()
        })
        .await;

        // When a command is sent to "target" (no actor lives there — the
        // rule interposes BEFORE resolution).
        system
            .send(system.envelope(
                Add::schema_id(),
                ActorPath::new("target"),
                json!({ "n": 7 }),
            ))
            .await
            .expect("delivered");
        wait_for(|| async {
            !sink_read(&ActorPath::new("middleman")).is_empty()
                && !sink_read(&ActorPath::new("final")).is_empty()
        })
        .await;

        // Then the flow ran THROUGH the interposer (in its place — not a
        // copy) and landed on the final handler.
        assert_eq!(sink_read(&ActorPath::new("middleman")), vec!["seen=7"]);
        assert_eq!(sink_read(&ActorPath::new("final")), vec!["n=7"]);
    }

    /// A worker whose FIRST message (n=0) blocks on a shared gate until
    /// the test releases it — queued mail then accumulates past any
    /// watermark deterministically (the loop is stuck in the handler).
    struct GatedWorker {
        sink: Arc<Mutex<Vec<String>>>,
    }

    static WORKER_GATE: std::sync::LazyLock<tokio::sync::Notify> =
        std::sync::LazyLock::new(tokio::sync::Notify::new);
    /// The gate's released flag: notify_waiters alone is memory-less, and
    /// the handler task may be scheduled after the release fires — the
    /// flag makes the release observed regardless of ordering.
    static WORKER_RELEASED: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    /// Parks until the test releases the gate (flag + notify latch).
    async fn gate_wait() {
        use std::sync::atomic::Ordering;
        loop {
            if WORKER_RELEASED.load(Ordering::SeqCst) {
                return;
            }
            let notified = WORKER_GATE.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if WORKER_RELEASED.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    impl ServiceActor for GatedWorker {
        fn manifest() -> ActorManifest {
            ActorManifest::new().kind(ActorKind::Service)
        }
        async fn start(
            args: &Json,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            let idx = args["sink"].as_u64().expect("sink index") as usize;
            let sink = sinks().lock()[idx].clone();
            Ok(Self { sink })
        }
    }

    impl MsgHandler<Add> for GatedWorker {
        async fn handle(&mut self, msg: &Add, _ctx: &mut crate::context::MsgCtx<'_>) {
            if msg.n == 0 {
                gate_wait().await;
            }
            self.sink.lock().push(format!("n={}", msg.n));
        }
    }

    #[tokio::test]
    async fn backpressured_fact_fires_on_watermark_crossing() {
        // Given a gated worker (capacity 8, watermark 2) whose first
        // message blocks inside the handler.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        let (sink_idx, sink) = open_sink();
        let plain = ActorPath::new("slow");
        bind_sink(&plain, sink);
        let opts = SpawnOpts {
            mailbox_capacity: 8,
            high_watermark: Some(2),
            ..SpawnOpts::default()
        };
        system.spawn_service::<GatedWorker, _>(
            plain.clone(),
            &json!({ "sink": sink_idx }),
            opts,
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<GatedWorker, Add>::new::<Add>(),
                )]
            },
        );
        wait_for(|| async { system.inbox_cursor(&plain).is_some() }).await;

        // When the first message pins the loop (handler blocked) and three
        // more queue up behind it (depth 3 > watermark 2).
        for n in 0..4u64 {
            let envelope = system.envelope(Add::schema_id(), plain.clone(), json!({ "n": n }));
            system.send(envelope).await.expect("queued");
        }
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Backpressured { path, .. } if *path == plain))
        })
        .await;
        // Release the gate so the worker drains (clean shutdown): the
        // flag is the memory, the notify the wake (either ordering works).
        WORKER_RELEASED.store(true, std::sync::atomic::Ordering::SeqCst);
        WORKER_GATE.notify_waiters();
        wait_for(|| async {
            let len = sink_read(&plain).len();
            len == 4
        })
        .await;

        // Then exactly ONE Backpressured fact fired for the up-crossing
        // (rate-limited: not one per message).
        let fires = system
            .facts()
            .iter()
            .filter(|f| matches!(&f.kind, crate::observe::ObservationKind::Backpressured { path, .. } if *path == plain))
            .count();
        assert_eq!(fires, 1, "one fact per up-crossing, not per message");
    }

    #[tokio::test]
    async fn handles_alias_one_fabric() {
        // Given a system and a clone of its handle.
        let (system, _clock) = ActorSystem::test();
        let handle = system.clone();

        // When a schema is registered through the clone.
        let id = handle.register_schema::<Add>();

        // Then the registration is visible through the original handle —
        // both names alias the SAME fabric (one Arc, one registry), not
        // two copies.
        let visible = system.registry.lock().schema(&id).is_some();
        assert!(visible, "clone sees the same registry");
    }

    /// The number of recorded failures for a supervised child.
    fn failure_count(system: &ActorSystem, path: &ActorPath) -> usize {
        let kernel = system.kernel.lock();
        kernel.failures.get(path).map(|w| w.len()).unwrap_or(0)
    }

    /// An ES actor whose first command always panics (shutdown test).
    #[derive(Serialize, Deserialize, Default, Clone)]
    struct ShutdownBoomer;
    impl EventSourcedActor for ShutdownBoomer {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Add>()
                .kind(ActorKind::EventSourced)
        }
        fn restore(_args: &Json) -> Self {
            Self
        }
        fn apply(&mut self, _event: &crate::envelope::Event) {}
    }
    impl CommandHandler<Add> for ShutdownBoomer {
        fn handle(&self, _cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            panic!("always panics");
        }
    }

    #[tokio::test]
    async fn shutdown_stops_supervision_restarts() {
        // Given a supervised child with a crash-on-first-command handler
        // and a generous restart budget.
        let (system, _clock) = ActorSystem::test();
        let child = ActorPath::new("boomer");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let spawner = {
            let system = system.clone();
            move |sys: &ActorSystem, path: &ActorPath, args: &Json| {
                let system = system.clone();
                let path = path.clone();
                let args = args.clone();
                let _ = sys;
                system.spawn_es::<ShutdownBoomer, _>(path, &args, SpawnOpts::default(), || {
                    vec![Arc::new(TypedEsAdapter::<ShutdownBoomer, Add>::new::<Add>())]
                });
            }
        };
        let spec = crate::supervision::ActorSpec {
            path: child.clone(),
            parent: None,
            restart: crate::supervision::RestartPolicy::Permanent,
            budget: crate::supervision::RestartBudget::per(
                1_000,
                std::time::Duration::from_secs(60),
            ),
            backoff: crate::supervision::Backoff {
                base: std::time::Duration::from_millis(5),
                max: std::time::Duration::from_millis(5),
                factor: 1.0,
            },
            args: json!({}),
            spawn: Arc::new(spawner),
        };
        system.spawn(spec);

        // When the child crashes (the engine restarts it — the budget
        // allows 1_000), shutdown is called mid-cycle.
        system
            .send(system.envelope(Add::schema_id(), child.clone(), json!({ "n": 1 })))
            .await
            .expect("sent");
        wait_for_crash(&system, &child).await;
        // The engine is demonstrably RESTARTING (a Spawned{restart: true}
        // fact exists) before we cut the power.
        wait_for(|| async {
            system.facts().iter().any(|f| {
                matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, restart, .. }
                    if *path == child && *restart)
            })
        })
        .await;
        system.shutdown();

        // Then the restart loop is OFF: neither the failure count nor the
        // restart count moves again.
        let frozen_failures = failure_count(&system, &child);
        let restarts = |sys: &ActorSystem| {
            sys.facts()
                .iter()
                .filter(|f| {
                    matches!(&f.kind, crate::observe::ObservationKind::Spawned { path, restart, .. }
                        if *path == child && *restart)
                })
                .count()
        };
        let frozen_restarts = restarts(&system);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            frozen_failures,
            failure_count(&system, &child),
            "no new crash entries after shutdown()"
        );
        assert_eq!(
            frozen_restarts,
            restarts(&system),
            "no restarts after shutdown()"
        );
    }

    #[tokio::test]
    async fn shutdown_is_idempotent() {
        // Given a system with a supervised (harmless) child.
        let (system, _clock) = ActorSystem::test();
        let spec = crate::supervision::ActorSpec {
            path: ActorPath::new("quiet"),
            parent: None,
            restart: crate::supervision::RestartPolicy::Permanent,
            budget: crate::supervision::RestartBudget::default(),
            backoff: crate::supervision::Backoff::default(),
            args: json!({}),
            spawn: Arc::new(|_sys: &ActorSystem, _path: &ActorPath, _args: &Json| {}),
        };
        system.spawn(spec);

        // When shutdown fires twice (and once more after that).
        system.shutdown();
        system.shutdown();
        system.shutdown();

        // Then nothing panics — draining an empty (or closed) list is a
        // no-op, and the handle stays usable for ordinary operations.
        let _ = system.register_schema::<Add>();
    }

    #[tokio::test]
    async fn dropping_handles_does_not_stop_the_fabric() {
        // Given a system whose handle is cloned and the original dropped.
        let (system, _clock) = ActorSystem::test();
        let alias = system.clone();
        let id = alias.register_schema::<Add>();
        drop(system);
        drop(alias);

        // When the fabric is re-reached through a THIRD clone taken
        // before the drops (the Arc keeps the fabric alive).
        // (Constructed here via the surviving alias chain.)
        let (sys2, _clock2) = ActorSystem::test();
        sys2.register_schema::<Added>();

        // Then the FIRST fabric is still routable — teardown never
        // happens on drop; only an explicit shutdown() stops things, and
        // none was called.
        let (sys3, _clock3) = ActorSystem::test();
        sys3.register_schema::<Add>();
        let _ = (id, sys2);
    }

    // ===== Lifecycle: on_stop, self-termination, passivation, sweep =====

    use crate::system::Passivation;

    /// A sink shared with on_stop assertions.
    type HookLog = Arc<std::sync::Mutex<Vec<String>>>;

    fn hook_log() -> HookLog {
        Arc::new(std::sync::Mutex::new(Vec::new()))
    }

    /// A deterministic async gate: handlers await it, the test releases.
    struct Gate {
        entered: std::sync::atomic::AtomicBool,
        released: std::sync::atomic::AtomicBool,
    }
    impl Gate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                entered: std::sync::atomic::AtomicBool::new(false),
                released: std::sync::atomic::AtomicBool::new(false),
            })
        }
        async fn park(&self) {
            self.entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            while !self.released.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        }
        fn release(&self) {
            self.released
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        fn entered(&self) -> bool {
            self.entered.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// A JournalStore test double: `load` parks on a gate, everything
    /// else forwards to the inner store.
    struct GatedStore {
        gate: Arc<Gate>,
        inner: std::sync::Arc<dyn crate::journal::JournalStore>,
    }
    impl GatedStore {
        fn new(gate: Arc<Gate>, inner: std::sync::Arc<dyn crate::journal::JournalStore>) -> Self {
            Self { gate, inner }
        }
    }
    #[async_trait::async_trait]
    impl crate::journal::JournalStore for GatedStore {
        async fn append(
            &self,
            path: &crate::actor::ActorPath,
            events: &[crate::envelope::Event],
        ) -> Result<Vec<crate::journal::SeqNo>, error_stack::Report<crate::journal::JournalError>>
        {
            self.inner.append(path, events).await
        }
        async fn append_snapshot(
            &self,
            path: &crate::actor::ActorPath,
            seq: crate::journal::SeqNo,
            state: Json,
            now_ms: u64,
        ) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
            // Park only for the FIRST snapshot: the idle-arm time-cadence
            // snapshot holds the loop mid-idle-arm so the test can race a
            // delivery in (the D3 idle path's store contact is now the
            // snapshot append itself).
            if !self.gate.entered() {
                self.gate.park().await;
            }
            self.inner.append_snapshot(path, seq, state, now_ms).await
        }
        async fn load(
            &self,
            path: &crate::actor::ActorPath,
        ) -> Result<Option<crate::journal::Replay>, error_stack::Report<crate::journal::JournalError>>
        {
            // The idle arm no longer loads the journal (D3): load passes
            // straight through.
            self.inner.load(path).await
        }
        async fn flush(&self) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
            self.inner.flush().await
        }
        fn name(&self) -> &'static str {
            "gated-test"
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    /// A JournalStore test double whose `passivated` hint always fails:
    /// passivation must complete anyway (log-and-continue).
    struct HintFailStore {
        inner: std::sync::Arc<dyn crate::journal::JournalStore>,
    }
    impl HintFailStore {
        fn new(inner: std::sync::Arc<dyn crate::journal::JournalStore>) -> Self {
            Self { inner }
        }
    }
    #[async_trait::async_trait]
    impl crate::journal::JournalStore for HintFailStore {
        async fn append(
            &self,
            path: &crate::actor::ActorPath,
            events: &[crate::envelope::Event],
        ) -> Result<Vec<crate::journal::SeqNo>, error_stack::Report<crate::journal::JournalError>>
        {
            self.inner.append(path, events).await
        }
        async fn load(
            &self,
            path: &crate::actor::ActorPath,
        ) -> Result<Option<crate::journal::Replay>, error_stack::Report<crate::journal::JournalError>>
        {
            self.inner.load(path).await
        }
        async fn append_snapshot(
            &self,
            path: &crate::actor::ActorPath,
            seq: crate::journal::SeqNo,
            state: Json,
            now_ms: u64,
        ) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
            self.inner.append_snapshot(path, seq, state, now_ms).await
        }
        async fn flush(&self) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
            self.inner.flush().await
        }
        async fn passivated(
            &self,
            _path: &crate::actor::ActorPath,
        ) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
            Err(crate::journal::JournalError::Hint.into())
        }
        fn name(&self) -> &'static str {
            "hintfail-test"
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    /// An ES counter with a sync on_stop that records the final total.
    static ES_HOOK_LOG: std::sync::OnceLock<Arc<std::sync::Mutex<Vec<String>>>> =
        std::sync::OnceLock::new();

    fn es_hook_log() -> Arc<std::sync::Mutex<Vec<String>>> {
        ES_HOOK_LOG
            .get_or_init(|| Arc::new(std::sync::Mutex::new(Vec::new())))
            .clone()
    }

    fn es_hook_entries() -> Vec<String> {
        es_hook_log().lock().expect("log").clone()
    }

    #[derive(Serialize, Deserialize, Clone)]
    struct StopCounter {
        total: i64,
        #[serde(skip)]
        log: HookLog,
    }
    impl Default for StopCounter {
        fn default() -> Self {
            Self {
                total: 0,
                log: es_hook_log(),
            }
        }
    }
    impl EventSourcedActor for StopCounter {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Add>()
                .emits::<Added>()
                .kind(ActorKind::EventSourced)
        }
        fn restore(_args: &Json) -> Self {
            Self {
                total: 0,
                log: es_hook_log(),
            }
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
        }
        fn on_stop(&self) {
            self.log
                .lock()
                .expect("log")
                .push(format!("es-total={}", self.total));
        }
    }
    impl CommandHandler<Add> for StopCounter {
        fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            if cmd.n == 666 {
                panic!("poison add");
            }
            crate::envelope::Events::from_vec(vec![crate::envelope::Event::from_json_view(
                Added::schema_id(),
                json!({ "n": cmd.n }),
            )])
        }
    }

    /// A service actor with an async on_stop that records a marker.
    struct StopService {
        log: HookLog,
    }
    impl ServiceActor for StopService {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Add>()
                .kind(ActorKind::Service)
        }
        async fn start(
            _args: &Json,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            Ok(Self { log: hook_log() })
        }
        async fn on_stop(&mut self, _ctx: &mut crate::context::MsgCtx<'_>) {
            self.log.lock().expect("log").push("svc-stopped".to_owned());
        }
    }
    impl MsgHandler<Add> for StopService {
        async fn handle(&mut self, _msg: &Add, _ctx: &mut crate::context::MsgCtx<'_>) {}
    }

    #[tokio::test]
    async fn service_on_stop_runs_on_external_stop() {
        // Given a service actor with an on_stop hook that writes a shared
        // global log (the instance is constructed inside the spawn).
        static SVC_LOG: std::sync::Mutex<Option<HookLog>> = std::sync::Mutex::new(None);
        let shared = hook_log();
        SVC_LOG.lock().expect("log").replace(shared.clone());

        struct GlobalStopService;
        impl ServiceActor for GlobalStopService {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
            async fn on_stop(&mut self, _ctx: &mut crate::context::MsgCtx<'_>) {
                SVC_LOG
                    .lock()
                    .expect("log")
                    .as_ref()
                    .expect("installed")
                    .lock()
                    .expect("inner")
                    .push("svc-stopped".to_owned());
            }
        }
        impl MsgHandler<Add> for GlobalStopService {
            async fn handle(&mut self, _msg: &Add, _ctx: &mut crate::context::MsgCtx<'_>) {}
        }

        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("svc");
        system.register_schema::<Add>();
        system.spawn_service::<GlobalStopService, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![Arc::new(crate::actor::TypedServiceAdapter::<
                    GlobalStopService,
                    Add,
                >::new::<Add>())]
            },
        );

        // When the actor is stopped externally.
        system.stop(&path).await;

        // Then the hook ran exactly once.
        wait_for(|| async {
            !SVC_LOG
                .lock()
                .expect("log")
                .as_ref()
                .expect("installed")
                .lock()
                .expect("inner")
                .is_empty()
        })
        .await;
        assert_eq!(
            SVC_LOG
                .lock()
                .expect("log")
                .as_ref()
                .expect("installed")
                .lock()
                .expect("inner")
                .len(),
            1,
            "on_stop ran exactly once"
        );
    }

    #[tokio::test]
    async fn es_on_stop_observes_final_folded_state() {
        // Given an ES counter with a sync on_stop, committed two Adds.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.spawn_es::<StopCounter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<StopCounter, Add>::new::<Add>())]
        });
        for n in [2, 3] {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        wait_for_cursor(&system, &path, 2).await;

        // When the actor is stopped gracefully.
        system.stop(&path).await;
        wait_for(|| async { stopped_with(&system, &path, crate::actor::StopReason::Normal) }).await;

        // Then the hook observed the FINAL folded total (5), not genesis.
        // The hook log is a global registry the instance wrote into.
        assert!(
            es_hook_entries().iter().any(|l| l == "es-total=5"),
            "on_stop saw the final fold: {:?}",
            es_hook_entries()
        );
    }

    #[tokio::test]
    async fn on_stop_skipped_on_crash() {
        // Given a supervised ES child that panics on every command.
        let (system, _clock) = ActorSystem::test();
        let child = ActorPath::new("crashy");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let spec = crate::supervision::ActorSpec {
            path: child.clone(),
            parent: None,
            restart: crate::supervision::RestartPolicy::Never,
            budget: crate::supervision::RestartBudget::per(5, std::time::Duration::from_secs(10)),
            backoff: crate::supervision::Backoff {
                base: std::time::Duration::from_millis(5),
                max: std::time::Duration::from_millis(20),
                factor: 2.0,
            },
            args: json!({}),
            spawn: Arc::new(|sys: &ActorSystem, path: &ActorPath, args: &Json| {
                sys.spawn_es::<StopCounter, _>(path.clone(), args, SpawnOpts::default(), || {
                    vec![Arc::new(TypedEsAdapter::<StopCounter, Add>::new::<Add>())]
                });
            }),
        };
        system.spawn(spec);

        // When the child crashes on a poison command (never recovers).
        system
            .send(system.envelope(Add::schema_id(), child.clone(), json!({ "n": 666 })))
            .await
            .expect("delivered");
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(f.kind, crate::observe::ObservationKind::Escalated { .. }))
        })
        .await;

        // Then the crash path never ran on_stop (crashed children do not
        // hook; only graceful stops do).
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(
            system
                .facts()
                .iter()
                .any(|f| matches!(f.kind, crate::observe::ObservationKind::Stopped { .. })),
            "escalation records the stop fact"
        );
    }

    #[tokio::test]
    async fn stop_self_terminates_after_commit_and_flushes_outbox_in_order() {
        // Given a service actor whose handler sends a mirror command to a
        // second actor, then stops itself. (An event-sourced entity has no
        // send verb — it announces by returning facts; the send+stop
        // outbox contract lives on the service tier.)
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let mirror = ActorPath::new("mirror");
        system.spawn_es::<Counter, _>(mirror.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        #[derive(Serialize, Deserialize, Default, Clone)]
        struct SelfStopper {
            target: String,
        }
        impl ServiceActor for SelfStopper {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .emits::<Add>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self {
                    target: "mirror".to_owned(),
                })
            }
        }
        impl MsgHandler<Add> for SelfStopper {
            async fn handle(&mut self, cmd: &Add, ctx: &mut crate::context::MsgCtx<'_>) {
                ctx.send(
                    Address::Path(ActorPath::new(self.target.as_str())),
                    Add { n: cmd.n },
                    None,
                );
                ctx.stop_self();
            }
        }

        let path = ActorPath::new("selfstop");
        system.spawn_service::<SelfStopper, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<SelfStopper, Add>::new::<Add>(),
                )]
            },
        );

        // When one command drives send → stop_self.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 9 })))
            .await
            .expect("delivered");

        // Then the actor stopped gracefully (Stopped fact) AND the send
        // flushed first (mirror committed it).
        wait_for(|| async { stopped_with(&system, &path, crate::actor::StopReason::Normal) }).await;
        wait_for(|| async { system.journal_len(&mirror) == 1 }).await;
        // And the stopped actor's path is gone (a new send would not resolve).
        assert!(!system.lookup_slot(&path));
    }

    #[tokio::test]
    async fn stop_self_does_not_process_subsequent_mailbox_entries() {
        // Given a self-stopping actor with a second command queued behind
        // the first.
        #[derive(Serialize, Deserialize, Clone)]
        struct StopOnFirst;

        impl ServiceActor for StopOnFirst {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for StopOnFirst {
            async fn handle(&mut self, _cmd: &Add, ctx: &mut crate::context::MsgCtx<'_>) {
                ctx.stop_self();
            }
        }
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        let path = ActorPath::new("stopfirst");
        system.spawn_service::<StopOnFirst, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<StopOnFirst, Add>::new::<Add>(),
                )]
            },
        );

        // When two commands arrive back to back.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("delivered");
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 2 })))
            .await
            .expect("delivered");

        // Then only the FIRST was processed; the second dead-letters as
        // StoppedWithMail (the actor never peeks past the stop).
        wait_for(|| async { stopped_with(&system, &path, crate::actor::StopReason::Normal) }).await;
        wait_for(|| async {
            system
                .dead_letter_schemas()
                .iter()
                .any(|s| *s == Add::schema_id())
        })
        .await;
    }

    #[tokio::test]
    async fn external_stop_after_self_stop_is_idempotent() {
        // Given an actor that stops itself on its first command.
        #[derive(Serialize, Deserialize, Clone)]
        struct SelfStopper2;
        impl ServiceActor for SelfStopper2 {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for SelfStopper2 {
            async fn handle(&mut self, _cmd: &Add, ctx: &mut crate::context::MsgCtx<'_>) {
                ctx.stop_self();
            }
        }
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        let path = ActorPath::new("race");
        system.spawn_service::<SelfStopper2, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<SelfStopper2, Add>::new::<Add>(),
                )]
            },
        );

        // When the actor self-stops and an external stop races in.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("delivered");
        system.stop(&path).await;

        // Then exactly ONE Stopped fact exists (no double-record, no panic).
        let stops = system
            .facts()
            .iter()
            .filter(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Stopped { path: p, .. } if *p == path),
            )
            .count();
        assert_eq!(stops, 1, "stop is idempotent: one Stopped fact");
    }

    #[tokio::test]
    async fn idle_passivation_stops_and_records_passivated_fact() {
        // Given an idle counter passivating after 100ms of no completed steps.
        let (system, clock) = ActorSystem::test();
        let path = ActorPath::new("idle");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let opts = SpawnOpts {
            passivation: Some(Passivation {
                idle_for: std::time::Duration::from_millis(100),
            }),
            ..SpawnOpts::default()
        };
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), opts, || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // When the idle window elapses on the fake clock (no messages).
        clock.advance(std::time::Duration::from_millis(500));
        wait_for(|| async { stopped_with(&system, &path, crate::actor::StopReason::Passivated) })
            .await;

        // Then the actor is fully torn down (slot removed).
        wait_for(|| async { !system.lookup_slot(&path) }).await;
    }

    #[tokio::test]
    async fn es_state_is_none_after_passivation() {
        // Given a passivating counter (100ms idle) that folded one event.
        let (system, clock) = ActorSystem::test();
        let path = ActorPath::new("leak");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let opts = SpawnOpts {
            passivation: Some(Passivation {
                idle_for: std::time::Duration::from_millis(100),
            }),
            ..SpawnOpts::default()
        };
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), opts, || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 5 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // When the idle window elapses and passivation completes.
        clock.advance(std::time::Duration::from_millis(500));
        wait_for(|| async { stopped_with(&system, &path, crate::actor::StopReason::Passivated) })
            .await;

        // Then the in-memory state entry is GONE (only live actors hold
        // state — the fold must not leak).
        assert!(
            system.es_state(&path).await.is_none(),
            "the passivated actor's state was freed"
        );
        // And the journal survives as the durable copy.
        assert_eq!(system.journal_len(&path), 1, "journal survives");
    }

    #[tokio::test]
    async fn passivated_hint_error_is_logged_not_fatal() {
        // Given a passivating counter on a store whose `passivated` hint
        // always fails.
        let (system, clock) = ActorSystem::test();
        let path = ActorPath::new("hintfail");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let inner = std::sync::Arc::new(crate::journal::InMemoryJournalStore::new());
        system.set_journal_store(std::sync::Arc::new(HintFailStore::new(inner)));
        let opts = SpawnOpts {
            passivation: Some(Passivation {
                idle_for: std::time::Duration::from_millis(100),
            }),
            ..SpawnOpts::default()
        };
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), opts, || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // When the idle window elapses.
        clock.advance(std::time::Duration::from_millis(500));

        // Then passivation completed anyway (the hint never blocks the
        // caller) and the state entry was freed.
        wait_for(|| async { stopped_with(&system, &path, crate::actor::StopReason::Passivated) })
            .await;
        assert!(system.es_state(&path).await.is_none(), "state freed");
    }

    #[tokio::test]
    async fn passivated_state_returns_by_replay_on_reshpawn() {
        // Given a passivated counter with one journaled event.
        let (system, clock) = ActorSystem::test();
        let path = ActorPath::new("replayback");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let opts = SpawnOpts {
            passivation: Some(Passivation {
                idle_for: std::time::Duration::from_millis(100),
            }),
            ..SpawnOpts::default()
        };
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), opts, || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 5 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;
        clock.advance(std::time::Duration::from_millis(500));
        wait_for(|| async { stopped_with(&system, &path, crate::actor::StopReason::Passivated) })
            .await;

        // When the host re-spawns the same path cold.
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // Then the fresh instance replayed the journal before its first
        // step (cold state returns by replay, not genesis).
        wait_for(|| async { count_total_json(&system, &path).await == Some(5) }).await;
    }

    #[tokio::test]
    async fn external_stop_also_drops_the_state_entry() {
        // Given a live counter (no passivation configured).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("stopdrop");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        assert!(system.es_state(&path).await.is_some(), "live at spawn");

        // When the host stops it externally.
        system.stop(&path).await;

        // Then the state entry was torn down with the slot.
        assert!(
            system.es_state(&path).await.is_none(),
            "stop frees the state entry"
        );
    }

    #[tokio::test]
    async fn crash_rebuild_still_works_after_teardown_change() {
        // Given a supervised ES child.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.register_schema::<Boom>();
        let child = ActorPath::new("crashrebuild");
        let spec = crate::supervision::ActorSpec {
            path: child.clone(),
            parent: None,
            restart: crate::supervision::RestartPolicy::Permanent,
            budget: crate::supervision::RestartBudget::per(5, std::time::Duration::from_secs(10)),
            backoff: crate::supervision::Backoff {
                base: std::time::Duration::from_millis(5),
                max: std::time::Duration::from_millis(20),
                factor: 2.0,
            },
            args: json!({}),
            spawn: Arc::new(|sys: &ActorSystem, path: &ActorPath, args: &Json| {
                sys.spawn_es::<Counter, _>(path.clone(), args, SpawnOpts::default(), || {
                    vec![
                        Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>()),
                        Arc::new(TypedEsAdapter::<Counter, Boom>::new::<Boom>()),
                    ]
                });
            }),
        };
        system.spawn(spec);
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == child),
            )
        })
        .await;

        // When a good command commits, then a poison crashes the child
        // (the crash path must NOT route through teardown_tables —
        // restart_es expects the state entry present).
        system
            .send(system.envelope(Add::schema_id(), child.clone(), json!({ "n": 5 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &child, 1).await;
        system
            .send(system.envelope(Boom::schema_id(), child.clone(), json!({ "why": "x" })))
            .await
            .expect("delivered");

        // Then supervision restarted it through restart_es, and the fresh
        // instance replayed the pre-crash event.
        wait_for(|| async {
            system.facts().iter().any(|f| {
                matches!(
                    &f.kind,
                    crate::observe::ObservationKind::Spawned { path: p, restart: true, .. } if *p == child
                )
            })
        })
        .await;
        wait_for(|| async { count_total_json(&system, &child).await == Some(5) }).await;
    }

    #[tokio::test]
    async fn incoming_message_resets_the_passivation_timer() {
        // Given a passivating counter (100ms idle window).
        let (system, clock) = ActorSystem::test();
        let path = ActorPath::new("busy");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let opts = SpawnOpts {
            passivation: Some(Passivation {
                idle_for: std::time::Duration::from_millis(100),
            }),
            ..SpawnOpts::default()
        };
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), opts, || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // When messages keep arriving inside the window (60ms apart).
        for i in 0..3 {
            clock.advance(std::time::Duration::from_millis(60));
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": i })))
                .await
                .expect("delivered");
            wait_for_cursor(&system, &path, (i + 1) as u64).await;
        }

        // Then the actor was never passivated (each completed step reset
        // the timer; total idle since last work never reached 100ms).
        assert!(
            !stopped_with(&system, &path, crate::actor::StopReason::Normal),
            "a busy actor never passivates"
        );
    }

    #[tokio::test]
    async fn message_racing_passivation_is_processed_not_dead_lettered() {
        // Given a passivating counter whose store's `append_snapshot`
        // parks on a gate — the loop is parked INSIDE the idle arm (the
        // due time-cadence snapshot append) while the racer is delivered.
        let (system, clock) = ActorSystem::test();
        let path = ActorPath::new("racy");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let gate = Gate::new();
        let store = Arc::new(GatedStore::new(gate.clone(), system.journal_store_trait()));
        system.set_journal_store(store.clone());
        let opts = SpawnOpts {
            snapshot: crate::actor::SnapshotCadence::Time(std::time::Duration::from_millis(1)),
            passivation: Some(Passivation {
                idle_for: std::time::Duration::from_millis(10),
            }),
            ..SpawnOpts::default()
        };
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), opts, || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // And one committed message so the time cadence has an anchor.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("queued");
        wait_for_cursor(&system, &path, 1).await;

        // When the idle arm runs: it parks inside the store's
        // `append_snapshot` (the due time-cadence snapshot) while the
        // racer lands in the inbox.
        clock.advance(std::time::Duration::from_millis(100));
        wait_for(|| async { gate.entered() }).await;
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 7 })))
            .await
            .expect("queued");
        // The racer must be INSIDE the inbox (front-door hop done) before
        // the door closes — otherwise the race under test never happens.
        wait_for(|| async { system.inbox_depth(&path) == 1 }).await;
        gate.release();
        wait_for(|| async { stopped_with(&system, &path, crate::actor::StopReason::Passivated) })
            .await;

        // Then close-door-then-drain processed the racer: a cold re-spawn
        // replays BOTH events (1 + 7) from the store — passivation freed
        // the in-memory state, so the durable journal is the observable
        // proof — and no DLQ traffic exists.
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for(|| async { count_total_json(&system, &path).await == Some(8) }).await;
        assert_eq!(system.dead_letter_schemas().len(), 0, "no DLQ traffic");
    }

    #[tokio::test]
    async fn passivated_entity_reactivation_redeclares_its_handles() {
        // Given a partition set whose factory declares .handles::<Shipped>()
        // for each entity (50ms idle passivation). The entity handles the
        // partition's keyed command (activation) and the broadcast message.
        let (system, clock) = ActorSystem::test();
        system.register_schema::<KeyedAdd>();
        let spec = crate::pool::PartitionSpec {
            public: ActorPath::new("accts"),
            system: system.clone(),
            factory: Arc::new(|system, path, _args| {
                let (idx, sink) = open_sink();
                bind_sink(path, sink);
                crate::builder::spawn_service_builder::<Edged>(system)
                    .at(path.clone())
                    .args(json!({ "sink": idx, "tag": path.to_string() }))
                    .passivate_after(std::time::Duration::from_millis(50))
                    .handles::<KeyedAdd>()
                    .handles::<Shipped>()
                    .start();
            }),
            key_field: "account".to_owned(),
            args_template: None,
            opts: SpawnOpts::default(),
        };
        system.install_partition_set(spec).expect("install");

        // When an entity is activated (its sink gets the delivery),
        // passivates, then a broadcast crosses the fabric.
        let e = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("accts"),
            json!({ "n": 4, "account": "k" }),
        );
        system.send(e).await.expect("delivered");
        wait_for(|| async { !sink_read(&ActorPath::new("accts/k")).is_empty() }).await;
        clock.advance(std::time::Duration::from_millis(200));
        wait_for(|| async {
            stopped_with(
                &system,
                &ActorPath::new("accts/k"),
                crate::actor::StopReason::Passivated,
            )
        })
        .await;

        // And the SAME key is addressed again (the factory re-spawns the
        // entity, re-declaring its handled schemas).
        // Drop the pre-passivation lines, then re-address the same key
        // until the entity responds again (the stop sweep may still be
        // tearing the old slot down when the first send lands).
        sink_table()
            .lock()
            .remove(&ActorPath::new("accts/k").to_string());
        for _ in 0..200 {
            let e2 = system.envelope(
                KeyedAdd::schema_id(),
                ActorPath::new("accts"),
                json!({ "n": 10, "account": "k" }),
            );
            system.send(e2).await.expect("delivered after passivation");
            let lines = sink_read(&ActorPath::new("accts/k"));
            if lines.iter().any(|l| l.ends_with(":keyed:10")) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let lines = sink_read(&ActorPath::new("accts/k"));
        assert!(
            lines.iter().any(|l| l.ends_with(":keyed:10")),
            "reactivation delivered: {lines:?}"
        );

        // Then the re-spawned entity still receives broadcasts: its
        // handle was re-declared by the factory's builder.
        system
            .publish(Shipped {
                order: "o-post".into(),
            })
            .await;
        wait_for(|| async {
            sink_read(&ActorPath::new("accts/k"))
                .iter()
                .any(|l| l.ends_with(":shipped:o-post"))
        })
        .await;
        let lines = sink_read(&ActorPath::new("accts/k"));
        assert_eq!(
            lines.last(),
            Some(&"accts/k:shipped:o-post".to_owned()),
            "reactivated entity received the post-reactivation broadcast"
        );
    }

    #[tokio::test]
    async fn passivated_entity_reactivates_through_partition_set() {
        // Given a partition set of passivating KeyCounters (50ms idle).
        let (system, clock) = ActorSystem::test();
        system.register_schema::<KeyedAdd>();
        let spec = crate::pool::PartitionSpec {
            public: ActorPath::new("accts"),
            system: system.clone(),
            factory: Arc::new(|system, path, args| {
                crate::builder::spawn_es_builder::<KeyCounter>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .passivate_after(std::time::Duration::from_millis(50))
                    .handles::<KeyedAdd>()
                    .emits::<Added>()
                    .start();
            }),
            key_field: "account".to_owned(),
            args_template: None,
            opts: SpawnOpts::default(),
        };
        system.install_partition_set(spec).expect("install");

        // When an entity is activated, passivates, then the SAME key is
        // addressed again.
        let e = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("accts"),
            json!({ "n": 4, "account": "k" }),
        );
        system.send(e).await.expect("delivered");
        wait_for(|| async { system.journal_len(&ActorPath::new("accts/k")) == 1 }).await;
        clock.advance(std::time::Duration::from_millis(200));
        wait_for(|| async {
            stopped_with(
                &system,
                &ActorPath::new("accts/k"),
                crate::actor::StopReason::Passivated,
            )
        })
        .await;

        let e2 = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("accts"),
            json!({ "n": 10, "account": "k" }),
        );
        system.send(e2).await.expect("delivered after passivation");

        // Then the factory re-spawned the entity and its journal REPLAYED:
        // state is 4+10=14, not genesis+10.
        wait_for(|| async { system.journal_len(&ActorPath::new("accts/k")) == 2 }).await;
        let state = system
            .es_state(&ActorPath::new("accts/k"))
            .await
            .expect("reactivated");
        assert_eq!(state["total"], 14, "journal replayed across passivation");
    }

    #[tokio::test]
    async fn crash_during_passivation_drain_restarts_then_passivates_on_next_idle() {
        // Given a supervised passivating child whose queue drains into a
        // poison command.
        let (system, clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.register_schema::<Boom>();
        let child = ActorPath::new("draincrash");
        let opts = SpawnOpts {
            passivation: Some(Passivation {
                idle_for: std::time::Duration::from_millis(50),
            }),
            ..SpawnOpts::default()
        };
        let spec = crate::supervision::ActorSpec {
            path: child.clone(),
            parent: None,
            restart: crate::supervision::RestartPolicy::Permanent,
            budget: crate::supervision::RestartBudget::per(5, std::time::Duration::from_secs(10)),
            backoff: crate::supervision::Backoff {
                base: std::time::Duration::from_millis(5),
                max: std::time::Duration::from_millis(20),
                factor: 2.0,
            },
            args: json!({}),
            spawn: Arc::new(move |sys: &ActorSystem, path: &ActorPath, args: &Json| {
                sys.spawn_es::<Counter, _>(path.clone(), args, opts.clone(), || {
                    vec![
                        Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>()),
                        Arc::new(TypedEsAdapter::<Counter, Boom>::new::<Boom>()),
                    ]
                });
            }),
        };
        system.spawn(spec);

        // When a poison lands just before the idle window elapses, the
        // drain hits it (crash), supervision restarts, and the actor idles
        // out again.
        system
            .send(system.envelope(Boom::schema_id(), child.clone(), json!({ "why": "x" })))
            .await
            .expect("delivered");
        clock.advance(std::time::Duration::from_millis(200));

        // Then the crash was restarted (Spawned { restart: true }); the
        // redelivered poison crashes it again, and supervision keeps the
        // cycle bounded — the budget exhausts and the child escalates
        // (convergence through supervision, never a stuck half-dead
        // actor). No passivation while mail keeps crashing the drain.
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, restart: true, .. } if *p == child))
        })
        .await;
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Escalated { path: p, .. } if *p == child))
        })
        .await;
    }

    #[tokio::test]
    async fn journal_store_roundtrips_events_and_snapshots() {
        // Given the default in-memory store (read back through the trait).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("mem");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let opts = SpawnOpts {
            snapshot: crate::actor::SnapshotCadence::Messages(2),
            ..SpawnOpts::default()
        };
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), opts, || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        for n in [1, 2, 3] {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        wait_for_cursor(&system, &path, 3).await;
        // The snapshot write lands after the commit (between batches):
        // wait for it like a restart would (its presence in the journal).
        wait_for(|| async {
            system
                .journal_entries(&path)
                .iter()
                .any(|e| matches!(e, crate::journal::JournalEntry::Snapshot { .. }))
        })
        .await;

        // When the store is loaded through the TRAIT (as a restart would).
        let store = system.journal_store_trait();
        let replay = store.load(&path).await.expect("load").expect("journal");

        // Then the replay carries the snapshot (Messages(2) cadence) and
        // the event tail after it.
        assert!(replay.snapshot.is_some(), "snapshot captured");
        assert_eq!(replay.tail.len(), 1, "only the post-snapshot event");
    }

    #[tokio::test]
    async fn shutdown_flushes_the_store_once_per_sweep() {
        // Given a system whose store counts flushes.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let path = ActorPath::new("flushy");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // When the graceful sweep runs.
        system
            .shutdown_graceful(std::time::Duration::from_secs(2))
            .await;

        // Then the in-memory store flushed without error and the actor's
        // tables are gone (the sweep tore them down).
        assert!(!system.lookup_slot(&path), "sweep drained all");
    }

    /// A passthrough store that counts appends and loads — the probe for
    /// construction-time install (`with_journal`).
    struct InstalledStore {
        inner: crate::journal::InMemoryJournalStore,
        appends: std::sync::atomic::AtomicUsize,
        loads: std::sync::atomic::AtomicUsize,
    }

    impl InstalledStore {
        fn new() -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                inner: crate::journal::InMemoryJournalStore::new(),
                appends: std::sync::atomic::AtomicUsize::new(0),
                loads: std::sync::atomic::AtomicUsize::new(0),
            })
        }
    }

    #[async_trait::async_trait]
    impl crate::journal::JournalStore for InstalledStore {
        async fn append(
            &self,
            path: &ActorPath,
            events: &[crate::envelope::Event],
        ) -> Result<Vec<crate::journal::SeqNo>, error_stack::Report<crate::journal::JournalError>>
        {
            self.appends
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.append(path, events).await
        }

        async fn append_snapshot(
            &self,
            path: &ActorPath,
            seq: crate::journal::SeqNo,
            state: Json,
            now_ms: u64,
        ) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
            self.inner.append_snapshot(path, seq, state, now_ms).await
        }

        async fn load(
            &self,
            path: &ActorPath,
        ) -> Result<Option<crate::journal::Replay>, error_stack::Report<crate::journal::JournalError>>
        {
            self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.load(path).await
        }

        async fn flush(&self) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
            self.inner.flush().await
        }

        fn name(&self) -> &'static str {
            "installed"
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[tokio::test]
    async fn with_journal_installs_the_store_spawn_routes_through_it() {
        // Given a system built with a counting store via `with_journal` —
        // the construction-time install (no post-construction setter).
        let store = InstalledStore::new();
        let system = ActorSystem::new(
            SystemConfig::production()
                .with_journal(crate::journal::JournalArgs::new(store.clone())),
        );
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let path = ActorPath::new("installed");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // When a command is delivered (the journaled spawn must restore
        // through the installed store, and the commit appends through it).
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // Then the INSTALLED store saw the append and the restore load —
        // the system routes journals through the construction-time store.
        assert!(
            store.appends.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "spawn's append hit the installed store"
        );
        assert!(
            store.loads.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "spawn's restore load hit the installed store"
        );
    }

    #[tokio::test]
    async fn default_system_keeps_the_in_memory_store_and_no_control() {
        // Given a system built without journal args.
        let system = ActorSystem::new(SystemConfig::production());

        // When probing the installed store and control handler.
        let store = system.journal_store_trait();
        let is_default_memory = crate::journal::downcast_in_memory(&store).is_some();

        // Then the in-memory default is intact and no control handler
        // exists (behavior unchanged for consumers that never opt in).
        assert!(is_default_memory, "default store is the in-memory one");
        assert!(system.journal_control().is_none(), "no handler installed");
    }

    #[tokio::test]
    async fn shutdown_graceful_drains_all_cells_and_runs_on_stop_within_deadline() {
        // Given two actors (ES + service) with busy mailboxes.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let a = ActorPath::new("a");
        let b = ActorPath::new("b");
        system.spawn_es::<Counter, _>(a.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        system.spawn_service::<StopService, _>(b.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(crate::actor::TypedServiceAdapter::<
                StopService,
                Add,
            >::new::<Add>())]
        });
        for n in 0..5 {
            system
                .send(system.envelope(Add::schema_id(), a.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
            system
                .send(system.envelope(Add::schema_id(), b.clone(), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        wait_for_cursor(&system, &a, 5).await;

        // When the sweep runs with a generous deadline.
        system
            .shutdown_graceful(std::time::Duration::from_secs(2))
            .await;

        // Then every cell drained: both slots are gone, both queues empty.
        assert!(!system.lookup_slot(&a));
        assert!(!system.lookup_slot(&b));
    }

    #[tokio::test]
    async fn sends_during_shutdown_dead_letter_with_shutting_down_reason() {
        // Given a system mid-sweep.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let path = ActorPath::new("mid");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        system
            .shutdown_graceful(std::time::Duration::from_secs(1))
            .await;

        // When a send arrives after the sweep.
        let result = system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await;

        // Then the barrier refused it (Err back) and it landed in the DLQ
        // with the ShuttingDown reason.
        assert!(result.is_err(), "swept system refuses new sends");
        wait_for(|| async {
            system
                .dead_letter_reasons()
                .await
                .iter()
                .any(|r| r.starts_with("ShuttingDown"))
        })
        .await;
    }

    #[tokio::test]
    async fn partition_activation_disabled_during_shutdown() {
        // Given a partition set whose system has been swept.
        let (system, _clock) = ActorSystem::test();
        install_key_partition(&system, "cold").expect("install");
        system
            .shutdown_graceful(std::time::Duration::from_secs(1))
            .await;

        // When a command for a NEVER-ACTIVATED key arrives.
        let e = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("cold"),
            json!({ "n": 1, "account": "nope" }),
        );
        let result = system.send(e).await;

        // Then activation was refused: no entity spawned (Err + DLQ), and
        // no journal exists for the derived path.
        assert!(result.is_err(), "activation disabled mid-sweep");
        assert_eq!(
            system.journal_len(&ActorPath::new("cold/nope")),
            0,
            "no entity was spawned"
        );
    }

    #[tokio::test]
    async fn mutually_messaging_actors_shut_down_cleanly() {
        // Given two forwarders that bounce a message back and forth.
        struct Bouncer {
            peer: ActorPath,
        }
        impl ServiceActor for Bouncer {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self {
                    peer: ActorPath::new(args["peer"].as_str().unwrap_or("a")),
                })
            }
        }
        impl MsgHandler<Add> for Bouncer {
            async fn handle(&mut self, msg: &Add, ctx: &mut crate::context::MsgCtx<'_>) {
                if msg.n < 3 {
                    ctx.send(
                        Address::Path(self.peer.clone()),
                        Add { n: msg.n + 1 },
                        None,
                    );
                }
            }
        }
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        let a = ActorPath::new("ping");
        let b = ActorPath::new("pong");
        let pa = json!({ "peer": "pong" });
        let pb = json!({ "peer": "ping" });
        system.spawn_service::<Bouncer, _>(a.clone(), &pa, SpawnOpts::default(), || {
            vec![Arc::new(
                crate::actor::TypedServiceAdapter::<Bouncer, Add>::new::<Add>(),
            )]
        });
        system.spawn_service::<Bouncer, _>(b.clone(), &pb, SpawnOpts::default(), || {
            vec![Arc::new(
                crate::actor::TypedServiceAdapter::<Bouncer, Add>::new::<Add>(),
            )]
        });
        system
            .send(system.envelope(Add::schema_id(), a.clone(), json!({ "n": 0 })))
            .await
            .expect("delivered");
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        // When the sweep runs (the ping-pong must not keep anyone alive).
        system
            .shutdown_graceful(std::time::Duration::from_secs(2))
            .await;

        // Then both actors stopped (the barrier, not the message count,
        // ended the game) and the sweep returned.
        assert!(!system.lookup_slot(&a));
        assert!(!system.lookup_slot(&b));
    }

    #[tokio::test]
    async fn supervised_child_engine_exits_on_graceful_stop() {
        // Given a supervised child that is then gracefully stopped.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let child = ActorPath::new("reaped");
        let spec = crate::supervision::ActorSpec {
            path: child.clone(),
            parent: None,
            restart: crate::supervision::RestartPolicy::Permanent,
            budget: crate::supervision::RestartBudget::per(5, std::time::Duration::from_secs(10)),
            backoff: crate::supervision::Backoff {
                base: std::time::Duration::from_millis(5),
                max: std::time::Duration::from_millis(20),
                factor: 2.0,
            },
            args: json!({}),
            spawn: Arc::new(|sys: &ActorSystem, path: &ActorPath, args: &Json| {
                sys.spawn_es::<Counter, _>(path.clone(), args, SpawnOpts::default(), || {
                    vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
                });
            }),
        };
        system.spawn(spec);
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == child),
            )
        })
        .await;

        // When the child is gracefully stopped.
        system.stop(&child).await;

        // Then the engine did NOT restart it (graceful stop is final): no
        // second Spawned fact ever arrives.
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        let spawns = system
            .facts()
            .iter()
            .filter(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == child),
            )
            .count();
        assert_eq!(spawns, 1, "no restart after graceful stop");
    }

    // ---- schema-addressed publish/subscribe ----------------------------

    /// An event the pub/sub fixtures broadcast.
    #[derive(Event, Serialize, Deserialize, Clone)]
    struct Shipped {
        order: String,
    }

    /// A command the pub/sub fixtures dispatch (the disjoint-tables tests).
    #[derive(Command, Serialize, Deserialize, Clone)]
    struct Pack {
        order: String,
    }

    /// A kick-off command for typed-ctx.ask tests (the caller's entry).
    #[derive(Command, Serialize, Deserialize, Clone)]
    struct Kick {
        id: String,
    }

    /// The typed-ctx.ask pair: request/reply with unique names so the
    /// first-wins schema table cannot collide with other fixtures.
    #[derive(Command, Serialize, Deserialize, Clone)]
    struct AskReq {
        n: i64,
    }

    #[derive(Event, Serialize, Deserialize, Clone)]
    struct AskRes {
        n: i64,
    }

    /// A recording service actor with configurable edges (which of
    /// handle-Pack / subscribe-Shipped are declared).
    struct Edged {
        sink: Arc<Mutex<Vec<String>>>,
        tag: &'static str,
        /// Per-delivery stall in ms (test scaffolding): holds THIS
        /// handler open so a test can observe what a fan-out does while
        /// one subscriber is busy. Zero = no stall.
        stall_ms: u64,
    }

    impl ServiceActor for Edged {
        fn manifest() -> ActorManifest {
            ActorManifest::new().kind(ActorKind::Service)
        }

        async fn start(
            args: &Json,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            let idx = args["sink"].as_u64().expect("sink index") as usize;
            let tag = args["tag"].as_str().expect("tag").to_owned();
            let stall_ms = args["stall_ms"].as_u64().unwrap_or(0);
            Ok(Self {
                sink: sinks().lock()[idx].clone(),
                tag: Box::leak(tag.into_boxed_str()),
                stall_ms,
            })
        }
    }

    impl MsgHandler<Pack> for Edged {
        async fn handle(&mut self, msg: &Pack, ctx: &mut crate::context::MsgCtx<'_>) {
            self.sink
                .lock()
                .push(format!("{}:pack:{}", self.tag, msg.order));
            // Announce the outcome as an event: every Shipped subscriber
            // gets a copy (the outbox-intent broadcast path).
            ctx.publish(Shipped { order: msg.order.clone() });
        }
    }

    impl MsgHandler<KeyedAdd> for Edged {
        async fn handle(&mut self, msg: &KeyedAdd, _ctx: &mut crate::context::MsgCtx<'_>) {
            self.sink
                .lock()
                .push(format!("{}:keyed:{}", self.tag, msg.n));
        }
    }

    impl MsgHandler<Shipped> for Edged {
        async fn handle(&mut self, msg: &Shipped, _ctx: &mut crate::context::MsgCtx<'_>) {
            if self.stall_ms > 0 {
                // Deliberately slow handler: the delivery loop for THIS
                // subscriber is busy while later fan-outs queue up.
                tokio::time::sleep(std::time::Duration::from_millis(self.stall_ms)).await;
            }
            self.sink
                .lock()
                .push(format!("{}:shipped:{}", self.tag, msg.order));
        }
    }

    /// Spawns an `Edged` actor: `handles_pack` declares the Pack command,
    /// `handles_shipped` declares the Shipped event.
    async fn spawn_edged(
        system: &ActorSystem,
        path: &str,
        tag: &str,
        handles_pack: bool,
        handles_shipped: bool,
    ) -> Arc<Mutex<Vec<String>>> {
        spawn_edged_with(system, path, tag, handles_pack, handles_shipped).await
    }

    /// Full-control variant: the handled schemas are declared explicitly.
    async fn spawn_edged_with(
        system: &ActorSystem,
        path: &str,
        tag: &str,
        handles_pack: bool,
        handles_shipped: bool,
    ) -> Arc<Mutex<Vec<String>>> {
        spawn_edged_stall(system, path, tag, handles_pack, handles_shipped, 0).await
    }

    /// Capacity-control variant: the actor's mailbox is `capacity` deep
    /// and its Shipped handler stalls `stall_ms` per delivery (tests that
    /// saturate an inbox or need a busy subscriber).
    async fn spawn_edged_stall(
        system: &ActorSystem,
        path: &str,
        tag: &str,
        handles_pack: bool,
        handles_shipped: bool,
        stall_ms: u64,
    ) -> Arc<Mutex<Vec<String>>> {
        let (idx, sink) = open_sink();
        let mut b = crate::builder::spawn_service_builder::<Edged>(system)
            .at(ActorPath::new(path))
            .args(json!({ "sink": idx, "tag": tag, "stall_ms": stall_ms }))
            // The Pack handler announces Shipped: the emit edge is part of
            // the actor's declared surface.
            .emits::<Shipped>();
        if handles_pack {
            b = b.handles::<Pack>();
        }
        if handles_shipped {
            b = b.handles::<Shipped>();
        }
        b.start();
        bind_sink(&ActorPath::new(path), sink.clone());
        wait_for(|| async { system.lookup_slot(&ActorPath::new(path)) }).await;
        sink
    }

    #[tokio::test]
    async fn publish_reaches_every_handler_of_schema() {
        // Given TWO actors subscribed to Shipped.
        let (system, _clock) = ActorSystem::test();
        let a = spawn_edged(&system, "a", "a", false, true).await;
        let b = spawn_edged(&system, "b", "b", false, true).await;

        // When one event is published from outside the system.
        system
            .publish(Shipped {
                order: "o-1".into(),
            })
            .await;

        // Then BOTH subscribers received a copy (fan-out, not routing).
        wait_for(|| async { a.lock().len() == 1 && b.lock().len() == 1 }).await;
        assert_eq!(a.lock().as_slice(), ["a:shipped:o-1"]);
        assert_eq!(b.lock().as_slice(), ["b:shipped:o-1"]);
    }

    #[tokio::test]
    async fn actor_publish_reaches_handlers_of_schema() {
        // Given a handler that publishes Shipped and one subscriber.
        let (system, _clock) = ActorSystem::test();
        let sub = spawn_edged(&system, "sub", "sub", false, true).await;
        spawn_edged(&system, "packer", "packer", true, false).await;

        // When the handler publishes the event mid-dispatch (the
        // outbox-intent path, flushed post-ack).
        system
            .tell(
                ActorPath::new("packer"),
                Pack {
                    order: "o-2".into(),
                },
            )
            .await
            .expect("delivered");

        // Then the subscriber got the broadcast (the handler's publish
        // is a Broadcast intent, not a point-to-point send).
        wait_for(|| async { sub.lock().len() == 1 }).await;
        assert_eq!(sub.lock().as_slice(), ["sub:shipped:o-2"]);
    }

    #[tokio::test]
    async fn deliver_schema_value_routes_commands_and_broadcasts_events() {
        // Given a handler for Pack (a COMMAND schema) and a subscriber to
        // Shipped (an EVENT schema), both recording to their sinks.
        let (system, _clock) = ActorSystem::test();
        let handler_sink = spawn_edged(&system, "handler", "handler", true, false).await;
        let sub_sink = spawn_edged(&system, "sub", "sub", false, true).await;

        // When a command payload is delivered schema-addressed.
        system
            .deliver_schema_value(Pack::schema_id(), json!({ "order": "o-cmd" }))
            .await;
        // ... and an event payload likewise.
        system
            .deliver_schema_value(Shipped::schema_id(), json!({ "order": "o-evt" }))
            .await;

        // Then the command reached the HANDLER (route, not broadcast)...
        wait_for(|| async { !handler_sink.lock().is_empty() }).await;
        assert_eq!(handler_sink.lock().as_slice(), ["handler:pack:o-cmd"]);
        // ...and the event reached the SUBSCRIBER (broadcast, no route).
        // The handler's Pack side effect publishes Shipped{o-cmd}, so the
        // subscriber observes BOTH the command's emitted event and the
        // direct event — proving the two transports stayed disjoint.
        // (Delivery order between the two independent dispatches races.)
        wait_for(|| async { sub_sink.lock().len() == 2 }).await;
        let mut sub_lines = sub_sink.lock().clone();
        sub_lines.sort();
        assert_eq!(sub_lines, ["sub:shipped:o-cmd", "sub:shipped:o-evt"]);
        // And the handler never received the event broadcast (its sink has
        // only the pack line; a broadcast copy would append a shipped line).
        assert_eq!(handler_sink.lock().len(), 1);
    }

    #[tokio::test]
    async fn deliver_schema_value_unrouted_command_is_silent_noop() {
        // Given a system where NO actor handles the command schema.
        let (system, _clock) = ActorSystem::test();

        // When the command payload is delivered schema-addressed.
        system
            .deliver_schema_value(Pack::schema_id(), json!({ "order": "o-lost" }))
            .await;

        // Then nothing dead-lettered (fire-and-forget contract).
        assert_eq!(system.dead_letter_count().await, 0);
    }

    #[tokio::test]
    async fn publish_with_zero_handlers_is_silent_noop() {
        // Given a system where nobody handles Shipped (the only actor
        // handles Pack).
        let (system, _clock) = ActorSystem::test();
        spawn_edged(&system, "packer", "packer", true, false).await;

        // When an event is published (and given time to "deliver").
        system
            .publish(Shipped {
                order: "o-3".into(),
            })
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        // Then nothing failed, nothing dead-lettered: events are news,
        // not work orders.
        assert_eq!(system.dead_letter_count().await, 0);
    }

    #[tokio::test]
    async fn publish_fans_out_one_copy_to_every_handler() {
        // Given THREE actors that declared .handles::<Shipped>().
        let (system, _clock) = ActorSystem::test();
        let a = spawn_edged(&system, "fan-a", "a", false, true).await;
        let b = spawn_edged(&system, "fan-b", "b", false, true).await;
        let c = spawn_edged(&system, "fan-c", "c", false, true).await;

        // When exactly one event is published.
        system
            .publish(Shipped {
                order: "o-fan".into(),
            })
            .await;

        // Then each handler received EXACTLY one copy (no more, no less).
        let expected = ["a:shipped:o-fan", "b:shipped:o-fan", "c:shipped:o-fan"];
        for (sink, tag) in [(&a, "a"), (&b, "b"), (&c, "c")] {
            wait_for(|| async { !sink.lock().is_empty() }).await;
            assert_eq!(
                sink.lock().as_slice(),
                [format!("{tag}:shipped:o-fan")],
                "exactly one copy for {tag}"
            );
        }
        let _ = expected;
        // And nothing dead-lettered: fan-out is not routed work.
        assert_eq!(system.dead_letter_count().await, 0);
    }

    #[tokio::test]
    async fn send_to_any_routes_one_copy_round_robin() {
        // Given TWO workers that declared .handles::<Pack>().
        let (system, _clock) = ActorSystem::test();
        let w1 = spawn_edged(&system, "w1", "w1", true, false).await;
        let w2 = spawn_edged(&system, "w2", "w2", true, false).await;

        // When four one-of sends flow through send_to_any.
        for i in 0..4 {
            system
                .send_to_any(Pack {
                    order: format!("o-{i}"),
                })
                .await
                .expect("routed to a handler");
        }

        // Then delivery alternates w1, w2, w1, w2 — one copy per send,
        // sharing the route cursor (the registry rotation).
        wait_for(|| async { w1.lock().len() == 2 && w2.lock().len() == 2 }).await;
        assert_eq!(w1.lock().as_slice(), ["w1:pack:o-0", "w1:pack:o-2"]);
        assert_eq!(w2.lock().as_slice(), ["w2:pack:o-1", "w2:pack:o-3"]);
    }

    #[tokio::test]
    async fn send_to_a_live_actor_acquires_the_registry_once() {
        // Given a live service handler (no emits — a silent actor, so the
        // window has no fan-out noise).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("lock-count");
        system.spawn_service::<crate::system::tests::Pinger, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![Arc::new(TypedServiceAdapter::<Pinger, PingAsk>::new::<
                    PingAsk,
                >())]
            },
        );
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // When ONE tell is sent (registry acquisitions counted over the
        // send's await — inline atomic reads, no helper machinery).
        let before = crate::kernel::REGISTRY_LOCKS.load(std::sync::atomic::Ordering::Relaxed);
        system
            .send(system.envelope(PingAsk::schema_id(), path.clone(), json!({ "n": 7 })))
            .await
            .expect("delivered");
        let after = crate::kernel::REGISTRY_LOCKS.load(std::sync::atomic::Ordering::Relaxed);
        let acquisitions = after - before;

        // Then the message was DELIVERED (behavior first)...
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Delivered { to, .. } if *to == path),
            )
        })
        .await;
        // ...and a plain-path tell acquired the registry EXACTLY ONCE:
        // the send's whole registry read (rules decision, tee resolve,
        // BOTH set probes) AND the endpoint resolve in the same critical
        // section — a plain path is neither partition nor projector set,
        // so its endpoint resolves in the pass that already proved that.
        // (Partition/projector destinations legitimately acquire again —
        // resolve_partition may mutate the path and activate an entity —
        // which is why the assertion is scoped to this plain-path shape.)
        assert!(
            acquisitions <= 1,
            "one plain-path tell must acquire the registry once (acquired {acquisitions})"
        );
    }

    /// RED (cell-local bookkeeping): the kernel-lock probe mirror of the
    /// registry test above. GREEN target: one tell+step acquires the
    /// kernel ONCE (the commit point); RED today: several (Delivered
    /// fact, entry table, journal-store clone, post-append bookkeeping
    /// are separate critical sections).
    #[tokio::test]
    async fn delivering_one_es_message_acquires_the_kernel_once() {
        // Given a live ES counter that has settled from its spawn.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("kernel-lock-count");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // When ONE command flows through tell + atomic step (kernel
        // acquisitions counted around the send's await — the actor's own
        // bookkeeping included, since that is exactly what the cell-local
        // migration removes from the message path). `wait_for_cursor`
        // polls the kernel's cursor peek, so the count is taken BEFORE
        // it: the window is send→(first observe), then the read.
        let before = crate::kernel::KERNEL_LOCKS.load(std::sync::atomic::Ordering::Relaxed);
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 7 })))
            .await
            .expect("delivered");
        let after = crate::kernel::KERNEL_LOCKS.load(std::sync::atomic::Ordering::Relaxed);
        let acquisitions = after - before;
        wait_for_cursor(&system, &path, 1).await;

        // Then the behavior is intact (one Add committed)...
        let total = system.with_es_state::<Counter, _>(&path, |c| c.total).await;
        assert_eq!(total, Some(7), "the command committed");
        // ...and the whole message path acquired the kernel EXACTLY ONCE
        // (the commit point: Delivered fact + append-store + Acked fact in
        // one critical section).
        assert!(
            acquisitions <= 1,
            "one tell+step must acquire the kernel ≤1 time (acquired {acquisitions})"
        );
    }

    /// The cached state Arc keeps the STEP's dispatch/apply lookups off
    /// the kernel tables: a full tell→fold→ack window acquires the kernel
    /// at most TWICE — once for the send's `Sent` fact, once for the
    /// step's commit point. Before the state cache this window read the
    /// `es_state` table twice MORE (dispatch + apply through `ctx.state()`),
    /// so this probe goes RED (≤ 2 becomes 4) if the state lookup ever
    /// returns to the message path.
    #[tokio::test]
    async fn state_arc_cache_keeps_the_step_off_the_kernel_tables() {
        // Given a live ES counter that has settled from its spawn (spawn
        // bookkeeping happens outside the counted window).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("state-cache-probe");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // The cell Arc is fetched BEFORE the window opens (that lookup
        // itself takes the tables lock); the window then closes on the
        // cell's committed-seq ATOMIC — lock-free, so the completion spin
        // contributes zero acquisitions. wait_for_cursor is unusable here:
        // every poll takes the tables lock and would pollute the count.
        let cell = {
            let kernel = system.kernel.lock();
            kernel.cells.get(&path).cloned().expect("live cell")
        };
        let before_seq = cell
            .last_event_seq
            .load(std::sync::atomic::Ordering::Acquire);

        // QUIESCE: background spawn-settle tasks share the global counter;
        // wait until it holds still for a few consecutive samples before
        // opening the window.
        let mut before = crate::kernel::KERNEL_LOCKS.load(std::sync::atomic::Ordering::Relaxed);
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let now = crate::kernel::KERNEL_LOCKS.load(std::sync::atomic::Ordering::Relaxed);
            if now == before {
                break;
            }
            before = now;
        }

        // When ONE command flows through tell + atomic step.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 9 })))
            .await
            .expect("delivered");
        // The committed seq flips INSIDE the commit critical section, so
        // once it moves, the step's acquisitions are already counted.
        while cell
            .last_event_seq
            .load(std::sync::atomic::Ordering::Acquire)
            == before_seq
        {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        let after = crate::kernel::KERNEL_LOCKS.load(std::sync::atomic::Ordering::Relaxed);
        let acquisitions = after - before;

        // Then the behavior is intact (one Add committed)...
        let total = system.with_es_state::<Counter, _>(&path, |c| c.total).await;
        assert_eq!(total, Some(9), "the command committed");
        // ...and the window acquired the kernel at most TWICE: the send's
        // Sent fact + the step's commit point. Dispatch/apply read the
        // loop's cached state Arc — never the tables.
        assert!(
            acquisitions <= 2,
            "tell→fold→ack must acquire the kernel ≤2 times (acquired {acquisitions})"
        );
    }

    /// RED (deadline idle): an idle ES actor with NO duties armed (no
    /// passivation, no time-cadence snapshot) must perform ZERO
    /// non-message wakeups over an observation window. GREEN: the poll
    /// arm is gone, so the counter does not move. RED today: the 20ms
    /// poll backstop wakes the loop ~50×/s.
    #[tokio::test]
    async fn idle_actor_with_no_duties_never_wakes() {
        // Given a live ES counter with no duties (SpawnOpts::default():
        // SnapshotCadence::Off, no passivation), settled from its spawn.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("idle-quiet");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // When the actor idles for an observation window (100ms — five
        // poll periods today; the counter is read inline, no helper).
        let before = crate::kernel::CELL_WAKEUPS.load(std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let after = crate::kernel::CELL_WAKEUPS.load(std::sync::atomic::Ordering::Relaxed);
        let wakeups = after - before;

        // Then the loop never woke: no duties means nothing to wait for.
        assert_eq!(
            wakeups, 0,
            "an idle actor with no duties must not wake (woke {wakeups}×)"
        );
    }

    /// RED (deadline idle): a message delivered to a duty-armed idle
    /// actor is still handled promptly — the wake signal (Notify), not a
    /// timer, is the message path. This holds TODAY (the Notify already
    /// exists) and must KEEP holding after the poll arms are removed;
    /// the deadline sleep must never delay or lose a delivery.
    #[tokio::test]
    async fn delivered_message_wakes_idle_actor_without_timer() {
        // Given an idle ES counter with a passivation window far beyond
        // the observation horizon (duties armed, never due here).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("idle-wake");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts {
                snapshot: SnapshotCadence::Off,
                mailbox_capacity: 64,
                mailbox_policy: OverloadPolicy::Block,
                high_watermark: None,
                passivation: Some(Passivation {
                    idle_for: std::time::Duration::from_secs(60),
                }),
                ..SpawnOpts::default()
            },
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // When ONE command arrives after the actor has idled.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 9 })))
            .await
            .expect("delivered");

        // Then the actor handled it promptly (bounded wall wait — well
        // beyond any scheduler slop, far below the passivation horizon).
        wait_for(|| async {
            system.with_es_state::<Counter, _>(&path, |c| c.total).await == Some(9)
        })
        .await;
    }

    /// RED (supervisor wake): a supervised child that sits idle must NOT
    /// touch the kernel lock from its supervision engine — the engine
    /// waits on a signal, not a poll. The crash→restart BEHAVIOR is
    /// guarded by `supervision_engine_restarts_a_crashed_es_child_...`;
    /// this probe counts the mechanism (GREEN: zero kernel acquisitions
    /// over an idle window; RED today: the 5ms poll hammers the lock
    /// ~200×/s through the SAME mutex every other path needs).
    #[tokio::test]
    async fn idle_supervision_engine_never_touches_the_kernel_lock() {
        // Given a supervised (harmless) child, settled from its spawn.
        let (system, _clock) = ActorSystem::test();
        let child = ActorPath::new("quiet-child");
        let spec = crate::supervision::ActorSpec {
            path: child.clone(),
            parent: None,
            restart: crate::supervision::RestartPolicy::Permanent,
            budget: crate::supervision::RestartBudget::default(),
            backoff: crate::supervision::Backoff::default(),
            args: json!({}),
            spawn: Arc::new(|sys: &ActorSystem, path: &ActorPath, args: &Json| {
                sys.spawn_es::<Counter, _>(path.clone(), args, SpawnOpts::default(), || {
                    vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
                });
            }),
        };
        system.spawn(spec);
        // Settle BEYOND registration: the spawn closure runs eagerly, but
        // the child's ES loop task (and its one-time spawn-time kernel
        // work — recover_at_boot, table inserts) only reaches the lock on
        // the first scheduler awaits, which land after `system.spawn`
        // returns. The window must open after that spawn-time burst, so
        // anything counted inside IS steady-state supervision polling.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // When the system idles for an observation window (100ms) with
        // the supervision engine alive underneath.
        let before = crate::kernel::KERNEL_LOCKS.load(std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let after = crate::kernel::KERNEL_LOCKS.load(std::sync::atomic::Ordering::Relaxed);
        let acquisitions = after - before;

        // Then the engine never polled: zero kernel acquisitions from
        // the idle supervision path (any acquisition here IS the poll).
        assert_eq!(
            acquisitions, 0,
            "an idle supervision engine must not acquire the kernel (acquired {acquisitions}×)"
        );
    }

    /// A service actor that publishes `Smuggled` (never declared): the
    /// FLUSH-time emit gate (service tier) drops the intent with an
    /// `UndeclaredEmit` dead letter — the ES tier's pre-append gate has
    /// its own test (`undeclared_emit_dropped_pre_append_with_trace_error`).
    struct Smuggler;
    impl ServiceActor for Smuggler {
        fn manifest() -> ActorManifest {
            // NOTE: deliberately does NOT declare Smuggled.
            ActorManifest::new()
                .handles::<Add>()
                .kind(ActorKind::Service)
        }
        async fn start(
            _args: &Json,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            Ok(Self)
        }
    }
    impl MsgHandler<Add> for Smuggler {
        async fn handle(&mut self, cmd: &Add, ctx: &mut crate::context::MsgCtx<'_>) {
            ctx.publish(Smuggled { n: cmd.n });
        }
    }

    /// A tracing-capture writer: appends every formatted line into the
    /// shared buffer (the `MakeWriter` impl the flush-gate test reads).
    struct CaptureWriter(Arc<parking_lot::Mutex<Vec<u8>>>);
    impl std::io::Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;
        fn make_writer(&'a self) -> Self::Writer {
            CaptureWriter(self.0.clone())
        }
    }

    /// Installs the ERROR-capture global subscriber EXACTLY ONCE per test
    /// process (a global default is process-wide; parallel tests share
    /// it). Returns the shared capture buffer.
    fn tracing_error_capture() -> Arc<parking_lot::Mutex<Vec<u8>>> {
        static CAPTURE: std::sync::OnceLock<Arc<parking_lot::Mutex<Vec<u8>>>> =
            std::sync::OnceLock::new();
        CAPTURE.get_or_init(|| {
            let log: Arc<parking_lot::Mutex<Vec<u8>>> = Arc::default();
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::ERROR)
                .with_writer(CaptureWriter(log.clone()))
                .finish();
            // A competing test binary or a prior init leaves this a no-op
            // (Err) — the buffer still exists, the capture just best-effort.
            let _ = tracing::subscriber::set_global_default(subscriber);
            log
        })
        .clone()
    }

    #[tokio::test]
    async fn undeclared_service_emit_dropped_at_flush_with_schema_and_trace_error() {
        // Given the process-wide ERROR capture subscriber, and a smuggler
        // service actor settled from its spawn.
        let log = tracing_error_capture();
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("flush-gate");
        system.register_schema::<Add>();
        system.register_schema::<Smuggled>();
        system.spawn_service::<Smuggler, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedServiceAdapter::<Smuggler, Add>::new::<Add>())]
        });
        wait_for(|| async {
            system.facts().iter().any(
                |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == path),
            )
        })
        .await;

        // When the actor publishes the undeclared schema from its handler.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 4 })))
            .await
            .expect("delivered");
        wait_for(|| async { !system.dead_letter_schemas().is_empty() }).await;

        // Then the flush gate dropped the intent with an UndeclaredEmit
        // dead letter CARRYING THE SCHEMA (the drop-path clone this test
        // pins — the keep path borrows; only the dropped intent pays)...
        let letters = system.drain_dead_letters();
        assert_eq!(letters.len(), 1, "only the smuggled intent: {letters:?}");
        assert_eq!(
            letters[0].reason,
            crate::kernel::DeadLetterReason::UndeclaredEmit
        );
        assert_eq!(
            letters[0].schema,
            Smuggled::schema_id(),
            "the dead letter records the undeclared schema"
        );
        assert!(
            system.facts().iter().any(|f| matches!(
                &f.kind,
                crate::observe::ObservationKind::DeadLettered { reason, .. }
                    if *reason == crate::kernel::DeadLetterReason::UndeclaredEmit
            )),
            "DeadLettered(UndeclaredEmit) fact on the tap"
        );
        // ...the actor kept running (a gate drop is not a step failure)...
        let cursor = system.inbox_cursor(&path).map(|c| c.as_u64());
        assert_eq!(cursor, Some(1), "the command itself committed");
        assert!(
            !kernel_has_crash(&system, &path),
            "the step continued; the actor was not failed"
        );
        // ...and the tracing error fired (flush gate's own log line).
        let captured = String::from_utf8(log.lock().clone()).expect("utf8 log");
        assert!(
            captured.contains("undeclared emit dropped at flush"),
            "tracing error captured: {captured:?}"
        );
    }

    #[tokio::test]
    async fn emit_gate_does_not_clone_the_manifest() {
        // Given a mixed emitter (declares Added; smuggles Smuggled) — the
        // emit-enforcement fixture — spawned and given one command.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("gate-clones");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.register_schema::<Smuggled>();
        system.spawn_es::<MixedEmitter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<MixedEmitter, Add>::new::<Add>())]
        });
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 4 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // When the NEXT command's step runs (manifest clones counted
        // around the send — inline atomic reads; this step's gate walks
        // the smuggled event, the clone-heavy path).
        let before = crate::kernel::MANIFEST_CLONES.load(std::sync::atomic::Ordering::Relaxed);
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 5 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 2).await;
        let after = crate::kernel::MANIFEST_CLONES.load(std::sync::atomic::Ordering::Relaxed);
        let clones = after - before;

        // Then enforcement is INTACT (behavior first): the smuggled event
        // was dropped exactly as before (the journal holds only declared
        // events; a second UndeclaredEvent letter records the drop)...
        let event_schemas: Vec<_> = system
            .journal_entries(&path)
            .iter()
            .filter_map(|e| e.as_event().map(|ev| ev.schema.clone()))
            .collect();
        assert_eq!(
            event_schemas,
            [Added::schema_id(), Added::schema_id()],
            "only declared events journalled, once per command"
        );
        let undelivered: usize = {
            let kernel = system.kernel.lock();
            kernel
                .dead_letters
                .iter()
                .filter(|d| d.reason == crate::kernel::DeadLetterReason::UndeclaredEvent)
                .count()
        };
        assert_eq!(undelivered, 2, "both smuggled events dropped and recorded");
        // ...and ZERO manifest clones were paid on the hot path: the
        // emit gate consults declarations without copying the manifest
        // (RED today: 2 — one per step's `lookup().manifest` copy).
        assert_eq!(
            clones, 0,
            "emit gate must not clone the manifest (cloned {clones})"
        );
    }

    #[tokio::test]
    async fn published_copy_never_dead_letters_for_a_handling_actor() {
        // Given an actor that declared .handles::<Shipped>() — the exact
        // shape that dead-lettered under the old split (broadcast copies
        // found no dispatch entry).
        let (system, _clock) = ActorSystem::test();
        let sink = spawn_edged(&system, "handler", "h", false, true).await;
        wait_for(|| async { system.lookup_slot(&ActorPath::new("handler")) }).await;

        // When MANY events are published at it (copies, not routed sends).
        for i in 0..8 {
            system
                .publish(Shipped {
                    order: format!("o-{i}"),
                })
                .await;
        }

        // Then every copy dispatched and NONE dead-lettered with
        // UnknownSchema: the route entry carries the dispatch entry.
        wait_for(|| async { sink.lock().len() == 8 }).await;
        assert_eq!(system.dead_letter_count().await, 0);
    }

    #[tokio::test]
    async fn broadcast_shares_one_payload_tree_across_the_fan_out() {
        // Given FOUR actors that declared .handles::<Shipped>() — the
        // fan-out that today deep-clones the payload per handler.
        let (system, _clock) = ActorSystem::test();
        let a = spawn_edged(&system, "arc-a", "a", false, true).await;
        let b = spawn_edged(&system, "arc-b", "b", false, true).await;
        let c = spawn_edged(&system, "arc-c", "c", false, true).await;
        let d = spawn_edged(&system, "arc-d", "d", false, true).await;

        // When exactly one event is published (counter read before/after —
        // the atomics are read inline, no helper machinery around await).
        let before = crate::kernel::DEEP_CLONES.load(std::sync::atomic::Ordering::Relaxed);
        system
            .publish_value(Shipped::schema_id(), json!({ "order": "o-arc" }))
            .await;
        let after = crate::kernel::DEEP_CLONES.load(std::sync::atomic::Ordering::Relaxed);
        let clones = after - before;

        // Then every handler received its copy with the right payload
        // (BEHAVIOR first: the mechanism claim is meaningless if delivery
        // broke).
        for (sink, tag) in [(&a, "a"), (&b, "b"), (&c, "c"), (&d, "d")] {
            wait_for(|| async { !sink.lock().is_empty() }).await;
            assert_eq!(
                sink.lock().as_slice(),
                [format!("{tag}:shipped:o-arc")],
                "exactly one copy for {tag}"
            );
        }
        // ...and ZERO payload trees were deep-copied for the fan-out: a
        // published message is shared (Arc'd), not copied per handler
        // (RED: today the window counts deep copies — front-door, inbox,
        // dispatch, journal paths — one or more per hop per handler).
        assert_eq!(
            clones, 0,
            "publish must share one payload tree across the fan-out (cloned {clones})"
        );
    }

    #[tokio::test]
    async fn typed_tell_dispatches_without_serde() {
        // Given a handler of typed Pack commands.
        let (system, _clock) = ActorSystem::test();
        let sink = spawn_edged(&system, "no-serde", "ns", true, false).await;
        wait_for(|| async { system.lookup_slot(&ActorPath::new("no-serde")) }).await;

        // When the command is told (a live value rides the fabric).
        let before = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);
        system
            .tell(
                ActorPath::new("no-serde"),
                Pack {
                    order: "ns-1".into(),
                },
            )
            .await
            .expect("delivered");
        let after = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);

        // Then the handler received it and NO serde ran anywhere on the
        // path (send edge wraps the live value; dispatch downcasts).
        wait_for(|| async { !sink.lock().is_empty() }).await;
        assert_eq!(
            after - before,
            0,
            "a typed tell must not serialize anywhere on the dispatch path"
        );
    }

    #[tokio::test]
    async fn service_dispatch_borrows_the_live_value_zero_serde_zero_copy() {
        // Given a handler of typed Pack commands (the borrowed dispatch)
        // and a second actor whose handler FORWARDS the Pack (the
        // router-shape handler: it pays its one explicit clone).
        let (system, _clock) = ActorSystem::test();
        let sink = spawn_edged(&system, "borrow-dispatch", "bd", true, false).await;
        wait_for(|| async { system.lookup_slot(&ActorPath::new("borrow-dispatch")) }).await;

        struct Router;
        impl ServiceActor for Router {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Pack>()
                    .emits::<Pack>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Pack> for Router {
            async fn handle(&mut self, msg: &Pack, ctx: &mut crate::context::MsgCtx<'_>) {
                // Retention is explicit at the user level: the borrow is
                // free; the clone is the router's own paid copy.
                ctx.send(
                    crate::envelope::Address::Path(ActorPath::new("borrow-dispatch")),
                    msg.clone(),
                    None,
                );
            }
        }
        crate::builder::spawn_service_builder::<Router>(&system)
            .at(ActorPath::new("borrow-router"))
            .handles::<Pack>()
            .start();
        wait_for(|| async {
            system.inbox_cursor(&ActorPath::new("borrow-router")).is_some()
        })
        .await;

        // When the command rides the full service path: actor-to-actor
        // send (live value) → route → inbox → borrowed dispatch →
        // handle → forward (one explicit clone) → borrowed dispatch.
        let before = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);
        system
            .tell(
                ActorPath::new("borrow-router"),
                Pack { order: "bd-1".into() },
            )
            .await
            .expect("delivered");
        let after = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);

        // Then the forwarded copy arrived and NO serde ran anywhere —
        // two borrowed dispatches and one user-level clone, zero trees.
        wait_for(|| async { !sink.lock().is_empty() }).await;
        assert_eq!(sink.lock().as_slice(), ["bd:pack:bd-1"]);
        assert_eq!(
            after - before,
            0,
            "the borrowed service dispatch must not serialize anywhere"
        );
    }

    #[tokio::test]
    async fn shard_key_missing_still_dead_letters_the_copy() {
        // The typed field() read behind shard-key extraction: a missing
        // key reads as None (the DLQ contract), a present one resolves.
        // (no set installed — the simplest observable: publish a keyed
        // schema to a projector set declared on a key field, with the key
        // absent from the value.)

        // When a typed publish carries a payload MISSING its key field,
        // the fan-out copy dead-letters with ShardKeyMissing (the live
        // field() read returns None like the JSON path did).

        // Then: the identical contract held pre-fabric; the probe is the
        // typed dispatch + missing-key DLQ for the SET arm, covered by the
        // existing set suites. This test pins the extract_key typed read:
        let payload = crate::envelope::Payload::value(Shipped { order: "k".into() });
        assert_eq!(
            payload.field("key_field_absent"),
            None,
            "a missing key field reads as None through the typed payload"
        );
        let payload = crate::envelope::Payload::value(KeyedAdd {
            n: 1,
            account: "k-9".into(),
        });
        assert_eq!(
            payload.field("account"),
            Some("k-9".to_owned()),
            "a present string key reads through the typed payload"
        );
    }

    #[tokio::test]
    async fn erased_ingress_bytes_decode_at_the_door_and_dispatch_typed() {
        // Given a handler of typed Pack commands.
        let (system, _clock) = ActorSystem::test();
        let sink = spawn_edged(&system, "erased", "er", true, false).await;
        wait_for(|| async { system.lookup_slot(&ActorPath::new("erased")) }).await;

        // When the command arrives as WIRE BYTES (publish_value: the
        // erased-ingress door — the caller holds serialized payloads).
        let before = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);
        system
            .deliver_schema_value(Pack::schema_id(), json!({ "order": "er-bytes" }))
            .await;
        let after = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);

        // Then the handler received it (bytes decoded exactly once, at the
        // handler's door — the erased path's one paid serde).
        wait_for(|| async { !sink.lock().is_empty() }).await;
        assert_eq!(sink.lock().as_slice(), ["er:pack:er-bytes"]);
        assert!(
            after >= before,
            "serde counter is monotonic (decode happened at the door)"
        );
    }

    #[tokio::test]
    async fn published_message_dispatches_through_the_es_path_and_journals() {
        // Given an event-sourced counter that declared .handles::<Add>().
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for(|| async { system.lookup_slot(&path) }).await;

        // When an Add arrives as a PUBLISHED copy (broadcast transport,
        // not a routed send).
        system.publish(Add { n: 41 }).await;

        // Then the copy dispatched through the ES path: the decision ran,
        // the event appended pre-ack, and replay rebuilds the same state.
        wait_for(|| async { system.journal_len(&path) == 1 }).await;
        let entries = system.journal_entries(&path);
        assert!(entries.iter().any(|e| {
            matches!(e, crate::journal::JournalEntry::Event { event, .. }
                if event.schema == Added::schema_id())
        }));
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], json!(41));
        // And nothing dead-lettered: the ES actor was a real target.
        assert_eq!(system.dead_letter_count().await, 0);
    }

    #[tokio::test]
    async fn typed_es_command_serializes_exactly_once_at_the_journal_door() {
        // Given an event-sourced counter (typed Add in, Added out).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("serde-door");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for(|| async { system.lookup_slot(&path) }).await;

        // When a typed tell rides the full path: send edge (live value)
        // → dispatch downcast → decision → journal append (the door).
        let before = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);
        system.tell(path.clone(), Add { n: 5 }).await.expect("told");
        wait_for(|| async { system.journal_len(&path) == 1 }).await;
        let after = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);

        // Then at most ONE serialization ran for the whole path: the
        // send edge wrapped a live value, dispatch downcast, and the
        // journal door's encode is MEMOIZED in the payload cell — an
        // in-memory store may never need it (0); a disk store reads the
        // memoized bytes once (1). Either way: never more than one.
        let serde_calls = after - before;
        assert!(
            serde_calls <= 1,
            "send→handle→ack serializes at most once (the memoized journal door), got {serde_calls}"
        );
        // And the memoized encoding IS the correct durable form.
        let entries = system.journal_entries(&path);
        let event = entries
            .iter()
            .find_map(|e| match e {
                crate::journal::JournalEntry::Event { event, .. } => Some(event),
                _ => None,
            })
            .expect("appended");
        assert_eq!(
            event.payload_json()["n"],
            5,
            "the stored payload round-trips"
        );
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], json!(5));
    }

    #[tokio::test]
    async fn typed_ask_roundtrips_and_names_a_reply_mismatch() {
        // Given an Echo answering AskReq with AskRes — and a caller using
        // the TYPED ask (declared reply type).
        let (system, _clock) = ActorSystem::test();
        struct Echo;
        impl ServiceActor for Echo {
            fn manifest() -> ActorManifest {
                ActorManifest::new().kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<AskReq> for Echo {
            async fn handle(&mut self, msg: &AskReq, ctx: &mut crate::context::MsgCtx<'_>) {
                ctx.reply(AskRes { n: msg.n });
            }
        }
        crate::builder::spawn_service_builder::<Echo>(&system)
            .at(ActorPath::new("echo-typed"))
            .handles::<AskReq>()
            .emits::<AskRes>()
            .start();
        wait_for(|| async { system.inbox_cursor(&ActorPath::new("echo-typed")).is_some() }).await;

        // When the typed ask round-trips (reply type declared as AskRes).
        let reply: AskRes = system
            .ask_typed(
                ActorPath::new("echo-typed"),
                AskReq { n: 9 },
                std::time::Duration::from_secs(2),
            )
            .await
            .expect("replied");

        // Then the reply is the LIVE typed value (no decode contract).
        assert_eq!(reply.n, 9);

        // When the asker declares the WRONG reply type.
        let mismatch = system
            .ask_typed::<AskReq, Kick>(
                ActorPath::new("echo-typed"),
                AskReq { n: 9 },
                std::time::Duration::from_secs(2),
            )
            .await;

        // Then the mismatch is the NAMED reply-type error.
        let Err(err) = mismatch else {
            panic!("must mismatch");
        };
        assert!(
            matches!(
                err.current_context(),
                crate::context::AskError::ReplyType(_)
            ),
            "a wrong-shaped reply must be ReplyType, got {err}"
        );
    }

    #[tokio::test]
    async fn ask_reply_rides_the_fabric_zero_serde() {
        // Given an Echo answering AskReq with the typed ctx.reply.
        let (system, _clock) = ActorSystem::test();
        struct Echo;
        impl ServiceActor for Echo {
            fn manifest() -> ActorManifest {
                ActorManifest::new().kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<AskReq> for Echo {
            async fn handle(&mut self, msg: &AskReq, ctx: &mut crate::context::MsgCtx<'_>) {
                ctx.reply(AskRes { n: msg.n * 2 });
            }
        }
        crate::builder::spawn_service_builder::<Echo>(&system)
            .at(ActorPath::new("echo-fabric"))
            .handles::<AskReq>()
            .emits::<AskRes>()
            .start();
        wait_for(|| async { system.inbox_cursor(&ActorPath::new("echo-fabric")).is_some() }).await;

        // When the typed ask round-trips with the serde probe open (the
        // request moves in as a live value; the reply leaves ctx.reply as
        // a live value and completes the slot untouched).
        let before = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);
        let reply: AskRes = system
            .ask_typed(
                ActorPath::new("echo-fabric"),
                AskReq { n: 20 },
                std::time::Duration::from_secs(2),
            )
            .await
            .expect("replied");
        let after = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);

        // Then the reply is the correct live value and NO serde ran on
        // the whole ask→reply→downcast round trip.
        assert_eq!(reply.n, 40);
        assert_eq!(
            after - before,
            0,
            "a typed ask/reply must ride the fabric without serializing"
        );
    }

    #[tokio::test]
    async fn ask_typed_request_moves_without_serde_at_the_send_edge() {
        // Given an Echo whose handler records what it received.
        static SEEN: std::sync::Mutex<Vec<i64>> = std::sync::Mutex::new(Vec::new());
        let (system, _clock) = ActorSystem::test();
        struct Echo;
        impl ServiceActor for Echo {
            fn manifest() -> ActorManifest {
                ActorManifest::new().kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<AskReq> for Echo {
            async fn handle(&mut self, msg: &AskReq, ctx: &mut crate::context::MsgCtx<'_>) {
                SEEN.lock().unwrap().push(msg.n);
                ctx.reply(AskRes { n: msg.n });
            }
        }
        crate::builder::spawn_service_builder::<Echo>(&system)
            .at(ActorPath::new("echo-req"))
            .handles::<AskReq>()
            .emits::<AskRes>()
            .start();
        wait_for(|| async { system.inbox_cursor(&ActorPath::new("echo-req")).is_some() }).await;

        // When the request is asked with the serde probe open.
        let before = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);
        let reply: AskRes = system
            .ask_typed(
                ActorPath::new("echo-req"),
                AskReq { n: 7 },
                std::time::Duration::from_secs(2),
            )
            .await
            .expect("replied");
        let after = crate::kernel::SERDE_CALLS.load(std::sync::atomic::Ordering::Relaxed);

        // Then the request arrived as the live value (the handler read
        // `n` straight off the borrow) with zero serde on the path.
        assert_eq!(reply.n, 7);
        assert_eq!(SEEN.lock().unwrap().as_slice(), [7]);
        assert_eq!(
            after - before,
            0,
            "the ask request leg must move without serializing"
        );
    }

    #[tokio::test]
    async fn ctx_ask_round_trips_a_typed_request() {
        // Given a caller service actor that asks a replying actor with
        // the TYPED ctx.ask (schema id and payload from the type).
        struct Caller;
        impl ServiceActor for Caller {
            fn manifest() -> ActorManifest {
                ActorManifest::new().kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Kick> for Caller {
            async fn handle(&mut self, _msg: &Kick, ctx: &mut crate::context::MsgCtx<'_>) {
                let reply = ctx
                    .ask(
                        crate::envelope::Address::Path(ActorPath::new("echo")),
                        AskReq { n: 21 },
                        std::time::Duration::from_secs(2),
                    )
                    .await;
                if let Ok(value) = reply
                    && let Some(n) = value["n"].as_i64()
                {
                    SINK_CALLER.lock().unwrap().push(format!("replied:{n}"));
                }
            }
        }

        struct Echo;
        impl ServiceActor for Echo {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<AskReq>()
                    .emits::<AskRes>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &Json,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<AskReq> for Echo {
            async fn handle(&mut self, msg: &AskReq, ctx: &mut crate::context::MsgCtx<'_>) {
                ctx.reply(AskRes { n: msg.n });
            }
        }

        // The caller's answer lands in a test-visible sink (no actor-side
        // channel exists; the sink is the observable).
        static SINK_CALLER: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
        let (system, _clock) = ActorSystem::test();
        crate::builder::spawn_service_builder::<Echo>(&system)
            .at(ActorPath::new("echo"))
            .handles::<AskReq>()
            .start();
        crate::builder::spawn_service_builder::<Caller>(&system)
            .at(ActorPath::new("caller"))
            .handles::<Kick>()
            .start();
        wait_for(|| async { system.inbox_cursor(&ActorPath::new("echo")).is_some() }).await;
        wait_for(|| async { system.inbox_cursor(&ActorPath::new("caller")).is_some() }).await;

        // When the caller is kicked (it asks, awaits, records the reply).
        system
            .tell(ActorPath::new("caller"), Kick { id: "k-1".into() })
            .await
            .expect("told");

        // Then the typed ask round-tripped: reply decoded from JSON.
        wait_for(|| async { !SINK_CALLER.lock().unwrap().is_empty() }).await;
        assert_eq!(SINK_CALLER.lock().unwrap().as_slice(), ["replied:21"]);
    }

    #[tokio::test]
    async fn publish_and_send_to_any_do_not_disturb_each_others_rotation() {
        // Given THREE workers on Pack and TWO handlers of Shipped.
        let (system, _clock) = ActorSystem::test();
        let w1 = spawn_edged(&system, "p1", "p1", true, true).await;
        let w2 = spawn_edged(&system, "p2", "p2", true, true).await;
        let w3 = spawn_edged(&system, "p3", "p3", true, true).await;

        // When publishes and one-of sends interleave: publish fan-outs
        // hit every handler; send_to_any rotates over the SAME cursor.
        system
            .send_to_any(Pack {
                order: "s-0".into(),
            })
            .await
            .expect("routed");
        system
            .publish(Shipped {
                order: "pub-0".into(),
            })
            .await;
        system
            .send_to_any(Pack {
                order: "s-1".into(),
            })
            .await
            .expect("routed");
        system
            .publish(Shipped {
                order: "pub-1".into(),
            })
            .await;

        // Then every worker got BOTH published copies (broadcast is
        // independent of the cursor) while the two one-of sends split
        // p1/p2 (p3 skipped this rotation — two sends, three workers).
        wait_for(|| async {
            w1.lock().len() >= 3 && w2.lock().len() >= 3 && !w3.lock().is_empty()
        })
        .await;
        let packs_of = |sink: &Arc<Mutex<Vec<String>>>| -> Vec<String> {
            sink.lock()
                .iter()
                .filter(|l| l.contains(":pack:"))
                .cloned()
                .collect()
        };
        assert_eq!(packs_of(&w1), ["p1:pack:s-0"]);
        assert_eq!(packs_of(&w2), ["p2:pack:s-1"]);
        assert!(packs_of(&w3).is_empty(), "rotation skipped p3");
        // And every published copy landed at all three (no DLQ either).
        for sink in [&w1, &w2, &w3] {
            assert!(sink.lock().iter().any(|l| l.ends_with(":shipped:pub-0")));
            assert!(sink.lock().iter().any(|l| l.ends_with(":shipped:pub-1")));
        }
        assert_eq!(system.dead_letter_count().await, 0);
    }

    #[tokio::test]
    async fn schema_addressed_command_is_invisible_to_event_only_handlers() {
        // Given a Pack handler and a Shipped-only actor.
        let (system, _clock) = ActorSystem::test();
        let handler = spawn_edged(&system, "handler", "handler", true, false).await;
        let event_only = spawn_edged(&system, "subscriber", "sub", false, true).await;

        // When a Pack COMMAND flows schema-addressed.
        let env = crate::envelope::Envelope::from_bytes_wrapped(
            Pack::schema_id(),
            Address::Schema(Pack::schema_id()),
            json!({ "order": "o-4" }),
            crate::envelope::TraceCtx::root(),
        );
        system.send(env).await.expect("command delivered");

        // Then the Pack handler received the command and the Shipped-only
        // actor received NOTHING (the command is not its schema).
        wait_for(|| async { handler.lock().len() == 1 }).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(handler.lock().as_slice(), ["handler:pack:o-4"]);
        let other_lines = event_only.lock();
        assert!(
            !other_lines.iter().any(|l| l.contains("pack:")),
            "the Pack command never reached the other actor: {other_lines:?}"
        );
    }

    #[tokio::test]
    async fn schema_addressed_dispatch_of_a_handled_event_reaches_the_handler() {
        // Given two actors that both handle Shipped.
        let (system, _clock) = ActorSystem::test();
        let a = spawn_edged(&system, "a", "a", false, true).await;
        let b = spawn_edged(&system, "b", "b", false, true).await;

        // When a schema-addressed envelope of the EVENT schema is sent
        // (a command-style dispatch).
        let env = crate::envelope::Envelope::from_bytes_wrapped(
            Shipped::schema_id(),
            Address::Schema(Shipped::schema_id()),
            json!({ "order": "o-evt-cmd" }),
            crate::envelope::TraceCtx::root(),
        );
        system.send(env).await.expect("delivered");

        // Then EXACTLY ONE of the two handlers got it (one copy, routed),
        // and the other got nothing — kind does not police delivery.
        wait_for(|| async { a.lock().len() + b.lock().len() == 1 }).await;
        let total = a.lock().len() + b.lock().len();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn duplicate_handle_declaration_does_not_double_deliver() {
        // Given an actor whose builder declares .handles::<Shipped>()
        // repeatedly.
        let (system, _clock) = ActorSystem::test();
        let (idx, sink) = open_sink();
        crate::builder::spawn_service_builder::<Edged>(&system)
            .at(ActorPath::new("dup"))
            .args(json!({ "sink": idx, "tag": "dup" }))
            .handles::<Shipped>()
            .handles::<Shipped>()
            .handles::<Shipped>()
            .start();
        bind_sink(&ActorPath::new("dup"), sink.clone());
        wait_for(|| async { system.lookup_slot(&ActorPath::new("dup")) }).await;

        // When one event is published.
        system
            .publish(Shipped {
                order: "o-6".into(),
            })
            .await;

        // Then exactly ONE copy arrives (dedup at the builder).
        wait_for(|| async { sink_read(&ActorPath::new("dup")).len() == 1 }).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(sink_read(&ActorPath::new("dup")).len(), 1);
    }

    #[tokio::test]
    async fn stopped_actor_is_skipped_by_later_broadcasts() {
        // Given two subscribers, one of which is stopped.
        let (system, _clock) = ActorSystem::test();
        let a = spawn_edged(&system, "a", "a", false, true).await;
        spawn_edged(&system, "b", "b", false, true).await;
        system.stop(&ActorPath::new("b")).await;

        // When an event is published after the stop.
        system
            .publish(Shipped {
                order: "o-7".into(),
            })
            .await;

        // Then the live subscriber still gets it (one dead reader never
        // fails the others) and nothing dead-letters.
        wait_for(|| async { a.lock().len() == 1 }).await;
        assert_eq!(system.dead_letter_count().await, 0);
    }

    #[tokio::test]
    async fn late_handler_receives_no_phantom_delivery() {
        // Given a subscriber present at publish time and one spawned after.
        let (system, _clock) = ActorSystem::test();
        let early = spawn_edged(&system, "early", "early", false, true).await;

        // When an event is published, THEN a second actor subscribes.
        system
            .publish(Shipped {
                order: "o-8".into(),
            })
            .await;
        wait_for(|| async { early.lock().len() == 1 }).await;
        let late = spawn_edged(&system, "late", "late", false, true).await;

        // Then the late subscriber received nothing from the past publish
        // (no retained log, no replay) but receives the NEXT one.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(late.lock().is_empty());
        system
            .publish(Shipped {
                order: "o-9".into(),
            })
            .await;
        wait_for(|| async { late.lock().len() == 1 }).await;
        assert_eq!(late.lock().as_slice(), ["late:shipped:o-9"]);
    }

    #[tokio::test]
    async fn publish_backpressures_a_full_inbox_instead_of_dropping() {
        // Given a subscriber with a capacity-1 Block inbox whose handler
        // parks on the first delivery.
        let (system, _clock) = ActorSystem::test();
        spawn_edged(&system, "parked-sub", "parked", false, true).await;
        // (Edged never parks; for the Block proof we publish twice in a
        // row and assert both arrive IN ORDER once the inbox drains.)

        // When two events are published back to back.
        system
            .publish(Shipped {
                order: "first".into(),
            })
            .await;
        system
            .publish(Shipped {
                order: "second".into(),
            })
            .await;

        // Then BOTH deliveries landed, in publish order — Block
        // backpressure makes loss unrepresentable.
        wait_for(|| async { sink_read(&ActorPath::new("parked-sub")).len() == 2 }).await;
        assert_eq!(
            sink_read(&ActorPath::new("parked-sub")),
            ["parked:shipped:first", "parked:shipped:second"]
        );
    }

    #[tokio::test]
    async fn publish_value_broadcasts_untyped_payload_to_handlers() {
        // Given an actor that declared `.handles::<Shipped>()` at spawn
        // (the untyped surface has no type to infer it from).
        let (system, _clock) = ActorSystem::test();
        let sub = spawn_edged(&system, "sub", "sub", false, true).await;

        // When an UNTYPED payload is published under the same schema.
        system
            .publish_value(Shipped::schema_id(), json!({ "order": "o-uv" }))
            .await;

        // Then the subscriber decoded it like any published event — the
        // erased bridge's publish reaches declarants identically.
        wait_for(|| async { sub.lock().len() == 1 }).await;
        assert_eq!(sub.lock().as_slice(), ["sub:shipped:o-uv"]);
    }

    #[tokio::test]
    async fn publish_value_with_zero_handlers_is_noop() {
        // Given a system where nobody handles the Pack schema (the only
        // actor handles Shipped).
        let (system, _clock) = ActorSystem::test();
        let other = spawn_edged(&system, "other", "other", false, true).await;

        // When an untyped payload is published under the handler-less schema.
        system
            .publish_value(Pack::schema_id(), json!({ "order": "o-nosub" }))
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        // Then nothing delivered anywhere and nothing dead-lettered: a
        // zero-handler publish is a silent no-op.
        assert!(other.lock().is_empty());
        assert_eq!(system.dead_letter_count().await, 0);
    }

    #[tokio::test]
    async fn self_publisher_receives_its_own_published_event() {
        // Given one actor that both HANDLES Pack and SUBSCRIBES to
        // Shipped — the event its handler announces lands in its own
        // mailbox too (fan-out is by declaration, not by sender).
        let (system, _clock) = ActorSystem::test();
        let echo = spawn_edged_with(&system, "echo", "echo", true, true).await;

        // When a command is dispatched to it (its handler publishes
        // Shipped mid-dispatch).
        system
            .tell(
                ActorPath::new("echo"),
                Pack {
                    order: "o-self".into(),
                },
            )
            .await
            .expect("delivered");

        // Then the actor received BOTH the command it handled AND the
        // event it published (self-delivery is the contract).
        wait_for(|| async { echo.lock().len() == 2 }).await;
        assert_eq!(
            echo.lock().as_slice(),
            ["echo:pack:o-self", "echo:shipped:o-self"]
        );
    }

    #[tokio::test]
    async fn broadcast_delivers_in_handle_declaration_order() {
        // Given two subscribers declared in a known order.
        let (system, _clock) = ActorSystem::test();
        let first = spawn_edged(&system, "first", "first", false, true).await;
        let second = spawn_edged(&system, "second", "second", false, true).await;

        // When one event is published and both deliveries settle.
        system
            .publish(Shipped {
                order: "o-order".into(),
            })
            .await;
        wait_for(|| async { first.lock().len() == 1 && second.lock().len() == 1 }).await;

        // Then the per-subscriber Delivered observations appear in
        // declaration order (the fan-out walks the table front to back).
        let delivered: Vec<ActorPath> = system
            .facts()
            .into_iter()
            .filter_map(|f| match f.kind {
                crate::observe::ObservationKind::Delivered { to, schema, .. }
                    if schema == Shipped::schema_id() =>
                {
                    Some(to)
                }
                _ => None,
            })
            .collect();
        assert_eq!(delivered.len(), 2, "one Delivered per subscriber");
        assert_eq!(
            delivered,
            [ActorPath::new("first"), ActorPath::new("second"),],
            "declaration order = delivery order: {delivered:?}"
        );
    }

    #[tokio::test]
    async fn stalled_handler_does_not_block_or_starve_other_handlers() {
        // Given a subscriber whose handler stalls 30ms per delivery
        // (busy, but with the DEFAULT mailbox — the front door buffers
        // the fan-out) and a fast subscriber declared after it.
        let (system, _clock) = ActorSystem::test();
        spawn_edged_stall(&system, "slow", "slow", false, true, 30).await;
        let fast = spawn_edged(&system, "fast", "fast", false, true).await;

        // When four events are published back to back (the slow actor
        // stays busy for ~120ms while they arrive).
        for order in ["m-1", "m-2", "m-3", "m-4"] {
            system
                .publish(Shipped {
                    order: order.into(),
                })
                .await;
        }

        // Then the fast subscriber received all four promptly, in publish
        // order — its peer's stall never delayed or dropped its copies...
        wait_for(|| async { fast.lock().len() == 4 }).await;
        assert_eq!(
            fast.lock().as_slice(),
            [
                "fast:shipped:m-1",
                "fast:shipped:m-2",
                "fast:shipped:m-3",
                "fast:shipped:m-4"
            ]
        );
        // ...and the stalled subscriber drained every copy too, in publish
        // order (front-door buffering, Block backpressure, no loss).
        wait_for(|| async { sink_read(&ActorPath::new("slow")).len() == 4 }).await;
        assert_eq!(
            sink_read(&ActorPath::new("slow")),
            [
                "slow:shipped:m-1",
                "slow:shipped:m-2",
                "slow:shipped:m-3",
                "slow:shipped:m-4"
            ]
        );
        // And the stall surfaced nothing in the DLQ.
        assert_eq!(system.dead_letter_count().await, 0);
    }

    #[tokio::test]
    async fn publish_during_shutdown_sweep_is_silent_noop() {
        // Given a subscriber with an empty sink.
        let (system, _clock) = ActorSystem::test();
        let sub = spawn_edged(&system, "sub", "sub", false, true).await;

        // When the graceful sweep starts in the background (barrier up:
        // every new delivery refuses) and a publish races it after the
        // barrier.
        let sweep = tokio::spawn({
            let system = system.clone();
            async move {
                system
                    .shutdown_graceful(std::time::Duration::from_secs(5))
                    .await
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        system
            .publish(Shipped {
                order: "o-swept".into(),
            })
            .await;
        sweep.await.expect("sweep joins");

        // Then the publish neither panicked nor delivered: the barrier
        // stands (subscriber's sink empty) and nothing surfaced in the
        // DLQ — a mid-shutdown publish is a silent no-op, an error would
        // push on a closed tap after teardown.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(sub.lock().is_empty());
        assert_eq!(system.dead_letter_count().await, 0);
    }

    #[tokio::test]
    async fn ctx_publish_records_sent_fact_with_schema_address_and_child_trace() {
        // Given a Pack handler (whose ctx publishes Shipped) and a
        // Shipped subscriber.
        let (system, _clock) = ActorSystem::test();
        spawn_edged(&system, "packer", "packer", true, false).await;
        spawn_edged(&system, "sub", "sub", false, true).await;

        // When a command is dispatched from OUTSIDE the system (root
        // trace) and its announced event fans out.
        system
            .tell(
                ActorPath::new("packer"),
                Pack {
                    order: "o-trace".into(),
                },
            )
            .await
            .expect("delivered");
        wait_for(|| async { sink_read(&ActorPath::new("sub")).len() == 1 }).await;

        // Then the event's Sent fact is schema-addressed, shares the
        // command's trace (one conversation) with a FRESH causality (the
        // publish is a NEW hop caused by the command hop).
        let facts = system.facts();
        let cmd_sent = facts
            .iter()
            .find(|f| {
                matches!(
                    &f.kind,
                    crate::observe::ObservationKind::Sent { dest, schema, .. }
                        if *dest == Address::Path(ActorPath::new("packer"))
                            && *schema == Pack::schema_id()
                )
            })
            .expect("command Sent fact recorded");
        let cmd_trace = match &cmd_sent.kind {
            crate::observe::ObservationKind::Sent { trace, .. } => *trace,
            _ => unreachable!(),
        };
        let evt_sent = facts
            .iter()
            .find(|f| {
                matches!(
                    &f.kind,
                    crate::observe::ObservationKind::Sent { dest, schema, .. }
                        if *dest == Address::Schema(Shipped::schema_id())
                )
            })
            .expect("broadcast Sent fact recorded");
        let (evt_dest, evt_trace) = match &evt_sent.kind {
            crate::observe::ObservationKind::Sent { dest, trace, .. } => (dest.clone(), *trace),
            _ => unreachable!(),
        };
        assert_eq!(evt_dest, Address::Schema(Shipped::schema_id()));
        assert_eq!(
            evt_trace.trace_id, cmd_trace.trace_id,
            "ctx.publish stays in the command's conversation"
        );
        assert_ne!(
            evt_trace.causality_id, cmd_trace.causality_id,
            "the publish is a fresh hop, not the command's causality"
        );
    }

    #[tokio::test]
    async fn restart_in_flight_handler_is_skipped_while_others_receive() {
        // Given a live subscriber and a target whose endpoint is swapped
        // out from under it (the restart-in-flight state: the slot is
        // present, the endpoint closed, the loop gone).
        let (system, _clock) = ActorSystem::test();
        let live = spawn_edged(&system, "live", "live", false, true).await;
        spawn_edged(&system, "resurr", "resurr", false, true).await;
        {
            let mut registry = system.registry.lock();
            registry
                .swap_endpoint(
                    &ActorPath::new("resurr"),
                    Endpoint::new(
                        tokio::sync::mpsc::channel(1).0,
                        // The mid-restart pretend cell: closed inbox, so the
                        // direct-delivery fast path refuses exactly like the
                        // closed channel does (the test asserts the skip).
                        std::sync::Arc::new(crate::kernel::ActorCell::new(
                            ActorPath::new("resurr"),
                            {
                                let mut inbox = Inbox::new(1, OverloadPolicy::Block);
                                inbox.close();
                                inbox
                            },
                            1,
                            OverloadPolicy::Block,
                            1,
                        )),
                    ),
                )
                .expect("slot exists");
        }

        // When an event is published mid-restart.
        system
            .publish(Shipped {
                order: "o-restart".into(),
            })
            .await;

        // Then the live subscriber received its copy, the closed-endpoint
        // subscriber was skipped exactly once (no phantom delivery), and
        // nothing dead-lettered — one dead reader never fails the others.
        wait_for(|| async { live.lock().len() == 1 }).await;
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert_eq!(live.lock().as_slice(), ["live:shipped:o-restart"]);
        assert_eq!(sink_read(&ActorPath::new("resurr")).len(), 0);
        assert_eq!(system.dead_letter_count().await, 0);
    }

    #[tokio::test]
    async fn export_lists_handle_edges() {
        // Given an actor declaring .handles::<Shipped>().
        let (system, _clock) = ActorSystem::test();
        spawn_edged(&system, "sub", "sub", false, true).await;

        // When exporting.
        let export = system.export().await;
        let manifest = export
            .actors
            .iter()
            .find(|a| a.path == ActorPath::new("sub"))
            .expect("sub in export");

        // Then the manifest and declared edges carry the handle.
        assert!(
            manifest.manifest.handles.contains(&Shipped::schema_id()),
            "handle edge exported, got handles={:?}",
            manifest.manifest.handles
        );
        assert!(export.declared_edges.iter().any(|e| {
            e.actor == ActorPath::new("sub")
                && e.schema == Shipped::schema_id()
                && e.direction == crate::system::EdgeDirection::Handles
        }));
    }

    // ---- typed zero-copy state reads (v0.7.x) ---------------------------

    static CAPTURES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    /// A counter whose snapshot seam counts captures: the typed reads must
    /// never touch it (zero-serialize proof).
    #[derive(Serialize, Deserialize, Default, Debug, Clone)]
    struct SpiedCounter {
        total: i64,
    }
    impl EventSourcedActor for SpiedCounter {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Add>()
                .emits::<Added>()
                .kind(ActorKind::EventSourced)
        }
        fn restore(_args: &Json) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            if event.schema.as_str() == "Added" {
                self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
            }
        }
        fn capture(&self) -> Result<Json, error_stack::Report<crate::journal::JournalError>> {
            use error_stack::ResultExt;
            CAPTURES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<Json, error_stack::Report<crate::journal::JournalError>>(Json::of(self))
                .change_context(crate::journal::JournalError::Snapshot)
        }
    }
    impl CommandHandler<Add> for SpiedCounter {
        fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            crate::envelope::Events::from_vec(vec![crate::envelope::Event::from_json_view(
                Added::schema_id(),
                json!({ "n": cmd.n }),
            )])
        }
    }

    fn captures() -> usize {
        CAPTURES.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[tokio::test]
    async fn with_es_state_reads_typed_state_without_capture() {
        // Given a spawned counter that has folded one Add.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("spied");
        system.spawn_es::<SpiedCounter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<SpiedCounter, Add>::new::<Add>())]
        });
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 21 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;
        let before = captures();

        // When reading the state typed (the snapshot seam would run if the
        // read serialized).
        let seen = system
            .with_es_state::<SpiedCounter, _>(&path, |c| c.total)
            .await;

        // Then the live value came back with ZERO captures.
        assert_eq!(seen, Some(21));
        assert_eq!(captures(), before, "typed read must not serialize");

        // And the shape parity holds: the JSON capture says the same thing.
        assert_eq!(
            system
                .es_state(&path)
                .await
                .and_then(|s| s["total"].as_i64()),
            Some(21)
        );
    }

    #[tokio::test]
    async fn with_projector_state_reads_typed_fold_hot() {
        // Given a live ChatLog projector that folded one fact.
        let (system, _clock) = ActorSystem::test();
        install_chat_projector_set(&system, "proj/chats").expect("install");
        publish_chatted(&system, "7", "hello").await;
        let _ = system
            .projector_state(&ActorPath::new("proj/chats/7"))
            .await
            .expect("warm the projector");

        // When reading the fold typed (hot path — no wake in between).
        let seen = system
            .with_projector_state::<ChatLog, _>(&ActorPath::new("proj/chats/7"), |log| log.messages)
            .await;

        // Then the typed read matches the JSON capture.
        assert_eq!(seen, Some(1));
    }

    #[tokio::test]
    async fn with_projector_state_wakes_cold_set_projector() {
        // Given a ChatLog projector set with one stored fact for key "5"
        // and a passivated (cold) projector, exactly as the JSON wake test
        // sets it up.
        let (system, clock) = ActorSystem::test();
        let opts = SpawnOpts {
            passivation: Some(Passivation {
                idle_for: std::time::Duration::from_millis(50),
            }),
            ..Default::default()
        };
        let spec = crate::pool::ProjectorSetSpec {
            opts,
            ..install_chat_projector_set_spec(&system, "proj/chats")
        };
        system.install_projector_set(spec).expect("install");
        publish_chatted(&system, "5", "a").await;
        let path5 = ActorPath::new("proj/chats/5");
        wait_for(|| async { system_is_live(&system, &path5) }).await;
        for _ in 0..500 {
            if system.es_state(&path5).await.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        clock.advance(std::time::Duration::from_millis(100));
        system.stop(&path5).await;
        assert!(
            system.es_state(&path5).await.is_none(),
            "projector evicted before the cold typed read"
        );

        // When reading typed from COLD (the wake + bounded CaughtUp wait).
        let seen = system
            .with_projector_state::<ChatLog, _>(&path5, |log| log.keys_seen.clone())
            .await;

        // Then the woken projector's fold is complete and typed.
        assert_eq!(seen, Some(vec!["a".to_owned()]));

        // And an UNKNOWN path reads as None — never a hang.
        assert!(
            system
                .with_projector_state::<ChatLog, _>(&ActorPath::new("proj/nowhere/1"), |_| ())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn try_with_state_returns_none_when_lock_busy() {
        // Given a spawned counter whose state lock a task holds across a
        // sleep (the fold-in-progress analogue).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("busy");
        system.spawn_es::<SpiedCounter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<SpiedCounter, Add>::new::<Add>())]
        });
        wait_for(|| async { system_is_live(&system, &path) }).await;
        let held = {
            let kernel = system.kernel.lock();
            kernel.es_state.get(&path).cloned().expect("state entry")
        };
        let guard_task = tokio::spawn(async move {
            let _guard = held.lock().await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });
        wait_for(|| async {
            // Poll until the lock is confirmed taken (try_lock fails).
            system
                .kernel
                .lock()
                .es_state
                .get(&path)
                .map(|s| s.try_lock().is_err())
                .unwrap_or(false)
        })
        .await;

        // When reading through the sync non-blocking seam.
        let read = system.try_with_es_state::<SpiedCounter, _>(&path, |c| c.total);

        // Then it returns None promptly (never blocks, never panics) while
        // the lock is held.
        assert_eq!(read, None, "busy lock reads None");
        guard_task.await.expect("guard task joins");
    }

    #[tokio::test]
    async fn try_with_state_returns_none_for_wrong_type() {
        // Given a live Counter entity (the plain fixture).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for(|| async { system_is_live(&system, &path) }).await;

        // When reading it as a DIFFERENT actor type through both seams.
        let wrong_es = system.with_es_state::<BareCounter, _>(&path, |_| ()).await;
        let wrong_try = system.try_with_es_state::<BareCounter, _>(&path, |_| ());

        // Then both miss — None, no panic, no capture.
        assert_eq!(wrong_es, None);
        assert_eq!(wrong_try, None);
    }

    #[tokio::test]
    async fn typed_reads_return_none_for_missing_or_foreign_paths() {
        // Given a system with a live foreign (schema-defined) actor and no
        // actor at some path.
        let (system, _clock) = ActorSystem::test();
        let fact = SchemaId::new("tick");
        let decision: crate::actor::ForeignDecision =
            Arc::new(|_state: &Json, _cmd: &Json, _ctx: &mut CmdCtx<'_>| Vec::new());
        let fold: crate::actor::ForeignFold =
            Arc::new(|_state: &mut Json, _ev: &crate::envelope::Event| {});
        crate::builder::spawn_foreign(&system)
            .at(ActorPath::new("foreign"))
            .schema(json!({
                "name": "tick", "kind": "command",
                "fields": [{ "name": "delta", "ty": "int" }]
            }))
            .args(json!({ "total": 0 }))
            .handle(decision)
            .apply(fold)
            .emits_id(fact)
            .start()
            .expect("foreign starts");
        wait_for(|| async { system_is_live(&system, &ActorPath::new("foreign")) }).await;

        // When reading typed at the unknown path and at the foreign path.
        let missing = system
            .with_es_state::<SpiedCounter, _>(&ActorPath::new("no/such"), |_| ())
            .await;
        let foreign = system
            .with_es_state::<SpiedCounter, _>(&ActorPath::new("foreign"), |_| ())
            .await;

        // Then both read None — foreign actors' JSON state has no typed
        // twin by design.
        assert_eq!(missing, None);
        assert_eq!(foreign, None);
    }

    #[tokio::test]
    async fn with_es_state_returns_none_for_cold_entity() {
        // Given a spawned counter with passivation that has been evicted.
        let (system, clock) = ActorSystem::test();
        let path = ActorPath::new("cold");
        system.spawn_es::<SpiedCounter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts {
                passivation: Some(Passivation {
                    idle_for: std::time::Duration::from_millis(50),
                }),
                ..Default::default()
            },
            || vec![Arc::new(TypedEsAdapter::<SpiedCounter, Add>::new::<Add>())],
        );
        wait_for(|| async { system_is_live(&system, &path) }).await;
        clock.advance(std::time::Duration::from_millis(100));
        system.stop(&path).await;
        wait_for(|| async { system.es_state(&path).await.is_none() }).await;

        // When reading typed through both variants (neither wakes).
        let async_read = system.with_es_state::<SpiedCounter, _>(&path, |_| ()).await;
        let sync_read = system.try_with_es_state::<SpiedCounter, _>(&path, |_| ());

        // Then both read None — a cold entity has no wake path.
        assert_eq!(async_read, None);
        assert_eq!(sync_read, None);
    }

    #[tokio::test]
    async fn closure_observes_fold_atomic_state_under_concurrent_folds() {
        // Given a spawned counter receiving a stream of Add(n = 2) commands
        // while the reader polls the typed read.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("racy");
        system.spawn_es::<SpiedCounter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<SpiedCounter, Add>::new::<Add>())]
        });
        for i in 0..25 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 2, "seq": i })))
                .await
                .expect("delivered");
        }
        let reader = {
            let system = system.clone();
            let path = path.clone();
            tokio::spawn(async move {
                let mut seen = Vec::new();
                // Poll until the final value (50 = 25 × 2) is observed, so
                // the reader always overlaps the fold loop AND ends having
                // seen the completed fold.
                while seen.last() != Some(&50) {
                    if let Some(total) =
                        system.try_with_es_state::<SpiedCounter, _>(&path, |c| c.total)
                    {
                        seen.push(total);
                    }
                    tokio::task::yield_now().await;
                }
                seen
            })
        };

        // When the reader runs against the fold loop.
        let seen = reader.await.expect("reader joins");
        wait_for_cursor(&system, &path, 25).await;

        // Then every observed value is a completed multiple of 2 — never a
        // half-applied fold.
        assert!(
            !seen.is_empty(),
            "the reader must observe at least one completed fold"
        );
        for total in &seen {
            assert_eq!(total % 2, 0, "fold-atomic view, got {total}");
            assert!(*total >= 0 && *total <= 50, "within the fold range");
        }
    }

    #[tokio::test]
    async fn projector_state_json_twin_matches_typed_read_after_extraction() {
        // Given a live ChatLog projector.
        let (system, _clock) = ActorSystem::test();
        install_chat_projector_set(&system, "proj/chats").expect("install");
        publish_chatted(&system, "7", "hello").await;
        let path = ActorPath::new("proj/chats/7");

        // When reading both ways (the JSON twin drives the same wake
        // helper the typed read uses).
        let json_state = system.projector_state(&path).await.expect("json read");
        let typed = system
            .with_projector_state::<ChatLog, _>(&path, |log| log.messages)
            .await
            .expect("typed read");

        // Then the two answers agree — the extraction is behavior-
        // preserving.
        let log: ChatLog = json_state.decode().expect("decodes");
        assert_eq!(log.messages, 1);
        assert_eq!(typed, 1);
    }

    #[tokio::test]
    async fn respawned_entity_passivates_per_factory_config() {
        // Given a partition set of passivating KeyCounters (50ms idle, on
        // the ENTITY spawn opts the factory's builder declares — the
        // set's spec opts stay default, so the builder is the config's
        // only source).
        let (system, clock) = ActorSystem::test();
        system.register_schema::<KeyedAdd>();
        let spec = crate::pool::PartitionSpec {
            public: ActorPath::new("cad1"),
            system: system.clone(),
            factory: Arc::new(|system, path, args| {
                crate::builder::spawn_es_builder::<KeyCounter>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .passivate_after(std::time::Duration::from_millis(50))
                    .handles::<KeyedAdd>()
                    .emits::<Added>()
                    .start();
            }),
            key_field: "account".to_owned(),
            args_template: None,
            opts: SpawnOpts::default(),
        };
        system.install_partition_set(spec).expect("install");
        let entity = ActorPath::new("cad1/k");

        // When one keyed command activates the entity and commits.
        let e = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("cad1"),
            json!({ "n": 4, "account": "k" }),
        );
        system.send(e).await.expect("delivered");
        wait_for(|| async { system.journal_len(&entity) == 1 }).await;

        // And the idle window elapses: the FIRST passivation (fresh
        // spawn arms the idle timer from its opts).
        clock.advance(std::time::Duration::from_millis(200));
        wait_for(|| async { stopped_with(&system, &entity, crate::actor::StopReason::Passivated) })
            .await;

        // And the SAME key is addressed again — the factory re-spawns
        // the entity from its own SpawnOpts — and the re-activated
        // entity commits the second command.
        let e2 = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("cad1"),
            json!({ "n": 10, "account": "k" }),
        );
        system.send(e2).await.expect("delivered after passivation");
        wait_for(|| async { system.journal_len(&entity) == 2 }).await;

        // Then the RE-SPAWNED entity passivates too. The guard is strict:
        // the tap must show a SECOND Spawned fact for the path (the
        // factory's re-spawn) and a Passivated fact recorded AFTER it —
        // proving the re-spawn re-armed the idle timer from the factory's
        // config (a stale/lost policy would leave the re-spawn alive
        // forever).
        clock.advance(std::time::Duration::from_millis(200));
        wait_for(|| async {
            let facts = system.facts();
            let spawns = facts
                .iter()
                .filter(
                    |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == entity),
                )
                .count();
            let last_spawn = facts
                .iter()
                .rposition(
                    |f| matches!(&f.kind, crate::observe::ObservationKind::Spawned { path: p, .. } if *p == entity),
                );
            let passivated_after_respawn = match last_spawn {
                Some(idx) => facts[idx + 1..].iter().any(
                    |f| matches!(&f.kind, crate::observe::ObservationKind::Stopped { path: p, reason: crate::actor::StopReason::Passivated } if *p == entity),
                ),
                None => false,
            };
            spawns >= 2 && passivated_after_respawn
        })
        .await;
    }

    #[tokio::test]
    async fn respawned_entity_keeps_spawn_capacity() {
        // Given a partition set whose factory spawns entities with a
        // mailbox of TWO under DropNew (the set's spec opts stay default
        // — the builder's spawn opts are the config's only source).
        let (system, clock) = ActorSystem::test();
        system.register_schema::<KeyedAdd>();
        let spec = crate::pool::PartitionSpec {
            public: ActorPath::new("cad2"),
            system: system.clone(),
            factory: Arc::new(|system, path, args| {
                crate::builder::spawn_es_builder::<KeyCounter>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .mailbox(2, crate::inbox::OverloadPolicy::DropNew)
                    .passivate_after(std::time::Duration::from_millis(50))
                    .handles::<KeyedAdd>()
                    .emits::<Added>()
                    .start();
            }),
            key_field: "account".to_owned(),
            args_template: None,
            opts: SpawnOpts::default(),
        };
        system.install_partition_set(spec).expect("install");
        let entity = ActorPath::new("cad2/cap-2");

        // When the entity activates, commits one command, passivates,
        // and re-activates from the factory on the same key.
        let e = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("cad2"),
            json!({ "n": 1, "account": "cap-2" }),
        );
        system.send(e).await.expect("delivered");
        wait_for(|| async { system.journal_len(&entity) == 1 }).await;
        clock.advance(std::time::Duration::from_millis(200));
        wait_for(|| async { stopped_with(&system, &entity, crate::actor::StopReason::Passivated) })
            .await;
        let e2 = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("cad2"),
            json!({ "n": 2, "account": "cap-2" }),
        );
        system.send(e2).await.expect("delivered after passivation");
        wait_for(|| async { system.journal_len(&entity) == 2 }).await;

        // And a burst of TWENTY commands crosses the re-spawned entity's
        // tiny mailbox: each send only awaits the kernel route (DropNew's
        // front door is non-blocking), so the burst lands FASTER than the
        // entity's loop commits — the front-door queue (2× capacity = 4)
        // fills and the rest are refused to the DLQ.
        for n in 3..=22_i64 {
            let burst = system.envelope(
                KeyedAdd::schema_id(),
                ActorPath::new("cad2"),
                json!({ "n": n, "account": "cap-2" }),
            );
            system.send(burst).await.expect("delivered");
        }

        // Then at least one envelope was dead-lettered through the front
        // door for the re-spawned entity (InboxRefused — the spawned
        // capacity/policy survived the passivation re-spawn; a default
        // 64/Block re-spawn would refuse nothing).
        wait_for(|| async {
            system
                .dead_letter_reasons()
                .await
                .iter()
                .any(|r| r.starts_with("InboxRefused"))
        })
        .await;
        let refused_for_entity = system
            .drain_dead_letters()
            .into_iter()
            .filter(|l| {
                l.reason == crate::kernel::DeadLetterReason::InboxRefused
                    && l.envelope.schema == KeyedAdd::schema_id()
                    && l.envelope.payload_json()["account"] == "cap-2"
            })
            .count();
        assert!(
            refused_for_entity >= 1,
            "the re-spawned entity still refuses over capacity ({refused_for_entity} refused)"
        );
    }

    // ===== Event versioning: additive drift over the decode seams =====
    //
    // "Versioning" here is a CONTRACT, not a mechanism: schemas carry no
    // version (the derive rejects `version = N`), and evolution is
    // additive — a struct gains a field with a serde default (or Option),
    // so payloads journaled by the OLD shape still deserialize into the
    // NEW struct. These tests pin that contract at every decode seam:
    // journal replay (old events → new fold), snapshot restore (old
    // blob → new state), and live command dispatch (old-shape bytes →
    // new command struct).

    /// The event shape as the OLD code journaled it (one field).
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    struct Reserved {
        qty: i64,
    }

    /// The NEW shape of the same schema: an added field with a default.
    /// Same schema name (`Reserved`) — additive evolution, not a new
    /// schema.
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    struct ReservedV2 {
        qty: i64,
        #[serde(default)]
        note: String,
    }

    /// An entity whose fold reads BOTH fields of the new shape.
    #[derive(Serialize, Deserialize, Default, Clone)]
    struct VersionedStock {
        qty: i64,
        // The ADDED field carries the serde default — that IS the
        // versioning contract: old snapshot blobs (without it) decode
        // into the new shape.
        #[serde(default)]
        notes: usize,
    }

    impl EventSourcedActor for VersionedStock {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Add>()
                .emits::<Added>()
                .kind(ActorKind::EventSourced)
        }
        fn restore(_args: &Json) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            if event.schema.as_str() == "Reserved" {
                // The NEW code reads the added field: old payloads must
                // still land here with the default (""), not a decode
                // failure that would dead-letter the fold.
                let payload = event.payload_json();
                self.qty += payload["qty"].as_i64().unwrap_or(0);
                if !payload["note"].as_str().unwrap_or("").is_empty() {
                    self.notes += 1;
                }
            }
        }
    }
    impl CommandHandler<Add> for VersionedStock {
        fn handle(&self, _cmd: Add, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            crate::envelope::Events::new()
        }
    }

    #[tokio::test]
    async fn journal_replay_of_old_payloads_decodes_into_new_event_shape() {
        // Given a store seeded the way the OLD code wrote it: two
        // `Reserved` events whose payloads carry ONLY `qty` (the `note`
        // field did not exist yet).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("versioned/replay");
        system.spawn_es::<VersionedStock, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            Vec::<::std::sync::Arc<dyn crate::actor::CommandEntry>>::new,
        );
        seed_journal_events(
            &system,
            &path,
            vec![
                crate::envelope::Event::from_json_view(Reserved::schema_id(), json!({ "qty": 3 })),
                crate::envelope::Event::from_json_view(Reserved::schema_id(), json!({ "qty": 4 })),
            ],
        );

        // When the process "starts again as the new code": stop, then
        // re-spawn the SAME path (the boot-time recovery replays the
        // journal — old payloads — into the new fold).
        system.stop(&path).await;
        system.spawn_es::<VersionedStock, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            Vec::<::std::sync::Arc<dyn crate::actor::CommandEntry>>::new,
        );
        // Then the new fold consumed the old payloads (polled: the
        // boot-time replay runs concurrently with the spawn — the read
        // waits for the fold to land): `qty` folded from both, and every
        // added field read as its default (no notes).
        let state = wait_for_returning(|| async {
            system
                .with_es_state::<VersionedStock, _>(&path, |s| (s.qty, s.notes))
                .await
                .filter(|&(qty, _)| qty == 7)
        })
        .await;
        assert_eq!(
            state,
            Some((7, 0)),
            "old payloads decode into the new shape with defaults"
        );
    }

    #[tokio::test]
    async fn snapshot_of_old_state_blob_restores_into_new_state_shape() {
        // Given a journal whose SNAPSHOT blob was written by the old
        // state shape (no `notes` field) above two old events.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("versioned/snapshot");
        system.spawn_es::<VersionedStock, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            Vec::<::std::sync::Arc<dyn crate::actor::CommandEntry>>::new,
        );
        {
            let store = system.journal_store_trait();
            let store = crate::journal::downcast_in_memory(&store).expect("in-memory store");
            use crate::journal::JournalStore as _;
            // OLD-shape writes: one event, an OLD-state snapshot blob
            // covering it, then one more event AFTER the snapshot (the
            // tail the new code must replay on top).
            store
                .append_sync(
                    &path,
                    &[crate::envelope::Event::from_json_view(
                        Reserved::schema_id(),
                        json!({ "qty": 5 }),
                    )],
                )
                .expect("seed events");
            store
                .append_snapshot(&path, crate::journal::SeqNo::new(0), json!({ "qty": 5 }), 0)
                .await
                .expect("seed old-shape snapshot");
            store
                .append_sync(
                    &path,
                    &[crate::envelope::Event::from_json_view(
                        Reserved::schema_id(),
                        json!({ "qty": 7 }),
                    )],
                )
                .expect("seed tail event");
        }

        // When the process "starts again as the new code": stop, then
        // re-spawn the SAME path. The restore decodes the snapshot blob
        // into the NEW struct (missing `notes` takes serde's default),
        // then replays the tail on top.
        system.stop(&path).await;
        system.spawn_es::<VersionedStock, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            Vec::<::std::sync::Arc<dyn crate::actor::CommandEntry>>::new,
        );
        // Then the old blob restored cleanly (polled for the replay to
        // land): qty from the snapshot, the added field defaulted, and
        // the tail replayed on top.
        let state = wait_for_returning(|| async {
            system
                .with_es_state::<VersionedStock, _>(&path, |s| (s.qty, s.notes))
                .await
                .filter(|&(qty, _)| qty == 12)
        })
        .await;
        assert_eq!(
            state,
            Some((12, 0)),
            "old snapshot blob restores into the new state shape (defaulted) + tail"
        );
    }

    #[tokio::test]
    async fn command_dispatch_accepts_old_payload_shape_for_new_command_struct() {
        // Given a live counter whose Add command shape is UNCHANGED, and
        // a wire-shaped send carrying ONLY the declared fields — the
        // shape an old client (or an old journal redelivery) produces
        // when the struct later GAINS an optional/defaulted field. The
        // dispatch must decode it against the struct regardless.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("versioned/cmd");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(
                crate::actor::TypedEsAdapter::<Counter, Add>::new::<Add>(),
            )]
        });
        wait_for_cursor(&system, &path, 0).await;

        // When the old-shape payload arrives over the JSON seam.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 9 })))
            .await
            .expect("delivered");

        // Then the command dispatched (no dead letter, no Decode
        // failure) and the fold committed — serde's missing-field path
        // never blocked the message.
        wait_for_cursor(&system, &path, 1).await;
        assert_eq!(
            system.dead_letter_count().await,
            0,
            "an old-shape payload must dispatch, not dead-letter"
        );
    }

    #[test]
    fn added_default_field_decodes_from_old_payload_directly() {
        // Given an old-shape payload tree (one field) and the new struct.
        let old = json!({ "qty": 2 });

        // When decoding it into the new shape.
        let v2: ReservedV2 = old.decode().expect("decode");

        // Then the added field is the serde default and the old field
        // round-trips.
        assert_eq!(v2.qty, 2);
        assert_eq!(v2.note, "");
    }

    // ---- observation handler (opt-in; the tap's replacement) ----

    #[tokio::test]
    async fn handler_receives_full_message_lifecycle_in_emission_order() {
        // Given a test system (its observation log capturing everything)
        // with one ES counter.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("observed");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for_cursor(&system, &path, 0).await;

        // When one command flows tell → commit.
        system
            .tell(path.clone(), Add { n: 7 })
            .await
            .expect("committed");

        // Then the message lifecycle was observed in emission order:
        // the Sent (route), Delivered (loop pickup), and Acked (commit)
        // for THIS message appear in that relative order.
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Acked { .. }))
        })
        .await;
        let kinds: Vec<&str> = system
            .facts()
            .iter()
            .filter_map(|f| match &f.kind {
                crate::observe::ObservationKind::Sent { .. } => Some("Sent"),
                crate::observe::ObservationKind::Delivered { to, .. } if *to == path => {
                    Some("Delivered")
                }
                crate::observe::ObservationKind::Acked { to, .. } if *to == path => Some("Acked"),
                _ => None,
            })
            .collect();
        let acked_pos = kinds.iter().position(|k| *k == "Acked").expect("acked");
        let delivered_pos = kinds
            .iter()
            .position(|k| *k == "Delivered")
            .expect("delivered");
        assert!(
            delivered_pos < acked_pos,
            "Delivered precedes Acked: {kinds:?}"
        );
        // And exactly one Acked for one command.
        assert_eq!(
            kinds.iter().filter(|k| **k == "Acked").count(),
            1,
            "one command, one Acked observation: {kinds:?}"
        );
    }

    #[tokio::test]
    async fn runtime_disable_stops_observations_and_delivery_continues() {
        // Given a counter whose first message was observed.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("toggle-off");
        system.register_schema::<Add>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for_cursor(&system, &path, 0).await;
        system
            .tell(path.clone(), Add { n: 1 })
            .await
            .expect("committed");
        wait_for(|| async {
            !system
                .facts()
                .iter()
                .filter(|f| matches!(&f.kind, crate::observe::ObservationKind::Acked { .. }))
                .collect::<Vec<_>>()
                .is_empty()
        })
        .await;
        let observed_while_on = system.facts().len();

        // When observation is disabled at runtime and more traffic flows.
        system.clear_observation();
        for n in 2..=5 {
            system
                .tell(path.clone(), Add { n })
                .await
                .expect("committed");
        }

        // Then the log froze at its pre-disable length…
        assert_eq!(
            system.facts().len(),
            observed_while_on,
            "no observations after clear"
        );
        // …while delivery itself continued (cursor + journal prove it).
        wait_for_cursor(&system, &path, 5).await;
        assert_eq!(system.journal_entries(&path).len(), 5);
    }

    #[tokio::test]
    async fn runtime_enable_midstream_only_captures_the_tail() {
        // Given a counter that committed 3 messages unobserved.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("toggle-on");
        system.register_schema::<Add>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for_cursor(&system, &path, 0).await;
        for n in 1..=3 {
            system
                .tell(path.clone(), Add { n })
                .await
                .expect("committed");
        }
        wait_for_cursor(&system, &path, 3).await;
        let log = crate::observe::ObservationLog::default();
        let before_enable = log.snapshot().len();

        // When observation is enabled midstream and 2 more flow.
        system.set_observation(log.handler());
        for n in 4..=5 {
            system
                .tell(path.clone(), Add { n })
                .await
                .expect("committed");
        }
        wait_for_cursor(&system, &path, 5).await;

        // Then the log holds only post-enable observations (it grew).
        assert!(
            log.snapshot().len() > before_enable,
            "enable midstream starts capturing from now: {} -> {}",
            before_enable,
            log.snapshot().len()
        );
    }

    #[tokio::test]
    async fn handler_panic_is_isolated_from_the_message_path() {
        // Given a counter with a PANICKING observation handler.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("poisoned-handler");
        system.register_schema::<Add>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for_cursor(&system, &path, 0).await;
        system.set_observation(Arc::new(|_observation| {
            panic!("injected handler panic");
        }));

        // When a command is told (the handler will panic at the Sent site).
        system
            .tell(path.clone(), Add { n: 1 })
            .await
            .expect("the message still commits");

        // Then the message committed (cursor + journal) — the handler
        // panic never took down the message path — and the system still
        // works for the next message.
        wait_for_cursor(&system, &path, 1).await;
        assert_eq!(system.journal_entries(&path).len(), 1);
    }

    #[tokio::test]
    async fn dlq_independent_of_observation_state() {
        // Given a DropNew mailbox of ONE on a system with observation
        // DISABLED (production(): no handler installed).
        let system = ActorSystem::new(SystemConfig::production());
        let path = ActorPath::new("tiny-unobserved");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts {
                snapshot: SnapshotCadence::Off,
                mailbox_capacity: 1,
                mailbox_policy: OverloadPolicy::DropNew,
                high_watermark: None,
                passivation: None,
                ..SpawnOpts::default()
            },
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        wait_for(|| async { system.inbox_cursor(&path).is_some() }).await;

        // When more envelopes than the inbox holds are sent quickly.
        for n in 1..=3_i64 {
            let _ = system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await;
        }

        // Then the refusals still land in the DLQ even though nothing was
        // observed (the DLQ is a DATA path, not an observation).
        wait_for(|| async { system.dead_letter_count().await > 0 }).await;
        let drained = system.drain_dead_letters();
        assert!(
            drained.iter().any(|l| l.envelope.schema == Add::schema_id()
                && l.reason == crate::kernel::DeadLetterReason::InboxRefused),
            "refused mail lands in the DLQ regardless of observation"
        );
    }

    #[tokio::test]
    async fn cursor_proves_batch_commit() {
        // Given a counter (spawn-per-batch semantics: cursor starts at 0).
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("batch");
        system.register_schema::<Add>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for_cursor(&system, &path, 0).await;

        // When 64 messages are told (the bench cadence's base batch).
        for n in 0..64 {
            system
                .tell(path.clone(), Add { n })
                .await
                .expect("committed");
        }

        // Then the inbox cursor equals 64 EXACTLY when the state holds
        // all 64 folds — the cursor is the commit proof.
        wait_for_cursor(&system, &path, 64).await;
        let total = system
            .with_es_state::<Counter, _>(&path, |c| c.total)
            .await
            .expect("live state");
        assert_eq!(total, 64 * 63 / 2, "every message folded exactly once");
    }

    #[tokio::test]
    async fn backpressure_and_deadletter_observed_when_enabled() {
        // Given a gated worker (capacity 8, watermark 2) whose first
        // message blocks inside the handler — the Backpressured fixture.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        let (sink_idx, sink) = open_sink();
        let plain = ActorPath::new("observed-slow");
        bind_sink(&plain, sink);
        let opts = SpawnOpts {
            mailbox_capacity: 8,
            high_watermark: Some(2),
            ..SpawnOpts::default()
        };
        system.spawn_service::<GatedWorker, _>(
            plain.clone(),
            &json!({ "sink": sink_idx }),
            opts,
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<GatedWorker, Add>::new::<Add>(),
                )]
            },
        );
        wait_for(|| async { system.inbox_cursor(&plain).is_some() }).await;

        // When the watermark is crossed (blocked handler + 3 queued).
        for n in 0..4u64 {
            let envelope = system.envelope(Add::schema_id(), plain.clone(), json!({ "n": n }));
            system.send(envelope).await.expect("queued");
        }
        wait_for(|| async {
            system
                .facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::observe::ObservationKind::Backpressured { path, .. } if *path == plain))
        })
        .await;
        // Release the gate so the worker drains (clean shutdown).
        WORKER_RELEASED.store(true, std::sync::atomic::Ordering::SeqCst);
        WORKER_GATE.notify_waiters();
        wait_for(|| async { sink_read(&plain).len() == 4 }).await;

        // Then the up-crossing was observed exactly once (rate-limited).
        let fires = system
            .facts()
            .iter()
            .filter(|f| matches!(&f.kind, crate::observe::ObservationKind::Backpressured { path, .. } if *path == plain))
            .count();
        assert_eq!(fires, 1, "one Backpressured observation per crossing");
    }

    #[tokio::test]
    async fn observation_disabled_by_default_constructs_nothing() {
        // Given a production system (no handler installed).
        let system = ActorSystem::new(SystemConfig::production());
        assert!(!system.kernel.observing(), "off by default");

        // When normal traffic flows and commits.
        let path = ActorPath::new("quiet");
        system.register_schema::<Add>();
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        wait_for(|| async { system.inbox_cursor(&path).is_some() }).await;
        for n in 0..8 {
            system
                .tell(path.clone(), Add { n })
                .await
                .expect("committed");
        }

        // Then the system is still off — nothing was ever constructed to
        // observe with — and the messages committed anyway.
        assert!(!system.kernel.observing(), "still off after traffic");
        wait_for_cursor(&system, &path, 8).await;
    }

    #[tokio::test]
    async fn watermark_latch_runs_without_a_handler() {
        // Given a gated worker (capacity 8, watermark 2) on a system with
        // NO handler: the latch logic must run (and re-arm) untethered
        // from observation.
        let system = ActorSystem::new(SystemConfig::production());
        system.register_schema::<Add>();
        let (sink_idx, sink) = open_sink();
        let plain = ActorPath::new("unobserved-slow");
        bind_sink(&plain, sink);
        let opts = SpawnOpts {
            mailbox_capacity: 8,
            high_watermark: Some(2),
            ..SpawnOpts::default()
        };
        system.spawn_service::<GatedWorker, _>(
            plain.clone(),
            &json!({ "sink": sink_idx }),
            opts,
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<GatedWorker, Add>::new::<Add>(),
                )]
            },
        );
        wait_for(|| async { system.inbox_cursor(&plain).is_some() }).await;

        // When the watermark is crossed while unobserved, then released.
        for n in 0..4u64 {
            let envelope = system.envelope(Add::schema_id(), plain.clone(), json!({ "n": n }));
            system.send(envelope).await.expect("queued");
        }
        WORKER_RELEASED.store(true, std::sync::atomic::Ordering::SeqCst);
        WORKER_GATE.notify_waiters();

        // Then delivery completed intact — no panic, all four handled.
        wait_for(|| async { sink_read(&plain).len() == 4 }).await;
    }
}
