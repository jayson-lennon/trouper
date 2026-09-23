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
//!
//! ## Wake contract (deadline idle, no polls)
//!
//! There are NO fixed-interval polls anywhere in the runtime. Three wake
//! mechanisms, all push-driven:
//!
//! 1. **Messages**: an accepted delivery wakes the loop directly — the
//!    sender pushes the inbox and fires `cell.work.notify_one()` itself
//!    (the fast path); the front-door task fires the same notify on the
//!    REFUSAL path it lands (Block-held retries, fallback deliveries).
//!    An idle loop parks on `notified()` and wakes instantly.
//! 2. **Duties** (snapshot cadence, passivation): the loop sleeps exactly
//!    until its next due deadline (`next_duty_deadline`, computed from
//!    cell-local reads). No duties armed ⇒ the loop parks forever — zero
//!    wakeups (probe-counted). Under a fake clock (tests) the sleep races
//!    the clock watch, so `advance()` recomputes duties instantly.
//! 3. **Supervision**: a crash stores the cell flag then fires the cell's
//!    `crash_signal` Notify; the engine parks on it (check-then-park) and
//!    reads the flag lock-free. An idle engine acquires nothing.
//!
//! ## Ownership rule (cell-local vs kernel tables)
//!
//! Per-actor bookkeeping that only the actor's own loop (plus its spawn/
//! restart path) writes lives on the [`ActorCell`] as atomics/`RwLock`s:
//! crash flag, watermark mark + latch, last-work stamp, last committed seq
//! (`u64::MAX` = never committed, preserving the seq-0 rule), snapshot
//! anchor (`u64::MAX` = unanchored), snapshot cadence + passivation config
//! (an `RwLock`, not `OnceLock`: the projector-set activation arm may
//! override spawn config before the first idle), dispatch entries, and the
//! declared-emits mirror (spawn-seeded from the manifest, re-synced at the
//! declaration-mutation point — `ActorSystemCore::declare_emits`). The
//! [`EsLoop`] likewise carries its own clone of the live state Arc,
//! captured at spawn and rebound at boot recovery / restart / projector
//! catch-up — the only moments the table's Arc is ever replaced. Step-hot
//! reads (state shell, emit declarations) touch NEITHER global lock.
//! Single-flag reads (supervisor, front door) are lock-free Acquire/Release.
//!
//! The kernel tables keep ONLY cross-actor state whose observations span
//! actors: cells, journal store, live state slots (restart swaps them under
//! the tables lock), specs/failures, replies/asks, projectors, and dead
//! letters. The `caught_up` counter stays kernel-side by contract: the
//! projector cold-wake snapshots it across a re-spawn (which builds a fresh
//! cell), so a cell-local counter would reset to zero and the wake would
//! false-succeed on an incomplete fold.
//!
//! Runtime observation is OPT-IN: the handler slot is a lock-free
//! `ArcSwapOption` shared between the tables and the kernel handle — with
//! no handler installed (the default) no observation is ever constructed,
//! and the message happy path acquires the kernel tables lock ZERO times
//! (the store rides a lock-free Arc handle; a send acquires the tables
//! only to observe when a handler is installed). The emit gates read the
//! cell-local declarations mirror — the registry lock is off the message
//! path too. Probe-counted under `cfg(test)` (`KERNEL_LOCKS`,
//! `CELL_WAKEUPS`).

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use tokio::sync::{Notify, mpsc, watch};

use serde::{Deserialize, Serialize};

use crate::actor::{ActorPath, CommandEntry, DynEsActor, DynServiceActor, MsgEntry};
use crate::context::{CmdCtx, Outbox, RuntimeView};
use crate::envelope::{Address, Envelope, TraceCtx};
use crate::inbox::Inbox;
use crate::journal::{JournalEntry, JournalError};
use crate::json::Json;
use crate::registry::{Endpoint, Registry};
use crate::schema::SchemaId;

/// An envelope the runtime could not deliver or decode.
///
/// Kept inspectable — dropped messages must stay observable, never silently
/// vanish. `drain_dead_letters` hands retained envelopes to the host; the
/// `DeadLettered` observation (opt-in) is the live observation surface.
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

/// An ask lifecycle event, observed when an ask opens and settles.
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
    #[allow(dead_code)] // asserted by tests, projected via ObservationKind in prod
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
    pub(crate) es_state: HashMap<ActorPath, Arc<tokio::sync::Mutex<Box<dyn DynEsActor>>>>,
    /// Live projectors: their step-9 fan-out is suppressed (a projector's
    /// re-records are checkpoint writes, never new facts).
    pub(crate) projectors: HashSet<ActorPath>,
    /// Live service instances (service actors are not journaled).
    pub(crate) services: HashMap<ActorPath, Arc<tokio::sync::Mutex<Box<dyn DynServiceActor>>>>,
    /// Reply-slot leases (the mechanism half of reply addresses).
    pub(crate) replies: crate::reply::ReplyTable,
    /// Ask lifecycle facts (the observation handler consumes these).
    pub(crate) ask_facts: Vec<AskFact>,
    /// Spawn args (genesis rebuild needs them at restart time).
    pub(crate) genesis_args: HashMap<ActorPath, Json>,
    /// Envelopes that could not be delivered or decoded.
    pub(crate) dead_letters: Vec<DeadLetter>,
    /// Supervised children: path → spec.
    pub(crate) specs: HashMap<ActorPath, crate::supervision::ActorSpec>,
    /// Sliding-window failure records: path → window.
    pub(crate) failures: HashMap<ActorPath, crate::supervision::FailureWindow>,
    /// Per-projector caught-up counter: incremented every time a
    /// projector's catch-up completes. The wake path polls this counter
    /// as its completeness signal. KERNEL-SIDE BY CONTRACT:
    /// the wake signal must survive cell teardown (wake_projector
    /// snapshots the count, re-spawns, and waits for it to move PAST —
    /// a cell-local counter would reset to 0 and false-succeed).
    pub(crate) caught_up: HashMap<ActorPath, u64>,
    /// The graceful-shutdown barrier: set by the sweep, read on every
    /// route (deliveries dead-letter with `ShuttingDown`), by partition
    /// activation (refused), by the supervision engines (suspended), and
    /// by passivation (stands down).
    ///
    /// Shared as an `Arc<AtomicBool>`: the send path reads it LOCK-FREE
    /// (the core keeps a clone), never through the kernel lock.
    pub(crate) shutting_down: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Default for KernelState {
    fn default() -> Self {
        Self::new()
    }
}

impl KernelState {
    /// A fresh state: observation off (the handler slot starts `None`).
    pub fn new() -> Self {
        Self {
            cells: HashMap::new(),
            journal_store: Arc::new(crate::journal::InMemoryJournalStore::new()),
            es_state: HashMap::new(),
            services: HashMap::new(),
            replies: crate::reply::ReplyTable::default(),
            ask_facts: Vec::new(),
            projectors: HashSet::new(),
            genesis_args: HashMap::new(),
            dead_letters: Vec::new(),
            specs: HashMap::new(),
            failures: HashMap::new(),
            caught_up: HashMap::new(),
            shutting_down: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}

/// The runtime's shared kernel lock, wrapped with a test-build acquisition
/// counter — the twin of [`crate::system::CountingRegistryLock`].
///
/// Production: a newtype over `Mutex<KernelState>` that derefs exactly like
/// the mutex — zero behavior change, one struct field. Test builds: every
/// `lock()` bumps [`KERNEL_LOCKS`] before handing out the guard, so tests
/// can count critical sections per message window (the cell-local
/// bookkeeping work's mechanism deliverable).
pub(crate) struct CountingKernelLock {
    tables: Mutex<KernelState>,
    /// The shared observation handler slot: `None` = observation off (the
    /// default — no observation is constructed, the load is the message
    /// path's only cost), `Some(handler)` = every observation is handed to
    /// the handler synchronously at the emission site. Panics inside the
    /// handler are isolated from the message path.
    observer: Arc<arc_swap::ArcSwapOption<crate::observe::ObservationHandler>>,
    /// A clone of the state's journal-store `Arc`: the append path clones
    /// the store without acquiring the tables (system-wide, set once in
    /// the core's assembly block before the lock is ever shared).
    store: Arc<dyn crate::journal::JournalStore>,
}

impl CountingKernelLock {
    /// Acquires the kernel tables, counting the critical section in test
    /// builds.
    #[inline]
    pub(crate) fn lock(&self) -> parking_lot::MutexGuard<'_, KernelState> {
        #[cfg(test)]
        bump_kernel_locks();
        self.tables.lock()
    }

    /// Whether a handler is installed — the observation sites' cheap gate:
    /// a lock-free load, and nothing is constructed when it is `false`.
    #[inline]
    pub(crate) fn observing(&self) -> bool {
        self.observer.load().is_some()
    }

    /// Hands one observation to the installed handler (if any).
    ///
    /// Panics inside the handler are isolated: observation must never take
    /// down the message path. Handlers must not call back into the system
    /// (see the [`crate::observe`] module docs).
    pub(crate) fn observe(&self, observation: crate::observe::Observation) {
        if let Some(handler) = self.observer.load().as_ref() {
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| handler(&observation)));
            if outcome.is_err() {
                tracing::error!("observation handler panicked; message path continued");
            }
        }
    }

    /// Replaces the observation handler at runtime (`None` = off). Takes
    /// effect at the next emission site — in-flight handler calls finish.
    pub(crate) fn set_observer(&self, handler: Option<crate::observe::ObservationHandler>) {
        self.observer.store(handler.map(Arc::new));
    }

    /// The journal store handle, cloned WITHOUT acquiring the tables.
    pub(crate) fn journal_store(&self) -> Arc<dyn crate::journal::JournalStore> {
        self.store.clone()
    }

    /// Installs the initial handler at wrap time (the config-time
    /// constructor path).
    pub(crate) fn with_state_and_observer(
        tables: KernelState,
        handler: Option<crate::observe::ObservationHandler>,
    ) -> Self {
        let observer = Arc::new(arc_swap::ArcSwapOption::new(handler.map(Arc::new)));
        let store = tables.journal_store.clone();
        Self {
            tables: Mutex::new(tables),
            observer,
            store,
        }
    }
}

impl std::ops::Deref for CountingKernelLock {
    type Target = Mutex<KernelState>;
    fn deref(&self) -> &Self::Target {
        &self.tables
    }
}

impl std::ops::DerefMut for CountingKernelLock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tables
    }
}

// ---------------------------------------------------------------------------
// TEST PROBES — mechanism counters, compiled ONLY under `cfg(test)`.
//
// The Arc-payload / lock-relief work's deliverable IS the mechanism (fewer
// deep payload-tree copies, fewer registry acquisitions, no manifest clones
// on the hot path) — allocation and lock behavior no behavioral test can
// observe. These counters are the designed test seam for it. They live on
// plain statics; counters NEVER reset inside the runtime, so tests take
// before/after deltas within their own window (concurrency-safe by
// construction under nextest's one-process-per-test isolation; under plain
// `cargo test` the counters are still correct, only shared — the mechanism
// tests use decisive bounds that tolerate cross-test noise).
// ---------------------------------------------------------------------------

/// Deep payload-tree copies (test builds only). Bumped by `Json::clone`
/// and by `Json::decode`'s owned-value bridge.
#[cfg(test)]
pub(crate) fn bump_deep_clones() {
    DEEP_CLONES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static DEEP_CLONES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The number of deep payload-tree clones in a test window (sync bodies:
/// pure closures like `Json::decode` — async windows read the atomics
/// inline instead).
#[cfg(test)]
pub(crate) fn deep_clones_in_window<T>(f: impl FnOnce() -> T) -> (T, u64) {
    let before = DEEP_CLONES.load(std::sync::atomic::Ordering::Relaxed);
    let out = f();
    let after = DEEP_CLONES.load(std::sync::atomic::Ordering::Relaxed);
    (out, after - before)
}

/// Live `Registry` critical sections (test builds only). The counting
/// mutex wraps every registry the runtime builds.
#[cfg(test)]
pub(crate) fn bump_registry_locks() {
    REGISTRY_LOCKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static REGISTRY_LOCKS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Live `KernelState` critical sections (test builds only). Counted where
/// the kernel guard is handed out, mirroring [`REGISTRY_LOCKS`] — the
/// cell-local bookkeeping work's mechanism deliverable (fewer kernel
/// acquisitions per message) is a lock count no behavioral test observes.
#[cfg(test)]
pub(crate) fn bump_kernel_locks() {
    KERNEL_LOCKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static KERNEL_LOCKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// ES/service loop wakeups that are NOT message deliveries (test builds
/// only): every iteration that completes the idle select's timer arm.
/// The deliverable "an idle actor with no duties never wakes" is
/// probe-observable through this counter.
#[cfg(test)]
pub(crate) fn bump_cell_wakeups() {
    CELL_WAKEUPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static CELL_WAKEUPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// `ActorManifest` clones (test builds only). Bumped by the derive's
/// hand-written `Clone` impl in schema.rs.
#[cfg(test)]
pub(crate) fn bump_manifest_clones() {
    MANIFEST_CLONES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static MANIFEST_CLONES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn bump_serde_calls() {
    SERDE_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static SERDE_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Hand-written `Schema` impls' first `schema_id()` per type (test builds
/// only) — the one allocation the fallback cache ever pays; the derive's
/// static-name arm never lands here.
#[cfg(test)]
pub(crate) fn bump_schema_id_cache_misses() {
    SCHEMA_ID_CACHE_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static SCHEMA_ID_CACHE_MISSES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// `SchemaId` clones on the Static arm (test builds only) — copies, per
/// the two-arm id deliverable; the counter proves the hot path's clones
/// land there.
#[cfg(test)]
pub(crate) fn bump_static_schema_clones() {
    STATIC_SCHEMA_CLONES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static STATIC_SCHEMA_CLONES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Uuid-shaped id generations that skipped `getrandom` (test builds only)
/// — the counter the trace-id deliverable ("zero getrandom on the tell
/// path") reads.
#[cfg(test)]
pub(crate) fn bump_getrandom_skipped() {
    GETRANDOM_SKIPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static GETRANDOM_SKIPPED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Real clock reads consumed by the id generator (test builds only).
#[cfg(test)]
pub(crate) fn bump_id_clock_reads() {
    ID_CLOCK_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static ID_CLOCK_READS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Service-step handler dispatches run ON the actor loop task (test builds
/// only) — pairs with [`TASK_SPAWNS`]: a step that dispatches inline bumps
/// this instead of spawning.
#[cfg(test)]
pub(crate) fn bump_inline_service_dispatch() {
    INLINE_SERVICE_DISPATCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static INLINE_SERVICE_DISPATCH: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// `tokio::spawn` calls anywhere in the runtime (test builds only) — the
/// per-message spawn deliverable reads the delta across service steps.
/// Runtime spawn sites go through [`spawn_tracked`]; test-only spawns
/// (clock fakes, ui harnesses) do not.
#[cfg(test)]
pub(crate) fn bump_task_spawns() {
    TASK_SPAWNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static TASK_SPAWNS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The runtime's spawn funnel: every task the RUNTIME starts passes here
/// (production it is a bare spawn; test builds it also counts). The
/// service-step deliverable is "zero of these per message".
pub(crate) fn spawn_tracked<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[cfg(test)]
    bump_task_spawns();
    tokio::spawn(fut)
}

/// Kernel-facing handle for one running actor loop.
pub(crate) struct ActorHandle {
    /// The kill switch: signaled on graceful stop.
    pub(crate) shutdown: watch::Sender<bool>,
    /// The task join handle; aborted on hard remove.
    pub(crate) task: Option<tokio::task::JoinHandle<()>>,
}

/// Everything the runtime owns for one actor across restarts.
///
/// OWNERSHIP RULE (cell-local bookkeeping): every field below is state
/// exactly one loop task reads and writes, or spawn-static config — so it
/// lives HERE, not in the kernel tables. The kernel lock guards cross-actor
/// tables only (cells, specs, failures, state shells, dead letters, the
/// reply table, the caught-up wake counters); a message path never takes it
/// for per-actor bookkeeping.
pub(crate) struct ActorCell {
    /// The actor's path (its identity).
    pub(crate) path: ActorPath,
    /// The runtime-owned inbox (survives endpoint swaps). The guard is a
    /// SYNC lock (parking_lot): every critical section is a synchronous
    /// body — push, peek, commit, close — never held across an await
    /// (audited site-by-site; the guard scopes are minimal by
    /// construction). The async machinery (a semaphore wake per message)
    /// bought nothing here.
    pub(crate) inbox: parking_lot::Mutex<Inbox>,
    /// The spawn-configured mailbox capacity. The front door and every
    /// restart derive their channel depth from it (D4: restarts keep the
    /// spawn's capacity, not a default).
    pub(crate) mailbox_capacity: usize,
    /// The spawn-configured inbox overload policy (drives the front-door
    /// channel depth and the Block hold-retry).
    pub(crate) mailbox_policy: crate::inbox::OverloadPolicy,
    /// The spawn-configured step batch size: how many queued messages one
    /// wake's step may drain and commit (spawn-static; read by the loop
    /// each step without a lock).
    pub(crate) step_batch: usize,
    /// Whether this actor declared a high-watermark at spawn. When false,
    /// the front door skips its watermark check entirely — the enqueue
    /// path never takes the kernel lock for it (D: the lock-free fast
    /// path; most actors declare no watermark).
    pub(crate) has_watermark: std::sync::atomic::AtomicBool,
    /// The running loop's handle, when a task is live.
    pub(crate) handle: tokio::sync::Mutex<Option<ActorHandle>>,
    /// Wakes the actor loop when work arrives (the message path is
    /// push-driven: the front door fires this on every accepted push; the
    /// loop never polls for mail).
    pub(crate) work: Arc<Notify>,
    /// Whether the actor's `on_stop` hook has run. Claimed by whichever
    /// caller reaches the graceful exit FIRST (the loop's self-stop/
    /// passivation exit, or the external stop after joining the task) —
    /// the hook runs exactly once per actor lifetime, never twice.
    pub(crate) on_stop_done: std::sync::atomic::AtomicBool,
    // --- CELL-LOCAL BOOKKEEPING (single-writer: the actor's own loop) ---
    /// Whether this actor's loop died to a panic (or its service `start`
    /// failed) and it is awaiting supervision. Written by the loop's
    /// crash paths, read lock-free by the supervision engine.
    pub(crate) crashed: std::sync::atomic::AtomicBool,
    /// Wakes the supervision engine when `crashed` flips true (the
    /// signal-driven replacement for the 5ms poll).
    pub(crate) crash_signal: Arc<Notify>,
    /// Backpressure watermark: the inbox depth that fires the
    /// `Backpressured` fact (spawn-static once set; 0 = none — checked
    /// only when `has_watermark` reads true).
    pub(crate) watermark_high: std::sync::atomic::AtomicU64,
    /// The watermark latch: `true` from the up-crossing until the depth
    /// falls back to/below the mark, so a sustained overload produces one
    /// fact, not one per message. Written by BOTH delivery writers (the
    /// direct-delivery fast path under the inbox guard, and the front
    /// door's fallback path); the worst race at a crossing is one
    /// suppressed or one extra `Backpressured` fact — never a lost
    /// message.
    pub(crate) watermark_fired: std::sync::atomic::AtomicBool,
    /// Injected-clock millis of the last COMPLETED message step (only a
    /// committed step resets the passivation timer). Written by the loop
    /// on `Step::Work`; the SPAWN stamps it as the idle window's start
    /// (an actor spawned and never messaged passivates from birth).
    pub(crate) last_work_ms: std::sync::atomic::AtomicU64,
    /// This ES actor's last COMMITTED event seq, updated at the ack point.
    /// `u64::MAX` = never committed (NOT 0 — seq 0 is a legitimate
    /// committed value, and the idle snapshot rule refuses to snapshot an
    /// actor whose first post-snapshot seq would skip event 0).
    pub(crate) last_event_seq: std::sync::atomic::AtomicU64,
    /// The time-cadence snapshot anchor (injected-clock millis of the last
    /// completed time snapshot). `u64::MAX` = unanchored — an unanchored
    /// cadence is never due (safe default, preserved verbatim).
    pub(crate) snapshot_anchor_ms: std::sync::atomic::AtomicU64,
    /// The spawn-declared snapshot cadence (ES actors; Off by default).
    /// Written at spawn (and by a projector-set activation's override),
    /// read by the step-10 and idle checks.
    pub(crate) snapshot_policy: std::sync::RwLock<crate::actor::SnapshotCadence>,
    /// The spawn-declared passivation window, if any. Written at spawn
    /// (a partition re-spawn re-derives it from the factory's builder;
    /// a projector-set activation overrides it from its spec).
    pub(crate) passivation: std::sync::RwLock<Option<crate::system::Passivation>>,
    /// The spawn-declared command entries (ES tier): what this actor can
    /// decode. Written at spawn and re-attached at restart; read per
    /// message (the read side is the hot side).
    pub(crate) entries: std::sync::RwLock<Vec<Arc<dyn CommandEntry>>>,
    /// The spawn-declared message entries (service tier).
    pub(crate) msg_entries: std::sync::RwLock<Vec<Arc<dyn MsgEntry>>>,
    /// The declared emit schemas, mirrored from the registry manifest at
    /// spawn and kept in sync at the single declaration-mutation point —
    /// so the step's emit gates read HERE, not through the registry lock
    /// (the registry keeps its own copy for export/manifests). Std RwLock
    /// like the other spawn-static cell config; the read side is the hot
    /// side.
    pub(crate) declared_emits: std::sync::RwLock<Vec<crate::schema::SchemaId>>,
}

/// Sentinel for "never"/"unanchored" in the cell's millis/seq atomics —
/// never a legitimate value (real timestamps are epoch millis; real seqs
/// start at 0, and 0 is a legitimate committed seq).
pub(crate) const CELL_SENTINEL: u64 = u64::MAX;

impl ActorCell {
    /// Creates a cell with a fresh inbox; the endpoint arrives on start.
    /// Cell-local bookkeeping starts at its defaults (never worked, never
    /// committed, unanchored, not crashed, no watermark); the spawn site
    /// stamps the idle window, cadence, and passivation config. `batch` is
    /// the spawn's step drain size (clamped to at least 1).
    pub fn new(
        path: ActorPath,
        inbox: Inbox,
        mailbox_capacity: usize,
        mailbox_policy: crate::inbox::OverloadPolicy,
        batch: usize,
    ) -> Self {
        Self {
            path,
            inbox: parking_lot::Mutex::new(inbox),
            mailbox_capacity,
            mailbox_policy,
            step_batch: batch.max(1),
            has_watermark: std::sync::atomic::AtomicBool::new(false),
            handle: tokio::sync::Mutex::new(None),
            work: Arc::new(Notify::new()),
            on_stop_done: std::sync::atomic::AtomicBool::new(false),
            crashed: std::sync::atomic::AtomicBool::new(false),
            crash_signal: Arc::new(Notify::new()),
            watermark_high: std::sync::atomic::AtomicU64::new(0),
            watermark_fired: std::sync::atomic::AtomicBool::new(false),
            last_work_ms: std::sync::atomic::AtomicU64::new(0),
            last_event_seq: std::sync::atomic::AtomicU64::new(CELL_SENTINEL),
            snapshot_anchor_ms: std::sync::atomic::AtomicU64::new(CELL_SENTINEL),
            snapshot_policy: std::sync::RwLock::new(crate::actor::SnapshotCadence::Off),
            passivation: std::sync::RwLock::new(None),
            entries: std::sync::RwLock::new(Vec::new()),
            msg_entries: std::sync::RwLock::new(Vec::new()),
            declared_emits: std::sync::RwLock::new(Vec::new()),
        }
    }

    /// Claims the right to run `on_stop`: `true` = this caller runs the
    /// hook; `false` = someone else already ran it.
    pub(crate) fn claim_on_stop(&self) -> bool {
        !self
            .on_stop_done
            .swap(true, std::sync::atomic::Ordering::SeqCst)
    }

    /// Marks the actor crashed and wakes its supervision engine (the
    /// crash signal; engines read the flag lock-free). Idempotent.
    pub(crate) fn mark_crashed(&self) {
        self.crashed
            .store(true, std::sync::atomic::Ordering::Release);
        self.crash_signal.notify_waiters();
    }

    /// Clears the crash flag: a restarted actor is not crashed (the
    /// supervision engine calls this after a successful restart, so the
    /// next wait observes a NEW crash, not the handled one).
    pub(crate) fn clear_crashed(&self) {
        self.crashed
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Whether the actor is crashed (lock-free read; Acquire pairs with
    /// `mark_crashed`'s Release).
    pub(crate) fn is_crashed(&self) -> bool {
        self.crashed.load(std::sync::atomic::Ordering::Acquire)
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
    pub(crate) registry: Arc<crate::system::CountingRegistryLock>,
    /// The shared actor tables.
    pub(crate) kernel: Arc<CountingKernelLock>,
    /// Lock-free clone of the shutdown barrier (the send-path check).
    pub(crate) shutting_down: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// The read-only view handed to handler contexts.
    pub(crate) view: Arc<dyn RuntimeView>,
    /// The injected clock (lease expiries, deterministic tests).
    pub(crate) clock: crate::clock::ClockService,
    /// Whether this actor is in the kernel's projector set (read once at
    /// spawn — the set only changes at spawn/teardown, never mid-step).
    /// Lets step 9 skip the kernel lock.
    pub(crate) is_projector: bool,
    /// The loop's live state shell, captured at spawn and cloned out per
    /// step — the tables lookup leaves the message path. `None` only for
    /// loops that never step ES (the service tier wraps one of these for
    /// its shared plumbing). Rebound on boot recovery and restart, the
    /// only two moments the table's Arc is replaced.
    pub(crate) state: Option<Arc<tokio::sync::Mutex<Box<dyn DynEsActor>>>>,
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
/// when the destination does not resolve. `shutting_down` is the shared
/// shutdown barrier, read LOCK-FREE (never through the kernel lock).
pub(crate) async fn route(
    registry: &crate::system::CountingRegistryLock,
    kernel: &CountingKernelLock,
    shutting_down: &std::sync::atomic::AtomicBool,
    envelope: Envelope,
) -> Result<ActorPath, Envelope> {
    route_inner(registry, kernel, shutting_down, envelope).await
}

async fn route_inner(
    registry: &crate::system::CountingRegistryLock,
    kernel: &CountingKernelLock,
    shutting_down: &std::sync::atomic::AtomicBool,
    envelope: Envelope,
) -> Result<ActorPath, Envelope> {
    // SHUTDOWN BARRIER: once the sweep starts, nothing new is accepted —
    // every delivery dead-letters with `ShuttingDown` (observably, never
    // silently). A lock-free atomic read: the hot send path never
    // touches the kernel lock for this. Activation is refused inside
    // resolve_partition.
    if shutting_down.load(std::sync::atomic::Ordering::SeqCst) {
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
            let (delivery, primary_dest, tee_endpoint, set_specs, fast_endpoint) = {
                // ONE critical section for the send's whole registry read:
                // the rules decision, the tee copy's endpoint resolve, and
                // BOTH set-table probes (a plain path pays one lock to
                // learn it is neither partition nor projector set). A
                // PLAIN destination resolves its endpoint in the same
                // pass — the probes just proved it needs no set
                // resolution, so the second lock is gone from the hot
                // path. (A set destination resolves after
                // resolve_partition, which may mutate the path.)
                let reg = registry.lock();
                let (delivery, primary_dest) = apply_rules(&reg, &envelope, path.clone());
                let tee_endpoint = delivery
                    .as_ref()
                    .and_then(|(_, tee_dest, _)| reg.resolve(tee_dest));
                let set_specs = (
                    reg.partitions.get(path).cloned(),
                    reg.projector_sets
                        .get(path)
                        .cloned()
                        .or_else(|| reg.projector_set_owning(path)),
                );
                let fast_endpoint = if set_specs.0.is_none() && set_specs.1.is_none() {
                    primary_dest.as_ref().or(Some(path)).and_then(|p| reg.resolve(p))
                } else {
                    None
                };
                (delivery, primary_dest, tee_endpoint, set_specs, fast_endpoint)
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
                let endpoint = tee_endpoint;
                if let Some(endpoint) = endpoint {
                    let _ = deliver_with_retry(&endpoint, copy).await;
                    if kernel.observing() {
                        kernel.observe(crate::observe::Observation::new(
                            origin_trace.causality_id.as_millis_ts(),
                            crate::observe::ObservationKind::Sent {
                                from: envelope.from.clone(),
                                dest: Address::Path(tee_dest.clone()),
                                schema: envelope.schema.clone(),
                                trace: origin_trace,
                            },
                        ));
                    }
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
            // PARTITION / PROJECTOR SETS: a public set path resolves to
            // ONE entity/projector, derived from the payload's shard key
            // (activated on demand); the specs were pre-read above (one
            // lock for the send's whole registry read).
            let path = match resolve_partition(
                registry,
                kernel,
                shutting_down,
                &envelope,
                path.clone(),
                set_specs.0.clone(),
                set_specs.1.clone(),
            )
            .await
            {
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
            let endpoint = match fast_endpoint {
                // PLAIN PATH: resolved in the first critical section (the
                // probes proved no set applies; `path` cannot mutate).
                resolved @ Some(_) => resolved,
                // SET PATH (or an unresolvable plain path): resolve AFTER
                // resolve_partition — a partition/projector set may
                // activate an entity and point `path` at it.
                None => {
                    let registry = registry.lock();
                    registry.resolve(&path)
                }
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
            //
            // DIRECT DELIVERY first: push the inbox and wake the loop
            // with no front-door hop. A refusal (full, closed, or the
            // closed inbox of a mid-restart slot) comes back BY VALUE
            // (never a clone) and takes the front-door channel, which
            // owns every refusal behavior — including the Closed error
            // that drives the re-activation retry below. Evictions
            // (DropOld) never reach the fallback: the push queued the
            // envelope already, so only the evicted victim dead-letters
            // (inside `direct_push`).
            let mut envelope = envelope;
            // The Sent fact's identity parts, captured before delivery
            // consumes the envelope (the direct path moves it into the
            // inbox). TraceCtx is Copy; the from/schema clones are
            // shallow (schema id + option).
            let (sent_from, sent_schema, sent_trace) = (
                envelope.from.clone(),
                envelope.schema.clone(),
                envelope.trace,
            );
            if let Err(refused) = direct_push(&endpoint, envelope, kernel, sent_trace).await {
                envelope = refused;
                if deliver_with_retry(&endpoint, envelope.clone())
                    .await
                    .is_err()
                {
                    let retry_path = match resolve_partition(
                        registry,
                        kernel,
                        shutting_down,
                        &envelope,
                        path.clone(),
                        set_specs.0,
                        set_specs.1,
                    )
                    .await
                    {
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
            }
            if kernel.observing() {
                kernel.observe(crate::observe::Observation::new(
                    sent_trace.causality_id.as_millis_ts(),
                    crate::observe::ObservationKind::Sent {
                        from: sent_from,
                        dest: Address::Path(path.clone()),
                        schema: sent_schema,
                        trace: sent_trace,
                    },
                ));
            }
            Ok(path)
        }
        Address::Slot(_) => Err(envelope), // reply routing: ctx only
        Address::Schema(ref schema) => {
            // Schema-addressed send: the route table picks the handler
            // (rotating when several actors handle the same schema).
            let (target, endpoint) = {
                let mut registry = registry.lock();
                registry.route_resolved(schema)
            };
            let Some(target) = target else {
                return Err(envelope);
            };
            let Some(endpoint) = endpoint else {
                return Err(envelope);
            };
            deliver_with_retry(&endpoint, envelope.clone()).await?;
            if kernel.observing() {
                kernel.observe(crate::observe::Observation::new(
                    envelope.trace.causality_id.as_millis_ts(),
                    crate::observe::ObservationKind::Sent {
                        from: envelope.from.clone(),
                        dest: Address::Schema(schema.clone()),
                        schema: schema.clone(),
                        trace: envelope.trace,
                    },
                ));
            }
            Ok(target)
        }
    }
}

/// Resolves a partition-set destination to its entity path.
///
/// `Ok(None)` = the dest is not a partition set (fall through). `Ok(Some)`
/// = the entity path (activated on demand if absent). `Err(envelope)` =
/// the command lacked its shard key — dead-lettered, never activated.
///
/// Determinism is structural: the entity path is `public/key`, so the same
/// key always reaches the same entity and journal. Activation is
/// check-then-insert under the registry lock: the loser of a concurrent
/// same-key race delivers to the winner's entity.
async fn resolve_partition(
    registry: &crate::system::CountingRegistryLock,
    kernel: &CountingKernelLock,
    shutting_down: &std::sync::atomic::AtomicBool,
    envelope: &Envelope,
    dest: ActorPath,
    spec: Option<crate::pool::PartitionSpec>,
    projector_spec: Option<crate::pool::ProjectorSetSpec>,
) -> Result<Option<ActorPath>, Envelope> {
    let Some(spec) = spec else {
        // Not an entity partition: fall through to the projector-set
        // check (a path belongs to at most one set — both tables are
        // keyed by public path; the caller pre-read BOTH in one lock).
        return resolve_projector_set(registry, kernel, shutting_down, envelope, projector_spec)
            .await;
    };
    let key = match extract_key(registry, envelope, &spec.key_field) {
        Some(key) => key,
        None => return Err(envelope.clone()),
    };
    // Determinism is structural: same key → same derived path.
    let entity_path = ActorPath::new(format!("{}/{}", dest, key).as_str());
    // Fast path: the entity is already live.
    if registry.lock().lookup(&entity_path).is_some() {
        return Ok(Some(entity_path));
    }
    // The sweep disables activation: nothing new may start mid-shutdown
    // (a lock-free read).
    if shutting_down.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(envelope.clone());
    } // ACTIVATE: spawn the entity from the shared factory. The factory's
    // spawn registers the entity's slot; a concurrent same-key send is
    // serialized by the registry lock inside the spawn, and the loser of
    // a race delivers to the winner's entity (same derived path).
    (spec.factory)(&spec.system, &entity_path, &spec.entity_args(&key));
    Ok(Some(entity_path))
}

/// Schema-aware shard-key extraction from an envelope's payload. A live
/// value answers from the derive-generated `field()` match — zero serde,
/// no JSON view built; the registry/Json fallback serves replayed/erased
/// payloads (Bytes) whose live value no longer exists.
fn extract_key(
    registry: &crate::system::CountingRegistryLock,
    envelope: &Envelope,
    key_field: &str,
) -> Option<String> {
    // LIVE VALUE FIRST: the derive's field() read (the shard-key field is
    // declared, so every live value answers it). A JSON-view payload
    // answers through the view directly — also no new materialization.
    if envelope
        .payload
        .field(key_field)
        .is_some_and(|key| !key.is_empty())
    {
        return envelope.payload.field(key_field);
    }
    // FALLBACK (the door): bytes/replay payloads decode for the read —
    // and live values whose key is genuinely absent fall through too,
    // preserving the registry-def contract (missing key → None → DLQ).
    let reg = registry.lock();
    let payload = envelope.payload_json();
    match reg.schema(&envelope.schema) {
        Some(def) => crate::pool::extract_shard_key(def, key_field, payload),
        None => payload
            .get(key_field)
            .and_then(|v| v.as_str().map(str::to_owned)),
    }
}

/// The projector-set arm of partition resolution: identical derive →
/// activate-on-demand → deliver core, over the projector-set table.
///
/// Only projector sets participate (an entity `PartitionSpec` never
/// declared consumption, so a broadcast copy aimed at it is not a
/// delivery obligation — the activation rule is declaration-scoped).
async fn resolve_projector_set(
    registry: &crate::system::CountingRegistryLock,
    kernel: &CountingKernelLock,
    shutting_down: &std::sync::atomic::AtomicBool,
    envelope: &Envelope,
    spec: Option<crate::pool::ProjectorSetSpec>,
) -> Result<Option<ActorPath>, Envelope> {
    // The spec arrives pre-read by the caller (one lock for both set
    // tables). The set is addressed either by its public path (a direct
    // send) or by an already-derived per-key path (the broadcast set-arm
    // derives `public/key` before routing); the key comes from the
    // payload either way.
    let Some(spec) = spec else {
        return Ok(None);
    };
    let Some(key) = extract_key(registry, envelope, &spec.key_field) else {
        return Err(envelope.clone());
    };
    let projector_path = ActorPath::new(format!("{}/{}", spec.public, key).as_str());
    if registry.lock().lookup(&projector_path).is_some() {
        return Ok(Some(projector_path));
    }
    if shutting_down.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(envelope.clone());
    }
    (spec.factory)(&spec.system, &projector_path, &spec.entity_args(&key));
    // The set owns lifecycle policy for its activated projectors:
    // passivation and snapshot cadence ride the spec — written to the
    // CELL (the policy's home now). (A projector's builder has no
    // passivate_after — a standalone passivated projector has no wake
    // path — so per-key passivation can only come from here.) Applied
    // AFTER the factory returns: the arm is synchronous, so the config is
    // in place before the loop's first idle check. The factory's arm
    // seeded the cell; the set's spec OVERRIDES it.
    {
        let cell = kernel.lock().cells.get(&projector_path).cloned();
        if let Some(cell) = cell {
            *cell.passivation.write().expect("passivation lock") = spec.opts.passivation;
            *cell.snapshot_policy.write().expect("snapshot policy lock") = spec.opts.snapshot;
        }
    }
    Ok(Some(projector_path))
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
                // The copy SHARES the payload (Arc bump), never a copy
                // of the value — every payload is shareable now.
                let copy = Envelope::shared_from(
                    envelope.schema.clone(),
                    Address::Path(observer.clone()),
                    envelope,
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
    kernel: &CountingKernelLock,
    envelope: &Envelope,
    reason: crate::kernel::DeadLetterReason,
    detail: &str,
) {
    kernel.lock().dead_letters.push(DeadLetter {
        schema: envelope.schema.clone(),
        dest: envelope.dest.clone(),
        reason: reason.clone(),
        detail: detail.to_owned(),
        trace: envelope.trace,
        envelope: envelope.clone(),
    });
    // The DLQ push above is the DATA path (retained payloads, unconditional);
    // only the observation is gated on a handler being installed.
    if kernel.observing() {
        kernel.observe(crate::observe::Observation::new(
            envelope.trace.causality_id.as_millis_ts(),
            crate::observe::ObservationKind::DeadLettered {
                dest: envelope.dest.clone(),
                schema: envelope.schema.clone(),
                reason,
                trace: envelope.trace,
            },
        ));
    }
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

/// The direct-delivery fast path: push the destination's inbox under its
/// own lock, check the backpressure watermark under the SAME guard, and
/// fire its work notify — no front-door task hop.
///
/// A refusal hands the envelope BACK to the caller, which sends it through
/// the front-door channel: the door task owns every refusal behavior
/// (Block holds, dead letters, its own wake), so the fast path never
/// re-implements policy. The one carve-out is a DropOld eviction: the push
/// has ALREADY queued the new envelope, so the delivery completes here —
/// falling back would deliver it twice — and the evicted envelope is
/// dead-lettered with the same reason and detail the door uses (outside
/// the inbox guard; inbox→kernel is the runtime's lock order).
///
/// WATERMARK: the fire-once latch check runs while still holding the
/// inbox guard, so a push and its depth observation cannot interleave an
/// ack between them. The latch is multi-writer now (this path AND the
/// door) but the atomics were always written from multiple tasks; the
/// worst race at a crossing is one suppressed or one extra `Backpressured`
/// fact, never a lost message.
async fn direct_push(
    endpoint: &Endpoint,
    envelope: Envelope,
    kernel: &CountingKernelLock,
    trace: crate::envelope::TraceCtx,
) -> Result<(), Envelope> {
    use std::sync::atomic::Ordering;
    let cell = &endpoint.cell;
    // The push and the recovery are clone-free: `push` moves the envelope
    // in and a refusal moves it back out by value. `queued` covers both
    // accepted outcomes (plain Ok and DropOld's queued-anyway eviction) —
    // the watermark depth includes the new envelope either way.
    let (evicted, fire_watermark): (Option<Envelope>, Option<u64>) = {
        let mut inbox = cell.inbox.lock();
        let evicted = match inbox.push(envelope) {
            Ok(_) => None,
            Err(refused) if refused.queued_anyway() => Some(refused.into_envelope()),
            Err(refused) => return Err(refused.into_envelope()),
        };
        // LATCH CHECK (same guard): fire on the up-crossing, re-arm at or
        // below the mark — identical semantics to the door's check.
        let fire = if cell.has_watermark.load(Ordering::SeqCst) {
            let depth = inbox.len() as u64;
            let mark = cell.watermark_high.load(Ordering::Acquire);
            if depth > mark && !cell.watermark_fired.swap(true, Ordering::AcqRel) {
                Some(depth)
            } else {
                if depth <= mark {
                    cell.watermark_fired.store(false, Ordering::Release);
                }
                None
            }
        } else {
            None
        };
        (evicted, fire)
    };
    if let Some(depth) = fire_watermark
        && kernel.observing()
    {
        kernel.observe(crate::observe::Observation::new(
            trace.causality_id.as_millis_ts(),
            crate::observe::ObservationKind::Backpressured {
                path: cell.path.clone(),
                depth,
            },
        ));
    }
    if let Some(evicted) = evicted {
        dead_letter(
            kernel,
            &evicted,
            crate::kernel::DeadLetterReason::InboxRefused,
            "inbox evicted oldest (DropOld)",
        );
    }
    // The wake fires AFTER the guard drops (a minimal critical section);
    // the loop's own push/ack path re-reads the inbox under the lock.
    cell.work.notify_one();
    Ok(())
}

/// The front-door task: drains the mpsc into the runtime-owned inbox.
///
/// The REFUSAL path only: accepted deliveries push the destination's
/// inbox directly from the sender (`direct_push`) and never enter the
/// channel. What lands here is what the fast path refused — a full or
/// closed inbox — and the door owns every refusal behavior: Block holds
/// (retry until an ack frees room), dead letters for DropNew/DropOld and
/// closed inboxes, and the fallback wake + watermark check for the
/// delivery it lands.
pub(crate) async fn front_door_loop(
    cell: Arc<ActorCell>,
    kernel: Arc<CountingKernelLock>,
    mut rx: mpsc::Receiver<Envelope>,
) {
    while let Some(envelope) = rx.recv().await {
        let accepted = push_holding_block(&cell, &kernel, envelope.clone()).await;
        // WATERMARK CHECK (rate-limited): fires on the UP-crossing only;
        // the latch re-arms when the depth falls back to/below the mark.
        // Actors with no declared watermark (the common case) skip the
        // whole block — no depth read, no lock of any kind (the cell-local
        // flag answers "declared?"). The latch and the mark are CELL-LOCAL
        // now (single writer: this front door), so the enqueue path never
        // takes the kernel lock — the Backpressured observation rides the
        // handler slot.
        if cell.has_watermark.load(std::sync::atomic::Ordering::SeqCst) {
            let depth = cell.inbox.lock().len() as u64;
            let wm = cell
                .watermark_high
                .load(std::sync::atomic::Ordering::Acquire);
            if depth > wm
                && !cell
                    .watermark_fired
                    .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                if kernel.observing() {
                    kernel.observe(crate::observe::Observation::new(
                        envelope.trace.causality_id.as_millis_ts(),
                        crate::observe::ObservationKind::Backpressured {
                            path: cell.path.clone(),
                            depth,
                        },
                    ));
                }
            } else if depth <= wm {
                cell.watermark_fired
                    .store(false, std::sync::atomic::Ordering::Release);
            }
        }
        if accepted {
            cell.work.notify_one();
        }
    }
}

/// How long the front door holds a Block-refused envelope between retry
/// pushes, and how many retries before dead-lettering as a last resort.
const BLOCK_HOLD_RETRY_MS: u64 = 2;
const BLOCK_HOLD_RETRIES: usize = 1_000;

/// Pushes one envelope into the actor's inbox, honoring the Block policy.
///
/// A `Refused::Full` under Block is HELD and retried — the sender was
/// already paced by the channel await, so a refusal here is the in-flight
/// race between the channel accept and the inbox filling (or a restart
/// swap), and dead-lettering it would make Block lossy. The hold ends
/// when a push succeeds (an ack freed room), or — after the retry budget —
/// dead-letters as a last resort. Closed inboxes and DropNew/DropOld
/// refusals dead-letter immediately, exactly as before.
///
/// ORDERING CAVEAT (direct delivery): a Block-refused message held HERE
/// can be overtaken by the SAME sender's next message — after the hold
/// ends, the sender's subsequent sends push the inbox directly and
/// land while msg N still waits for room. Per-sender FIFO holds only in
/// the no-refusal regime; cross-producer ordering was never guaranteed.
async fn push_holding_block(
    cell: &ActorCell,
    kernel: &CountingKernelLock,
    envelope: Envelope,
) -> bool {
    let mut attempt = 0usize;
    loop {
        let refusal = {
            let mut inbox = cell.inbox.lock();
            match inbox.push(envelope.clone()) {
                Ok(_) => return true,
                Err(refused) => refused,
            }
        };
        let holds = matches!(refusal, crate::inbox::Refused::Full(_))
            && cell.mailbox_policy == crate::inbox::OverloadPolicy::Block;
        if !holds || attempt >= BLOCK_HOLD_RETRIES {
            let detail = if refusal.queued_anyway() {
                "inbox evicted oldest (DropOld)"
            } else {
                "inbox refused (overload/closed)"
            };
            let evicted = refusal.into_envelope();
            dead_letter(
                kernel,
                &evicted,
                crate::kernel::DeadLetterReason::InboxRefused,
                detail,
            );
            return false;
        }
        attempt += 1;
        tokio::time::sleep(std::time::Duration::from_millis(BLOCK_HOLD_RETRY_MS)).await;
    }
}

/// The idler: everything one loop's idle tail needs. Cloned per loop.
#[derive(Clone)]
struct Idler {
    clock: crate::clock::ClockService,
}

impl Idler {
    /// The next DUE DUTY TIME (injected-clock millis), or `None` when no
    /// duties are armed:
    /// - time-cadence snapshot: anchor + interval, ONLY when the cadence
    ///   is Time AND anchored AND the actor has committed at least once
    ///   (the seq-0 rule — an unanchored or never-committed cadence is
    ///   never due, exactly as `maybe_snapshot_on_idle` decides);
    /// - passivation: birth/last-work stamp + idle window.
    ///
    /// The minimum of the armed duties wins. Mirrors the idle checks'
    /// cell reads verbatim — a deadline and the check that consumes it
    /// can never disagree.
    fn next_duty_due_ms(&self, cell: &ActorCell) -> Option<u64> {
        let snapshot_due: Option<u64> =
            match *cell.snapshot_policy.read().expect("snapshot policy lock") {
                crate::actor::SnapshotCadence::Time(interval) => {
                    let anchor = cell
                        .snapshot_anchor_ms
                        .load(std::sync::atomic::Ordering::Acquire);
                    let committed = cell
                        .last_event_seq
                        .load(std::sync::atomic::Ordering::Acquire);
                    if anchor == CELL_SENTINEL || committed == CELL_SENTINEL {
                        None
                    } else {
                        Some(anchor.saturating_add(interval.as_millis() as u64))
                    }
                }
                _ => None,
            };
        let passivation_due: Option<u64> = (*cell.passivation.read().expect("passivation lock"))
            .map(|p| {
                cell.last_work_ms
                    .load(std::sync::atomic::Ordering::Acquire)
                    .saturating_add(p.idle_for.as_millis() as u64)
            });
        match (snapshot_due, passivation_due) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// The loop's idle tail: park until a MESSAGE (notify), SHUTDOWN
    /// (watch), or the next DUE DUTY — never a fixed poll.
    ///
    /// Deadline sleeps the real remaining window (exact in production).
    /// Under an injected fake clock the sleep is raced against the clock
    /// watch: a test's `advance()` wakes the loop to recompute duties
    /// immediately (the removed 20ms arm was also the tests' observation
    /// wake; the clock jump is now). No duties armed: park forever in
    /// production (`pending` — zero wakeups, the deliverable), or just
    /// on clock jumps under a fake.
    async fn idle_wait(&self, cell: &ActorCell, shutdown: &mut watch::Receiver<bool>) {
        let notified = cell.work.notified();
        let fake_watch = self.clock.fake_watch();
        tokio::select! {
            _ = notified => {}
            _ = shutdown.changed() => {}
            _ = async {
                loop {
                    let due = self.next_duty_due_ms(cell);
                    let now = self.clock.now().as_millis();
                    match fake_watch.clone() {
                        // REAL CLOCK: deadline sleep, exact — the loop
                        // wakes when the earliest duty is due (or never,
                        // when nothing is armed: zero wakeups).
                        None => match due {
                            Some(due) => {
                                let window = due.saturating_sub(now);
                                tokio::time::sleep(
                                    std::time::Duration::from_millis(window),
                                )
                                .await;
                                #[cfg(test)]
                                bump_cell_wakeups();
                                return;
                            }
                            None => std::future::pending::<()>().await,
                        },
                        // FAKE CLOCK (tests): the fake may jump far ahead
                        // of real time, so the due delta cannot map to a
                        // real sleep — recompute on every jump and return
                        // as soon as the duty is due. The removed 20ms
                        // arm was also the tests' observation wake; the
                        // clock jump is now.
                        Some(mut rx) => {
                            if due.is_some_and(|d| d <= now) {
                                #[cfg(test)]
                                bump_cell_wakeups();
                                return;
                            }
                            let _ = rx.changed().await;
                        }
                    }
                }
            } => {}
        }
    }
}

/// The ES actor loop: the atomic step, forever, until shutdown or crash.
///
/// PUSH-DRIVEN: a message wakes the loop through the front door's notify;
/// the idle tail parks on the notify, the shutdown watch, or the next due
/// duty (deadline idle — no fixed poll; a duty-armed actor wakes exactly
/// when its earliest duty is due, an actor with no duties never wakes).
pub(crate) async fn es_actor_loop(mut loop_ctx: EsLoop, mut shutdown: watch::Receiver<bool>) {
    // SPAWN-TIME RECOVERY: a re-activated entity (partition re-spawn,
    // or any spawn onto a journaled path) replays its journal before the
    // first step — passivation is lossless for the ES tier. A fresh
    // genesis spawn has no journal; the store answers None. Recovery may
    // REPLACE the table's Arc — rebind the loop's cache here.
    recover_at_boot(&mut loop_ctx).await;
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
                // here (deadline idle is the wake), never mid-step —
                // snapshots stay BETWEEN messages. Passivation is
                // TERMINAL for this loop task: on true, break (the
                // graceful exit already ran inside the check).
                maybe_snapshot_on_idle(&loop_ctx).await;
                if maybe_passivate(&loop_ctx).await {
                    break;
                }
            }
            Step::Crashed => break, // supervisor takes over
            Step::Stop => {
                loop_ctx
                    .graceful_exit(crate::actor::StopReason::Normal)
                    .await;
                break;
            }
        }
        // DEADLINE IDLE: park on message / shutdown / next due duty. The
        // ES loop's shutdown wake IS this watch arm (the removed poll was
        // its only other timer). notified() is created BEFORE the select
        // — a wake that fired between the last await and now must not be
        // lost to a permit granted after the arm registered.
        let idler = Idler {
            clock: loop_ctx.clock.clone(),
        };
        idler.idle_wait(&loop_ctx.cell, &mut shutdown).await;
    }
    drain_inbox_on_stop(&loop_ctx).await;
}

/// Boot-time journal recovery for an ES actor: rebuilds the live state
/// from the store's replay (snapshot + tail) when this path has a
/// journal. Runs BEFORE the first step; no command is processed
/// unrecovered.
async fn recover_at_boot(ctx: &mut EsLoop) {
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
            .unwrap_or_else(Json::default)
    };
    let fresh = {
        let old = old.lock().await;
        old.rebuild(&genesis_args, snapshot_state, &tail).ok()
    };
    if let Some(fresh) = fresh {
        let state = Arc::new(tokio::sync::Mutex::new(fresh));
        let mut kernel = ctx.kernel.lock();
        kernel.es_state.insert(ctx.path.clone(), state.clone());
        // Rebind the loop's cache: the table's Arc was just replaced, and
        // every step after this reads the cache, not the table.
        ctx.state = Some(state);
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
    /// loop must exit through the graceful path (on_stop + teardown).
    Stop,
}

/// The atomic step, batched. Per batch: snapshot up to N envelopes →
/// per message: observe → find entry → decide (catch_unwind, pure) with
/// events collected into ONE buffer → ONE journal append for the whole
/// batch → commit every offset → one state-lock pass applying all events
/// in order → per-message outbox flush (in-order effects) → emit fan-out
/// with per-event (path, seq) stamps → maybe snapshot once at the batch's
/// last seq.
///
/// The single-message invariants survive the batching:
/// - NOTHING is appended or acked until every decision has succeeded; a
///   decision panic leaves the whole batch queued (snapshots remove
///   nothing; the fold is pure, replay is recomputed, and the journal
///   never saw a partial batch).
/// - An append failure leaves the whole batch queued (the store was not
///   changed atomically; "never ack what isn't journaled").
/// - The fan-out stamps stay per-event (each event zips with its own seq
///   — append answers one seq per event, in order).
/// - Unknown-schema and decode failures dead-letter THEIR message alone
///   (never entering the batch buffer) and the batch continues.
async fn step_es(ctx: &EsLoop) -> Step {
    // 1. SNAPSHOT up to the spawn's batch size (FIFO, nothing removed:
    // the commit point is stage 6, after the append lands).
    let batch_n = ctx.cell.step_batch;
    let batch: Vec<(crate::inbox::InboxOffset, Envelope)> = {
        let mut inbox = ctx.cell.inbox.lock();
        inbox.peek_up_to(batch_n)
    };
    if batch.is_empty() {
        return Step::Idle;
    }

    // The Delivered observation rides the handler slot — observation on
    // the message path never takes the kernel lock, and constructs
    // nothing when observation is off.
    let observing = ctx.kernel.observing();
    if observing {
        for (_, envelope) in &batch {
            ctx.kernel.observe(crate::observe::Observation::new(
                envelope.trace.causality_id.as_millis_ts(),
                crate::observe::ObservationKind::Delivered {
                    to: ctx.path.clone(),
                    schema: envelope.schema.clone(),
                    trace: envelope.trace,
                },
            ));
        }
    }

    // 2+3+4. Per message: FIND the command entry, build ctx, dispatch under
    // catch_unwind. DECIDE ONLY: no state mutation, no journal write, no
    // commit inside the handler. Events collect into ONE ordered buffer;
    // each message's outbox is kept for its own flush. The state lock is
    // taken ONCE for the whole decide pass.
    let mut decisions: Vec<Outbox> = Vec::with_capacity(batch.len());
    let mut batch_events = crate::envelope::Events::new();
    // PER-BATCH entry resolution (cell-local spawn-static config): ONE
    // read-guard clone of the table per batch — the per-message find
    // borrows from it (no per-message lock, no per-message Arc bump).
    // The table is written only at spawn/restart; per-batch is the safe
    // staleness bound (a re-attached entry is visible the batch after the
    // write, never stale WITHIN a batch).
    let batch_entries: Vec<Arc<dyn CommandEntry>> =
        ctx.cell.entries.read().expect("entries lock").clone();
    {
        let state = ctx.state();
        let mut state = state.lock().await;
        for (_, envelope) in &batch {
            // FIND the command entry for this schema by reference.
            let entry = batch_entries
                .iter()
                .find(|e| e.schema() == envelope.schema);
            let Some(entry) = entry else {
                // Unknown schema: dead-letter THIS message (it can never
                // be handled; redelivering it would be futile) and let
                // the rest of the batch continue.
                dead_letter(
                    &ctx.kernel,
                    envelope,
                    crate::kernel::DeadLetterReason::UnknownSchema,
                    "no entry for this schema",
                );
                decisions.push(Outbox::new());
                continue;
            };

            let mut outbox = Outbox::new();
            let mut cmd_ctx = CmdCtx::new(
                &ctx.path,
                &envelope.trace,
                envelope.reply_to.as_ref(),
                ctx.view.as_ref(),
                &mut outbox,
            );

            let dispatch_result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                entry.dispatch(state.as_mut(), &envelope.payload, &mut cmd_ctx)
            }));

            match dispatch_result {
                Ok(Ok(events)) => {
                    for event in events {
                        batch_events.push(event);
                    }
                    decisions.push(outbox);
                }
                Ok(Err(report)) => {
                    // Decode failure: dead-letter this message alone and
                    // continue (kernel bug only if the schema registry
                    // and adapter disagree).
                    let reason = format!("{report}");
                    dead_letter(
                        &ctx.kernel,
                        envelope,
                        crate::kernel::DeadLetterReason::Decode,
                        &reason,
                    );
                    decisions.push(Outbox::new());
                }
                Err(poison) => {
                    // PANIC: nothing appended, nothing committed, outboxes
                    // discarded. The whole batch stays queued (snapshots
                    // never removed anything) and the state may be
                    // poisoned — mark crashed and stop; the supervisor
                    // rebuilds from the journal (never reuses `state`).
                    ctx.cell.mark_crashed();
                    if observing {
                        ctx.kernel.observe(crate::observe::Observation::new(
                            envelope.trace.causality_id.as_millis_ts(),
                            crate::observe::ObservationKind::Failed {
                                path: ctx.path.clone(),
                                error: "handler panic".to_owned(),
                            },
                        ));
                    }
                    let _ = poison;
                    return Step::Crashed;
                }
            }
        }
    }
    // 4.5 EMIT FILTER (declaration enforcement, PRE-append, whole batch).
    // The declared surface is the only surface: events whose schema the
    // actor never declared are dropped here — never journalled, never
    // applied — with a DeadLettered fact + tracing error as the observable
    // record. The step CONTINUES with the declared remainder: dropping is
    // a state-consistent outcome (apply runs per appended event), while
    // failing the step would burn restart budget on a static condition
    // redelivery can never heal. The gate consults the CELL-LOCAL mirror
    // (spawn-static config, synced at the declaration-mutation point): no
    // registry lock, no kernel lock. One read-held pass partitions the
    // buffer, the read guard DROPS, then the undeclared remainder is
    // dead-lettered (no lock is held across the async dead-letter work).
    // A dropped event dead-letters with a root trace: the envelope→command
    // link is per-message and batching owns no per-event trace mapping —
    // the schema + payload stay the full observable record.
    let (declared_events, undeclared) = {
        let declared_emits = ctx.cell.declared_emits.read().expect("declared emits lock");
        let mut declared = crate::envelope::Events::new();
        let mut undeclared = crate::envelope::Events::new();
        for event in batch_events {
            if declared_emits.contains(&event.schema) {
                declared.push(event);
            } else {
                undeclared.push(event);
            }
        }
        (declared, undeclared)
    };
    for event in undeclared {
        tracing::error!(
            actor = %ctx.path,
            schema = %event.schema,
            "undeclared event dropped before journal append"
        );
        // The dropped EVENT is what died: it is dead-lettered as an
        // envelope addressed back to the emitting actor. The envelope
        // SHARES the event's payload (Arc bump).
        let dropped = Envelope::raw(
            event.schema.clone(),
            crate::envelope::Address::Path(ctx.path.clone()),
            crate::envelope::Payload::shared(&event.payload),
            crate::envelope::TraceCtx::root(),
        )
        .from(ctx.path.clone());
        dead_letter(
            &ctx.kernel,
            &dropped,
            crate::kernel::DeadLetterReason::UndeclaredEvent,
            "emitted undeclared schema (dropped before journal append)",
        );
    }
    let events = declared_events;

    // 5. JOURNAL APPEND — ONE call for the whole batch (durable record
    // first). The store is awaited OUTSIDE the kernel sync guard — a
    // write-through backend gets "never ack what isn't journaled"; a
    // failure aborts the step BEFORE any commit (the whole batch stays
    // queued; supervision treats it as a crash). An empty buffer (every
    // message was unknown/decode-failed) skips the store entirely.
    // PROJECTOR CHECKPOINT PATH: a stamped envelope IS a recorded fact
    // from elsewhere (a projector consuming a broadcast copy) — its
    // re-records journal as CatchUp checkpoints (append answers `None`
    // slots when the seeding path already held the fact). A batch of
    // stamped copies of the SAME foreign fact (the projector loop's
    // steady shape) rides the catchup arm; anything else (plain commands,
    // mixed runs) takes the plain append.
    let seqs: Vec<crate::journal::SeqNo> = {
        let store = ctx.kernel.journal_store();
        if events.is_empty() {
            Vec::new()
        } else {
            let mut origins = batch
                .iter()
                .filter_map(|(_, envelope)| envelope.recorded_origin());
            let first = origins.next();
            let all_same_origin = first.is_some()
                && origins.all(|o| {
                    let f = first.expect("checked");
                    o.journal == f.journal && o.seq == f.seq
                })
                && batch.len()
                    == batch
                        .iter()
                        .filter(|(_, e)| e.recorded_origin().is_some())
                        .count();
            if all_same_origin {
                let origin = first.expect("checked").clone();
                let scanned: Vec<crate::journal::ScannedEvent> = events
                    .iter()
                    .map(|event| crate::journal::ScannedEvent {
                        journal: origin.journal.clone(),
                        seq: origin.seq,
                        ingest_seq: 0,
                        event: event.clone(),
                    })
                    .collect();
                match store.append_catchup(&ctx.path, &scanned).await {
                    Ok(results) => {
                        if !results.iter().all(|slot| slot.is_some()) {
                            // Already checkpointed by the seeding path:
                            // commit (durably held) and move on — replay
                            // restores this fact from the journal.
                            let mut inbox = ctx.cell.inbox.lock();
                            for (offset, _) in &batch {
                                inbox.commit_through(*offset);
                            }
                            return Step::Work;
                        }
                        events
                            .iter()
                            .map(|_| crate::journal::SeqNo::new(0))
                            .collect::<Vec<_>>()
                    }
                    Err(report) => {
                        tracing::error!(actor = %ctx.path, error = ?report, "journal append failed");
                        ctx.cell.mark_crashed();
                        return Step::Crashed;
                    }
                }
            } else {
                match store.append(&ctx.path, &events).await {
                    Ok(seqs) => seqs,
                    Err(report) => {
                        tracing::error!(actor = %ctx.path, error = ?report, "journal append failed");
                        ctx.cell.mark_crashed();
                        return Step::Crashed;
                    }
                }
            }
        }
    };

    // 6. COMMIT the whole batch (the commit point: none of these messages
    // will ever redeliver). One inbox acquisition moves the cursor past
    // every offset. The commit-point bookkeeping is cell-local: the last
    // committed seq and the inbox cursor land without the kernel lock (the
    // seq and the cursor are single-writer — this loop is the only
    // appender for the path). Acked observations ride the handler slot.
    {
        let mut inbox = ctx.cell.inbox.lock();
        for (offset, _) in &batch {
            inbox.commit_through(*offset);
        }
    }
    if let Some(last) = seqs.last() {
        ctx.cell
            .last_event_seq
            .store(last.as_u64(), std::sync::atomic::Ordering::Release);
    }
    if observing {
        for (_, envelope) in &batch {
            ctx.kernel.observe(crate::observe::Observation::new(
                envelope.trace.causality_id.as_millis_ts(),
                crate::observe::ObservationKind::Acked {
                    to: ctx.path.clone(),
                    schema: envelope.schema.clone(),
                    trace: envelope.trace,
                },
            ));
        }
    }

    // 7. APPLY (the same fold replay uses; state may now lag the journal
    // only if the process dies before this line — rebuild covers that).
    // One state-lock pass over the whole batch's events, in order.
    if !events.is_empty() {
        let state = ctx.state();
        let mut state = state.lock().await;
        for event in events.iter() {
            state.apply_erased(event);
        }
    }

    // 8. OUTBOX FLUSH per message (deferred sends/replies, causality-
    // linked, in-order across the batch: message k's effects land before
    // message k+1's). A StopSelf intent concludes the step with Step::Stop
    // — sends recorded before it have already flushed (in-order).
    let mut stop_self = false;
    for outbox in decisions {
        if flush_outbox(ctx, outbox).await {
            stop_self = true;
            break;
        }
    }

    // 9. EMIT FAN-OUT (recorded, declared facts broadcast to every actor
    // that declared .handles — the named step exists so the order never
    // changes). Each copy is STAMPED with the recording (path, seq): a
    // consuming projector reads the stamp into its checkpoint, so a
    // restart never re-seeds (never double-folds) a fact it folded live.
    // The batch fans out over the ONE zip: event k pairs with seq k
    // (append answered one seq per event, in order), so stamps stay
    // per-event correct.
    // A projector's own re-records fan out NOWHERE: they are checkpoint
    // writes of facts that were already broadcast when their source
    // recorded them (re-broadcasting would echo every consumed fact back
    // into the fabric — and echo the echo). The gate is the ACTOR (the
    // kernel's projector set), not the envelope: a projector consuming a
    // plain host publish has no stamp, but its re-record is still just a
    // checkpoint write.
    if !ctx.is_projector {
        fan_out_emits(ctx, events, &seqs).await;
    }

    // 10. MAYBE SNAPSHOT (policy-gated, BETWEEN batches). The cadence
    // anchors at the batch's LAST seq — a boundary crossed mid-batch
    // snapshots after the batch commits, never mid-step.
    maybe_snapshot(ctx, seqs).await;

    if stop_self { Step::Stop } else { Step::Work }
}

impl EsLoop {
    /// The live state shell — the Arc captured at spawn (rebound at boot
    /// recovery / restart / projector catch-up, the only moments the
    /// table's Arc changes). LOCK-FREE: the kernel tables lock leaves the
    /// message path entirely.
    fn state(&self) -> Arc<tokio::sync::Mutex<Box<dyn DynEsActor>>> {
        self.state
            .clone()
            .expect("es state cached for a running ES loop")
    }

    /// The loop's own graceful exit: run the actor's `on_stop` hook (the
    /// instance is still alive here — not a crash), then hand the table
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
    pub(crate) registry: Arc<crate::system::CountingRegistryLock>,
    /// The shared actor tables (reply leases + ask facts live here).
    pub(crate) kernel: Arc<CountingKernelLock>,
    /// The clock for lease expiries.
    pub(crate) clock: crate::clock::ClockService,
}

impl crate::context::AskPort for KernelAskPort {
    fn ask_channel(
        &self,
        dest: Address,
        schema: SchemaId,
        payload: crate::envelope::Payload,
        ttl: std::time::Duration,
    ) -> std::pin::Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (
                            crate::reply::LeaseId,
                            tokio::sync::oneshot::Receiver<crate::envelope::Payload>,
                        ),
                        error_stack::Report<crate::context::AskError>,
                    >,
                > + Send,
        >,
    > {
        let registry = self.registry.clone();
        let kernel = self.kernel.clone();
        let clock = self.clock.clone();
        // The settle path rides a clone: the boxed future must be 'static,
        // and the port is an cheap Arc bundle.
        let settle_port = self.clone();
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

            // Open the lease and route the request envelope. Expired
            // leases are reaped here as a side effect of opening — a
            // dead asker's slot can never accumulate (no reaper task).
            // The AskOpened observation is emitted from the HANDLE (the
            // gate reads the lock-free slot), never under the tables lock
            // — a handler that re-acquired the tables would deadlock.
            let now = clock.now();
            let (lease, receiver) = {
                let mut kernel = kernel.lock();
                kernel.replies.prune(now);
                let trace = crate::envelope::TraceCtx::root();
                let (lease, receiver) = kernel.replies.open(ttl, now);
                kernel.ask_facts.push(AskFact {
                    opened: true,
                    outcome: None,
                    dest: dest.clone(),
                    trace,
                });
                (lease, receiver)
            };
            if kernel.observing() {
                kernel.observe(crate::observe::Observation::new(
                    now,
                    crate::observe::ObservationKind::AskOpened {
                        from: ActorPath::new("anonymous"),
                        dest: dest.clone(),
                        trace: crate::envelope::TraceCtx::root(),
                    },
                ));
            }
            let trace = crate::envelope::TraceCtx::root();
            let envelope =
                Envelope::raw(schema, dest.clone(), payload, trace).reply_to(Address::Slot(lease));
            match deliver_with_retry(&endpoint, envelope).await {
                Ok(()) => Ok((lease, receiver)),
                Err(_) => {
                    // The request never left: settle the ask as Failed so
                    // the ledger stays opened/settled-paired and the lease
                    // is CANCELLED (a leaked slot would hold the reply
                    // channel open forever).
                    settle_port.ask_settled(
                        lease,
                        dest.clone(),
                        AskOutcome::Failed,
                        crate::envelope::TraceCtx::root(),
                    );
                    Err(error_stack::Report::new(
                        crate::context::AskError::Unresolved(format!("{dest:?}")),
                    ))
                }
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
        {
            let mut kernel = self.kernel.lock();
            kernel.ask_facts.push(AskFact {
                opened: false,
                outcome: Some(outcome.clone()),
                dest: dest.clone(),
                trace,
            });
            // Drop the lease: settled (consumed) or timed out (late
            // replies land nowhere). The reply's `complete` already
            // removed it on the Replied path; removal here is idempotent.
            kernel.replies.cancel(&lease);
        }
        if self.kernel.observing() {
            self.kernel.observe(crate::observe::Observation::new(
                trace.causality_id.as_millis_ts(),
                crate::observe::ObservationKind::AskSettled { outcome, trace },
            ));
        }
        let _ = dest;
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
    registry: &crate::system::CountingRegistryLock,
    kernel: &CountingKernelLock,
    shutting_down: &std::sync::atomic::AtomicBool,
    schema: SchemaId,
    envelope: Envelope,
) {
    let targets: Vec<(ActorPath, Arc<Endpoint>)> = {
        let reg = registry.lock();
        let all: Vec<(ActorPath, Arc<Endpoint>)> = reg
            .handlers_of(&schema)
            .into_iter()
            .filter_map(|path| reg.resolve(&path).map(|endpoint| (path, endpoint)))
            .collect();
        // Projector-set MEMBERS are pulled out of the plain fan-out: a
        // per-key projector's only delivery obligation is its keyed copy
        // (the set arm below). A copy of every OTHER key's fact would
        // fold cross-key data into its fold — the set's key derivation
        // is the whole point of per-key read models.
        let member_paths: Vec<ActorPath> = all
            .iter()
            .map(|(path, _)| path)
            .filter(|path| reg.projector_set_owning(path).is_some())
            .cloned()
            .collect();
        all.into_iter()
            .filter(|(path, _)| !member_paths.contains(path))
            .collect()
    };
    // ONE Sent observation per publish (not per delivery): the broadcast
    // itself is the observable event — a zero-subscriber publish still
    // happened.
    if kernel.observing() {
        kernel.observe(crate::observe::Observation::new(
            envelope.trace.causality_id.as_millis_ts(),
            crate::observe::ObservationKind::Sent {
                from: envelope.from.clone(),
                dest: crate::envelope::Address::Schema(schema.clone()),
                schema: schema.clone(),
                trace: envelope.trace,
            },
        ));
    }
    for (_path, endpoint) in &targets {
        // Block backpressure: a full inbox stalls the publisher (loss is
        // unrepresentable; sizing mailboxes is the spawner's call). A
        // closed endpoint (restart in flight) skips this one delivery.
        let _ = deliver_with_retry(endpoint, envelope.clone()).await;
    }
    // PROJECTOR SETS: a declared consumption with a shard key is a
    // delivery obligation — a per-set copy resolves `public/key` and
    // activates the owning projector on demand (like a told command). A
    // live projector that declared the schema already received its copy
    // above; only derived paths NOT among the targets are serviced here
    // (never a double delivery).
    let sets = {
        let reg = registry.lock();
        reg.projector_sets_consuming(&schema)
    };
    for set in sets {
        let Some(key) = extract_key(registry, &envelope, &set.key_field) else {
            // That copy only: live declarants are already served.
            dead_letter(
                kernel,
                &envelope,
                DeadLetterReason::ShardKeyMissing,
                "broadcast fact for a projector set arrived without its key",
            );
            continue;
        };
        let derived = ActorPath::new(format!("{}/{}", set.public, key).as_str());
        // NEVER deliver a derived copy back to its own source journal: the
        // projector's journal already holds the fact (it recorded it), so
        // a checkpoint of itself would seed a phantom second fold.
        if envelope.from.as_ref() == Some(&derived) {
            continue;
        }
        // NEVER deliver a derived copy sourced from ANY projector-set
        // member: that member already folded the fact AND checkpointed
        // it (its journal holds a CatchUp copy). A second delivery
        // would seed a phantom re-fold into a sibling view.
        if envelope
            .from
            .as_ref()
            .is_some_and(|from| registry.lock().projector_set_owning(from).is_some())
        {
            continue;
        }
        if targets.iter().any(|(path, _)| *path == derived) {
            continue;
        }
        let mut keyed = envelope.clone();
        keyed.dest = crate::envelope::Address::Path(derived);
        if let Err(undeliverable) = route(registry, kernel, shutting_down, keyed).await {
            dead_letter(
                kernel,
                &undeliverable,
                DeadLetterReason::Unresolvable,
                "projector copy did not resolve after activation",
            );
        }
    }
}

/// Resolves one reply: a slot goes straight to the asker's oneshot (the
/// mechanism); a path routes an ordinary envelope through the registry
/// (the durable name).
async fn resolve_reply(
    kernel: &CountingKernelLock,
    registry: &crate::system::CountingRegistryLock,
    shutting_down: &std::sync::atomic::AtomicBool,
    to: Address,
    schema: SchemaId,
    payload: crate::envelope::Payload,
    trace: TraceCtx,
) {
    match to {
        Address::Slot(lease) => {
            // Mechanism: complete the lease if it is still live; a dead
            // (expired/pruned) slot just drops the reply — the asker is
            // gone, and the ask timed out on its side already. The live
            // value completes the slot AS IS — the asker downcasts it
            // (zero serde on the whole reply path).
            kernel.lock().replies.complete(&lease, payload);
        }
        Address::Schema(_) => {
            // A schema-addressed reply is an ordinary routed send (the
            // route table picks a handler).
            let envelope = Envelope::raw(schema, to, payload, trace);
            if let Err(undeliverable) = route(registry, kernel, shutting_down, envelope).await {
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
            let envelope = Envelope::raw(schema, Address::Path(path.clone()), payload, trace);
            if let Err(undeliverable) = route(registry, kernel, shutting_down, envelope).await {
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
async fn fan_out_emits(
    ctx: &EsLoop,
    events: crate::envelope::Events,
    seqs: &[crate::journal::SeqNo],
) {
    let cause = crate::envelope::TraceCtx::root();
    for (event, seq) in events.into_iter().zip(seqs.iter()) {
        // The broadcast copy SHARES the event's payload (one Arc bump,
        // ZERO copies): N subscribers read one value. The event is
        // consumed — its payload moves into the envelope.
        let schema = event.schema.clone();
        let envelope = Envelope::raw(
            event.schema.clone(),
            crate::envelope::Address::Schema(event.schema.clone()),
            event.payload,
            cause,
        )
        .from(ctx.path.clone())
        .with_recorded_origin(ctx.path.clone(), *seq);
        broadcast(
            &ctx.registry,
            &ctx.kernel,
            &ctx.shutting_down,
            schema,
            envelope,
        )
        .await;
    }
}

/// The single flush-time gate for outbound actor messages: every intent an
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
    // The gate consults the CELL-LOCAL mirror (spawn-static config, synced
    // at the declaration-mutation point): no registry lock, no kernel lock.
    // One read-held pass splits intents by declaration, the read guard
    // DROPS, then both arms proceed (no lock across the async delivery).
    let (gated, ungated) = {
        let declared_emits = ctx.cell.declared_emits.read().expect("declared emits lock");
        let mut ok = Vec::new();
        let mut dropped = Vec::new();
        for intent in outbox.drain() {
            // THE GATE: every outbound message declares itself. The
            // check BORROWS the schema — only a dropped intent pays a
            // clone (its schema must outlive it for the tracing error
            // and the dead-letter record). StopSelf is not a message —
            // it passes untouched.
            let undeclared = intent
                .emitted_schema()
                .is_some_and(|schema| !declared_emits.contains(schema));
            if undeclared {
                let schema = intent
                    .emitted_schema()
                    .expect("undeclared intent has a schema")
                    .clone();
                dropped.push((intent, schema));
            } else {
                ok.push(intent);
            }
        }
        (ok, dropped)
    };
    for (intent, schema) in ungated {
        tracing::error!(
            actor = %ctx.path,
            schema = %schema,
            "undeclared emit dropped at flush (add .emits::<M>() at the spawn site)"
        );
        dead_letter_schema(ctx, &intent, &schema);
    }
    let mut stop_self = false;
    for intent in gated {
        match intent {
            crate::context::Intent::Send(envelope) => {
                if let Err(undeliverable) =
                    route(&ctx.registry, &ctx.kernel, &ctx.shutting_down, envelope).await
                {
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
                    &ctx.shutting_down,
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
                resolve_reply(
                    &ctx.kernel,
                    &ctx.registry,
                    &ctx.shutting_down,
                    to,
                    schema,
                    payload,
                    trace,
                )
                .await;
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
            to,
            payload,
            trace,
            ..
        } => Envelope::raw(
            schema.clone(),
            to.clone(),
            crate::envelope::Payload::shared(payload),
            *trace,
        ),
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
    let cadence = *ctx
        .cell
        .snapshot_policy
        .read()
        .expect("snapshot policy lock");
    let crate::actor::SnapshotCadence::Messages(n) = cadence else {
        return;
    };
    if n == 0 || seqs.is_empty() {
        return;
    }
    let last = *seqs.last().expect("non-empty");
    // Snapshot at EVERY cadence boundary the batch crossed (or exactly
    // hit): the anchors are the events at 1-based positions k*n — between
    // batches, never mid-step. A single-event batch (trickle) hits at
    // most one boundary, matching the legacy per-message shape exactly; a
    // loaded batch that jumps several boundaries records each anchor (the
    // journal keeps every snapshot; replay takes the last).
    let first = seqs.first().expect("non-empty").as_u64();
    let last_seq = last.as_u64();
    if last_seq + 1 - first < n {
        return;
    }
    let mut boundary = ((first + 1).div_ceil(n) * n).saturating_sub(1);
    loop {
        snapshot_now(ctx, crate::journal::SeqNo::new(boundary)).await;
        let next = boundary + n;
        if next > last_seq {
            break;
        }
        boundary = next;
    }
}

/// Writes one snapshot of the live state at `seq` (the shared tail of both
/// cadence checks; always BETWEEN messages, never mid-step).
async fn snapshot_now(ctx: &EsLoop, last: crate::journal::SeqNo) {
    // Capture the blob under the state lock, then DROP the guard before
    // the store await (the erased state shell is not Send; no lock may be
    // held across the store call).
    let captured = {
        let state = ctx.state();
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
            // The time-cadence anchor is cell-local policy bookkeeping
            // (written by this loop alone; `u64::MAX` = unanchored).
            ctx.cell
                .snapshot_anchor_ms
                .store(now.as_millis(), std::sync::atomic::Ordering::Release);
            if ctx.kernel.observing() {
                ctx.kernel.observe(crate::observe::Observation::new(
                    now,
                    crate::observe::ObservationKind::SnapshotTaken {
                        path: ctx.path.clone(),
                        seq: last,
                    },
                ));
            }
        }
    }
}

/// The time-cadence half of snapshotting: checked on the ES loop's idle
/// path, so an actor that never receives another message still snapshots
/// when the interval elapses. BETWEEN messages by construction — the idle
/// check runs after a full step drained the inbox.
///
/// The due check is O(1) and LOCK-FREE: cadence, anchor, and last
/// committed seq are cell-local bookkeeping (single-writer), so an idle
/// check never loads the journal — `snapshot_now`'s capture reads the
/// live state, and the store only learns of the snapshot via
/// `append_snapshot`.
async fn maybe_snapshot_on_idle(ctx: &EsLoop) {
    let cadence = *ctx
        .cell
        .snapshot_policy
        .read()
        .expect("snapshot policy lock");
    let crate::actor::SnapshotCadence::Time(interval) = cadence else {
        return;
    };
    let anchor = ctx
        .cell
        .snapshot_anchor_ms
        .load(std::sync::atomic::Ordering::Acquire);
    let last_committed = ctx
        .cell
        .last_event_seq
        .load(std::sync::atomic::Ordering::Acquire);
    // An unanchored cadence is never due (safe default), and an actor
    // that never committed has nothing to anchor a snapshot to: the
    // first post-snapshot seq would wrongly skip event seq 0 on restore.
    // (Both are `CELL_SENTINEL`, never 0 — 0 is a legitimate value for
    // both a millis timestamp and a committed seq.)
    if anchor == CELL_SENTINEL || last_committed == CELL_SENTINEL {
        return;
    }
    if ctx.clock.now().as_millis().saturating_sub(anchor) < interval.as_millis() as u64 {
        return;
    }
    snapshot_now(ctx, crate::journal::SeqNo::new(last_committed)).await;
}

/// Closes the inbox on stop; teardown (or the sweep) flushes undelivered
/// entries to the DLQ.
async fn drain_inbox_on_stop(ctx: &EsLoop) {
    let mut inbox = ctx.cell.inbox.lock();
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
    ctx.cell
        .last_work_ms
        .store(now, std::sync::atomic::Ordering::Release);
}

/// The idle arm's passivation check: when the actor's declared idle
/// window has elapsed with no completed step, passivate — close the
/// door FIRST (front door refuses new pushes to the DLQ), drain what is
/// already queued through the normal step path (bounded by inbox
/// capacity), then exit gracefully with `StopReason::Passivated`.
///
/// Returns `true` when the actor PASSIVATED (the caller's loop must
/// break — passivation is terminal for this loop task; re-activation is
/// the factory's job, not this task's).
///
/// Suspended during the shutdown sweep: the sweep owns termination.
async fn maybe_passivate(ctx: &EsLoop) -> bool {
    // The sweep owns termination: passivation stands down (lock-free —
    // the loop holds its own Arc of the barrier).
    if ctx.shutting_down.load(std::sync::atomic::Ordering::SeqCst) {
        return false;
    }
    let idle_for = *ctx.cell.passivation.read().expect("passivation lock");
    let Some(idle_for) = idle_for.map(|p| p.idle_for) else {
        return false;
    };
    // The spawn ALWAYS stamps the idle window's start (the birth stamp —
    // millis 0 is a legitimate injected-clock value, so no sentinel here:
    // an actor spawned and never messaged passivates from birth).
    let last = ctx
        .cell
        .last_work_ms
        .load(std::sync::atomic::Ordering::Acquire);
    let now = ctx.clock.now().as_millis();
    if now.saturating_sub(last) < idle_for.as_millis() as u64 {
        return false;
    }
    // CLOSE THE DOOR first: pushes now refuse to the DLQ, but entries
    // already inside stay processable (peek/ack still work on a closed
    // inbox). Then drain: process what's queued — the ES step is sync,
    // so this is bounded by capacity.
    {
        let mut inbox = ctx.cell.inbox.lock();
        inbox.close();
    }
    loop {
        let has_mail = {
            let mut inbox = ctx.cell.inbox.lock();
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
    // Lifecycle hint BEFORE the exit: the journal is durable (everything
    // drained), so the store may demote the path to cold storage. A
    // failing hint never blocks passivation — log and proceed.
    let store = ctx.kernel.journal_store();
    if let Err(e) = store.passivated(&ctx.path).await {
        tracing::error!(
            actor = %ctx.path,
            "journal store refused the passivated hint: {e}"
        );
    }
    ctx.graceful_exit(crate::actor::StopReason::Passivated)
        .await;
    // Terminal for THIS loop task: the loop must break, never re-idle
    // (re-idling with cell-local config intact would spin a zombie:
    // still-due → passivate → idempotent teardown → forever).
    true
}
/// The service actor loop: pop → decode → dispatch (async, impure) →
/// drop the message. No journal, no cursor — service actors are at-most-once
/// by design (supervision wraps this loop).
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
                // Idle window: passivation is checked here (deadline
                // idle is the wake), never mid-step. TERMINAL on true.
                if maybe_passivate(&loop_ctx.es).await {
                    break;
                }
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
        // DEADLINE IDLE (see the ES loop's arm): message / shutdown /
        // next due duty. notified() is created before the select.
        let idler = Idler {
            clock: loop_ctx.es.clock.clone(),
        };
        idler.idle_wait(&loop_ctx.es.cell, &mut shutdown).await;
    }
    drain_inbox_on_stop(&loop_ctx.es).await;
}

/// Everything one running service loop needs.
#[derive(Clone)]
pub(crate) struct ServiceLoop {
    /// The ES-shaped plumbing the service loop shares (routing, cell).
    pub(crate) es: EsLoop,
}

/// A future whose POLLS are panic-isolated: a handler panic unwinds out of
/// `poll`, is captured here, and surfaces as `Err(poison)` — the async
/// twin of `step_es`'s `catch_unwind` around the sync decision call.
///
/// Why poll-level (not around the whole await): `catch_unwind` cannot
/// span `.await` — the wrapper is the only way to keep real-waker
/// semantics (handlers legitimately await `ctx.ask` and internal I/O)
/// while still turning any panic into `Step::Crashed` instead of tearing
/// down the loop task.
struct PanicIsolated<F>(F);

/// The stand-in marker for a handler panic surfaced through the unified
/// dispatch `Err` (the poison box is discarded — the cell's crashed flag
/// is the durable record, supervision reads that).
enum DispatchFailure {
    Decode(error_stack::Report<crate::actor::DispatchError>),
    Panic,
}

impl<F: Future> Future for PanicIsolated<F> {
    type Output = Result<F::Output, Box<dyn std::any::Any + Send>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // Projection by structural pin: the wrapper adds no unpin
        // requirement, so `map_unchecked_mut` is sound (the inner field's
        // pin state is the wrapper's).
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        match std::panic::catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(poison) => std::task::Poll::Ready(Err(poison)),
        }
    }
}

/// One service step, batched: snapshot up to N → dispatch+commit each in
/// order. One inbox acquisition, one loop wake, one pass.
///
/// Service messages are consumed on HANDOFF (commit before dispatch):
/// there is no journal to replay from, so redelivery after a crash would
/// re-run side effects — at-most-once semantics are the honest contract
/// here. The batch keeps every message's commit at exactly the point the
/// single-message step acked (before its dispatch); what amortizes is the
/// WAKE (one loop iteration + one inbox lock for up to N messages), not
/// the commit. A parked handler therefore still leaves the messages behind
/// it fully queued and visible (stop-time DLQ flush, watermark depth), and
/// a step killed mid-batch loses nothing the per-message step kept.
///
/// Dispatch runs INLINE on the loop task (no per-message `tokio::spawn`):
/// each handler future is awaited through [`PanicIsolated`], so a handler
/// panic marks the cell crashed and returns `Step::Crashed` — the loop
/// task itself is not torn down. The inbox serializes: one handler at a
/// time. Per-message outbox flushes stay INSIDE the batch loop, so message
/// k's sends still land before message k+1's (cross-message ordering
/// preserved; batching amortizes the wake/lock, never the intent order).
async fn step_service(ctx: &ServiceLoop) -> Step {
    // 1. SNAPSHOT up to the spawn's batch size (FIFO, nothing removed).
    let batch_n = ctx.es.cell.step_batch;
    let batch: Vec<(crate::inbox::InboxOffset, Envelope)> = {
        let mut inbox = ctx.es.cell.inbox.lock();
        inbox.peek_up_to(batch_n)
    };
    let Some((_, first)) = batch.first() else {
        return Step::Idle;
    };
    let _ = first;

    // 2. DISPATCH each snapshot inline, in order. Delivered observations
    // ride the handler slot (no kernel lock, nothing constructed when
    // observation is off). Unknown-schema and decode failures dead-letter
    // THEIR message alone and continue the batch (one bad envelope never
    // costs the rest of the run).
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
    let ask_port = KernelAskPort {
        registry: ctx.es.registry.clone(),
        kernel: ctx.es.kernel.clone(),
        clock: ctx.es.clock.clone(),
    };

    let observing = ctx.es.kernel.observing();
    // PER-BATCH entry resolution (cell-local spawn-static config): ONE
    // read-guard clone of the table per batch — the per-message find
    // borrows from it (no per-message lock, no per-message Arc bump).
    // The table is written only at spawn/restart (the declaration-mutation
    // point can run between steps), so per-batch is the safe staleness
    // bound: a re-attached entry is visible no later than the batch after
    // the write, exactly as the per-message read would have seen it
    // within any single batch.
    let batch_entries: Vec<Arc<dyn MsgEntry>> = ctx
        .es
        .cell
        .msg_entries
        .read()
        .expect("msg entries lock")
        .clone();
    for (offset, envelope) in batch {
        // COMMIT (the at-most-once handoff, at the same point the
        // single-message step acked: before this message's dispatch).
        ctx.es.cell.inbox.lock().commit_through(offset);
        if observing {
            ctx.es.kernel.observe(crate::observe::Observation::new(
                envelope.trace.causality_id.as_millis_ts(),
                crate::observe::ObservationKind::Delivered {
                    to: ctx.es.path.clone(),
                    schema: envelope.schema.clone(),
                    trace: envelope.trace,
                },
            ));
        }

        // FIND the message entry for this schema by reference.
        let entry = batch_entries
            .iter()
            .find(|e| e.schema() == envelope.schema);
        let Some(entry) = entry else {
            dead_letter(
                &ctx.es.kernel,
                &envelope,
                crate::kernel::DeadLetterReason::UnknownSchema,
                "no entry for this schema",
            );
            continue;
        };

        let trace = envelope.trace;
        let reply_to = envelope.reply_to.clone();
        let mut outbox = Outbox::new();
        // DECODE + DISPATCH (inline on the loop task, panic-isolated).
        // The payload is BORROWED: the adapter lends the live value to the
        // handler (`&M`, zero copies) or decodes a wire shape at the door.
        let dispatched = {
            let mut msg_ctx = crate::context::MsgCtx::new(
                &path,
                &trace,
                reply_to.as_ref(),
                view.as_ref(),
                &mut outbox,
                Some(&ask_port),
            );
            let mut service = service.lock().await;
            match entry.dispatch(service.as_mut(), &envelope.payload, &mut msg_ctx) {
                Ok(fut) => {
                    #[cfg(test)]
                    crate::kernel::bump_inline_service_dispatch();
                    if PanicIsolated(fut).await.is_err() {
                        Err(DispatchFailure::Panic)
                    } else {
                        Ok(())
                    }
                }
                Err(report) => Err(DispatchFailure::Decode(report)),
            }
        };
        match dispatched {
            Ok(()) => {}
            Err(DispatchFailure::Panic) => {
                // Handler panicked: mark crashed (supervision restarts via
                // `start`). This message was already consumed at handoff (the
                // same at-most-once window as the single-message step); every
                // LATER snapshot is still fully queued (nothing was removed),
                // so no tail surgery is needed. The outbox dies with the
                // dispatch — nothing recorded, the standing contract.
                ctx.es.cell.mark_crashed();
                return Step::Crashed;
            }
            Err(DispatchFailure::Decode(report)) => {
                // Decode failure: dead-letter THIS message alone and
                // continue the batch (one bad envelope never costs the
                // rest of the run).
                let reason = format!("{report}");
                dead_letter(
                    &ctx.es.kernel,
                    &envelope,
                    crate::kernel::DeadLetterReason::Decode,
                    &reason,
                );
                if flush_outbox(&ctx.es, outbox).await {
                    return Step::Stop;
                }
                // Effects already flushed for this message; the next one
                // starts on a fresh outbox.
                continue;
            }
        }

        // FLUSH deferred effects from THIS message before the next one
        // runs (cross-message ordering: k's sends land before k+1's). A
        // StopSelf intent concludes the step — sends recorded before it
        // have flushed (in-order); the loop exits after this step.
        if flush_outbox(&ctx.es, outbox).await {
            return Step::Stop;
        }
    }

    Step::Work
}

/// Restarts a crashed ES actor (spec algorithm):
/// 1. rebuild state from the last snapshot (fast path) or genesis args,
/// 2. apply the journal tail after the snapshot seq (replay: apply only —
///    handlers never re-run, effects never re-fire),
/// 3. swap the slot endpoint to a fresh loop; the inbox and its cursor
///    persist, so pending messages (including the one that crashed the
///    actor) redeliver exactly once from where the cursor stopped.
///
/// The supervisor wraps this with policy/budget/backoff checks.
///
/// # Errors
///
/// Propagates rebuild failures (a corrupt snapshot or undecodable state).
pub(crate) async fn restart_es(
    ctx: &mut EsLoop,
    genesis_args: &Json,
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
                events: Vec::new(),
            })
    };
    let (snapshot, tail) = (replay.snapshot.clone(), replay.tail.clone());

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
    // Fresh instance swapped in; the poisoned one is gone. The CELL is
    // reused (identity + inbox + config survive): its transient state is
    // reset here — a restarted actor is not crashed, its idle window
    // restarts from the restart stamp, and the last committed seq is
    // re-derived from the replay (the journal's head; the seq is the
    // journal's, not the loop's, at this moment).
    {
        let state = Arc::new(tokio::sync::Mutex::new(fresh));
        let mut kernel = ctx.kernel.lock();
        kernel.es_state.insert(ctx.path.clone(), state.clone());
        // The supervised restart reuses THIS ctx for the fresh loop
        // (spawned below) — rebind the cache so the new loop reads the
        // fresh instance, not the replaced shell.
        ctx.state = Some(state);
        ctx.cell.clear_crashed();
        let now_ms = ctx.clock.now().as_millis();
        ctx.cell
            .last_work_ms
            .store(now_ms, std::sync::atomic::Ordering::Release);
        let journal_head = replay
            .events
            .iter()
            .map(|j| j.seq.as_u64())
            .max()
            .unwrap_or(0);
        ctx.cell.last_event_seq.store(
            if replay.events.is_empty() {
                CELL_SENTINEL
            } else {
                journal_head
            },
            std::sync::atomic::Ordering::Release,
        );
        if ctx.kernel.observing() {
            ctx.kernel.observe(crate::observe::Observation::new(
                ctx.clock.now(),
                crate::observe::ObservationKind::Spawned {
                    path: ctx.path.clone(),
                    kind: crate::actor::ActorKind::EventSourced,
                    restart: true,
                },
            ));
        }
    }

    // Fresh endpoint behind the SAME path: senders holding pre-crash
    // clones never notice (identity = path; slots are swapped, not
    // dropped). The fresh door matches the spawn's capacity and policy
    // (the cell carries them), never a default.
    let (tx, rx) = mpsc::channel(door_capacity(
        ctx.cell.mailbox_capacity,
        ctx.cell.mailbox_policy,
    ));
    // The fresh endpoint couples the fresh channel to the SAME cell (the
    // inbox is the identity that survives; only the door swaps).
    let endpoint = Endpoint::new(tx, ctx.cell.clone());
    {
        let mut registry = ctx.registry.lock();
        registry
            .swap_endpoint(&ctx.path, endpoint)
            .expect("slot exists at restart");
    }

    // Redelivery: the cursor never moved; the crash-loop left the inbox
    // open-and-queued. Reopen and run a fresh loop over the SAME cell.
    {
        let mut inbox = ctx.cell.inbox.lock();
        inbox.reopen();
    }
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    spawn_tracked(front_door_loop(ctx.cell.clone(), ctx.kernel.clone(), rx));
    spawn_tracked(es_actor_loop(ctx.clone(), shutdown_rx));
    Ok(())
}

/// The front-door channel depth for a mailbox: under `Block` the channel
/// is exactly the inbox's capacity (the channel `.send().await` is the
/// block, so senders pace at the configured depth); the lossy policies
/// keep 2× headroom so bursts actually reach the inbox's own policy.
pub(crate) fn door_capacity(
    mailbox_capacity: usize,
    policy: crate::inbox::OverloadPolicy,
) -> usize {
    let capacity = mailbox_capacity.max(1);
    match policy {
        crate::inbox::OverloadPolicy::Block => capacity,
        crate::inbox::OverloadPolicy::DropNew | crate::inbox::OverloadPolicy::DropOld => {
            capacity * 2
        }
    }
}

/// The supervision engine: one task per supervised child, awaiting the
/// child's crash, then applying the spec — policy → budget → backoff →
/// restart, or stop + escalate.
///
/// SIGNAL-DRIVEN: the wait is the child's crash Notify (a push), not a
/// poll — an idle engine acquires nothing. The crashed flag is read
/// LOCK-FREE off the child's cell (Acquire pairs with the crash store's
/// Release); the kernel lock appears only on the cold restart path
/// (budget, genesis args, restart mechanics). `cell` is `None` for an
/// edge-only spec whose spawn closure registered no actor — the engine
/// parks on shutdown alone.
pub(crate) async fn supervise_child(
    system: crate::system::ActorSystem,
    spec: crate::supervision::ActorSpec,
    mut cell: Option<std::sync::Arc<ActorCell>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        // SHUTDOWN FIRST: a stop that fired before/while a crash signal
        // raced us wins — checked before every wait AND before every
        // restart action (the old poll loop noticed shutdown within its
        // 5ms sweep; the instant signal path must check it explicitly or
        // a crash landing between cycles would restart after the sweep).
        if *shutdown.borrow_and_update() {
            return;
        }
        // Wait for this child to crash (or the system to shut down).
        let mut crashed_seen = false;
        let watch = async {
            loop {
                let Some(cell) = cell.as_ref() else {
                    // Edge-only spec: no actor exists, so none can crash —
                    // the watch arm parks forever and only shutdown can
                    // wake the engine.
                    loop {
                        std::future::pending::<()>().await;
                    }
                };
                // CHECK FIRST: a crash that fired before we park (e.g.
                // the fresh loop re-crashing on its redelivered poison
                // before the engine re-arms) left the flag set with no
                // pending waiter — notify_waiters does not coalesce for
                // absent waiters. Lock-free read (Acquire pairs with
                // mark_crashed's Release).
                if cell.is_crashed() {
                    break;
                }
                // Then PARK on the signal: zero wakeups while healthy.
                cell.crash_signal.notified().await;
            }
        };
        tokio::select! {
            _ = watch => { crashed_seen = true; }
            _ = shutdown.changed() => {
                if *shutdown.borrow() { return; }
            }
        }
        let _ = crashed_seen;
        // The engine's `cell` handle may point at a DEAD cell after a
        // service restart re-spawned a fresh one — the budget/backoff
        // section below re-resolves it before acting on the crash.
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
        // The backoff sleep is the engine's only await between the
        // cycle-top shutdown check and the restart — a stop that lands
        // during the sleep must abort the restart (the old 5ms poll
        // noticed shutdown within its sweep; the signal path checks
        // here instead).
        if *shutdown.borrow_and_update() {
            return;
        }
        let is_es_child = system_is_es_child(&system, &spec.path);
        if is_es_child {
            // Journal-anchored recovery: rebuild from snapshot-or-genesis,
            // apply the tail, swap the endpoint under the SAME path, and
            // reopen the inbox (redelivery resumes from the cursor).
            let mut ctx = crate::kernel::EsLoop {
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
                shutting_down: system.shutting_down.clone(),
                view: system.view.clone(),
                clock: system.clock.clone(),
                is_projector: false,
                // The pre-restart shell (replaced under the OLD loop, which
                // has exited); restart_es REBINDS this cache to the fresh
                // instance's Arc before the new loop spawns.
                state: {
                    let kernel = system.kernel.lock();
                    kernel.es_state.get(&spec.path).cloned()
                },
            };
            let genesis_args = {
                let kernel = system.kernel.lock();
                kernel
                    .genesis_args
                    .get(&spec.path)
                    .cloned()
                    .unwrap_or_else(|| crate::json!({}))
            };
            restart_es(&mut ctx, &genesis_args)
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
            // loop). A service spawn builds a FRESH cell (already clean) —
            // but the engine's `cell` handle still points at the dead one:
            // re-resolve so the next wait listens on the LIVE cell's crash
            // signal, then make sure the fresh cell starts clear.
            {
                let fresh_cell = system.kernel.lock().cells.get(&spec.path).cloned();
                if let Some(fresh_cell) = fresh_cell {
                    fresh_cell.clear_crashed();
                    cell = Some(fresh_cell);
                } else {
                    // The spawn closure registered nothing this time —
                    // fall back to the parked edge-only watch.
                    cell = None;
                }
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
        let kernel = system.kernel.lock();
        // The child is gone: clear its crash flag (cell-local now) and
        // observe the stop WITH its typed reason, then the escalation
        // (both gated on a handler being installed).
        if let Some(cell) = kernel.cells.get(&spec.path).cloned() {
            cell.clear_crashed();
        }
        drop(kernel);
        if system.kernel.observing() {
            let now = system.clock.now();
            system.kernel.observe(crate::observe::Observation::new(
                now,
                crate::observe::ObservationKind::Stopped {
                    path: spec.path.clone(),
                    reason: stop_reason,
                },
            ));
            system.kernel.observe(crate::observe::Observation::new(
                now,
                crate::observe::ObservationKind::Escalated {
                    path: spec.path.clone(),
                    reason: reason.to_owned(),
                },
            ));
        }
    }
    // Remove the child's slot (its identity leaves the registry). The
    // in-memory state dies too: only live actors hold state, and this
    // child is terminal (crash recovery already returned above).
    {
        let mut registry = system.registry.lock();
        let _ = registry.remove_slot(&spec.path);
        system.kernel.lock().es_state.remove(&spec.path);
    }
    let message = spec.escalation_message(reason);
    if let Some(parent) = &spec.parent {
        system
            .send(system.envelope(
                crate::schema::SchemaId::new("Escalated"),
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
    /// The actor emitted an event whose schema it never declared (checked
    /// before the journal append).
    UndeclaredEvent,
    /// The actor recorded an outbound effect whose schema it never
    /// declared in `.emits` (checked when recorded effects are flushed).
    UndeclaredEmit,
    /// A partition-set command arrived without its shard key.
    ShardKeyMissing,
}
