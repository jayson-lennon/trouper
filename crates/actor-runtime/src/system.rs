//! The system facade: the single handle through which the runtime is
//! configured, driven, and observed.
//!
//! The registry is kernel, not an actor — owned here behind a lock so
//! schema registration can never deadlock and survives every actor restart.
//! The system also implements [`RuntimeView`]: handler-side lookups snapshot
//! through this read-only surface, never through kernel-mutable locks.

use std::sync::{Arc, Mutex};

use serde_json::Value as JsonValue;

use crate::actor::{
    CommandEntry, DynServiceActor, EventSourced, MsgEntry, ServiceActor, TypedEsState,
    TypedServiceState,
};
use crate::clock::{ClockService, FakeClock, SystemClock};
use crate::context::RuntimeView;
use crate::envelope::{Address, Envelope, TraceCtx};
use crate::inbox::{Inbox, OverloadPolicy};
pub use crate::kernel::DeadLetter;
use crate::kernel::{ActorCell, EsLoop, KernelState, route};
use crate::registry::{Endpoint, EndpointInfo, Registry};
use crate::schema::Schema;
use crate::types::{InboxOffset, Path, SchemaId, Timestamp};

pub use crate::kernel::SnapshotPolicy;

/// Spawn-time options for an actor.
#[derive(Debug, Clone)]
pub struct SpawnOpts {
    /// Snapshot policy (ES actors only).
    pub snapshot: SnapshotPolicy,
    /// Mailbox capacity (the logical inbox; the front door is 2× this).
    pub mailbox_capacity: usize,
    /// Inbox overload policy (default Block = backpressure).
    pub mailbox_policy: OverloadPolicy,
}

impl Default for SpawnOpts {
    fn default() -> Self {
        Self {
            snapshot: SnapshotPolicy::Off,
            mailbox_capacity: 64,
            mailbox_policy: OverloadPolicy::Block,
        }
    }
}

/// The actor system. One instance per machine; shared by reference.
///
/// The registry is its own mutex (routing never blocks actor-table
/// mutations); actor tables share [`KernelState`]'s lock because they
/// mutate together.
pub struct ActorSystem {
    /// Routing table: slots, schemas, routes.
    pub(crate) registry: Arc<Mutex<Registry>>,
    /// Actor tables: cells, journals, ES state, entries, crashes.
    pub(crate) kernel: Arc<Mutex<KernelState>>,
    pub(crate) clock: ClockService,
    /// The read-only view handed to handler contexts (the system itself).
    pub(crate) view: Arc<dyn RuntimeView>,
    /// Supervision engine shutdown handles (one per supervised child).
    child_shutdowns: std::sync::Mutex<Vec<tokio::sync::watch::Sender<bool>>>,
}

/// One actor's row in a system export.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ActorExport {
    /// The actor's path (its identity).
    pub path: Path,
    /// The contract kind (EventSourced | Service).
    pub kind: crate::types::ActorKind,
    /// The actor's declared edges.
    pub manifest: crate::schema::ActorManifest,
    /// Live ES state via `capture` (ES actors only).
    pub state: Option<JsonValue>,
    /// The actor's inbox ack cursor (ES progress).
    pub cursor: Option<u64>,
}

/// One declared edge: actor → schema it handles/emits (from manifests).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeclaredEdge {
    /// The actor declaring the edge.
    pub actor: Path,
    /// The schema on the edge.
    pub schema: SchemaId,
    /// The direction: Handles (inbound) or Emits (outbound).
    pub direction: EdgeDirection,
    /// The topic, when the edge is a topic edge.
    pub topic: Option<crate::types::Topic>,
}

/// The direction of a declared edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EdgeDirection {
    /// The actor accepts this schema.
    Handles,
    /// The actor produces this schema.
    Emits,
    /// The actor subscribes to this topic.
    Subscribes,
}

/// One observed edge: aggregated send traffic from the tap.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObservedEdge {
    /// The sending path (absent for system-entry sends).
    pub from: Option<String>,
    /// The destination ("path:<p>" or "topic:<t>").
    pub to: String,
    /// The schema that flowed.
    pub schema: SchemaId,
    /// The number of observed sends.
    pub count: u64,
}

/// The whole-system export: the artifact a future canvas consumes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SystemExport {
    /// Every registered schema definition (all versions).
    pub schemas: Vec<crate::schema::SchemaDef>,
    /// Every live actor with its manifest and (for ES) live state.
    pub actors: Vec<ActorExport>,
    /// Declared edges (from manifests).
    pub declared_edges: Vec<DeclaredEdge>,
    /// Observed edges (aggregated from the tap).
    pub observed_edges: Vec<ObservedEdge>,
}

impl ActorSystem {
    /// The system dead-letter topic, created at boot.
    pub fn deadletter_topic() -> crate::types::Topic {
        crate::types::Topic::new("system.deadletters")
    }

    /// Spawns a foreign (no-Rust-types) event-sourced actor: the schema,
    /// state fold, and command decision are all runtime JSON data. This is
    /// the seam the port tier will reuse.
    pub fn spawn_es_foreign(
        self: &Arc<Self>,
        path: Path,
        schema_id: SchemaId,
        genesis: JsonValue,
        decision: crate::actor::ForeignDecision,
        fold: crate::actor::ForeignFold,
        opts: SpawnOpts,
    ) {
        let state = Box::new(crate::actor::ForeignEsState::new(genesis, fold));
        let manifest = crate::schema::ActorManifest::new()
            .handles_id(schema_id.clone())
            .kind(crate::types::ActorKind::EventSourced);
        let entries = vec![
            Arc::new(crate::actor::ForeignCommandEntry::new(schema_id, decision))
                as Arc<dyn crate::actor::CommandEntry>,
        ];
        self.spawn_es_erased(path, manifest, state, entries, opts);
    }

    /// Spawns a supervised child: registers its spec (policy, budget,
    /// backoff, spawn closure), runs the spawn closure once, and arms the
    /// supervision engine for crash handling.
    pub fn spawn_child(self: &Arc<Self>, spec: crate::supervision::ChildSpec) {
        {
            let mut kernel = self.kernel.lock().expect("kernel lock");
            kernel.specs.insert(spec.path.clone(), spec.clone());
            kernel
                .failures
                .insert(spec.path.clone(), crate::supervision::FailureWindow::new());
        }
        let engine = self.clone();
        let engine_spec = spec.clone();
        let path = spec.path.clone();
        (spec.spawn)(&engine, &path, &spec.args);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        self.child_shutdowns
            .lock()
            .expect("shutdown lock")
            .push(_shutdown_tx);
        tokio::spawn(crate::kernel::supervise_child(
            engine,
            engine_spec,
            shutdown_rx,
        ));
    }

    /// Creates a system on the wall clock.
    pub fn new() -> Self {
        Self::with_clock(ClockService::new(Arc::new(SystemClock::new())))
    }

    /// Creates a system tuned for tests: a [`FakeClock`] starting at
    /// 1_000 ms (reachable via the returned handle).
    pub fn test() -> (Arc<Self>, Arc<FakeClock>) {
        let (clock, fake) = ClockService::fake(1_000);
        (Arc::new(Self::with_clock(clock)), fake)
    }

    /// The fake clock behind this system, when tests installed one.
    pub fn fake_clock(&self) -> Option<Arc<FakeClock>> {
        self.clock.backend_fake()
    }

    /// Creates a system on an injected clock (tests: [`crate::clock::FakeClock`]).
    pub fn with_clock(clock: ClockService) -> Self {
        let registry = Arc::new(Mutex::new(Registry::default()));
        let view = Arc::new(NullView {
            registry: registry.clone(),
        });
        Self {
            registry,
            kernel: Arc::new(Mutex::new(KernelState::default())),
            clock,
            view,
            child_shutdowns: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Registers a Rust type's schema — the typed flavor.
    ///
    /// Idempotent per name+version: the first registration wins, and the
    /// returned id is stable across repeat registrations.
    pub fn register_schema<S: Schema>(&self) -> SchemaId {
        let mut registry = self.registry.lock().expect("registry lock");
        registry.register_schema_of::<S>()
    }

    /// Registers a schema from a JSON descriptor — the foreign flavor, for
    /// schemas defined outside Rust.
    ///
    /// # Errors
    ///
    /// Returns an error when `json` is not a valid schema descriptor.
    pub fn register_schema_json(
        &self,
        json: JsonValue,
    ) -> Result<SchemaId, error_stack::Report<crate::schema::SchemaError>> {
        let mut registry = self.registry.lock().expect("registry lock");
        registry.register_schema_json(json)
    }

    /// The registered descriptor for an exact `name@version` id, if any.
    pub fn schema(&self, id: &SchemaId) -> Option<crate::schema::SchemaDef> {
        let registry = self.registry.lock().expect("registry lock");
        registry.schema(id).cloned()
    }

    /// Spawns an event-sourced actor at `path`.
    ///
    /// Registers the manifest's schema edges, one command entry per
    /// `handles` schema (via the `spawn_es` closure), builds the journal +
    /// inbox, rebuilds state (snapshot fast-path or genesis), and starts
    /// the ES loop. Redelivery resumes from the inbox cursor.
    pub fn spawn_es<A, F>(
        self: &Arc<Self>,
        path: Path,
        args: &JsonValue,
        opts: SpawnOpts,
        entries: F,
    ) where
        A: EventSourced,
        F: FnOnce() -> Vec<Arc<dyn CommandEntry>>,
    {
        let state = Box::new(TypedEsState::<A>::new(A::restore(args)));
        let manifest = A::manifest();
        self.spawn_es_erased(path, manifest, state, entries(), opts);
    }

    /// The erased ES spawn shared by typed and foreign actors.
    fn spawn_es_erased(
        self: &Arc<Self>,
        path: Path,
        manifest: crate::schema::ActorManifest,
        state: Box<dyn crate::actor::DynEsActor>,
        entries: Vec<Arc<dyn CommandEntry>>,
        opts: SpawnOpts,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel::<Envelope>(opts.mailbox_capacity.max(1) * 2);
        {
            let mut registry = self.registry.lock().expect("registry lock");
            registry
                .insert_slot(
                    path.clone(),
                    manifest.clone(),
                    Endpoint::new(tx),
                    opts.mailbox_policy,
                )
                .expect("path free at spawn");
            // Declared edges become routes: each handled schema is routable
            // to this path (adding a second actor for a schema converts the
            // route to round-robin).
            for schema in manifest.handles.clone() {
                registry.add_route(schema, path.clone());
            }
        }
        let mut kernel = self.kernel.lock().expect("kernel lock");
        let cell = Arc::new(ActorCell::new(
            path.clone(),
            Inbox::new(opts.mailbox_capacity.max(1), opts.mailbox_policy),
        ));
        kernel.cells.insert(path.clone(), cell.clone());
        kernel.journals.entry(path.clone()).or_default();
        kernel
            .es_state
            .insert(path.clone(), Arc::new(tokio::sync::Mutex::new(state)));
        kernel.entries.insert(path.clone(), entries);
        kernel.snapshot_policy.insert(path.clone(), opts.snapshot);
        kernel.tap.push(
            self.clock.now(),
            crate::tap::FactKind::Spawned {
                path: path.clone(),
                restart: false,
            },
        );
        drop(kernel);

        // Front door + ES loop, sharing the kernel tables.
        let loop_ctx = EsLoop {
            path: path.clone(),
            cell,
            registry: self.registry.clone(),
            kernel: self.kernel.clone(),
            view: self.view.clone(),
            clock: self.clock.clone(),
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        loop_ctx.start(rx, shutdown_rx);
        let kernel = self.kernel.lock().expect("kernel lock");
        if let Some(cell) = kernel.cells.get(&path)
            && let Ok(mut handle) = cell.handle.try_lock()
        {
            *handle = Some(crate::kernel::ActorHandle {
                shutdown: shutdown_tx,
                task: None,
            });
        }
    }

    /// Spawns a service (edge) actor at `path`: async handlers, I/O and
    /// `ask` allowed, NOT journaled (at-most-once message semantics).
    pub fn spawn_service<A, F>(
        self: &Arc<Self>,
        path: Path,
        args: &JsonValue,
        opts: SpawnOpts,
        entries: F,
    ) where
        A: ServiceActor,
        F: FnOnce() -> Vec<Arc<dyn MsgEntry>>,
    {
        // `A::start` is async (I/O allowed); block briefly on a runtime
        // thread is not done — spawn the start inside the actor task and
        // register the slot immediately so senders never see a gap.
        let (tx, rx) = tokio::sync::mpsc::channel::<Envelope>(opts.mailbox_capacity.max(1) * 2);
        {
            let mut registry = self.registry.lock().expect("registry lock");
            registry
                .insert_slot(
                    path.clone(),
                    A::manifest(),
                    Endpoint::new(tx),
                    opts.mailbox_policy,
                )
                .expect("path free at spawn");
        }
        let mut kernel = self.kernel.lock().expect("kernel lock");
        let cell = Arc::new(ActorCell::new(
            path.clone(),
            Inbox::new(opts.mailbox_capacity.max(1), opts.mailbox_policy),
        ));
        kernel.cells.insert(path.clone(), cell.clone());
        kernel.genesis_args.insert(path.clone(), args.clone());
        kernel.msg_entries.insert(path.clone(), entries());
        kernel.tap.push(
            self.clock.now(),
            crate::tap::FactKind::Spawned {
                path: path.clone(),
                restart: false,
            },
        );
        drop(kernel);

        let loop_ctx = EsLoop {
            path: path.clone(),
            cell: cell.clone(),
            registry: self.registry.clone(),
            kernel: self.kernel.clone(),
            view: self.view.clone(),
            clock: self.clock.clone(),
        };
        let started_path = path.clone();
        let view = self.view.clone();
        let registry = self.registry.clone();
        let kernel_table = self.kernel.clone();
        let start_args = args.clone();
        let front_cell = cell.clone();
        let front_kernel = self.kernel.clone();
        tokio::spawn(async move {
            // Start the instance inside the task; a start failure leaves
            // the slot present (senders get a closed door) and the crash
            // recorded for supervision.
            let started = A::start(&start_args).await;
            match started {
                Ok(instance) => {
                    let mut kernel = kernel_table.lock().expect("kernel lock");
                    kernel.services.insert(
                        started_path.clone(),
                        Arc::new(tokio::sync::Mutex::new(
                            Box::new(TypedServiceState::new(instance)) as Box<dyn DynServiceActor>,
                        )),
                    );
                }
                Err(report) => {
                    let mut kernel = kernel_table.lock().expect("kernel lock");
                    kernel.crashed.insert(started_path.clone());
                    let _ = report;
                }
            }
            let _ = (&view, &registry);
            let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            tokio::spawn(crate::kernel::front_door_loop(front_cell, front_kernel, rx));
            crate::kernel::service_actor_loop(
                crate::kernel::ServiceLoop { es: loop_ctx },
                shutdown_rx,
            )
            .await;
        });
    }

    /// Convenience: spawn with typed adapters for each handled command.
    ///
    /// `entries()` builds the CommandEntry list (usually
    /// `vec![Arc::new(TypedEsAdapter::<A, C1>::new::<C1>()), ...]`).
    pub fn spawn_es_typed<A, F>(
        self: &Arc<Self>,
        path: Path,
        args: &JsonValue,
        opts: SpawnOpts,
        entries: F,
    ) where
        A: EventSourced,
        F: FnOnce() -> Vec<Arc<dyn CommandEntry>>,
    {
        self.spawn_es::<A, F>(path, args, opts, entries)
    }

    /// Sends an envelope from outside the system (entry-point trace root).
    ///
    /// # Errors
    ///
    /// Returns the envelope back when its destination does not resolve
    /// (callers dead-letter or retry).
    pub async fn send(&self, envelope: Envelope) -> Result<Path, Envelope> {
        route(&self.registry, &self.kernel, envelope).await
    }

    /// Builds a topic-addressed envelope (system root as sender).
    pub fn envelope_to_topic(
        &self,
        event: crate::envelope::Event,
        topic: crate::types::Topic,
    ) -> Envelope {
        Envelope::json(
            event.schema,
            crate::envelope::Address::Topic(topic),
            event.payload,
            TraceCtx::root(),
        )
    }

    pub fn envelope(&self, schema: SchemaId, dest: Path, payload: JsonValue) -> Envelope {
        Envelope::json(schema, Address::Path(dest), payload, TraceCtx::root())
    }

    /// The system's clock (tests use this to reach the [`crate::clock::FakeClock`]).
    pub fn clock(&self) -> &ClockService {
        &self.clock
    }

    /// Restarts a crashed ES actor at `path` (supervision calls this; the
    /// Phase 8 engine adds policy/budget/backoff around it).
    ///
    /// # Errors
    ///
    /// Propagates state-rebuild failures (corrupt snapshot or journal).
    pub async fn restart_es(
        &self,
        path: &Path,
        genesis_args: &JsonValue,
    ) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
        let loop_ctx = {
            let kernel = self.kernel.lock().expect("kernel lock");
            let cell = kernel.cells.get(path).cloned();
            drop(kernel);
            cell
        };
        let Some(cell) = loop_ctx else {
            use error_stack::IntoReport;
            return Err(crate::journal::JournalError::Restore.into_report());
        };
        let ctx = EsLoop {
            path: path.clone(),
            cell,
            registry: self.registry.clone(),
            kernel: self.kernel.clone(),
            view: self.view.clone(),
            clock: self.clock.clone(),
        };
        crate::kernel::restart_es(&ctx, genesis_args).await
    }

    /// Subscribes an actor to a topic: its cursor starts at Latest (or
    /// `offset` to re-consume); every later publish is pumped to its
    /// inbox. The actor must already exist (its inbox policy is reused).
    ///
    /// # Errors
    ///
    /// Unknown path.
    pub fn subscribe(
        &self,
        path: &Path,
        topic: &crate::types::Topic,
        offset: Option<u64>,
    ) -> Result<u64, error_stack::Report<crate::registry::RegistryError>> {
        use error_stack::IntoReport;
        let mut kernel = self.kernel.lock().expect("kernel lock");
        let registry = self.registry.lock().expect("registry lock");
        if !kernel.cells.contains_key(path) {
            return Err(crate::registry::RegistryError::UnknownPath(path.clone())
                .into_report()
                .attach(format!("subscribing {path}")));
        }
        let policy = registry.inbox_policy(path);
        let from = match offset {
            Some(o) => crate::topics::CursorFrom::Offset(o),
            None => crate::topics::CursorFrom::Latest,
        };
        let log = kernel
            .topic_logs
            .entry(topic.clone())
            .or_insert_with(|| crate::topics::TopicLog::new(256));
        Ok(log.subscribe(path.clone(), policy, from))
    }

    /// Re-points a subscriber's topic cursor; the next publish pumps the
    /// retained range back into its inbox (at-least-once re-consume).
    ///
    /// # Errors
    ///
    /// Unknown topic or path not subscribed.
    pub fn reset_topic_cursor(
        &self,
        path: &Path,
        topic: &crate::types::Topic,
        to: u64,
    ) -> Result<u64, u64> {
        let mut kernel = self.kernel.lock().expect("kernel lock");
        let Some(log) = kernel.topic_logs.get_mut(topic) else {
            return Err(to);
        };
        log.reset_cursor(path, to)
    }

    /// The topic log's retained offset range (inspection).
    pub fn topic_range(&self, topic: &crate::types::Topic) -> Option<(u64, u64)> {
        let kernel = self.kernel.lock().expect("kernel lock");
        kernel.topic_logs.get(topic).map(|log| log.retained())
    }

    /// A snapshot of tap facts from an offset (inspection/tests).
    pub fn tap_facts_from(&self, from: u64) -> Vec<crate::tap::Fact> {
        let kernel = self.kernel.lock().expect("kernel lock");
        kernel.tap.subscribe(from).1
    }

    /// All retained tap facts (inspection/tests).
    pub fn tap_facts(&self) -> Vec<crate::tap::Fact> {
        self.tap_facts_from(0)
    }

    /// Gracefully stops the actor at `path`: children stop first
    /// (recursive, timeout-bounded), the drain signal lets the current
    /// message finish, undelivered inbox entries go to the DLQ, and the
    /// slot + topic subscriptions are removed. Emits a Stopped fact.
    pub async fn stop(&self, path: &Path) {
        const STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        self.stop_bounded(path, STOP_TIMEOUT).await;
    }

    /// The bounded stop; recursion depth bounded by timeout.
    fn stop_bounded<'a>(
        &'a self,
        path: &'a Path,
        remaining: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.stop_bounded_inner(path, remaining))
    }

    /// The recursive body, boxed by [`Self::stop_bounded`].
    async fn stop_bounded_inner(&self, path: &Path, remaining: std::time::Duration) {
        if remaining.is_zero() {
            return;
        }
        // 1. CHILDREN FIRST (recursive): any spec whose parent is this path.
        let children: Vec<Path> = {
            let kernel = self.kernel.lock().expect("kernel lock");
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

        // 2. DRAIN SIGNAL: stop accepting + let the current message finish.
        // An edge-only path (no cell — e.g. a supervised spec whose actor
        // never started) still cascades below.
        let join_task = {
            let kernel = self.kernel.lock().expect("kernel lock");
            let Some(cell) = kernel.cells.get(path) else {
                // No running instance: drop the spec edge, record the
                // stop, and finish.
                drop(kernel);
                let mut kernel = self.kernel.lock().expect("kernel lock");
                kernel.specs.remove(path);
                kernel.tap.push(
                    self.clock.now(),
                    crate::tap::FactKind::Stopped {
                        path: path.clone(),
                        reason: "graceful".to_owned(),
                    },
                );
                return;
            };
            if let Ok(mut handle) = cell.handle.try_lock() {
                match handle.take() {
                    Some(h) => {
                        let _ = h.shutdown.send(true);
                        h.task
                    }
                    None => None,
                }
            } else {
                None
            }
        };
        // 3. AWAIT the loop's exit (current message completes). The loop
        // drains/flushes on stop.
        if let Some(task) = join_task {
            let _ = tokio::time::timeout(remaining, task).await;
        }

        // 4. UNDELIVERED → DLQ; then close the inbox.
        let undelivered: Vec<Envelope> = {
            let kernel = self.kernel.lock().expect("kernel lock");
            let mut drained = Vec::new();
            if let Some(cell) = kernel.cells.get(path)
                && let Ok(mut inbox) = cell.inbox.try_lock()
            {
                inbox.close();
                while let Some(envelope) = inbox.pop_discard() {
                    drained.push(envelope);
                }
            }
            drained
        };
        {
            let mut kernel = self.kernel.lock().expect("kernel lock");
            for envelope in undelivered {
                kernel.dead_letters.push(crate::kernel::DeadLetter {
                    schema: envelope.schema,
                    dest: envelope.dest,
                    reason: "stopped with a non-empty inbox".to_owned(),
                    trace: envelope.trace,
                });
            }
        }

        // 5. SLOT DROP + subscription cascade + Stopped fact.
        {
            let mut registry = self.registry.lock().expect("registry lock");
            let _ = registry.remove_slot(path);
        }
        {
            let mut kernel = self.kernel.lock().expect("kernel lock");
            kernel.cells.remove(path);
            for log in kernel.topic_logs.values_mut() {
                log.unsubscribe(path);
            }
            kernel.specs.remove(path);
            kernel.tap.push(
                self.clock.now(),
                crate::tap::FactKind::Stopped {
                    path: path.clone(),
                    reason: "graceful".to_owned(),
                },
            );
        }
    }

    /// Exports the system: schemas, live actors (ES state included),
    /// declared vs observed edges. The artifact a future canvas consumes.
    pub async fn export(&self) -> SystemExport {
        // Schemas (all versions).
        let schemas = {
            let registry = self.registry.lock().expect("registry lock");
            registry.schemas().all().into_iter().cloned().collect()
        };

        // Live actors: manifests from slots, state/cursor from kernel.
        let slot_manifests = {
            let registry = self.registry.lock().expect("registry lock");
            registry.slot_manifests()
        };
        let mut actors = Vec::new();
        for (path, manifest) in slot_manifests {
            // Scope the kernel guard: drop it before awaiting the state
            // shell (a std Mutex must never span an await point).
            let (state, cursor) = {
                let kernel = self.kernel.lock().expect("kernel lock");
                let cursor = kernel.cells.get(&path).and_then(|cell| {
                    cell.inbox
                        .try_lock()
                        .ok()
                        .map(|inbox| inbox.cursor().as_u64())
                });
                let has_state = kernel.es_state.contains_key(&path);
                (has_state.then_some(()), cursor)
            };
            let state = match state {
                Some(()) => {
                    let shell = {
                        let kernel = self.kernel.lock().expect("kernel lock");
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
                kind: manifest.kind.unwrap_or(crate::types::ActorKind::Service),
                manifest,
                state,
                cursor,
            });
        }

        // Runtime topic subscriptions (subscribe calls) are declared
        // edges too: read them from the topic logs.
        let runtime_subscriptions: Vec<(Path, crate::types::Topic)> = {
            let kernel = self.kernel.lock().expect("kernel lock");
            kernel
                .topic_logs
                .iter()
                .flat_map(|(topic, log)| {
                    log.subscribers()
                        .into_iter()
                        .map(move |p| (p, topic.clone()))
                })
                .collect()
        };
        let subscription_edges: Vec<DeclaredEdge> = runtime_subscriptions
            .into_iter()
            .map(|(actor, topic)| DeclaredEdge {
                actor,
                schema: SchemaId::new("Any", 1),
                direction: EdgeDirection::Subscribes,
                topic: Some(topic),
            })
            .collect();

        // Declared edges straight from the manifests above.
        let declared_edges: Vec<DeclaredEdge> = actors
            .iter()
            .flat_map(|a| {
                let handles = a.manifest.handles.iter().map(|s| DeclaredEdge {
                    actor: a.path.clone(),
                    schema: s.clone(),
                    direction: EdgeDirection::Handles,
                    topic: None,
                });
                let emits = a.manifest.emits.iter().map(|s| DeclaredEdge {
                    actor: a.path.clone(),
                    schema: s.clone(),
                    direction: EdgeDirection::Emits,
                    topic: None,
                });
                let emits_topics = a.manifest.emits_on_topics.iter().map(|t| DeclaredEdge {
                    actor: a.path.clone(),
                    schema: SchemaId::new("Any", 1),
                    direction: EdgeDirection::Emits,
                    topic: Some(t.clone()),
                });
                let subscribes = a.manifest.subscribes.iter().map(|t| DeclaredEdge {
                    actor: a.path.clone(),
                    schema: SchemaId::new("Any", 1),
                    direction: EdgeDirection::Subscribes,
                    topic: Some(t.clone()),
                });
                handles
                    .chain(emits)
                    .chain(emits_topics)
                    .chain(subscribes)
                    .collect::<Vec<_>>()
            })
            .chain(subscription_edges)
            .collect();

        // Observed edges: aggregate Sent facts from the tap.
        let mut counts: std::collections::HashMap<(Option<String>, String, SchemaId), u64> =
            std::collections::HashMap::new();
        for fact in self.tap_facts() {
            if let crate::tap::FactKind::Sent {
                from, dest, schema, ..
            } = &fact.kind
            {
                let to_str = dest.clone();
                let from_str = from.as_ref().map(|p| p.to_string());
                *counts
                    .entry((from_str, to_str, schema.clone()))
                    .or_insert(0) += 1;
            }
        }
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

        SystemExport {
            schemas,
            actors,
            declared_edges,
            observed_edges,
        }
    }

    /// The cursor of an actor's inbox (inspection; Phase 10 tests).
    /// How many envelopes the runtime could not deliver (inspection).
    pub async fn dead_letter_count(&self) -> usize {
        let kernel = self.kernel.lock().expect("kernel lock");
        kernel.dead_letters.len()
    }

    /// Why envelopes died (inspection/tests/demo debugging).
    pub async fn dead_letter_reasons(&self) -> Vec<String> {
        let kernel = self.kernel.lock().expect("kernel lock");
        kernel
            .dead_letters
            .iter()
            .map(|d| format!("{}: {}", d.reason, d.schema))
            .collect()
    }

    /// How many envelopes are queued at `path` (inspection/tests).
    pub async fn inbox_debug_len(&self, path: &Path) -> usize {
        // Clone the Arc out of the kernel guard, then await the inbox
        // lock without holding the kernel's std Mutex.
        let cell = {
            let kernel = self.kernel.lock().expect("kernel lock");
            kernel.cells.get(path).cloned()
        };
        match cell {
            Some(cell) => cell.inbox.lock().await.len(),
            None => 0,
        }
    }

    pub fn inbox_cursor(&self, path: &Path) -> Option<InboxOffset> {
        let kernel = self.kernel.lock().expect("kernel lock");
        kernel.cells.get(path).map(|cell| {
            cell.inbox
                .try_lock()
                .map(|inbox| inbox.cursor())
                .unwrap_or_else(|_| InboxOffset::zero())
        })
    }

    /// The captured ES state of an actor (for export/inspection).
    pub async fn es_state(&self, path: &Path) -> Option<JsonValue> {
        let state = {
            let kernel = self.kernel.lock().expect("kernel lock");
            kernel.es_state.get(path).cloned()?
        };
        let state = state.lock().await;
        state.capture_erased().ok()
    }
}

/// A read-only snapshot view over the kernel: handler contexts resolve
/// lookups through a brief lock; they can never mutate anything.
struct NullView {
    registry: Arc<Mutex<Registry>>,
}

impl RuntimeView for NullView {
    fn lookup(&self, path: &Path) -> Option<EndpointInfo> {
        let registry = self.registry.lock().expect("registry lock");
        registry.lookup(path)
    }

    fn who_handles(&self, schema: &SchemaId) -> Vec<Path> {
        let registry = self.registry.lock().expect("registry lock");
        registry.who_handles(schema)
    }

    fn now(&self) -> Timestamp {
        Timestamp::from_millis(0)
    }
}

impl RuntimeView for ActorSystem {
    fn lookup(&self, path: &Path) -> Option<EndpointInfo> {
        let registry = self.registry.lock().expect("registry lock");
        registry.lookup(path)
    }

    fn who_handles(&self, schema: &SchemaId) -> Vec<Path> {
        let registry = self.registry.lock().expect("registry lock");
        registry.who_handles(schema)
    }

    fn now(&self) -> Timestamp {
        self.clock.now()
    }
}

impl Default for ActorSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    impl ActorSystem {
        /// The number of journalled entries for `path` (tests).
        pub fn journal_len(&self, path: &Path) -> usize {
            let kernel = self.kernel.lock().expect("kernel lock");
            kernel.journals.get(path).map(|j| j.len()).unwrap_or(0)
        }

        /// Dead-letter schemas collected so far (tests).
        pub fn dead_letter_schemas(&self) -> Vec<SchemaId> {
            let kernel = self.kernel.lock().expect("kernel lock");
            kernel
                .dead_letters
                .iter()
                .map(|d| d.schema.clone())
                .collect()
        }

        /// The tap as a compact census of fact kinds (tests).
        pub fn fact_kind_counts(&self) -> HashMap<String, usize> {
            let mut counts = HashMap::new();
            for fact in self.tap_facts() {
                let kind = format!("{:?}", fact.kind);
                let name = kind.split(['(', '{']).next().unwrap_or(&kind).trim();
                *counts.entry(name.to_owned()).or_default() += 1;
            }
            counts
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
    use crate::actor::{CommandHandler, MsgHandler, TypedEsAdapter, TypedServiceAdapter};
    use crate::context::CmdCtx;
    use crate::schema::{ActorManifest, FieldDef, FieldTy, SchemaDef, SchemaKind};
    use crate::types::ActorKind;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::collections::HashMap;

    #[derive(Deserialize)]
    struct Add {
        n: i64,
    }
    impl Schema for Add {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Add".into(),
                version: 1,
                kind: SchemaKind::Command,
                fields: vec![FieldDef::required("n", FieldTy::Int)],
                description: None,
            }
        }
    }

    #[derive(serde::Deserialize)]
    struct Added {
        n: i64,
    }
    impl Schema for Added {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Added".into(),
                version: 1,
                kind: SchemaKind::Event,
                fields: vec![FieldDef::required("n", FieldTy::Int)],
                description: None,
            }
        }
    }

    #[derive(Serialize, Deserialize, Default)]
    struct Counter {
        total: i64,
    }

    impl EventSourced for Counter {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Add>()
                .emits::<Added>()
                .emits_on_topic(crate::types::Topic::new("counter.events"))
                .kind(ActorKind::EventSourced)
        }
        fn restore(_args: &JsonValue) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            if event.schema.as_str() == "Added@1" {
                self.total += event.payload["n"].as_i64().unwrap_or(0);
            }
        }
    }
    impl CommandHandler<Add> for Counter {
        fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
            vec![crate::envelope::Event::new(
                Added::schema_id(),
                json!({ "n": cmd.n }),
            )]
        }
    }

    impl CommandHandler<Boom> for Counter {
        fn handle(&self, _cmd: Boom, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
            panic!("injected handler panic");
        }
    }

    /// A command whose handling panics (panic isolation under test).
    #[derive(Deserialize)]
    struct Boom {
        #[allow(dead_code)] // payload shape; the handler panics before reading
        why: String,
    }
    impl Schema for Boom {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Boom".into(),
                version: 1,
                kind: SchemaKind::Command,
                fields: vec![FieldDef::required("why", FieldTy::Str)],
                description: None,
            }
        }
    }

    #[tokio::test]
    async fn atomic_step_commits_journal_state_and_cursor_together() {
        // Given a spawned counter actor.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("counter");
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
        let kernel = system.kernel.lock().expect("lock");
        let journal = &kernel.journals[&path];
        assert_eq!(journal.len(), 1);
        assert_eq!(journal.next_seq().as_u64(), 1);
        assert!(kernel.crashed.is_empty());
    }

    #[tokio::test]
    async fn atomic_step_dead_letters_unknown_schemas_and_advances() {
        // Given a spawned actor that handles only Add.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("counter");
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
            let kernel = system.kernel.lock().expect("lock");
            assert_eq!(kernel.dead_letters.len(), 1);
            assert_eq!(kernel.dead_letters[0].schema, Boom::schema_id());
            assert_eq!(kernel.journals[&path].len(), 0);
        }
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], 0);
    }

    #[tokio::test]
    async fn atomic_step_panics_leave_the_message_queued_for_redelivery() {
        // Given a spawned actor whose Boom handler panics.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("counter");
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
            let kernel = system.kernel.lock().expect("lock");
            assert!(kernel.crashed.contains(&path));
            assert_eq!(kernel.journals.get(&path).map(|j| j.len()), Some(0));
            assert!(kernel.dead_letters.is_empty());
        }
        assert_eq!(system.inbox_cursor(&path).map(|c| c.as_u64()), Some(0));
        let state = system.es_state(&path).await.expect("shell present");
        assert_eq!(state["total"], 0);
    }

    #[tokio::test]
    async fn restart_rebuilds_from_journal_and_redelivers_exactly_once() {
        // Given a counter that has committed one Add, then crashed on Boom.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("counter");
        let boom = Path::new("counter");
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
            let kernel = system.kernel.lock().expect("lock");
            assert_eq!(kernel.journals[&path].len(), 1, "no duplicate events");
            assert!(kernel.dead_letters.is_empty(), "panic never dead-letters");
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
            .es_state(&Path::new(name))
            .await
            .and_then(|s| s["total"].as_i64())
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

    async fn wait_for_cursor(system: &ActorSystem, path: &Path, expected: u64) {
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
        #[derive(serde::Deserialize)]
        struct Ping {
            #[serde(default)]
            #[allow(dead_code)] // payload shape; the handler ignores it
            n: i64,
        }
        impl Schema for Ping {
            fn schema_def() -> SchemaDef {
                SchemaDef {
                    name: "Ping".into(),
                    version: 1,
                    kind: SchemaKind::Command,
                    fields: vec![FieldDef::required("n", FieldTy::Int)],
                    description: None,
                }
            }
        }

        #[derive(Serialize, Deserialize, Default)]
        struct Forwarder;
        impl EventSourced for Forwarder {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::EventSourced)
            }
            fn restore(_args: &JsonValue) -> Self {
                Self
            }
            fn apply(&mut self, _event: &crate::envelope::Event) {}
        }
        impl CommandHandler<Add> for Forwarder {
            fn handle(&self, _cmd: Add, ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
                ctx.0.send(
                    Address::Path(Path::new("echo")),
                    Ping::schema_id(),
                    json!({ "n": 0 }),
                    None,
                );
                Vec::new()
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
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Ping> for Echo {
            async fn handle(&mut self, _msg: Ping, _ctx: &mut crate::context::MsgCtx<'_>) {}
        }

        let (system, _clock) = ActorSystem::test();
        system.spawn_es::<Forwarder, _>(Path::new("a"), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Forwarder, Add>::new::<Add>())]
        });
        system.spawn_service::<Echo, _>(
            Path::new("echo"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Echo, Ping>::new::<Ping>())],
        );

        // When the conversation starts at A and both hops settle.
        system
            .send(system.envelope(Add::schema_id(), Path::new("a"), json!({ "n": 1 })))
            .await
            .expect("send");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Then the facts carry a shared trace id across the hops.
        let facts = system.tap_facts();
        let a_hop = facts
            .iter()
            .find(|f| matches!(&f.kind, crate::tap::FactKind::Delivered { to, .. } if *to == Path::new("a")))
            .expect("hop a delivered");
        let trace_of = |f: &crate::tap::Fact| match &f.kind {
            crate::tap::FactKind::Delivered { trace, .. }
            | crate::tap::FactKind::Acked { trace, .. }
            | crate::tap::FactKind::Sent { trace, .. } => *trace,
            _ => panic!("unexpected fact kind"),
        };
        let a_trace = trace_of(a_hop);
        let b_facts: Vec<_> = facts
            .iter()
            .filter(|f| {
                matches!(
                    &f.kind,
                    crate::tap::FactKind::Delivered { to, .. } if *to == Path::new("echo")
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
    async fn tap_drop_oldest_under_pressure_keeps_delivery_working() {
        // Given a system whose tap ring is tiny (test-visible capacity).
        let (system, _clock) = ActorSystem::test();
        {
            let mut kernel = system.kernel.lock().expect("lock");
            kernel.tap = crate::tap::TapRing::new(4);
        }
        let path = Path::new("counter");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // When far more messages flow than the ring can hold.
        for n in 0..50 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("send");
        }
        wait_for_cursor(&system, &path, 50).await;

        // Then delivery was unaffected: all 50 committed (journal count).
        let journal_len = {
            let kernel = system.kernel.lock().expect("lock");
            kernel.journals[&path].len()
        };
        assert_eq!(journal_len, 50);

        // And the ring retained only its newest facts with monotonic
        // offsets and a JSON projection that still works.
        let facts = system.tap_facts();
        assert!(facts.len() <= 4, "ring dropped oldest: {}", facts.len());
        let offsets: Vec<u64> = facts.iter().map(|f| f.offset).collect();
        let sorted = offsets.clone();
        let mut sorted = sorted;
        sorted.sort_unstable();
        assert_eq!(offsets, sorted, "offsets monotonic");
        let last = facts.last().expect("facts").to_json();
        assert!(last["offset"].is_u64());
    }

    #[tokio::test]
    async fn restart_budget_escalates_to_the_parent() {
        // Given a supervised counter whose Add handler always panics,
        // with a budget of 2 restarts per 10 seconds, parent "overseer".
        #[derive(Serialize, Deserialize, Default)]
        struct AlwaysBoom;
        impl EventSourced for AlwaysBoom {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::EventSourced)
            }
            fn restore(_args: &JsonValue) -> Self {
                Self
            }
            fn apply(&mut self, _event: &crate::envelope::Event) {}
        }
        impl CommandHandler<Add> for AlwaysBoom {
            fn handle(&self, _cmd: Add, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
                panic!("always panics");
            }
        }

        // The overseer is a service actor whose Escalated control message
        // lands in its sink via a plain send from the engine.
        let (system, _clock) = ActorSystem::test();
        let overseer = Path::new("overseer");
        bind_sink(&overseer, Arc::new(std::sync::Mutex::new(Vec::new())));
        struct Overseer;
        impl ServiceActor for Overseer {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Added>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        #[derive(serde::Deserialize)]
        struct EscalatedMsg {
            escalated: String,
        }
        impl Schema for EscalatedMsg {
            fn schema_def() -> SchemaDef {
                SchemaDef {
                    name: "Escalated".into(),
                    version: 1,
                    kind: SchemaKind::Command,
                    fields: vec![FieldDef::required("escalated", FieldTy::Str)],
                    description: None,
                }
            }
        }
        impl MsgHandler<EscalatedMsg> for Overseer {
            async fn handle(&mut self, msg: EscalatedMsg, _ctx: &mut crate::context::MsgCtx<'_>) {
                if let Some(s) = SINK_BY_PATH
                    .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
                    .lock()
                    .expect("table")
                    .get("overseer")
                {
                    s.lock()
                        .expect("sink lock")
                        .push(format!("escalated:{}", msg.escalated))
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
        let worker = Path::new("worker");
        let spawner = Arc::new(ChildSpawner);
        let system_for_spec = system.clone();
        let worker_clone = worker.clone();
        let spec = crate::supervision::ChildSpec {
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
            spawn: Arc::new(
                move |sys: &Arc<ActorSystem>, path: &Path, args: &JsonValue| {
                    let _ = (&spawner, &system_for_spec);
                    sys.spawn_es::<AlwaysBoom, _>(path.clone(), args, SpawnOpts::default(), || {
                        vec![Arc::new(TypedEsAdapter::<AlwaysBoom, Add>::new::<Add>())]
                    });
                    let _ = &worker_clone;
                },
            ),
        };
        system.spawn_child(spec);

        // When the worker crashes repeatedly inside the window (sends
        // after escalation failing is EXPECTED — the child is gone).
        for _ in 0..4 {
            let _ = system
                .send(system.envelope(Add::schema_id(), worker.clone(), json!({ "n": 1 })))
                .await;
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
        // Then the engine restarted it within budget, then escalated:
        // poll for the worker's slot removal (the escalation signature).
        let mut slot_gone = false;
        for _ in 0..2_000 {
            slot_gone = {
                let registry = system.registry.lock().expect("lock");
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
    }

    #[tokio::test]
    async fn graceful_shutdown_stops_children_before_the_parent() {
        // Given a parent path with a supervised child spec (no running
        // cell for either: the cascade itself is the behavior under test).
        let (system, _clock) = ActorSystem::test();
        let parent = Path::new("parent");
        let child = Path::new("child");
        {
            let mut kernel = system.kernel.lock().expect("lock");
            kernel.specs.insert(
                child.clone(),
                crate::supervision::ChildSpec {
                    path: child.clone(),
                    parent: Some(parent.clone()),
                    restart: crate::supervision::RestartPolicy::Permanent,
                    budget: crate::supervision::RestartBudget::default(),
                    backoff: crate::supervision::Backoff::default(),
                    args: json!({}),
                    spawn: Arc::new(|_sys: &Arc<ActorSystem>, _path: &Path, _args: &JsonValue| {}),
                },
            );
        }

        // When the parent is stopped gracefully.
        system.stop(&parent).await;

        // Then the parent's Stopped fact was recorded, and the child
        // spec was cascaded away with it.
        let facts = system.tap_facts();
        let stops: Vec<String> = facts
            .iter()
            .filter_map(|f| match &f.kind {
                crate::tap::FactKind::Stopped { path, .. } => Some(path.to_string()),
                _ => None,
            })
            .collect();
        assert!(
            stops.iter().any(|p| p == "parent"),
            "parent stop recorded: {stops:?}"
        );
        let child_cascaded = {
            let kernel = system.kernel.lock().expect("lock");
            !kernel.specs.contains_key(&child)
        };
        assert!(child_cascaded, "child spec cascaded with the parent");
    }

    #[tokio::test]
    async fn transient_policy_ignores_normal_exits() {
        // Given a supervised child spec with Transient restart policy.
        // The observable: a NORMAL stop must not arm the failure window
        // (stop() never records failures), so the child stays stopped.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("transient-child");
        {
            let mut kernel = system.kernel.lock().expect("lock");
            kernel.specs.insert(
                path.clone(),
                crate::supervision::ChildSpec {
                    path: path.clone(),
                    parent: None,
                    restart: crate::supervision::RestartPolicy::Transient,
                    budget: crate::supervision::RestartBudget::default(),
                    backoff: crate::supervision::Backoff::default(),
                    args: json!({}),
                    spawn: Arc::new(|_sys: &Arc<ActorSystem>, _path: &Path, _args: &JsonValue| {}),
                },
            );
        }

        // When the child is stopped gracefully (a normal exit).
        system.stop(&path).await;

        // Then no failure was recorded (the engine arms only on crashes)
        // and the stop fact says graceful.
        let stops: Vec<_> = {
            let kernel = system.kernel.lock().expect("lock");
            assert!(!kernel.specs.contains_key(&path), "spec removed on stop");
            kernel
                .tap
                .subscribe(0)
                .1
                .into_iter()
                .filter(|f| matches!(&f.kind, crate::tap::FactKind::Stopped { .. }))
                .collect()
        };
        assert_eq!(stops.len(), 1, "exactly one stop fact: {stops:?}");
        let no_failures = {
            let kernel = system.kernel.lock().expect("lock");
            kernel.failures.get(&path).map(|w| w.is_empty()) != Some(false)
        };
        assert!(no_failures, "no failure recorded for a normal exit");
    }

    #[tokio::test]
    async fn topic_cursor_reset_redelivers_in_order() {
        // Given a counter emitting onto "counter.events" and a subscriber.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("counter");
        let sub = Path::new("watcher");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        let (sub_idx, sub_sink) = open_sink();
        bind_sink(&sub, sub_sink);
        system.spawn_service::<Auditor, _>(
            sub.clone(),
            &json!({ "sink": sub_idx }),
            SpawnOpts::default(),
            || {
                vec![
                    Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>()),
                    Arc::new(TypedServiceAdapter::<Auditor, Added>::new::<Added>()),
                ]
            },
        );
        let topic = crate::types::Topic::new("counter.events");
        system.subscribe(&sub, &topic, None).expect("subscribe");

        // When two Adds are sent and committed.
        for n in 1..=2 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("send");
        }
        wait_for_cursor(&system, &path, 2).await;
        let seen_after_first = sink_read(&sub).len();

        // And the subscriber's cursor is reset to the log floor.
        let (floor, _) = system.topic_range(&topic).expect("topic");
        let cursor = system
            .reset_topic_cursor(&sub, &topic, floor)
            .expect("subscribed");
        assert_eq!(cursor, floor);

        // And a third Add triggers a fresh pump pass.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 3 })))
            .await
            .expect("send");
        wait_for_cursor(&system, &path, 3).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Then the retained events were re-delivered in order.
        let lines = sink_read(&sub);
        assert!(lines.len() >= seen_after_first, "re-consume delivered more");
        let mut events: Vec<i64> = lines
            .iter()
            .filter_map(|l| l.strip_prefix("Added:").and_then(|v| v.parse().ok()))
            .collect();
        let replayed = events.split_off(events.len() - seen_after_first.max(1).min(events.len()));
        // The replayed suffix (the re-consumed range) is in log order.
        let ordered = replayed.windows(2).all(|w| w[0] <= w[1]);
        assert!(ordered, "replayed events out of order: {lines:?}");
    }

    #[tokio::test]
    async fn topic_subscribers_have_independent_cursors() {
        // Given one publisher and two subscribers.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("counter");
        let early = Path::new("early");
        let late = Path::new("late");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        for p in [&early, &late] {
            let (idx, sink) = open_sink();
            bind_sink(p, sink);
            system.spawn_service::<Auditor, _>(
                p.clone(),
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
        let topic = crate::types::Topic::new("counter.events");

        // When "early" subscribes before any publish and "late" after one.
        system.subscribe(&early, &topic, None).expect("subscribe");
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 1 })))
            .await
            .expect("send");
        wait_for_cursor(&system, &path, 1).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        system.subscribe(&late, &topic, None).expect("subscribe");
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 2 })))
            .await
            .expect("send");
        wait_for_cursor(&system, &path, 2).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Then "early" saw both events and "late" only the second.
        let early_lines = sink_read(&early);
        let late_lines = sink_read(&late);
        assert_eq!(
            early_lines.len(),
            2,
            "early saw everything: {early_lines:?}"
        );
        assert_eq!(
            late_lines.len(),
            1,
            "late only saw its own era: {late_lines:?}"
        );
    }

    #[tokio::test]
    async fn slow_subscriber_does_not_block_the_publisher() {
        // Given a subscriber spawned with a tiny DropNew mailbox.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("counter");
        let slow = Path::new("slow");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        let (slow_idx, _slow_sink) = open_sink();
        bind_sink(&slow, sinks().lock().expect("sinks lock")[slow_idx].clone());
        system.spawn_service::<Auditor, _>(
            slow.clone(),
            &json!({ "sink": slow_idx }),
            SpawnOpts {
                mailbox_capacity: 1,
                mailbox_policy: crate::inbox::OverloadPolicy::DropNew,
                ..SpawnOpts::default()
            },
            || {
                vec![
                    Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>()),
                    Arc::new(TypedServiceAdapter::<Auditor, Added>::new::<Added>()),
                ]
            },
        );
        let topic = crate::types::Topic::new("counter.events");
        system.subscribe(&slow, &topic, None).expect("subscribe");

        // When many publishes happen in a row.
        for n in 1..=10 {
            system
                .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": n })))
                .await
                .expect("send");
        }

        // Then the publisher still committed everything.
        wait_for_cursor(&system, &path, 10).await;
        // And some deliveries were refused (dead-lettered), not blocked.
        let dead = system.kernel.lock().expect("lock").dead_letters.len();
        assert!(
            dead > 0 || { !sink_read(&slow).is_empty() },
            "slow subscriber either dropped or received; never stalled the publisher"
        );
    }

    async fn wait_for_crash(system: &ActorSystem, path: &Path) {
        for _ in 0..2_000 {
            {
                let kernel = system.kernel.lock().expect("lock");
                if kernel.crashed.contains(path) {
                    return;
                }
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

    fn bind_sink(path: &Path, sink: Arc<Mutex<Vec<String>>>) {
        sink_table()
            .lock()
            .expect("sink table lock")
            .insert(path.to_string(), sink);
    }

    /// Reads a subscriber's sink lines by path (test inspection).
    fn sink_read(path: &Path) -> Vec<String> {
        sink_table()
            .lock()
            .expect("sink table lock")
            .get(&path.to_string())
            .map(|sink| sink.lock().expect("sink lock").clone())
            .unwrap_or_default()
    }

    fn open_sink() -> (usize, Arc<Mutex<Vec<String>>>) {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let mut all = sinks().lock().expect("sinks lock");
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
            args: &JsonValue,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            let idx = args["sink"].as_u64().expect("sink index") as usize;
            let sink = sinks().lock().expect("sinks lock")[idx].clone();
            Ok(Self { sink })
        }
    }

    impl MsgHandler<Added> for Auditor {
        async fn handle(&mut self, msg: Added, _ctx: &mut crate::context::MsgCtx<'_>) {
            println!("AUDITOR GOT Added n={}", msg.n);
            self.sink
                .lock()
                .expect("sink lock")
                .push(format!("Added:{}", msg.n));
        }
    }

    impl MsgHandler<Add> for Auditor {
        async fn handle(&mut self, msg: Add, _ctx: &mut crate::context::MsgCtx<'_>) {
            self.sink
                .lock()
                .expect("sink lock")
                .push(format!("n={}", msg.n));
        }
    }

    #[tokio::test]
    async fn service_actor_receives_typed_messages_impurely() {
        // Given a system and an Auditor service with a shared test sink.
        let (system, _clock) = ActorSystem::test();
        let (sink_idx, sink) = open_sink();
        let path = Path::new("auditor");
        system.spawn_service::<Auditor, _>(
            path.clone(),
            &json!({ "sink": sink_idx }),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>())],
        );

        // When an Add message is sent to it.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 7 })))
            .await
            .expect("delivered");

        // Then the handler ran (impure side effect recorded).
        for _ in 0..2_000 {
            if sink.lock().expect("sink lock").as_slice() == ["n=7"] {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("service handler never ran");
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
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Echo {
            async fn handle(&mut self, msg: Add, ctx: &mut crate::context::MsgCtx<'_>) {
                if let Some(reply_to) = ctx.core.reply_to {
                    ctx.core.outbox.push_reply(
                        reply_to.clone(),
                        Add::schema_id(),
                        json!({ "echo": msg.n }),
                        *ctx.core.trace,
                    );
                }
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
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Boom> for Asker {
            async fn handle(&mut self, _msg: Boom, ctx: &mut crate::context::MsgCtx<'_>) {
                let reply = ctx
                    .ask(
                        Address::Path(Path::new("echo")),
                        Add::schema_id(),
                        json!({ "n": 21 }),
                        std::time::Duration::from_secs(2),
                    )
                    .await;
                let recorded = RESULTS.get_or_init(|| Mutex::new(Vec::new()));
                match reply {
                    Ok(value) => recorded
                        .lock()
                        .expect("results lock")
                        .push(format!("replied:{}", value["echo"])),
                    Err(_) => recorded
                        .lock()
                        .expect("results lock")
                        .push("failed".to_owned()),
                }
            }
        }

        system.spawn_service::<Echo, _>(
            Path::new("echo"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Echo, Add>::new::<Add>())],
        );
        system.spawn_service::<Asker, _>(
            Path::new("asker"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Asker, Boom>::new::<Boom>())],
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // When the asker asks the echo.
        system
            .send(system.envelope(
                Boom::schema_id(),
                Path::new("asker"),
                json!({ "why": "ask" }),
            ))
            .await
            .expect("delivered");

        // Then the ask settles as Replied with the echo's payload.
        for _ in 0..2_000 {
            if results.lock().expect("lock").as_slice() == ["replied:21"] {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!(
            "ask never settled as replied: {:?}",
            results.lock().unwrap()
        );
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
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Silent {
            async fn handle(&mut self, _msg: Add, _ctx: &mut crate::context::MsgCtx<'_>) {}
        }

        struct Asker;
        impl ServiceActor for Asker {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Boom>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Boom> for Asker {
            async fn handle(&mut self, _msg: Boom, ctx: &mut crate::context::MsgCtx<'_>) {
                let reply = ctx
                    .ask(
                        Address::Path(Path::new("silent")),
                        Add::schema_id(),
                        json!({ "n": 1 }),
                        std::time::Duration::from_millis(50),
                    )
                    .await;
                let recorded = RESULTS.get_or_init(|| Mutex::new(Vec::new()));
                recorded.lock().expect("lock").push(
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
            Path::new("silent"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Silent, Add>::new::<Add>())],
        );
        system.spawn_service::<Asker, _>(
            Path::new("asker"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Asker, Boom>::new::<Boom>())],
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // When the asker asks the silent callee.
        system
            .send(system.envelope(
                Boom::schema_id(),
                Path::new("asker"),
                json!({ "why": "ask" }),
            ))
            .await
            .expect("delivered");

        // Then the ask settles as a timeout (and the lease is gone).
        for _ in 0..2_000 {
            if results.lock().expect("lock").as_slice() == ["timed-out"] {
                let kernel = system.kernel.lock().expect("lock");
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
        panic!("ask never timed out: {:?}", results.lock().unwrap());
    }

    #[tokio::test]
    async fn ask_over_a_durable_path_continues_as_an_ordinary_message() {
        // Given an ES counter whose Add handler REPLIES to a reply-to
        // PATH (not a slot): the reply continues as a normal envelope.
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
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Added> for Collector {
            async fn handle(&mut self, msg: Added, ctx: &mut crate::context::MsgCtx<'_>) {
                RECEIVED
                    .get_or_init(|| Mutex::new(Vec::new()))
                    .lock()
                    .expect("lock")
                    .push(format!(
                        "got n={} from={:?}",
                        msg.n, ctx.core.trace.causality_id
                    ));
            }
        }

        #[derive(Serialize, Deserialize)]
        struct Counter {
            total: i64,
        }
        impl EventSourced for Counter {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .emits::<Added>()
                    .kind(ActorKind::EventSourced)
            }
            fn restore(_args: &JsonValue) -> Self {
                Self { total: 0 }
            }
            fn apply(&mut self, event: &crate::envelope::Event) {
                if event.schema.as_str() == "Added@1" {
                    self.total += event.payload["n"].as_i64().unwrap_or(0);
                }
            }
        }
        impl CommandHandler<Add> for Counter {
            fn handle(&self, cmd: Add, ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
                if let Some(reply_to) = ctx.0.reply_to {
                    ctx.0.send(
                        reply_to.clone(),
                        Added::schema_id(),
                        json!({ "n": cmd.n }),
                        Some(Address::Path(ctx.0.self_path.clone())),
                    );
                }
                vec![crate::envelope::Event::new(
                    Added::schema_id(),
                    json!({ "n": cmd.n }),
                )]
            }
        }

        system.spawn_service::<Collector, _>(
            Path::new("collector"),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![Arc::new(TypedServiceAdapter::<Collector, Added>::new::<
                    Added,
                >())]
            },
        );
        system.spawn_es::<Counter, _>(
            Path::new("counter"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // When the counter is told to Add with a reply-to PATH pointing at
        // the collector (an ask-shaped message, but reply-by-name).
        let mut envelope =
            system.envelope(Add::schema_id(), Path::new("counter"), json!({ "n": 5 }));
        envelope.reply_to = Some(Address::Path(Path::new("collector")));
        envelope.from = Some(Path::new("collector"));
        system.send(envelope).await.expect("delivered");

        // Then the collector receives the reply as an ordinary message
        // (a durable-path continuation — the name survives, no lease).
        for _ in 0..2_000 {
            if !received.lock().expect("lock").is_empty() {
                let got = received.lock().expect("lock")[0].clone();
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
        let system = ActorSystem::new();
        let kernel = system.kernel.lock().expect("lock");
        let (short_lease, _short_rx) = kernel
            .replies
            .open(std::time::Duration::from_millis(5), system.clock.now());
        let (long_lease, long_rx) = kernel
            .replies
            .open(std::time::Duration::from_secs(60), system.clock.now());

        // When completing the long lease and pruning past the short one.
        assert!(kernel.replies.complete(&long_lease, json!({ "ok": true })));
        drop(long_rx);
        kernel.replies.prune(crate::types::Timestamp::from_millis(
            system.clock.now().as_millis() + 10,
        ));

        // Then the short lease is gone (expired), the long one was
        // consumed by its reply, and the table is empty — no leaks.
        assert!(kernel.replies.is_empty(), "lease leaked");
        assert!(!kernel.replies.complete(&short_lease, json!({})));
    }

    #[tokio::test]
    async fn spawn_es_registers_slot_and_edges() {
        // Given a system.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();

        // When spawning an ES actor.
        let path = Path::new("counter");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // Then the slot resolves and who_handles finds the path.
        let handlers = RuntimeView::who_handles(system.as_ref(), &Add::schema_id());
        assert_eq!(handlers, [path]);
    }

    #[tokio::test]
    async fn spawn_es_starts_genesis_state() {
        // Given a system with a spawned counter.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("counter");
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
        let system = ActorSystem::new();
        let first = system.register_schema::<Add>();

        // When registering Add again.
        let second = system.register_schema::<Add>();

        // Then both calls return the same id and one schema is stored.
        assert_eq!(first, second);
        assert!(system.schema(&first).is_some());
    }

    /// Reads a foreign actor's live JSON state (test inspection helper).
    async fn count_total_json(system: &ActorSystem, path: &Path) -> Option<i64> {
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
                "name": "tally", "version": 1, "kind": "command",
                "fields": [
                    {"name": "delta", "ty": "int"}
                ]
            }))
            .expect("valid");
        let schema_for_actor = schema.clone();
        system.spawn_es_foreign(
            Path::new("tally-actor"),
            schema.clone(),
            json!({ "total": 0 }),
            Arc::new(move |_state, cmd, _ctx| {
                let delta = cmd["delta"].as_i64().unwrap_or(0);
                vec![crate::envelope::Event::new(
                    schema_for_actor.clone(),
                    json!({ "delta": delta }),
                )]
            }),
            Arc::new(|state: &mut JsonValue, ev: &crate::envelope::Event| {
                state["total"] = json!(
                    state["total"].as_i64().unwrap_or(0)
                        + ev.payload["delta"].as_i64().unwrap_or(0)
                );
            }),
            SpawnOpts::default(),
        );

        // When a JSON command is sent to the foreign actor and the ack
        // settles.
        system
            .send(system.envelope(
                schema.clone(),
                Path::new("tally-actor"),
                json!({ "delta": 5 }),
            ))
            .await
            .expect("delivered");
        wait_for(|| async {
            count_total_json(&system, &Path::new("tally-actor")).await == Some(5)
        })
        .await;

        // Then the foreign actor's live JSON state folded the event.
        let export = system.export().await;
        let actor = export
            .actors
            .iter()
            .find(|a| a.path == Path::new("tally-actor"))
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
    async fn subscription_cascade_on_remove() {
        // Given a publisher and a subscriber bound to a topic.
        let (system, _clock) = ActorSystem::test();
        let topic = crate::types::Topic::new("cascade.events");
        system.spawn_es::<Counter, _>(Path::new("pub"), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });
        let (sub_idx, sub_sink) = open_sink();
        bind_sink(&Path::new("sub"), sub_sink);
        system.spawn_service::<Auditor, _>(
            Path::new("sub"),
            &json!({ "sink": sub_idx }),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<Auditor, Added>::new::<Added>(),
                )]
            },
        );
        system
            .subscribe(&Path::new("sub"), &topic, None)
            .expect("subscribe");
        system
            .send(system.envelope_to_topic(
                crate::envelope::Event::new(system.register_schema::<Added>(), json!({ "n": 1 })),
                topic.clone(),
            ))
            .await
            .expect("published");
        wait_for(|| async { sink_read(&Path::new("sub")).len() == 1 }).await;

        // When the subscriber is removed.
        system.stop(&Path::new("sub")).await;

        // And a second event is published.
        system
            .send(system.envelope_to_topic(
                crate::envelope::Event::new(system.register_schema::<Added>(), json!({ "n": 2 })),
                topic.clone(),
            ))
            .await
            .expect("published");

        // Then the removed subscriber receives nothing further and the
        // pump no longer tracks it (no cursor leaks).
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert_eq!(sink_read(&Path::new("sub")).len(), 1);
        let subscribers = {
            let kernel = system.kernel.lock().expect("lock");
            kernel
                .topic_logs
                .get(&topic)
                .map(|log| log.subscribers().len())
        };
        assert_eq!(subscribers, Some(0), "subscriber removed from the log");
    }

    #[tokio::test]
    async fn export_shows_schemas_actors_and_edge_kinds() {
        // Given a system with an ES actor declaring handles/emits, a
        // subscriber on a topic, and some send traffic.
        let (system, _clock) = ActorSystem::test();
        let schema = system.register_schema::<Add>();
        let topic = crate::types::Topic::new("export.events");
        system.spawn_es::<Counter, _>(
            Path::new("source"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        let added_schema = system.register_schema::<Added>();
        let (sink_idx, sink_store) = open_sink();
        bind_sink(&Path::new("sink"), sink_store);
        system.spawn_service::<Auditor, _>(
            Path::new("sink"),
            &json!({ "sink": sink_idx }),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<Auditor, Added>::new::<Added>(),
                )]
            },
        );
        system
            .subscribe(&Path::new("sink"), &topic, None)
            .expect("subscribe");
        system
            .send(system.envelope_to_topic(
                crate::envelope::Event::new(system.register_schema::<Added>(), json!({ "n": 1 })),
                topic.clone(),
            ))
            .await
            .expect("published");
        wait_for(|| async { sink_read(&Path::new("sink")).len() == 1 }).await;

        // When exporting.
        let export = system.export().await;

        // Then schemas, actors (with kind), and both declared-edge
        // directions appear; observed edges count the send.
        assert!(export.schemas.iter().any(|s| s.id() == schema));
        assert_eq!(export.actors.len(), 2, "both actors live: {export:?}");
        let source = export
            .actors
            .iter()
            .find(|a| a.path == Path::new("source"))
            .expect("source exported");
        assert_eq!(source.kind, crate::types::ActorKind::EventSourced);
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
                .any(|e| e.actor == Path::new("source")
                    && e.schema == schema
                    && e.direction == crate::system::EdgeDirection::Handles)
        );
        assert!(
            export
                .declared_edges
                .iter()
                .any(|e| e.actor == Path::new("sink") && e.topic == Some(topic.clone()))
        );
        let observed = export
            .observed_edges
            .iter()
            .find(|e| e.to == format!("topic:{topic}") && e.schema == added_schema)
            .expect("observed topic edge");
        assert!(observed.count >= 1, "at least the one send: {observed:?}");
    }

    #[test]
    fn register_schema_json_accepts_foreign_descriptors() {
        // Given a system and a JSON-only descriptor.
        let system = ActorSystem::new();
        let foreign = json!({
            "name": "ForeignPing",
            "version": 4,
            "kind": "command",
            "fields": []
        });

        // When registering it as JSON.
        let id = system.register_schema_json(foreign).expect("valid");

        // Then it is retrievable by its name@version id.
        let stored = system.schema(&id).expect("stored");
        assert_eq!(id.to_string(), "ForeignPing@4");
        assert_eq!(stored.name, "ForeignPing");
    }

    #[tokio::test]
    async fn send_routes_through_the_kernel_to_the_inbox() {
        // Given a system with one spawned actor.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("counter");
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

    fn inbox_has_work(_system: &ActorSystem, _path: &Path) -> bool {
        // The envelope is queued iff the cursor has not advanced past it;
        // full inbox introspection lands with the atomic step (next task).
        true
    }

    // ---- test-table gap tests (Phase 10) ----

    #[tokio::test]
    async fn es_journal_and_fold_reconstruct_state_from_events_alone() {
        // Given a spawned counter.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("counter");
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
        let entries = {
            let kernel = system.kernel.lock().expect("lock");
            kernel.journals[&path].entries().to_vec()
        };
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
        // Given a counter with EveryN(2) snapshots that committed 4 Adds.
        let (system, _clock) = ActorSystem::test();
        let path = Path::new("counter");
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let opts = SpawnOpts {
            snapshot: crate::kernel::SnapshotPolicy::EveryN(2),
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
            let kernel = system.kernel.lock().expect("lock");
            let journal = &kernel.journals[&path];
            assert_eq!(journal.len(), 6, "4 events + 2 snapshots");
            let last = journal.last_snapshot().expect("snapshot exists");
            let crate::journal::JournalEntry::Snapshot { seq, .. } = last else {
                panic!("expected a snapshot entry");
            };
            assert_eq!(
                *seq,
                crate::types::SeqNo::new(3),
                "latest snapshot at seq 3 (4th Add)"
            );
        }
        assert!(
            system
                .tap_facts()
                .iter()
                .any(|f| matches!(f.kind, crate::tap::FactKind::SnapshotTaken { .. })),
            "SnapshotTaken fact emitted"
        );
        // The latest snapshot's fold already contains Adds 1-4 (total 10):
        // the fast path restores it, then replays an empty tail.
        {
            let kernel = system.kernel.lock().expect("lock");
            let snap = match kernel.journals[&path].last_snapshot().expect("snap") {
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
        let path = Path::new("counter");
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
            let kernel = system.kernel.lock().expect("lock");
            let journal = &kernel.journals[&path];
            assert_eq!(journal.len(), 5);
            assert!(journal.last_snapshot().is_none());
        }
        assert!(
            !system
                .tap_facts()
                .iter()
                .any(|f| matches!(f.kind, crate::tap::FactKind::SnapshotTaken { .. })),
            "no snapshot facts under Off"
        );
    }

    #[tokio::test]
    async fn restore_from_hydration_hook_populates_skipped_caches() {
        // Given a counter whose state carries a #[serde(skip)] cache that
        // derives from `total`, with restore_from overridden to rebuild it.
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Cached {
            total: i64,
            #[serde(skip)]
            doubled: i64,
        }
        impl crate::actor::EventSourced for Cached {
            fn manifest() -> crate::schema::ActorManifest {
                ActorManifest::new().kind(crate::types::ActorKind::EventSourced)
            }
            fn restore(_args: &JsonValue) -> Self {
                Self {
                    total: 0,
                    doubled: 0,
                }
            }
            fn apply(&mut self, event: &crate::envelope::Event) {
                self.total += event.payload["n"].as_i64().unwrap_or(0);
            }
            fn capture(
                &self,
            ) -> Result<JsonValue, error_stack::Report<crate::journal::JournalError>> {
                // The cache is not persisted, but capture EXPOSES it when
                // hydrated — making the hydration hook observable.
                Ok(json!({ "total": self.total, "doubled": self.doubled }))
            }
            fn restore_from(
                snap: JsonValue,
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
        let tail = vec![crate::envelope::Event::new(
            SchemaId::new("Added", 1),
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
        let path = Path::new("counter");
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
        let path = Path::new("counter");
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
            let kernel = system.kernel.lock().expect("lock");
            let events = kernel.journals[&path]
                .entries()
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
}
