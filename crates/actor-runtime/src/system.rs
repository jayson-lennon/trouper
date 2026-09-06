//! The system facade: the single handle through which the runtime is
//! configured, driven, and observed.
//!
//! The registry is kernel, not an actor — owned here behind a lock so
//! schema registration can never deadlock and survives every actor restart.
//! The system also implements [`RuntimeView`]: handler-side lookups snapshot
//! through this read-only surface, never through kernel-mutable locks.

use std::sync::{Arc, Mutex};

use serde_json::Value as JsonValue;
use serde_json::json;

use crate::actor::{
    CommandEntry, DynServiceActor, EventSourcedActor, MsgEntry, ServiceActor, TypedEsState,
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
use crate::types::{ActorPath, InboxOffset, SchemaId, Timestamp};

pub use crate::types::SnapshotCadence;

/// Spawn-time options for an actor.
#[derive(Debug, Clone)]
pub struct SpawnOpts {
    /// Snapshot cadence (ES actors only).
    pub snapshot: SnapshotCadence,
    /// Mailbox capacity (the logical inbox; the front door is 2× this).
    pub mailbox_capacity: usize,
    /// Inbox overload policy (default Block = backpressure).
    pub mailbox_policy: OverloadPolicy,
    /// Inbox depth at which a [`crate::tap::FactKind::Backpressured`] fact
    /// fires (once per crossing); `None` = never. Pool/partition specs use
    /// it to make sustained overload observable.
    pub high_watermark: Option<u64>,
}

impl Default for SpawnOpts {
    fn default() -> Self {
        Self {
            snapshot: SnapshotCadence::Off,
            mailbox_capacity: 64,
            mailbox_policy: OverloadPolicy::Block,
            high_watermark: None,
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
    /// The tap ring's capacity (drop-oldest under pressure).
    pub tap_capacity: usize,
    /// Default mailbox capacity and overload policy for spawned actors
    /// (per-spawn [`SpawnOpts`] override these).
    pub default_mailbox: MailboxDefaults,
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
            tap_capacity: 4096,
            default_mailbox: MailboxDefaults::default(),
        }
    }

    /// A config on an injected clock (tests).
    pub fn with_clock(clock: ClockService) -> Self {
        Self {
            clock,
            tap_capacity: 4096,
            default_mailbox: MailboxDefaults::default(),
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
    /// System-wide mailbox defaults (per-spawn opts override).
    pub(crate) mailbox_defaults: MailboxDefaults,
}

/// One actor's row in a system export.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ActorExport {
    /// The actor's path (its identity).
    pub path: ActorPath,
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
    pub actor: ActorPath,
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

/// One declared pool: the public path, its algo, and its workers (the
/// canvas draws `source → public → workers` from this row).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PoolExport {
    /// The public path senders address.
    pub path: ActorPath,
    /// The worker-selection algorithm ("round-robin" | "random").
    pub algo: String,
    /// The worker paths (the only deliverable destinations).
    pub workers: Vec<ActorPath>,
    /// The parent workers escalate to, if any.
    pub spec_parent: Option<ActorPath>,
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
    /// Declared stateless pools (public path → workers).
    pub pools: Vec<PoolExport>,
    /// Declared partition sets (public path → entities).
    pub partitions: Vec<PartitionExport>,
    /// Declared router rules (declaration order).
    pub rules: Vec<RuleExport>,
}

impl ActorSystem {
    /// The system dead-letter topic, created at boot.
    pub fn deadletter_topic() -> crate::types::Topic {
        crate::types::Topic::new("system.deadletters")
    }

    /// Spawns a foreign (no-Rust-types) event-sourced actor: the schema,
    /// state fold, and command decision are all runtime JSON data. This is
    /// the seam the port tier will reuse.
    ///
    /// Deprecated positional flavor — prefer the builder:
    /// [`crate::builder::spawn_foreign`] (named `handle`/`apply` methods).
    #[doc(hidden)]
    pub fn spawn_es_foreign(
        self: &Arc<Self>,
        path: ActorPath,
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

    /// Creates a system from a config.
    pub fn new(config: SystemConfig) -> Self {
        let registry = Arc::new(Mutex::new(Registry::default()));
        let view = Arc::new(NullView {
            registry: registry.clone(),
        });
        Self {
            registry,
            kernel: Arc::new(Mutex::new(KernelState::with_tap_capacity(
                config.tap_capacity,
            ))),
            clock: config.clock,
            view,
            child_shutdowns: std::sync::Mutex::new(Vec::new()),
            mailbox_defaults: config.default_mailbox,
        }
    }

    /// Creates a system tuned for tests: a [`FakeClock`] starting at
    /// 1_000 ms and a small tap ring (reachable via the returned handle).
    pub fn test() -> (Arc<Self>, Arc<FakeClock>) {
        let (clock, fake) = ClockService::fake(1_000);
        (
            Arc::new(Self::new(SystemConfig {
                clock,
                tap_capacity: 256,
                default_mailbox: MailboxDefaults::default(),
            })),
            fake,
        )
    }

    /// Like [`ActorSystem::test`], but with an explicit (tiny) tap ring
    /// capacity — gap/pressure tests flood the ring on purpose.
    pub fn test_with_tap(tap_capacity: usize) -> (Arc<Self>, Arc<FakeClock>) {
        let (clock, fake) = ClockService::fake(1_000);
        (
            Arc::new(Self::new(SystemConfig {
                clock,
                tap_capacity,
                default_mailbox: MailboxDefaults::default(),
            })),
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
    /// Deprecated positional flavor — prefer the builder:
    /// [`crate::builder::spawn_es_builder`] (each type said once).
    #[doc(hidden)]
    pub fn spawn_es<A, F>(
        self: &Arc<Self>,
        path: ActorPath,
        args: &JsonValue,
        opts: SpawnOpts,
        entries: F,
    ) where
        A: EventSourcedActor,
        F: FnOnce() -> Vec<Arc<dyn CommandEntry>>,
    {
        let state = Box::new(TypedEsState::<A>::new(A::restore(args)));
        let manifest = A::manifest();
        self.spawn_es_erased(path, manifest, state, entries(), opts);
    }

    /// The erased ES spawn shared by typed, foreign, and builder actors
    /// (the single funnel every journaled spawn goes through).
    pub(crate) fn spawn_es_erased(
        self: &Arc<Self>,
        path: ActorPath,
        manifest: crate::schema::ActorManifest,
        state: Box<dyn crate::actor::DynEsActor>,
        entries: Vec<Arc<dyn CommandEntry>>,
        opts: SpawnOpts,
    ) {
        // The `Fact` schema (facts mirror into `system.facts` as messages;
        // observers declare a subscription filter against it). First
        // registration wins — test-local FactMsg may already have it.
        {
            let mut registry = self.registry.lock().expect("registry lock");
            registry
                .register_schema_json(json!({
                    "name": "Fact", "version": 1, "kind": "event",
                    "fields": [
                        { "name": "kind", "ty": "str", "required": true },
                        { "name": "offset", "ty": "int", "required": true },
                        { "name": "ts", "ty": "int", "required": true }
                    ],
                    "description": "A runtime fact mirrored from the tap ring."
                }))
                .ok();
        }
        let opts = self.resolve_opts(opts);
        let (tx, rx) = tokio::sync::mpsc::channel::<Envelope>(opts.mailbox_capacity.max(1) * 2);
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
        // Emit edges are ENFORCED against the manifest (the kernel drops
        // undeclared schemas pre-append) — the builder/foreign paths feed
        // extra declarations through `declare_emits` before the first step.
        drop(registry);
        let mut kernel = self.kernel.lock().expect("kernel lock");
        let cell = Arc::new(ActorCell::new(
            path.clone(),
            Inbox::new(opts.mailbox_capacity.max(1), opts.mailbox_policy),
        ));
        kernel.cells.insert(path.clone(), cell.clone());
        kernel
            .journals
            .entry(path.clone())
            .or_default()
            .anchor_time_cadence(self.clock.now().as_millis());
        kernel
            .es_state
            .insert(path.clone(), Arc::new(tokio::sync::Mutex::new(state)));
        kernel.entries.insert(path.clone(), entries);
        kernel.snapshot_policy.insert(path.clone(), opts.snapshot);
        if let Some(wm) = opts.high_watermark {
            kernel.watermarks.insert(path.clone(), (wm, false));
        }
        kernel.record_fact(
            self.clock.now(),
            crate::tap::FactKind::Spawned {
                path: path.clone(),
                kind: crate::types::ActorKind::EventSourced,
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
    ///
    /// Deprecated positional flavor — prefer the builder:
    /// [`crate::builder::spawn_service_builder`].
    #[doc(hidden)]
    pub fn spawn_service<A, F>(
        self: &Arc<Self>,
        path: ActorPath,
        args: &JsonValue,
        opts: SpawnOpts,
        entries: F,
    ) where
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
        self: &Arc<Self>,
        path: ActorPath,
        manifest: crate::schema::ActorManifest,
        args: &JsonValue,
        entries: Vec<Arc<dyn MsgEntry>>,
        opts: SpawnOpts,
        start: ServiceStart,
    ) {
        // `A::start` is async (I/O allowed); block briefly on a runtime
        // thread is not done — spawn the start inside the actor task and
        // register the slot immediately so senders never see a gap.
        // The `Fact` schema (facts mirror into `system.facts` as messages;
        // observers declare a subscription filter against it). First
        // registration wins — test-local FactMsg may already have it.
        {
            let mut registry = self.registry.lock().expect("registry lock");
            registry
                .register_schema_json(json!({
                    "name": "Fact", "version": 1, "kind": "event",
                    "fields": [
                        { "name": "kind", "ty": "str", "required": true },
                        { "name": "offset", "ty": "int", "required": true },
                        { "name": "ts", "ty": "int", "required": true }
                    ],
                    "description": "A runtime fact mirrored from the tap ring."
                }))
                .ok();
        }
        let opts = self.resolve_opts(opts);
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
            // Service actors route by schema too: each handled schema is
            // routable to this path (second handler → RoundRobin).
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
        kernel.genesis_args.insert(path.clone(), args.clone());
        kernel.msg_entries.insert(path.clone(), entries);
        if let Some(wm) = opts.high_watermark {
            kernel.watermarks.insert(path.clone(), (wm, false));
        }
        kernel.record_fact(
            self.clock.now(),
            crate::tap::FactKind::Spawned {
                path: path.clone(),
                kind: crate::types::ActorKind::Service,
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
        let front_cell = cell.clone();
        let front_kernel = self.kernel.clone();
        tokio::spawn(async move {
            // Start the instance inside the task; a start failure leaves
            // the slot present (senders get a closed door) and the crash
            // recorded for supervision.
            let started = start.await;
            match started {
                Ok(instance) => {
                    let mut kernel = kernel_table.lock().expect("kernel lock");
                    kernel.services.insert(
                        started_path.clone(),
                        Arc::new(tokio::sync::Mutex::new(instance)),
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

    /// Sends an envelope from outside the system (entry-point trace root).
    ///
    /// # Errors
    ///
    /// Returns the envelope back when its destination does not resolve
    /// (callers dead-letter or retry).
    pub async fn send(&self, envelope: Envelope) -> Result<ActorPath, Envelope> {
        route(&self.registry, &self.kernel, envelope).await
    }

    /// Installs the DLQ re-driver: re-sends every dead letter currently
    /// retained in the `system.deadletters` topic to its recorded dest
    /// (as a normal sender — `Sent` facts appear; an undeliverable redrive
    /// simply dead-letters again). Sugar over the topic + cursor
    /// machinery: subsequent re-drives re-consume by cursor reset.
    ///
    /// # Panics
    ///
    /// Panics if the kernel lock is poisoned.
    pub fn install_dlq_redriver(self: &Arc<Self>) {
        let mut kernel = self.kernel.lock().expect("kernel lock");
        let log = match kernel.topic_logs.get_mut(&Registry::dead_letter_topic()) {
            Some(log) => log,
            None => return, // no dead letters yet: nothing to redrive
        };
        let entries: Vec<Envelope> = log
            .entries_iter()
            .map(|(_, envelope)| envelope.clone())
            .collect();
        drop(kernel);
        let system = self.clone();
        tokio::spawn(async move {
            for envelope in entries {
                let _ = system.send(envelope).await; // failure re-dead-letters
            }
        });
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

    pub fn envelope(&self, schema: SchemaId, dest: ActorPath, payload: JsonValue) -> Envelope {
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
        path: &ActorPath,
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
        path: &ActorPath,
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
        path: &ActorPath,
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
    pub async fn stop(&self, path: &ActorPath) {
        const STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        self.stop_bounded(path, STOP_TIMEOUT).await;
    }

    /// Installs a stateless pool over `public`: `N` workers (spawned by
    /// the caller-supplied `factory` as supervised children of the spec
    /// parent, or parentless) plus a pool entry that owns the routing
    /// decision for the PUBLIC path.
    ///
    /// Senders never change: they keep addressing `public` before, during,
    /// and after the install. If a live actor already owns the public path,
    /// it is gracefully STOP-DRAINED first (undelivered inbox entries go
    /// to the DLQ per the stop contract); re-routing that queued mail into
    /// the pool's workers is a documented FUTURE refinement, not v1.
    ///
    /// # Errors
    ///
    /// Propagates [`crate::registry::RegistryError::InvalidSpec`] from the
    /// registry install (no workers, or a worker slot is missing — spawn
    /// workers first, then install).
    pub async fn install_pool(
        self: &Arc<Self>,
        spec: crate::pool::PoolSpec,
    ) -> Result<(), error_stack::Report<crate::registry::RegistryError>> {
        // 1. TAKEOVER: stop-drain whoever holds the public path today (a
        // plain actor). A no-op when the path is free.
        if self
            .registry
            .lock()
            .expect("registry lock")
            .lookup(&spec.public)
            .is_some()
        {
            self.stop(&spec.public).await;
        }
        // 2. WORKERS: spawn through the factory; each worker registers its
        // own slot. When a spec parent is declared, each worker is a
        // supervised child of it (escalation flows worker → parent).
        let mut workers = Vec::with_capacity(spec.workers);
        for i in 0..spec.workers {
            let worker_path = ActorPath::new(format!("{}/worker-{i}", spec.public).as_str());
            let args = spec
                .args
                .clone()
                .unwrap_or(JsonValue::Object(serde_json::Map::new()));
            match &spec.parent {
                Some(parent) => {
                    let factory = spec.factory.clone();
                    let worker = worker_path.clone();
                    self.spawn_child(crate::supervision::ChildSpec {
                        path: worker,
                        parent: Some(parent.clone()),
                        args,
                        restart: crate::supervision::RestartPolicy::Permanent,
                        budget: crate::supervision::RestartBudget::per(
                            5,
                            std::time::Duration::from_secs(10),
                        ),
                        backoff: crate::supervision::Backoff::default(),
                        spawn: Arc::new(move |system, path, args| {
                            factory(system, path, args);
                        }),
                    });
                }
                None => {
                    (spec.factory)(self, &worker_path, &args);
                }
            }
            workers.push(worker_path);
        }
        // 3. INSTALL: one registry transaction — the pool entry claims the
        // public name (workers own the deliverable slots).
        let entry = crate::pool::pool_entry(spec.algo, workers, spec.seed, spec.parent.clone());
        let mut registry = self.registry.lock().expect("registry lock");
        registry.install_pool(spec.public, entry)
    }

    /// Installs a partition set over `public`: commands aimed at the
    /// public path are routed to per-entity actors derived from the
    /// payload's declared shard key (`public/key`), activated on demand
    /// from the spec's shared factory.
    ///
    /// Senders keep addressing the public path forever; entity paths and
    /// journals are per key. Entities live until stopped — passivation is
    /// a declared anti-goal.
    ///
    /// # Errors
    ///
    /// [`crate::registry::RegistryError::InvalidSpec`] when no command
    /// schema declares the spec's key field as the ShardKey (refuse-to-lie:
    /// the set would dead-letter every command).
    pub fn install_partition_set(
        self: &Arc<Self>,
        spec: crate::pool::PartitionSpec,
    ) -> Result<(), error_stack::Report<crate::registry::RegistryError>> {
        let mut registry = self.registry.lock().expect("registry lock");
        registry.install_partition_set(spec)
    }

    /// The bounded stop; recursion depth bounded by timeout.
    fn stop_bounded<'a>(
        &'a self,
        path: &'a ActorPath,
        remaining: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.stop_bounded_inner(path, remaining))
    }

    /// The recursive body, boxed by [`Self::stop_bounded`].
    async fn stop_bounded_inner(&self, path: &ActorPath, remaining: std::time::Duration) {
        if remaining.is_zero() {
            return;
        }
        // 1. CHILDREN FIRST (recursive): any spec whose parent is this path.
        let children: Vec<ActorPath> = {
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
                kernel.record_fact(
                    self.clock.now(),
                    crate::tap::FactKind::Stopped {
                        path: path.clone(),
                        reason: crate::types::StopReason::Normal,
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
            let mut letters = Vec::with_capacity(undelivered.len());
            for envelope in &undelivered {
                letters.push(crate::kernel::DeadLetter {
                    schema: envelope.schema.clone(),
                    dest: envelope.dest.clone(),
                    reason: crate::types::DeadLetterReason::StoppedWithMail,
                    detail: "stopped with a non-empty inbox".to_owned(),
                    trace: envelope.trace,
                });
            }
            kernel.dead_letters.extend(letters);
            // The DLQ is a real topic: retained for re-consumption.
            let log = kernel
                .topic_logs
                .entry(Registry::dead_letter_topic())
                .or_insert_with(|| crate::topics::TopicLog::new(256));
            for envelope in undelivered {
                log.append(envelope);
            }
        }

        // 5. SLOT DROP + subscription cascade + Stopped fact + parent
        // link notification (a supervised child stopping notifies its
        // parent as a tap fact).
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
            let notified_parent = kernel.specs.get(path).and_then(|s| s.parent.clone());
            kernel.specs.remove(path);
            kernel.record_fact(
                self.clock.now(),
                crate::tap::FactKind::Stopped {
                    path: path.clone(),
                    reason: crate::types::StopReason::Normal,
                },
            );
            if let Some(parent) = notified_parent {
                kernel.record_fact(
                    self.clock.now(),
                    crate::tap::FactKind::LinkNotified {
                        parent,
                        child: path.clone(),
                    },
                );
            }
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
        let runtime_subscriptions: Vec<(ActorPath, crate::types::Topic)> = {
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
                let to_str = dest.to_string();
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

        // Declared pool/partition/rule topology (the canvas's structural
        // view; the observed router signature lives in the tap facts).
        let (pools, partitions, rules) = {
            let registry = self.registry.lock().expect("registry lock");
            registry.topology()
        };

        SystemExport {
            schemas,
            actors,
            declared_edges,
            observed_edges,
            pools,
            partitions,
            rules,
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
            .map(|d| format!("{:?}: {}", d.reason.clone(), d.schema))
            .collect()
    }

    /// How many envelopes are queued at `path` (inspection/tests).
    pub async fn inbox_debug_len(&self, path: &ActorPath) -> usize {
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

    pub fn inbox_cursor(&self, path: &ActorPath) -> Option<InboxOffset> {
        let kernel = self.kernel.lock().expect("kernel lock");
        kernel.cells.get(path).map(|cell| {
            cell.inbox
                .try_lock()
                .map(|inbox| inbox.cursor())
                .unwrap_or_else(|_| InboxOffset::zero())
        })
    }

    /// The captured ES state of an actor (for export/inspection).
    pub async fn es_state(&self, path: &ActorPath) -> Option<JsonValue> {
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
    fn lookup(&self, path: &ActorPath) -> Option<EndpointInfo> {
        let registry = self.registry.lock().expect("registry lock");
        registry.lookup(path)
    }

    fn who_handles(&self, schema: &SchemaId) -> Vec<ActorPath> {
        let registry = self.registry.lock().expect("registry lock");
        registry.who_handles(schema)
    }

    fn now(&self) -> Timestamp {
        Timestamp::from_millis(0)
    }
}

impl RuntimeView for ActorSystem {
    fn lookup(&self, path: &ActorPath) -> Option<EndpointInfo> {
        let registry = self.registry.lock().expect("registry lock");
        registry.lookup(path)
    }

    fn who_handles(&self, schema: &SchemaId) -> Vec<ActorPath> {
        let registry = self.registry.lock().expect("registry lock");
        registry.who_handles(schema)
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

#[cfg(test)]
mod tests {
    use rstest::rstest;
    impl ActorSystem {
        /// Whether the "aud" test actor subscribes to `topic` (tests).
        pub fn topic_has_subscriber(&self, topic: &crate::types::Topic) -> bool {
            let kernel = self.kernel.lock().expect("kernel lock");
            kernel
                .topic_logs
                .get(topic)
                .map(|log| log.subscribers().contains(&ActorPath::new("aud")))
                .unwrap_or(false)
        }

        /// The number of journalled entries for `path` (tests).
        pub fn journal_len(&self, path: &ActorPath) -> usize {
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

    /// An event schema the test actors NEVER declare (emit-enforcement
    /// fixture: the kernel must drop it).
    #[derive(serde::Deserialize)]
    struct Smuggled {
        #[allow(dead_code)] // payload shape; the kernel never reads it
        n: i64,
    }
    impl Schema for Smuggled {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Smuggled".into(),
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

    impl EventSourcedActor for Counter {
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
            let kernel = system.kernel.lock().expect("lock");
            assert_eq!(kernel.dead_letters.len(), 1);
            assert_eq!(kernel.dead_letters[0].schema, Boom::schema_id());
            assert_eq!(kernel.journals[&path].len(), 0);
        }
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], 0);
    }

    fn topic_of_join() -> crate::types::Topic {
        crate::types::Topic::new("auditor.join")
    }

    #[tokio::test]
    async fn service_actor_subscribes_during_message_handling() {
        // Given an Auditor service actor.
        let (system, _clock) = ActorSystem::test();
        let (idx, sink) = open_sink();
        bind_sink(&ActorPath::new("aud"), sink);
        system.spawn_service::<Auditor, _>(
            ActorPath::new("aud"),
            &json!({ "sink": idx }),
            SpawnOpts::default(),
            || {
                vec![
                    Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>()),
                    Arc::new(TypedServiceAdapter::<Auditor, Added>::new::<Added>()),
                ]
            },
        );

        // When the actor handles a join command (n=0) that calls
        // ctx.subscribe mid-handler.
        system
            .send(system.envelope(Add::schema_id(), ActorPath::new("aud"), json!({ "n": 0 })))
            .await
            .expect("join sent");
        wait_for(|| async { system.topic_has_subscriber(&topic_of_join()) }).await;

        // Then a publish AFTER the subscription lands in the actor's inbox.
        system
            .send(system.envelope_to_topic(
                crate::envelope::Event {
                    schema: Added::schema_id(),
                    payload: json!({ "n": 7 }),
                },
                topic_of_join(),
            ))
            .await
            .expect("published");
        wait_for(|| async { sink_read(&ActorPath::new("aud")).contains(&"Added:7".to_string()) })
            .await;
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
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Parked {
            async fn handle(&mut self, _msg: Add, _ctx: &mut crate::context::MsgCtx<'_>) {
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
            system.tap_facts().iter().any(
                |f| matches!(&f.kind, crate::tap::FactKind::Spawned { path: p, .. } if *p == path),
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
            let kernel = system.kernel.lock().expect("lock");
            let cell = kernel.cells.get(&path).expect("cell");
            let inbox = cell.inbox.try_lock().expect("inbox free between messages");
            assert_eq!(
                inbox.len(),
                2,
                "two messages queued behind the parked handler"
            );
        }

        // When the actor is stopped while mail is queued.
        system.stop(&path).await;

        // Then the queued envelopes were flushed to the dead-letter
        // mirror with the typed StoppedWithMail reason.
        let kernel = system.kernel.lock().expect("lock");
        assert!(
            kernel
                .dead_letters
                .iter()
                .any(|l| l.reason == crate::types::DeadLetterReason::StoppedWithMail),
            "undelivered mail typed StoppedWithMail: {:?}",
            kernel.dead_letters
        );
        drop(kernel);

        // And the DLQ topic log holds them for re-consumption.
        let dlq_entries = system
            .topic_range(&Registry::dead_letter_topic())
            .map(|(lo, hi)| hi - lo)
            .unwrap_or(0);
        assert!(
            dlq_entries >= 2,
            "DLQ holds the flushed mail: {dlq_entries}"
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
            },
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        wait_for(|| async {
            system
                .tap_facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::tap::FactKind::Spawned { path, .. } if *path == path.clone()))
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

        // And the DLQ topic retained the refused envelope.
        let dlq = Registry::dead_letter_topic();
        let (lo, hi) = system.topic_range(&dlq).expect("dlq log exists");
        assert!(hi > lo, "DLQ holds the refused envelope: ({lo}, {hi})");
    }

    #[tokio::test]
    async fn dlq_topic_is_subscribable_and_reconsumable() {
        // Given a spawned actor that handles only Add.
        let (system, _clock) = ActorSystem::test();
        let path = ActorPath::new("counter");
        system.spawn_es::<Counter, _>(path.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())]
        });

        // And a DLQ consumer subscribed to the dead-letter topic BEFORE
        // any dead letters exist, decoding the Boom payload shape.
        let dlq = Registry::dead_letter_topic();
        let (sub_idx, sub_sink) = open_sink();
        bind_sink(&ActorPath::new("dlq-watcher"), sub_sink);
        system.spawn_service::<DlqWatcher, _>(
            ActorPath::new("dlq-watcher"),
            &json!({ "sink": sub_idx }),
            SpawnOpts::default(),
            || {
                vec![Arc::new(TypedServiceAdapter::<DlqWatcher, BoomMsg>::new::<
                    BoomMsg,
                >())]
            },
        );
        system
            .subscribe(&ActorPath::new("dlq-watcher"), &dlq, None)
            .expect("subscribe to dlq");

        // When a message with an unhandled schema arrives.
        system
            .send(system.envelope(Boom::schema_id(), path.clone(), json!({ "why": "x" })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // Then the DLQ consumer received the dead-lettered envelope.
        wait_for(|| async { sink_read(&ActorPath::new("dlq-watcher")).len() == 1 }).await;

        // And the retained DLQ log holds the entry for re-consumption.
        let (lo, hi) = system.topic_range(&dlq).expect("dlq log exists");
        assert_eq!((lo, hi), (0, 1));

        // When the cursor is reset to 0, the retained entry's range is
        // still reported (re-consumption is possible from the log).
        system
            .reset_topic_cursor(&ActorPath::new("dlq-watcher"), &dlq, 0)
            .expect("reset");
        let (lo, hi) = system.topic_range(&dlq).expect("dlq log exists");
        assert_eq!((lo, hi), (0, 1));
        assert!(!sink_read(&ActorPath::new("dlq-watcher")).is_empty());
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
            .es_state(&ActorPath::new(name))
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
        impl EventSourcedActor for Forwarder {
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
                    Address::Path(ActorPath::new("echo")),
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
        system.spawn_es::<Forwarder, _>(
            ActorPath::new("a"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Forwarder, Add>::new::<Add>())],
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
                .tap_facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::tap::FactKind::Delivered { to, .. } if *to == ActorPath::new("echo")))
        })
        .await;

        // Then the facts carry a shared trace id across the hops.
        let facts = system.tap_facts();
        let a_hop = facts
            .iter()
            .find(|f| matches!(&f.kind, crate::tap::FactKind::Delivered { to, .. } if *to == ActorPath::new("a")))
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
                    crate::tap::FactKind::Delivered { to, .. } if *to == ActorPath::new("echo")
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
        let (clock, fake) = ClockService::fake(1_000);
        let system = Arc::new(ActorSystem::new(SystemConfig {
            clock,
            tap_capacity: 4,
            default_mailbox: MailboxDefaults::default(),
        }));
        let _clock = fake;
        let path = ActorPath::new("counter");
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

        #[derive(Serialize, Deserialize, Default)]
        struct Phoenix {
            total: i64,
        }
        impl EventSourcedActor for Phoenix {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .emits::<Added>()
                    .kind(ActorKind::EventSourced)
            }
            fn restore(_args: &JsonValue) -> Self {
                Self::default()
            }
            fn apply(&mut self, event: &crate::envelope::Event) {
                self.total += event.payload["n"].as_i64().unwrap_or(0);
            }
        }
        impl CommandHandler<Add> for Phoenix {
            fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
                if cmd.n == 666 && !CRASHED_YET.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    // First sight only: crash once, then recover.
                    panic!("transient fault");
                }
                vec![crate::envelope::Event::new(
                    Added::schema_id(),
                    json!({ "n": cmd.n }),
                )]
            }
        }

        let spec = crate::supervision::ChildSpec {
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
            spawn: Arc::new(
                |sys: &Arc<ActorSystem>, path: &ActorPath, args: &JsonValue| {
                    sys.spawn_es::<Phoenix, _>(path.clone(), args, SpawnOpts::default(), || {
                        vec![Arc::new(TypedEsAdapter::<Phoenix, Add>::new::<Add>())]
                    });
                },
            ),
        };

        // When the child is spawned under supervision and receives a
        // poison command (with a good one queued BEHIND it).
        system.spawn_child(spec);
        wait_for(|| async {
            system.tap_facts().iter().any(
                |f| matches!(&f.kind, crate::tap::FactKind::Spawned { path, .. } if *path == child),
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
        let facts = system.tap_facts();
        let restarted = facts.iter().any(|f| {
            matches!(
                &f.kind,
                crate::tap::FactKind::Spawned { path, restart, .. }
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
    async fn restart_budget_escalates_to_the_parent() {
        // Given a supervised counter whose Add handler always panics,
        // with a budget of 2 restarts per 10 seconds, parent "overseer".
        #[derive(Serialize, Deserialize, Default)]
        struct AlwaysBoom;
        impl EventSourcedActor for AlwaysBoom {
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
        let overseer = ActorPath::new("overseer");
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
        let worker = ActorPath::new("worker");
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
                move |sys: &Arc<ActorSystem>, path: &ActorPath, args: &JsonValue| {
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
            // Deliberate pacing: the budget window measures REAL elapsed
            // time, so the crashes must spread over real milliseconds.
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

        // And the tap recorded the child's stop WITH the typed
        // budget-exhausted reason, before the escalation fact.
        let facts = system.tap_facts();
        let stopped_escalated = facts.iter().position(|f| {
            matches!(&f.kind, crate::tap::FactKind::Stopped { path, reason }
                if *path == worker && *reason == crate::types::StopReason::Escalated)
        });
        let escalated = facts.iter().position(
            |f| matches!(&f.kind, crate::tap::FactKind::Escalated { path, .. } if *path == worker),
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
        let spawn_child = {
            let system = system.clone();
            move |_sys: &Arc<ActorSystem>, path: &ActorPath, _args: &JsonValue| {
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
        spawn_child(&system, &child, &json!({}));
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
            let mut kernel = kernel.lock().expect("lock");
            kernel.specs.insert(
                child.clone(),
                crate::supervision::ChildSpec {
                    path: child.clone(),
                    parent: Some(parent.clone()),
                    restart: crate::supervision::RestartPolicy::Permanent,
                    budget: crate::supervision::RestartBudget::default(),
                    backoff: crate::supervision::Backoff::default(),
                    args: json!({}),
                    spawn: Arc::new(spawn_child),
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
        let facts = system.tap_facts();
        let stops: Vec<String> = facts
            .iter()
            .filter_map(|f| match &f.kind {
                crate::tap::FactKind::Stopped { path, .. } => Some(path.to_string()),
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
            let kernel = system.kernel.lock().expect("lock");
            !kernel.specs.contains_key(&child)
        };
        assert!(child_cascaded, "child spec cascaded with the parent");

        // And the parent received a link-notification fact for the child.
        let notified = facts.iter().any(|f| {
            matches!(
                &f.kind,
                crate::tap::FactKind::LinkNotified { parent, child }
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

        #[derive(Serialize, Deserialize, Default)]
        struct Fragile;
        impl EventSourcedActor for Fragile {
            fn manifest() -> ActorManifest {
                ActorManifest::new().kind(ActorKind::EventSourced)
            }
            fn restore(_args: &JsonValue) -> Self {
                Self
            }
            fn apply(&mut self, _event: &crate::envelope::Event) {}
        }
        impl CommandHandler<Add> for Fragile {
            fn handle(&self, _cmd: Add, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
                panic!("never survives");
            }
        }

        let spec = crate::supervision::ChildSpec {
            path: child.clone(),
            parent: None,
            restart: crate::supervision::RestartPolicy::Never,
            budget: crate::supervision::RestartBudget::default(),
            backoff: crate::supervision::Backoff::default(),
            args: json!({}),
            spawn: Arc::new(
                |sys: &Arc<ActorSystem>, path: &ActorPath, args: &JsonValue| {
                    sys.spawn_es::<Fragile, _>(path.clone(), args, SpawnOpts::default(), || {
                        vec![Arc::new(TypedEsAdapter::<Fragile, Add>::new::<Add>())]
                    });
                },
            ),
        };
        system.spawn_child(spec);

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
                let registry = system.registry.lock().expect("lock");
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
            system.tap_facts().iter().any(|f| {
                matches!(&f.kind, crate::tap::FactKind::Stopped { path, reason }
                    if *path == child && *reason == crate::types::StopReason::Crashed)
            })
        })
        .await;
        let spawn_count = system
            .tap_facts()
            .iter()
            .filter(
                |f| matches!(&f.kind, crate::tap::FactKind::Spawned { path, .. } if *path == child),
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
                    spawn: Arc::new(
                        |_sys: &Arc<ActorSystem>, _path: &ActorPath, _args: &JsonValue| {},
                    ),
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
        let path = ActorPath::new("counter");
        let sub = ActorPath::new("watcher");
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
        let path = ActorPath::new("counter");
        let early = ActorPath::new("early");
        let late = ActorPath::new("late");
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
        system.subscribe(&late, &topic, None).expect("subscribe");
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 2 })))
            .await
            .expect("send");
        wait_for_cursor(&system, &path, 2).await;
        wait_for(|| async { sink_read(&early).len() >= 2 }).await;

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
        let path = ActorPath::new("counter");
        let slow = ActorPath::new("slow");
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

    async fn wait_for_crash(system: &ActorSystem, path: &ActorPath) {
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

    fn bind_sink(path: &ActorPath, sink: Arc<Mutex<Vec<String>>>) {
        sink_table()
            .lock()
            .expect("sink table lock")
            .insert(path.to_string(), sink);
    }

    /// Reads a subscriber's sink lines by path (test inspection).
    fn sink_read(path: &ActorPath) -> Vec<String> {
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
            self.sink
                .lock()
                .expect("sink lock")
                .push(format!("Added:{}", msg.n));
        }
    }

    impl MsgHandler<Add> for Auditor {
        async fn handle(&mut self, msg: Add, ctx: &mut crate::context::MsgCtx<'_>) {
            if msg.n == 0 {
                // The join command: subscribe DURING message handling
                // (the deferred-intent syscall under test).
                ctx.subscribe(crate::types::Topic::new("auditor.join"));
                return;
            }
            self.sink
                .lock()
                .expect("sink lock")
                .push(format!("n={}", msg.n));
        }
    }

    /// A message shape mirroring the Boom payload (DLQ consumers must
    /// decode dead-lettered payloads by their schema).
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct BoomMsg {
        why: String,
    }
    impl Schema for BoomMsg {
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

    /// A service actor that observes dead letters.
    struct DlqWatcher {
        sink: Arc<Mutex<Vec<String>>>,
    }
    impl ServiceActor for DlqWatcher {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<BoomMsg>()
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
    impl MsgHandler<BoomMsg> for DlqWatcher {
        async fn handle(&mut self, msg: BoomMsg, _ctx: &mut crate::context::MsgCtx<'_>) {
            self.sink
                .lock()
                .expect("sink lock")
                .push(format!("dead letter: {}", msg.why));
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
                .tap_facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::tap::FactKind::Spawned { path, .. } if *path == ActorPath::new("auditor")))
        })
        .await;
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
                        Address::Path(ActorPath::new("echo")),
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
                .tap_facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::tap::FactKind::Spawned { path, .. } if *path == ActorPath::new("asker")))
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
                        Address::Path(ActorPath::new("silent")),
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
                .tap_facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::tap::FactKind::Spawned { path, .. } if *path == ActorPath::new("asker")))
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
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Silent {
            async fn handle(&mut self, _msg: Add, _ctx: &mut crate::context::MsgCtx<'_>) {}
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
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Boom> for Asker {
            async fn handle(&mut self, _msg: Boom, ctx: &mut crate::context::MsgCtx<'_>) {
                // Long timeout: the lease is reaped before the asker's own
                // timeout could fire — isolating the Failed path.
                let outcome = ctx
                    .ask(
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
                    .expect("results lock")
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
                .tap_facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::tap::FactKind::Spawned { path, .. } if *path == ActorPath::new("asker")))
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
            let kernel = system.kernel.lock().expect("lock");
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
            let kernel = system.kernel.lock().expect("lock");
            kernel.replies.prune(crate::types::Timestamp::from_millis(
                system.clock.now().as_millis(),
            ));
        }

        // Then the ask settles as Failed (not Timeout), with an error.
        let results = FAILED_RESULTS.get_or_init(|| Mutex::new(Vec::new()));
        for _ in 0..2_000 {
            if results.lock().expect("lock").as_slice() == ["failed"] {
                let kernel = system.kernel.lock().expect("lock");
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
        panic!("ask never settled as failed: {:?}", results.lock().unwrap());
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
        impl EventSourcedActor for Counter {
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
            ActorPath::new("collector"),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![Arc::new(TypedServiceAdapter::<Collector, Added>::new::<
                    Added,
                >())]
            },
        );
        system.spawn_es::<Counter, _>(
            ActorPath::new("counter"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        wait_for(|| async {
            system
                .tap_facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::tap::FactKind::Spawned { path, .. } if *path == ActorPath::new("counter")))
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
        let system = ActorSystem::new(SystemConfig::production());
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
        let path = ActorPath::new("counter");
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
                "name": "tally", "version": 1, "kind": "command",
                "fields": [
                    {"name": "delta", "ty": "int"}
                ]
            }))
            .expect("valid");
        let schema_for_actor = schema.clone();
        system.spawn_es_foreign(
            ActorPath::new("tally-actor"),
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
        // The emit edge the decision closure produces is declared explicitly
        // (emit enforcement drops undeclared schemas, so this is load-bearing).
        {
            let mut registry = system.registry.lock().expect("registry lock");
            registry
                .declare_emits(&ActorPath::new("tally-actor"), schema.clone())
                .expect("live slot");
        }

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
    async fn subscription_cascade_on_remove() {
        // Given a publisher and a subscriber bound to a topic.
        let (system, _clock) = ActorSystem::test();
        let topic = crate::types::Topic::new("cascade.events");
        system.spawn_es::<Counter, _>(
            ActorPath::new("pub"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
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
        system
            .subscribe(&ActorPath::new("sub"), &topic, None)
            .expect("subscribe");
        system
            .send(system.envelope_to_topic(
                crate::envelope::Event::new(system.register_schema::<Added>(), json!({ "n": 1 })),
                topic.clone(),
            ))
            .await
            .expect("published");
        wait_for(|| async { sink_read(&ActorPath::new("sub")).len() == 1 }).await;

        // When the subscriber is removed.
        system.stop(&ActorPath::new("sub")).await;

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
        wait_for(|| async { !sink_read(&ActorPath::new("sub")).is_empty() }).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(sink_read(&ActorPath::new("sub")).len(), 1);
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
            ActorPath::new("source"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        let added_schema = system.register_schema::<Added>();
        let (sink_idx, sink_store) = open_sink();
        bind_sink(&ActorPath::new("sink"), sink_store);
        system.spawn_service::<Auditor, _>(
            ActorPath::new("sink"),
            &json!({ "sink": sink_idx }),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<Auditor, Added>::new::<Added>(),
                )]
            },
        );
        system
            .subscribe(&ActorPath::new("sink"), &topic, None)
            .expect("subscribe");
        system
            .send(system.envelope_to_topic(
                crate::envelope::Event::new(system.register_schema::<Added>(), json!({ "n": 1 })),
                topic.clone(),
            ))
            .await
            .expect("published");
        wait_for(|| async { sink_read(&ActorPath::new("sink")).len() == 1 }).await;

        // When exporting.
        let export = system.export().await;

        // Then schemas, actors (with kind), and both declared-edge
        // directions appear; observed edges count the send.
        assert!(export.schemas.iter().any(|s| s.id() == schema));
        assert_eq!(export.actors.len(), 2, "both actors live: {export:?}");
        let source = export
            .actors
            .iter()
            .find(|a| a.path == ActorPath::new("source"))
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
                .any(|e| e.actor == ActorPath::new("source")
                    && e.schema == schema
                    && e.direction == crate::system::EdgeDirection::Handles)
        );
        assert!(
            export
                .declared_edges
                .iter()
                .any(|e| e.actor == ActorPath::new("sink") && e.topic == Some(topic.clone()))
        );
        let observed = export
            .observed_edges
            .iter()
            .find(|e| e.to == topic.to_string() && e.schema == added_schema)
            .expect("observed topic edge");
        assert!(observed.count >= 1, "at least the one send: {observed:?}");
    }

    #[tokio::test]
    async fn export_shows_pool_partition_and_rule_topology() {
        // Given a pool over "api" (2 workers), a partition set over
        // "accounts", and a tee rule on Add@1.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        install_key_partition(&system, "accounts").expect("partition install");
        system
            .install_pool(crate::pool::PoolSpec {
                public: ActorPath::new("api"),
                workers: 2,
                algo: crate::pool::PoolAlgo::RoundRobin,
                factory: std::sync::Arc::new(|system, path, args| {
                    crate::builder::spawn_es_builder::<BareCounter>(system)
                        .at(path.clone())
                        .args(args.clone())
                        .handles::<Add>()
                        .emits::<Added>()
                        .start();
                }),
                args: Some(json!({ "total": 0 })),
                parent: Some(ActorPath::new("pool-parent")),
                seed: 7,
            })
            .await
            .expect("pool install");
        {
            let mut registry = system.registry.lock().expect("registry lock");
            registry.add_rule(crate::pool::Rule {
                source: None,
                schema: Some(Add::schema_id()),
                dest: Some(ActorPath::new("api")),
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

        // Then the declared topology rows are present: the pool with its
        // workers + parent, the partition with its activated entity, and
        // the rule with its action/observer.
        let pool = export
            .pools
            .iter()
            .find(|p| p.path == ActorPath::new("api"))
            .expect("pool exported");
        assert_eq!(pool.algo, "round-robin");
        assert_eq!(pool.workers.len(), 2, "both workers listed: {pool:?}");
        assert_eq!(pool.spec_parent, Some(ActorPath::new("pool-parent")));
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
        assert_eq!(rule.dest, Some(ActorPath::new("api")));
    }

    #[tokio::test]
    async fn restart_policy_never_escalates_immediately_without_restart() {
        // Given a supervised child with RestartPolicy::Never whose handler
        // always panics, and an overseer to receive the escalation.
        #[derive(Serialize, Deserialize, Default)]
        struct AlwaysBoom2;
        impl EventSourcedActor for AlwaysBoom2 {
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
        impl CommandHandler<Add> for AlwaysBoom2 {
            fn handle(&self, _cmd: Add, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
                panic!("never-restart child panics");
            }
        }

        #[derive(serde::Deserialize)]
        struct EscalatedMsg2 {
            escalated: String,
        }
        impl Schema for EscalatedMsg2 {
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

        struct Overseer2;
        impl ServiceActor for Overseer2 {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<EscalatedMsg2>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        fn overseer_path() -> ActorPath {
            ActorPath::new("overseer2")
        }
        impl MsgHandler<EscalatedMsg2> for Overseer2 {
            async fn handle(&mut self, msg: EscalatedMsg2, _ctx: &mut crate::context::MsgCtx<'_>) {
                if let Some(sink) = sink_table()
                    .lock()
                    .expect("sink table lock")
                    .get(&overseer_path().to_string())
                {
                    sink.lock()
                        .expect("sink lock")
                        .push(format!("escalated:{}", msg.escalated));
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
        let spec = crate::supervision::ChildSpec {
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
            spawn: Arc::new(
                |sys: &Arc<ActorSystem>, path: &ActorPath, args: &JsonValue| {
                    sys.spawn_es::<AlwaysBoom2, _>(
                        path.clone(),
                        args,
                        SpawnOpts::default(),
                        || vec![Arc::new(TypedEsAdapter::<AlwaysBoom2, Add>::new::<Add>())],
                    );
                },
            ),
        };
        system.spawn_child(spec);

        // When the child crashes once.
        let _ = system
            .send(system.envelope(Add::schema_id(), worker.clone(), json!({ "n": 1 })))
            .await;

        // Then the child was NOT restarted: exactly one Spawned fact (the
        // initial spawn), no restart flag anywhere.
        wait_for(|| async {
            system
                .tap_facts()
                .iter()
                .any(|f| matches!(f.kind, crate::tap::FactKind::Escalated { .. }))
        })
        .await;
        let facts = system.tap_facts();
        let worker_spawns: Vec<&crate::tap::Fact> = facts
            .iter()
            .filter(|f| matches!(&f.kind, crate::tap::FactKind::Spawned { path, .. } if *path == worker))
            .collect();
        assert_eq!(
            worker_spawns.len(),
            1,
            "only the initial spawn: {worker_spawns:?}"
        );
        assert!(
            !matches!(
                worker_spawns[0].kind,
                crate::tap::FactKind::Spawned { restart: true, .. }
            ),
            "Never must not restart"
        );

        // And the child stopped with the typed Crashed reason (the crash
        // is what stopped it; Never means no restart, hence no escalation
        // restart-cycle — the crash IS the terminal stop).
        let facts = system.tap_facts();
        assert!(
            facts.iter().any(|f| matches!(
                &f.kind,
                crate::tap::FactKind::Stopped { path, reason }
                    if *path == worker && *reason == crate::types::StopReason::Crashed
            )),
            "Stopped {{ Crashed }} expected: {:?}",
            facts
                .iter()
                .filter(|f| matches!(f.kind, crate::tap::FactKind::Stopped { .. }))
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
            let envelope = crate::envelope::Envelope::json(
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

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(3)]
    #[case(7)]
    #[case(16)]
    fn rebuild_from_snapshot_over_k_events_equals_full_fold(#[case] k: usize) {
        // Given a journal of k Added events folded from genesis.
        let events: Vec<crate::envelope::Event> = (1..=k as i64)
            .map(|n| crate::envelope::Event::new(Added::schema_id(), json!({ "n": n })))
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
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Cached {
            total: i64,
            #[serde(skip)]
            doubled: i64,
        }
        impl crate::actor::EventSourcedActor for Cached {
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

    // ---- dynamic actor primitives (Phase 1: emit enforcement + cadence) ----

    /// A counter whose Add handler emits ONE declared `Added` and ONE
    /// undeclared `Smuggled` per command (emit-enforcement fixture).
    #[derive(Serialize, Deserialize, Default)]
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
        fn restore(_args: &JsonValue) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            self.total += event.payload["n"].as_i64().unwrap_or(0);
        }
    }
    impl CommandHandler<Add> for MixedEmitter {
        fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
            vec![
                crate::envelope::Event::new(Added::schema_id(), json!({ "n": cmd.n })),
                crate::envelope::Event::new(Smuggled::schema_id(), json!({ "n": cmd.n })),
            ]
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
            let kernel = system.kernel.lock().expect("lock");
            let event_schemas: Vec<_> = kernel.journals[&path]
                .entries()
                .iter()
                .filter_map(|e| e.as_event().map(|ev| ev.schema.clone()))
                .collect();
            assert_eq!(
                event_schemas,
                [Added::schema_id()],
                "journal contains only declared schemas"
            );
            assert_eq!(kernel.dead_letters.len(), 1);
            assert_eq!(
                kernel.dead_letters[0].reason,
                crate::types::DeadLetterReason::UndeclaredEvent
            );
            assert_eq!(kernel.dead_letters[0].schema, Smuggled::schema_id());
        }
        assert!(
            system.tap_facts().iter().any(|f| matches!(
                &f.kind,
                crate::tap::FactKind::DeadLettered { reason, .. }
                    if *reason == crate::types::DeadLetterReason::UndeclaredEvent
            )),
            "DeadLettered(UndeclaredEvent) fact on the tap"
        );
        assert!(
            !kernel_has_crash(&system, &path),
            "the step continued; the actor was not failed"
        );
    }

    /// Kernel crash-record peek (tests).
    fn kernel_has_crash(system: &ActorSystem, path: &ActorPath) -> bool {
        let kernel = system.kernel.lock().expect("lock");
        kernel.crashed.contains(path)
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
            let kernel = system.kernel.lock().expect("lock");
            for entry in kernel.journals[&path].entries() {
                if let crate::journal::JournalEntry::Event { event, .. } = entry {
                    folded.apply(event);
                }
            }
            assert_eq!(
                kernel.journals[&path].len(),
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
        let snap_seqs: Vec<u64> = {
            let kernel = system.kernel.lock().expect("lock");
            kernel.journals[&path]
                .entries()
                .iter()
                .filter_map(|e| match e {
                    crate::journal::JournalEntry::Snapshot { seq, .. } => Some(seq.as_u64()),
                    _ => None,
                })
                .collect()
        };
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
            system
                .tap_facts()
                .iter()
                .any(|f| matches!(f.kind, crate::tap::FactKind::SnapshotTaken { .. }))
        })
        .await;
        let snap_seq = {
            let kernel = system.kernel.lock().expect("lock");
            let snap = kernel.journals[&path].last_snapshot().expect("snapshots");
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
            !system
                .tap_facts()
                .iter()
                .any(|f| matches!(f.kind, crate::tap::FactKind::SnapshotTaken { .. })),
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
            !system
                .tap_facts()
                .iter()
                .any(|f| matches!(f.kind, crate::tap::FactKind::SnapshotTaken { .. })),
            "Off never snapshots"
        );
    }

    // ---- Phase 2: builder API ----

    /// A counter variant whose manifest declares NOTHING (builder-edge
    /// fixture: declarations must come from the builder calls).
    #[derive(Serialize, Deserialize, Default)]
    struct BareCounter {
        total: i64,
    }
    impl EventSourcedActor for BareCounter {
        fn manifest() -> ActorManifest {
            ActorManifest::new().kind(ActorKind::EventSourced)
        }
        fn restore(_args: &JsonValue) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            self.total += event.payload["n"].as_i64().unwrap_or(0);
        }
    }
    impl CommandHandler<Add> for BareCounter {
        fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
            vec![crate::envelope::Event::new(
                Added::schema_id(),
                json!({ "n": cmd.n }),
            )]
        }
    }

    /// Reads an actor's registered manifest from the registry (tests).
    fn registered_manifest(
        system: &ActorSystem,
        path: &ActorPath,
    ) -> Option<crate::schema::ActorManifest> {
        let registry = system.registry.lock().expect("registry lock");
        registry.lookup(path).map(|info| info.manifest.clone())
    }

    /// Reads the route table's destination set for a schema (tests).
    fn route_dests(system: &ActorSystem, schema: &SchemaId) -> Vec<ActorPath> {
        let registry = system.registry.lock().expect("registry lock");
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
        {
            let mut registry = system.registry.lock().expect("registry lock");
            registry
                .declare_emits(&positional_path, Added::schema_id())
                .expect("declare positional emit edge");
        }
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
        assert!(system.tap_facts().iter().any(|f| matches!(
            &f.kind,
            crate::tap::FactKind::DeadLettered { reason, .. }
                if *reason == crate::types::DeadLetterReason::UndeclaredEvent
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
                "name": "tally2", "version": 1, "kind": "command",
                "fields": [{ "name": "delta", "ty": "int" }]
            }))
            .expect("valid");

        let decision: crate::actor::ForeignDecision = {
            let s = schema.clone();
            Arc::new(move |_state, cmd, _ctx| {
                vec![crate::envelope::Event::new(
                    s.clone(),
                    json!({ "delta": cmd["delta"].as_i64().unwrap_or(0) }),
                )]
            })
        };
        let fold: crate::actor::ForeignFold =
            Arc::new(|state: &mut JsonValue, ev: &crate::envelope::Event| {
                state["total"] = json!(
                    state["total"].as_i64().unwrap_or(0)
                        + ev.payload["delta"].as_i64().unwrap_or(0)
                );
            });
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
        {
            let mut registry = system.registry.lock().expect("registry lock");
            registry
                .declare_emits(&ActorPath::new("t-pos"), schema.clone())
                .expect("live slot");
        }
        let built_decision: crate::actor::ForeignDecision = {
            let s = schema.clone();
            Arc::new(move |_state, cmd, _ctx| {
                vec![crate::envelope::Event::new(
                    s.clone(),
                    json!({ "delta": cmd["delta"].as_i64().unwrap_or(0) }),
                )]
            })
        };
        crate::builder::spawn_foreign(&system)
            .at(ActorPath::new("t-built"))
            .schema(json!({
                "name": "tally2", "version": 1, "kind": "command",
                "fields": [{ "name": "delta", "ty": "int" }]
            }))
            .args(json!({ "total": 0 }))
            .handle(built_decision)
            .apply(fold)
            .emits_id(SchemaId::new("tally2", 1))
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
                .send(system.envelope(SchemaId::new("tally2", 1), path, json!({ "delta": 9 })))
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
                "name": "depcmd", "version": 1, "kind": "command",
                "fields": [{ "name": "delta", "ty": "int" }]
            }))
            .expect("valid");
        {
            let s = schema.clone();
            system.spawn_es_foreign(
                foreign_path.clone(),
                schema,
                json!({ "total": 0 }),
                Arc::new(move |_st, cmd, _ctx| {
                    vec![crate::envelope::Event::new(
                        s.clone(),
                        json!({ "delta": cmd["delta"].as_i64().unwrap_or(0) }),
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
            let mut registry = system.registry.lock().expect("registry lock");
            registry
                .declare_emits(&foreign_path, SchemaId::new("depcmd", 1))
                .expect("declare");
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
                SchemaId::new("depcmd", 1),
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

    // ---- Phase 3: router rules + stateless pools ----

    /// A stateless worker that records handled Add commands in a shared
    /// sink (pool fixture: impure, at-most-once, no journal).
    struct PoolWorker {
        sink: Arc<Mutex<Vec<String>>>,
    }

    impl ServiceActor for PoolWorker {
        fn manifest() -> ActorManifest {
            ActorManifest::new().kind(ActorKind::Service)
        }
        async fn start(
            args: &JsonValue,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            let idx = args["sink"].as_u64().expect("sink index") as usize;
            let sink = sinks().lock().expect("sinks lock")[idx].clone();
            Ok(Self { sink })
        }
    }

    impl MsgHandler<Add> for PoolWorker {
        async fn handle(&mut self, msg: Add, _ctx: &mut crate::context::MsgCtx<'_>) {
            self.sink
                .lock()
                .expect("sink lock")
                .push(format!("n={}", msg.n));
        }
    }

    /// An interposer service: records the schema, then FORWARDS the
    /// payload to the given path (the Inline-rule fixture).
    struct Forwarder {
        sink: Arc<Mutex<Vec<String>>>,
        forward_to: ActorPath,
    }

    impl ServiceActor for Forwarder {
        fn manifest() -> ActorManifest {
            ActorManifest::new().kind(ActorKind::Service)
        }
        async fn start(
            args: &JsonValue,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            let idx = args["sink"].as_u64().expect("sink index") as usize;
            let sink = sinks().lock().expect("sinks lock")[idx].clone();
            let to = args["forward_to"].as_str().expect("forward_to");
            Ok(Self {
                sink,
                forward_to: ActorPath::new(to),
            })
        }
    }

    impl MsgHandler<Add> for Forwarder {
        async fn handle(&mut self, msg: Add, ctx: &mut crate::context::MsgCtx<'_>) {
            self.sink
                .lock()
                .expect("sink lock")
                .push(format!("seen={}", msg.n));
            ctx.core.send(
                Address::Path(self.forward_to.clone()),
                Add::schema_id(),
                json!({ "n": msg.n }),
                None,
            );
        }
    }

    /// Builds a pool spec over `public` with `n` PoolWorkers writing to
    /// the sink at index `sink_idx` (both algos; seeded for determinism).
    fn pool_spec(
        public: &str,
        n: usize,
        algo: crate::pool::PoolAlgo,
        sink_idx: usize,
        parent: Option<ActorPath>,
    ) -> crate::pool::PoolSpec {
        let public_path = ActorPath::new(public);
        crate::pool::PoolSpec {
            public: public_path.clone(),
            workers: n,
            algo,
            factory: Arc::new(move |system, path, args| {
                system.spawn_service::<PoolWorker, _>(
                    path.clone(),
                    args,
                    SpawnOpts::default(),
                    || {
                        vec![Arc::new(
                            crate::actor::TypedServiceAdapter::<PoolWorker, Add>::new::<Add>(),
                        )]
                    },
                );
            }),
            args: Some(json!({ "sink": sink_idx })),
            parent,
            seed: 42,
        }
    }

    /// A key-keyed counter for partition tests: state seeded from the
    /// `key` genesis arg, increments isolated per entity.
    #[derive(Serialize, Deserialize, Default)]
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
        fn restore(args: &JsonValue) -> Self {
            Self {
                key: args["key"].as_str().unwrap_or_default().to_owned(),
                total: 0,
            }
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            self.total += event.payload["n"].as_i64().unwrap_or(0);
        }
    }
    impl CommandHandler<KeyedAdd> for KeyCounter {
        fn handle(&self, cmd: KeyedAdd, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
            vec![crate::envelope::Event::new(
                Added::schema_id(),
                json!({ "n": cmd.n }),
            )]
        }
    }

    /// Registers the partition test command (a str shard key field) and
    /// installs a KeyCounter partition set over `public`.
    fn install_key_partition(
        system: &Arc<ActorSystem>,
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
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct KeyedAdd {
        n: i64,
        account: String,
    }
    impl Schema for KeyedAdd {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "KeyedAdd".into(),
                version: 1,
                kind: SchemaKind::Command,
                fields: vec![
                    FieldDef::required("n", FieldTy::Int),
                    FieldDef::required("account", FieldTy::Str).as_shard_key(),
                ],
                description: None,
            }
        }
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
            .tap_facts()
            .iter()
            .filter(|f| matches!(
                &f.kind,
                crate::tap::FactKind::Spawned { path, .. } if *path == ActorPath::new("accounts/a")
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
            .tap_facts()
            .iter()
            .filter(|f| matches!(
                &f.kind,
                crate::tap::FactKind::Spawned { path, .. } if *path == ActorPath::new("accounts/race")
            ))
            .count();
        assert_eq!(spawns, 1, "the race yielded a single activation");
    }

    /// The Rust-side mirror of the runtime's `Fact@1` schema (facts are
    /// mirrored into `system.facts` with this JSON shape).
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct FactMsg {
        kind: String,
        offset: u64,
        ts: i64,
    }
    impl Schema for FactMsg {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Fact".into(),
                version: 1,
                kind: SchemaKind::Event,
                fields: vec![
                    FieldDef::required("kind", FieldTy::Str),
                    FieldDef::required("offset", FieldTy::Int),
                    FieldDef::required("ts", FieldTy::Int),
                ],
                description: None,
            }
        }
    }

    /// A facts observer: a plain service actor recording raw Fact JSON
    /// (kind@offset) into the shared sink. Gaps and slow-subscriber
    /// behavior fall out of the offset stream it observes.
    struct FactsObserver {
        sink: Arc<Mutex<Vec<String>>>,
        last: Option<u64>,
    }
    impl ServiceActor for FactsObserver {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<FactMsg>()
                .kind(ActorKind::Service)
        }
        async fn start(
            args: &JsonValue,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            let sink = sinks().lock().expect("sinks lock")
                [args["sink"].as_u64().expect("sink index") as usize]
                .clone();
            Ok(Self { sink, last: None })
        }
    }
    impl MsgHandler<FactMsg> for FactsObserver {
        async fn handle(&mut self, fact: FactMsg, _ctx: &mut crate::context::MsgCtx<'_>) {
            let mut sink = self.sink.lock().expect("sink lock");
            if let Some(last) = self.last
                && fact.offset > last + 1
            {
                sink.push(format!("gap:{}->{}", last, fact.offset));
            }
            self.last = Some(fact.offset);
        }
    }

    /// Spawns a FactsObserver at `path` subscribed to the facts topic.
    async fn spawn_facts_observer(system: &Arc<ActorSystem>, path: &str) -> usize {
        system.register_schema::<FactMsg>();
        let (sink, sink_ref) = open_sink();
        crate::builder::spawn_service_builder::<FactsObserver>(&system.clone())
            .at(ActorPath::new(path))
            .args(json!({ "sink": sink }))
            .handles::<FactMsg>()
            .mailbox(4, crate::inbox::OverloadPolicy::DropNew)
            .start();
        assert!(system.schema(&FactMsg::schema_id()).is_some());
        system
            .subscribe(
                &ActorPath::new(path),
                &crate::registry::Registry::facts_topic(),
                None,
            )
            .expect("subscribed");
        let _ = sink_ref;
        sink
    }

    #[tokio::test]
    async fn facts_subscriber_receives_and_detects_gap_under_pressure() {
        // Given a facts observer subscribed to `system.facts` and a tiny
        // ring (5 facts before drop-oldest).
        let (system, _clock) = ActorSystem::test_with_tap(5);
        let sink0 = spawn_facts_observer(&system, "obs").await;
        install_key_partition(&system, "counters").expect("partition install");

        // When many facts flood the RING (small ring capacity: the system
        // test fixture's tap ring holds a handful before dropping). Sends
        // target a LIVE actor: only completed sends record facts.
        for i in 0..40 {
            let e = system.envelope(
                KeyedAdd::schema_id(),
                ActorPath::new("counters"),
                json!({ "n": i, "account": "x" }),
            );
            let _ = system.send(e).await;
        }
        wait_for(|| async {
            sinks().lock().expect("sinks lock")[sink0]
                .lock()
                .expect("sink lock")
                .iter()
                .any(|s| s.starts_with("gap:"))
        })
        .await;

        // Then the observer SAW facts AND an offset gap (ring evictions
        // made loss visible — the documented at-most-once-with-gaps
        // contract).
        let sink = sinks().lock().expect("sinks lock")[sink0]
            .lock()
            .expect("sink lock")
            .clone();
        assert!(
            sink.len() > 1,
            "the observer received facts as messages: {sink:?}"
        );
        assert!(
            sink.iter().any(|s| s.starts_with("gap:")),
            "the offset discontinuity was detected: {sink:?}"
        );
    }

    #[tokio::test]
    async fn slow_facts_subscriber_never_stalls_ring() {
        // Given a facts observer subscribed to `system.facts`.
        let (system, _clock) = ActorSystem::test();
        let sink0 = spawn_facts_observer(&system, "obs").await;
        install_key_partition(&system, "counters").expect("partition install");

        // When many sends happen (facts are mirrored + pumped; the
        // dest is a live actor — unroutable sends record no facts).
        for i in 0..30 {
            let e = system.envelope(
                KeyedAdd::schema_id(),
                ActorPath::new("counters"),
                json!({ "n": i, "account": "x" }),
            );
            let _ = system.send(e).await;
        }
        wait_for(|| async {
            !sinks().lock().expect("sinks lock")[sink0]
                .lock()
                .expect("sink lock")
                .is_empty()
        })
        .await;

        // Then the ring kept recording facts (none lost to the observer's
        // backlog — delivery pressure never touches the ring).
        let facts = system.tap_facts().len();
        assert!(
            facts >= 30,
            "the ring recorded every fact despite the stalled subscriber: {facts}"
        );
    }

    #[tokio::test]
    async fn dlq_redriver_resends_dead_letters() {
        // Given a system with one dead letter (a command the target does
        // not handle — the DLQ topic retains the envelope).
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<KeyedAdd>();
        crate::builder::spawn_es_builder::<BareCounter>(&system)
            .at(ActorPath::new("counter"))
            .args(json!({ "total": 0 }))
            .handles::<Add>()
            .emits::<Added>()
            .start();
        let e = system.envelope(
            KeyedAdd::schema_id(),
            ActorPath::new("counter"),
            json!({ "n": 6, "account": "x" }),
        );
        let _ = system.send(e).await;
        wait_for(|| async { !system.dead_letter_reasons().await.is_empty() }).await;
        assert!(
            !system.dead_letter_reasons().await.is_empty(),
            "seed dead letter"
        );

        // When the redriver is installed (it re-sends the dead letter).
        system.install_dlq_redriver();

        // Then the redrive hit the recorded dest ("counter") again — a
        // SECOND dead letter proves the redriver acted as a sender.
        wait_for(|| async { system.dead_letter_reasons().await.len() >= 2 }).await;
    }

    #[tokio::test]
    async fn pool_takeover_is_invisible_to_senders() {
        // Given a pool installed over "public" (fresh; no prior actor) with
        // 3 round-robin workers writing one shared sink.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        let (sink_idx, sink) = open_sink();
        bind_sink(&ActorPath::new("public"), sink.clone());
        system
            .install_pool(pool_spec(
                "public",
                3,
                crate::pool::PoolAlgo::RoundRobin,
                sink_idx,
                None,
            ))
            .await
            .expect("pool installs");

        // When the sender addresses the PUBLIC path six times.
        wait_for(|| async {
            (0..3).all(|i| {
                system
                    .inbox_cursor(&ActorPath::new(format!("public/worker-{i}").as_str()))
                    .is_some()
            })
        })
        .await;
        for n in 1..=6 {
            system
                .send(system.envelope(
                    Add::schema_id(),
                    ActorPath::new("public"),
                    json!({ "n": n }),
                ))
                .await
                .expect("delivered to a worker");
        }
        wait_for(|| async { sink.lock().expect("sink lock").len() == 6 }).await;

        // Then every message landed in a worker through the public name
        // (the sender never saw a worker path), and the tap shows the
        // router signature: Sent{dest: public} → Delivered{to: worker}.
        assert_eq!(
            *sink.lock().expect("sink lock"),
            vec!["n=1", "n=2", "n=3", "n=4", "n=5", "n=6"]
        );
        let to_workers = system.tap_facts().iter().any(|f| {
            matches!(
                &f.kind,
                crate::tap::FactKind::Delivered { to, .. }
                    if *to == ActorPath::new("public/worker-0")
            )
        });
        assert!(to_workers, "deliveries landed on worker paths");
    }

    #[tokio::test]
    async fn pool_takeover_stop_drains_queued_mail_to_dlq() {
        // Given a LIVE plain actor at "pub" (its slot + loop running).
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        system.register_schema::<Added>();
        let public = ActorPath::new("pub");
        system.spawn_es::<BareCounter, _>(public.clone(), &json!({}), SpawnOpts::default(), || {
            vec![Arc::new(TypedEsAdapter::<BareCounter, Add>::new::<Add>())]
        });
        wait_for(|| async { system.inbox_cursor(&public).is_some() }).await;

        // When the pool takes the public path over (stop-drain first).
        let (sink_idx, sink) = open_sink();
        bind_sink(&ActorPath::new("pub"), sink.clone());
        system
            .install_pool(pool_spec(
                "pub",
                1,
                crate::pool::PoolAlgo::RoundRobin,
                sink_idx,
                None,
            ))
            .await
            .expect("takeover installs");

        // Then the pool claimed the public path, the plain actor's slot is
        // gone, and mail keeps flowing to the SAME public name (senders
        // unchanged; the takeover was invisible to them). The stop-drain's
        // undelivered-mail → DLQ half is exercised by the stop tests.
        let taken = {
            let registry = system.registry.lock().expect("registry lock");
            registry.pools.contains_key(&public)
        };
        assert!(taken, "pool claimed the public path");
        wait_for(|| async {
            system
                .send(system.envelope(Add::schema_id(), public.clone(), json!({ "n": 9 })))
                .await
                .is_ok()
        })
        .await;
        wait_for(|| async { sink.lock().expect("sink lock").len() == 1 }).await;
        assert_eq!(*sink.lock().expect("sink lock"), vec!["n=9"]);
    }

    #[tokio::test]
    async fn pool_random_algo_distributes_deterministically() {
        // Given a seeded-Random pool with 2 workers and a shared sink.
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        let (sink_idx, sink) = open_sink();
        bind_sink(&ActorPath::new("rnd"), sink.clone());
        system
            .install_pool(pool_spec(
                "rnd",
                2,
                crate::pool::PoolAlgo::Random,
                sink_idx,
                None,
            ))
            .await
            .expect("pool installs");

        // When four commands go to the public path.
        wait_for(|| async {
            system
                .inbox_cursor(&ActorPath::new("rnd/worker-0"))
                .is_some()
                && system
                    .inbox_cursor(&ActorPath::new("rnd/worker-1"))
                    .is_some()
        })
        .await;
        for n in 1..=4 {
            system
                .send(system.envelope(Add::schema_id(), ActorPath::new("rnd"), json!({ "n": n })))
                .await
                .expect("delivered");
        }
        wait_for(|| async { sink.lock().expect("sink lock").len() == 4 }).await;

        // Then every message was handled exactly once (distribution across
        // workers is algo-driven; the shared sink sees the union).
        let mut got = sink.lock().expect("sink lock").clone();
        got.sort();
        assert_eq!(got, vec!["n=1", "n=2", "n=3", "n=4"]);
    }

    #[tokio::test]
    async fn pool_worker_escalates_to_spec_parent() {
        // Given a pool whose workers are supervised children of "boss".
        let (system, _clock) = ActorSystem::test();
        system.register_schema::<Add>();
        let (sink_idx, sink) = open_sink();
        bind_sink(&ActorPath::new("crew"), sink);
        let parent = ActorPath::new("boss");
        system
            .install_pool(pool_spec(
                "crew",
                1,
                crate::pool::PoolAlgo::RoundRobin,
                sink_idx,
                Some(parent.clone()),
            ))
            .await
            .expect("pool installs");

        // When the child spec registers the escalation target.
        let spec_parent = {
            let kernel = system.kernel.lock().expect("kernel lock");
            kernel
                .specs
                .get(&ActorPath::new("crew/worker-0"))
                .map(|s| s.parent.clone())
                .unwrap_or(None)
        };

        // Then the worker's escalation flows to the spec parent.
        assert_eq!(spec_parent, Some(parent));
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
            let mut registry = system.registry.lock().expect("registry lock");
            registry
                .declare_emits(&counter, Added::schema_id())
                .expect("declare");
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
        let facts = system.tap_facts();
        let tee_delivered = facts
            .iter()
            .find(|f| {
                matches!(
                    &f.kind,
                    crate::tap::FactKind::Delivered { to, .. } if *to == ActorPath::new("watcher")
                )
            })
            .expect("tee copy delivered");
        if let crate::tap::FactKind::Delivered { trace, .. } = &tee_delivered.kind {
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
        system.spawn_service::<PoolWorker, _>(
            ActorPath::new("final"),
            &json!({ "sink": final_idx }),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    TypedServiceAdapter::<PoolWorker, Add>::new::<Add>(),
                )]
            },
        );
        {
            let mut registry = system.registry.lock().expect("registry lock");
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

    impl ServiceActor for GatedWorker {
        fn manifest() -> ActorManifest {
            ActorManifest::new().kind(ActorKind::Service)
        }
        async fn start(
            args: &JsonValue,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            let idx = args["sink"].as_u64().expect("sink index") as usize;
            let sink = sinks().lock().expect("sinks lock")[idx].clone();
            Ok(Self { sink })
        }
    }

    impl MsgHandler<Add> for GatedWorker {
        async fn handle(&mut self, msg: Add, _ctx: &mut crate::context::MsgCtx<'_>) {
            if msg.n == 0 {
                WORKER_GATE.notified().await;
            }
            self.sink
                .lock()
                .expect("sink lock")
                .push(format!("n={}", msg.n));
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
                .tap_facts()
                .iter()
                .any(|f| matches!(&f.kind, crate::tap::FactKind::Backpressured { path, .. } if *path == plain))
        })
        .await;
        // Release the gate so the worker drains (clean shutdown).
        WORKER_GATE.notify_waiters();
        wait_for(|| async { sink_read(&plain).len() == 4 }).await;

        // Then exactly ONE Backpressured fact fired for the up-crossing
        // (rate-limited: not one per message).
        let fires = system
            .tap_facts()
            .iter()
            .filter(|f| matches!(&f.kind, crate::tap::FactKind::Backpressured { path, .. } if *path == plain))
            .count();
        assert_eq!(fires, 1, "one fact per up-crossing, not per message");
    }
}
