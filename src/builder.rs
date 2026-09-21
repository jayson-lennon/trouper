//! The declarative spawn surface: builders over the runtime's
//! type-erased spawn machinery.
//!
//! One call says each type ONCE. The typed builder accumulates handles,
//! emit edges, and mailbox/snapshot options, constructing the erased
//! adapters internally — the runtime still sees exactly what the positional
//! spawns see (one [`crate::actor::CommandEntry`] per handled schema,
//! declared edges in the manifest). The foreign builder replaces anonymous
//! closure parameters with named EventSourced-vocabulary methods
//! ([`ForeignBuilder::handle`] decides, [`ForeignBuilder::apply`] folds).
//!
//! Positional spawns (`spawn_es`, `spawn_service`, `spawn_es_foreign`)
//! remain only as deprecated thin wrappers over the same machinery.

use std::sync::Arc;

use crate::actor::{ActorKind, ActorPath};
use crate::actor::{
    CommandEntry, ForeignCommandEntry, ForeignDecision, ForeignEsState, ForeignFold, MsgEntry,
    ServiceActor, TypedEsAdapter, TypedServiceAdapter,
};
use crate::json::Json;
use crate::schema::Schema;
use crate::schema::SchemaId;
use crate::system::SpawnOpts;

/// Begins a typed spawn of event-sourced actor `A`.
///
/// ```ignore
/// let h = trouper::builder::spawn_es_builder::<Inventory>(&system)
///     .at("inventory.west")
///     .args(json!({ "on_hand": 100 }))
///     .handles::<ReserveStock>()
///     .emits::<StockReserved>()
///     .snapshot(SnapshotCadence::Messages(100))
///     .start();
/// ```
pub fn spawn_es_builder<A: crate::actor::EventSourcedActor>(
    system: &crate::system::ActorSystem,
) -> SpawnBuilder<A> {
    SpawnBuilder {
        system: system.clone(),
        path: None,
        args: Json::default(),
        entries: Vec::new(),
        emits: Vec::new(),
        opts: SpawnOpts::default(),
        _actor: std::marker::PhantomData,
    }
}

/// Begins a typed spawn of service actor `A`.
pub fn spawn_service_builder<A: ServiceActor>(
    system: &crate::system::ActorSystem,
) -> ServiceBuilder<A> {
    ServiceBuilder {
        system: system.clone(),
        path: None,
        args: Json::default(),
        start_override: None,
        entries: Vec::new(),
        emits: Vec::new(),
        opts: SpawnOpts::default(),
        _actor: std::marker::PhantomData,
    }
}

/// Begins a foreign (no-Rust-types) event-sourced spawn.
pub fn spawn_foreign(system: &crate::system::ActorSystem) -> ForeignBuilder {
    ForeignBuilder {
        system: system.clone(),
        path: None,
        schema_json: None,
        genesis: crate::json!({}),
        emits: Vec::new(),
        decision: None,
        fold: None,
        opts: SpawnOpts::default(),
    }
}

/// The typed event-sourced builder. Every type is named exactly once:
/// `A` at [`spawn_es_builder`], each command `C` at [`SpawnBuilder::handles`],
/// each event `E` at [`SpawnBuilder::emits`].
pub struct SpawnBuilder<A: crate::actor::EventSourcedActor> {
    system: crate::system::ActorSystem,
    path: Option<ActorPath>,
    args: Json,
    entries: Vec<Arc<dyn CommandEntry>>,
    emits: Vec<SchemaId>,
    opts: SpawnOpts,
    _actor: std::marker::PhantomData<fn(&A)>,
}

impl<A: crate::actor::EventSourcedActor> SpawnBuilder<A> {
    /// The path to spawn the actor at (required).
    pub fn at(mut self, path: impl Into<ActorPath>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Genesis arguments (seed state; snapshot restores need no args).
    ///
    /// Accepts any serializable value — a typed genesis struct or a
    /// `json!` literal. The value serializes once, here; serialization
    /// failure panics (a programmer error, not a domain outcome).
    pub fn args<T: serde::Serialize>(mut self, value: T) -> Self {
        self.args = Json::of(&value);
        self
    }

    /// Declares a handled command `C` — the one receive declaration. It
    /// installs the route and the dispatch entry together: whether a copy
    /// arrives via `tell`, `send_to_any`, or `publish` is invisible to the
    /// receiver. `C` is written exactly once; registration here is what
    /// makes `C` deliverable to this actor.
    ///
    /// `C`'s schema descriptor is registered into the schema table here,
    /// at the declaration site — spawning through the builder is all a
    /// caller needs for `system.export()` to carry the full contract.
    pub fn handles<C>(mut self) -> Self
    where
        A: crate::actor::CommandHandler<C>,
        C: Schema + serde::de::DeserializeOwned + Send + 'static,
    {
        self.system.register_schema::<C>();
        let id = C::schema_id();
        // Dedup per schema: a repeated declaration must never push a
        // second adapter (a double adapter would double-dispatch).
        if !self.entries.iter().any(|e| e.schema() == id) {
            self.entries
                .push(Arc::new(TypedEsAdapter::<A, C>::new::<C>()));
        }
        self
    }

    /// Declares an emitted event schema — an enforced edge: the runtime
    /// drops undeclared emits before the journal append.
    ///
    /// `E`'s schema descriptor is registered into the schema table here,
    /// at the declaration site (idempotent — see
    /// [`crate::registry::SchemaTable::register`]).
    pub fn emits<E: Schema>(mut self) -> Self {
        self.system.register_schema::<E>();
        let id = E::schema_id();
        if !self.emits.contains(&id) {
            self.emits.push(id);
        }
        self
    }

    /// The snapshot cadence (default Off).
    pub fn snapshot(mut self, cadence: crate::actor::SnapshotCadence) -> Self {
        self.opts.snapshot = cadence;
        self
    }

    /// Mailbox capacity and overload policy (default 64 / Block).
    pub fn mailbox(mut self, capacity: usize, policy: crate::inbox::OverloadPolicy) -> Self {
        self.opts.mailbox_capacity = capacity;
        self.opts.mailbox_policy = policy;
        self
    }

    /// Inbox depth at which a `Backpressured` fact fires (once/crossing).
    pub fn high_watermark(mut self, depth: u64) -> Self {
        self.opts.high_watermark = Some(depth);
        self
    }

    /// Declarative idle passivation: the runtime stops this actor after
    /// `idle_for` without a completed message step (close-door-then-drain,
    /// `Stopped { Passivated }` fact; a partition set re-spawns it on the
    /// next send). See [`crate::system::Passivation`].
    pub fn passivate_after(mut self, idle_for: std::time::Duration) -> Self {
        self.opts.passivation = Some(crate::system::Passivation { idle_for });
        self
    }

    /// Starts the actor; returns its path.
    ///
    /// # Panics
    ///
    /// Panics when `.at()` was never called, or the path is already taken.
    pub fn start(self) -> ActorPath {
        let path = self.path.clone().expect("builder requires .at(path)");
        // The actor's own manifest is the base; the builder's explicit
        // edges are merged on top (an actor type whose manifest already
        // declares edges stays compatible).
        let mut manifest = A::manifest();
        for entry in &self.entries {
            let schema = entry.schema();
            if !manifest.handles.contains(&schema) {
                manifest.handles.push(schema);
            }
        }
        for schema in self.emits {
            if !manifest.emits.contains(&schema) {
                manifest.emits.push(schema);
            }
        }
        manifest = manifest.kind(ActorKind::EventSourced);
        let state = Box::new(crate::actor::TypedEsState::<A>::new(A::restore(&self.args)));
        self.system.spawn_es_erased(
            path.clone(),
            manifest,
            state,
            self.entries,
            self.opts,
            &self.args,
        );
        path
    }
}

/// The typed service builder.
pub struct ServiceBuilder<A: ServiceActor> {
    system: crate::system::ActorSystem,
    path: Option<ActorPath>,
    args: Json,
    start_override: Option<crate::system::ServiceStart>,
    entries: Vec<Arc<dyn MsgEntry>>,
    emits: Vec<SchemaId>,
    opts: SpawnOpts,
    _actor: std::marker::PhantomData<fn(&A)>,
}

impl<A: ServiceActor> ServiceBuilder<A> {
    /// The path to spawn the actor at (required).
    pub fn at(mut self, path: impl Into<ActorPath>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Start arguments (I/O allowed inside `start`).
    ///
    /// Accepts any serializable value; it serializes once, here.
    /// Serialization failure panics (a programmer error).
    pub fn args<T: serde::Serialize>(mut self, value: T) -> Self {
        self.args = Json::of(&value);
        self
    }

    /// Overrides the constructor used at spawn: instead of
    /// `A::start(&args)`, the provided future builds the instance.
    ///
    /// For actors whose dependencies are typed values that cannot ride
    /// JSON args (cells, channels, backend handles): capture them in a
    /// closure and hand the future here. The manifest, edges, and
    /// options are unaffected — `A::start` remains the default
    /// constructor when this is not called.
    pub fn start_with(
        mut self,
        start: impl FnOnce() -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<A, error_stack::Report<crate::registry::RegistryError>>,
                    > + Send,
            >,
        > + Send
        + 'static,
    ) -> Self {
        self.start_override = Some(Box::pin(async move {
            start().await.map(|instance| {
                Box::new(crate::actor::TypedServiceState::new(instance))
                    as Box<dyn crate::actor::DynServiceActor>
            })
        }));
        self
    }

    /// Declares a handled message `M` — the one receive declaration. It
    /// installs the route and the dispatch entry together: whether a copy
    /// arrives via `tell`, `send_to_any`, or `publish` is invisible to the
    /// receiver. `M` is written exactly once; registration here is what
    /// makes `M` deliverable to this actor.
    ///
    /// `M`'s schema descriptor is registered into the schema table here,
    /// at the declaration site — mirroring the typed ES builder and the
    /// foreign builder (which registers at `start`).
    pub fn handles<M>(mut self) -> Self
    where
        A: crate::actor::MsgHandler<M>,
        M: Schema + serde::de::DeserializeOwned + Send + 'static,
    {
        self.system.register_schema::<M>();
        let id = M::schema_id();
        // Dedup per schema: a repeated declaration must never push a
        // second adapter (a double adapter would double-dispatch).
        if !self.entries.iter().any(|e| e.schema() == id) {
            self.entries
                .push(Arc::new(TypedServiceAdapter::<A, M>::new::<M>()));
        }
        self
    }

    /// Declares an emitted message schema for a service actor. The
    /// flush-time gate drops every outbound message whose schema is not
    /// declared here, as an `UndeclaredEmit` dead letter — a service actor
    /// publishing undeclared schemas never silently delivers. Declared
    /// edges are published in the manifest so the export/GUI shows the
    /// actor's outputs.
    ///
    /// `E`'s schema descriptor is registered into the schema table here,
    /// at the declaration site.
    pub fn emits<E: Schema>(mut self) -> Self {
        self.system.register_schema::<E>();
        if !self.emits.contains(&E::schema_id()) {
            self.emits.push(E::schema_id());
        }
        self
    }

    /// Mailbox capacity and overload policy.
    pub fn mailbox(mut self, capacity: usize, policy: crate::inbox::OverloadPolicy) -> Self {
        self.opts.mailbox_capacity = capacity;
        self.opts.mailbox_policy = policy;
        self
    }

    /// Inbox depth at which a `Backpressured` fact fires.
    pub fn high_watermark(mut self, depth: u64) -> Self {
        self.opts.high_watermark = Some(depth);
        self
    }

    /// Declarative idle passivation: the runtime stops this actor after
    /// `idle_for` without a completed message step (close-door-then-drain,
    /// `Stopped { Passivated }` fact; a partition set re-spawns it on the
    /// next send). See [`crate::system::Passivation`].
    pub fn passivate_after(mut self, idle_for: std::time::Duration) -> Self {
        self.opts.passivation = Some(crate::system::Passivation { idle_for });
        self
    }

    /// Starts the actor; returns its path.
    ///
    /// # Panics
    ///
    /// Panics when `.at()` was never called, or the path is already taken.
    pub fn start(self) -> ActorPath {
        let path = self.path.clone().expect("builder requires .at(path)");
        let mut manifest = A::manifest();
        for entry in &self.entries {
            let schema = entry.schema();
            if !manifest.handles.contains(&schema) {
                manifest.handles.push(schema);
            }
        }
        for schema in self.emits {
            if !manifest.emits.contains(&schema) {
                manifest.emits.push(schema);
            }
        }
        manifest = manifest.kind(ActorKind::Service);
        let start_args = self.args.clone();
        let start: crate::system::ServiceStart = match self.start_override {
            Some(overridden) => overridden,
            None => Box::pin(async move {
                A::start(&start_args).await.map(|instance| {
                    Box::new(crate::actor::TypedServiceState::new(instance))
                        as Box<dyn crate::actor::DynServiceActor>
                })
            }),
        };
        self.system.spawn_service_erased(
            path.clone(),
            manifest,
            &self.args,
            self.entries,
            self.opts,
            start,
        );
        path
    }
}

/// The foreign event-sourced builder: JSON schema in, JSON state out,
/// decision and fold closures attached by name.
pub struct ForeignBuilder {
    system: crate::system::ActorSystem,
    path: Option<ActorPath>,
    schema_json: Option<Json>,
    genesis: Json,
    emits: Vec<SchemaId>,
    decision: Option<ForeignDecision>,
    fold: Option<ForeignFold>,
    opts: SpawnOpts,
}

impl ForeignBuilder {
    /// The path to spawn the actor at (required).
    pub fn at(mut self, path: impl Into<ActorPath>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// The command schema descriptor this foreign actor handles.
    /// Registered into the schema table at `start`.
    pub fn schema(mut self, json: Json) -> Self {
        self.schema_json = Some(json);
        self
    }

    /// Genesis JSON state. Accepts a `json!` literal or any `Json`
    /// (foreign actors seed from the raw document by design).
    pub fn args(mut self, genesis: Json) -> Self {
        self.genesis = genesis;
        self
    }

    /// The decision closure: (state, command, ctx) → events. Pure.
    pub fn handle(mut self, f: crate::actor::ForeignDecision) -> Self {
        self.decision = Some(f);
        self
    }

    /// The fold closure: `(state, event)` — applies each event to the
    /// state. Runs for live delivery and replay alike.
    pub fn apply(mut self, f: crate::actor::ForeignFold) -> Self {
        self.fold = Some(f);
        self
    }

    /// Declares an emit edge by id (the kernel enforces these).
    pub fn emits_id(mut self, id: SchemaId) -> Self {
        if !self.emits.contains(&id) {
            self.emits.push(id);
        }
        self
    }

    /// Snapshot cadence.
    pub fn snapshot(mut self, cadence: crate::actor::SnapshotCadence) -> Self {
        self.opts.snapshot = cadence;
        self
    }

    /// Mailbox capacity and overload policy.
    pub fn mailbox(mut self, capacity: usize, policy: crate::inbox::OverloadPolicy) -> Self {
        self.opts.mailbox_capacity = capacity;
        self.opts.mailbox_policy = policy;
        self
    }

    /// Starts the actor; returns its path.
    ///
    /// # Errors
    ///
    /// [`crate::schema::SchemaError::InvalidDescriptor`] when `.at()`,
    /// `.schema()`, `.handle()`, or `.apply()` was never called, or the
    /// schema descriptor fails to parse.
    pub fn start(self) -> Result<ActorPath, error_stack::Report<crate::schema::SchemaError>> {
        use error_stack::{IntoReport, ResultExt};
        let invalid = || crate::schema::SchemaError::InvalidDescriptor.into_report();
        let path = self.path.ok_or_else(invalid)?;
        let schema_json = self.schema_json.ok_or_else(invalid)?;
        let decision = self.decision.ok_or_else(invalid)?;
        let fold = self.fold.ok_or_else(invalid)?;
        let schema_id = self
            .system
            .register_schema_json(schema_json)
            .change_context(crate::schema::SchemaError::InvalidDescriptor)?;
        let state = Box::new(ForeignEsState::new(self.genesis.clone(), fold));
        let mut manifest = crate::schema::ActorManifest::new()
            .handles_id(schema_id.clone())
            .kind(ActorKind::EventSourced);
        for id in self.emits {
            manifest = manifest.emits_id(id);
        }
        let entries: Vec<Arc<dyn CommandEntry>> =
            vec![Arc::new(ForeignCommandEntry::new(schema_id, decision))];
        self.system.spawn_es_erased(
            path.clone(),
            manifest,
            state,
            entries,
            self.opts,
            &self.genesis,
        );
        Ok(path)
    }
}

/// Begins a projector spawn: a read model `P` that folds other actors'
/// recorded facts.
///
/// ```ignore
/// spawn_projector_builder::<Balances>(&system)
///     .at(ActorPath::new("proj/balances"))
///     .consumes::<Deposited>()
///     .consumes::<Withdrawn>()
///     .start_and_catchup()   // or .start().await for background seeding
///     .await;
/// ```
///
/// A projector is an event-sourced actor whose consumed facts are
/// re-recorded into its own journal (the checkpoint): on spawn it replays
/// its journal, scans the store for what it lacks, seeds the gap, and only
/// then opens its inbox loop — live facts published during seeding queue
/// up and fold after history. Passivation is deliberately not offered
/// here: a standalone passivated projector has no wake path and would
/// never re-activate. Passivation for projectors exists only through
/// [`crate::pool::ProjectorSetSpec`].
pub fn spawn_projector_builder<P: crate::actor::Projector>(
    system: &crate::system::ActorSystem,
) -> ProjectorBuilder<P> {
    ProjectorBuilder {
        system: system.clone(),
        path: None,
        args: Json::default(),
        consumed: Vec::new(),
        opts: SpawnOpts::default(),
        _actor: std::marker::PhantomData,
    }
}

/// The typed projector builder. `P` is named once at
/// [`spawn_projector_builder`]; each consumed fact schema at
/// [`ProjectorBuilder::consumes`].
pub struct ProjectorBuilder<P: crate::actor::Projector> {
    system: crate::system::ActorSystem,
    path: Option<ActorPath>,
    args: Json,
    consumed: Vec<SchemaId>,
    opts: SpawnOpts,
    _actor: std::marker::PhantomData<fn(&P)>,
}

impl<P: crate::actor::Projector> ProjectorBuilder<P> {
    /// The path to spawn the projector at (required).
    pub fn at(mut self, path: impl Into<ActorPath>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Spawn arguments. A projector's genesis is [`Default`]; args
    /// are carried for manifests/export only.
    ///
    /// Accepts any serializable value; it serializes once, here.
    /// Serialization failure panics (a programmer error).
    pub fn args<T: serde::Serialize>(mut self, value: T) -> Self {
        self.args = Json::of(&value);
        self
    }

    /// Declares a consumed fact schema `D`: this projector handles `D` (a
    /// route is installed — broadcast copies included) and re-records every
    /// folded copy into its own journal with a CatchUp origin (the
    /// checkpoint). Repeat per schema; `D` is written exactly once.
    ///
    /// `D`'s descriptor is registered into the schema table here, at the
    /// declaration site.
    pub fn consumes<D: Schema>(mut self) -> Self {
        self.system.register_schema::<D>();
        let id = D::schema_id();
        if !self.consumed.contains(&id) {
            self.consumed.push(id);
        }
        self
    }

    /// The snapshot cadence (default Off). A projector's snapshot folds
    /// its re-recorded journal like any other.
    pub fn snapshot(mut self, cadence: crate::actor::SnapshotCadence) -> Self {
        self.opts.snapshot = cadence;
        self
    }

    /// Mailbox capacity and overload policy (default 64 / Block).
    pub fn mailbox(mut self, capacity: usize, policy: crate::inbox::OverloadPolicy) -> Self {
        self.opts.mailbox_capacity = capacity;
        self.opts.mailbox_policy = policy;
        self
    }

    /// Inbox depth at which a `Backpressured` fact fires (once/crossing).
    pub fn high_watermark(mut self, depth: u64) -> Self {
        self.opts.high_watermark = Some(depth);
        self
    }

    /// Starts the actor; arms synchronously (slot + routes exist the
    /// moment this returns — an activation factory can rely on it) and
    /// returns the path immediately. Catch-up continues in the background:
    /// the fold is complete only after a `CaughtUp { .. }` tap fact for
    /// this path.
    ///
    /// # Panics
    ///
    /// Panics when `.at()` was never called, the path is already taken, or
    /// a consumed schema is registered as a Command (a projector folds
    /// facts, not commands). Requires a tokio runtime (the catch-up task
    /// spawns onto it).
    pub fn start(self) -> ActorPath {
        let (system, path, consumed, args, opts) = self.validated_parts();
        let armed = system.arm_projector::<P>(&path, &consumed, &args, opts);
        tokio::spawn(crate::system::catch_up_projector(system, armed, consumed));
        path
    }

    /// Starts the actor and awaits its catch-up: the returned projector
    /// has folded every fact the store held at spawn time (later facts
    /// arrive live). A `CaughtUp { path, seeded }` fact records the
    /// outcome.
    ///
    /// # Panics
    ///
    /// Panics when `.at()` was never called, the path is already taken, or
    /// a consumed schema is registered as a Command.
    pub async fn start_and_catchup(self) -> ActorPath {
        let (system, path, consumed, args, opts) = self.validated_parts();
        let armed = system.arm_projector::<P>(&path, &consumed, &args, opts);
        crate::system::catch_up_projector(system, armed, consumed).await;
        path
    }

    /// Shared validation + part assembly (register → kind check →
    /// consumed list). Kind checking happens HERE, not at `.consumes()`:
    /// a def can only be validated after its registration.
    fn validated_parts(
        self,
    ) -> (
        crate::system::ActorSystem,
        ActorPath,
        Vec<SchemaId>,
        Json,
        SpawnOpts,
    ) {
        let ProjectorBuilder {
            system,
            path,
            args,
            consumed,
            opts,
            _actor,
        } = self;
        let path = path.expect("builder requires .at(path)");
        for schema in &consumed {
            let def = system
                .schema(schema)
                .unwrap_or_else(|| panic!("consumed schema {schema} vanished after registration"));
            assert!(
                def.kind == crate::schema::SchemaKind::Event,
                "projector at {path} consumes {schema}, which is a Command — \
                 projectors fold facts, not commands"
            );
        }
        (system, path, consumed, args, opts)
    }
}
