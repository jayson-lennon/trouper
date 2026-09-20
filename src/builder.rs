//! The declarative spawn surface: builders over the erased spawn funnel.
//!
//! One call says each type ONCE. The typed builder accumulates handles,
//! emit edges, and mailbox/snapshot options, constructing the erased
//! adapters internally — the kernel still sees exactly what the positional
//! spawns see (one [`crate::actor::CommandEntry`] per handled schema,
//! declared edges in the manifest). The foreign builder replaces anonymous
//! closure parameters with named EventSourced-vocabulary methods
//! ([`ForeignBuilder::handle`] decides, [`ForeignBuilder::apply`] folds).
//!
//! Positional spawns (`spawn_es`, `spawn_service`, `spawn_es_foreign`)
//! remain only as deprecated thin wrappers over the same funnel.

use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::actor::{ActorKind, ActorPath};
use crate::actor::{
    CommandEntry, ForeignCommandEntry, ForeignDecision, ForeignEsState, ForeignFold, MsgEntry,
    ServiceActor, TypedEsAdapter, TypedServiceAdapter,
};
use crate::schema::Schema;
use crate::schema::SchemaId;
use crate::system::SpawnOpts;
use crate::topics::Topic;

/// Begins a typed spawn of event-sourced actor `A`.
///
/// ```ignore
/// let h = trouper::builder::spawn_es_builder::<Inventory>(&system)
///     .at("inventory.west")
///     .args(json!({ "on_hand": 100 }))
///     .handles::<ReserveStock>()
///     .emits::<StockReserved>()
///     .emits_on_topic("inventory.events")
///     .snapshot(SnapshotCadence::Messages(100))
///     .start();
/// ```
pub fn spawn_es_builder<A: crate::actor::EventSourcedActor>(
    system: &crate::system::ActorSystem,
) -> SpawnBuilder<A> {
    SpawnBuilder {
        system: system.clone(),
        path: None,
        args: JsonValue::Null,
        entries: Vec::new(),
        emits: Vec::new(),
        topics: Vec::new(),
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
        args: JsonValue::Null,
        start_override: None,
        entries: Vec::new(),
        emits: Vec::new(),
        subscribed: Vec::new(),
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
        genesis: JsonValue::Object(serde_json::Map::new()),
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
    args: JsonValue,
    entries: Vec<Arc<dyn CommandEntry>>,
    emits: Vec<SchemaId>,
    topics: Vec<Topic>,
    opts: SpawnOpts,
    _actor: std::marker::PhantomData<fn(&A)>,
}

impl<A: crate::actor::EventSourcedActor> SpawnBuilder<A> {
    /// The actor's identity (required; identity IS the path).
    pub fn at(mut self, path: impl Into<ActorPath>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Genesis arguments (seed state; snapshot restores need no args).
    pub fn args(mut self, args: JsonValue) -> Self {
        self.args = args;
        self
    }

    /// Declares a handled command `C`: registers the schema edge, the
    /// route, and — internally — the erased command adapter. `C` is
    /// written exactly once. Registration here is what makes `C`
    /// routable to this actor; a command type is deliverable only to
    /// actors that declared it.
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
        self.entries
            .push(Arc::new(TypedEsAdapter::<A, C>::new::<C>()));
        self
    }

    /// Declares an emitted event schema — an ENFORCED edge: the kernel
    /// drops undeclared emits before journal append.
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

    /// Declares a topic committed events are published onto.
    pub fn emits_on_topic(mut self, topic: impl Into<Topic>) -> Self {
        let topic = topic.into();
        if !self.topics.contains(&topic) {
            self.topics.push(topic);
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
        for topic in self.topics {
            if !manifest.emits_on_topics.contains(&topic) {
                manifest.emits_on_topics.push(topic);
            }
        }
        manifest = manifest.kind(ActorKind::EventSourced);
        let state = Box::new(crate::actor::TypedEsState::<A>::new(A::restore(&self.args)));
        self.system
            .spawn_es_erased(path.clone(), manifest, state, self.entries, self.opts);
        path
    }
}

/// The typed service builder.
pub struct ServiceBuilder<A: ServiceActor> {
    system: crate::system::ActorSystem,
    path: Option<ActorPath>,
    args: JsonValue,
    start_override: Option<crate::system::ServiceStart>,
    entries: Vec<Arc<dyn MsgEntry>>,
    emits: Vec<SchemaId>,
    subscribed: Vec<SchemaId>,
    opts: SpawnOpts,
    _actor: std::marker::PhantomData<fn(&A)>,
}

impl<A: ServiceActor> ServiceBuilder<A> {
    /// The actor's identity (required).
    pub fn at(mut self, path: impl Into<ActorPath>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Start arguments (I/O allowed inside `start`).
    pub fn args(mut self, args: JsonValue) -> Self {
        self.args = args;
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

    /// Declares a handled message `M` (schema edge + route + adapter).
    /// Registration here is what makes `M` routable to this actor; a
    /// message type is deliverable only to actors that declared it.
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
        self.entries
            .push(Arc::new(TypedServiceAdapter::<A, M>::new::<M>()));
        self
    }

    /// Declares a SUBSCRIPTION to event schema `M`: every event publish
    /// of `M` (see [`ActorSystem::publish`]) is copied into this actor's
    /// inbox — insertion order, no round-robin. Subscribing never makes
    /// this actor a dispatch target for `M` (no route, no handler entry);
    /// a `.handles::<M>()` declaration is independent (commands route to
    /// handlers, events fan out to subscribers). To RECEIVE the event the
    /// actor must also handle its type — declare both.
    ///
    /// `M`'s schema descriptor is registered into the schema table here,
    /// at the declaration site. Zero subscribers ⇒ publish is a no-op:
    /// events are news, not work orders.
    pub fn subscribe<M>(mut self) -> Self
    where
        M: Schema + serde::de::DeserializeOwned + Send + 'static,
    {
        self.system.register_schema::<M>();
        let id = M::schema_id();
        if !self.subscribed.contains(&id) {
            self.subscribed.push(id);
        }
        self
    }

    /// Declares an emitted message schema for a service actor — a
    /// DECLARED edge (advisory: the service tier is not emit-enforced,
    /// unlike [`SpawnBuilder::emits`]), published in the manifest so the
    /// export/GUI shows the actor's outputs.
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
        for schema in &self.subscribed {
            if !manifest.subscribed.contains(schema) {
                manifest.subscribed.push(schema.clone());
            }
        }
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
/// decision + fold closures attached by NAME instead of positional soup.
pub struct ForeignBuilder {
    system: crate::system::ActorSystem,
    path: Option<ActorPath>,
    schema_json: Option<JsonValue>,
    genesis: JsonValue,
    emits: Vec<SchemaId>,
    decision: Option<ForeignDecision>,
    fold: Option<ForeignFold>,
    opts: SpawnOpts,
}

impl ForeignBuilder {
    /// The actor's identity (required).
    pub fn at(mut self, path: impl Into<ActorPath>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// The command schema descriptor this foreign actor handles.
    /// Registered into the schema table at `start`.
    pub fn schema(mut self, json: JsonValue) -> Self {
        self.schema_json = Some(json);
        self
    }

    /// Genesis JSON state.
    pub fn args(mut self, genesis: JsonValue) -> Self {
        self.genesis = genesis;
        self
    }

    /// The decision closure: (state, command, ctx) → events. Pure.
    pub fn handle(mut self, f: crate::actor::ForeignDecision) -> Self {
        self.decision = Some(f);
        self
    }

    /// The fold closure: (state, event) — THE mutation, live and replay.
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
        let state = Box::new(ForeignEsState::new(self.genesis, fold));
        let mut manifest = crate::schema::ActorManifest::new()
            .handles_id(schema_id.clone())
            .kind(ActorKind::EventSourced);
        for id in self.emits {
            manifest = manifest.emits_id(id);
        }
        let entries: Vec<Arc<dyn CommandEntry>> =
            vec![Arc::new(ForeignCommandEntry::new(schema_id, decision))];
        self.system
            .spawn_es_erased(path.clone(), manifest, state, entries, self.opts);
        Ok(path)
    }
}
