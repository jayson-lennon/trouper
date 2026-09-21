//! The two-tier actor contract.
//!
//! Tier 1 — [`EventSourced`]: pure decision functions. `handle` is sync and
//! takes `&self`, so no I/O and no await are physically possible; the events
//! it returns are facts. [`EventSourced::apply`] is THE mutation site, used
//! identically for live application and replay.
//!
//! Tier 2 — [`ServiceActor`]: impure by design (Phase 5).
//!
//! The kernel only ever sees the erased shells ([`DynEsActor`],
//! [`CommandEntry`]); generic adapters erase the Rust types exactly once at
//! spawn, which is what lets foreign actors (no Rust types at all) share
//! every table and code path.

use std::sync::Arc;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt;

use crate::context::CmdCtx;
use crate::journal::JournalError;
use crate::json::Json;
use crate::schema::{ActorManifest, Schema, SchemaId};

/// Event-sourced domain actor: pure, journaled, replayable.
///
/// Implementors get journal-backed restart for free; snapshots are opt-in
/// via the spawn policy and go through [`EventSourced::capture`] /
/// [`EventSourced::restore_from`].
pub trait EventSourcedActor:
    Send + Sync + Serialize + DeserializeOwned + Default + 'static
{
    /// Declares edges and the contract kind (always [`ActorKind::EventSourced`]).
    ///
    /// Defaults to an EMPTY manifest: the typed spawn builder stamps the
    /// contract kind and merges its declared edges, making the builder the
    /// single source of an actor's declared surface. Override only when
    /// spawning through the positional entry points, which take edges from
    /// here.
    fn manifest() -> ActorManifest {
        ActorManifest::new()
    }

    /// Genesis state — a fresh instance (no snapshot exists).
    ///
    /// `args` are spawn arguments; use them to seed initial state. Decode
    /// them with [`Json::decode`] into a genesis type:
    ///
    /// ```ignore
    /// fn restore(args: &Json) -> Self {
    ///     let g: Genesis = args.decode().expect("genesis args");
    ///     Self { on_hand: g.on_hand }
    /// }
    /// ```
    ///
    /// The default ignores args and constructs `Self::default()` (the
    /// trait requires [`Default`] — event-sourced state is snapshot data,
    /// always constructible empty) — most actors need no override.
    fn restore(args: &Json) -> Self {
        let _ = args;
        Self::default()
    }

    /// THE mutation. Used for live application AND replay — one code path,
    /// so live state and replayed state can never diverge.
    fn apply(&mut self, event: &crate::envelope::Event);

    /// Snapshot seam: serialize state. Default = state IS the snapshot.
    ///
    /// # Errors
    ///
    /// Fails when the state cannot be serialized to JSON.
    fn capture(&self) -> Result<Json, error_stack::Report<JournalError>> {
        use error_stack::ResultExt;
        Ok::<Json, error_stack::Report<JournalError>>(Json::of(self))
            .change_context(JournalError::Snapshot)
    }

    /// Snapshot seam: rebuild from a snapshot blob. Default = decode JSON.
    ///
    /// Takes no spawn args: a snapshot is self-sufficient; args matter only
    /// at genesis. This override IS the sanctioned cache-hydration hook for
    /// `#[serde(skip)]` fields.
    ///
    /// # Errors
    ///
    /// Fails when the blob does not decode into this state type.
    fn restore_from(snap: Json) -> Result<Self, error_stack::Report<JournalError>> {
        use error_stack::ResultExt;
        snap.decode::<Self>().change_context(JournalError::Restore)
    }

    /// Graceful-stop hook: runs ONCE after the final inbox drain, with
    /// the fully folded state, on external stop, self-stop, passivation,
    /// and the shutdown sweep. NEVER on crash (the instance is poisoned
    /// mid-panic) or hard [`crate::system::ActorSystem::shutdown`].
    ///
    /// Sync and `&self` — the ES tier is pure; there is no context, no
    /// I/O, no await. Observation only (export a summary, stamp a metric).
    fn on_stop(&self) {}
}

/// Typed sugar over the erased dispatch table: a pure decision function for
/// one command type. Registered once per (actor, command) pair at spawn.
pub trait CommandHandler<C>: EventSourcedActor {
    /// Decides: given current state and the command, which events happen?
    ///
    /// Sync, `&self` — no I/O, no await, no mutation. Effects are *declared*
    /// through `ctx` (deferred to post-ack by the kernel).
    ///
    /// Returns an [`Events`](crate::envelope::Events) buffer: build events
    /// from typed facts ([`Events::one`](crate::envelope::Events::one),
    /// [`Events::push_event`](crate::envelope::Events::push_event)) — never
    /// by hand.
    fn handle(&self, cmd: C, ctx: &mut CmdCtx<'_>) -> crate::envelope::Events;
}

/// A read model: a state struct that folds other actors' recorded facts.
///
/// The struct IS the actor (like [`EventSourcedActor`], no wrapper): one
/// fold — [`Projector::apply`] — runs identically for live deliveries,
/// catch-up seeding, and restart replay, so a projector can never show one
/// answer to history and another to the present. No `manifest`: the
/// projector builder is the single source of the declared surface — every
/// `.consumes` schema becomes both a handled input and a re-recorded
/// output (the journal IS the checkpoint).
///
/// [`Default`] is the genesis: replay-from-nothing and rebuild-from-genesis
/// construct the fold deterministically from it.
pub trait Projector: Send + Sync + Serialize + DeserializeOwned + Default + 'static {
    /// THE fold. Same code for history (catch-up), live facts, and restart
    /// replay. Match on `event.schema` — unmatched schemas are ignored, so
    /// one read model can consume several fact types (declare each with
    /// `.consumes`).
    fn apply(&mut self, event: &crate::envelope::Event);

    /// Graceful-stop hook: runs ONCE after the final drain, with the fully
    /// folded state, on external stop / self-stop / the shutdown sweep.
    /// Sync and `&self` — observation only.
    fn on_stop(&self) {}
}

/// Edge/service actor: async, I/O and `ask` allowed; NOT journaled.
pub trait ServiceActor: Send + 'static {
    /// Declares edges and the contract kind (always [`ActorKind::Service`]).
    ///
    /// Defaults to an EMPTY manifest: the typed spawn builder stamps the
    /// contract kind and merges its declared edges, making the builder the
    /// single source of an actor's declared surface. Override only when
    /// spawning through the positional entry points, which take edges from
    /// here.
    fn manifest() -> ActorManifest {
        ActorManifest::new()
    }

    /// Constructs the actor instance. I/O allowed.
    ///
    /// # Errors
    ///
    /// Fails when the service cannot start (the spawn fails).
    fn start(
        args: &Json,
    ) -> impl Future<Output = Result<Self, error_stack::Report<crate::registry::RegistryError>>> + Send
    where
        Self: Sized;

    /// Graceful-stop hook: runs ONCE after the final inbox drain, on
    /// external stop, self-stop, passivation, and the shutdown sweep.
    /// NEVER on crash (the instance is poisoned mid-panic) or hard
    /// [`crate::system::ActorSystem::shutdown`].
    ///
    /// Async and `&mut self` — flush buffers, close connections, send
    /// farewell messages via `ctx` if needed. Keep it bounded: the
    /// shutdown sweep joins it under the sweep deadline.
    fn on_stop(&mut self, ctx: &mut crate::context::MsgCtx<'_>) -> impl Future<Output = ()> + Send {
        let _ = ctx;
        async {}
    }
}

/// Typed sugar for service actors, mirroring [`CommandHandler`].
pub trait MsgHandler<M>: ServiceActor {
    /// Handles one typed message.
    fn handle(
        &mut self,
        msg: M,
        ctx: &mut crate::context::MsgCtx<'_>,
    ) -> impl Future<Output = ()> + Send;
}

/// Errors surfaced while dispatching a command.
#[derive(Debug, wherror::Error)]
#[error(debug)]
pub enum DispatchError {
    /// The payload failed to decode into the handler's command type.
    Decode(String),
    /// The state shell was not the type this adapter registered (kernel bug).
    StateMismatch,
}

/// The object-safe ES shell the kernel actually drives.
///
/// Erases `A: EventSourced` behind four verbs — downcast (for the adapter
/// that registered this exact type), mutate ([`DynEsActor::apply_erased`]),
/// persist ([`DynEsActor::capture_erased`]), and rebuild. A poisoned
/// instance (post-panic) is never reused: [`DynEsActor::rebuild`]
/// constructs a fresh one from snapshot-or-genesis plus the replay tail.
pub trait DynEsActor: Send {
    /// The live state as `Any` — the downcast seam for adapters.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;

    /// The live state as shared `Any` — the seam for typed zero-copy reads.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Applies one event to live state (the ONLY mutation path).
    fn apply_erased(&mut self, event: &crate::envelope::Event);

    /// Serializes live state for a journal snapshot.
    ///
    /// # Errors
    ///
    /// Propagates [`EventSourced::capture`] failures.
    fn capture_erased(&self) -> Result<Json, error_stack::Report<JournalError>>;

    /// Rebuilds state: genesis, or snapshot + replay tail. Runs on spawn
    /// AND on restart after a panic — the poisoned instance is dropped,
    /// never mutated.
    ///
    /// # Errors
    ///
    /// Propagates [`EventSourced::restore_from`] failures.
    fn rebuild(
        &self,
        args: &Json,
        snapshot: Option<Json>,
        tail: &[crate::envelope::Event],
    ) -> Result<Box<dyn DynEsActor>, error_stack::Report<JournalError>>;

    /// The erased ES graceful-stop hook: forwards to
    /// [`EventSourcedActor::on_stop`] over the final folded state.
    fn on_stop_es(&self);

    /// The live state type's name — diagnostics for typed-read misses.
    fn state_type_name(&self) -> &'static str;
}

/// Downcast seam for a SHARED read of ES state (generic: the reader knows
/// the state type), mirroring [`ServiceAny`].
///
/// Live ES state is always behind
/// `Arc<tokio::Mutex<Box<dyn DynEsActor>>>`, so a shared borrow cannot
/// escape the guard; this trait is how a lock-holding closure sees the
/// typed state without serializing. A [`ForeignEsState`] (or a wrong typed
/// shell) misses → `None`.
pub trait EsAny {
    /// Runs `f` over the live state as `&A`, when this shell holds an
    /// [`TypedEsState`]`<A>`.
    fn with_es_state<A: EventSourcedActor, R>(&self, f: impl FnOnce(&A) -> R) -> Option<R>;

    /// Runs `f` over the live read model as `&P`, when this shell holds a
    /// [`TypedProjectorState`]`<P>`.
    fn with_projector_state<P: Projector, R>(&self, f: impl FnOnce(&P) -> R) -> Option<R>;
}

impl EsAny for dyn DynEsActor {
    fn with_es_state<A: EventSourcedActor, R>(&self, f: impl FnOnce(&A) -> R) -> Option<R> {
        let typed = self.as_any().downcast_ref::<TypedEsState<A>>()?;
        Some(f(&typed.state))
    }

    fn with_projector_state<P: Projector, R>(&self, f: impl FnOnce(&P) -> R) -> Option<R> {
        let typed = self.as_any().downcast_ref::<TypedProjectorState<P>>()?;
        Some(f(&typed.state))
    }
}

/// Concrete `DynEsActor` for a typed state `A`.
pub struct TypedEsState<A: EventSourcedActor> {
    state: A,
}

impl<A: EventSourcedActor> TypedEsState<A> {
    /// Wraps live state.
    pub fn new(state: A) -> Self {
        Self { state }
    }
}

impl<A: EventSourcedActor> DynEsActor for TypedEsState<A> {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn state_type_name(&self) -> &'static str {
        std::any::type_name::<A>()
    }

    fn apply_erased(&mut self, event: &crate::envelope::Event) {
        self.state.apply(event);
    }

    fn capture_erased(&self) -> Result<Json, error_stack::Report<JournalError>> {
        self.state.capture()
    }

    fn rebuild(
        &self,
        args: &Json,
        snapshot: Option<Json>,
        tail: &[crate::envelope::Event],
    ) -> Result<Box<dyn DynEsActor>, error_stack::Report<JournalError>> {
        let mut fresh: A = match snapshot {
            Some(snap) => A::restore_from(snap)?,
            None => A::restore(args),
        };
        for event in tail {
            fresh.apply(event);
        }
        Ok(Box::new(TypedEsState { state: fresh }))
    }

    fn on_stop_es(&self) {
        self.state.on_stop();
    }
}

/// The object-safe command dispatch the registry routes by [`SchemaId`].
///
/// One entry per (actor path, command schema); the kernel asks the entry to
/// decode the JSON, run the typed handler, and apply the resulting events.
/// No `A` or `C` survives the waist — the adapter holds the types, the
/// kernel holds the state shell.
pub trait CommandEntry: Send + Sync {
    /// The command schema this entry decodes.
    fn schema(&self) -> SchemaId;

    /// Decodes `payload`, runs the typed handler, and applies the returned
    /// events to `state` — decision and fold in one kernel-driven step.
    ///
    /// # Errors
    ///
    /// [`DispatchError::Decode`] when the JSON does not match the command
    /// type (the message is dead-lettered, not panicked on);
    /// [`DispatchError::StateMismatch`] when the state is not this
    /// adapter's actor type (a kernel bug).
    fn dispatch(
        &self,
        state: &mut dyn DynEsActor,
        payload: &Json,
        ctx: &mut CmdCtx<'_>,
    ) -> Result<crate::envelope::Events, error_stack::Report<DispatchError>>;
}

/// Generic adapter: erases `A`'s handler for command type `C`.
pub struct TypedEsAdapter<A, C> {
    schema: SchemaId,
    _actor: std::marker::PhantomData<fn(&A)>,
    _cmd: std::marker::PhantomData<fn(&C)>,
}

impl<A: EventSourcedActor, C> TypedEsAdapter<A, C> {
    /// Creates the adapter for command schema `S`.
    pub fn new<S: Schema>() -> Self {
        Self {
            schema: S::schema_id(),
            _actor: std::marker::PhantomData,
            _cmd: std::marker::PhantomData,
        }
    }
}

impl<A, C> CommandEntry for TypedEsAdapter<A, C>
where
    A: EventSourcedActor + CommandHandler<C>,
    C: DeserializeOwned + Send + 'static,
{
    fn schema(&self) -> SchemaId {
        self.schema.clone()
    }

    fn dispatch(
        &self,
        state: &mut dyn DynEsActor,
        payload: &Json,
        ctx: &mut CmdCtx<'_>,
    ) -> Result<crate::envelope::Events, error_stack::Report<DispatchError>> {
        use error_stack::ResultExt;
        let cmd: C = payload
            .decode::<C>()
            .change_context(DispatchError::Decode(format!(
                "command {} did not match its schema",
                self.schema
            )))?;

        // Safe: the spawn that registered this adapter built the state as
        // the same `A` — a mismatch is a kernel bug, hence a panic.
        let typed = state
            .as_any_mut()
            .downcast_mut::<TypedEsState<A>>()
            .expect("adapter/state type mismatch — kernel bug");

        // DECIDE ONLY: `handle` takes `&self`, so this cannot poison the
        // state even if it panics. The LOOP applies the returned events
        // after journal-append + ack (spec atomic ordering, step 7).
        Ok(typed.state.handle(cmd, ctx))
    }
}

/// Concrete [`DynEsActor`] for a typed read model `P`: the kernel drives it
/// exactly like an entity, but there is no decision function — every
/// delivered fact IS the event (the builder's `ConsumeEntry` records it
/// verbatim) and [`Projector::apply`] is the only mutation.
pub struct TypedProjectorState<P: Projector> {
    state: P,
}

impl<P: Projector> TypedProjectorState<P> {
    /// Wraps live read-model state.
    pub fn new(state: P) -> Self {
        Self { state }
    }
}

impl<P: Projector> DynEsActor for TypedProjectorState<P> {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn state_type_name(&self) -> &'static str {
        std::any::type_name::<P>()
    }

    fn apply_erased(&mut self, event: &crate::envelope::Event) {
        self.state.apply(event);
    }

    fn capture_erased(&self) -> Result<Json, error_stack::Report<JournalError>> {
        use error_stack::ResultExt;
        Ok::<Json, error_stack::Report<JournalError>>(Json::of(&self.state))
            .change_context(JournalError::Snapshot)
    }

    fn rebuild(
        &self,
        _args: &Json,
        snapshot: Option<Json>,
        tail: &[crate::envelope::Event],
    ) -> Result<Box<dyn DynEsActor>, error_stack::Report<JournalError>> {
        use error_stack::ResultExt;
        // A projector's genesis is Default (not spawn args): the read model
        // is derived entirely from the facts it folds. Snapshot or genesis,
        // then the fold runs over the tail exactly as the live path does.
        let mut fresh: P = match snapshot {
            Some(snap) => snap.decode::<P>().change_context(JournalError::Restore)?,
            None => P::default(),
        };
        for event in tail {
            fresh.apply(event);
        }
        Ok(Box::new(TypedProjectorState { state: fresh }))
    }

    fn on_stop_es(&self) {
        self.state.on_stop();
    }
}

/// The projector's dispatch table entry: the delivered fact IS the event.
///
/// No decode, no user code — the identity decision. The kernel journals
/// what `dispatch` returns and applies it to the fold, so a consumed fact
/// is re-recorded into the projector's own journal (its checkpoint) with
/// zero per-consumption domain code.
pub struct ConsumeEntry {
    schema: SchemaId,
}

impl ConsumeEntry {
    /// Creates the identity entry for a consumed fact schema.
    pub fn new(schema: SchemaId) -> Self {
        Self { schema }
    }
}

impl CommandEntry for ConsumeEntry {
    fn schema(&self) -> SchemaId {
        self.schema.clone()
    }

    fn dispatch(
        &self,
        _state: &mut dyn DynEsActor,
        payload: &Json,
        _ctx: &mut CmdCtx<'_>,
    ) -> Result<crate::envelope::Events, error_stack::Report<DispatchError>> {
        let mut events = crate::envelope::Events::new();
        events.push(crate::envelope::Event::new(
            self.schema.clone(),
            payload.clone(),
        ));
        Ok(events)
    }
}

/// A foreign actor's decision function: JSON state + JSON command → events.
pub type ForeignDecision =
    Arc<dyn Fn(&Json, &Json, &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> + Send + Sync>;

/// The erased twin for actors defined entirely outside Rust: state is JSON,
/// decisions are a [`ForeignDecision`] closure.
///
/// Implements [`DynEsActor`] so the kernel drives it identically; its
/// `capture` is a JSON clone because the state is already at the waist.
#[derive(Clone)]
pub struct ForeignEsState {
    state: Json,
    fold: ForeignFold,
}

impl ForeignEsState {
    /// Wraps foreign JSON state with its fold closure.
    pub fn new(state: Json, fold: ForeignFold) -> Self {
        Self { state, fold }
    }

    /// The current JSON state.
    pub fn state(&self) -> &Json {
        &self.state
    }
}

/// A foreign actor's fold: JSON state + event → mutated state.
pub type ForeignFold = Arc<dyn Fn(&mut Json, &crate::envelope::Event) + Send + Sync>;

impl DynEsActor for ForeignEsState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn state_type_name(&self) -> &'static str {
        // The foreign state IS JSON — name it as such in diagnostics.
        std::any::type_name::<Json>()
    }

    fn apply_erased(&mut self, event: &crate::envelope::Event) {
        (self.fold)(&mut self.state, event);
    }

    fn capture_erased(&self) -> Result<Json, error_stack::Report<JournalError>> {
        Ok(self.state.clone())
    }

    fn rebuild(
        &self,
        args: &Json,
        snapshot: Option<Json>,
        tail: &[crate::envelope::Event],
    ) -> Result<Box<dyn DynEsActor>, error_stack::Report<JournalError>> {
        // Foreign rebuild: snapshot or a genesis shell, then the fold runs
        // over the tail exactly as the live path does (one code path).
        let mut fresh = match snapshot {
            Some(snap) => ForeignEsState::new(snap, self.fold.clone()),
            None => ForeignEsState::new(crate::json!({ "args": args }), self.fold.clone()),
        };
        for event in tail {
            fresh.apply_erased(event);
        }
        Ok(Box::new(fresh))
    }

    fn on_stop_es(&self) {
        // The foreign tier has no typed on_stop surface (a JSON state has
        // no methods); the fold closure family is pure by construction.
    }
}

/// The foreign command entry: decodes nothing (JSON passes through) and
/// runs the decision closure. The fold lives on the foreign state itself.
pub struct ForeignCommandEntry {
    schema: SchemaId,
    decision: ForeignDecision,
}

impl ForeignCommandEntry {
    /// Creates a foreign entry from its decision closure.
    pub fn new(schema: SchemaId, decision: ForeignDecision) -> Self {
        Self { schema, decision }
    }
}

impl CommandEntry for ForeignCommandEntry {
    fn schema(&self) -> SchemaId {
        self.schema.clone()
    }

    fn dispatch(
        &self,
        state: &mut dyn DynEsActor,
        payload: &Json,
        ctx: &mut CmdCtx<'_>,
    ) -> Result<crate::envelope::Events, error_stack::Report<DispatchError>> {
        // DECIDE ONLY (like the typed adapter): the foreign state folds the
        // returned events itself, post-ack, via its own fold closure. The
        // decision closure is the one `Vec`-returning seam left in the
        // kernel — wrapped once, here, into the compact buffer.
        let foreign = state
            .as_any_mut()
            .downcast_mut::<ForeignEsState>()
            .expect("foreign entry on non-foreign state — kernel bug");
        Ok(crate::envelope::Events::from_vec((self.decision)(
            foreign.state(),
            payload,
            ctx,
        )))
    }
}

/// The object-safe service shell the kernel drives: one erased instance.
/// NOT journaled — restart constructs a fresh instance via
/// [`ServiceActor::start`]. Dispatch runs through a [`MsgEntry`] adapter,
/// which downcasts the shell and the decoded message by type.
pub trait DynServiceActor: std::any::Any + Send {
    /// The erased graceful-stop hook: forwards to [`ServiceActor::on_stop`].
    fn on_stop_erased<'a>(
        &'a mut self,
        ctx: &'a mut crate::context::MsgCtx<'_>,
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// Concrete `DynServiceActor` for a typed service `A`.
pub struct TypedServiceState<A: ServiceActor> {
    /// The live service instance.
    pub state: A,
}

impl<A: ServiceActor> TypedServiceState<A> {
    /// Wraps a started service instance.
    pub fn new(state: A) -> Self {
        Self { state }
    }
}

impl<A: ServiceActor> DynServiceActor for TypedServiceState<A> {
    fn on_stop_erased<'a>(
        &'a mut self,
        ctx: &'a mut crate::context::MsgCtx<'_>,
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(self.state.on_stop(ctx))
    }
}

/// The object-safe async message dispatch routed by [`SchemaId`].
pub trait MsgEntry: Send + Sync {
    /// The message schema this entry decodes.
    fn schema(&self) -> SchemaId;

    /// Decodes the JSON payload into a boxed `Any` of the handler's type
    /// (the DECODE side runs sync so decode failures dead-letter cleanly).
    ///
    /// # Errors
    ///
    /// [`DispatchError::Decode`] when the payload does not match.
    fn decode(
        &self,
        payload: &Json,
    ) -> Result<Box<dyn std::any::Any + Send>, error_stack::Report<DispatchError>>;

    /// Runs the typed handler against the boxed message (consumes it).
    fn dispatch<'a>(
        &'a self,
        state: &'a mut dyn DynServiceActor,
        msg: Box<dyn std::any::Any + Send>,
        ctx: &'a mut crate::context::MsgCtx<'_>,
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// Generic adapter: erases `A`'s handler for message type `M`.
pub struct TypedServiceAdapter<A, M> {
    schema: SchemaId,
    _actor: std::marker::PhantomData<fn(&A)>,
    _msg: std::marker::PhantomData<fn(&M)>,
}

impl<A: ServiceActor, M> TypedServiceAdapter<A, M> {
    /// Creates the adapter for message schema `S`.
    pub fn new<S: Schema>() -> Self {
        Self {
            schema: S::schema_id(),
            _actor: std::marker::PhantomData,
            _msg: std::marker::PhantomData,
        }
    }
}

impl<A, M> MsgEntry for TypedServiceAdapter<A, M>
where
    A: ServiceActor + MsgHandler<M>,
    M: DeserializeOwned + Send + 'static,
{
    fn schema(&self) -> SchemaId {
        self.schema.clone()
    }

    fn decode(
        &self,
        payload: &Json,
    ) -> Result<Box<dyn std::any::Any + Send>, error_stack::Report<DispatchError>> {
        use error_stack::ResultExt;
        let msg: M = payload
            .decode::<M>()
            .change_context(DispatchError::Decode(format!(
                "message {} did not match its schema",
                self.schema
            )))?;
        Ok(Box::new(msg))
    }

    fn dispatch<'a>(
        &'a self,
        state: &'a mut dyn DynServiceActor,
        msg: Box<dyn std::any::Any + Send>,
        ctx: &'a mut crate::context::MsgCtx<'_>,
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let typed = state
                .as_any_service_mut::<A>()
                .expect("service adapter/type mismatch — kernel bug");
            let msg = match msg.downcast::<M>() {
                Ok(msg) => *msg,
                Err(_) => panic!("service message type mismatch — kernel bug"),
            };
            typed.state.handle(msg, ctx).await;
        })
    }
}

/// Downcast seam for the service shell (generic: the adapter knows `A`).
pub trait ServiceAny {
    /// The live instance as `&mut TypedServiceState<A>`, when it is one.
    fn as_any_service_mut<A: ServiceActor>(&mut self) -> Option<&mut TypedServiceState<A>>;
}

impl ServiceAny for dyn DynServiceActor {
    fn as_any_service_mut<A: ServiceActor>(&mut self) -> Option<&mut TypedServiceState<A>> {
        (self as &mut dyn std::any::Any).downcast_mut::<TypedServiceState<A>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{ActorKind, ActorPath};
    use crate::json;
    use crate::schema::{FieldDef, FieldTy, SchemaDef, SchemaKind};
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct ReserveStock {
        qty: u32,
    }

    impl Schema for ReserveStock {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "ReserveStock".into(),
                version: 1,
                kind: SchemaKind::Command,
                fields: vec![FieldDef::required("qty", FieldTy::Int)],
                description: None,
            }
        }
    }

    struct StockReserved;

    impl Schema for StockReserved {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "StockReserved".into(),
                version: 1,
                kind: SchemaKind::Event,
                fields: vec![FieldDef::required("qty", FieldTy::Int)],
                description: None,
            }
        }
    }

    /// A counter as the canonical ES actor under test.
    #[derive(Serialize, Deserialize, Default)]
    struct Counter {
        count: i64,
    }

    impl EventSourcedActor for Counter {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<ReserveStock>()
                .emits::<StockReserved>()
                .kind(ActorKind::EventSourced)
        }

        fn restore(_args: &Json) -> Self {
            Self { count: 0 }
        }

        fn apply(&mut self, event: &crate::envelope::Event) {
            if event.schema.as_str() == "StockReserved@1" {
                self.count += event.payload["qty"].as_i64().unwrap_or(0);
            }
        }
    }

    impl CommandHandler<ReserveStock> for Counter {
        fn handle(&self, cmd: ReserveStock, _ctx: &mut CmdCtx<'_>) -> crate::envelope::Events {
            crate::envelope::Events::from_vec(vec![crate::envelope::Event::new(
                StockReserved::schema_id(),
                json!({ "qty": cmd.qty }),
            )])
        }
    }

    /// Drives the adapter without a registry: a no-op context.
    fn null_ctx() -> (Vec<crate::envelope::Event>, ()) {
        (Vec::new(), ())
    }

    #[test]
    fn apply_erased_folds_events_into_live_state() {
        // Given a typed ES state wrapper at genesis.
        let mut live = TypedEsState::new(Counter::restore(&json!({})));

        // When applying two events through the erased shell.
        live.apply_erased(&crate::envelope::Event::new(
            StockReserved::schema_id(),
            json!({ "qty": 2 }),
        ));
        live.apply_erased(&crate::envelope::Event::new(
            StockReserved::schema_id(),
            json!({ "qty": 5 }),
        ));

        // Then the fold matches a hand-computed total.
        let captured = live.capture_erased().expect("capture");
        assert_eq!(captured["count"], 7);
    }

    #[test]
    fn rebuild_from_genesis_applies_the_replay_tail() {
        // Given a live state (poisoned, say) and a replay tail of two events.
        let live = TypedEsState::new(Counter { count: 999 });
        let tail = vec![
            crate::envelope::Event::new(StockReserved::schema_id(), json!({ "qty": 2 })),
            crate::envelope::Event::new(StockReserved::schema_id(), json!({ "qty": 3 })),
        ];

        // When rebuilding with no snapshot.
        let fresh = live.rebuild(&json!({}), None, &tail).expect("rebuild");

        // Then genesis + tail is the rebuilt state (0 + 5), not the old 999.
        let captured = fresh.capture_erased().expect("capture");
        assert_eq!(captured["count"], 5);
    }

    #[test]
    fn rebuild_from_snapshot_applies_only_the_tail() {
        // Given a snapshot claiming count 10 and a tail of two events.
        let live = TypedEsState::new(Counter { count: 0 });
        let tail = vec![
            crate::envelope::Event::new(StockReserved::schema_id(), json!({ "qty": 1 })),
            crate::envelope::Event::new(StockReserved::schema_id(), json!({ "qty": 1 })),
        ];

        // When rebuilding from the snapshot.
        let fresh = live
            .rebuild(&json!({}), Some(json!({ "count": 10 })), &tail)
            .expect("rebuild");

        // Then the snapshot is the base and the tail folds on top.
        let captured = fresh.capture_erased().expect("capture");
        assert_eq!(captured["count"], 12);
    }

    #[test]
    fn capture_roundtrips_through_restore_from() {
        // Given a state with count 42.
        let live = TypedEsState::new(Counter { count: 42 });

        // When capturing and restoring through the seam.
        let blob = live.capture_erased().expect("capture");
        let restored = Counter::restore_from(blob).expect("restore");

        // Then the state survives exactly.
        assert_eq!(restored.count, 42);
    }

    #[test]
    fn typed_adapter_decodes_dispatches_and_folds_in_one_step() {
        // Given an adapter, its state shell, and a JSON command.
        let adapter = TypedEsAdapter::<Counter, ReserveStock>::new::<ReserveStock>();
        let mut state = TypedEsState::new(Counter::restore(&json!({})));

        // The adapter needs a CmdCtx; build a minimal one over a null view.
        use crate::context::{CmdCtx, Outbox, RuntimeView};
        struct NullView;
        impl RuntimeView for NullView {
            fn lookup(&self, _path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
                None
            }
            fn handlers_of(&self, _schema: &SchemaId) -> Vec<ActorPath> {
                Vec::new()
            }
            fn now(&self) -> crate::clock::Timestamp {
                crate::clock::Timestamp::from_millis(0)
            }
        }
        let trace = crate::envelope::TraceCtx::root();
        let path = ActorPath::new("counter");
        let mut outbox = Outbox::new();
        let mut ctx = CmdCtx::new(&path, &trace, None, &NullView, &mut outbox);

        // When dispatching a well-formed command.
        let events = adapter
            .dispatch(&mut state, &json!({ "qty": 4 }), &mut ctx)
            .expect("dispatch");

        // Then one event came back — DECIDED but not yet applied (the loop
        // applies post-ack, per the atomic ordering).
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload["qty"], 4);
        let captured = state.capture_erased().expect("capture");
        assert_eq!(captured["count"], 0, "dispatch must not mutate state");

        // When the kernel applies the events (post-ack step).
        for event in events.iter() {
            state.apply_erased(event);
        }

        // Then the fold lands exactly once.
        let captured = state.capture_erased().expect("capture");
        assert_eq!(captured["count"], 4);
    }

    #[test]
    fn typed_adapter_rejects_malformed_payloads_as_decode_errors() {
        // Given an adapter and a payload missing the required field.
        let adapter = TypedEsAdapter::<Counter, ReserveStock>::new::<ReserveStock>();
        let mut state = TypedEsState::new(Counter::restore(&json!({})));

        use crate::context::{CmdCtx, Outbox, RuntimeView};
        struct NullView;
        impl RuntimeView for NullView {
            fn lookup(&self, _path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
                None
            }
            fn handlers_of(&self, _schema: &SchemaId) -> Vec<ActorPath> {
                Vec::new()
            }
            fn now(&self) -> crate::clock::Timestamp {
                crate::clock::Timestamp::from_millis(0)
            }
        }
        let trace = crate::envelope::TraceCtx::root();
        let path = ActorPath::new("counter");
        let mut outbox = Outbox::new();
        let mut ctx = CmdCtx::new(&path, &trace, None, &NullView, &mut outbox);

        // When dispatching a malformed payload.
        let result = adapter.dispatch(&mut state, &json!({ "nope": true }), &mut ctx);

        // Then it is a Decode error and the state is untouched.
        let report = result.expect_err("must not decode");
        assert!(matches!(report.current_context(), DispatchError::Decode(_)));
        assert_eq!(state.capture_erased().expect("capture")["count"], 0);
    }

    #[test]
    fn foreign_entry_decides_and_folds_in_json() {
        // Given a foreign state and entry (decision: echo qty; fold: add it).
        let schema = ReserveStock::schema_id();
        let decision: ForeignDecision = Arc::new(|_state, cmd, _ctx| {
            vec![crate::envelope::Event::new(
                StockReserved::schema_id(),
                json!({ "qty": cmd["qty"] }),
            )]
        });
        let fold: ForeignFold = Arc::new(|state, event| {
            let qty = event.payload["qty"].as_i64().unwrap_or(0);
            state["count"] = serde_json::json!(state["count"].as_i64().unwrap_or(0) + qty);
        });
        let entry = ForeignCommandEntry::new(schema, decision);
        let mut state = ForeignEsState::new(json!({ "count": 0 }), fold);

        use crate::context::{CmdCtx, Outbox, RuntimeView};
        struct NullView;
        impl RuntimeView for NullView {
            fn lookup(&self, _path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
                None
            }
            fn handlers_of(&self, _schema: &SchemaId) -> Vec<ActorPath> {
                Vec::new()
            }
            fn now(&self) -> crate::clock::Timestamp {
                crate::clock::Timestamp::from_millis(0)
            }
        }
        let trace = crate::envelope::TraceCtx::root();
        let path = ActorPath::new("foreign");
        let mut outbox = Outbox::new();
        let mut ctx = CmdCtx::new(&path, &trace, None, &NullView, &mut outbox);

        // When dispatching a JSON command (no Rust type involved).
        let events = entry
            .dispatch(&mut state, &json!({ "qty": 6 }), &mut ctx)
            .expect("dispatch");

        // Then the event came back decided, not applied.
        assert_eq!(events[0].payload["qty"], 6);
        assert_eq!(state.state()["count"], 0, "dispatch must not fold");

        // When the kernel applies (post-ack).
        for event in events.iter() {
            state.apply_erased(event);
        }

        // Then the JSON state folded it exactly once.
        assert_eq!(state.state()["count"], 6);
    }

    #[test]
    fn foreign_state_captures_as_a_clone_of_json() {
        // Given a foreign state holding JSON with a no-op fold.
        let noop: ForeignFold = Arc::new(|_state, _event| {});
        let state = ForeignEsState::new(json!({ "count": 9 }), noop);

        // When capturing.
        let captured = state.capture_erased().expect("capture");

        // Then the capture is the JSON itself (already at the waist).
        assert_eq!(captured["count"], 9);
    }

    #[test]
    fn typed_adapter_reports_its_schema() {
        // Given a typed adapter for ReserveStock.
        let adapter = TypedEsAdapter::<Counter, ReserveStock>::new::<ReserveStock>();

        // When asking for its schema.
        let schema = adapter.schema();

        // Then it matches the command's schema id.
        assert_eq!(schema, ReserveStock::schema_id());
    }

    #[test]
    fn null_ctx_helper_compiles() {
        // Given the local helper (guards unused-import churn across edits).
        let (events, _guard) = null_ctx();

        // Then it is an empty decision.
        assert!(events.is_empty());
    }

    #[test]
    fn es_any_reads_typed_es_state_without_capture() {
        // Given a typed ES state holding a folded counter.
        let live = TypedEsState::new(Counter { count: 41 });
        let erased: &dyn DynEsActor = &live;

        // When reading through the shared downcast seam.
        let seen = erased.with_es_state(|c: &Counter| c.count);

        // Then the closure saw the exact live value.
        assert_eq!(seen, Some(41));
    }

    #[test]
    fn es_any_reads_typed_projector_state() {
        // Given a typed projector shell wrapping a read model at 7.
        #[derive(Serialize, Deserialize, Default)]
        struct View {
            total: i64,
        }
        impl Projector for View {
            fn apply(&mut self, _event: &crate::envelope::Event) {}
        }
        let live = TypedProjectorState::new(View { total: 7 });
        let erased: &dyn DynEsActor = &live;

        // When reading through the shared downcast seam.
        let seen = erased.with_projector_state(|v: &View| v.total);

        // Then the closure saw the exact live value.
        assert_eq!(seen, Some(7));
    }

    #[test]
    fn es_any_misses_on_wrong_shell_and_foreign_state() {
        // Given an erased TypedEsState<Counter>.
        let live = TypedEsState::new(Counter { count: 1 });
        let erased: &dyn DynEsActor = &live;

        // When reading it as a different actor type.
        #[derive(Serialize, Deserialize, Default)]
        struct Other {
            x: i64,
        }
        impl EventSourcedActor for Other {
            fn manifest() -> ActorManifest {
                ActorManifest::new().kind(ActorKind::EventSourced)
            }
            fn restore(_args: &Json) -> Self {
                Self { x: 0 }
            }
            fn apply(&mut self, _event: &crate::envelope::Event) {}
        }
        let wrong = erased.with_es_state(|_o: &Other| ());

        // Then it is a miss, not a panic.
        assert_eq!(wrong, None);

        // And a foreign (JSON) state misses the typed seam as well.
        let noop: ForeignFold = Arc::new(|_state, _event| {});
        let foreign: &dyn DynEsActor = &ForeignEsState::new(json!({ "count": 9 }), noop);
        let foreign_read = foreign.with_es_state(|_c: &Counter| ());
        assert_eq!(foreign_read, None);
    }

    #[test]
    fn state_type_name_reports_the_state_type() {
        // Given shells for a typed entity, a projector, and a foreign actor.
        let typed = TypedEsState::new(Counter { count: 0 });
        let foreign = ForeignEsState::new(json!({}), Arc::new(|_s: &mut Json, _e: &crate::envelope::Event| {}));

        // When asking each for its state type name.
        let typed_name = typed.state_type_name();
        let foreign_name = foreign.state_type_name();

        // Then the names identify the state types, not the shells.
        assert_eq!(typed_name, std::any::type_name::<Counter>());
        assert_eq!(foreign_name, std::any::type_name::<Json>());
    }
}

/// Actor identity IS its path: handles survive restarts because the registry
/// maps the path to a swappable endpoint slot.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActorPath(Arc<str>);

/// Which of the two actor contracts an actor implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ActorKind {
    /// Pure, journaled, replayable — implements [`crate::actor::EventSourced`].
    EventSourced,
    /// Impure by design: async handlers, I/O and `ask` allowed.
    Service,
}

/// Why an actor's endpoint ceased to exist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StopReason {
    /// Finished on its own or was stopped gracefully via the system.
    Normal,
    /// A handler panicked and the supervisor declined to restart it.
    Crashed,
    /// The restart budget was exhausted; escalated to the parent.
    Escalated,
    /// Idled past its declared passivation window; the runtime stopped
    /// it (a partition set re-spawns the entity on the next send).
    Passivated,
    /// Torn down by the graceful shutdown sweep.
    Shutdown,
}

/// How often an event-sourced actor takes journal snapshots. Default: OFF.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum SnapshotCadence {
    /// Never snapshot (replay is always full).
    #[default]
    Off,
    /// Snapshot every `n` events (taken BETWEEN messages, never mid-step) —
    /// bounds recovery cost deterministically.
    Messages(u64),
    /// Snapshot when at least this much clock time passed since the last
    /// snapshot. Checked while the actor idles (never mid-step), so a
    /// steady-trickle actor that never reaches a message count still gets
    /// bounded recovery cost.
    Time(std::time::Duration),
}

impl ActorPath {
    /// Creates a path from a string.
    pub fn new(s: impl Into<Arc<str>>) -> Self {
        Self(s.into())
    }

    /// The path as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ActorPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[test]
fn path_survives_serde_roundtrip() {
    // Given an actor path.
    let path = ActorPath::new("inventory.west");

    // When round-tripping through JSON.
    let json = serde_json::to_string(&path).expect("serialize");
    let round: ActorPath = serde_json::from_str(&json).expect("deserialize");

    // Then the value is preserved as a bare string.
    assert_eq!(json, "\"inventory.west\"");
    assert_eq!(round, path);
}
#[test]
fn path_displays_as_bare_name() {
    // Given an actor path.
    let path = ActorPath::new("inventory.west");

    // When displaying it.
    let rendered = path.to_string();

    // Then only the name is shown.
    assert_eq!(rendered, "inventory.west");
}
#[test]
fn stop_reason_survives_serde_roundtrip() {
    // Given a stop reason.
    let reason = StopReason::Escalated;

    // When round-tripping through JSON.
    let json = serde_json::to_string(&reason).expect("serialize");
    let round: StopReason = serde_json::from_str(&json).expect("deserialize");

    // Then the variant is preserved.
    assert_eq!(round, reason);
}
