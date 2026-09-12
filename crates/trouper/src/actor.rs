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

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

use crate::context::CmdCtx;
use crate::journal::JournalError;
use crate::schema::{ActorManifest, Schema};
use crate::types::SchemaId;

/// Event-sourced domain actor: pure, journaled, replayable.
///
/// Implementors get journal-backed restart for free; snapshots are opt-in
/// via the spawn policy and go through [`EventSourced::capture`] /
/// [`EventSourced::restore_from`].
pub trait EventSourcedActor: Send + Sync + Serialize + DeserializeOwned + 'static {
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
    /// `args` are spawn arguments (JSON); use them to seed initial state.
    fn restore(args: &JsonValue) -> Self;

    /// THE mutation. Used for live application AND replay — one code path,
    /// so live state and replayed state can never diverge.
    fn apply(&mut self, event: &crate::envelope::Event);

    /// Snapshot seam: serialize state. Default = state IS the snapshot.
    ///
    /// # Errors
    ///
    /// Fails when the state cannot be serialized to JSON.
    fn capture(&self) -> Result<JsonValue, error_stack::Report<JournalError>> {
        use error_stack::ResultExt;
        serde_json::to_value(self).change_context(JournalError::Snapshot)
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
    fn restore_from(snap: JsonValue) -> Result<Self, error_stack::Report<JournalError>> {
        use error_stack::ResultExt;
        serde_json::from_value(snap).change_context(JournalError::Restore)
    }
}

/// Typed sugar over the erased dispatch table: a pure decision function for
/// one command type. Registered once per (actor, command) pair at spawn.
pub trait CommandHandler<C>: EventSourcedActor {
    /// Decides: given current state and the command, which events happen?
    ///
    /// Sync, `&self` — no I/O, no await, no mutation. Effects are *declared*
    /// through `ctx` (deferred to post-ack by the kernel).
    fn handle(&self, cmd: C, ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event>;
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
        args: &JsonValue,
    ) -> impl Future<Output = Result<Self, error_stack::Report<crate::registry::RegistryError>>> + Send
    where
        Self: Sized;
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

    /// Applies one event to live state (the ONLY mutation path).
    fn apply_erased(&mut self, event: &crate::envelope::Event);

    /// Serializes live state for a journal snapshot.
    ///
    /// # Errors
    ///
    /// Propagates [`EventSourced::capture`] failures.
    fn capture_erased(&self) -> Result<JsonValue, error_stack::Report<JournalError>>;

    /// Rebuilds state: genesis, or snapshot + replay tail. Runs on spawn
    /// AND on restart after a panic — the poisoned instance is dropped,
    /// never mutated.
    ///
    /// # Errors
    ///
    /// Propagates [`EventSourced::restore_from`] failures.
    fn rebuild(
        &self,
        args: &JsonValue,
        snapshot: Option<JsonValue>,
        tail: &[crate::envelope::Event],
    ) -> Result<Box<dyn DynEsActor>, error_stack::Report<JournalError>>;
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

    fn apply_erased(&mut self, event: &crate::envelope::Event) {
        self.state.apply(event);
    }

    fn capture_erased(&self) -> Result<JsonValue, error_stack::Report<JournalError>> {
        self.state.capture()
    }

    fn rebuild(
        &self,
        args: &JsonValue,
        snapshot: Option<JsonValue>,
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
        payload: &JsonValue,
        ctx: &mut CmdCtx<'_>,
    ) -> Result<Vec<crate::envelope::Event>, error_stack::Report<DispatchError>>;
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
        payload: &JsonValue,
        ctx: &mut CmdCtx<'_>,
    ) -> Result<Vec<crate::envelope::Event>, error_stack::Report<DispatchError>> {
        use error_stack::ResultExt;
        let cmd: C = serde_json::from_value(payload.clone()).change_context(
            DispatchError::Decode(format!("command {} did not match its schema", self.schema)),
        )?;

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

/// A foreign actor's decision function: JSON state + JSON command → events.
pub type ForeignDecision = Arc<
    dyn Fn(&JsonValue, &JsonValue, &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> + Send + Sync,
>;

/// The erased twin for actors defined entirely outside Rust: state is JSON,
/// decisions are a [`ForeignDecision`] closure.
///
/// Implements [`DynEsActor`] so the kernel drives it identically; its
/// `capture` is a JSON clone because the state is already at the waist.
#[derive(Clone)]
pub struct ForeignEsState {
    state: JsonValue,
    fold: ForeignFold,
}

impl ForeignEsState {
    /// Wraps foreign JSON state with its fold closure.
    pub fn new(state: JsonValue, fold: ForeignFold) -> Self {
        Self { state, fold }
    }

    /// The current JSON state.
    pub fn state(&self) -> &JsonValue {
        &self.state
    }
}

/// A foreign actor's fold: JSON state + event → mutated state.
pub type ForeignFold = Arc<dyn Fn(&mut JsonValue, &crate::envelope::Event) + Send + Sync>;

impl DynEsActor for ForeignEsState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn apply_erased(&mut self, event: &crate::envelope::Event) {
        (self.fold)(&mut self.state, event);
    }

    fn capture_erased(&self) -> Result<JsonValue, error_stack::Report<JournalError>> {
        Ok(self.state.clone())
    }

    fn rebuild(
        &self,
        args: &JsonValue,
        snapshot: Option<JsonValue>,
        tail: &[crate::envelope::Event],
    ) -> Result<Box<dyn DynEsActor>, error_stack::Report<JournalError>> {
        // Foreign rebuild: snapshot or a genesis shell, then the fold runs
        // over the tail exactly as the live path does (one code path).
        let mut fresh = match snapshot {
            Some(snap) => ForeignEsState::new(snap, self.fold.clone()),
            None => ForeignEsState::new(serde_json::json!({ "args": args }), self.fold.clone()),
        };
        for event in tail {
            fresh.apply_erased(event);
        }
        Ok(Box::new(fresh))
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
        payload: &JsonValue,
        ctx: &mut CmdCtx<'_>,
    ) -> Result<Vec<crate::envelope::Event>, error_stack::Report<DispatchError>> {
        // DECIDE ONLY (like the typed adapter): the foreign state folds the
        // returned events itself, post-ack, via its own fold closure.
        let foreign = state
            .as_any_mut()
            .downcast_mut::<ForeignEsState>()
            .expect("foreign entry on non-foreign state — kernel bug");
        Ok((self.decision)(foreign.state(), payload, ctx))
    }
}

/// The object-safe service shell the kernel drives: one erased instance.
/// NOT journaled — restart constructs a fresh instance via
/// [`ServiceActor::start`]. Dispatch runs through a [`MsgEntry`] adapter,
/// which downcasts the shell and the decoded message by type.
pub trait DynServiceActor: std::any::Any + Send {}

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

impl<A: ServiceActor> DynServiceActor for TypedServiceState<A> {}

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
        payload: &JsonValue,
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
        payload: &JsonValue,
    ) -> Result<Box<dyn std::any::Any + Send>, error_stack::Report<DispatchError>> {
        use error_stack::ResultExt;
        let msg: M = serde_json::from_value(payload.clone()).change_context(
            DispatchError::Decode(format!("message {} did not match its schema", self.schema)),
        )?;
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
    use crate::schema::{FieldDef, FieldTy, SchemaDef, SchemaKind};
    use crate::types::{ActorKind, ActorPath};
    use serde::Deserialize;
    use serde_json::json;

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

        fn restore(_args: &JsonValue) -> Self {
            Self { count: 0 }
        }

        fn apply(&mut self, event: &crate::envelope::Event) {
            if event.schema.as_str() == "StockReserved@1" {
                self.count += event.payload["qty"].as_i64().unwrap_or(0);
            }
        }
    }

    impl CommandHandler<ReserveStock> for Counter {
        fn handle(&self, cmd: ReserveStock, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
            vec![crate::envelope::Event::new(
                StockReserved::schema_id(),
                json!({ "qty": cmd.qty }),
            )]
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
            fn who_handles(&self, _schema: &SchemaId) -> Vec<ActorPath> {
                Vec::new()
            }
            fn now(&self) -> crate::types::Timestamp {
                crate::types::Timestamp::from_millis(0)
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
        for event in &events {
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
            fn who_handles(&self, _schema: &SchemaId) -> Vec<ActorPath> {
                Vec::new()
            }
            fn now(&self) -> crate::types::Timestamp {
                crate::types::Timestamp::from_millis(0)
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
            state["count"] = json!(state["count"].as_i64().unwrap_or(0) + qty);
        });
        let entry = ForeignCommandEntry::new(schema, decision);
        let mut state = ForeignEsState::new(json!({ "count": 0 }), fold);

        use crate::context::{CmdCtx, Outbox, RuntimeView};
        struct NullView;
        impl RuntimeView for NullView {
            fn lookup(&self, _path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
                None
            }
            fn who_handles(&self, _schema: &SchemaId) -> Vec<ActorPath> {
                Vec::new()
            }
            fn now(&self) -> crate::types::Timestamp {
                crate::types::Timestamp::from_millis(0)
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
        for event in &events {
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
}
