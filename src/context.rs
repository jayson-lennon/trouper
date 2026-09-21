//! Handler-facing context: the actors' entire syscall surface.
//!
//! Event-sourced handlers are pure — [`CmdCtx`] records *intents* into an
//! [`Outbox`] and performs nothing; the kernel flushes the outbox AFTER
//! journal-append + ack, so a crash before ack never duplicates a send.
//! Service handlers get [`MsgCtx`], a superset; its `ask` method arrives
//! with the reply-lease machinery (Phase 5).
//!
//! Contexts touch the registry only through [`RuntimeView`], a read-only
//! view — user code never holds the registry lock, so lookups from inside a
//! handler can never deadlock the kernel.

use crate::actor::ActorPath;
use crate::clock::Timestamp;
use crate::envelope::{Address, Envelope, TraceCtx};
use crate::json::Json;
use crate::kernel::AskOutcome;
use crate::schema::Message;
use crate::schema::SchemaId;

/// Read-only runtime view for handlers: registry lookups plus the clock.
///
/// Implemented by the system facade; the kernel hands contexts a reference.
pub(crate) trait RuntimeView: Send + Sync {
    /// Snapshot info about a path, if registered.
    fn lookup(&self, path: &ActorPath) -> Option<crate::registry::EndpointInfo>;

    /// Every path registered as a handler for a schema.
    fn handlers_of(&self, schema: &SchemaId) -> Vec<ActorPath>;

    /// The current time from the injected clock.
    fn now(&self) -> Timestamp;
}

/// One deferred effect, fully stamped; the kernel executes these post-ack.
#[derive(Debug)]
pub(crate) enum Intent {
    /// A point-to-point send to an address.
    Send(Envelope),
    /// An event broadcast: fanned out to every subscriber of the schema.
    Broadcast(Envelope),
    /// A reply to the message being handled (address = its reply_to).
    Reply {
        /// The reply address copied from the incoming envelope.
        to: Address,
        /// The reply schema (tracing) — may be the request's schema id.
        schema: SchemaId,
        /// The reply payload.
        payload: Json,
        /// The trace of the message being replied to (causality links).
        trace: TraceCtx,
    },
    /// Terminate the handling actor gracefully after this message
    /// commits (performed post-ack, so a crash before ack discards the
    /// stop exactly like any other intent — the actor restarts and
    /// continues). NEVER journaled: replay never synthesizes a stop.
    StopSelf,
}

impl Intent {
    /// The schema this intent would emit, if any (`StopSelf` emits
    /// nothing). The flush-time emit gate reads this: an intent whose
    /// schema the actor never declared in `.emits` is dropped, never
    /// routed.
    pub(crate) fn emitted_schema(&self) -> Option<&SchemaId> {
        match self {
            Intent::Send(envelope) | Intent::Broadcast(envelope) => Some(&envelope.schema),
            Intent::Reply { schema, .. } => Some(schema),
            Intent::StopSelf => None,
        }
    }
}

/// Effects recorded by a handler, flushed by the kernel after ack.
#[derive(Debug, Default)]
pub(crate) struct Outbox {
    intents: Vec<Intent>,
}

impl Outbox {
    /// An empty outbox.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a send intent.
    pub(crate) fn push_send(&mut self, envelope: Envelope) {
        self.intents.push(Intent::Send(envelope));
    }

    /// Records a broadcast intent (performed by the kernel post-ack).
    pub(crate) fn push_broadcast(&mut self, envelope: Envelope) {
        self.intents.push(Intent::Broadcast(envelope));
    }

    /// Records a reply intent (resolved by the kernel at flush time).
    pub(crate) fn push_reply(
        &mut self,
        to: Address,
        schema: SchemaId,
        payload: Json,
        trace: TraceCtx,
    ) {
        self.intents.push(Intent::Reply {
            to,
            schema,
            payload,
            trace,
        });
    }

    /// Records the self-termination intent. Executed post-ack by the
    /// kernel: intents recorded BEFORE it flush first (in-order), and a
    /// crash before ack discards it entirely.
    pub(crate) fn push_stop_self(&mut self) {
        self.intents.push(Intent::StopSelf);
    }

    /// Takes every intent, leaving the outbox empty (the flush).
    pub fn drain(&mut self) -> std::vec::Drain<'_, Intent> {
        self.intents.drain(..)
    }

    /// Whether any effects are pending.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.intents.is_empty()
    }

    /// The number of pending effects.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.intents.len()
    }
}

/// The boxed future a [`AskPort::ask_channel`] resolves to.
pub(crate) type AskChannelFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    (crate::reply::LeaseId, tokio::sync::oneshot::Receiver<Json>),
                    error_stack::Report<AskError>,
                >,
            > + Send,
    >,
>;

/// The fields every context shares.
///
/// Crate-private plumbing: the two tier contexts ([`CmdCtx`], [`MsgCtx`])
/// are the public surface, and their fields are opaque to handlers. The
/// outbox, trace, and view are never in user hands directly.
pub(crate) struct CtxCore<'a> {
    /// The processing actor's registered path.
    self_path: &'a ActorPath,
    /// Trace metadata of the message being processed.
    trace: &'a TraceCtx,
    /// Where a reply should go, if the sender asked for one.
    reply_to: Option<&'a Address>,
    /// Read-only runtime view (lookups + clock).
    view: &'a dyn RuntimeView,
    /// The outbox the kernel flushes after ack.
    outbox: &'a mut Outbox,
}

impl CtxCore<'_> {
    /// The trace metadata for a hop caused by this message: same
    /// `trace_id`, fresh `causality_id`.
    pub(crate) fn child_trace(&self) -> TraceCtx {
        self.trace.caused()
    }

    /// The incoming message's reply address, if the sender asked for one
    /// (the service tier's `reply` reads it).
    pub(crate) fn reply_to(&self) -> Option<&Address> {
        self.reply_to
    }

    /// Snapshot info about a path.
    pub fn lookup(&self, path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
        self.view.lookup(path)
    }

    /// Every path registered as a handler for a schema.
    pub fn handlers_of(&self, schema: &SchemaId) -> Vec<ActorPath> {
        self.view.handlers_of(schema)
    }

    /// The time the message was received (injected clock = deterministic).
    pub fn recv_ts(&self) -> Timestamp {
        self.view.now()
    }
}

/// Context for event-sourced handlers: PURE INTROSPECTION.
///
/// An entity announces only by RETURNING FACTS from its decision — the
/// kernel appends, applies, and broadcasts them. [`CmdCtx`] exposes no
/// effects: no send, no publish, no send_to_any, no reply, no stop_self
/// (an entity owns no lifecycle intents — only passivation, external
/// stop, or supervision ends one). It cannot `ask` either: a decision
/// function is sync and CANNOT await, so no ask can even compile.
///
/// The constructor is crate-private: only the kernel assembles contexts.
pub struct CmdCtx<'a> {
    core: CtxCore<'a>,
}

impl<'a> CmdCtx<'a> {
    /// Assembles the context for one command dispatch.
    pub(crate) fn new(
        self_path: &'a ActorPath,
        trace: &'a TraceCtx,
        #[allow(unused_variables)] reply_to: Option<&'a Address>,
        view: &'a dyn RuntimeView,
        outbox: &'a mut Outbox,
    ) -> CmdCtx<'a> {
        CmdCtx {
            core: CtxCore {
                self_path,
                trace,
                // Entities never reply: the field stays None by construction.
                reply_to: None,
                view,
                outbox,
            },
        }
    }

    /// Snapshot info about a path.
    pub fn lookup(&self, path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
        self.core.lookup(path)
    }

    /// Every path registered as a handler for a schema.
    pub fn handlers_of(&self, schema: &SchemaId) -> Vec<ActorPath> {
        self.core.handlers_of(schema)
    }

    /// The time the message was received (injected clock = deterministic).
    pub fn recv_ts(&self) -> Timestamp {
        self.core.recv_ts()
    }

    /// The processing actor's registered path.
    pub fn self_path(&self) -> &ActorPath {
        self.core.self_path
    }
}

/// The impure syscall port a service actor's [`MsgCtx`] carries: opens
/// reply leases, routes envelopes, reads the clock. The system implements
/// it; tests swap it (like [`RuntimeView`]).
pub(crate) trait AskPort: Send + Sync {
    /// Sends an envelope with a freshly opened reply lease; returns the
    /// lease id and receiver (the asker awaits the receiver under its
    /// timeout; the id lets the settle path drop the lease).
    fn ask_channel(
        &self,
        dest: Address,
        schema: SchemaId,
        payload: Json,
        ttl: std::time::Duration,
    ) -> AskChannelFuture;

    /// Records an ask-settled outcome and drops the lease (a settled or
    /// timed-out ask must not leak its slot).
    fn ask_settled(
        &self,
        lease: crate::reply::LeaseId,
        dest: Address,
        outcome: AskOutcome,
        trace: TraceCtx,
    );
}

/// Errors surfaced by `ctx.ask`.
#[derive(Debug, wherror::Error)]
#[error(debug)]
pub enum AskError {
    /// No handler for the message schema is registered at the
    /// destination (`SpawnBuilder::handles` was never called for it),
    /// or nothing is registered at the destination at all.
    Unresolved(String),
}

/// The ask machinery shared by [`MsgCtx::ask`] (in-actor asks) and
/// [`crate::system::ActorSystem::ask`] (system-level asks): open a reply
/// lease over the port, await the reply under the MANDATORY timeout, and
/// settle the lease with a Replied/Timeout/Failed fact so nothing leaks.
///
/// The timeout produces an [`AskOutcome::Timeout`] fact; a late reply lands
/// nowhere (the lease is dropped before the receiver is).
pub(crate) async fn ask_via_port(
    port: &dyn AskPort,
    dest: Address,
    schema: SchemaId,
    payload: Json,
    timeout: std::time::Duration,
    trace: TraceCtx,
) -> Result<Json, error_stack::Report<AskError>> {
    use error_stack::ResultExt;
    let dest_label = format!("{dest:?}");
    let (lease, mut receiver) = port
        .ask_channel(dest.clone(), schema, payload, timeout)
        .await
        .change_context(AskError::Unresolved(format!("{dest:?}")))?;
    let outcome = match tokio::time::timeout(timeout, &mut receiver).await {
        Ok(Ok(reply)) => Some((AskOutcome::Replied, reply)),
        Ok(Err(_)) => Some((AskOutcome::Failed, Json::default())),
        Err(_) => None,
    };
    match outcome {
        Some((AskOutcome::Replied, reply)) => {
            port.ask_settled(lease, dest, AskOutcome::Replied, trace);
            Ok(reply)
        }
        Some((outcome, _)) => {
            port.ask_settled(lease, dest, outcome, trace);
            Err(error_stack::Report::new(AskError::Unresolved(dest_label)))
        }
        None => {
            // Timed out: drop the lease so a late reply lands nowhere.
            port.ask_settled(lease, dest, AskOutcome::Timeout, trace);
            drop(receiver);
            Err(error_stack::Report::new(AskError::Unresolved(dest_label)))
        }
    }
}

/// Context for service-actor handlers: async, impure by design.
///
/// The sync surface matches [`CmdCtx`]; `ask` is exclusive to this tier —
/// an event-sourced decision function cannot await.
///
/// The constructor is crate-private: only the kernel assembles contexts.
pub struct MsgCtx<'a> {
    core: CtxCore<'a>,
    /// The impure port (leases + routing); absent only in pure tests that
    /// never ask.
    port: Option<&'a dyn AskPort>,
}

impl<'a> MsgCtx<'a> {
    /// Terminate this actor gracefully: records the intent and returns
    /// immediately — no await semantics, never blocks. The stop executes
    /// after this message commits (and after any intents recorded before
    /// it); a crash before the commit discards it. Code after the call in
    /// the handler still runs; call it last (or `return` after) to make
    /// "stop now" unambiguous.
    pub fn stop_self(&mut self) {
        self.core.outbox.push_stop_self();
    }

    /// Records a send to `dest` (typed: the schema id comes from the
    /// message type, the payload from serde).
    pub(crate) fn send_json(
        &mut self,
        dest: Address,
        schema: SchemaId,
        payload: Json,
        reply_to: Option<Address>,
    ) {
        let mut envelope = Envelope::json(schema, dest, payload, self.core.child_trace())
            .from(self.core.self_path.clone());
        if let Some(reply_to) = reply_to {
            envelope = envelope.reply_to(reply_to);
        }
        self.core.outbox.push_send(envelope);
    }

    /// Escape hatch: records a broadcast with an explicit schema id and
    /// hand-built payload. The envelope's destination is the schema
    /// address itself — the trace's `dest` reads as the fan-out target.
    pub(crate) fn publish_json(&mut self, schema: SchemaId, payload: Json) {
        let envelope = Envelope::json(
            schema.clone(),
            Address::Schema(schema),
            payload,
            self.core.child_trace(),
        )
        .from(self.core.self_path.clone());
        self.core.outbox.push_broadcast(envelope);
    }

    /// Records a reply with an explicit schema id and hand-built payload.
    /// Same silent-drop contract as [`MsgCtx::reply`].
    pub(crate) fn reply_json(&mut self, schema: SchemaId, payload: Json) {
        if let Some(reply_to) = self.core.reply_to() {
            self.core
                .outbox
                .push_reply(reply_to.clone(), schema, payload, self.core.child_trace());
        }
    }

    /// Assembles the context for one message dispatch.
    pub(crate) fn new(
        self_path: &'a ActorPath,
        trace: &'a TraceCtx,
        reply_to: Option<&'a Address>,
        view: &'a dyn RuntimeView,
        outbox: &'a mut Outbox,
        port: Option<&'a dyn AskPort>,
    ) -> MsgCtx<'a> {
        MsgCtx {
            core: CtxCore {
                self_path,
                trace,
                reply_to,
                view,
                outbox,
            },
            port,
        }
    }

    /// THE ask: send a request and await its reply, with a MANDATORY
    /// timeout. Exclusive to service actors — an ES decision function is
    /// sync and cannot await (AC5).
    ///
    /// The timeout produces an [`AskOutcome::Timeout`] fact; the reply
    /// lease dies with it, so a late reply lands nowhere.
    pub async fn ask_json(
        &mut self,
        dest: Address,
        schema: SchemaId,
        payload: Json,
        timeout: std::time::Duration,
    ) -> Result<Json, error_stack::Report<AskError>> {
        let port = self.port.expect("ask requires a port (service tier)");
        ask_via_port(port, dest, schema, payload, timeout, *self.core.trace).await
    }

    /// Typed ask: the request's schema id and payload come from the
    /// message type — the typed sibling of [`MsgCtx::ask_json`]. Same
    /// MANDATORY-timeout lease, same tier exclusivity (`ask` awaits, so a
    /// sync ES decision function has none; see [`CmdCtx`]).
    ///
    /// The reply arrives as raw JSON this pass, matching
    /// [`crate::system::ActorSystem::ask`]'s return; decode it with the
    /// reply schema's type when the contract is known.
    ///
    /// # Errors
    ///
    /// [`AskError::Unresolved`] when the destination does not resolve,
    /// the ask times out, or the lease dies before the reply.
    ///
    /// # Panics
    ///
    /// Panics when the request cannot serialize — a programmer error
    /// (serde only fails on pathological map keys), not a domain outcome.
    pub async fn ask<M: Message>(
        &mut self,
        dest: Address,
        req: &M,
        timeout: std::time::Duration,
    ) -> Result<Json, error_stack::Report<AskError>> {
        let payload = Json::of(req);
        self.ask_json(dest, M::schema_id(), payload, timeout).await
    }

    /// Records a ONE-OF send (typed): exactly one copy goes to ONE actor
    /// that declared `.handles::<M>()`, round-robin through the route
    /// table (deferred; flushed post-ack). The typed sibling of
    /// [`MsgCtx::publish`] — news (publish) reaches everyone, work
    /// (send_to_any) reaches one. Zero handlers ⇒ dead-letter on flush.
    pub fn send_to_any<M: Message>(&mut self, msg: &M) {
        let payload = Json::of(&msg);
        let schema = M::schema_id();
        self.send_json(Address::Schema(schema.clone()), schema, payload, None);
    }

    /// Records a send to `dest` (deferred; the kernel flushes post-ack).
    /// Typed: the schema id comes from the message type.
    pub fn send<M: Message>(&mut self, dest: Address, msg: &M, reply_to: Option<Address>) {
        let payload = Json::of(&msg);
        self.send_json(dest, M::schema_id(), payload, reply_to);
    }

    /// Records an event broadcast (deferred; flushed post-ack). Typed:
    /// the schema id comes from the message type. Zero subscribers ⇒
    /// silent no-op: events are news, not work orders.
    pub fn publish<M: Message>(&mut self, msg: &M) {
        let payload = Json::of(&msg);
        self.publish_json(M::schema_id(), payload);
    }

    /// Records a reply to the message's `reply_to`, if the sender asked.
    ///
    /// A reply without a `reply_to` is dropped silently: the asker is gone,
    /// so the fact is unobservable by definition. It is NEVER a broadcast —
    /// use [`MsgCtx::publish`] for events.
    pub fn reply<M: Message>(&mut self, msg: M) {
        let payload = Json::of(&msg);
        self.reply_json(M::schema_id(), payload);
    }

    /// Snapshot info about a path.
    pub fn lookup(&self, path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
        self.core.lookup(path)
    }

    /// Every path registered as a handler for a schema.
    pub fn handlers_of(&self, schema: &SchemaId) -> Vec<ActorPath> {
        self.core.handlers_of(schema)
    }

    /// The time the message was received (injected clock = deterministic).
    pub fn recv_ts(&self) -> Timestamp {
        self.core.recv_ts()
    }

    /// The processing actor's registered path.
    pub fn self_path(&self) -> &ActorPath {
        self.core.self_path
    }

    /// The incoming message's reply address, if the sender asked for a
    /// reply. Handlers usually want `reply` instead; this is for routing
    /// continuations to a durable path.
    pub fn reply_dest(&self) -> Option<Address> {
        self.core.reply_to().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorKind;
    use crate::registry::EndpointInfo;
    use crate::schema::{ActorManifest, Command, Event, Schema, SchemaKind};
    use parking_lot::Mutex;
    use serde::{Deserialize, Serialize};

    /// Typed messages for the effect tests: the schema id comes from the
    /// type, so the assertions prove the derivation.
    #[derive(Command, Serialize, Deserialize)]
    struct ReserveStock {
        qty: i64,
    }

    #[derive(Event, Serialize, Deserialize)]
    struct Reserved {
        ok: bool,
    }

    #[derive(Event, Serialize, Deserialize)]
    struct StockReserved {
        qty: i64,
    }

    #[derive(Command, Serialize, Deserialize)]
    struct Ping;

    #[derive(Event, Serialize, Deserialize)]
    struct Pong;

    /// A view over static data; tests never touch a real registry.
    struct FakeView {
        paths: Vec<(ActorPath, ActorKind)>,
        handlers: Vec<(SchemaId, ActorPath)>,
        now: Timestamp,
    }

    impl FakeView {
        fn at_millis(millis: u64) -> Self {
            Self {
                paths: vec![
                    (ActorPath::new("inventory.west"), ActorKind::EventSourced),
                    (ActorPath::new("auditor"), ActorKind::Service),
                ],
                handlers: vec![(
                    SchemaId::new("ReserveStock", 1),
                    ActorPath::new("inventory.west"),
                )],
                now: Timestamp::from_millis(millis),
            }
        }
    }

    impl RuntimeView for FakeView {
        fn lookup(&self, path: &ActorPath) -> Option<EndpointInfo> {
            self.paths
                .iter()
                .find(|(p, _)| p == path)
                .map(|(p, kind)| EndpointInfo {
                    path: p.clone(),
                    kind: *kind,
                    manifest: ActorManifest::new().kind(*kind),
                })
        }

        fn handlers_of(&self, schema: &SchemaId) -> Vec<ActorPath> {
            self.handlers
                .iter()
                .filter(|(s, _)| s == schema)
                .map(|(_, p)| p.clone())
                .collect()
        }

        fn now(&self) -> Timestamp {
            self.now
        }
    }

    #[test]
    fn send_records_a_deferred_intent_with_child_trace() {
        // Given a context over a fake view with a parent trace.
        let view = FakeView::at_millis(1_000);
        let parent = TraceCtx::root();
        let mut outbox = Outbox::new();
        let path = ActorPath::new("storefront");
        let mut ctx = MsgCtx::new(&path, &parent, None, &view, &mut outbox, None);

        // When sending a command.
        ctx.send(
            Address::Path(ActorPath::new("inventory.west")),
            &ReserveStock { qty: 2 },
            None,
        );

        // Then one send intent is pending, stamped with the sender, the
        // parent's trace id, and a fresh causality id.
        assert_eq!(outbox.len(), 1);
        let drained: Vec<_> = outbox.drain().collect();
        match &drained[0] {
            Intent::Send(envelope) => {
                assert_eq!(
                    envelope.from.as_ref().map(|p| p.as_str()),
                    Some("storefront")
                );
                assert_eq!(envelope.trace.trace_id, parent.trace_id);
                assert_ne!(envelope.trace.causality_id, parent.causality_id);
                assert_eq!(envelope.schema.as_str(), "ReserveStock@1");
            }
            Intent::Reply { .. } => panic!("expected a send"),
            Intent::Broadcast(_) | Intent::StopSelf => panic!("expected a send"),
        }
    }

    #[test]
    fn reply_targets_the_requester_only_when_reply_to_exists() {
        // Given a context whose message carries a reply-to path.
        let mut view = Mutex::new(FakeView::at_millis(0));
        let view = view.get_mut();
        let trace = TraceCtx::root();
        let mut outbox = Outbox::new();
        let path = ActorPath::new("server");
        let reply_to = Address::Path(ActorPath::new("client"));
        let mut ctx = MsgCtx::new(&path, &trace, Some(&reply_to), view, &mut outbox, None);

        // When replying.
        ctx.reply(Reserved { ok: true });

        // Then one reply intent targets the client.
        let drained: Vec<_> = outbox.drain().collect();
        match &drained[0] {
            Intent::Send(_) => panic!("expected a reply intent"),
            Intent::Reply { to, .. } => {
                assert_eq!(*to, Address::Path(ActorPath::new("client")));
            }
            Intent::Broadcast(_) | Intent::StopSelf => panic!("expected a reply intent"),
        }

        // When a context without reply-to replies.
        let mut silent_outbox = Outbox::new();
        let mut silent = MsgCtx::new(&path, &trace, None, view, &mut silent_outbox, None);
        silent.reply(Reserved { ok: false });

        // Then nothing is recorded.
        assert!(silent_outbox.is_empty());
    }

    #[test]
    fn publish_records_a_broadcast_intent() {
        // Given a context.
        let view = FakeView::at_millis(0);
        let trace = TraceCtx::root();
        let mut outbox = Outbox::new();
        let path = ActorPath::new("inventory.west");
        let mut ctx = MsgCtx::new(&path, &trace, None, &view, &mut outbox, None);

        // When publishing an event.
        ctx.publish(&StockReserved { qty: 2 });

        // Then a broadcast intent is pending, addressed to the schema.
        let drained: Vec<_> = outbox.drain().collect();
        match &drained[0] {
            Intent::Broadcast(envelope) => {
                assert_eq!(envelope.schema, StockReserved::schema_id());
                assert_eq!(envelope.trace.trace_id, trace.trace_id);
            }
            _ => panic!("expected a broadcast"),
        }
    }

    #[test]
    fn lookups_delegate_to_the_runtime_view() {
        // Given a context over a view that knows one handler.
        let view = FakeView::at_millis(7_777);
        let trace = TraceCtx::root();
        let mut outbox = Outbox::new();
        let path = ActorPath::new("someone");
        let ctx = CmdCtx::new(&path, &trace, None, &view, &mut outbox);

        // When querying lookups, handlers_of, and the receive timestamp.
        let info = ctx.lookup(&ActorPath::new("auditor"));
        let missing = ctx.lookup(&ActorPath::new("ghost"));
        let handlers = ctx.handlers_of(&SchemaId::new("ReserveStock", 1));
        let ts = ctx.recv_ts();

        // Then the view's answers come through, including the clock's.
        assert_eq!(info.map(|i| i.kind), Some(ActorKind::Service));
        assert!(missing.is_none());
        assert_eq!(handlers, [ActorPath::new("inventory.west")]);
        assert_eq!(ts.as_millis(), 7_777);
    }

    #[test]
    fn outbox_drain_empties_pending_effects() {
        // Given an outbox with two recorded sends.
        let view = FakeView::at_millis(0);
        let trace = TraceCtx::root();
        let mut outbox = Outbox::new();
        let path = ActorPath::new("a");
        let mut ctx = MsgCtx::new(&path, &trace, None, &view, &mut outbox, None);
        ctx.send(Address::Path(ActorPath::new("b")), &Ping, None);
        ctx.send(Address::Path(ActorPath::new("c")), &Pong, None);

        // When draining.
        let count = outbox.drain().count();

        // Then both intents flushed and the outbox is empty.
        assert_eq!(count, 2);
        assert!(outbox.is_empty());
    }

    #[test]
    fn msg_ctx_shares_the_same_deferred_surface() {
        // Given a message context (service actor).
        let view = FakeView::at_millis(0);
        let trace = TraceCtx::root();
        let mut outbox = Outbox::new();
        let path = ActorPath::new("auditor");
        let mut ctx = MsgCtx::new(&path, &trace, None, &view, &mut outbox, None);

        // When it sends.
        ctx.send(Address::Path(ActorPath::new("b")), &Ping, None);

        // Then the effect is deferred identically.
        assert_eq!(outbox.len(), 1);
        assert!(matches!(SchemaKind::Command, SchemaKind::Command));
    }

    /// The silent-drop contract: a reply with no reply_to records nothing.
    /// It is NEVER a broadcast — the fact is unobservable by definition.
    #[test]
    fn reply_without_reply_to_is_dropped_silently() {
        // Given a message context with no reply_to (a tell delivery).
        let view = FakeView::at_millis(0);
        let trace = TraceCtx::root();
        let mut outbox = Outbox::new();
        let path = ActorPath::new("auditor");
        let mut ctx = MsgCtx::new(&path, &trace, None, &view, &mut outbox, None);

        // When the handler replies.
        ctx.reply(Pong);

        // Then the outbox stays empty — nothing is recorded anywhere.
        assert_eq!(outbox.len(), 0);
    }

    /// The counterpart: WITH a reply_to, the same call records a Reply
    /// intent (point-to-point to the asker).
    #[test]
    fn reply_with_reply_to_records_a_reply_intent() {
        // Given a message context whose message carried a reply_to.
        let view = FakeView::at_millis(0);
        let trace = TraceCtx::root();
        let reply_to = Address::Path(ActorPath::new("asker"));
        let mut outbox = Outbox::new();
        let path = ActorPath::new("auditor");
        let mut ctx = MsgCtx::new(&path, &trace, Some(&reply_to), &view, &mut outbox, None);

        // When the handler replies.
        ctx.reply(Pong);

        // Then exactly one Reply intent is recorded, addressed to the asker.
        assert_eq!(outbox.len(), 1);
    }

    /// The typed reply records EXACTLY the intent the raw variant does:
    /// schema id from `E::schema_id()`, payload from serde.
    #[test]
    fn typed_reply_records_the_same_intent_as_the_raw_variant() {
        // Given two identical contexts (one typed, one raw).
        let view = FakeView::at_millis(0);
        let trace = TraceCtx::root();
        let reply_to = Address::Path(ActorPath::new("client"));
        let path = ActorPath::new("server");
        let mut typed_outbox = Outbox::new();
        let mut raw_outbox = Outbox::new();
        {
            let mut typed = MsgCtx::new(
                &path,
                &trace,
                Some(&reply_to),
                &view,
                &mut typed_outbox,
                None,
            );
            let mut raw = MsgCtx::new(&path, &trace, Some(&reply_to), &view, &mut raw_outbox, None);

            // When replying the same outcome both ways.
            typed.reply(Reserved { ok: true });
            raw.reply_json(Reserved::schema_id(), crate::json!({ "ok": true }));
        }

        // Then the drained intents are identical.
        let typed_intents: Vec<_> = typed_outbox.drain().collect();
        let raw_intents: Vec<_> = raw_outbox.drain().collect();
        assert_eq!(typed_intents.len(), 1);
        assert_eq!(raw_intents.len(), 1);
        // (Both contexts dropped above; only the intents remain.)
        match (&typed_intents[0], &raw_intents[0]) {
            (
                Intent::Reply {
                    to: a,
                    schema: sa,
                    payload: pa,
                    ..
                },
                Intent::Reply {
                    to: b,
                    schema: sb,
                    payload: pb,
                    ..
                },
            ) => {
                assert_eq!(a, b);
                assert_eq!(*sa, Reserved::schema_id());
                assert_eq!(sa, sb);
                assert_eq!(pa, pb);
            }
            _ => panic!("expected reply intents"),
        }
    }

    /// Same equivalence for publish: event broadcast, derived schema id, payload.
    #[test]
    fn typed_publish_records_the_same_intent_as_the_raw_variant() {
        // Given two identical contexts.
        let view = FakeView::at_millis(0);
        let trace = TraceCtx::root();
        let path = ActorPath::new("inventory.west");
        let mut typed_outbox = Outbox::new();
        let mut raw_outbox = Outbox::new();
        {
            let mut typed = MsgCtx::new(&path, &trace, None, &view, &mut typed_outbox, None);
            typed.publish(&StockReserved { qty: 2 });
        }
        {
            let mut raw = MsgCtx::new(&path, &trace, None, &view, &mut raw_outbox, None);
            raw.publish_json(StockReserved::schema_id(), crate::json!({ "qty": 2 }));
        }

        // Then the broadcast intents are identical.
        let typed: Vec<_> = typed_outbox.drain().collect();
        let raw: Vec<_> = raw_outbox.drain().collect();
        match (&typed[0], &raw[0]) {
            (Intent::Broadcast(ea), Intent::Broadcast(eb)) => {
                assert_eq!(ea.schema, eb.schema);
                assert_eq!(ea.schema, StockReserved::schema_id());
                assert_eq!(ea.dest, eb.dest);
                assert_eq!(
                    ea.clone().into_json().expect("json"),
                    eb.clone().into_json().expect("json")
                );
            }
            _ => panic!("expected broadcast intents"),
        }
    }

    /// Same equivalence for send: destination, derived schema id, payload,
    /// sender stamp.
    #[test]
    fn typed_send_records_the_same_intent_as_the_raw_variant() {
        // Given two identical contexts.
        let view = FakeView::at_millis(0);
        let trace = TraceCtx::root();
        let path = ActorPath::new("storefront");
        let mut typed_outbox = Outbox::new();
        let mut raw_outbox = Outbox::new();
        {
            let mut typed = MsgCtx::new(&path, &trace, None, &view, &mut typed_outbox, None);
            typed.send(
                Address::Path(ActorPath::new("inventory")),
                &ReserveStock { qty: 2 },
                None,
            );
        }
        {
            let mut raw = MsgCtx::new(&path, &trace, None, &view, &mut raw_outbox, None);
            raw.send_json(
                Address::Path(ActorPath::new("inventory")),
                ReserveStock::schema_id(),
                crate::json!({ "qty": 2 }),
                None,
            );
        }

        // Then the send intents are identical.
        let typed: Vec<_> = typed_outbox.drain().collect();
        let raw: Vec<_> = raw_outbox.drain().collect();
        match (&typed[0], &raw[0]) {
            (Intent::Send(ea), Intent::Send(eb)) => {
                assert_eq!(ea.dest, eb.dest);
                assert_eq!(ea.schema, ReserveStock::schema_id());
                assert_eq!(ea.schema, eb.schema);
                assert_eq!(
                    ea.clone().into_json().expect("json"),
                    eb.clone().into_json().expect("json")
                );
                assert_eq!(ea.from, eb.from);
            }
            _ => panic!("expected send intents"),
        }
    }

    #[test]
    fn es_entity_cannot_stop_itself() {
        // Given the purified CmdCtx surface, a handler body that merely
        // USES every public method must compile WITHOUT any lifecycle or
        // send capability - an entity owns no intents at all.
        fn uses_surface(ctx: &mut CmdCtx<'_>) {
            let _ = ctx.lookup(&ActorPath::new("x"));
            let _ = ctx.handlers_of(&SchemaId::new("S", 1));
            let _ = ctx.recv_ts();
            let _ = ctx.self_path();
            // ctx.stop_self();       <- no longer exists (compile-fail by design)
            // ctx.send(...);         <- no longer exists
            // ctx.publish(...);      <- no longer exists
            // ctx.reply(...);        <- no longer exists
            // ctx.send_to_any(...);  <- no longer exists
        }
        // Then the surface compiles pure (this test IS the assertion).
        let view = FakeView::at_millis(0);
        let trace = TraceCtx::root();
        let mut outbox = Outbox::new();
        let path = ActorPath::new("entity");
        let mut ctx = CmdCtx::new(&path, &trace, None, &view, &mut outbox);
        uses_surface(&mut ctx);
        // And recording nothing: the outbox stays empty by construction.
        assert!(outbox.is_empty());
    }
}
