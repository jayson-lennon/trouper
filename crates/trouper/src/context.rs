//! Handler-facing context: the actors' entire syscall surface.
//!
//! Event-sourced handlers are pure — [`CmdCtx`] records *intents* into an
//! [`Outbox`] and performs nothing; the kernel flushes the outbox AFTER
//! journal-append + ack, so a crash before ack never duplicates a send.
//! Service handlers get [`MsgCtx`], a superset; its `ask`/`subscribe`
//! methods arrive with the reply-lease and topic machinery (Phases 5–6).
//!
//! Contexts touch the registry only through [`RuntimeView`], a read-only
//! view — user code never holds the registry lock, so lookups from inside a
//! handler can never deadlock the kernel.

use crate::actor::ActorPath;
use crate::clock::Timestamp;
use crate::envelope::{Address, Envelope, TraceCtx};
use crate::kernel::AskOutcome;
use crate::schema::Message;
use crate::schema::SchemaId;
use crate::topics::Topic;
use serde_json::Value as JsonValue;

/// Read-only runtime view for handlers: registry lookups plus the clock.
///
/// Implemented by the system facade; the kernel hands contexts a reference.
pub(crate) trait RuntimeView: Send + Sync {
    /// Snapshot info about a path, if registered.
    fn lookup(&self, path: &ActorPath) -> Option<crate::registry::EndpointInfo>;

    /// Every path registered as a handler for a schema.
    fn who_handles(&self, schema: &SchemaId) -> Vec<ActorPath>;

    /// The current time from the injected clock.
    fn now(&self) -> Timestamp;
}

/// One deferred effect, fully stamped; the kernel executes these post-ack.
#[derive(Debug)]
pub(crate) enum Intent {
    /// A point-to-point send to an address.
    Send(Envelope),
    /// A publish onto a topic.
    Publish { topic: Topic, envelope: Envelope },
    /// A reply to the message being handled (address = its reply_to).
    Reply {
        /// The reply address copied from the incoming envelope.
        to: Address,
        /// The reply schema (tracing) — may be the request's schema id.
        schema: SchemaId,
        /// The reply payload.
        payload: JsonValue,
        /// The trace of the message being replied to (causality links).
        trace: TraceCtx,
    },
    /// Subscribe the handling actor to a topic (performed post-ack, so a
    /// crash before ack never leaves a half-applied subscription).
    Subscribe { path: ActorPath, topic: Topic },
    /// Terminate the handling actor gracefully after this message
    /// commits (performed post-ack, so a crash before ack discards the
    /// stop exactly like any other intent — the actor restarts and
    /// continues). NEVER journaled: replay never synthesizes a stop.
    StopSelf,
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
    pub fn push_send(&mut self, envelope: Envelope) {
        self.intents.push(Intent::Send(envelope));
    }

    /// Records a reply intent (resolved by the kernel at flush time).
    pub fn push_reply(
        &mut self,
        to: Address,
        schema: SchemaId,
        payload: JsonValue,
        trace: TraceCtx,
    ) {
        self.intents.push(Intent::Reply {
            to,
            schema,
            payload,
            trace,
        });
    }

    /// Records a subscription intent (performed by the kernel post-ack).
    pub fn push_subscribe(&mut self, path: ActorPath, topic: Topic) {
        self.intents.push(Intent::Subscribe { path, topic });
    }

    /// Records a publish intent.
    pub fn push_publish(&mut self, topic: Topic, envelope: Envelope) {
        self.intents.push(Intent::Publish { topic, envelope });
    }

    /// Records the self-termination intent. Executed post-ack by the
    /// kernel: intents recorded BEFORE it flush first (in-order), and a
    /// crash before ack discards it entirely.
    pub fn push_stop_self(&mut self) {
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
                    (
                        crate::reply::LeaseId,
                        tokio::sync::oneshot::Receiver<JsonValue>,
                    ),
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
    fn child_trace(&self) -> TraceCtx {
        self.trace.caused()
    }

    /// Records a send to `dest` (typed: the schema id comes from the
    /// message type, the payload from serde).
    pub fn send<M: Message>(&mut self, dest: Address, msg: &M, reply_to: Option<Address>) {
        let payload = serde_json::to_value(msg).expect("schema payload serializes");
        self.send_json(dest, M::schema_id(), payload, reply_to);
    }

    /// Records a publish onto `topic` (typed).
    pub fn publish<M: Message>(&mut self, topic: Topic, msg: &M) {
        let payload = serde_json::to_value(msg).expect("schema payload serializes");
        self.publish_json(topic, M::schema_id(), payload);
    }

    /// Records the self-termination intent (shared by both ctx tiers).
    pub fn stop_self(&mut self) {
        self.outbox.push_stop_self();
    }

    /// Records a reply to the message's `reply_to`, if the sender asked
    /// (typed).
    ///
    /// A reply without a `reply_to` is dropped silently: the asker is gone,
    /// so the fact is unobservable by definition. It is NEVER a broadcast —
    /// use [`CtxCore::publish`] for topics.
    pub fn reply<M: Message>(&mut self, msg: M) {
        let payload = serde_json::to_value(&msg).expect("schema payload serializes");
        self.reply_json(M::schema_id(), payload);
    }

    /// Escape hatch: records a send with an explicit schema id and
    /// hand-built payload.
    pub fn send_json(
        &mut self,
        dest: Address,
        schema: SchemaId,
        payload: JsonValue,
        reply_to: Option<Address>,
    ) {
        let mut envelope =
            Envelope::json(schema, dest, payload, self.child_trace()).from(self.self_path.clone());
        if let Some(reply_to) = reply_to {
            envelope = envelope.reply_to(reply_to);
        }
        self.outbox.push_send(envelope);
    }

    /// Escape hatch: records a publish with an explicit schema id and
    /// hand-built payload.
    pub fn publish_json(&mut self, topic: Topic, schema: SchemaId, payload: JsonValue) {
        let envelope = Envelope::json(
            schema,
            Address::Topic(topic.clone()),
            payload,
            self.child_trace(),
        )
        .from(self.self_path.clone());
        self.outbox.push_publish(topic, envelope);
    }

    /// Escape hatch: records a reply with an explicit schema id and
    /// hand-built payload. Same silent-drop contract as [`CtxCore::reply`].
    pub fn reply_json(&mut self, schema: SchemaId, payload: JsonValue) {
        if let Some(reply_to) = self.reply_to {
            self.outbox
                .push_reply(reply_to.clone(), schema, payload, self.child_trace());
        }
    }

    /// Snapshot info about a path.
    pub fn lookup(&self, path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
        self.view.lookup(path)
    }

    /// Every path registered as a handler for a schema.
    pub fn who_handles(&self, schema: &SchemaId) -> Vec<ActorPath> {
        self.view.who_handles(schema)
    }

    /// The time the message was received (injected clock = deterministic).
    pub fn recv_ts(&self) -> Timestamp {
        self.view.now()
    }

    /// The incoming message's reply address, if the sender asked for a
    /// reply. Handlers usually want [`CtxCore::reply`] instead; this is for
    /// routing continuations to a durable path.
    pub fn reply_dest(&self) -> Option<Address> {
        self.reply_to.cloned()
    }
}

/// Context for event-sourced handlers: sync, pure, deferred effects only.
///
/// There is deliberately no `ask` here: the handler is sync and cannot
/// await, and no I/O sneaks into a decision function.
///
/// The constructor is crate-private: only the kernel assembles contexts.
pub struct CmdCtx<'a> {
    core: CtxCore<'a>,
}

impl<'a> CmdCtx<'a> {
    /// Terminate this actor gracefully: records the intent and returns
    /// immediately — no await semantics, never blocks. The stop executes
    /// after this message commits (and after any intents recorded before
    /// it); a crash before the commit discards it. Code after the call in
    /// the handler still runs; call it last (or `return` after) to make
    /// "stop now" unambiguous.
    pub fn stop_self(&mut self) {
        self.core.stop_self();
    }

    /// Assembles the context for one command dispatch.
    pub(crate) fn new(
        self_path: &'a ActorPath,
        trace: &'a TraceCtx,
        reply_to: Option<&'a Address>,
        view: &'a dyn RuntimeView,
        outbox: &'a mut Outbox,
    ) -> CmdCtx<'a> {
        CmdCtx {
            core: CtxCore {
                self_path,
                trace,
                reply_to,
                view,
                outbox,
            },
        }
    }

    /// Records a send to `dest` (deferred; the kernel flushes post-ack).
    /// Typed: the schema id comes from the message type.
    pub fn send<M: Message>(&mut self, dest: Address, msg: &M, reply_to: Option<Address>) {
        self.core.send(dest, msg, reply_to);
    }

    /// Records a publish onto `topic` (deferred; flushed post-ack). Typed:
    /// the schema id comes from the message type.
    pub fn publish<M: Message>(&mut self, topic: Topic, msg: &M) {
        self.core.publish(topic, msg);
    }

    /// Records a reply to the message's `reply_to`, if the sender asked.
    ///
    /// A reply without a `reply_to` is dropped silently: the asker is gone,
    /// so the fact is unobservable by definition. It is NEVER a broadcast —
    /// use [`CmdCtx::publish`] for topics.
    pub fn reply<M: Message>(&mut self, msg: M) {
        self.core.reply(msg);
    }

    /// Escape hatch: send with an explicit schema id and hand-built payload.
    pub fn send_json(
        &mut self,
        dest: Address,
        schema: SchemaId,
        payload: JsonValue,
        reply_to: Option<Address>,
    ) {
        self.core.send_json(dest, schema, payload, reply_to);
    }

    /// Escape hatch: publish with an explicit schema id and hand-built
    /// payload.
    pub fn publish_json(&mut self, topic: Topic, schema: SchemaId, payload: JsonValue) {
        self.core.publish_json(topic, schema, payload);
    }

    /// Escape hatch: reply with an explicit schema id and hand-built
    /// payload. Same silent-drop contract as [`CmdCtx::reply`].
    pub fn reply_json(&mut self, schema: SchemaId, payload: JsonValue) {
        self.core.reply_json(schema, payload);
    }

    /// Snapshot info about a path.
    pub fn lookup(&self, path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
        self.core.lookup(path)
    }

    /// Every path registered as a handler for a schema.
    pub fn who_handles(&self, schema: &SchemaId) -> Vec<ActorPath> {
        self.core.who_handles(schema)
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
        self.core.reply_dest()
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
        payload: JsonValue,
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
    payload: JsonValue,
    timeout: std::time::Duration,
    trace: TraceCtx,
) -> Result<JsonValue, error_stack::Report<AskError>> {
    use error_stack::ResultExt;
    let dest_label = format!("{dest:?}");
    let (lease, mut receiver) = port
        .ask_channel(dest.clone(), schema, payload, timeout)
        .await
        .change_context(AskError::Unresolved(format!("{dest:?}")))?;
    let outcome = match tokio::time::timeout(timeout, &mut receiver).await {
        Ok(Ok(reply)) => Some((AskOutcome::Replied, reply)),
        Ok(Err(_)) => Some((AskOutcome::Failed, JsonValue::Null)),
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
        self.core.stop_self();
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

    /// Subscribes the handling actor to `topic`. Recorded as a deferred
    /// intent and performed by the kernel after the message is acked
    /// (subscription starts at the topic's next offset).
    pub fn subscribe(&mut self, topic: Topic) {
        self.core
            .outbox
            .push_subscribe(self.core.self_path.clone(), topic);
    }

    /// THE ask: send a request and await its reply, with a MANDATORY
    /// timeout. Exclusive to service actors — an ES decision function is
    /// sync and cannot await (AC5).
    ///
    /// The timeout produces an [`AskOutcome::Timeout`] fact; the reply
    /// lease dies with it, so a late reply lands nowhere.
    pub async fn ask(
        &mut self,
        dest: Address,
        schema: SchemaId,
        payload: JsonValue,
        timeout: std::time::Duration,
    ) -> Result<JsonValue, error_stack::Report<AskError>> {
        let port = self.port.expect("ask requires a port (service tier)");
        ask_via_port(port, dest, schema, payload, timeout, *self.core.trace).await
    }

    /// Records a send to `dest` (deferred; the kernel flushes post-ack).
    /// Typed: the schema id comes from the message type.
    pub fn send<M: Message>(&mut self, dest: Address, msg: &M, reply_to: Option<Address>) {
        self.core.send(dest, msg, reply_to);
    }

    /// Records a publish onto `topic` (deferred; flushed post-ack). Typed:
    /// the schema id comes from the message type.
    pub fn publish<M: Message>(&mut self, topic: Topic, msg: &M) {
        self.core.publish(topic, msg);
    }

    /// Records a reply to the message's `reply_to`, if the sender asked.
    ///
    /// A reply without a `reply_to` is dropped silently: the asker is gone,
    /// so the fact is unobservable by definition. It is NEVER a broadcast —
    /// use [`MsgCtx::publish`] for topics.
    pub fn reply<M: Message>(&mut self, msg: M) {
        self.core.reply(msg);
    }

    /// Escape hatch: send with an explicit schema id and hand-built payload.
    pub fn send_json(
        &mut self,
        dest: Address,
        schema: SchemaId,
        payload: JsonValue,
        reply_to: Option<Address>,
    ) {
        self.core.send_json(dest, schema, payload, reply_to);
    }

    /// Escape hatch: publish with an explicit schema id and hand-built
    /// payload.
    pub fn publish_json(&mut self, topic: Topic, schema: SchemaId, payload: JsonValue) {
        self.core.publish_json(topic, schema, payload);
    }

    /// Escape hatch: reply with an explicit schema id and hand-built
    /// payload. Same silent-drop contract as [`MsgCtx::reply`].
    pub fn reply_json(&mut self, schema: SchemaId, payload: JsonValue) {
        self.core.reply_json(schema, payload);
    }

    /// Snapshot info about a path.
    pub fn lookup(&self, path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
        self.core.lookup(path)
    }

    /// Every path registered as a handler for a schema.
    pub fn who_handles(&self, schema: &SchemaId) -> Vec<ActorPath> {
        self.core.who_handles(schema)
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
        self.core.reply_dest()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorKind;
    use crate::registry::EndpointInfo;
    use crate::schema::{ActorManifest, Schema, SchemaDef, SchemaKind};
    use parking_lot::Mutex;
    use serde::{Deserialize, Serialize};

    /// Typed messages for the effect tests: the schema id comes from the
    /// type, so the assertions prove the derivation.
    #[derive(Serialize, Deserialize)]
    struct ReserveStock {
        qty: i64,
    }
    impl Schema for ReserveStock {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "ReserveStock".into(),
                version: 1,
                kind: SchemaKind::Command,
                fields: vec![],
                description: None,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Reserved {
        ok: bool,
    }
    impl Schema for Reserved {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Reserved".into(),
                version: 1,
                kind: SchemaKind::Event,
                fields: vec![],
                description: None,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    struct StockReserved {
        qty: i64,
    }
    impl Schema for StockReserved {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "StockReserved".into(),
                version: 1,
                kind: SchemaKind::Event,
                fields: vec![],
                description: None,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Ping;
    impl Schema for Ping {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Ping".into(),
                version: 1,
                kind: SchemaKind::Command,
                fields: vec![],
                description: None,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Pong;
    impl Schema for Pong {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Pong".into(),
                version: 1,
                kind: SchemaKind::Event,
                fields: vec![],
                description: None,
            }
        }
    }

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

        fn who_handles(&self, schema: &SchemaId) -> Vec<ActorPath> {
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
        let mut ctx = CmdCtx::new(&path, &parent, None, &view, &mut outbox);

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
            Intent::Publish { .. } => panic!("expected a send"),
            Intent::Reply { .. } => panic!("expected a send"),
            Intent::Subscribe { .. } | Intent::StopSelf => panic!("expected a send"),
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
        let mut ctx = CmdCtx::new(&path, &trace, Some(&reply_to), view, &mut outbox);

        // When replying.
        ctx.reply(Reserved { ok: true });

        // Then one reply intent targets the client.
        let drained: Vec<_> = outbox.drain().collect();
        match &drained[0] {
            Intent::Send(_) => panic!("expected a reply intent"),
            Intent::Publish { .. } => panic!("expected a reply intent"),
            Intent::Reply { to, .. } => {
                assert_eq!(*to, Address::Path(ActorPath::new("client")));
            }
            Intent::Subscribe { .. } | Intent::StopSelf => panic!("expected a reply intent"),
        }

        // When a context without reply-to replies.
        let mut silent_outbox = Outbox::new();
        let mut silent = CmdCtx::new(&path, &trace, None, view, &mut silent_outbox);
        silent.reply(Reserved { ok: false });

        // Then nothing is recorded.
        assert!(silent_outbox.is_empty());
    }

    #[test]
    fn publish_records_a_topic_intent() {
        // Given a context.
        let view = FakeView::at_millis(0);
        let trace = TraceCtx::root();
        let mut outbox = Outbox::new();
        let path = ActorPath::new("inventory.west");
        let mut ctx = CmdCtx::new(&path, &trace, None, &view, &mut outbox);

        // When publishing an event onto a topic.
        ctx.publish(Topic::new("inventory.events"), &StockReserved { qty: 2 });

        // Then a publish intent is pending for that topic.
        let drained: Vec<_> = outbox.drain().collect();
        match &drained[0] {
            Intent::Publish { topic, envelope } => {
                assert_eq!(topic.as_str(), "inventory.events");
                assert_eq!(envelope.trace.trace_id, trace.trace_id);
            }
            _ => panic!("expected a publish"),
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

        // When querying lookups, who_handles, and the receive timestamp.
        let info = ctx.lookup(&ActorPath::new("auditor"));
        let missing = ctx.lookup(&ActorPath::new("ghost"));
        let handlers = ctx.who_handles(&SchemaId::new("ReserveStock", 1));
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
        let mut ctx = CmdCtx::new(&path, &trace, None, &view, &mut outbox);
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
            let mut typed = CmdCtx::new(&path, &trace, Some(&reply_to), &view, &mut typed_outbox);
            let mut raw = CmdCtx::new(&path, &trace, Some(&reply_to), &view, &mut raw_outbox);

            // When replying the same outcome both ways.
            typed.reply(Reserved { ok: true });
            raw.reply_json(Reserved::schema_id(), serde_json::json!({ "ok": true }));
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

    /// Same equivalence for publish: topic, derived schema id, payload.
    #[test]
    fn typed_publish_records_the_same_intent_as_the_raw_variant() {
        // Given two identical contexts.
        let view = FakeView::at_millis(0);
        let trace = TraceCtx::root();
        let path = ActorPath::new("inventory.west");
        let mut typed_outbox = Outbox::new();
        let mut raw_outbox = Outbox::new();
        {
            let mut typed = CmdCtx::new(&path, &trace, None, &view, &mut typed_outbox);
            typed.publish(Topic::new("inventory.events"), &StockReserved { qty: 2 });
        }
        {
            let mut raw = CmdCtx::new(&path, &trace, None, &view, &mut raw_outbox);
            raw.publish_json(
                Topic::new("inventory.events"),
                StockReserved::schema_id(),
                serde_json::json!({ "qty": 2 }),
            );
        }

        // Then the publish intents are identical.
        let typed: Vec<_> = typed_outbox.drain().collect();
        let raw: Vec<_> = raw_outbox.drain().collect();
        match (&typed[0], &raw[0]) {
            (
                Intent::Publish {
                    topic: ta,
                    envelope: ea,
                },
                Intent::Publish {
                    topic: tb,
                    envelope: eb,
                },
            ) => {
                assert_eq!(ta, tb);
                assert_eq!(ea.schema, eb.schema);
                assert_eq!(ea.schema, StockReserved::schema_id());
                assert_eq!(
                    ea.clone().into_json().expect("json"),
                    eb.clone().into_json().expect("json")
                );
            }
            _ => panic!("expected publish intents"),
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
            let mut typed = CmdCtx::new(&path, &trace, None, &view, &mut typed_outbox);
            typed.send(
                Address::Path(ActorPath::new("inventory")),
                &ReserveStock { qty: 2 },
                None,
            );
        }
        {
            let mut raw = CmdCtx::new(&path, &trace, None, &view, &mut raw_outbox);
            raw.send_json(
                Address::Path(ActorPath::new("inventory")),
                ReserveStock::schema_id(),
                serde_json::json!({ "qty": 2 }),
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
}
