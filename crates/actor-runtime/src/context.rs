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

use crate::envelope::{Address, Envelope, TraceCtx};
use crate::kernel::AskOutcome;
use crate::types::{ActorPath, SchemaId, Timestamp, Topic};
use serde_json::Value as JsonValue;

/// Read-only runtime view for handlers: registry lookups plus the clock.
///
/// Implemented by the system facade; the kernel hands contexts a reference.
pub trait RuntimeView: Send + Sync {
    /// Snapshot info about a path, if registered.
    fn lookup(&self, path: &ActorPath) -> Option<crate::registry::EndpointInfo>;

    /// Every path registered as a handler for a schema.
    fn who_handles(&self, schema: &SchemaId) -> Vec<ActorPath>;

    /// The current time from the injected clock.
    fn now(&self) -> Timestamp;
}

/// One deferred effect, fully stamped; the kernel executes these post-ack.
#[derive(Debug)]
pub enum Intent {
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
}

/// Effects recorded by a handler, flushed by the kernel after ack.
#[derive(Debug, Default)]
pub struct Outbox {
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

    /// Takes every intent, leaving the outbox empty (the flush).
    pub fn drain(&mut self) -> std::vec::Drain<'_, Intent> {
        self.intents.drain(..)
    }

    /// Whether any effects are pending.
    pub fn is_empty(&self) -> bool {
        self.intents.is_empty()
    }

    /// The number of pending effects.
    pub fn len(&self) -> usize {
        self.intents.len()
    }
}

/// The boxed future a [`AskPort::ask_channel`] resolves to.
pub type AskChannelFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    (
                        crate::types::LeaseId,
                        tokio::sync::oneshot::Receiver<JsonValue>,
                    ),
                    error_stack::Report<AskError>,
                >,
            > + Send,
    >,
>;

/// The fields every context shares.
pub struct CtxCore<'a> {
    /// The processing actor's registered path.
    pub self_path: &'a ActorPath,
    /// Trace metadata of the message being processed.
    pub trace: &'a TraceCtx,
    /// Where a reply should go, if the sender asked for one.
    pub reply_to: Option<&'a Address>,
    /// Read-only runtime view (lookups + clock).
    pub view: &'a dyn RuntimeView,
    /// The outbox the kernel flushes after ack.
    pub outbox: &'a mut Outbox,
}

impl CtxCore<'_> {
    /// The trace metadata for a hop caused by this message: same
    /// `trace_id`, fresh `causality_id`.
    fn child_trace(&self) -> TraceCtx {
        self.trace.caused()
    }

    /// Records a send to `dest`.
    ///
    /// The schema is explicit: it is the wire contract, and the destination
    /// decodes with it. The effect is deferred until the kernel flushes.
    pub fn send(
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

    /// Records a publish onto `topic`.
    pub fn publish(&mut self, topic: Topic, schema: SchemaId, payload: JsonValue) {
        let envelope = Envelope::json(
            schema,
            Address::Topic(topic.clone()),
            payload,
            self.child_trace(),
        )
        .from(self.self_path.clone());
        self.outbox.push_publish(topic, envelope);
    }

    /// Records a reply to the message's `reply_to`, if the sender asked.
    ///
    /// A reply without a `reply_to` is dropped silently: the asker is gone,
    /// so the fact is unobservable by definition.
    pub fn reply(&mut self, schema: SchemaId, payload: JsonValue) {
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
}

/// Context for event-sourced handlers: sync, pure, deferred effects only.
///
/// There is deliberately no `ask` here: the handler is sync and cannot
/// await, and no I/O sneaks into a decision function.
pub struct CmdCtx<'a>(pub CtxCore<'a>);

impl CmdCtx<'_> {
    /// Assembles the context for one command dispatch.
    pub fn new<'ctx>(
        self_path: &'ctx ActorPath,
        trace: &'ctx TraceCtx,
        reply_to: Option<&'ctx Address>,
        view: &'ctx dyn RuntimeView,
        outbox: &'ctx mut Outbox,
    ) -> CmdCtx<'ctx> {
        CmdCtx(CtxCore {
            self_path,
            trace,
            reply_to,
            view,
            outbox,
        })
    }
}

/// The impure syscall port a service actor's [`MsgCtx`] carries: opens
/// reply leases, routes envelopes, reads the clock. The system implements
/// it; tests swap it (like [`RuntimeView`]).
pub trait AskPort: Send + Sync {
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
        lease: crate::types::LeaseId,
        dest: Address,
        outcome: AskOutcome,
        trace: TraceCtx,
    );
}

/// Errors surfaced by `ctx.ask`.
#[derive(Debug, wherror::Error)]
#[error(debug)]
pub enum AskError {
    /// The destination did not resolve.
    Unresolved(String),
}

/// Context for service-actor handlers: async, impure by design.
///
/// The sync surface matches [`CmdCtx`]; `ask` is exclusive to this tier —
/// an event-sourced decision function cannot await.
pub struct MsgCtx<'a> {
    pub core: CtxCore<'a>,
    /// The impure port (leases + routing); absent only in pure tests that
    /// never ask.
    pub port: Option<&'a dyn AskPort>,
}

impl MsgCtx<'_> {
    /// Assembles the context for one message dispatch.
    pub fn new<'ctx>(
        self_path: &'ctx ActorPath,
        trace: &'ctx TraceCtx,
        reply_to: Option<&'ctx Address>,
        view: &'ctx dyn RuntimeView,
        outbox: &'ctx mut Outbox,
        port: Option<&'ctx dyn AskPort>,
    ) -> MsgCtx<'ctx> {
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
        use error_stack::ResultExt;
        let port = self.port.expect("ask requires a port (service tier)");
        let trace = *self.core.trace;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::EndpointInfo;
    use crate::schema::{ActorManifest, SchemaKind};
    use crate::types::ActorKind;
    use std::sync::Mutex;

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
        let json = serde_json::json!({ "qty": 2 });
        ctx.0.send(
            Address::Path(ActorPath::new("inventory.west")),
            SchemaId::new("ReserveStock", 1),
            json,
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
            Intent::Subscribe { .. } => panic!("expected a send"),
        }
    }

    #[test]
    fn reply_targets_the_requester_only_when_reply_to_exists() {
        // Given a context whose message carries a reply-to path.
        let mut view = Mutex::new(FakeView::at_millis(0));
        let view = view.get_mut().expect("locked");
        let trace = TraceCtx::root();
        let mut outbox = Outbox::new();
        let path = ActorPath::new("server");
        let reply_to = Address::Path(ActorPath::new("client"));
        let mut ctx = CmdCtx::new(&path, &trace, Some(&reply_to), view, &mut outbox);

        // When replying.
        ctx.0.reply(
            SchemaId::new("Reserved", 1),
            serde_json::json!({ "ok": true }),
        );

        // Then one reply intent targets the client.
        let drained: Vec<_> = outbox.drain().collect();
        match &drained[0] {
            Intent::Send(_) => panic!("expected a reply intent"),
            Intent::Publish { .. } => panic!("expected a reply intent"),
            Intent::Reply { to, .. } => {
                assert_eq!(*to, Address::Path(ActorPath::new("client")));
            }
            Intent::Subscribe { .. } => panic!("expected a reply intent"),
        }

        // When a context without reply-to replies.
        let mut silent_outbox = Outbox::new();
        let mut silent = CmdCtx::new(&path, &trace, None, view, &mut silent_outbox);
        silent
            .0
            .reply(SchemaId::new("Reserved", 1), serde_json::json!({}));

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
        ctx.0.publish(
            Topic::new("inventory.events"),
            SchemaId::new("StockReserved", 1),
            serde_json::json!({ "qty": 2 }),
        );

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
        let info = ctx.0.lookup(&ActorPath::new("auditor"));
        let missing = ctx.0.lookup(&ActorPath::new("ghost"));
        let handlers = ctx.0.who_handles(&SchemaId::new("ReserveStock", 1));
        let ts = ctx.0.recv_ts();

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
        ctx.0.send(
            Address::Path(ActorPath::new("b")),
            SchemaId::new("Ping", 1),
            serde_json::json!({}),
            None,
        );
        ctx.0.send(
            Address::Path(ActorPath::new("c")),
            SchemaId::new("Pong", 1),
            serde_json::json!({}),
            None,
        );

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
        ctx.core.send(
            Address::Path(ActorPath::new("b")),
            SchemaId::new("Ping", 1),
            serde_json::json!({}),
            None,
        );

        // Then the effect is deferred identically.
        assert_eq!(outbox.len(), 1);
        assert!(matches!(SchemaKind::Command, SchemaKind::Command));
    }
}
