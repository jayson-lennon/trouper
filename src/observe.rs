//! Opt-in runtime observation.
//!
//! An [`Observation`] is one observed runtime boundary crossing (send,
//! deliver, ack, spawn, stop, ask, fail, escalate, dead-letter,
//! backpressure, catch-up). Observations are not a delivery path —
//! envelopes never travel through them — and they are not retained:
//! with no handler installed nothing is constructed at all, and an
//! observation nobody captures is gone. The dead-letter queue is the
//! only after-the-fact artifact the runtime keeps on its own.
//!
//! Install a handler with [`crate::ActorSystem::set_observation`] (or at
//! config time); remove it with
//! [`crate::ActorSystem::clear_observation`]. The handler is invoked
//! synchronously at the emission site.
//!
//! # Handler rules
//!
//! - **Handlers must not call back into the system** (tell, ask, spawn…):
//!   the call happens on the emitting path, so re-entry deadlocks or
//!   re-enters observation. Fan the observation out into your own channel
//!   and process it elsewhere.
//! - **Keep handlers fast.** Observation cost is paid by the message path.
//! - A panicking handler is isolated: the message path continues.

use std::sync::Arc;

use crate::json;
use crate::json::Json;

use crate::actor::{ActorKind, ActorPath, StopReason};
use crate::envelope::{Address, TraceCtx};
use crate::journal::SeqNo;
use crate::kernel::AskOutcome;
use crate::kernel::DeadLetterReason;
use crate::schema::SchemaId;

/// A user-installed observation handler.
///
/// See the [module docs](self) for the handler rules (no system re-entry,
/// keep it fast). Panics inside the handler are isolated from the message
/// path.
pub type ObservationHandler = Arc<dyn Fn(&Observation) + Send + Sync>;

/// One observed runtime event.
#[derive(Debug, Clone)]
pub struct Observation {
    /// Wall clock at emission (injected clock = deterministic tests).
    pub ts: crate::clock::Timestamp,
    /// What happened.
    pub kind: ObservationKind,
}

impl Observation {
    /// Creates an observation of `kind` at `ts`.
    pub fn new(ts: crate::clock::Timestamp, kind: ObservationKind) -> Self {
        Self { ts, kind }
    }
}

/// The observation variants, one variant per observable runtime event.
#[derive(Debug, Clone)]
pub enum ObservationKind {
    /// An envelope was routed to a destination.
    Sent {
        from: Option<ActorPath>,
        dest: Address,
        schema: SchemaId,
        trace: TraceCtx,
    },
    /// A loop picked an envelope out of its inbox.
    Delivered {
        to: ActorPath,
        schema: SchemaId,
        trace: TraceCtx,
    },
    /// An ES actor durably committed a message (journal + ack).
    Acked {
        to: ActorPath,
        schema: SchemaId,
        trace: TraceCtx,
    },
    /// A service actor opened an ask.
    AskOpened {
        from: ActorPath,
        dest: Address,
        trace: TraceCtx,
    },
    /// An ask settled.
    AskSettled {
        outcome: AskOutcome,
        trace: TraceCtx,
    },
    /// An actor was spawned (fresh or restarted).
    Spawned {
        path: ActorPath,
        kind: ActorKind,
        restart: bool,
    },
    /// An actor stopped gracefully.
    Stopped { path: ActorPath, reason: StopReason },
    /// A handler failed (panic or dispatch error).
    Failed { path: ActorPath, error: String },
    /// An ES actor took a snapshot at a journal seq.
    SnapshotTaken { path: ActorPath, seq: SeqNo },
    /// A restart budget was exhausted; escalation to the parent.
    Escalated { path: ActorPath, reason: String },
    /// A parent was notified that its linked child stopped/removed.
    LinkNotified {
        /// The parent that was (would have been) notified.
        parent: ActorPath,
        /// The child whose stop triggered the notification.
        child: ActorPath,
    },
    /// An envelope was dead-lettered.
    DeadLettered {
        dest: Address,
        schema: SchemaId,
        reason: DeadLetterReason,
        trace: TraceCtx,
    },
    /// An actor's inbox depth crossed its configured high watermark.
    /// Fires once per crossing (down-crossings re-arm it), never per
    /// message — sustained overload stays observable without flooding.
    Backpressured { path: ActorPath, depth: u64 },
    /// A projector finished its catch-up (history folded; the live tail
    /// is now authoritative). `seeded` counts the messages the scan filled
    /// in (0 = the journal already covered everything).
    CaughtUp { path: ActorPath, seeded: u64 },
}

impl Observation {
    /// The JSON projection (the observation boundary — serialization
    /// lives here and nowhere else).
    pub fn to_json(&self) -> Json {
        let base = json!({
            "ts": self.ts.as_millis(),
        });
        let mut value = match &self.kind {
            ObservationKind::Sent {
                from,
                dest,
                schema,
                trace,
            } => json!({
                "kind": "sent",
                "from": from.as_ref().map(|p| p.to_string()),
                "dest": dest.to_string(),
                "schema": schema.to_string(),
                "trace_id": trace.trace_id.to_string(),
                "causality_id": trace.causality_id.to_string(),
            }),
            ObservationKind::Delivered { to, schema, trace } => json!({
                "kind": "delivered",
                "to": to.to_string(),
                "schema": schema.to_string(),
                "trace_id": trace.trace_id.to_string(),
                "causality_id": trace.causality_id.to_string(),
            }),
            ObservationKind::Acked { to, schema, trace } => json!({
                "kind": "acked",
                "to": to.to_string(),
                "schema": schema.to_string(),
                "trace_id": trace.trace_id.to_string(),
                "causality_id": trace.causality_id.to_string(),
            }),
            ObservationKind::AskOpened { from, dest, trace } => json!({
                "kind": "ask_opened",
                "from": from.to_string(),
                "dest": dest.to_string(),
                "trace_id": trace.trace_id.to_string(),
                "causality_id": trace.causality_id.to_string(),
            }),
            ObservationKind::AskSettled { outcome, trace } => json!({
                "kind": "ask_settled",
                "outcome": outcome,
                "trace_id": trace.trace_id.to_string(),
                "causality_id": trace.causality_id.to_string(),
            }),
            ObservationKind::Spawned {
                path,
                kind,
                restart,
            } => json!({
                "kind": "spawned",
                "path": path.to_string(),
                "actor_kind": kind,
                "restart": restart,
            }),
            ObservationKind::Stopped { path, reason } => json!({
                "kind": "stopped",
                "path": path.to_string(),
                "reason": reason,
            }),
            ObservationKind::Failed { path, error } => json!({
                "kind": "failed",
                "path": path.to_string(),
                "error": error,
            }),
            ObservationKind::SnapshotTaken { path, seq } => json!({
                "kind": "snapshot_taken",
                "path": path.to_string(),
                "seq": seq.as_u64(),
            }),
            ObservationKind::Escalated { path, reason } => json!({
                "kind": "escalated",
                "path": path.to_string(),
                "reason": reason,
            }),
            ObservationKind::LinkNotified { parent, child } => json!({
                "kind": "link_notified",
                "parent": parent.to_string(),
                "child": child.to_string(),
            }),
            ObservationKind::DeadLettered {
                dest,
                schema,
                reason,
                trace,
            } => json!({
                "kind": "dead_lettered",
                "dest": dest.to_string(),
                "schema": schema.to_string(),
                "reason": reason,
                "trace_id": trace.trace_id.to_string(),
                "causality_id": trace.causality_id.to_string(),
            }),
            ObservationKind::Backpressured { path, depth } => json!({
                "kind": "backpressured",
                "path": path.to_string(),
                "depth": depth,
            }),
            ObservationKind::CaughtUp { path, seeded } => json!({
                "kind": "caught_up",
                "path": path.to_string(),
                "seeded": seeded,
            }),
        };
        let object = value
            .as_object_mut()
            .expect("observation json is an object");
        for (key, val) in base.as_object().expect("base json is an object") {
            object.insert(key.clone(), val.clone());
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Timestamp;
    use crate::envelope::TraceCtx;

    #[test]
    fn observations_project_to_json_with_trace_context_only_at_the_boundary() {
        // Given an acked observation carrying a trace.
        let observation = Observation::new(
            Timestamp::from_millis(42),
            ObservationKind::Acked {
                to: ActorPath::new("counter"),
                schema: SchemaId::new("Add"),
                trace: TraceCtx::root(),
            },
        );

        // When projecting to JSON.
        let value = observation.to_json();

        // Then the projection carries the kind, ts, and trace ids.
        assert_eq!(value["kind"], "acked");
        assert_eq!(value["ts"], 42);
        assert_eq!(value["to"], "counter");
        assert_eq!(value["schema"], "Add");
        assert!(value["trace_id"].is_string());
        assert!(value["causality_id"].is_string());
    }
}

/// A capturing observation log for TESTS.
///
/// The test-build twin of a user handler: `test()` systems install it so
/// existing assertions can read the observation flow (`facts()`), exactly
/// as the removed ring used to serve them. Production builds never create
/// one — observation there is strictly the host's handler, and the log
/// has no prod role (uncaptured observations are gone).
/// `pub(crate)` (not `#[cfg(test)]`): downstream crates' test suites call
/// `ActorSystem::test`, so the log must exist in release builds too.
#[derive(Clone, Default)]
pub(crate) struct ObservationLog(Arc<parking_lot::Mutex<Vec<Observation>>>);

#[cfg_attr(not(test), allow(dead_code))] // consumed by cfg(test) suites
impl ObservationLog {
    /// The handler that feeds the log (install with
    /// `ActorSystem::set_observation`).
    pub(crate) fn handler(&self) -> ObservationHandler {
        let entries = self.0.clone();
        Arc::new(move |observation| entries.lock().push(observation.clone()))
    }

    /// Everything captured so far, in emission order.
    pub(crate) fn snapshot(&self) -> Vec<Observation> {
        self.0.lock().clone()
    }

    /// A compact census of observation kinds (`{Delivered: 3, ...}`).
    pub(crate) fn kind_counts(&self) -> std::collections::HashMap<String, usize> {
        let mut counts = std::collections::HashMap::new();
        for observation in self.snapshot() {
            let kind = format!("{:?}", observation.kind);
            let name = kind.split(['(', '{']).next().unwrap_or(&kind).trim();
            *counts.entry(name.to_owned()).or_default() += 1;
        }
        counts
    }
}
