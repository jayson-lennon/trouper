//! The tap: a global observation ring of facts.
//!
//! The tap is NOT a delivery path — envelopes never travel through it.
//! Every waist crossing (send, deliver, ack, spawn, stop, ask, publish,
//! fail, escalate, dead-letter) appends a [`Fact`] to a bounded ring that
//! drops the OLDEST entries under pressure. Subscribers hold an offset
//! into the ring and read forward; the ring's JSON projection exists only
//! at its boundary (`Fact::to_json`), never on the emit path.

use std::collections::VecDeque;

use serde_json::json;

use crate::envelope::{Address, TraceCtx};
use crate::kernel::AskOutcome;
use crate::types::{ActorKind, DeadLetterReason, Path, SchemaId, SeqNo, StopReason, Topic};

/// One observed runtime fact.
#[derive(Debug, Clone)]
pub struct Fact {
    /// Monotonic offset in the global tap ring.
    pub offset: u64,
    /// Wall clock at emission (injected clock = deterministic tests).
    pub ts: crate::types::Timestamp,
    /// What happened.
    pub kind: FactKind,
}

/// The fact variants, keyed to the spec's emit points.
#[derive(Debug, Clone)]
pub enum FactKind {
    /// An envelope was routed to a destination.
    Sent {
        from: Option<Path>,
        dest: Address,
        schema: SchemaId,
        trace: TraceCtx,
    },
    /// A loop picked an envelope out of its inbox.
    Delivered {
        to: Path,
        schema: SchemaId,
        trace: TraceCtx,
    },
    /// An ES actor durably committed a message (journal + ack).
    Acked {
        to: Path,
        schema: SchemaId,
        trace: TraceCtx,
    },
    /// A service actor opened an ask.
    AskOpened {
        from: Path,
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
        path: Path,
        kind: ActorKind,
        restart: bool,
    },
    /// An actor stopped gracefully.
    Stopped { path: Path, reason: StopReason },
    /// A handler failed (panic or dispatch error).
    Failed { path: Path, error: String },
    /// An envelope was published onto a topic.
    TopicPublished {
        topic: Topic,
        schema: SchemaId,
        trace: TraceCtx,
    },
    /// An ES actor took a snapshot at a journal seq.
    SnapshotTaken { path: Path, seq: SeqNo },
    /// A restart budget was exhausted; escalation to the parent.
    Escalated { path: Path, reason: String },
    /// An envelope was dead-lettered.
    DeadLettered {
        dest: Address,
        schema: SchemaId,
        reason: DeadLetterReason,
        trace: TraceCtx,
    },
}

impl Fact {
    /// The JSON projection (the tap boundary — serialization lives HERE
    /// and nowhere else).
    pub fn to_json(&self) -> serde_json::Value {
        let base = json!({
            "offset": self.offset,
            "ts": self.ts.as_millis(),
        });
        let mut value = match &self.kind {
            FactKind::Sent {
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
            FactKind::Delivered { to, schema, trace } => json!({
                "kind": "delivered",
                "to": to.to_string(),
                "schema": schema.to_string(),
                "trace_id": trace.trace_id.to_string(),
                "causality_id": trace.causality_id.to_string(),
            }),
            FactKind::Acked { to, schema, trace } => json!({
                "kind": "acked",
                "to": to.to_string(),
                "schema": schema.to_string(),
                "trace_id": trace.trace_id.to_string(),
                "causality_id": trace.causality_id.to_string(),
            }),
            FactKind::AskOpened { from, dest, trace } => json!({
                "kind": "ask_opened",
                "from": from.to_string(),
                "dest": dest.to_string(),
                "trace_id": trace.trace_id.to_string(),
                "causality_id": trace.causality_id.to_string(),
            }),
            FactKind::AskSettled { outcome, trace } => json!({
                "kind": "ask_settled",
                "outcome": outcome,
                "trace_id": trace.trace_id.to_string(),
                "causality_id": trace.causality_id.to_string(),
            }),
            FactKind::Spawned {
                path,
                kind,
                restart,
            } => json!({
                "kind": "spawned",
                "path": path.to_string(),
                "actor_kind": kind,
                "restart": restart,
            }),
            FactKind::Stopped { path, reason } => json!({
                "kind": "stopped",
                "path": path.to_string(),
                "reason": reason,
            }),
            FactKind::Failed { path, error } => json!({
                "kind": "failed",
                "path": path.to_string(),
                "error": error,
            }),
            FactKind::TopicPublished {
                topic,
                schema,
                trace,
            } => json!({
                "kind": "topic_published",
                "topic": topic.to_string(),
                "schema": schema.to_string(),
                "trace_id": trace.trace_id.to_string(),
                "causality_id": trace.causality_id.to_string(),
            }),
            FactKind::SnapshotTaken { path, seq } => json!({
                "kind": "snapshot_taken",
                "path": path.to_string(),
                "seq": seq.as_u64(),
            }),
            FactKind::Escalated { path, reason } => json!({
                "kind": "escalated",
                "path": path.to_string(),
                "reason": reason,
            }),
            FactKind::DeadLettered {
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
        };
        let object = value.as_object_mut().expect("fact json is an object");
        for (key, val) in base.as_object().expect("base json is an object") {
            object.insert(key.clone(), val.clone());
        }
        value
    }
}

/// The bounded observation ring (drop-oldest under pressure).
#[derive(Debug)]
pub struct TapRing {
    capacity: usize,
    entries: VecDeque<Fact>,
    /// The offset the NEXT fact gets (monotonic across evictions).
    next_offset: u64,
    /// The lowest retained offset.
    floor: u64,
}

impl TapRing {
    /// Creates a tap ring holding `capacity` facts.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: VecDeque::new(),
            next_offset: 0,
            floor: 0,
        }
    }

    /// Appends a fact; the oldest drops when full. Returns the offset.
    pub fn push(&mut self, ts: crate::types::Timestamp, kind: FactKind) -> u64 {
        let offset = self.next_offset;
        self.next_offset += 1;
        self.entries.push_back(Fact { offset, ts, kind });
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
            self.floor += 1;
        }
        offset
    }

    /// Subscribes from an offset (clamped to the retained floor).
    ///
    /// Returns a snapshot of everything from there.
    pub fn subscribe(&self, from: u64) -> (u64, Vec<Fact>) {
        let start = from.max(self.floor);
        let facts: Vec<Fact> = self
            .entries
            .iter()
            .filter(|f| f.offset >= start)
            .cloned()
            .collect();
        (start, facts)
    }

    /// The retained range: `[floor, next_offset)`.
    pub fn retained(&self) -> (u64, u64) {
        (self.floor, self.next_offset)
    }

    /// The number of retained facts.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing is retained.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::TraceCtx;
    use crate::types::Timestamp;

    fn ts(ms: u64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    fn spawned_fact(path: &str) -> FactKind {
        FactKind::Spawned {
            path: Path::new(path),
            kind: crate::types::ActorKind::EventSourced,
            restart: false,
        }
    }

    #[test]
    fn tap_ring_assigns_monotonic_offsets_and_drops_oldest() {
        // Given a tap ring of capacity two.
        let mut ring = TapRing::new(2);

        // When three facts are pushed.
        let a = ring.push(ts(0), spawned_fact("a"));
        let b = ring.push(ts(1), spawned_fact("b"));
        let c = ring.push(ts(2), spawned_fact("c"));

        // Then offsets are monotonic and only the newest two are retained.
        assert_eq!((a, b, c), (0, 1, 2));
        assert_eq!(ring.retained(), (1, 3));
        assert_eq!(ring.len(), 2);
    }

    #[test]
    fn subscribe_clamps_to_the_floor_and_returns_retained_facts() {
        // Given a ring that evicted its first entry.
        let mut ring = TapRing::new(2);
        ring.push(ts(0), spawned_fact("a"));
        ring.push(ts(1), spawned_fact("b"));
        ring.push(ts(2), spawned_fact("c"));

        // When subscribing from offset zero (evicted).
        let (start, facts) = ring.subscribe(0);

        // Then the subscription starts at the floor with the retained tail.
        assert_eq!(start, 1);
        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].offset, 1);
    }

    #[test]
    fn facts_project_to_json_with_trace_context_only_at_the_boundary() {
        // Given an acked fact carrying a trace.
        let mut ring = TapRing::new(8);
        ring.push(
            ts(42),
            FactKind::Acked {
                to: Path::new("counter"),
                schema: SchemaId::new("Add", 1),
                trace: TraceCtx::root(),
            },
        );

        // When projecting to JSON.
        let (_, facts) = ring.subscribe(0);
        let value = facts[0].to_json();

        // Then the projection carries the kind, ts, and trace ids.
        assert_eq!(value["kind"], "acked");
        assert_eq!(value["ts"], 42);
        assert_eq!(value["to"], "counter");
        assert_eq!(value["schema"], "Add@1");
        assert!(value["trace_id"].is_string());
        assert!(value["causality_id"].is_string());
    }

    #[test]
    fn tap_drop_oldest_never_loses_future_facts() {
        // Given a tiny ring flooded past capacity.
        let mut ring = TapRing::new(3);
        for i in 0..10 {
            ring.push(ts(i), spawned_fact("a"));
        }

        // When subscribing at the newest possible offset.
        let (start, facts) = ring.subscribe(7);

        // Then only offsets 7..10 are visible and the next push continues
        // the sequence.
        assert_eq!(start, 7);
        assert_eq!(facts.len(), 3);
        let next = ring.push(ts(99), spawned_fact("b"));
        assert_eq!(next, 10);
    }
}
