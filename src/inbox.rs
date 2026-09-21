//! Per-actor delivery: bounded inboxes with offset-based peek/ack.
//!
//! Delivery never rides the tap: the tap may drop facts under pressure;
//! delivery must not. Each actor has one inbox; the runtime peeks at its
//! cursor, processes, and acks — the offset survives actor restarts, which
//! is what makes redelivery possible.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::envelope::Envelope;

/// What an inbox does when it is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverloadPolicy {
    /// The sender waits until there is room (backpressure). The default.
    #[default]
    Block,
    /// The incoming message is dropped.
    DropNew,
    /// The oldest queued message is dropped to make room.
    DropOld,
}

/// Errors surfaced by an inbox.
#[derive(Debug, wherror::Error)]
#[error(debug)]
pub enum InboxError {
    /// The inbox is at capacity and the policy declined to make room.
    Full,
    /// The inbox is closed to new deliveries (draining for shutdown).
    Closed,
}

/// A refused push, carrying the envelope back to the caller.
///
/// The runtime turns every variant into a `DeadLettered` fact: dropped
/// messages must stay observable, never silently vanish.
#[derive(Debug, wherror::Error)]
#[error(debug)]
pub enum Refused {
    /// The inbox is full under Block/DropNew.
    Full(Envelope),
    /// The inbox accepted the new envelope but evicted this older one.
    Evicted(Envelope),
    /// The inbox is closed (shutdown drain).
    Closed(Envelope),
}

impl Refused {
    /// The refused envelope itself.
    pub fn into_envelope(self) -> Envelope {
        match self {
            Self::Full(envelope) | Self::Evicted(envelope) | Self::Closed(envelope) => envelope,
        }
    }

    /// Whether the new envelope was still queued despite the refusal
    /// (true only for [`Refused::Evicted`]).
    pub fn queued_anyway(&self) -> bool {
        matches!(self, Self::Evicted(_))
    }
}

/// A bounded FIFO inbox holding envelopes under a monotonic offset space.
///
/// Offsets are never reused and survive restarts: acking is the runtime's
/// record that a message was durably processed (journalled), so the offset
/// cursor is what redelivery resumes from.
#[derive(Debug)]
pub struct Inbox {
    capacity: usize,
    policy: OverloadPolicy,
    /// Offset of the next envelope to be enqueued.
    next_offset: u64,
    /// Offset of the next un-acked envelope (the peek cursor).
    cursor: u64,
    /// Queue of (offset, envelope) waiting to be peeked/acked.
    queue: VecDeque<(u64, Envelope)>,
    open: bool,
}

impl Inbox {
    /// Creates an inbox with a capacity and overload policy.
    pub fn new(capacity: usize, policy: OverloadPolicy) -> Self {
        Self {
            capacity: capacity.max(1),
            policy,
            next_offset: 0,
            cursor: 0,
            queue: VecDeque::new(),
            open: true,
        }
    }

    /// Attempts to enqueue an envelope, applying the overload policy.
    ///
    /// * `Block`  → returns `Err(Full(envelope))` and the caller retries
    ///   (the runtime's front door is an mpsc whose `.send().await` is the
    ///   block).
    /// * `DropNew` → the envelope is returned refused.
    /// * `DropOld` → the oldest queued envelope is evicted and returned
    ///   (the runtime dead-letters it), and the new envelope is queued.
    ///
    /// # Errors
    ///
    /// Returns the offset assigned to the envelope, or the refusal reason
    /// with the envelope back by value ([`Refused::Full`] under Block/
    /// DropNew when full, [`Refused::Closed`] once draining, and
    /// [`Refused::Evicted`] with the oldest envelope under DropOld).
    // The refused envelope travels back by value on purpose: the kernel
    // dead-letters exactly what was refused (allowed workspace-wide in
    // Cargo.toml).
    pub fn push(&mut self, envelope: Envelope) -> Result<InboxOffset, Refused> {
        if !self.open {
            return Err(Refused::Closed(envelope));
        }
        if self.queue.len() >= self.capacity {
            match self.policy {
                OverloadPolicy::Block | OverloadPolicy::DropNew => {
                    return Err(Refused::Full(envelope));
                }
                OverloadPolicy::DropOld => {
                    // Evicting the front consumes its offset forever: advance
                    // the cursor so peek/ack stay aligned with the new front.
                    if let Some((_, evicted)) = self.queue.pop_front() {
                        self.cursor += 1;
                        self.queue_next(envelope);
                        return Err(Refused::Evicted(evicted));
                    }
                }
            }
        }
        self.queue_next(envelope);
        Ok(InboxOffset::new(self.next_offset - 1))
    }

    /// Assigns the next offset and queues the envelope (single push path).
    fn queue_next(&mut self, envelope: Envelope) {
        let offset = self.next_offset;
        self.next_offset += 1;
        self.queue.push_back((offset, envelope));
    }

    /// Closes the inbox: no new deliveries, existing entries stay readable.
    pub fn close(&mut self) {
        self.open = false;
    }

    /// Drains one queued entry for shutdown-flush purposes (the cursor
    /// advances: the entry is leaving the system via the DLQ).
    pub fn pop_discard(&mut self) -> Option<Envelope> {
        let (_offset, envelope) = self.queue.pop_front()?;
        self.cursor += 1;
        Some(envelope)
    }

    /// Reopens a closed inbox (restart: redelivery resumes from the
    /// cursor — queued entries were never dropped).
    pub fn reopen(&mut self) {
        self.open = true;
    }

    /// Whether the inbox accepts new deliveries.
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// The cursor's current offset — the next entry the runtime will see.
    pub fn cursor(&self) -> InboxOffset {
        InboxOffset::new(self.cursor)
    }

    /// Peeks the envelope at the cursor, if one is ready.
    pub fn peek(&mut self) -> Option<&Envelope> {
        let cursor = self.cursor;
        self.queue
            .front()
            .filter(|(offset, _)| *offset == cursor)
            .map(|(_, envelope)| envelope)
    }

    /// Acks the envelope at the cursor, advancing it.
    ///
    /// If the cursor's entry was evicted (`DropOld`), this just advances past
    /// the hole; either way the cursor never skips a live entry un-acked.
    ///
    /// # Panics
    ///
    /// Panics if the cursor is already past every entry ever enqueued — a
    /// runtime bug, since only the runtime peeks and acks.
    pub fn ack(&mut self) -> InboxOffset {
        if self.cursor >= self.next_offset {
            panic!("inbox acked past the write head at cursor {self:?}");
        }
        if self
            .queue
            .front()
            .is_some_and(|(offset, _)| *offset == self.cursor)
        {
            self.queue.pop_front();
        }
        self.cursor += 1;
        InboxOffset::new(self.cursor)
    }

    /// Drains every un-acked entry (the runtime uses this on shutdown/DLQ flush).
    pub fn drain(&mut self) -> impl Iterator<Item = (InboxOffset, Envelope)> + '_ {
        self.queue
            .drain(..)
            .map(|(offset, envelope)| (InboxOffset::new(offset), envelope))
    }

    /// The number of entries waiting between the cursor and the write head.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Whether no entries are waiting.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

/// Position within one actor's inbox; independent of the journal's [`SeqNo`](crate::journal::SeqNo).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InboxOffset(u64);

impl InboxOffset {
    /// The offset of the next never-delivered inbox entry.
    pub fn zero() -> Self {
        Self(0)
    }

    /// Wraps a raw offset value.
    pub fn new(v: u64) -> Self {
        Self(v)
    }

    /// The raw offset value.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for InboxOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorPath;
    use crate::envelope::Envelope;
    use crate::envelope::TraceCtx;
    use crate::schema::SchemaId;
    use serde_json::json;

    fn envelope(n: u32) -> Envelope {
        Envelope::json(
            SchemaId::new("Ping", 1),
            crate::envelope::Address::Path(ActorPath::new("a")),
            json!({ "n": n }),
            TraceCtx::root(),
        )
    }

    #[test]
    fn push_then_peek_returns_envelopes_in_fifo_order() {
        // Given an inbox with two envelopes pushed.
        let mut inbox = Inbox::new(4, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");

        // When peeking, dropping the borrow, then peeking again.
        let first_n = inbox.peek().map(|e| e.as_json().map(|j| j["n"].as_u64()));
        let again_n = inbox.peek().map(|e| e.as_json().map(|j| j["n"].as_u64()));

        // Then the same first envelope is returned both times.
        assert_eq!(first_n, Some(Some(Some(1))));
        assert_eq!(again_n, first_n);
    }

    #[test]
    fn ack_advances_the_cursor_and_exposes_the_next_entry() {
        // Given an inbox holding two envelopes.
        let mut inbox = Inbox::new(4, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");

        // When acking the first entry.
        inbox.ack();

        // Then the cursor moved and the second envelope is at the front.
        assert_eq!(inbox.cursor(), InboxOffset::new(1));
        let front = inbox.peek().expect("front");
        assert_eq!(front.as_json().map(|j| j["n"].as_u64()), Some(Some(2)));
    }

    #[test]
    fn offsets_survive_drain_and_are_monotonic() {
        // Given an inbox that accepted three envelopes.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        let offsets: Vec<_> = (0..3)
            .map(|n| inbox.push(envelope(n)).expect("push"))
            .collect();

        // When draining it.
        let drained: Vec<_> = inbox.drain().collect();

        // Then offsets are monotonic and identical to the push results.
        assert_eq!(
            drained.iter().map(|(o, _)| o.as_u64()).collect::<Vec<_>>(),
            offsets.iter().map(|o| o.as_u64()).collect::<Vec<_>>()
        );
        assert!(inbox.is_empty());

        // And the next push continues the offset space (redelivery-safe).
        let next = inbox.push(envelope(9)).expect("push");
        assert!(next.as_u64() > drained[2].0.as_u64());
    }

    #[test]
    fn drop_new_policy_refuses_when_full() {
        // Given a capacity-2 inbox under DropNew that is already full.
        let mut inbox = Inbox::new(2, OverloadPolicy::DropNew);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");

        // When pushing a third envelope.
        let result = inbox.push(envelope(3));

        // Then it is refused (envelope returned) and the queue is untouched.
        let refused = result.expect_err("must refuse").into_envelope();
        assert_eq!(refused.as_json().map(|j| j["n"].as_u64()), Some(Some(3)));
        assert_eq!(inbox.len(), 2);
    }

    #[test]
    fn drop_old_policy_evicts_the_oldest_entry() {
        // Given a capacity-2 inbox under DropOld that is already full.
        let mut inbox = Inbox::new(2, OverloadPolicy::DropOld);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");

        // When pushing a third envelope (evicting the oldest).
        let evicted = inbox
            .push(envelope(3))
            .expect_err("eviction is reported")
            .into_envelope();
        assert_eq!(evicted.as_json().map(|j| j["n"].as_u64()), Some(Some(1)));

        // Then the evicted envelope is gone: the cursor already sits on
        // envelope 2, which is what a peek returns.
        let front_n = inbox
            .peek()
            .and_then(|e| e.as_json().map(|j| j["n"].as_u64()));
        assert_eq!(front_n, Some(Some(2)));

        // And after acking, envelope 3 follows in order.
        inbox.ack();
        let next_n = inbox
            .peek()
            .and_then(|e| e.as_json().map(|j| j["n"].as_u64()));
        assert_eq!(next_n, Some(Some(3)));
    }

    #[test]
    fn block_policy_signals_full_for_the_caller_to_retry() {
        // Given a capacity-1 inbox under the default Block policy.
        let mut inbox = Inbox::new(1, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");

        // When pushing while full.
        let result = inbox.push(envelope(2));

        // Then the caller is told to back off (kernel's mpsc send blocks).
        assert!(result.is_err());
        assert_eq!(inbox.policy, OverloadPolicy::Block);
    }

    #[test]
    fn closed_inbox_refuses_new_deliveries_but_keeps_readables() {
        // Given an inbox holding one envelope that is then closed.
        let mut inbox = Inbox::new(4, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");
        inbox.close();

        // When pushing another and peeking.
        let push = inbox.push(envelope(2));
        let readable = inbox.peek().is_some();

        // Then the push is refused but the held entry stays readable.
        assert!(matches!(push, Err(Refused::Closed(_))));
        assert!(readable);
        assert!(!inbox.is_open());
    }
}

#[test]
fn inbox_offset_orders_numerically() {
    // Given two inbox offsets.
    let earlier = InboxOffset::zero();
    let later = InboxOffset::new(3);

    // When comparing them.
    let ordered = earlier < later;

    // Then ordering follows the numeric value.
    assert!(ordered);
}
