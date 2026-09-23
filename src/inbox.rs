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

    /// Drains up to `n` un-acked entries, oldest first, in FIFO order.
    ///
    /// The CURSOR DOES NOT MOVE: draining takes the entries out of the
    /// queue without committing them. The caller commits the whole drained
    /// run with [`Inbox::commit_through`] (the batch commit point), or
    /// hands a suffix back with [`Inbox::requeue`] (the service panic
    /// path). A drained-but-uncommitted entry is neither acked nor
    /// redeliverable until one of those runs — only the actor's own loop
    /// holds the inbox across a step, so nothing can observe the gap.
    ///
    /// Fewer than `n` entries drain when the queue is shorter (trickle
    /// loads drain exactly what is queued; batch 1 collapses to peek+ack).
    pub fn drain_up_to(&mut self, n: usize) -> Vec<(InboxOffset, Envelope)> {
        let take = n.min(self.queue.len());
        self.queue
            .drain(..take)
            .map(|(offset, envelope)| (InboxOffset::new(offset), envelope))
            .collect()
    }

    /// Atomically advances the cursor past `offset` — the batch commit.
    ///
    /// Pops every queued entry at or before `offset` (drained entries were
    /// already removed by [`Inbox::drain_up_to`]; this catches the
    /// `DropOld` holes the cursor jumps on the push path) and sets the
    /// cursor to `offset + 1`. One call commits the whole batch: there is
    /// no partially-advanced state between the old cursor and the new one.
    ///
    /// # Panics
    ///
    /// Panics under the same contract as [`Inbox::ack`]: `offset` past the
    /// write head is a runtime bug (only the runtime drains and commits).
    pub fn commit_through(&mut self, offset: InboxOffset) {
        let offset = offset.as_u64();
        if offset >= self.next_offset {
            panic!("inbox committed past the write head at {offset}");
        }
        // A batch commit never rewinds: `offset` is the last entry of a
        // drained run, so it is at or after the cursor by construction
        // (drain_up_to only hands out entries at/after the cursor).
        while self
            .queue
            .front()
            .is_some_and(|(front, _)| *front <= offset)
        {
            self.queue.pop_front();
        }
        self.cursor = offset + 1;
    }

    /// Hands a drained suffix BACK to the queue, in order, at its ORIGINAL
    /// offsets, and rewinds the cursor to just past `last_committed` —
    /// the service panic path's undo of an uncommitted drain tail.
    ///
    /// After the call, [`Inbox::peek`] returns exactly the first requeued
    /// entry (redelivery resumes at it), and every offset before it is
    /// committed. Offsets are never rewritten: the inbox's offset space
    /// stays monotonic across the round trip.
    ///
    /// # Panics
    ///
    /// Panics if any returned offset is at or before `last_committed` (a
    /// runtime bug: the step would requeue an entry it already committed).
    pub fn requeue(
        &mut self,
        entries: impl IntoIterator<Item = (InboxOffset, Envelope)>,
        last_committed: InboxOffset,
    ) {
        let last_committed = last_committed.as_u64();
        let mut restored: VecDeque<(u64, Envelope)> = entries
            .into_iter()
            .map(|(offset, envelope)| {
                assert!(
                    offset.as_u64() > last_committed,
                    "requeued offset {} at/below the commit point {last_committed}",
                    offset.as_u64()
                );
                (offset.as_u64(), envelope)
            })
            .collect();
        if restored.is_empty() {
            return;
        }
        // The requeued run was drained from the queue FRONT, so it is
        // contiguous with whatever is queued behind it: restored entries
        // go before the existing queue, oldest first.
        for (offset, envelope) in restored.drain(..).rev() {
            self.queue.push_front((offset, envelope));
        }
        self.cursor = last_committed + 1;
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
    use crate::envelope::PayloadBytes;
    use crate::envelope::TraceCtx;
    use crate::json::Json;
    use crate::schema::SchemaId;
    use serde_json::json;

    fn envelope(n: u32) -> Envelope {
        Envelope::from_bytes(
            SchemaId::new("Ping"),
            crate::envelope::Address::Path(ActorPath::new("a")),
            PayloadBytes::from(Json::from(json!({ "n": n }))),
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
        let first_n = inbox.peek().map(|e| e.payload_json()["n"].as_u64());
        let again_n = inbox.peek().map(|e| e.payload_json()["n"].as_u64());

        // Then the same first envelope is returned both times.
        assert_eq!(first_n, Some(Some(1)));
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
        assert_eq!(front.payload_json()["n"].as_u64(), Some(2));
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
        assert_eq!(refused.payload_json()["n"].as_u64(), Some(3));
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
        assert_eq!(evicted.payload_json()["n"].as_u64(), Some(1));

        // Then the evicted envelope is gone: the cursor already sits on
        // envelope 2, which is what a peek returns.
        let front_n = inbox.peek().map(|e| e.payload_json()["n"].as_u64());
        assert_eq!(front_n, Some(Some(2)));

        // And after acking, envelope 3 follows in order.
        inbox.ack();
        let next_n = inbox.peek().map(|e| e.payload_json()["n"].as_u64());
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

    #[test]
    fn drain_up_to_returns_fifo_entries_without_moving_the_cursor() {
        // Given an inbox holding three envelopes.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        for n in 1..=3 {
            inbox.push(envelope(n)).expect("push");
        }

        // When draining two.
        let drained = inbox.drain_up_to(2);

        // Then the drained pairs are FIFO with their push offsets, and the
        // cursor did NOT move (nothing is committed yet).
        assert_eq!(
            drained.iter().map(|(o, _)| o.as_u64()).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(drained[0].1.payload_json()["n"].as_u64(), Some(1));
        assert_eq!(drained[1].1.payload_json()["n"].as_u64(), Some(2));
        assert_eq!(inbox.cursor(), InboxOffset::new(0));
        assert_eq!(inbox.len(), 1, "only envelope 3 stays queued");
    }

    #[test]
    fn drain_up_to_takes_what_is_queued_when_shorter_than_n() {
        // Given an inbox holding two envelopes and a drain budget of five.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");

        // When draining up to five.
        let drained = inbox.drain_up_to(5);

        // Then exactly the two queued entries drain (trickle behavior).
        assert_eq!(drained.len(), 2);
        assert!(inbox.is_empty());

        // And a drain on an empty inbox yields nothing.
        assert!(inbox.drain_up_to(5).is_empty());
    }

    #[test]
    fn commit_through_advances_the_cursor_atomically() {
        // Given an inbox whose first two entries are drained.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        for n in 1..=3 {
            inbox.push(envelope(n)).expect("push");
        }
        let drained = inbox.drain_up_to(2);

        // When committing through the last drained offset.
        let last = drained.last().expect("non-empty").0;
        inbox.commit_through(last);

        // Then the cursor sits exactly past the batch and the third
        // envelope is what a peek returns.
        assert_eq!(inbox.cursor(), InboxOffset::new(2));
        let front = inbox.peek().expect("front");
        assert_eq!(front.payload_json()["n"].as_u64(), Some(3));
    }

    #[test]
    fn commit_through_pops_live_entries_it_jumps_over() {
        // Given an inbox where entries were pushed AFTER a partial drain
        // (a batch committed while more mail queued behind it).
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        for n in 1..=2 {
            inbox.push(envelope(n)).expect("push");
        }
        let drained = inbox.drain_up_to(1);
        inbox.push(envelope(3)).expect("push");

        // When committing through the single drained entry.
        inbox.commit_through(drained[0].0);

        // Then the cursor moved past it and the queue still holds the
        // later entries in order (nothing live was dropped).
        assert_eq!(inbox.cursor(), InboxOffset::new(1));
        assert_eq!(inbox.len(), 2);
        let front = inbox.peek().expect("front");
        assert_eq!(front.payload_json()["n"].as_u64(), Some(2));
    }

    #[test]
    #[should_panic(expected = "committed past the write head")]
    fn commit_through_past_the_write_head_panics() {
        // Given an inbox holding one entry.
        let mut inbox = Inbox::new(4, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");

        // When committing through an offset the inbox never assigned.
        inbox.commit_through(InboxOffset::new(9));

        // Then it panics: the guard matches ack's write-head contract.
    }

    #[test]
    fn requeue_round_trips_a_drained_suffix_with_original_offsets() {
        // Given an inbox whose three entries were drained.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        for n in 1..=3 {
            inbox.push(envelope(n)).expect("push");
        }
        let drained = inbox.drain_up_to(3);
        assert_eq!(inbox.len(), 0);

        // When the first entry's effects commit and the suffix comes back
        // (the panic path: dispatch died on entry 2).
        inbox.commit_through(drained[0].0);
        inbox.requeue(drained[1..].to_vec(), drained[0].0);

        // Then the queue holds the suffix again, at its ORIGINAL offsets,
        // and peek returns exactly entry 2 (redelivery resumes there).
        assert_eq!(inbox.cursor(), InboxOffset::new(1));
        assert_eq!(inbox.len(), 2);
        let front = inbox.peek().expect("front");
        assert_eq!(front.payload_json()["n"].as_u64(), Some(2));

        // And the offset space stays monotonic: the next push continues
        // past every offset ever assigned (3), never reuses one.
        let next = inbox.push(envelope(9)).expect("push");
        assert!(next.as_u64() > drained[2].0.as_u64());

        // And the requeued entries drain again in FIFO order (restart
        // replays the queue from the cursor).
        let again = inbox.drain_up_to(4);
        assert_eq!(
            again.iter().map(|(_, e)| e.payload_json()["n"].as_u64()).collect::<Vec<_>>(),
            vec![Some(2), Some(3), Some(9)]
        );
        assert_eq!(
            again.iter().map(|(o, _)| o.as_u64()).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn requeue_with_an_empty_suffix_is_a_no_op() {
        // Given an inbox whose single entry was drained and committed.
        let mut inbox = Inbox::new(4, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");
        let drained = inbox.drain_up_to(1);
        inbox.commit_through(drained[0].0);

        // When requeueing nothing.
        inbox.requeue(Vec::new(), drained[0].0);

        // Then the cursor stands and the inbox is empty (a panic on the
        // LAST batch entry requeues no tail).
        assert_eq!(inbox.cursor(), InboxOffset::new(1));
        assert!(inbox.is_empty());
    }

    #[test]
    #[should_panic(expected = "at/below the commit point")]
    fn requeue_of_an_already_committed_offset_panics() {
        // Given an inbox whose first entry was drained and committed.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");
        let drained = inbox.drain_up_to(2);
        inbox.commit_through(drained[0].0);

        // When the committed entry itself is handed back (a runtime bug).
        inbox.requeue(drained.clone(), drained[0].0);

        // Then the offset guard panics: requeue never rewinds a commit.
    }

    #[test]
    fn drop_old_holes_stay_committed_through_a_batch() {
        // Given a capacity-2 DropOld inbox that evicted its first entry.
        let mut inbox = Inbox::new(2, OverloadPolicy::DropOld);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");
        let evicted = inbox.push(envelope(3)).expect_err("eviction");
        assert!(evicted.queued_anyway(), "envelope 1 was evicted");

        // When draining the live pair and committing through the last.
        let drained = inbox.drain_up_to(4);
        assert_eq!(drained.len(), 2, "only envelopes 2 and 3 are live");
        assert_eq!(
            drained.iter().map(|(o, _)| o.as_u64()).collect::<Vec<_>>(),
            vec![1, 2],
            "offsets stay monotonic across the eviction hole"
        );
        let last = drained.last().expect("non-empty").0;
        inbox.commit_through(last);

        // Then the cursor jumped the hole AND the batch in one commit.
        assert_eq!(inbox.cursor(), InboxOffset::new(3));
        assert!(inbox.is_empty());
    }

    #[test]
    fn drain_still_flushes_a_closed_inbox_for_shutdown() {
        // Given a closed inbox holding two entries (the shutdown flush).
        let mut inbox = Inbox::new(4, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");
        inbox.close();

        // When draining everything.
        let drained: Vec<_> = inbox.drain().collect();

        // Then the entries come out FIFO with their offsets even though
        // the door is closed (shutdown DLQ flush path unchanged).
        assert_eq!(drained.len(), 2);
        assert_eq!(
            drained.iter().map(|(o, _)| o.as_u64()).collect::<Vec<_>>(),
            vec![0, 1]
        );
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
