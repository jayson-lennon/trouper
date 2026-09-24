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
    /// No capacity: the inbox never refuses and grows without bound.
    ///
    /// There is no backpressure — a fast sender can grow the mailbox
    /// until memory runs out (Erlang's trade, made explicit). Nothing
    /// else changes: offsets, claims/restore, restart redelivery, and
    /// at-most-once commits are all capacity-blind. The queue grows on
    /// demand; nothing is pre-allocated. A declared high watermark still
    /// works as an observe-only signal (it fires `Backpressured`, never
    /// a hold).
    Unbounded,
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
///
/// The step loops CLAIM batches: [`Inbox::claim_up_to`] MOVES envelopes out
/// of their slots (dispatch needs owned envelopes — the handler future
/// borrows from them across an await, so the inbox guard can never stay
/// held) and leaves tombstones in place. A tombstone is an in-flight claim:
/// it still counts toward capacity (the message is inside the actor until
/// its commit point), it still holds its offset, and
/// [`Inbox::restore_claims`] puts un-committed envelopes back into their
/// exact slots on crash/stop. Nothing ever changes position — the old
/// snapshot-clone and its per-message refcount traffic are gone.
#[derive(Debug)]
pub struct Inbox {
    capacity: usize,
    policy: OverloadPolicy,
    /// Offset of the next envelope to be enqueued.
    next_offset: u64,
    /// Offset of the next un-acked envelope (the peek cursor).
    cursor: u64,
    /// Queue of (offset, envelope) waiting to be peeked/acked; `None` is
    /// a claim tombstone (the envelope is out in a step batch until it is
    /// committed or restored).
    queue: VecDeque<(u64, Option<Envelope>)>,
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
    /// * `Unbounded` → the capacity check never runs; the envelope is
    ///   always queued.
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
        // Under Unbounded the capacity is meaningless: the refusal check
        // (and every eviction arm behind it) is skipped entirely.
        if self.policy == OverloadPolicy::Unbounded {
            self.queue_next(envelope);
            return Ok(InboxOffset::new(self.next_offset - 1));
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
                        let evicted = evicted.expect("evicting a claim tombstone");
                        self.cursor += 1;
                        self.queue_next(envelope);
                        return Err(Refused::Evicted(evicted));
                    }
                }
                OverloadPolicy::Unbounded => unreachable!("early-returned above"),
            }
        }
        self.queue_next(envelope);
        Ok(InboxOffset::new(self.next_offset - 1))
    }

    /// Assigns the next offset and queues the envelope (single push path).
    fn queue_next(&mut self, envelope: Envelope) {
        let offset = self.next_offset;
        self.next_offset += 1;
        self.queue.push_back((offset, Some(envelope)));
    }

    /// Closes the inbox: no new deliveries, existing entries stay readable.
    pub fn close(&mut self) {
        self.open = false;
    }

    /// Drains every LIVE entry for shutdown-flush purposes (the DLQ), skipping
    /// claim tombstones — their envelopes are owned by live steps (possibly
    /// parked mid-dispatch), and the step's claim guard disposes of each
    /// (StoppedWithMail when the actor is stopping; restore otherwise).
    ///
    /// Tombstones stay queued in order; the cursor is untouched (the inbox
    /// is closed for good at this point — only the guards' disposition of
    /// their claims remains).
    pub fn drain_live(&mut self) -> Vec<Envelope> {
        let mut drained = Vec::new();
        let mut keep = VecDeque::new();
        while let Some((offset, slot)) = self.queue.pop_front() {
            match slot {
                Some(envelope) => drained.push(envelope),
                None => keep.push_back((offset, None)),
            }
        }
        self.queue = keep;
        drained
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
    ///
    /// # Panics
    ///
    /// Panics if the cursor's slot is a claim tombstone — commit removes
    /// tombstones synchronously with the cursor advance, so one at the
    /// cursor is a runtime bug.
    pub fn peek(&mut self) -> Option<&Envelope> {
        let cursor = self.cursor;
        self.queue
            .front()
            .filter(|(offset, _)| *offset == cursor)
            .map(|(_, slot)| slot.as_ref().expect("peek on a claim tombstone"))
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
    ///
    /// # Panics
    ///
    /// Panics if any slot is a claim tombstone (runs after the loop task is
    /// gone — no claimants exist).
    pub fn drain(&mut self) -> impl Iterator<Item = (InboxOffset, Envelope)> + '_ {
        self.queue.drain(..).map(|(offset, slot)| {
            (
                InboxOffset::new(offset),
                slot.expect("drain on a claim tombstone"),
            )
        })
    }

    /// Claims up to `n` un-acked entries, oldest first, in FIFO order,
    /// WITHOUT moving the cursor.
    ///
    /// This is the batch step's read: each claimed envelope MOVES out of
    /// its queue slot (leaving a tombstone) into the caller's reused batch
    /// buffer — the loop dispatches from the owned batch (the handler
    /// future borrows across an await, so the inbox guard cannot stay
    /// held) and commits each with [`Inbox::commit_through`] at the same
    /// point the single-message step acked (service: before dispatch; ES:
    /// after the journal append). A claim is in flight, not consumed:
    /// tombstones still count toward capacity, still hold their offsets,
    /// and [`Inbox::restore_claims`] puts un-committed envelopes back into
    /// their exact slots when the step ends without committing them.
    /// Nothing ever changes position, so a step killed mid-batch loses
    /// nothing and re-dispatches in original order.
    ///
    /// Fewer than `n` come back when the queue is shorter — a trickle load
    /// claims exactly what is queued.
    ///
    /// # Panics
    ///
    /// Panics if the front slot is a claim tombstone — commit removes
    /// tombstones synchronously with the cursor advance, so one at the
    /// claim front is a runtime bug.
    pub fn claim_up_to(&mut self, n: usize, batch: &mut Vec<(InboxOffset, Envelope)>) -> usize {
        let mut claimed = 0usize;
        for (offset, slot) in self.queue.iter_mut() {
            if claimed == n {
                break;
            }
            // Claims run from the cursor forward; a tombstone ahead of the
            // committed prefix cannot exist (commit pops them), and the
            // gate below keeps offsets contiguous from the cursor.
            if *offset < self.cursor {
                continue; // already-committed head (shouldn't linger, but skip)
            }
            if *offset != self.cursor + claimed as u64 {
                break; // gap (evicted hole or empty) — claim only the run
            }
            let envelope = slot
                .take()
                .expect("claim on a claim tombstone ahead of the committed prefix");
            batch.push((InboxOffset::new(*offset), envelope));
            claimed += 1;
        }
        claimed
    }

    /// Restores previously-claimed envelopes into their original slots.
    ///
    /// The step's exit path for un-committed claims: every `(offset,
    /// envelope)` moves back into the slot that claims vacated. Offsets
    /// are slot addresses, so restoration is exact — FIFO order across a
    /// crash is preserved even when a concurrent `push` landed during the
    /// claim window (that push took a LATER offset and sits behind the
    /// restored tail by construction).
    ///
    /// Returns whether any slot was restored — restoration returns
    /// capacity to the queue (the space-available signal for a parked
    /// Block hold).
    ///
    /// # Panics
    ///
    /// Panics if a slot is missing, is live (not a tombstone), or sits
    /// before the cursor — restoring an already-committed claim is a
    /// runtime bug.
    pub fn restore_claims(&mut self, batch: Vec<(InboxOffset, Envelope)>) -> bool {
        let restored_any = !batch.is_empty();
        for (offset, envelope) in batch {
            let offset = offset.as_u64();
            let slot = self
                .queue
                .iter_mut()
                .find(|(front, _)| *front == offset)
                .map(|(_, slot)| slot)
                .unwrap_or_else(|| {
                    panic!("restore: offset {offset} is not queued (committed or evicted?)")
                });
            if offset < self.cursor {
                panic!("restore: offset {offset} is behind the cursor (already committed)");
            }
            let prev = slot.replace(envelope);
            if prev.is_some() {
                panic!("restore: offset {offset} slot was live (double restore)");
            }
        }
        restored_any
    }

    /// Atomically advances the cursor past `offset` — the batch commit.
    ///
    /// Pops every queued entry at or before `offset` (a batch step
    /// commits message-by-message with ever-larger offsets; the final
    /// call pops the whole run), tombstones included, and sets the cursor
    /// to `offset + 1`. One call is one commit point: there is no
    /// partially-advanced state between the old cursor and the new one.
    ///
    /// Returns whether any entry was popped — the space-available signal
    /// for a parked Block hold (a commit freed at least one slot).
    ///
    /// # Panics
    ///
    /// Panics under the same contract as [`Inbox::ack`]: `offset` past the
    /// write head is a runtime bug (only the runtime peeks and commits).
    pub fn commit_through(&mut self, offset: InboxOffset) -> bool {
        let offset = offset.as_u64();
        if offset >= self.next_offset {
            panic!("inbox committed past the write head at {offset}");
        }
        // A batch commit never rewinds: `offset` is an entry the step has
        // fully processed, so it is at or after the cursor by construction
        // (claim_up_to only hands out entries at/after the cursor).
        let mut popped = false;
        while self
            .queue
            .front()
            .is_some_and(|(front, _)| *front <= offset)
        {
            self.queue.pop_front();
            popped = true;
        }
        self.cursor = offset + 1;
        popped
    }

    /// The number of entries waiting between the cursor and the write
    /// head, claims-in-flight included (a tombstone is an occupied slot
    /// until its commit pops it).
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
    fn unbounded_inbox_never_refuses_at_any_depth() {
        // Given an Unbounded inbox whose capacity field is irrelevant.
        let mut inbox = Inbox::new(1, OverloadPolicy::Unbounded);

        // When pushing far past any sane capacity.
        for n in 0..100_000u32 {
            inbox.push(envelope(n)).expect("push");
        }

        // Then every envelope queued (len == pushes) and the offsets
        // stayed monotonic — nothing was refused, nothing pre-allocated.
        assert_eq!(inbox.len(), 100_000);
        assert_eq!(inbox.cursor(), InboxOffset::zero());

        // And a drain hands back all of them FIFO.
        let drained: Vec<_> = inbox.drain().collect();
        assert_eq!(drained.len(), 100_000);
        assert_eq!(
            drained[0].1.payload_json()["n"].as_u64(),
            Some(0),
            "FIFO order intact"
        );
    }

    #[test]
    fn unbounded_push_never_refuses_even_with_a_full_capacity_field() {
        // Given an Unbounded inbox already holding capacity-many entries.
        let mut inbox = Inbox::new(2, OverloadPolicy::Unbounded);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");

        // When pushing a third envelope (a bounded inbox would refuse).
        let result = inbox.push(envelope(3));

        // Then it queues anyway and the queue holds all three.
        assert!(result.is_ok());
        assert_eq!(inbox.len(), 3);
        assert_eq!(inbox.cursor(), InboxOffset::zero());
    }

    #[test]
    fn unbounded_inbox_still_refuses_when_closed() {
        // Given a closed Unbounded inbox.
        let mut inbox = Inbox::new(4, OverloadPolicy::Unbounded);
        inbox.push(envelope(1)).expect("push");
        inbox.close();

        // When pushing another envelope.
        let push = inbox.push(envelope(2));

        // Then the shutdown-drain refusal still applies (Unbounded only
        // lifts the CAPACITY refusal, never the closed one).
        assert!(matches!(push, Err(Refused::Closed(_))));
    }

    #[test]
    fn unbounded_claims_commits_and_restores_still_work() {
        // Given an Unbounded inbox holding three entries.
        let mut inbox = Inbox::new(1, OverloadPolicy::Unbounded);
        for n in 1..=3 {
            inbox.push(envelope(n)).expect("push");
        }

        // When claiming two and committing through the first.
        let mut batch = Vec::new();
        inbox.claim_up_to(2, &mut batch);
        inbox.commit_through(batch[0].0);
        inbox.restore_claims(vec![batch[1].clone()]);

        // Then the claim machinery behaves exactly as under a bounded
        // policy: the cursor sits past offset 0 and envelope 2 is at the
        // front (restored into its slot).
        assert_eq!(inbox.cursor(), InboxOffset::new(1));
        assert_eq!(inbox.len(), 2);
        let front = inbox.peek().expect("front");
        assert_eq!(front.payload_json()["n"].as_u64(), Some(2));
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
    fn claim_up_to_returns_fifo_claims_without_moving_the_cursor() {
        // Given an inbox holding three envelopes.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        for n in 1..=3 {
            inbox.push(envelope(n)).expect("push");
        }

        // When claiming two into a fresh batch buffer.
        let mut batch = Vec::new();
        inbox.claim_up_to(2, &mut batch);

        // Then the claims are FIFO with their offsets, the cursor did NOT
        // move, and the queue still counts all three slots (two are
        // tombstones — claims are in flight, not consumed).
        assert_eq!(
            batch.iter().map(|(o, _)| o.as_u64()).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(batch[0].1.payload_json()["n"].as_u64(), Some(1));
        assert_eq!(batch[1].1.payload_json()["n"].as_u64(), Some(2));
        assert_eq!(inbox.cursor(), InboxOffset::new(0));
        assert_eq!(inbox.len(), 3, "tombstones count toward len");
    }

    #[test]
    fn claim_up_to_takes_what_is_queued_when_shorter_than_n() {
        // Given an inbox holding two envelopes and a batch budget of five.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");

        // When claiming up to five.
        let mut batch = Vec::new();
        inbox.claim_up_to(5, &mut batch);

        // Then exactly the two queued entries come back (trickle behavior).
        assert_eq!(batch.len(), 2);
        assert_eq!(inbox.len(), 2);

        // And a claim on a drained inbox yields nothing.
        let mut empty = Inbox::new(4, OverloadPolicy::Block);
        let mut none = Vec::new();
        empty.claim_up_to(5, &mut none);
        assert!(none.is_empty());
    }

    #[test]
    fn commit_through_advances_the_cursor_atomically() {
        // Given an inbox whose first two entries are claimed.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        for n in 1..=3 {
            inbox.push(envelope(n)).expect("push");
        }
        let mut batch = Vec::new();
        inbox.claim_up_to(2, &mut batch);

        // When committing through the last batch offset.
        let last = batch.last().expect("non-empty").0;
        let popped = inbox.commit_through(last);

        // Then the cursor sits exactly past the batch (and the commit
        // reports that it freed slots), and the third envelope is what a
        // peek returns.
        assert_eq!(inbox.cursor(), InboxOffset::new(2));
        assert_eq!(inbox.len(), 1);
        let front = inbox.peek().expect("front");
        assert_eq!(front.payload_json()["n"].as_u64(), Some(3));
        assert!(popped);
    }

    #[test]
    fn commit_through_pops_live_entries_it_jumps_over() {
        // Given an inbox where entries were pushed AFTER a claim (new
        // mail queues behind a batch that is mid-flight).
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        for n in 1..=2 {
            inbox.push(envelope(n)).expect("push");
        }
        let mut batch = Vec::new();
        inbox.claim_up_to(1, &mut batch);
        inbox.push(envelope(3)).expect("push");

        // When committing through the single claimed entry.
        inbox.commit_through(batch[0].0);

        // Then the cursor moved past it and the queue still holds the
        // later entries in order (nothing live was dropped).
        assert_eq!(inbox.cursor(), InboxOffset::new(1));
        assert_eq!(inbox.len(), 2);
        let front = inbox.peek().expect("front");
        assert_eq!(front.payload_json()["n"].as_u64(), Some(2));
    }

    #[test]
    fn tombstones_count_toward_capacity_until_commit() {
        // Given a capacity-2 inbox.
        let mut inbox = Inbox::new(2, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");

        // When claiming one (its slot becomes a tombstone).
        let mut batch = Vec::new();
        inbox.claim_up_to(1, &mut batch);
        assert_eq!(batch.len(), 1);

        // Then the inbox is still at capacity — a claim is in flight, not
        // consumed — so the next push is refused.
        let refused = inbox.push(envelope(3)).expect_err("still full");
        assert!(!refused.queued_anyway(), "Block refuses without queuing");

        // And after the commit, the slot frees and the push succeeds.
        inbox.commit_through(batch[0].0);
        inbox.push(envelope(3)).expect("push after commit");
    }

    #[test]
    fn dropped_claims_restore_to_their_original_slots() {
        // Given an inbox whose first two entries are claimed.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        for n in 1..=3 {
            inbox.push(envelope(n)).expect("push");
        }
        let mut batch = Vec::new();
        inbox.claim_up_to(2, &mut batch);
        assert_eq!(inbox.len(), 3, "tombstones in place");

        // When restoring both claims (the step died before committing).
        inbox.restore_claims(batch);

        // Then every envelope is back in its original slot: peek sees
        // envelope 1 at the cursor, len is unchanged, offsets intact.
        assert_eq!(inbox.cursor(), InboxOffset::new(0));
        assert_eq!(inbox.len(), 3);
        let front = inbox.peek().expect("front");
        assert_eq!(front.payload_json()["n"].as_u64(), Some(1));

        // And a re-claim hands back the SAME envelopes in the SAME order.
        let mut again = Vec::new();
        inbox.claim_up_to(2, &mut again);
        assert_eq!(
            again.iter().map(|(o, _)| o.as_u64()).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(again[0].1.payload_json()["n"].as_u64(), Some(1));
        assert_eq!(again[1].1.payload_json()["n"].as_u64(), Some(2));
    }

    #[test]
    fn restored_claims_keep_fifo_order_across_a_concurrent_push() {
        // Given an inbox whose first two entries are claimed, with a
        // concurrent push landing at the tail during the claim window.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        for n in 1..=2 {
            inbox.push(envelope(n)).expect("push");
        }
        let mut batch = Vec::new();
        inbox.claim_up_to(2, &mut batch);
        let late = inbox.push(envelope(9)).expect("push during claim window");
        assert_eq!(late.as_u64(), 2, "the push took the next offset");

        // When committing only the first claim and restoring the second
        // (a mid-batch crash after message one).
        inbox.commit_through(batch[0].0);
        inbox.restore_claims(vec![batch.pop().expect("non-empty")]);

        // Then the restored envelope sits at its ORIGINAL offset ahead of
        // the late push — the drain+requeue reorder hole stays closed.
        assert_eq!(inbox.cursor(), InboxOffset::new(1));
        assert_eq!(inbox.len(), 2);
        let front = inbox.peek().expect("front");
        assert_eq!(front.payload_json()["n"].as_u64(), Some(2));
    }

    #[test]
    fn commit_through_returns_false_when_nothing_pops() {
        // Given an inbox holding one entry.
        let mut inbox = Inbox::new(4, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");

        // When committing through the write head minus one (the entry's
        // own offset): the pop happens, so this is true.
        let mut batch = Vec::new();
        inbox.claim_up_to(1, &mut batch);
        assert!(inbox.commit_through(batch[0].0));

        // And a second entry's commit pops exactly it.
        inbox.push(envelope(2)).expect("push");
        let mut batch = Vec::new();
        inbox.claim_up_to(1, &mut batch);
        assert!(inbox.commit_through(batch[0].0));
        assert!(inbox.is_empty());
    }

    #[test]
    fn snapshot_commit_round_trip_leaves_no_residue() {
        // Given an inbox holding three entries.
        let mut inbox = Inbox::new(8, OverloadPolicy::Block);
        for n in 1..=3 {
            inbox.push(envelope(n)).expect("push");
        }

        // When claiming all and committing message-by-message (the
        // batch step's shape: the offset grows as the batch progresses).
        let mut batch = Vec::new();
        inbox.claim_up_to(3, &mut batch);
        for (offset, _) in &batch {
            inbox.commit_through(*offset);
        }

        // Then everything is committed and the offset space stays
        // monotonic for the next push.
        assert!(inbox.is_empty());
        assert_eq!(inbox.cursor(), InboxOffset::new(3));
        let next = inbox.push(envelope(9)).expect("push");
        assert!(next.as_u64() >= 3);

        // And a fresh claim sees only the new entry.
        let mut again = Vec::new();
        inbox.claim_up_to(4, &mut again);
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].1.payload_json()["n"].as_u64(), Some(9));
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
    #[should_panic(expected = "not queued")]
    fn restoring_a_committed_claim_panics() {
        // Given an inbox whose only entry was claimed AND committed.
        let mut inbox = Inbox::new(4, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");
        let mut batch = Vec::new();
        inbox.claim_up_to(1, &mut batch);
        inbox.commit_through(batch[0].0);

        // When restoring the committed claim.
        inbox.restore_claims(batch);

        // Then it panics: the slot is gone (commit popped it).
    }

    #[test]
    #[should_panic(expected = "double restore")]
    fn restoring_a_live_slot_panics() {
        // Given an inbox with one claimed entry restored twice.
        let mut inbox = Inbox::new(4, OverloadPolicy::Block);
        inbox.push(envelope(1)).expect("push");
        let mut batch = Vec::new();
        inbox.claim_up_to(1, &mut batch);
        let clone_batch = batch.clone();
        inbox.restore_claims(batch);
        let restored = clone_batch.into_iter().next().expect("non-empty");

        // When restoring the same claim again (the slot is live now).
        inbox.restore_claims(vec![restored]);

        // Then it panics: double restore is a runtime bug.
    }

    #[test]
    fn drop_old_holes_stay_committed_through_a_batch() {
        // Given a capacity-2 DropOld inbox that evicted its first entry.
        let mut inbox = Inbox::new(2, OverloadPolicy::DropOld);
        inbox.push(envelope(1)).expect("push");
        inbox.push(envelope(2)).expect("push");
        let evicted = inbox.push(envelope(3)).expect_err("eviction");
        assert!(evicted.queued_anyway(), "envelope 1 was evicted");

        // When claiming the live pair and committing through the last.
        let mut batch = Vec::new();
        inbox.claim_up_to(4, &mut batch);
        assert_eq!(batch.len(), 2, "only envelopes 2 and 3 are live");
        assert_eq!(
            batch.iter().map(|(o, _)| o.as_u64()).collect::<Vec<_>>(),
            vec![1, 2],
            "offsets stay monotonic across the eviction hole"
        );
        let last = batch.last().expect("non-empty").0;
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
