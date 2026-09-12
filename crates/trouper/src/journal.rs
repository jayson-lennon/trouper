//! In-memory, seq-anchored journals: lists of [`Event`] and [`Snapshot`]
//! entries. Restart restores from the latest snapshot plus the tail; command
//! redelivery is independent of snapshots. Persistence is deliberately out of
//! scope (see spec Anti-Goals).

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::envelope::Event;

/// One durable entry in an actor's journal.
///
/// A single container keeps events and snapshots interleaved, which makes
/// compaction (dropping pre-snapshot events) trivial later.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JournalEntry {
    /// A domain event — a fact that already happened.
    Event {
        /// Position of this event in the actor's journal.
        seq: SeqNo,
        /// The event itself.
        event: Event,
    },
    /// A memoized fold of every event up to and including `seq`.
    Snapshot {
        /// Sequence of the last event folded into `state`.
        seq: SeqNo,
        /// The snapshot state (JSON at the waist).
        state: JsonValue,
    },
}

impl JournalEntry {
    /// The entry's sequence number.
    pub fn seq(&self) -> SeqNo {
        match self {
            Self::Event { seq, .. } | Self::Snapshot { seq, .. } => *seq,
        }
    }

    /// The event payload, if this is an event entry.
    pub fn as_event(&self) -> Option<&Event> {
        match self {
            Self::Event { event, .. } => Some(event),
            Self::Snapshot { .. } => None,
        }
    }
}

/// Errors surfaced by journal operations.
#[derive(Debug, wherror::Error)]
#[error(debug)]
pub enum JournalError {
    /// Snapshot capture failed (state not serializable).
    Snapshot,
    /// Restore from a snapshot blob failed (blob not decodable).
    Restore,
}

/// One actor's in-memory journal: seq-anchored events and snapshots.
///
/// Sequence anchoring: the first event is seq 0 (after a genesis state);
/// a snapshot carries the seq of the last event it folded. Restarts
/// restore from the latest snapshot's seq and apply the events after it —
/// command redelivery (the inbox cursor) is a fully independent axis.
#[derive(Debug, Default)]
pub struct Journal {
    entries: Vec<JournalEntry>,
    /// Count of appended events — the anchor for the next event's seq.
    event_count: u64,
    /// Epoch millis (injected clock) of the last snapshot; `None` before the
    /// first one. Drives the time-based snapshot cadence.
    last_snapshot_ms: Option<u64>,
}

impl Journal {
    /// An empty journal (genesis).
    pub fn new() -> Self {
        Self::default()
    }

    /// The sequence the next appended event will get.
    pub fn next_seq(&self) -> SeqNo {
        SeqNo::new(self.event_count)
    }

    /// The sequence of the last appended event; [`SeqNo::genesis`] when
    /// the journal holds no events (the idle snapshot target).
    pub fn last_seq(&self) -> SeqNo {
        SeqNo::new(self.event_count.saturating_sub(1))
    }

    /// Appends an event, assigning it the next event sequence.
    ///
    /// Snapshots never consume event sequences: they anchor to the event
    /// they folded.
    pub fn append_event(&mut self, event: Event) -> SeqNo {
        let seq = self.next_seq();
        self.event_count += 1;
        self.entries.push(JournalEntry::Event { seq, event });
        seq
    }

    /// Appends a snapshot anchored at `seq` (the last folded event),
    /// stamped with `now_ms` for the time cadence.
    pub fn append_snapshot(&mut self, seq: SeqNo, state: JsonValue, now_ms: u64) {
        self.last_snapshot_ms = Some(now_ms);
        self.entries.push(JournalEntry::Snapshot { seq, state });
    }

    /// Anchors the time cadence at `now_ms` (spawn time), without writing
    /// a snapshot: the first time-cadence snapshot becomes due one full
    /// interval after the journal began.
    pub fn anchor_time_cadence(&mut self, now_ms: u64) {
        if self.last_snapshot_ms.is_none() {
            self.last_snapshot_ms = Some(now_ms);
        }
    }

    /// Millis elapsed since the last snapshot (or since the spawn-time
    /// anchor); `None` when the cadence was never anchored — the caller
    /// treats that as "not due" (never snapshot on an unanchored journal).
    pub fn since_snapshot_ms(&self, now: crate::clock::Timestamp) -> Option<u64> {
        self.last_snapshot_ms
            .map(|ms| now.as_millis().saturating_sub(ms))
    }

    /// The highest snapshot in the journal, if any.
    pub fn last_snapshot(&self) -> Option<&JournalEntry> {
        self.entries
            .iter()
            .rev()
            .find(|entry| matches!(entry, JournalEntry::Snapshot { .. }))
    }

    /// Every event entry strictly after `seq` (the replay tail).
    ///
    /// Passing [`SeqNo::before_genesis`] replays every event, including the
    /// first.
    pub fn after(&self, seq: SeqNo) -> impl Iterator<Item = &JournalEntry> {
        let from_beginning = seq.is_before_genesis();
        self.entries.iter().filter(move |entry| {
            entry.as_event().is_some() && (from_beginning || entry.seq() > seq)
        })
    }

    /// Every entry (for inspection and export).
    pub fn entries(&self) -> &[JournalEntry] {
        &self.entries
    }

    /// The number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing has been journalled.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Sequence number of a journal entry within one actor's journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SeqNo(u64);

impl SeqNo {
    /// The sequence of the first journal entry.
    pub fn genesis() -> Self {
        Self(0)
    }

    /// The sequence "before genesis": replaying `after(this)` yields every
    /// event, including the first.
    pub fn before_genesis() -> Self {
        Self(u64::MAX)
    }

    /// Wraps a raw sequence value.
    pub fn new(v: u64) -> Self {
        Self(v)
    }

    /// The raw sequence value.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Whether this is the [`before_genesis`] sentinel (compare by value,
    /// since `u64::MAX` is unreachable by honest counting).
    pub fn is_before_genesis(self) -> bool {
        self.0 == u64::MAX
    }
}

impl std::fmt::Display for SeqNo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaId;
    use serde_json::json;

    fn event(qty: i64) -> Event {
        Event::new(SchemaId::new("StockReserved", 1), json!({ "qty": qty }))
    }

    #[test]
    fn journal_entries_survive_serde_roundtrip() {
        // Given one entry of each kind.
        let entries = [
            JournalEntry::Event {
                seq: SeqNo::new(0),
                event: event(2),
            },
            JournalEntry::Snapshot {
                seq: SeqNo::new(4),
                state: json!({ "on_hand": 8 }),
            },
        ];

        for entry in entries {
            // When round-tripping through JSON.
            let round: JournalEntry =
                serde_json::from_str(&serde_json::to_string(&entry).expect("ser")).expect("de");

            // Then kind, sequence, and payload are preserved.
            assert_eq!(round, entry);
        }
    }

    #[test]
    fn entry_seq_reports_the_anchored_sequence() {
        // Given a snapshot anchored at seq 4.
        let snapshot = JournalEntry::Snapshot {
            seq: SeqNo::new(4),
            state: json!({}),
        };

        // When asking for its sequence.
        let seq = snapshot.seq();

        // Then it reports 4.
        assert_eq!(seq, SeqNo::new(4));
    }

    #[test]
    fn as_event_exposes_only_event_entries() {
        // Given an event entry and a snapshot entry.
        let event_entry = JournalEntry::Event {
            seq: SeqNo::new(2),
            event: event(1),
        };
        let snapshot = JournalEntry::Snapshot {
            seq: SeqNo::new(2),
            state: json!({}),
        };

        // When asking each for its event payload.
        let from_event = event_entry.as_event().is_some();
        let from_snapshot = snapshot.as_event().is_none();

        // Then only the event entry yields one.
        assert!(from_event);
        assert!(from_snapshot);
    }

    #[test]
    fn append_event_assigns_monotonically_increasing_sequences() {
        // Given an empty journal.
        let mut journal = Journal::new();

        // When appending three events.
        let seqs: Vec<_> = (0..3).map(|_| journal.append_event(event(1))).collect();

        // Then sequences are 0, 1, 2.
        assert_eq!(seqs, [SeqNo::new(0), SeqNo::new(1), SeqNo::new(2)]);
        assert_eq!(journal.next_seq(), SeqNo::new(3));
    }

    #[test]
    fn last_snapshot_returns_the_highest_snapshot() {
        // Given a journal with two snapshots and events after each.
        let mut journal = Journal::new();
        journal.append_event(event(1));
        journal.append_snapshot(SeqNo::new(0), json!({ "v": 1 }), 0);
        journal.append_event(event(2));
        journal.append_event(event(3));
        journal.append_snapshot(SeqNo::new(2), json!({ "v": 2 }), 0);
        journal.append_event(event(4));

        // When asking for the last snapshot.
        let last = journal.last_snapshot().expect("snapshot");

        // Then it is the one anchored at seq 2.
        assert_eq!(last.seq(), SeqNo::new(2));
    }

    #[test]
    fn last_snapshot_is_none_before_any_snapshot() {
        // Given a journal with only events.
        let mut journal = Journal::new();
        journal.append_event(event(1));

        // When asking for the last snapshot.
        let snapshot = journal.last_snapshot();

        // Then there is none.
        assert!(snapshot.is_none());
    }

    #[test]
    fn after_yields_only_events_strictly_past_the_anchor() {
        // Given a journal: events 0..4 with a snapshot anchored at 1.
        let mut journal = Journal::new();
        for _ in 0..4 {
            journal.append_event(event(1));
        }
        journal.append_snapshot(SeqNo::new(1), json!({}), 0);

        // When asking for events after seq 1.
        let tail: Vec<SeqNo> = journal.after(SeqNo::new(1)).map(|e| e.seq()).collect();

        // Then only event seqs 2 and 3 come back (snapshot seq 1 excluded).
        assert_eq!(tail, [SeqNo::new(2), SeqNo::new(3)]);
    }

    #[test]
    fn after_genesis_yields_every_event() {
        // Given a journal with two events and no snapshot.
        let mut journal = Journal::new();
        journal.append_event(event(1));
        journal.append_event(event(2));

        // When asking for events after genesis.
        let all: Vec<SeqNo> = journal
            .after(SeqNo::before_genesis())
            .map(|e| e.seq())
            .collect();

        // Then both events replay.
        assert_eq!(all, [SeqNo::new(0), SeqNo::new(1)]);
    }
}

#[test]
fn seqno_orders_numerically_and_roundtrips() {
    // Given two sequence numbers.
    let earlier = SeqNo::genesis();
    let later = SeqNo::new(7);

    // When comparing and round-tripping through JSON.
    let ordered = earlier < later;
    let round: SeqNo =
        serde_json::from_str(&serde_json::to_string(&later).expect("ser")).expect("de");

    // Then ordering follows the numeric value and the value survives.
    assert!(ordered);
    assert_eq!(round, later);
}
