//! In-memory, seq-anchored journals: lists of [`Event`] and [`Snapshot`]
//! entries. Restart restores from the latest snapshot plus the tail; command
//! redelivery is independent of snapshots. Persistence is deliberately out of
//! scope (see spec Anti-Goals).

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::envelope::Event;
use crate::types::SeqNo;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SchemaId;
    use serde_json::json;

    fn sample_event(seq: u64) -> JournalEntry {
        JournalEntry::Event {
            seq: SeqNo::new(seq),
            event: Event::new(SchemaId::new("StockReserved", 1), json!({ "qty": 2 })),
        }
    }

    #[test]
    fn journal_entries_survive_serde_roundtrip() {
        // Given one entry of each kind.
        let entries = [
            sample_event(0),
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
        let event = sample_event(2);
        let snapshot = JournalEntry::Snapshot {
            seq: SeqNo::new(2),
            state: json!({}),
        };

        // When asking each for its event payload.
        let from_event = event.as_event().is_some();
        let from_snapshot = snapshot.as_event().is_none();

        // Then only the event entry yields one.
        assert!(from_event);
        assert!(from_snapshot);
    }
}

