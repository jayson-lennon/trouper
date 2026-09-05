//! Topic logs: bounded rings of published envelopes with per-subscriber
//! cursors (the record/replay seed).
//!
//! Delivery model: every publish is also a pump pass — each subscriber is
//! offered every log entry past its cursor. A slow subscriber's cursor
//! simply falls behind; the ring keeps delivering to everyone else from
//! their own cursors. A cursor advances ONLY when its inbox accepted the
//! envelope, so delivery is at-least-once per subscriber. Ring eviction is
//! the only way an entry becomes undeliverable: cursors below the floor
//! jump to the floor (the evicted range is gone; late subscribers see the
//! retained tail).

use std::collections::{HashMap, VecDeque};

use crate::envelope::Envelope;
use crate::inbox::OverloadPolicy;
use crate::types::{ActorPath, InboxOffset, SchemaId, Topic};

/// The fact recorded when an envelope is published onto a topic.
#[derive(Debug, Clone)]
pub struct TopicPublishFact {
    /// The topic published to.
    pub topic: Topic,
    /// The log offset assigned to the entry.
    pub offset: InboxOffset,
    /// The payload's schema.
    pub schema: SchemaId,
    /// The logical publisher, when known.
    pub from: Option<ActorPath>,
    /// The publish's trace.
    pub trace: crate::envelope::TraceCtx,
}

/// Where a fresh subscription starts reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorFrom {
    /// Only entries published from now on.
    Latest,
    /// From an explicit log offset (re-consume).
    Offset(u64),
}

/// One topic's log and its subscribers' cursors.
#[derive(Debug)]
pub struct TopicLog {
    capacity: usize,
    /// The bounded ring: (log offset, envelope).
    entries: VecDeque<(u64, Envelope)>,
    /// The offset the NEXT publish gets.
    next_offset: u64,
    /// The lowest offset still retained (everything below was evicted).
    floor: u64,
    /// Per-subscriber cursor: the next log offset to deliver.
    cursors: HashMap<ActorPath, u64>,
    /// Per-subscriber inbox policy, applied by the pump.
    policies: HashMap<ActorPath, OverloadPolicy>,
}

impl TopicLog {
    /// Creates a topic log holding `capacity` entries.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: VecDeque::new(),
            next_offset: 0,
            floor: 0,
            cursors: HashMap::new(),
            policies: HashMap::new(),
        }
    }

    /// Appends an envelope to the log, evicting past the capacity.
    ///
    /// Returns the log offset assigned to the entry.
    pub fn append(&mut self, envelope: Envelope) -> u64 {
        let offset = self.next_offset;
        self.next_offset += 1;
        self.entries.push_back((offset, envelope));
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
            self.floor += 1;
        }
        offset
    }

    /// Subscribes a path with an inbox policy, starting per `from`.
    ///
    /// Re-subscribing resets the cursor (the caller's choice).
    pub fn subscribe(&mut self, path: ActorPath, policy: OverloadPolicy, from: CursorFrom) -> u64 {
        let start = match from {
            CursorFrom::Latest => self.next_offset,
            CursorFrom::Offset(o) => o.max(self.floor),
        };
        self.cursors.insert(path.clone(), start);
        self.policies.insert(path, policy);
        start
    }

    /// Removes a subscriber (cascades on actor removal).
    pub fn unsubscribe(&mut self, path: &ActorPath) -> bool {
        let had = self.cursors.remove(path).is_some();
        self.policies.remove(path);
        had
    }

    /// Re-points a subscriber's cursor; the next pump re-delivers from
    /// there (at-least-once: side effects may repeat).
    ///
    /// # Errors
    ///
    /// Returns the requested offset unchanged if the path is not a
    /// subscriber; offsets below the ring floor clamp to the floor.
    pub fn reset_cursor(&mut self, path: &ActorPath, to: u64) -> Result<u64, u64> {
        let cursor = self.cursors.get_mut(path).ok_or(to)?;
        let clamped = to.max(self.floor);
        *cursor = clamped;
        Ok(clamped)
    }

    /// Whether the path is subscribed.
    pub fn is_subscribed(&self, path: &ActorPath) -> bool {
        self.cursors.contains_key(path)
    }

    /// The subscriber paths (registry snapshots).
    pub fn subscribers(&self) -> Vec<ActorPath> {
        self.cursors.keys().cloned().collect()
    }

    /// The retained range: `[floor, next_offset)`.
    pub fn retained(&self) -> (u64, u64) {
        (self.floor, self.next_offset)
    }

    /// One pump pass: offers every subscriber every retained entry past
    /// its cursor. `deliver` receives `(subscriber, envelope)` and
    /// answers whether the subscriber's inbox ACCEPTED it; only then
    /// does the cursor advance.
    ///
    /// Returns `(delivered, skipped)` counts for the tap/tests.
    pub fn pump_once(
        &mut self,
        mut deliver: impl FnMut(&ActorPath, &Envelope, OverloadPolicy) -> bool,
    ) -> (usize, usize) {
        let mut delivered = 0;
        let mut skipped = 0;
        for (path, cursor) in self.cursors.clone() {
            let Some(policy) = self.policies.get(&path).copied() else {
                continue;
            };
            // Offer entries cursor..next_offset that are still retained.
            for (offset, envelope) in &self.entries {
                if *offset < cursor {
                    continue;
                }
                if deliver(&path, envelope, policy) {
                    self.cursors.insert(path.clone(), offset + 1);
                    delivered += 1;
                } else {
                    // This subscriber's inbox is full/blocked: it stays
                    // behind; the pump moves on to other subscribers.
                    skipped += 1;
                    break;
                }
            }
        }
        (delivered, skipped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::TraceCtx;
    use crate::types::SchemaId;
    use serde_json::json;

    fn envelope(n: u64) -> Envelope {
        Envelope::json(
            SchemaId::new("Tick", 1),
            crate::envelope::Address::Topic(Topic::new("ticks")),
            json!({ "n": n }),
            TraceCtx::root(),
        )
    }

    #[test]
    fn append_assigns_monotonic_offsets_and_evicts_past_capacity() {
        // Given a log of capacity two.
        let mut log = TopicLog::new(2);

        // When appending three entries.
        let a = log.append(envelope(0));
        let b = log.append(envelope(1));
        let c = log.append(envelope(2));

        // Then offsets are monotonic and the oldest entry was evicted.
        assert_eq!((a, b, c), (0, 1, 2));
        assert_eq!(log.retained(), (1, 3));
    }

    #[test]
    fn subscribe_latest_starts_at_next_offset() {
        // Given a log with one published entry.
        let mut log = TopicLog::new(8);
        log.append(envelope(0));

        // When subscribing with CursorFrom::Latest.
        let start = log.subscribe(
            ActorPath::new("watcher"),
            OverloadPolicy::DropNew,
            CursorFrom::Latest,
        );

        // Then the cursor starts past the existing entry.
        assert_eq!(start, 1);
    }

    #[test]
    fn subscribe_from_offset_clamps_to_the_ring_floor() {
        // Given a log that evicted its first two entries.
        let mut log = TopicLog::new(2);
        for n in 0..4 {
            log.append(envelope(n));
        }

        // When subscribing from offset zero (evicted).
        let start = log.subscribe(
            ActorPath::new("watcher"),
            OverloadPolicy::DropNew,
            CursorFrom::Offset(0),
        );

        // Then the cursor clamps to the floor.
        assert_eq!(start, 2);
    }

    #[test]
    fn pump_delivers_per_subscriber_and_advances_only_on_accept() {
        // Given a log with three entries and one subscriber.
        let mut log = TopicLog::new(8);
        log.append(envelope(0));
        log.append(envelope(1));
        log.append(envelope(2));
        log.subscribe(
            ActorPath::new("sub"),
            OverloadPolicy::DropNew,
            CursorFrom::Offset(0),
        );

        // When pumping with a deliver fn that rejects odd payloads.
        let (delivered, skipped) = log.pump_once(|path, envelope, _| {
            let n = envelope.payload_json()["n"].as_u64().unwrap_or(0);
            if n % 2 == 0 {
                let _ = path;
                true
            } else {
                false
            }
        });

        // Then only the even entries were delivered and the cursor
        // stopped at the first rejection.
        assert_eq!(delivered, 1);
        assert_eq!(skipped, 1);
    }

    #[test]
    fn pump_delivers_independently_per_subscriber_cursor() {
        // Given two subscribers: one at offset 2, one at 0.
        let mut log = TopicLog::new(8);
        log.append(envelope(0));
        log.append(envelope(1));
        log.append(envelope(2));
        log.subscribe(
            ActorPath::new("ahead"),
            OverloadPolicy::DropNew,
            CursorFrom::Latest,
        );
        log.reset_cursor(&ActorPath::new("ahead"), 2)
            .expect("subscribed");
        log.subscribe(
            ActorPath::new("behind"),
            OverloadPolicy::DropNew,
            CursorFrom::Offset(0),
        );

        // When pumping with an always-accept deliver.
        let (delivered, _) = log.pump_once(|_, _, _| true);

        // Then each subscriber got exactly what its cursor was missing:
        // "ahead" re-reads entry 2, "behind" reads all three.
        assert_eq!(delivered, 4);
    }

    #[test]
    fn reset_cursor_requeues_entries_and_clamps_below_floor() {
        // Given a subscribed path that consumed everything.
        let mut log = TopicLog::new(8);
        log.append(envelope(0));
        log.append(envelope(1));
        log.subscribe(
            ActorPath::new("sub"),
            OverloadPolicy::DropNew,
            CursorFrom::Offset(0),
        );
        log.pump_once(|_, _, _| true);

        // When resetting the cursor to offset zero.
        let cursor = log
            .reset_cursor(&ActorPath::new("sub"), 0)
            .expect("subscribed");

        // Then the next pump re-delivers from there.
        let (delivered, _) = log.pump_once(|_, _, _| true);
        assert_eq!(cursor, 0);
        assert_eq!(delivered, 2);

        // And a reset below the floor clamps.
        log.append(envelope(9));
        while log.retained().0 == 0 {
            log.append(envelope(9));
        }
        let clamped = log
            .reset_cursor(&ActorPath::new("sub"), 0)
            .expect("subscribed");
        assert_eq!(clamped, log.retained().0);
    }

    #[test]
    fn unsubscribe_removes_the_subscriber() {
        // Given a subscribed path.
        let mut log = TopicLog::new(8);
        log.subscribe(
            ActorPath::new("sub"),
            OverloadPolicy::DropNew,
            CursorFrom::Latest,
        );

        // When unsubscribing.
        let removed = log.unsubscribe(&ActorPath::new("sub"));

        // Then the path is gone and re-unsubscribe reports nothing.
        assert!(removed);
        assert!(!log.unsubscribe(&ActorPath::new("sub")));
        assert!(!log.is_subscribed(&ActorPath::new("sub")));
    }

    #[test]
    fn reset_cursor_fails_when_not_subscribed() {
        // Given an empty log.
        let mut log = TopicLog::new(8);

        // When resetting a non-subscriber's cursor.
        let result = log.reset_cursor(&ActorPath::new("ghost"), 4);

        // Then the reset is refused, echoing the requested offset.
        assert_eq!(result, Err(4));
    }

    #[test]
    fn inbox_offset_roundtrips_log_positions() {
        // Given a log offset.
        let offset = InboxOffset::new(7);

        // When converting through the raw value.
        let raw = offset.as_u64();

        // Then it round-trips.
        assert_eq!(InboxOffset::new(raw).as_u64(), 7);
    }
}
