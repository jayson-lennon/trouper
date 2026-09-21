//! In-memory, seq-anchored journals: lists of events and snapshots
//! entries. Restart restores from the latest snapshot plus the tail; command
//! redelivery is independent of snapshots. Persisted backends implement the
//! [`JournalStore`] trait; the in-memory store is the default.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::envelope::Event;
pub use crate::json::Json;

/// Why an actor's journal holds a fact.
///
/// `Recorded` is a decision the actor made in its own step. `CatchUp` is a
/// fact another actor recorded, re-recorded into this journal during
/// catch-up seeding — a projector's checkpoint. Scans surface only
/// `Recorded` entries so a projector never folds another projector's
/// re-recorded copies.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum EventOrigin {
    /// A decision an actor recorded in its own step.
    #[default]
    Recorded,
    /// A catch-up seed re-recorded into a projector's journal, traced to
    /// its origin.
    CatchUp {
        /// The actor journal the fact was scanned from.
        source: crate::actor::ActorPath,
        /// The fact's seq inside the source journal.
        source_seq: SeqNo,
    },
}

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
        /// Why this journal holds the fact (defaults to [`EventOrigin::Recorded`]
        /// for journals written before projectors existed).
        #[serde(default)]
        origin: EventOrigin,
        /// The store-assigned global arrival order across all paths.
        /// Defaults to 0 for legacy entries (they sort early; ordering
        /// among them falls back to per-journal seq).
        #[serde(default)]
        ingest_seq: u64,
    },
    /// A memoized fold of every event up to and including `seq`.
    Snapshot {
        /// Sequence of the last event folded into `state`.
        seq: SeqNo,
        /// The snapshot state.
        state: Json,
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
    /// An event append failed (the backing store refused the write).
    Append,
    /// A replay load failed (the backing store could not be read).
    Load,
    /// A journal drop was refused (the backend cannot purge this path).
    Purge,
    /// The store refused a lifecycle hint (`passivated`); the runtime
    /// logs and continues — hints never block the caller.
    Hint,
    /// A catch-up scan failed (the backing store could not be read).
    Scan,
}

/// One event entry as the store presents it (the replay seam).
#[derive(Debug, Clone)]
pub struct JournaledEvent {
    /// Position in the owning journal.
    pub seq: SeqNo,
    /// The event itself.
    pub event: Event,
    /// Why the journal holds the fact.
    pub origin: EventOrigin,
    /// The store-assigned global arrival order.
    pub ingest_seq: u64,
}

impl JournaledEvent {
    /// Whether this entry is a recorded fact (never a checkpoint
    /// re-record) whose payload names `key` under `key_field` — the
    /// per-key projector seed test, mirroring broadcast's derivation.
    pub fn recorded_payload_key(&self, key_field: &str, key: &str) -> bool {
        self.origin == EventOrigin::Recorded
            && self.event.payload.get(key_field).and_then(|v| v.as_str()) == Some(key)
    }
}

/// One consumed fact surfaced by a [`JournalStore::scan`].
#[derive(Debug, Clone)]
pub struct ScannedEvent {
    /// The actor journal the fact was recorded in.
    pub journal: crate::actor::ActorPath,
    /// Position inside that journal.
    pub seq: SeqNo,
    /// The store-assigned global arrival order (the scan's sort key).
    pub ingest_seq: u64,
    /// The fact itself.
    pub event: Event,
}

/// What a replay needs: the latest snapshot (if any) plus the event tail
/// after it — the exact input [`Journal`]'s restore path consumes.
///
/// `events` is the FULL history regardless of snapshot position (every
/// event ever recorded by this path): a projector's checkpoint is the set
/// of CatchUp origins across the WHOLE journal, including entries a
/// snapshot already folded.
#[derive(Debug, Clone)]
pub struct Replay {
    /// The latest snapshot, anchored at the seq of the event it folded.
    pub snapshot: Option<JournalEntry>,
    /// Every event strictly after the snapshot's seq (all events when no
    /// snapshot exists).
    pub tail: Vec<Event>,
    /// Every event entry ever recorded by this path, in journal order,
    /// with origin and arrival-order metadata.
    pub events: Vec<JournaledEvent>,
}

/// Where an actor's journal lives.
///
/// Contract:
/// - [`append`](JournalStore::append) is awaited before the command's
///   ack — a write-through store therefore gets "never ack what isn't
///   journaled"; a buffering store persists pending entries in
///   [`flush`](JournalStore::flush), which the runtime calls once during
///   the graceful shutdown sweep (after every actor drained, before slot
///   removal). Between flushes, `load` must reflect buffered state, so
///   reactivation sees everything appended.
/// - Per-path sequence assignment belongs to the store: one loop task per
///   actor path, so per-path appends are already serialized. The store
///   additionally assigns each recorded fact a globally monotonic
///   `ingest_seq` at append time (assignment is serialized with the
///   append; one machine, one store — it sees every append).
/// - Appends are cheap buffered writes; the sweep-time flush is where a
///   backing database commits (or a write-through store no-ops).
#[async_trait::async_trait]
pub trait JournalStore: Send + Sync {
    /// Appends events to the actor's journal, returning their seqs.
    ///
    /// # Errors
    ///
    /// [`JournalError::Append`] when the store refuses the write; the
    /// runtime aborts the step before the ack (the message stays queued).
    async fn append(
        &self,
        path: &crate::actor::ActorPath,
        events: &[Event],
    ) -> Result<Vec<SeqNo>, error_stack::Report<JournalError>>;

    /// Appends a snapshot of the folded state, anchored at `seq` (the
    /// last event folded into it), stamped `now_ms` for the time cadence.
    ///
    /// # Errors
    ///
    /// [`JournalError::Snapshot`] when the store refuses the write.
    async fn append_snapshot(
        &self,
        path: &crate::actor::ActorPath,
        seq: SeqNo,
        state: Json,
        now_ms: u64,
    ) -> Result<(), error_stack::Report<JournalError>>;

    /// The latest snapshot plus the event tail after it (the replay input
    /// for restart and spawn-time recovery). `Ok(None)` = no journal.
    ///
    /// # Errors
    ///
    /// [`JournalError::Load`] when the store cannot be read.
    async fn load(
        &self,
        path: &crate::actor::ActorPath,
    ) -> Result<Option<Replay>, error_stack::Report<JournalError>>;

    /// Persists everything buffered, for ALL paths. Called once during
    /// the graceful shutdown sweep; a write-through store no-ops this.
    ///
    /// # Errors
    ///
    /// Store-specific write failures.
    async fn flush(&self) -> Result<(), error_stack::Report<JournalError>>;

    /// The backend's name (debug/export).
    fn name(&self) -> &'static str;

    /// The store as `Any` (downcast seam for the in-memory default).
    fn as_any(&self) -> &dyn std::any::Any;

    /// Appends catch-up seeds to a projector's journal: `events` are
    /// recorded with origin [`EventOrigin::CatchUp`], traced to
    /// `(journal, seq)` — the projector's checkpoint. Idempotent: an
    /// entry whose `(journal, seq)` the checkpoint already holds is
    /// skipped (the projector folded it live or seeded it earlier), which
    /// is what makes live copies and re-seeds fold exactly once. The
    /// result aligns with `events`: `None` = already known (skipped),
    /// `Some(seq)` = newly appended.
    ///
    /// # Errors
    ///
    /// [`JournalError::Append`] when the store refuses the write.
    async fn append_catchup(
        &self,
        path: &crate::actor::ActorPath,
        events: &[ScannedEvent],
    ) -> Result<Vec<Option<SeqNo>>, error_stack::Report<JournalError>> {
        let _ = (path, events);
        Err(error_stack::Report::new(JournalError::Append))
    }

    /// Surfaces every `Recorded`-origin entry whose schema is in `schemas`,
    /// across all paths (passivated actors included — the store holds what
    /// the runtime forgets), ascending `ingest_seq`. Re-recorded
    /// projector entries are invisible by construction.
    ///
    /// # Errors
    ///
    /// [`JournalError::Scan`] when the store cannot be read.
    async fn scan(
        &self,
        schemas: &[crate::schema::SchemaId],
    ) -> Result<Vec<ScannedEvent>, error_stack::Report<JournalError>> {
        let _ = schemas;
        Err(error_stack::Report::new(JournalError::Scan))
    }

    /// Lifecycle hint: the actor at `path` just passivated (left memory
    /// with its journal durable). A backend may use this to switch it to
    /// cold storage. A failing hint never blocks passivation — the runtime
    /// logs and continues.
    ///
    /// # Errors
    ///
    /// Store-specific hint failures (logged by the caller, never fatal).
    async fn passivated(
        &self,
        path: &crate::actor::ActorPath,
    ) -> Result<(), error_stack::Report<JournalError>> {
        let _ = path;
        Ok(())
    }

    /// Drops the actor's journal entirely (host-initiated rebuild
    /// primitive). The next spawn replays nothing. Never resets the
    /// store's global ingest counter — arrival-order history is
    /// append-only.
    ///
    /// # Errors
    ///
    /// [`JournalError::Purge`] when the backend cannot drop the path.
    async fn purge(
        &self,
        path: &crate::actor::ActorPath,
    ) -> Result<(), error_stack::Report<JournalError>> {
        let _ = path;
        Err(error_stack::Report::new(JournalError::Purge))
    }
}

/// The default store: an in-memory [`Journal`] per actor path.
#[derive(Debug, Default)]
pub struct InMemoryJournalStore {
    journals: parking_lot::Mutex<HashMap<crate::actor::ActorPath, Journal>>,
    /// Globally monotonic arrival order across all paths. Assigned under
    /// the journals lock (append serialization), never reset by purge —
    /// ordering history is append-only even when a journal is dropped.
    ingest_seq: AtomicU64,
}

impl InMemoryJournalStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of actors with (possibly empty) journals.
    pub fn len(&self) -> usize {
        self.journals.lock().len()
    }

    /// Whether no actor has journaled anything.
    pub fn is_empty(&self) -> bool {
        self.journals.lock().is_empty()
    }

    /// Sync access to one actor's entries (inspection/tests: the in-memory
    /// store answers under a short lock; the trait stays async).
    pub fn entries_of(&self, path: &crate::actor::ActorPath) -> Vec<JournalEntry> {
        self.journals
            .lock()
            .get(path)
            .map(|j| j.entries().to_vec())
            .unwrap_or_default()
    }

    /// Sync append (tests: seeding a source journal without a runtime
    /// block). Same assignment rules as the async trait method.
    pub fn append_sync(
        &self,
        path: &crate::actor::ActorPath,
        events: &[Event],
    ) -> Result<Vec<SeqNo>, error_stack::Report<JournalError>> {
        let mut journals = self.journals.lock();
        let journal = journals.entry(path.clone()).or_default();
        Ok(events
            .iter()
            .map(|ev| {
                let ingest = self.ingest_seq.fetch_add(1, Ordering::SeqCst);
                journal.append_event(ev.clone(), ingest)
            })
            .collect())
    }
}

/// Downcasts a trait-object store to the in-memory implementation (tests
/// and inspection only — production code rides the trait).
pub fn downcast_in_memory(
    store: &std::sync::Arc<dyn JournalStore>,
) -> Option<&InMemoryJournalStore> {
    // `Arc<dyn JournalStore>` is not `Any` at the trait level; the
    // concrete handle minted at construction is. Comparison by the
    // concrete address: the runtime keeps the typed Arc alongside.
    store.as_any().downcast_ref::<InMemoryJournalStore>()
}

#[async_trait::async_trait]
impl JournalStore for InMemoryJournalStore {
    async fn append(
        &self,
        path: &crate::actor::ActorPath,
        events: &[Event],
    ) -> Result<Vec<SeqNo>, error_stack::Report<JournalError>> {
        let mut journals = self.journals.lock();
        let journal = journals.entry(path.clone()).or_default();
        Ok(events
            .iter()
            .map(|ev| {
                let ingest = self.ingest_seq.fetch_add(1, Ordering::SeqCst);
                journal.append_event(ev.clone(), ingest)
            })
            .collect())
    }

    async fn append_catchup(
        &self,
        path: &crate::actor::ActorPath,
        events: &[ScannedEvent],
    ) -> Result<Vec<Option<SeqNo>>, error_stack::Report<JournalError>> {
        let mut journals = self.journals.lock();
        let journal = journals.entry(path.clone()).or_default();
        Ok(events
            .iter()
            .map(|scanned| {
                // IDEMPOTENCE: the origin IS the checkpoint. An entry this
                // journal already holds (live copy or earlier seed) is
                // skipped — the caller must not re-apply it.
                if journal.holds_origin(&scanned.journal, scanned.seq) {
                    return None;
                }
                let origin = EventOrigin::CatchUp {
                    source: scanned.journal.clone(),
                    source_seq: scanned.seq,
                };
                let ingest = self.ingest_seq.fetch_add(1, Ordering::SeqCst);
                Some(journal.append_event_with_origin(scanned.event.clone(), origin, ingest))
            })
            .collect())
    }

    async fn scan(
        &self,
        schemas: &[crate::schema::SchemaId],
    ) -> Result<Vec<ScannedEvent>, error_stack::Report<JournalError>> {
        let journals = self.journals.lock();
        let mut found: Vec<ScannedEvent> = journals
            .iter()
            .flat_map(|(path, journal)| {
                journal
                    .entries()
                    .iter()
                    .filter_map(move |entry| match entry {
                        JournalEntry::Event {
                            seq,
                            event,
                            origin: EventOrigin::Recorded,
                            ingest_seq,
                        } => {
                            // Recorded origins only: a projector's own
                            // checkpoint entries (CatchUp) are NEVER
                            // another projector's history.
                            if schemas.contains(&event.schema) {
                                Some(ScannedEvent {
                                    journal: path.clone(),
                                    seq: *seq,
                                    ingest_seq: *ingest_seq,
                                    event: event.clone(),
                                })
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
            })
            .collect();
        found.sort_by_key(|sc| sc.ingest_seq);
        Ok(found)
    }

    async fn append_snapshot(
        &self,
        path: &crate::actor::ActorPath,
        seq: SeqNo,
        state: Json,
        now_ms: u64,
    ) -> Result<(), error_stack::Report<JournalError>> {
        let mut journals = self.journals.lock();
        journals
            .entry(path.clone())
            .or_default()
            .append_snapshot(seq, state, now_ms);
        Ok(())
    }

    async fn load(
        &self,
        path: &crate::actor::ActorPath,
    ) -> Result<Option<Replay>, error_stack::Report<JournalError>> {
        let journals = self.journals.lock();
        let Some(journal) = journals.get(path) else {
            return Ok(None);
        };
        let snapshot = journal.last_snapshot().cloned();
        let snap_seq = snapshot.as_ref().map(|entry| entry.seq());
        // Restore tail: events strictly after the latest snapshot.
        let tail: Vec<Event> = journal
            .after(snap_seq.unwrap_or_else(SeqNo::before_genesis))
            .filter_map(|entry| entry.as_event().cloned())
            .collect();
        // Full history: every event entry regardless of snapshot position
        // (the projector checkpoint reads CatchUp origins from the WHOLE
        // journal, including snapshot-covered entries).
        let events: Vec<JournaledEvent> = journal
            .entries()
            .iter()
            .filter_map(|entry| match entry {
                JournalEntry::Event {
                    seq,
                    event,
                    origin,
                    ingest_seq,
                } => Some(JournaledEvent {
                    seq: *seq,
                    event: event.clone(),
                    origin: origin.clone(),
                    ingest_seq: *ingest_seq,
                }),
                JournalEntry::Snapshot { .. } => None,
            })
            .collect();
        Ok(Some(Replay {
            snapshot,
            tail,
            events,
        }))
    }

    async fn flush(&self) -> Result<(), error_stack::Report<JournalError>> {
        // In-memory: there is nothing to persist.
        Ok(())
    }

    async fn passivated(
        &self,
        _path: &crate::actor::ActorPath,
    ) -> Result<(), error_stack::Report<JournalError>> {
        // In-memory: nothing to demote.
        Ok(())
    }

    async fn purge(
        &self,
        path: &crate::actor::ActorPath,
    ) -> Result<(), error_stack::Report<JournalError>> {
        self.journals.lock().remove(path);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "in-memory"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
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

    /// Appends an event, assigning it the next event sequence and the
    /// store-provided global arrival order.
    ///
    /// Snapshots never consume event sequences: they anchor to the event
    /// they folded.
    pub fn append_event(&mut self, event: Event, ingest_seq: u64) -> SeqNo {
        self.append_event_with_origin(event, EventOrigin::Recorded, ingest_seq)
    }

    /// Appends an event with an explicit origin and global arrival order
    /// (the catch-up seeding path).
    pub fn append_event_with_origin(
        &mut self,
        event: Event,
        origin: EventOrigin,
        ingest_seq: u64,
    ) -> SeqNo {
        let seq = self.next_seq();
        self.event_count += 1;
        self.entries.push(JournalEntry::Event {
            seq,
            event,
            origin,
            ingest_seq,
        });
        seq
    }

    /// Whether this journal already holds the CatchUp origin
    /// `(source, source_seq)` — the checkpoint lookup that makes seeding
    /// idempotent (live projector commits write the SAME identity).
    pub fn holds_origin(&self, source: &crate::actor::ActorPath, source_seq: SeqNo) -> bool {
        self.entries.iter().any(|entry| {
            matches!(
                entry,
                JournalEntry::Event {
                    origin:
                        EventOrigin::CatchUp {
                            source: s,
                            source_seq: q,
                        },
                    ..
                } if s == source && *q == source_seq
            )
        })
    }

    /// Appends a snapshot anchored at `seq` (the last folded event),
    /// stamped with `now_ms` for the time cadence.
    pub fn append_snapshot(&mut self, seq: SeqNo, state: Json, now_ms: u64) {
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

    /// Whether this is the [`SeqNo::before_genesis`] sentinel (compare by
    /// value, since `u64::MAX` is unreachable by honest counting).
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
    use crate::json;
    use crate::schema::SchemaId;

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
                origin: EventOrigin::Recorded,
                ingest_seq: 41,
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

            // Then kind, sequence, payload, origin, and arrival order are
            // preserved.
            assert_eq!(round, entry);
        }
    }

    #[test]
    fn legacy_event_entries_deserialize_with_defaulted_origin_and_ingest() {
        // Given a pre-0.6.0 serialized event entry (no origin/ingest_seq).
        let legacy = r#"{
            "Event": { "seq": 3, "event": { "schema": "StockReserved@1", "payload": { "qty": 2 } } }
        }"#;

        // When deserializing it.
        let entry: JournalEntry = serde_json::from_str(legacy).expect("legacy entry loads");

        // Then the entry loads with the Recorded origin and ingest 0 —
        // old journals remain readable across the upgrade.
        assert_eq!(entry.seq(), SeqNo::new(3));
        assert_eq!(
            entry,
            JournalEntry::Event {
                seq: SeqNo::new(3),
                event: event(2),
                origin: EventOrigin::Recorded,
                ingest_seq: 0,
            }
        );
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
            origin: EventOrigin::Recorded,
            ingest_seq: 0,
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
        let seqs: Vec<_> = (0..3).map(|i| journal.append_event(event(1), i)).collect();

        // Then sequences are 0, 1, 2.
        assert_eq!(seqs, [SeqNo::new(0), SeqNo::new(1), SeqNo::new(2)]);
        assert_eq!(journal.next_seq(), SeqNo::new(3));
    }

    #[test]
    fn last_snapshot_returns_the_highest_snapshot() {
        // Given a journal with two snapshots and events after each.
        let mut journal = Journal::new();
        journal.append_event(event(1), 0);
        journal.append_snapshot(SeqNo::new(0), json!({ "v": 1 }), 0);
        journal.append_event(event(2), 1);
        journal.append_event(event(3), 2);
        journal.append_snapshot(SeqNo::new(2), json!({ "v": 2 }), 0);
        journal.append_event(event(4), 3);

        // When asking for the last snapshot.
        let last = journal.last_snapshot().expect("snapshot");

        // Then it is the one anchored at seq 2.
        assert_eq!(last.seq(), SeqNo::new(2));
    }

    #[test]
    fn last_snapshot_is_none_before_any_snapshot() {
        // Given a journal with only events.
        let mut journal = Journal::new();
        journal.append_event(event(1), 0);

        // When asking for the last snapshot.
        let snapshot = journal.last_snapshot();

        // Then there is none.
        assert!(snapshot.is_none());
    }

    #[test]
    fn after_yields_only_events_strictly_past_the_anchor() {
        // Given a journal: events 0..4 with a snapshot anchored at 1.
        let mut journal = Journal::new();
        for i in 0..4 {
            journal.append_event(event(1), i);
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
        journal.append_event(event(1), 0);
        journal.append_event(event(2), 1);

        // When asking for events after genesis.
        let all: Vec<SeqNo> = journal
            .after(SeqNo::before_genesis())
            .map(|e| e.seq())
            .collect();

        // Then both events replay.
        assert_eq!(all, [SeqNo::new(0), SeqNo::new(1)]);
    }

    #[tokio::test]
    async fn append_assigns_globally_monotonic_ingest_seqs() {
        // Given one shared store with two actors' journals.
        let store = InMemoryJournalStore::new();
        let a = crate::actor::ActorPath::new("a");
        let b = crate::actor::ActorPath::new("b");

        // When appends INTERLEAVE across the two paths.
        store.append(&a, &[event(1)]).await.expect("append a1");
        store
            .append(&b, &[event(1), event(2)])
            .await
            .expect("append b1");
        store.append(&a, &[event(2)]).await.expect("append a2");

        // Then every event's ingest_seq is unique and the interleaved
        // arrival order holds globally: a1 < b1 < b2 < a2.
        let mut all: Vec<(crate::actor::ActorPath, u64)> = Vec::new();
        for path in [&a, &b] {
            let replay = store.load(path).await.expect("load").expect("journal");
            for je in replay.events {
                all.push((path.clone(), je.ingest_seq));
            }
        }
        assert_eq!(all.len(), 4, "one ingest_seq per event");
        let seq_of = |path: &crate::actor::ActorPath, idx: usize| {
            all.iter()
                .filter(|(p, _)| p == path)
                .map(|(_, s)| *s)
                .nth(idx)
                .expect("event present")
        };
        let (a1, a2) = (seq_of(&a, 0), seq_of(&a, 1));
        let (b1, b2) = (seq_of(&b, 0), seq_of(&b, 1));
        assert!(a1 < b1 && b1 < b2 && b2 < a2, "arrival order is global");
    }

    #[tokio::test]
    async fn scan_returns_only_recorded_origin_entries_in_ingest_order() {
        // Given two actors whose journals hold a `Ticked` event each, and a
        // third journal holding CatchUp seeds (a projector's re-records).
        let store = InMemoryJournalStore::new();
        let a = crate::actor::ActorPath::new("a");
        let b = crate::actor::ActorPath::new("b");
        let proj = crate::actor::ActorPath::new("proj");
        let tick = || Event::new(SchemaId::new("Ticked", 1), json!({ "qty": 1 }));
        store.append(&a, &[event(1), tick()]).await.expect("a");
        store.append(&b, &[tick()]).await.expect("b");
        let scanned = store.scan(&[tick().schema]).await.expect("scan");
        assert_eq!(scanned.len(), 2, "both Recorded Ticked entries surface");
        let first = store
            .append_catchup(&proj, &scanned)
            .await
            .expect("seed projector");
        assert!(
            first.iter().all(|s| s.is_some()),
            "first seeding appends everything"
        );

        // When scanning for the schema across all paths.
        let found = store.scan(&[tick().schema]).await.expect("scan");

        // Then only the two Recorded originals surface (the projector's
        // re-recorded copies are invisible), in ascending ingest order, each
        // traced to its source journal.
        assert_eq!(found.len(), 2, "CatchUp entries never surface in scans");
        assert_eq!(found[0].journal, a);
        assert_eq!(found[1].journal, b);
        assert!(found[0].ingest_seq < found[1].ingest_seq, "ingest order");

        // And seeding the SAME origins again appends nothing (the
        // checkpoint is idempotent — a projector never double-folds).
        let again = store
            .append_catchup(&proj, &scanned)
            .await
            .expect("re-seed");
        assert!(
            again.iter().all(|s| s.is_none()),
            "known origins are skipped, not re-appended"
        );
        let after = store.scan(&[tick().schema]).await.expect("scan");
        assert_eq!(after.len(), 2, "still exactly the two originals");
    }

    #[tokio::test]
    async fn purge_removes_journal_but_not_the_ingest_counter() {
        // Given a store that ingested events for two actors.
        let store = InMemoryJournalStore::new();
        let a = crate::actor::ActorPath::new("a");
        let b = crate::actor::ActorPath::new("b");
        store.append(&a, &[event(1)]).await.expect("a");
        store.append(&b, &[event(1)]).await.expect("b");
        let before = store.load(&b).await.expect("load").expect("journal");

        // When purging a's journal and appending to b again.
        store.purge(&a).await.expect("purge");
        store.append(&b, &[event(2)]).await.expect("b again");

        // Then a's journal is gone (a fresh spawn replays nothing) while b's
        // new event continues the GLOBAL counter exactly where it left off —
        // a reset counter would have handed out a low value again.
        assert!(store.load(&a).await.expect("load").is_none(), "purged");
        let after = store.load(&b).await.expect("load").expect("journal");
        let last = after.events.last().expect("b has two events");
        assert_eq!(
            last.ingest_seq,
            before.events.last().expect("b first").ingest_seq + 1,
            "purge did not reset the global ingest counter"
        );
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
