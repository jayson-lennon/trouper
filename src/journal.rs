//! In-memory, seq-anchored journals: lists of events and snapshots
//! entries. Restart restores from the latest snapshot plus the tail; command
//! redelivery is independent of snapshots. Persisted backends implement the
//! [`JournalStore`] trait; the in-memory store is the default.

use crate::envelope::{Event, PayloadBytes};
pub use crate::json::Json;
use crate::schema::SchemaId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

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
#[derive(Debug, Clone, PartialEq)]
pub enum JournalEntry {
    /// A domain event — a fact that already happened.
    Event {
        /// Position of this event in the actor's journal.
        seq: SeqNo,
        /// The event itself (payload carried live; the wire encoding is
        /// taken at append — one serialization per committed event).
        event: Event,
        /// Why this journal holds the fact (defaults to [`EventOrigin::Recorded`]
        /// for journals written before projectors existed).
        origin: EventOrigin,
        /// The store-assigned global arrival order across all paths.
        /// Defaults to 0 for legacy entries (they sort early; ordering
        /// among them falls back to per-journal seq).
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

/// The WIRE form of one journal entry (what a persistence backend stores
/// and replays). The event payload is compact JSON text — the journal
/// door's contract: written once at append via the payload's memoized
/// encoding, decoded straight into the declared struct at replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WireJournalEntry {
    /// A domain event — a fact that already happened.
    Event {
        /// Position of this event in the actor's journal.
        seq: SeqNo,
        /// The event: schema name + JSON-text payload.
        event: WireEvent,
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

/// The wire form of an event: schema + payload bytes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireEvent {
    /// The event's schema.
    pub schema: SchemaId,
    /// The event's compact JSON-text payload.
    pub payload: PayloadBytes,
}

impl From<&Event> for WireEvent {
    fn from(event: &Event) -> Self {
        Self {
            schema: event.schema.clone(),
            payload: event.payload.wire_bytes(),
        }
    }
}

impl TryFrom<WireEvent> for Event {
    type Error = error_stack::Report<JournalError>;

    fn try_from(wire: WireEvent) -> Result<Self, Self::Error> {
        let bytes = wire.payload;
        bytes.assert_json_text();
        Ok(Event::from_bytes(wire.schema, bytes))
    }
}

impl From<&JournalEntry> for WireJournalEntry {
    fn from(entry: &JournalEntry) -> Self {
        match entry {
            JournalEntry::Event {
                seq,
                event,
                origin,
                ingest_seq,
            } => Self::Event {
                seq: *seq,
                event: WireEvent::from(event),
                origin: origin.clone(),
                ingest_seq: *ingest_seq,
            },
            JournalEntry::Snapshot { seq, state } => Self::Snapshot {
                seq: *seq,
                state: state.clone(),
            },
        }
    }
}

impl TryFrom<WireJournalEntry> for JournalEntry {
    type Error = error_stack::Report<JournalError>;

    fn try_from(wire: WireJournalEntry) -> Result<Self, Self::Error> {
        match wire {
            WireJournalEntry::Event {
                seq,
                event,
                origin,
                ingest_seq,
            } => Ok(JournalEntry::Event {
                seq,
                event: Event::try_from(event)?,
                origin,
                ingest_seq,
            }),
            WireJournalEntry::Snapshot { seq, state } => Ok(JournalEntry::Snapshot { seq, state }),
        }
    }
}

impl Event {
    /// The event as a JSON view (test mirror of the wire shape).
    #[cfg(test)]
    pub(crate) fn to_json_view(&self) -> Json {
        crate::json!({
            "schema": self.schema.as_str(),
            "payload": self.payload_json(),
        })
    }
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

    /// Renders the entry as its JSON view (the on-disk shape): event
    /// payloads appear as their wire bytes parsed back into a tree.
    #[cfg(test)]
    pub(crate) fn to_json_view(&self) -> Json {
        match self {
            JournalEntry::Event {
                seq,
                event,
                ingest_seq,
                ..
            } => {
                crate::json!({
                    "Event": {
                        "seq": seq.0,
                        "event": event.to_json_view(),
                        "origin": "recorded",
                        "ingest_seq": ingest_seq,
                    }
                })
            }
            JournalEntry::Snapshot { seq, state } => {
                crate::json!({ "Snapshot": { "seq": seq.0, "state": state } })
            }
        }
    }

    /// Parses a JSON view produced by [`JournalEntry::to_json_view`]
    /// (test-only mirror of the serde shape).
    #[cfg(test)]
    pub(crate) fn from_json_view(
        view: Json,
    ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
        use error_stack::ResultExt;
        let obj = view
            .as_object()
            .ok_or_else(|| error_stack::Report::new(crate::registry::RegistryError::InvalidSpec))
            .change_context(crate::registry::RegistryError::InvalidSpec)?;
        if let Some(e) = obj.get("Event") {
            let seq = e["seq"].as_u64().unwrap_or(0);
            let ev = &e["event"];
            let bytes = crate::envelope::PayloadBytes::from(crate::json::Json::of(&ev["payload"]));
            return Ok(JournalEntry::Event {
                seq: SeqNo(seq),
                event: Event::from_bytes(
                    SchemaId::parse(ev["schema"].as_str().unwrap_or_default())
                        .unwrap_or_else(|| SchemaId::new("?")),
                    bytes,
                ),
                origin: EventOrigin::Recorded,
                ingest_seq: e["ingest_seq"].as_u64().unwrap_or(0),
            });
        }
        if let Some(snap) = obj.get("Snapshot") {
            return Ok(JournalEntry::Snapshot {
                seq: SeqNo(snap["seq"].as_u64().unwrap_or(0)),
                state: Json(snap["state"].clone()),
            });
        }
        Err(error_stack::Report::new(
            crate::registry::RegistryError::InvalidSpec,
        ))
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
    /// A journaled event payload did not decode into its schema's
    /// registered type (the boundary-decode contract: a failing payload
    /// is named by schema and seq, never silently skipped).
    Decode {
        /// The schema whose payload failed to decode.
        schema: SchemaId,
        /// The seq of the failing entry.
        seq: SeqNo,
    },
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
        // Short-circuit on origin FIRST: the payload field read (a tree
        // decode on wire bytes) only ever runs for recorded facts. This
        // is a cold scan path (seed-time, not per-message), so the read
        // is paid per scanned recorded entry, not per delivered message.
        self.origin == EventOrigin::Recorded
            && self.event.payload.field(key_field).as_deref() == Some(key)
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

    /// Lifecycle hint: the actor at `path` just passivated. A buffering
    /// backend may flush it and release retained state once that flush is
    /// durable, but must retain acknowledged unflushed entries on
    /// failure for a later retry. A failing hint never blocks passivation —
    /// the runtime logs and continues.
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

/// A synchronous store-control message the host may observe.
///
/// The store (or its install path) calls the installed
/// [`ControlHandler`] in place of returning an error to the caller when
/// a failure surfaces on a path that has no caller — the writer task's
/// periodic flush and the shutdown sweep's `flush` (whose result the
/// runtime ignores) are the two that matter. The host keeps the receiving
/// end (a channel sender, a tracing hook, a metric bump) and decides what
/// a store failure means for the deployment.
#[derive(Debug, Clone)]
pub enum StoreControlMessage {
    /// A store operation failed after its result had no caller. `op`
    /// names the failing operation (`"append"`, `"flush"`, `"tick"`,
    /// `"load"`, `"scan"`, `"purge"`, `"passivated"`), `detail` carries
    /// the error rendering.
    Error {
        /// The operation that failed.
        op: &'static str,
        /// The failure rendering.
        detail: String,
    },
}

/// The host's store-control closure. Called synchronously from store
/// internals (the writer task, the sweep-adjacent paths) — keep it
/// cheap and non-blocking (a channel send, a counter).
pub type ControlHandler = std::sync::Arc<dyn Fn(StoreControlMessage) + Send + Sync>;

/// A [`JournalStore`] plus its control wiring, handed to
/// [`SystemConfig::with_journal`](crate::system::SystemConfig::with_journal)
/// at construction — the only moment a system's store installs (no
/// post-construction setter exists: a live swap would fork journals
/// across two stores).
///
/// The default is the in-memory store with no control handler. Build a
/// persistent system with the `daow` feature (see the `journal_daow`
/// module's `JournalArgs::daow`).
#[derive(Clone)]
pub struct JournalArgs {
    /// The store every ES actor's journal lives in.
    pub store: std::sync::Arc<dyn JournalStore>,
    /// The host's store-control handler (see
    /// [`StoreControlMessage`]); `None` leaves store errors only traced.
    pub control: Option<ControlHandler>,
}

impl std::fmt::Debug for JournalArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalArgs")
            .field("store_name", &self.store.name())
            .field("control", &self.control.is_some())
            .finish()
    }
}

impl JournalArgs {
    /// Wraps a custom store (control: none — add one with
    /// [`JournalArgs::with_control`]).
    pub fn new(store: std::sync::Arc<dyn JournalStore>) -> Self {
        Self {
            store,
            control: None,
        }
    }

    /// Attaches the host's store-control handler.
    pub fn with_control(mut self, handler: ControlHandler) -> Self {
        self.control = Some(handler);
        self
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
    use crate::schema::Schema as _;
    use crate::schema::SchemaId;

    fn event(qty: i64) -> Event {
        Event::from_json_view(SchemaId::new("StockReserved"), json!({ "qty": qty }))
    }

    #[test]
    fn journal_payload_is_valid_json_text_and_queryable() {
        // Given a typed fact appended through the wire door.
        use crate::envelope::Payload;
        let fact = StockReservedWire {
            qty: 7,
            sku: "w-1".into(),
        };
        let wire = WireEvent::from(&Event::new(
            StockReservedWire::schema_id(),
            Payload::value(fact),
        ));

        // When the stored bytes are read back (what a SQL TEXT/JSONB
        // column would hold).
        let text = std::str::from_utf8(wire.payload.as_bytes()).expect("valid utf8");
        let parsed: Json = serde_json::from_str(text).expect("valid JSON text");

        // Then the payload is compact queryable JSON (a json_extract-style
        // read works directly on the stored form).
        assert_eq!(parsed["qty"], 7);
        assert_eq!(parsed["sku"], "w-1");
        assert!(!text.contains(' '), "compact encoding, not pretty-printed");
    }

    #[test]
    fn additive_fields_fold_as_none_from_old_payloads() {
        // Given an OLD-shaped payload (no `sku` field) on the wire — what
        // a pre-evolution journal entry holds.
        let old_shape = json!({ "qty": 3 });
        let wire = WireEvent {
            schema: StockReservedWire::schema_id(),
            payload: crate::envelope::PayloadBytes::from(Json::of(&old_shape)),
        };
        let event = Event::try_from(wire).expect("wire -> event");

        // When the LATEST type decodes it (the replay/fold door).
        let fact: StockReservedWire = serde_json::from_slice(event.payload.json_text().as_ref())
            .expect("additive evolution decodes");

        // Then the new field defaults (the additive-fields contract) and
        // the old data survives.
        assert_eq!(fact.qty, 3);
        assert_eq!(fact.sku, "", "missing new field defaults, never fails");
    }

    #[test]
    fn boundary_decode_failure_is_named_by_schema_and_seq() {
        // Given a wire payload whose shape does not fit its schema's
        // registered type (qty is a string where an int is declared).
        let bad = json!({ "qty": "not-a-number", "sku": "w-2" });
        let wire = WireEvent {
            schema: StockReservedWire::schema_id(),
            payload: crate::envelope::PayloadBytes::from(Json::of(&bad)),
        };
        let seq = SeqNo(41);

        // When the boundary decode runs and fails.
        let event = Event::try_from(wire).expect("wire -> event (bytes are valid JSON)");
        let decoded =
            serde_json::from_slice::<StockReservedWire>(event.payload.json_text().as_ref());

        // Then the failure is surfaceable as the named Decode error
        // carrying schema + seq (the loud-rebuild contract).
        assert!(decoded.is_err(), "type-shape mismatch must not decode");
        let err = JournalError::Decode {
            schema: StockReservedWire::schema_id(),
            seq,
        };
        let rendered = format!("{err}");
        assert!(
            rendered.contains("StockReserved"),
            "names the schema: {rendered}"
        );
        assert!(rendered.contains("41"), "names the seq: {rendered}");
    }

    /// A derived fact type for the wire-door probes.
    #[derive(crate::schema::Event, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
    struct StockReservedWire {
        qty: i64,
        #[serde(default)]
        sku: String,
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
            // When round-tripping through the entry's JSON view (the
            // on-disk rendering; payloads are wire bytes, held in a view
            // tree for the assertion).
            let view = entry.to_json_view();
            let round = JournalEntry::from_json_view(view).expect("parses");

            // Then kind, sequence, payload, origin, and arrival order are
            // preserved.
            assert_eq!(round, entry);
        }
    }

    #[test]
    fn legacy_event_entries_deserialize_with_defaulted_origin_and_ingest() {
        // Given a pre-0.6.0 serialized event entry (no origin/ingest_seq).
        let legacy = r#"{
            "Event": { "seq": 3, "event": { "schema": "StockReserved", "payload": { "qty": 2 } } }
        }"#;

        // When parsing it through the entry view (the on-disk shape).
        let entry = JournalEntry::from_json_view(serde_json::from_str(legacy).expect("view"))
            .expect("legacy entry loads");

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
        let tick = || Event::from_json_view(SchemaId::new("Ticked"), json!({ "qty": 1 }));
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
