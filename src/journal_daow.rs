//! The `daow`-gated SQLite [`JournalStore`] backend: journals persist to
//! a [`daow::Pool`] without ever stalling the message path.
//!
//! The contract follows the journal door's write-behind shape: `append`
//! is a cheap buffered write (an in-memory [`Journal`] per actor path,
//! awaited before the command's ack, never touching SQLite), a dedicated
//! writer task flushes pending buffer entries to SQLite on a periodic
//! tick ([`DaowConfig::flush_interval`]), and the runtime's one
//! shutdown-sweep `flush` drains everything, so a graceful shutdown
//! leaves zero unflushed entries. Each active path's in-memory [`Journal`]
//! remains authoritative between flushes: `load` answers from it directly,
//! while `scan` unions it with SQLite to include passivated paths and
//! retained failed-passivation buffers.
//!
//! Durability and failure shape:
//! - A failed flush keeps the buffer and its durable frontier unchanged;
//!   the next cycle retries the same entries. Failures with no caller (the
//!   writer tick, the sweep's flush whose result the runtime ignores)
//!   report through the installed control handler — see
//!   [`JournalArgs::control`](crate::journal::JournalArgs::control).
//! - [`JournalStore::passivated`] flushes that path and drops its buffer
//!   only after commit. A failed flush retains the acknowledged tail for
//!   a later retry; reactivation clears its release marker.
//! - Dropping the store aborts the writer task; a graceful shutdown is
//!   the durability path (an un-graceful drop loses the unflushed tail).
//!
//! Install happens at construction, via
//! [`SystemConfig::with_journal`](crate::system::SystemConfig::with_journal):
//!
//! ```no_run
//! # async fn demo(pool: daow::Pool) -> Result<(), error_stack::Report<trouper::journal::JournalError>> {
//! use trouper::journal_daow::DaowConfig;
//! let control = trouper::journal::JournalArgs::daow(pool, DaowConfig::default())
//!     .with_control(std::sync::Arc::new(|msg| {
//!         tracing::error!(?msg, "journal store");
//!     }))
//!     .build()
//!     .await?;
//! let system = trouper::system::ActorSystem::new(
//!     trouper::system::SystemConfig::production().with_journal(control),
//! );
//! # Ok(())
//! # }
//! ```

use crate::actor::ActorPath;
use crate::envelope::Event;
use crate::journal::{
    ControlHandler, EventOrigin, Journal, JournalArgs, JournalEntry, JournalError, JournalStore,
    JournaledEvent, Replay, ScannedEvent, SeqNo, StoreControlMessage, WireEvent,
};
use error_stack::{Report, ResultExt};
use rusqlite::OptionalExtension as _;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

// ── Configuration ────────────────────────────────────────────────────────

/// Knobs for the SQLite journal backend.
#[derive(Debug, Clone)]
pub struct DaowConfig {
    /// How often the writer task flushes pending buffer entries to
    /// SQLite. The default (500 ms) bounds the loss window of an
    /// un-graceful crash; the shutdown sweep always flushes regardless
    /// of this interval.
    pub flush_interval: Duration,
}

impl Default for DaowConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(500),
        }
    }
}

/// The `daow` flavor of [`JournalArgs`]: the pool plus config, with the
/// control handler attachable before [`build`](DaowJournalArgs::build)
/// runs the migrations and spawns the writer task.
pub struct DaowJournalArgs {
    pool: daow::Pool,
    config: DaowConfig,
    control: Option<ControlHandler>,
}

impl JournalArgs {
    /// Begins a SQLite-backed journal args: runs
    /// [`DaowJournalArgs::build`] to install.
    pub fn daow(pool: daow::Pool, config: DaowConfig) -> DaowJournalArgs {
        DaowJournalArgs {
            pool,
            config,
            control: None,
        }
    }
}

impl DaowJournalArgs {
    /// Attaches the host's store-control handler (flush failures land
    /// here — see [`StoreControlMessage`]).
    pub fn with_control(mut self, handler: ControlHandler) -> Self {
        self.control = Some(handler);
        self
    }

    /// Validates the pool, runs the journal-table migration chain, seeds
    /// the ingest counter from the stored maximum, and spawns the writer
    /// task.
    ///
    /// # Errors
    ///
    /// [`JournalError::Restore`] when the pool is unreachable or the
    /// migration chain fails (the schema is never half-applied).
    pub async fn build(self) -> Result<JournalArgs, Report<JournalError>> {
        let shared = Arc::new(Shared::build(self.pool, self.config, self.control).await?);
        let writer = shared.spawn_writer();
        let store = DaowJournalStore {
            shared,
            writer: parking_lot::Mutex::new(Some(writer)),
        };
        Ok(JournalArgs {
            store: Arc::new(store),
            control: None,
        })
    }
}

// ── Migration chain (jinn method: versioned, atomic, drift-guarded) ──────

/// The newest migration in [`MIGRATIONS`], kept in lockstep with
/// [`LATEST_VERSION`] (asserted equal by the drift-guard test).
const CHAIN_LATEST_VERSION: i32 = 0;

/// The schema version `build` brings a database to. A database already at
/// this version pays no transaction cost.
const LATEST_VERSION: i32 = CHAIN_LATEST_VERSION;

/// One migration in the chain: its version, stable name, and DDL.
struct Migration {
    version: i32,
    name: &'static str,
    apply: fn(&mut rusqlite::Connection) -> Result<(), Report<JournalError>>,
}

/// The full migration chain, in application order. New migrations append
/// with [`CHAIN_LATEST_VERSION`] bumped in lockstep (the drift-guard test
/// enforces it).
const MIGRATIONS: &[Migration] = &[Migration {
    version: 0,
    name: "create_journal_schema",
    apply: migrate_v0,
}];

/// The journal tables: events (one row per journaled fact, carrying the
/// origin so replay is byte-faithful and scans can filter recorded-only
/// from the index) and snapshots (one row per memoized fold).
///
/// STRICT DDL: any pre-existing foreign table under these names fails
/// loudly instead of being silently absorbed.
fn migrate_v0(conn: &mut rusqlite::Connection) -> Result<(), Report<JournalError>> {
    conn.execute_batch(
        "CREATE TABLE trouper_journal_events (
             journal        TEXT    NOT NULL,
             seq            INTEGER NOT NULL,
             kind           INTEGER NOT NULL,
             schema         TEXT    NOT NULL,
             payload        TEXT    NOT NULL,
             origin         TEXT    NOT NULL,
             catchup_source TEXT,
             catchup_seq    INTEGER,
             ingest_seq     INTEGER NOT NULL,
             PRIMARY KEY (journal, seq)
         );
         CREATE INDEX trouper_journal_events_ingest ON trouper_journal_events(ingest_seq);
         CREATE INDEX trouper_journal_events_schema ON trouper_journal_events(schema);
         CREATE TABLE trouper_journal_snapshots (
             journal TEXT    NOT NULL,
             seq     INTEGER NOT NULL,
             state   TEXT    NOT NULL,
             PRIMARY KEY (journal, seq)
         );",
    )
    .change_context(JournalError::Restore)
    .attach("failed to create journal schema")?;
    Ok(())
}

/// Applies every unapplied migration inside one `BEGIN IMMEDIATE …
/// COMMIT` transaction; a failure rolls the whole chain back, leaving no
/// half-applied schema. A database already at [`LATEST_VERSION`] is a
/// no-op (no transaction issued).
///
/// The tracker is `_trouper_journal_migrations` — deliberately NOT
/// jinn's `_migrations`, so the two version spaces can coexist in one
/// database file without reading each other's rows.
fn run_pending(conn: &mut rusqlite::Connection) -> Result<bool, Report<JournalError>> {
    bootstrap_tracking_table(conn)?;
    let current = current_version(conn)?;

    // No-op path: an up-to-date database pays no transaction cost.
    if current >= LATEST_VERSION {
        return Ok(false);
    }

    conn.execute_batch("BEGIN IMMEDIATE")
        .change_context(JournalError::Restore)
        .attach("begin journal migration transaction")?;

    match apply_migration_chain(conn, current) {
        Ok(()) => {
            conn.execute_batch("COMMIT")
                .change_context(JournalError::Restore)
                .attach("commit journal migration transaction")?;
            Ok(true)
        }
        Err(report) => {
            // Best-effort rollback; the original error propagates.
            let _ = conn.execute_batch("ROLLBACK");
            Err(report)
        }
    }
}

/// Applies every migration above `current` (recording each version).
/// Runs inside the caller's open transaction.
fn apply_migration_chain(
    conn: &mut rusqlite::Connection,
    current: i32,
) -> Result<(), Report<JournalError>> {
    for migration in MIGRATIONS {
        if current < migration.version {
            (migration.apply)(conn)?;
            record_version(conn, migration.version, migration.name)?;
        }
    }
    Ok(())
}

/// Creates the tracking table. The ONLY place `IF NOT EXISTS` is used:
/// the tracker must bootstrap itself before version checking can begin;
/// all schema migrations use strict DDL so failures are loud.
fn bootstrap_tracking_table(conn: &mut rusqlite::Connection) -> Result<(), Report<JournalError>> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _trouper_journal_migrations (\
             version INTEGER NOT NULL,\
             name TEXT NOT NULL,\
             applied_at TEXT NOT NULL DEFAULT (datetime('now')))",
    )
    .change_context(JournalError::Restore)
    .attach("failed to create the journal migration tracker")?;
    Ok(())
}

/// The highest recorded migration version; `-1` for an empty database.
fn current_version(conn: &mut rusqlite::Connection) -> Result<i32, Report<JournalError>> {
    let version: Option<i32> = conn
        .query_row(
            "SELECT MAX(version) FROM _trouper_journal_migrations",
            [],
            |row| row.get(0),
        )
        .change_context(JournalError::Restore)
        .attach("failed to read the journal migration version")?;
    Ok(version.unwrap_or(-1))
}

/// Records a completed migration in the tracker.
fn record_version(
    conn: &mut rusqlite::Connection,
    version: i32,
    name: &str,
) -> Result<(), Report<JournalError>> {
    conn.execute(
        "INSERT INTO _trouper_journal_migrations (version, name) VALUES (?, ?)",
        rusqlite::params![version, name],
    )
    .change_context(JournalError::Restore)
    .attach("failed to record the journal migration version")?;
    Ok(())
}

// ── Shared state ─────────────────────────────────────────────────────────

/// Number of leading entries in a path's in-memory journal that are durable
/// in SQLite.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FlushedEntryCount(usize);

impl FlushedEntryCount {
    fn start() -> Self {
        Self(0)
    }

    fn through(end: usize) -> Self {
        Self(end)
    }

    fn as_usize(self) -> usize {
        self.0
    }
}

/// One path's authoritative in-memory journal plus its durable prefix.
struct PathBuffer {
    journal: Journal,
    /// `journal.entries()[..flushed_entry_count]` is durable in SQLite.
    flushed_entry_count: FlushedEntryCount,
    /// A passivation flush failed: remove this buffer after a later flush
    /// commits the same captured end without a concurrent append/reactivation.
    drop_when_durable: bool,
    /// A cold read is building this path's authoritative journal. Appends
    /// reject the placeholder rather than racing the seed.
    activation_in_progress: bool,
}

impl PathBuffer {
    fn new() -> Self {
        Self {
            journal: Journal::new(),
            flushed_entry_count: FlushedEntryCount::start(),
            drop_when_durable: false,
            activation_in_progress: false,
        }
    }

    fn activating() -> Self {
        Self {
            activation_in_progress: true,
            ..Self::new()
        }
    }

    fn durable(journal: Journal) -> Self {
        Self {
            flushed_entry_count: FlushedEntryCount::through(journal.len()),
            journal,
            drop_when_durable: false,
            activation_in_progress: false,
        }
    }
}

/// The state every store method shares: the per-path buffers (appends
/// take ONLY this lock — that is the entire performance point), the
/// drain lock serializing every SQLite-writing path, the pool, and the
/// globally monotonic ingest counter seeded from the stored maximum.
#[doc(hidden)]
pub struct Shared {
    journals: parking_lot::Mutex<HashMap<ActorPath, PathBuffer>>,
    /// Serializes tick / flush / passivated-flush / read paths so each
    /// sees one consistent DB+buffer view. Appends never take it.
    drain: tokio::sync::Mutex<()>,
    pool: daow::Pool,
    control: Option<ControlHandler>,
    flush_interval: Duration,
    /// Globally monotonic arrival order, seeded from
    /// `MAX(ingest_seq)` at build; never reset by purge.
    ingest_seq: AtomicU64,
    /// Test-only synchronization point after suffix capture and before SQLite.
    #[cfg(test)]
    pending_capture_hook: parking_lot::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl Shared {
    /// Test seam (`cfg(test)` builds): opens the schema and builds the
    /// shared state without spawning a writer (the writer-exit test
    /// spawns and joins it itself).
    #[doc(hidden)]
    pub async fn test_build(
        pool: daow::Pool,
        config: DaowConfig,
    ) -> Result<Arc<Self>, Report<JournalError>> {
        Ok(Arc::new(Self::build(pool, config, None).await?))
    }

    /// Opens the schema and builds the shared state: run the migration
    /// chain, then seed the ingest counter from the stored maximum so a
    /// fresh store over a populated DB continues the global arrival
    /// order.
    async fn build(
        pool: daow::Pool,
        config: DaowConfig,
        control: Option<ControlHandler>,
    ) -> Result<Self, Report<JournalError>> {
        pool.with_conn(|conn| {
            run_pending(conn).map_err(|report| {
                daow::Error::custom(format!("journal schema setup failed: {report}"))
            })
        })
        .await
        .change_context(JournalError::Restore)
        .attach("journal schema setup failed")?;

        let max_ingest: Option<i64> = pool
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT MAX(ingest_seq) FROM trouper_journal_events",
                    [],
                    |row| row.get(0),
                )
                .map_err(|err| daow::Error::custom(format!("ingest seed read failed: {err}")))
            })
            .await
            .change_context(JournalError::Restore)
            .attach("journal ingest seed failed")?;

        Ok(Self {
            journals: parking_lot::Mutex::new(HashMap::new()),
            drain: tokio::sync::Mutex::new(()),
            pool,
            control,
            flush_interval: config.flush_interval,
            // Seeded ABOVE the stored max: the next ingest hands out
            // max+1 (a fresh store over an empty DB starts at 0).
            ingest_seq: AtomicU64::new(max_ingest.map(|m| m as u64).unwrap_or(0) + 1),
            #[cfg(test)]
            pending_capture_hook: parking_lot::Mutex::new(None),
        })
    }

    #[cfg(test)]
    fn run_pending_capture_hook(&self) {
        let hook = self.pending_capture_hook.lock().take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn set_pending_capture_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.pending_capture_hook.lock() = Some(hook);
    }

    /// Test seam: marks `path` as cold-activating so integration tests
    /// can verify every mutation boundary without racing the SQLite read.
    #[cfg(any(test, feature = "daow"))]
    #[doc(hidden)]
    pub fn mark_activation_for_test(&self, path: &ActorPath) {
        self.journals
            .lock()
            .entry(path.clone())
            .or_insert_with(PathBuffer::activating);
    }

    /// Reports a failure with no caller to the control handler (and
    /// tracing). The handler runs inline: keep it cheap.
    fn report(&self, op: &'static str, report: &Report<JournalError>) {
        tracing::error!(op, error = %report, "journal store failure");
        if let Some(control) = &self.control {
            control(StoreControlMessage::Error {
                op,
                detail: format!("{report}"),
            });
        }
    }

    /// Spawns the writer task: a fixed-interval flush loop holding the
    /// store `Weak`-ly, so it exits when the store drops (no leak). The
    /// first tick fires ONE FULL INTERVAL after spawn (never immediately
    /// at boot — a first tick racing the host's startup would flush
    /// before the host finished installing) and `Delay` prevents burst
    /// catch-up after a long block.
    #[doc(hidden)]
    pub fn spawn_writer(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        let interval = self.flush_interval;
        tokio::spawn(async move {
            let mut tick =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(shared) = weak.upgrade() else {
                    return; // store dropped: writer retires
                };
                if let Err(report) = flush_cycle(&shared).await {
                    shared.report("tick", &report);
                }
            }
        })
    }
}

/// The SQLite-backed [`JournalStore`]: ack-path buffering in front of a
/// `daow::Pool`, with a writer task flushing on [`DaowConfig::flush_interval`].
pub struct DaowJournalStore {
    shared: Arc<Shared>,
    /// The writer task; aborted on drop. `None` after `Drop` ran (or in
    /// tests, after the handle was taken via the test seam).
    writer: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl std::fmt::Debug for DaowJournalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaowJournalStore")
            .field("name", &Self::name_static())
            .finish_non_exhaustive()
    }
}

impl DaowJournalStore {
    /// The backend's name (mirrors [`JournalStore::name`] as an
    /// associated fn for the `Debug` impl).
    fn name_static() -> &'static str {
        "daow-sqlite"
    }

    /// Test seam (`cfg(test)` builds): assembles the concrete store
    /// WITHOUT a writer task — the writer-exit test owns the writer's
    /// join handle itself, so it can observe the task after the store
    /// drops.
    #[cfg(any(test, feature = "daow"))]
    pub fn no_writer(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            writer: parking_lot::Mutex::new(None),
        }
    }

    /// Test seam: installs the same cold-activation placeholder used by
    /// `load`, without racing the SQLite read.
    #[cfg(any(test, feature = "daow"))]
    #[doc(hidden)]
    pub fn mark_activation_for_test(&self, path: &ActorPath) {
        self.shared.mark_activation_for_test(path);
    }
}

impl Drop for DaowJournalStore {
    fn drop(&mut self) {
        // Sync drop cannot await a final flush — the graceful shutdown
        // sweep is the durability path. Aborting the writer is what
        // makes it terminate when the store drops (no task leak).
        if let Some(handle) = self.writer.lock().take() {
            handle.abort();
        }
    }
}

// ── Ack paths (buffer only; never SQLite, never the drain lock) ──────────

fn activation_error(context: JournalError) -> Report<JournalError> {
    Report::new(context).attach("cannot append while the journal is activating")
}

fn mutable_buffer<'a>(
    journals: &'a mut HashMap<ActorPath, PathBuffer>,
    path: &ActorPath,
    context: JournalError,
) -> Result<&'a mut PathBuffer, Report<JournalError>> {
    let buffer = journals.entry(path.clone()).or_insert_with(PathBuffer::new);
    if buffer.activation_in_progress {
        return Err(activation_error(context));
    }
    Ok(buffer)
}

fn buffer_append(
    shared: &Shared,
    path: &ActorPath,
    events: &[Event],
) -> Result<Vec<SeqNo>, Report<JournalError>> {
    let mut journals = shared.journals.lock();
    let buffer = mutable_buffer(&mut journals, path, JournalError::Append)?;
    let journal = &mut buffer.journal;
    Ok(events
        .iter()
        .map(|ev| {
            let ingest = shared.ingest_seq.fetch_add(1, Ordering::SeqCst);
            journal.append_event(ev.clone(), ingest)
        })
        .collect())
}

fn buffer_append_snapshot(
    shared: &Shared,
    path: &ActorPath,
    seq: SeqNo,
    state: crate::json::Json,
    now_ms: u64,
) -> Result<(), Report<JournalError>> {
    let mut journals = shared.journals.lock();
    let buffer = mutable_buffer(&mut journals, path, JournalError::Snapshot)?;
    buffer.journal.append_snapshot(seq, state, now_ms);
    Ok(())
}

// ── Flush machinery ──────────────────────────────────────────────────────

/// One path's captured suffix and the frontier value to install after commit.
struct PendingFlush {
    path: ActorPath,
    events: Vec<PendingEventRow>,
    snapshot: Option<PendingSnapshotRow>,
    captured_end: FlushedEntryCount,
    drop_when_durable: bool,
}

/// Post-transaction metadata that does not own the rows sent to SQLite.
struct FlushCompletion {
    path: ActorPath,
    captured_end: FlushedEntryCount,
    drop_when_durable: bool,
}

/// One event row as it travels buffer → SQLite.
#[derive(Clone)]
struct PendingEventRow {
    seq: SeqNo,
    kind: i64,
    schema: String,
    payload: String,
    origin_json: String,
    catchup_source: Option<String>,
    catchup_seq: Option<i64>,
    ingest_seq: u64,
}

/// One snapshot row as it travels buffer → SQLite.
#[derive(Clone)]
struct PendingSnapshotRow {
    seq: SeqNo,
    state: String,
}

/// Decodes JSON text into the runtime's [`Json`] tree.
fn json_from_text(text: &str) -> Result<crate::json::Json, Report<JournalError>> {
    serde_json::from_str::<serde_json::Value>(text)
        .map(crate::json::Json::from)
        .change_context(JournalError::Load)
        .attach("stored JSON did not decode")
}

/// The event row's kind code: 0 = Recorded, 1 = CatchUp (the scan
/// filter's DB-side discriminator).
const KIND_RECORDED: i64 = 0;
const KIND_CATCHUP: i64 = 1;

fn serialize_event(row: &JournalEntry) -> Option<PendingEventRow> {
    let JournalEntry::Event {
        seq,
        event,
        origin,
        ingest_seq,
    } = row
    else {
        return None;
    };
    let wire = WireEvent::from(event);
    let (kind, catchup_source, catchup_seq) = match origin {
        EventOrigin::Recorded => (KIND_RECORDED, None, None),
        EventOrigin::CatchUp { source, source_seq } => (
            KIND_CATCHUP,
            Some(source.to_string()),
            Some(source_seq.as_u64() as i64),
        ),
    };
    Some(PendingEventRow {
        seq: *seq,
        kind,
        schema: event.schema.as_str().to_owned(),
        payload: std::str::from_utf8(wire.payload.as_bytes())
            .expect("wire payloads are JSON text")
            .to_owned(),
        origin_json: serde_json::to_string(origin).expect("origin serializes"),
        catchup_source,
        catchup_seq,
        ingest_seq: *ingest_seq,
    })
}

/// Materializes every requested path's unflushed journal suffix and releases
/// the journals lock before any DB work. Old durable entries are never visited.
fn collect_pending(shared: &Shared, target: Option<&ActorPath>) -> Vec<PendingFlush> {
    let journals = shared.journals.lock();
    journals
        .iter()
        .filter(|(path, _)| target.is_none_or(|target| *path == target))
        .filter_map(|(path, buffer)| {
            let start = buffer.flushed_entry_count.as_usize();
            let end = buffer.journal.len();
            if start == end {
                return None;
            }
            let mut events = Vec::new();
            let mut snapshot = None;
            for entry in &buffer.journal.entries()[start..end] {
                match entry {
                    JournalEntry::Event { .. } => {
                        if let Some(row) = serialize_event(entry) {
                            events.push(row);
                        }
                    }
                    JournalEntry::Snapshot { seq, state } => {
                        snapshot = Some(PendingSnapshotRow {
                            seq: *seq,
                            state: state.to_string(),
                        });
                    }
                }
            }
            Some(PendingFlush {
                path: path.clone(),
                events,
                snapshot,
                captured_end: FlushedEntryCount::through(end),
                drop_when_durable: buffer.drop_when_durable,
            })
        })
        .collect()
}

/// Writes one cycle's pending rows in a single transaction (all paths or
/// one path — the caller's `pending` decides), advancing durable frontiers
/// only on success. On failure frontiers stay put, so the next cycle
/// retries exactly the same entries.
async fn write_pending(
    shared: &Shared,
    pending: Vec<PendingFlush>,
    op: &'static str,
) -> Result<(), Report<JournalError>> {
    if pending.is_empty() {
        return Ok(());
    }
    #[cfg(test)]
    shared.run_pending_capture_hook();
    let completions = pending
        .iter()
        .map(|batch| FlushCompletion {
            path: batch.path.clone(),
            captured_end: batch.captured_end,
            drop_when_durable: batch.drop_when_durable,
        })
        .collect();
    write_pending_tx(&shared.pool, pending)
        .await
        .change_context(JournalError::Append)
        .attach(format!("{op}: journal flush failed; entries stay buffered"))?;

    commit_flush(shared, completions);
    Ok(())
}

fn commit_flush(shared: &Shared, completions: Vec<FlushCompletion>) {
    let mut journals = shared.journals.lock();
    for completion in completions {
        let Some(buffer) = journals.get_mut(&completion.path) else {
            continue;
        };
        buffer.flushed_entry_count = completion.captured_end;
        if buffer.drop_when_durable && completion.drop_when_durable {
            if buffer.journal.len() == completion.captured_end.as_usize() {
                journals.remove(&completion.path);
            } else {
                buffer.drop_when_durable = false;
            }
        }
    }
}

/// The transaction: events (multi-row) + snapshots (upsert) in one
/// `BEGIN IMMEDIATE … COMMIT` on a single pooled connection.
async fn write_pending_tx(
    pool: &daow::Pool,
    pending: Vec<PendingFlush>,
) -> Result<(), daow::Error> {
    pool.with_conn(move |conn| {
        conn.execute_batch("BEGIN IMMEDIATE")?;
        let write_result = write_pending_statements(conn, &pending);
        let result = match write_result {
            Ok(()) => conn.execute_batch("COMMIT"),
            Err(err) => {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(daow::Error::from(err));
            }
        };
        if let Err(err) = result {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(daow::Error::from(err));
        }
        Ok(())
    })
    .await
}

fn insert_event(
    conn: &mut rusqlite::Connection,
    batch: &PendingFlush,
    row: &PendingEventRow,
) -> Result<(), rusqlite::Error> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO trouper_journal_events \
         (journal, seq, kind, schema, payload, origin, catchup_source, catchup_seq, ingest_seq) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?;
    stmt.execute(rusqlite::params![
        batch.path.to_string(),
        row.seq.as_u64() as i64,
        row.kind,
        row.schema,
        row.payload,
        row.origin_json,
        row.catchup_source,
        row.catchup_seq,
        row.ingest_seq as i64,
    ])?;
    Ok(())
}

fn verify_existing_event(
    conn: &mut rusqlite::Connection,
    batch: &PendingFlush,
    row: &PendingEventRow,
) -> Result<(), rusqlite::Error> {
    let stored = conn.query_row(
        "SELECT kind, schema, payload, origin, catchup_source, catchup_seq, ingest_seq \
         FROM trouper_journal_events WHERE journal = ?1 AND seq = ?2",
        rusqlite::params![batch.path.to_string(), row.seq.as_u64() as i64],
        |stored| {
            Ok((
                stored.get::<_, i64>(0)?,
                stored.get::<_, String>(1)?,
                stored.get::<_, String>(2)?,
                stored.get::<_, String>(3)?,
                stored.get::<_, Option<String>>(4)?,
                stored.get::<_, Option<i64>>(5)?,
                stored.get::<_, i64>(6)?,
            ))
        },
    )?;
    let matches = stored
        == (
            row.kind,
            row.schema.clone(),
            row.payload.clone(),
            row.origin_json.clone(),
            row.catchup_source.clone(),
            row.catchup_seq,
            row.ingest_seq as i64,
        );
    if matches {
        Ok(())
    } else {
        Err(rusqlite::Error::InvalidParameterName(
            "event row conflicts at an existing journal sequence".to_owned(),
        ))
    }
}

fn write_pending_statements(
    conn: &mut rusqlite::Connection,
    pending: &[PendingFlush],
) -> Result<(), rusqlite::Error> {
    for batch in pending {
        for row in &batch.events {
            match insert_event(conn, batch, row) {
                Ok(()) => {}
                Err(err)
                    if err.sqlite_error_code()
                        == Some(rusqlite::ErrorCode::ConstraintViolation) =>
                {
                    verify_existing_event(conn, batch, row)?;
                }
                Err(err) => return Err(err),
            }
        }
        if let Some(snapshot) = &batch.snapshot {
            let mut stmt = conn.prepare_cached(
                "INSERT OR REPLACE INTO trouper_journal_snapshots (journal, seq, state) \
                 VALUES (?1, ?2, ?3)",
            )?;
            stmt.execute(rusqlite::params![
                batch.path.to_string(),
                snapshot.seq.as_u64() as i64,
                snapshot.state
            ])?;
        }
    }
    Ok(())
}

/// One flush cycle: drain lock → snapshot pending → one transaction →
/// advance durable frontiers. Shared by the writer tick and `flush`.
async fn flush_cycle(shared: &Shared) -> Result<(), Report<JournalError>> {
    let _guard = shared.drain.lock().await;
    let pending = collect_pending(shared, None);
    write_pending(shared, pending, "tick").await
}

// ── DB-side row reads ────────────────────────────────────────────────────

/// One stored event row read back from SQLite.
struct StoredEventRow {
    seq: i64,
    /// Kept as the decoded origin discriminator's input via `origin_json`;
    /// the kind column itself is only read by SQL filters.
    #[allow(dead_code)]
    kind: i64,
    schema: String,
    payload: String,
    origin_json: String,
    ingest_seq: i64,
}

impl StoredEventRow {
    /// Decodes into the replay shape (the boundary-decode contract: the
    /// payload stays wire bytes; the typed decode happens at fold).
    fn into_journaled(self) -> Result<JournaledEvent, Report<JournalError>> {
        let origin: EventOrigin = serde_json::from_str(&self.origin_json)
            .change_context(JournalError::Load)
            .attach("stored event origin did not decode")?;
        let schema = crate::schema::SchemaId::new(&self.schema);
        // The payload column IS the wire's compact JSON TEXT: it becomes
        // the payload bytes verbatim (no re-encode).
        let payload = crate::envelope::PayloadBytes::from_bytes(self.payload.into_bytes());
        let event = Event::try_from(WireEvent { schema, payload })?;
        Ok(JournaledEvent {
            seq: SeqNo::new(self.seq as u64),
            event,
            origin,
            ingest_seq: self.ingest_seq as u64,
        })
    }

    /// Decodes into a scan row (`None` for CatchUp origins — the scan
    /// filter never surfaces re-recorded checkpoints).
    fn into_scanned(self, journal: ActorPath) -> Option<ScannedEvent> {
        let origin: EventOrigin = serde_json::from_str(&self.origin_json).ok()?;
        if origin != EventOrigin::Recorded {
            return None;
        }
        let schema = crate::schema::SchemaId::new(&self.schema);
        let payload = crate::envelope::PayloadBytes::from_bytes(self.payload.into_bytes());
        let event = Event::try_from(WireEvent { schema, payload }).ok()?;
        Some(ScannedEvent {
            journal,
            seq: SeqNo::new(self.seq as u64),
            ingest_seq: self.ingest_seq as u64,
            event,
        })
    }
}

/// Reads one path's event rows (ascending seq) and its latest snapshot.
async fn read_path_rows(
    pool: &daow::Pool,
    path: &ActorPath,
) -> Result<(Vec<StoredEventRow>, Option<(i64, String)>), daow::Error> {
    let journal = path.to_string();
    pool.with_conn(move |conn| {
        let mut stmt = conn.prepare_cached(
            "SELECT seq, kind, schema, payload, origin, ingest_seq \
             FROM trouper_journal_events WHERE journal = ?1 ORDER BY seq ASC",
        )?;
        let events = stmt
            .query_map(rusqlite::params![journal], |row| {
                Ok(StoredEventRow {
                    seq: row.get(0)?,
                    kind: row.get(1)?,
                    schema: row.get(2)?,
                    payload: row.get(3)?,
                    origin_json: row.get(4)?,
                    ingest_seq: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot = conn
            .query_row(
                "SELECT seq, state FROM trouper_journal_snapshots \
                 WHERE journal = ?1 ORDER BY seq DESC LIMIT 1",
                rusqlite::params![journal],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        Ok((events, snapshot))
    })
    .await
}

/// Whether the DB already holds the CatchUp origin `(source, source_seq)`
/// for `journal` — the passivation-correct half of seed idempotence (the
/// buffer's `holds_origin` only sees buffered entries).
async fn db_holds_origin(
    pool: &daow::Pool,
    journal: &ActorPath,
    source: &ActorPath,
    source_seq: SeqNo,
) -> Result<bool, daow::Error> {
    let journal = journal.to_string();
    let source = source.to_string();
    let source_seq = source_seq.as_u64() as i64;
    pool.with_conn(move |conn| {
        let found: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM trouper_journal_events \
                 WHERE journal = ?1 AND kind = ?2 AND catchup_source = ?3 AND catchup_seq = ?4",
                rusqlite::params![journal, KIND_CATCHUP, source, source_seq],
                |row| row.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    })
    .await
}

/// Rebuilds one path's buffer from its stored rows (the passivated →
/// reactivated transition): entries re-append with their stored origins
/// and ingest seqs (seq anchoring and CatchUp checkpoints survive), the
/// latest snapshot rides along as an entry, and the durable frontier marks
/// every stored row committed. `Ok(false)` = nothing stored.
///
/// The drain lock must be held: this reads the DB and inserts into the
/// journals map as one step.
async fn seed_buffer(shared: &Shared, path: &ActorPath) -> Result<bool, Report<JournalError>> {
    {
        let mut journals = shared.journals.lock();
        match journals.entry(path.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(PathBuffer::activating());
            }
            std::collections::hash_map::Entry::Occupied(entry) => {
                if !entry.get().activation_in_progress {
                    return Ok(true);
                }
            }
        }
    }
    let (rows, snapshot) = read_path_rows(&shared.pool, path)
        .await
        .change_context(JournalError::Load)
        .attach("journal seed read failed")?;
    if rows.is_empty() && snapshot.is_none() {
        shared.journals.lock().remove(path);
        return Ok(false);
    }
    let journal = rebuild_journal(rows, snapshot)?;
    let mut journals = shared.journals.lock();
    if let Some(buffer) = journals.get_mut(path)
        && buffer.activation_in_progress
    {
        *buffer = PathBuffer::durable(journal);
    }
    Ok(true)
}

/// Assembles a [`Journal`] from stored rows (events first — seqs are
/// dense and ordered — then the latest snapshot as an entry).
fn rebuild_journal(
    rows: Vec<StoredEventRow>,
    snapshot: Option<(i64, String)>,
) -> Result<Journal, Report<JournalError>> {
    let mut journal = Journal::new();
    for row in rows {
        let journaled = row.into_journaled()?;
        journal.append_event_with_origin(journaled.event, journaled.origin, journaled.ingest_seq);
    }
    if let Some((seq, state)) = snapshot {
        journal.append_snapshot(SeqNo::new(seq as u64), json_from_text(&state)?, 0);
    }
    Ok(journal)
}

/// Builds replay from one authoritative [`Journal`]. Full event history is
/// retained for projector checkpoints; the tail is strictly after the
/// journal's latest snapshot.
fn build_replay(journal: &Journal) -> Result<Option<Replay>, Report<JournalError>> {
    let snapshot = journal.last_snapshot().cloned();
    let events: Vec<JournaledEvent> = journal
        .entries()
        .iter()
        .filter_map(|entry| {
            let JournalEntry::Event {
                seq,
                event,
                origin,
                ingest_seq,
            } = entry
            else {
                return None;
            };
            Some(JournaledEvent {
                seq: *seq,
                event: event.clone(),
                origin: origin.clone(),
                ingest_seq: *ingest_seq,
            })
        })
        .collect();
    if events.is_empty() && snapshot.is_none() {
        return Ok(None);
    }
    let tail = match &snapshot {
        Some(snapshot) => events
            .iter()
            .filter(|event| event.seq > snapshot.seq())
            .map(|event| event.event.clone())
            .collect(),
        None => events.iter().map(|event| event.event.clone()).collect(),
    };
    Ok(Some(Replay {
        snapshot,
        tail,
        events,
    }))
}

#[async_trait::async_trait]
impl JournalStore for DaowJournalStore {
    async fn append(
        &self,
        path: &ActorPath,
        events: &[Event],
    ) -> Result<Vec<SeqNo>, Report<JournalError>> {
        // The ack path: a buffered write, resolved before the command's
        // ack. SQLite contact happens on the writer task only.
        buffer_append(&self.shared, path, events)
    }

    async fn append_catchup(
        &self,
        path: &ActorPath,
        events: &[ScannedEvent],
    ) -> Result<Vec<Option<SeqNo>>, Report<JournalError>> {
        // Two-phase per event (the parking_lot lock is NEVER held across
        // the DB await): (1) under the journals lock — the buffer's
        // checkpoint test; (2) DB EXISTS check (lock released) for
        // origins below the buffer base, i.e. after a passivation; (3)
        // re-lock to append. Appends per path are serialized by the
        // caller, so no other task interleaves on this journal between
        // the phases.
        let mut results = Vec::with_capacity(events.len());
        for scanned in events {
            {
                let mut journals = self.shared.journals.lock();
                mutable_buffer(&mut journals, path, JournalError::Append)?;
            }
            let buffer_known = {
                let journals = self.shared.journals.lock();
                journals
                    .get(path)
                    .map(|buffer| buffer.journal.holds_origin(&scanned.journal, scanned.seq))
                    .unwrap_or(false)
            };
            if buffer_known {
                results.push(None);
                continue;
            }
            // The passivation-correct half of seed idempotence: the DB
            // may already hold the origin the dropped buffer cannot see.
            let known = db_holds_origin(&self.shared.pool, path, &scanned.journal, scanned.seq)
                .await
                .change_context(JournalError::Append)
                .attach("catch-up origin check failed")?;
            if known {
                results.push(None);
                continue;
            }
            let origin = EventOrigin::CatchUp {
                source: scanned.journal.clone(),
                source_seq: scanned.seq,
            };
            let mut journals = self.shared.journals.lock();
            let buffer = mutable_buffer(&mut journals, path, JournalError::Append)?;
            let ingest = self.shared.ingest_seq.fetch_add(1, Ordering::SeqCst);
            let seq =
                buffer
                    .journal
                    .append_event_with_origin(scanned.event.clone(), origin, ingest);
            results.push(Some(seq));
        }
        Ok(results)
    }

    async fn scan(
        &self,
        schemas: &[crate::schema::SchemaId],
    ) -> Result<Vec<ScannedEvent>, Report<JournalError>> {
        // Drain lock: the scan reads DB + buffer as ONE consistent view.
        let _guard = self.shared.drain.lock().await;
        let names: Vec<String> = schemas.iter().map(|s| s.as_str().to_owned()).collect();

        let mut rows = self
            .shared
            .pool
            .with_conn(move |conn| {
                // One `IN (…) WHERE kind = 0` query: the DB-side scan
                // filter (recorded facts only, ascending arrival).
                let placeholders = names.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
                let sql = format!(
                    "SELECT journal, seq, schema, payload, origin, ingest_seq \
                     FROM trouper_journal_events WHERE kind = {KIND_RECORDED} \
                     AND schema IN ({placeholders}) ORDER BY ingest_seq ASC"
                );
                let mut stmt = conn.prepare(&sql)?;
                let mapped = stmt.query_map(rusqlite::params_from_iter(names.iter()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        StoredEventRow {
                            seq: row.get(1)?,
                            kind: KIND_RECORDED, // filtered in SQL (kind = 0)
                            schema: row.get(2)?,
                            payload: row.get(3)?,
                            origin_json: row.get(4)?,
                            ingest_seq: row.get(5)?,
                        },
                    ))
                })?;
                mapped
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(daow::Error::from)
            })
            .await
            .change_context(JournalError::Scan)
            .attach("journal scan failed")?;

        let mut found: Vec<ScannedEvent> = Vec::new();
        for (journal, row) in rows.drain(..) {
            if let Some(scanned) = row.into_scanned(ActorPath::new(journal)) {
                found.push(scanned);
            }
        }

        // Scan union: SQLite rows plus authoritative in-memory journals.
        // Retained failed-passivation buffers contribute their unflushed
        // suffix here; dedup by `(journal, seq)` keeps the set exact.
        let mut seen: HashSet<(ActorPath, u64)> = found
            .iter()
            .map(|s| (s.journal.clone(), s.seq.as_u64()))
            .collect();
        let journals = self.shared.journals.lock();
        for (path, buffer) in journals.iter() {
            for entry in buffer.journal.entries() {
                let JournalEntry::Event {
                    seq,
                    event,
                    origin: EventOrigin::Recorded,
                    ingest_seq,
                } = entry
                else {
                    continue;
                };
                if !schemas.contains(&event.schema) || seen.contains(&(path.clone(), seq.as_u64()))
                {
                    continue;
                }
                seen.insert((path.clone(), seq.as_u64()));
                found.push(ScannedEvent {
                    journal: path.clone(),
                    seq: *seq,
                    ingest_seq: *ingest_seq,
                    event: event.clone(),
                });
            }
        }
        found.sort_by_key(|s| s.ingest_seq);
        Ok(found)
    }

    async fn append_snapshot(
        &self,
        path: &ActorPath,
        seq: SeqNo,
        state: crate::json::Json,
        now_ms: u64,
    ) -> Result<(), Report<JournalError>> {
        buffer_append_snapshot(&self.shared, path, seq, state, now_ms)
    }

    async fn load(&self, path: &ActorPath) -> Result<Option<Replay>, Report<JournalError>> {
        // The drain lock serializes cold seeding, flush, passivation, and
        // replay so activation observes one authoritative view.
        let _guard = self.shared.drain.lock().await;

        // A retained path is already the complete authoritative view. Loading
        // it means reactivation: cancel any pending failed-passivation
        // release marker without querying SQLite again.
        if let Some(buffer) = self.shared.journals.lock().get_mut(path)
            && !buffer.activation_in_progress
        {
            buffer.drop_when_durable = false;
            return build_replay(&buffer.journal);
        }

        // Cold or failed-previous seed: one SQLite read becomes the
        // authoritative buffer, whose frontier starts at its full length
        // because every entry is durable.
        seed_buffer(&self.shared, path).await?;
        let journals = self.shared.journals.lock();
        journals
            .get(path)
            .map(|buffer| build_replay(&buffer.journal))
            .unwrap_or(Ok(None))
    }

    async fn flush(&self) -> Result<(), Report<JournalError>> {
        // The sweep calls this once, after every journal is final, and
        // IGNORES the result by design — failures report through the
        // control handler here so the host observes them after the
        // sweep returns.
        let _guard = self.shared.drain.lock().await;
        let pending = collect_pending(&self.shared, None);
        if let Err(report) = write_pending(&self.shared, pending, "flush").await {
            self.shared.report("flush", &report);
            return Err(report);
        }
        Ok(())
    }

    async fn passivated(&self, path: &ActorPath) -> Result<(), Report<JournalError>> {
        // Cold storage: flush this path, but never discard acknowledged
        // entries on failure. The actor loop treats this as a non-blocking
        // hint and passivates either way.
        let _guard = self.shared.drain.lock().await;
        let pending = collect_pending(&self.shared, Some(path));
        if pending.is_empty() {
            self.shared.journals.lock().remove(path);
            return Ok(());
        }
        {
            let mut journals = self.shared.journals.lock();
            if let Some(buffer) = journals.get_mut(path) {
                buffer.drop_when_durable = true;
            }
        }
        if let Err(report) = write_pending(&self.shared, pending, "passivated").await {
            self.shared.report("passivated", &report);
            return Err(report);
        }
        Ok(())
    }

    async fn purge(&self, path: &ActorPath) -> Result<(), Report<JournalError>> {
        let _guard = self.shared.drain.lock().await;
        let journal = path.to_string();
        self.shared
            .pool
            .with_conn(move |conn| {
                let mut events =
                    conn.prepare_cached("DELETE FROM trouper_journal_events WHERE journal = ?1")?;
                events.execute(rusqlite::params![journal])?;
                let mut snapshots = conn
                    .prepare_cached("DELETE FROM trouper_journal_snapshots WHERE journal = ?1")?;
                snapshots.execute(rusqlite::params![journal])?;
                Ok(())
            })
            .await
            .change_context(JournalError::Purge)
            .attach("journal purge failed")?;
        // The ingest counter never resets: arrival-order history is
        // append-only (trait contract).
        self.shared.journals.lock().remove(path);
        Ok(())
    }

    fn name(&self) -> &'static str {
        Self::name_static()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::PayloadBytes;
    use crate::json;
    use crate::schema::SchemaId;

    fn event(n: i64) -> Event {
        Event::from_bytes(
            SchemaId::new("ConcurrentFlush"),
            PayloadBytes::from(json!({ "n": n })),
        )
    }

    fn event_count(runtime: &tokio::runtime::Runtime, pool: &daow::Pool, path: &str) -> i64 {
        let path = path.to_owned();
        runtime
            .block_on(pool.with_conn(move |conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM trouper_journal_events WHERE journal = ?1",
                    rusqlite::params![path],
                    |row| row.get(0),
                )
                .map_err(daow::Error::from)
            }))
            .expect("count events")
    }

    #[test]
    fn append_after_pending_capture_remains_pending_for_next_flush() {
        // Given one event captured by a flush that pauses before SQLite.
        let dir = tempfile::tempdir().expect("tempdir");
        let pool = daow::Pool::builder()
            .path(dir.path().join("journal.db").display().to_string())
            .max_size(2)
            .build()
            .expect("pool");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let shared = runtime
            .block_on(Shared::test_build(pool.clone(), DaowConfig::default()))
            .expect("shared");
        let store = Arc::new(DaowJournalStore::no_writer(Arc::clone(&shared)));
        let path = ActorPath::new("concurrent-flush");
        runtime
            .block_on(store.append(&path, &[event(1)]))
            .expect("initial append");
        let captured = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::clone(&captured);
        shared.set_pending_capture_hook(Arc::new(move || {
            release.wait();
        }));
        let flush_store = Arc::clone(&store);
        let flush = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("flush runtime")
                .block_on(flush_store.flush())
        });

        // When another append lands after capture but before commit.
        captured.wait();
        runtime
            .block_on(store.append(&path, &[event(2)]))
            .expect("concurrent append");
        flush.join().expect("flush thread").expect("first flush");

        // Then only the captured event is durable, while replay contains both;
        // the next flush persists the newly pending suffix.
        assert_eq!(event_count(&runtime, &pool, "concurrent-flush"), 1);
        let replay = runtime
            .block_on(store.load(&path))
            .expect("load")
            .expect("replay");
        assert_eq!(replay.events.len(), 2, "both buffered events replay");
        runtime.block_on(store.flush()).expect("second flush");
        assert_eq!(event_count(&runtime, &pool, "concurrent-flush"), 2);
    }
}
