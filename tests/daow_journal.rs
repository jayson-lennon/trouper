#![cfg(feature = "daow")]
//! Integration tests for the SQLite journal backend: schema migrations,
//! buffered ack path, periodic flush, restart replay, passivation cold
//! storage, purge, scan unions, and the control-closure failure surface.
//!
//! Two live systems over ONE database file prove the restart story; a
//! `RAISE(ABORT)` trigger on the events table is the deterministic DB
//! failure injection (installed/removed through the pool's raw
//! connection), and the control channel is read only AFTER the calls
//! whose failures it reports.

use std::sync::Arc;
use std::time::Duration;

use trouper::actor::{ActorKind, ActorPath as Path, EventSourcedActor};
use trouper::builder::spawn_es_builder;
use trouper::context::CmdCtx;
use trouper::envelope::Events;
use trouper::journal::{JournalArgs, SeqNo, StoreControlMessage};
use trouper::journal_daow::DaowConfig;
use trouper::schema::{ActorManifest, Command, Schema};
use trouper::system::{ActorSystem, SystemConfig};
use trouper::{json, json::Json};

// ── Fixtures ─────────────────────────────────────────────────────────────

#[derive(Clone, Command, serde::Serialize, serde::Deserialize)]
struct Tick {
    n: i64,
}

#[derive(trouper::Event, serde::Serialize, serde::Deserialize, Clone)]
struct Ticked {
    n: i64,
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct Accum {
    count: u64,
    last: i64,
}

impl EventSourcedActor for Accum {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<Tick>()
            .emits::<Ticked>()
            .kind(ActorKind::EventSourced)
    }
    fn restore(_args: &Json) -> Self {
        Self::default()
    }
    fn apply(&mut self, event: &trouper::envelope::Event) {
        if event.schema.as_str() == "Ticked" {
            self.count += 1;
            self.last = event.payload_json()["n"].as_i64().unwrap_or(0);
        }
    }
}
impl trouper::actor::CommandHandler<Tick> for Accum {
    fn handle(&self, cmd: Tick, _ctx: &mut CmdCtx<'_>) -> Events {
        Events::from_vec(vec![trouper::envelope::Event::from_json_view(
            Ticked::schema_id(),
            Json::of(&Ticked { n: cmd.n }),
        )])
    }
}

// ── Harness ──────────────────────────────────────────────────────────────

/// An on-disk SQLite file that removes itself on drop.
struct TestDb {
    file: tempfile::TempDir,
}

impl TestDb {
    fn new() -> Self {
        let file = tempfile::tempdir().expect("tempdir");
        Self { file }
    }
    /// The filesystem path daow pools open.
    fn path_str(&self) -> String {
        self.file.path().join("journal.db").display().to_string()
    }
    /// A pool over this database (daow's builder validates eagerly).
    fn pool(&self) -> daow::Pool {
        daow::Pool::builder()
            .path(self.path_str())
            .max_size(2)
            .build()
            .expect("pool")
    }
}

/// A pool over `:memory:` (daow forces max_size = 1 there).
fn memory_pool() -> daow::Pool {
    daow::Pool::builder()
        .path(":memory:")
        .build()
        .expect("pool")
}

/// A store built directly (no `ActorSystem`) — the store-level tests.
async fn memory_store(config: DaowConfig) -> Arc<dyn trouper::journal::JournalStore> {
    JournalArgs::daow(memory_pool(), config)
        .build()
        .await
        .expect("store builds")
        .store
}

/// Builds the concrete daow store directly and hands back its writer
/// handle (the writer-exit test's seam — the concrete Arc is reachable
/// before anything clones it).
async fn test_store_with_writer(
    pool: daow::Pool,
    config: DaowConfig,
) -> (
    Arc<dyn trouper::journal::JournalStore>,
    tokio::task::JoinHandle<()>,
) {
    let shared = trouper::journal_daow::Shared::test_build(pool, config)
        .await
        .expect("test shared");
    // Spawn the writer OUTSIDE the store: the store is built with
    // `no_writer` and the test owns the join handle directly (sync Drop
    // aborts only the store-owned handle; here the test joins it).
    let writer = shared.spawn_writer();
    let store: Arc<dyn trouper::journal::JournalStore> =
        Arc::new(trouper::journal_daow::DaowJournalStore::no_writer(shared));
    (store, writer)
}

/// An event the fixture schemas use for direct store appends.
fn event(qty: i64) -> trouper::envelope::Event {
    trouper::envelope::Event::from_json_view(
        trouper::schema::SchemaId::new("StockReserved"),
        json!({ "qty": qty }),
    )
}

/// Installs a trigger that makes ANY insert into the events table fail
/// (SQLite's deterministic, pool-independent failure injection).
async fn install_fault(pool: &daow::Pool) {
    pool.with_conn(|conn| {
        conn.execute_batch(
            "CREATE TRIGGER trouper_test_fault BEFORE INSERT ON trouper_journal_events \
             BEGIN SELECT RAISE(ABORT, 'test fault'); END;",
        )
        .map_err(daow::Error::from)
    })
    .await
    .expect("fault trigger installed");
}

/// Removes the fault trigger.
async fn clear_fault(pool: &daow::Pool) {
    pool.with_conn(|conn| {
        conn.execute_batch("DROP TRIGGER trouper_test_fault;")
            .map_err(daow::Error::from)
    })
    .await
    .expect("fault trigger removed");
}

/// The number of event rows in the database (sync read through the pool).
async fn db_event_count(pool: &daow::Pool, journal: &str) -> i64 {
    let journal = journal.to_owned();
    pool.with_conn(move |conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM trouper_journal_events WHERE journal = ?1",
            rusqlite::params![journal],
            |row| row.get(0),
        )
        .map_err(|err| daow::Error::custom(format!("{err}")))
    })
    .await
    .expect("count")
}

/// A live system over `db` with a SHORT flush interval.
async fn daow_system(db: &TestDb) -> ActorSystem {
    let args = JournalArgs::daow(db.pool(), DaowConfig::default())
        .build()
        .await
        .expect("journal args");
    ActorSystem::new(SystemConfig::production().with_journal(args))
}

/// A live system over `db` with a LONG flush interval (60s): anything in
/// the DB right after `shutdown_graceful` proves the sweep flushed.
async fn daow_system_slow_flush(db: &TestDb) -> ActorSystem {
    let args = JournalArgs::daow(
        db.pool(),
        DaowConfig {
            flush_interval: Duration::from_secs(60),
        },
    )
    .build()
    .await
    .expect("journal args");
    ActorSystem::new(SystemConfig::production().with_journal(args))
}

/// Spawns an `Accum` at `path` and waits until it is live.
async fn spawn_accum(system: &ActorSystem, path: &Path) {
    spawn_es_builder::<Accum>(system)
        .at(path.clone())
        .handles::<Tick>()
        .emits::<Ticked>()
        .start();
    for _ in 0..5_000 {
        if system
            .with_es_state::<Accum, _>(path, |a| a.count)
            .await
            .is_some()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("actor never went live: {path}");
}

/// Delivers one `Tick` and waits for its commit (the inbox cursor).
async fn tick(system: &ActorSystem, path: &Path, n: i64) {
    let base = system.inbox_cursor(path).map(|c| c.as_u64()).unwrap_or(0);
    system
        .tell(path.clone(), Tick { n })
        .await
        .expect("tell accepted");
    for _ in 0..200_000_000 {
        if system.inbox_cursor(path).map(|c| c.as_u64()).unwrap_or(0) > base {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("commit wait stalled: {path}");
}

// ── Migrations ───────────────────────────────────────────────────────────

#[tokio::test]
async fn migration_chain_applies_once_and_twice_is_a_noop() {
    // Given a fresh on-disk database with its pool open.
    let db = TestDb::new();
    let pool = db.pool();

    // When the schema is set up twice (two stores over one file — the
    // restart case).
    for _ in 0..2 {
        trouper::journal::JournalArgs::daow(pool.clone(), DaowConfig::default())
            .build()
            .await
            .expect("build");
    }

    // Then the schema exists exactly once (no duplicate-table error on
    // the second build) and the tracker records v0 exactly once.
    let (table_count, version_rows): (i64, i64) = pool
        .with_conn(|conn| {
            let tables: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' \
                     AND name IN ('trouper_journal_events', 'trouper_journal_snapshots')",
                    [],
                    |row| row.get(0),
                )
                .map_err(|err| daow::Error::custom(format!("{err}")))?;
            let versions: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM _trouper_journal_migrations",
                    [],
                    |row| row.get(0),
                )
                .map_err(|err| daow::Error::custom(format!("{err}")))?;
            Ok((tables, versions))
        })
        .await
        .expect("schema read");
    assert_eq!(table_count, 2, "both journal tables exist");
    assert_eq!(version_rows, 1, "second build was a no-op, not a re-run");
}

#[tokio::test]
async fn migration_coexists_with_a_foreign_migrations_table() {
    // Given a database already carrying a jinn-style `_migrations` table
    // at version 28 (the name-collision hazard).
    let db = TestDb::new();
    let pool = db.pool();
    pool.with_conn(|conn| {
        conn.execute_batch(
            "CREATE TABLE _migrations (version INTEGER NOT NULL, name TEXT NOT NULL, \
             applied_at TEXT NOT NULL DEFAULT (datetime('now')));\
             INSERT INTO _migrations (version, name) VALUES (28, 'foreign');",
        )
        .map_err(daow::Error::from)
    })
    .await
    .expect("foreign table seeded");

    // When the journal schema installs.
    trouper::journal::JournalArgs::daow(pool.clone(), DaowConfig::default())
        .build()
        .await
        .expect("build");

    // Then the foreign version space is untouched and the journal's own
    // tracker holds its own v0.
    pool.with_conn(|conn| {
        let foreign: i64 = conn
            .query_row("SELECT MAX(version) FROM _migrations", [], |row| row.get(0))
            .map_err(|err| daow::Error::custom(format!("{err}")))?;
        assert_eq!(foreign, 28, "jinn-style table untouched");
        let ours: i64 = conn
            .query_row(
                "SELECT MAX(version) FROM _trouper_journal_migrations",
                [],
                |row| row.get(0),
            )
            .map_err(|err| daow::Error::custom(format!("{err}")))?;
        assert_eq!(ours, 0, "journal tracker reached its own latest");
        Ok(())
    })
    .await
    .expect("version read");
}

#[tokio::test]
async fn failed_migration_rolls_back_to_no_half_schema() {
    // Given a database whose journal v0 DDL cannot succeed (a foreign
    // table already owns the name — strict DDL fails loudly).
    let db = TestDb::new();
    let pool = db.pool();
    pool.with_conn(|conn| {
        conn.execute_batch("CREATE TABLE trouper_journal_events (junk TEXT);")
            .map_err(daow::Error::from)
    })
    .await
    .expect("pre-collision table");

    // When a store build attempts the schema.
    let result = trouper::journal::JournalArgs::daow(pool.clone(), DaowConfig::default())
        .build()
        .await;

    // Then the build fails AND no version was recorded — the tracker
    // stays empty so a later fixed run re-applies from scratch.
    assert!(result.is_err(), "name collision must fail loudly");
    pool.with_conn(|conn| {
        let versions: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _trouper_journal_migrations",
                [],
                |row| row.get(0),
            )
            .map_err(|err| daow::Error::custom(format!("{err}")))?;
        assert_eq!(versions, 0, "rolled back: nothing recorded");
        Ok(())
    })
    .await
    .expect("tracker read");
}

// ── Store-level behavior ─────────────────────────────────────────────────

#[tokio::test]
async fn fresh_store_continues_the_global_ingest_order() {
    // Given a store that ingested events and was dropped.
    let db = TestDb::new();
    let pool = db.pool();
    {
        let args = JournalArgs::daow(pool.clone(), DaowConfig::default())
            .build()
            .await
            .expect("build");
        args.store
            .append(&Path::new("a"), &[event(1), event(2)])
            .await
            .expect("append");
        args.store.flush().await.expect("flush");
    }

    // When a fresh store opens the same database and appends.
    let args = JournalArgs::daow(pool, DaowConfig::default())
        .build()
        .await
        .expect("rebuild");
    args.store
        .append(&Path::new("b"), &[event(3)])
        .await
        .expect("append b");

    // Then the new append's ingest continues ABOVE the stored max — a
    // reset counter would have reused low values.
    let replay_b = args
        .store
        .load(&Path::new("b"))
        .await
        .expect("load")
        .expect("journal b");
    let replay_a = args
        .store
        .load(&Path::new("a"))
        .await
        .expect("load")
        .expect("journal a");
    let max_a = replay_a
        .events
        .iter()
        .map(|e| e.ingest_seq)
        .max()
        .expect("a");
    let b0 = replay_b.events.first().expect("b0").ingest_seq;
    assert!(
        b0 > max_a,
        "ingest continues above the stored max ({b0} > {max_a})"
    );
}

#[tokio::test]
async fn buffered_append_fails_the_write_and_the_tick_reports_and_recovers() {
    // Given a store whose writer will fail (a RAISE(ABORT) trigger) with
    // the control closure installed.
    let db = TestDb::new();
    let pool = db.pool();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let args = JournalArgs::daow(pool.clone(), DaowConfig::default())
        .with_control(Arc::new(move |msg| {
            let _ = tx.send(msg);
        }))
        .build()
        .await
        .expect("build");
    install_fault(&pool).await;
    let path = Path::new("faulty");

    // When events append (the ack path succeeds — buffered) and a manual
    // flush hits the fault.
    args.store
        .append(&path, &[event(1), event(2)])
        .await
        .expect("buffered");
    let flushed = args.store.flush().await;

    // Then the flush FAILS, the control closure reports it, and the load
    // still sees both events (the buffer was retained, not dropped).
    assert!(flushed.is_err(), "flush must fail while the fault stands");
    let msg = rx
        .try_recv()
        .expect("control closure notified of the flush failure");
    let op = match msg {
        StoreControlMessage::Error { op, .. } => op,
    };
    assert_eq!(op, "flush", "the failing op is named");

    let replay = args
        .store
        .load(&path)
        .await
        .expect("load")
        .expect("journal");
    assert_eq!(
        replay.events.len(),
        2,
        "buffer retained through the failure"
    );

    // And when the fault is removed, the next flush succeeds with the
    // SAME entries (retried, never dropped) — the DB now holds them.
    clear_fault(&pool).await;
    args.store.flush().await.expect("flush after recovery");
    assert_eq!(
        db_event_count(&pool, "faulty").await,
        2,
        "retried entries committed after the fault cleared"
    );
}

#[tokio::test]
async fn load_unknown_path_is_none() {
    // Given an empty store.
    let store = memory_store(DaowConfig::default()).await;

    // When loading a path that never journaled.
    let replay = store.load(&Path::new("nowhere")).await.expect("load");

    // Then there is nothing.
    assert!(replay.is_none());
}

#[tokio::test]
async fn scan_unions_buffer_and_db_excluding_catchup_entries() {
    // Given a store holding one flushed (DB) recorded fact, one buffered
    // recorded fact, and one buffered CatchUp seed.
    let store = memory_store(DaowConfig::default()).await;
    let schema = trouper::schema::SchemaId::new("StockReserved");
    let source = Path::new("source");
    store.append(&source, &[event(1)]).await.expect("db row");
    store.flush().await.expect("flush");
    let projector = Path::new("projector");
    store
        .append(&projector, &[event(2)])
        .await
        .expect("buffered recorded");
    let scanned = [trouper::journal::ScannedEvent {
        journal: source.clone(),
        seq: SeqNo::new(0),
        ingest_seq: 999,
        event: event(7),
    }];
    let seeded = store
        .append_catchup(&projector, &scanned)
        .await
        .expect("seed");
    assert_eq!(seeded, vec![Some(SeqNo::new(1))], "catch-up seed appended");

    // When scanning for the schema.
    let found = store.scan(&[schema]).await.expect("scan");

    // Then both recorded facts surface (buffer ∪ DB, ascending ingest)
    // and the CatchUp re-record is invisible.
    assert_eq!(found.len(), 2, "recorded facts only");
    assert!(found[0].ingest_seq < found[1].ingest_seq, "ingest order");
    assert_eq!(found[0].journal, source, "the DB row comes first");
    assert_eq!(found[1].journal, projector, "the buffered row comes second");
}

#[tokio::test]
async fn catchup_seed_is_idempotent_across_passivation_via_db_check() {
    // Given a projector that seeded a CatchUp origin which is now ONLY in
    // the database (passivation dropped the buffer).
    let store = memory_store(DaowConfig::default()).await;
    let source = Path::new("source");
    store.append(&source, &[event(1)]).await.expect("append");
    let scanned = [trouper::journal::ScannedEvent {
        journal: source.clone(),
        seq: SeqNo::new(0),
        ingest_seq: 999,
        event: event(7),
    }];
    let projector = Path::new("projector");
    let first = store
        .append_catchup(&projector, &scanned)
        .await
        .expect("seed");
    assert_eq!(first, vec![Some(SeqNo::new(0))]);
    store.flush().await.expect("flush");
    store.passivated(&projector).await.expect("passivate");

    // When the SAME origin is re-seeded after reactivation (a fresh
    // buffer that cannot know the origin — only the DB check can).
    let again = store
        .append_catchup(&projector, &scanned)
        .await
        .expect("re-seed");

    // Then it is suppressed — the projector never double-folds.
    assert_eq!(again, vec![None], "DB EXISTS check suppressed the re-seed");
}

#[tokio::test]
async fn passivated_flushes_drops_and_reactivation_continues_seq() {
    // Given a store whose path holds two flushed events and one buffered
    // event.
    let store = memory_store(DaowConfig::default()).await;
    let pool_memory = memory_pool();
    let _ = pool_memory; // (store owns its pool; kept for symmetry)
    let path = Path::new("cold");
    store
        .append(&path, &[event(1), event(2)])
        .await
        .expect("append");
    store.flush().await.expect("flush");
    store.append(&path, &[event(3)]).await.expect("append");

    // When passivation runs.
    store.passivated(&path).await.expect("passivate");

    // Then the buffered tail flushed to SQLite (all three rows) and the
    // buffer dropped — and a load REACTIVATES from SQLite: full replay,
    // seq anchoring at 3, appends continuing at seq 3.
    let store_any = store.as_any();
    let _ = store_any;
    let replay = store.load(&path).await.expect("load").expect("reactivated");
    assert_eq!(replay.events.len(), 3, "cold storage holds everything");
    assert_eq!(replay.tail.len(), 3, "no snapshot: full tail");
    let seqs: Vec<u64> = replay.events.iter().map(|e| e.seq.as_u64()).collect();
    assert_eq!(seqs, [0, 1, 2], "stored seqs are dense");

    let next = store.append(&path, &[event(4)]).await.expect("append");
    assert_eq!(
        next,
        vec![SeqNo::new(3)],
        "append continues at the stored max seq"
    );
}

#[tokio::test]
async fn purge_drops_rows_and_next_spawn_replays_nothing() {
    // Given a store holding one flushed event.
    let store = memory_store(DaowConfig::default()).await;
    let path = Path::new("doomed");
    store.append(&path, &[event(1)]).await.expect("append");
    store.flush().await.expect("flush");

    // When purging the path.
    store.purge(&path).await.expect("purge");

    // Then the rows are gone (DB AND buffer) and the next load replays
    // nothing.
    let replay = store.load(&path).await.expect("load");
    assert!(replay.is_none(), "purged path replays nothing");

    // And the ingest counter did NOT reset: the new append's ingest
    // continues above the purged event's (a reset would hand out 0).
    store
        .append(&Path::new("other"), &[event(1)])
        .await
        .expect("o");
    let replay = store
        .load(&Path::new("other"))
        .await
        .expect("load")
        .expect("journal");
    let assigned = replay.events.first().expect("e").ingest_seq;
    assert!(assigned > 1, "ingest continued ({assigned} > 1)");
}

#[tokio::test]
async fn load_unions_buffer_over_db_with_snapshot_precedence() {
    // Given a store with events 0..2 in the DB and events 2..3 (plus a
    // higher snapshot) in the buffer.
    let store = memory_store(DaowConfig::default()).await;
    let path = Path::new("union");
    store
        .append(&path, &[event(1), event(2)])
        .await
        .expect("db part");
    store.flush().await.expect("flush");
    store
        .append_snapshot(&path, SeqNo::new(2), json!({ "v": 2 }), 0)
        .await
        .expect("buffer snapshot");
    store.append(&path, &[event(3)]).await.expect("buffer part");

    // When loading.
    let replay = store.load(&path).await.expect("load").expect("journal");

    // Then the union is exact: three events, the buffer's higher
    // snapshot, and the tail strictly after it.
    assert_eq!(replay.events.len(), 3, "no dup, no loss");
    let snap = replay.snapshot.expect("snapshot");
    assert_eq!(
        snap.seq(),
        SeqNo::new(2),
        "buffer snapshot wins (higher seq)"
    );
    assert_eq!(
        replay.tail.len(),
        0,
        "tail is strictly post-snapshot (none)"
    );
}

// ── System-level end-to-end ──────────────────────────────────────────────

#[tokio::test]
async fn flush_on_tick_lands_buffered_events_in_sqlite() {
    // Given a system over a short flush interval with one actor.
    let db = TestDb::new();
    let system = daow_system(&db).await;
    let path = Path::new("tick/1");
    spawn_accum(&system, &path).await;
    tick(&system, &path, 1).await;

    // When one flush interval elapses (bounded wait on the DB row count
    // — the writer task flushes on its own schedule).
    let pool = db.pool();
    let mut rows = 0;
    for _ in 0..500 {
        rows = db_event_count(&pool, "tick/1").await;
        if rows > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Then the Ticked fact reached SQLite without any manual flush.
    assert_eq!(rows, 1, "the writer task flushed the buffered fact");
    system.shutdown_graceful(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn restart_replays_state_across_two_systems_on_one_db() {
    // Given a system over a SLOW flush (60s) whose actor folded two
    // commands, gracefully shut down — the sweep flushed, so the DB is
    // complete even though no tick ever ran.
    let db = TestDb::new();
    let path = Path::new("restart/1");
    {
        let system = daow_system_slow_flush(&db).await;
        spawn_accum(&system, &path).await;
        tick(&system, &path, 1).await;
        tick(&system, &path, 2).await;
        system.shutdown_graceful(Duration::from_secs(5)).await;
    }
    assert_eq!(
        db_event_count(&db.pool(), "restart/1").await,
        2,
        "sweep flushed both facts despite the 60s interval"
    );

    // When a SECOND system opens the same database and respawns the
    // actor.
    let system2 = daow_system_slow_flush(&db).await;
    spawn_accum(&system2, &path).await;
    // Settle: boot replay may replace the state Arc just after the actor
    // goes live; poll until the replayed fold is visible.
    let mut count = 0u64;
    for _ in 0..500 {
        if let Some(c) = system2.with_es_state::<Accum, _>(&path, |a| a.count).await {
            count = c;
            if count == 2 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Then the state replayed from SQLite: both events folded.
    assert_eq!(count, 2, "restart replayed the journal");
    system2.shutdown_graceful(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn restart_with_snapshot_replays_snapshot_plus_tail() {
    // Given a system whose actor snapshotted (Messages(2) cadence) after
    // three commands, shut down gracefully.
    let db = TestDb::new();
    let path = Path::new("snap/1");
    {
        let args = JournalArgs::daow(
            db.pool(),
            DaowConfig {
                flush_interval: Duration::from_secs(60),
            },
        )
        .build()
        .await
        .expect("args");
        let system = ActorSystem::new(SystemConfig::production().with_journal(args));
        spawn_es_builder::<Accum>(&system)
            .at(path.clone())
            .handles::<Tick>()
            .emits::<Ticked>()
            .snapshot(trouper::actor::SnapshotCadence::Messages(2))
            .start();
        for _ in 0..5_000 {
            if system
                .with_es_state::<Accum, _>(&path, |a| a.count)
                .await
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        for n in 1..=3 {
            tick(&system, &path, n).await;
        }
        system.shutdown_graceful(Duration::from_secs(5)).await;
    }

    assert_eq!(
        db_event_count(&db.pool(), "snap/1").await,
        3,
        "sweep flushed three events"
    );

    // When a second system respawns the actor.
    let system2 = daow_system_slow_flush(&db).await;
    spawn_accum(&system2, &path).await;
    // Settle: the boot replay may replace the state Arc moments after the
    // actor goes live; poll until the fold is visible.
    let mut state = (0u64, 0i64);
    for _ in 0..500 {
        if let Some(s) = system2
            .with_es_state::<Accum, _>(&path, |a| (a.count, a.last))
            .await
        {
            state = s;
            if state == (3, 3) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Then snapshot + tail replay to the exact folded state.
    assert_eq!(state, (3, 3), "snapshot restored plus the tail event");
    system2.shutdown_graceful(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn shutdown_flush_is_immediate_complete_and_waited_for() {
    // Given a system on a 60s flush interval with TWO actors each holding
    // unflushed facts (no tick can run during the test).
    let db = TestDb::new();
    let system = daow_system_slow_flush(&db).await;
    let a = Path::new("sweep/a");
    let b = Path::new("sweep/b");
    spawn_accum(&system, &a).await;
    spawn_accum(&system, &b).await;
    tick(&system, &a, 1).await;
    tick(&system, &b, 2).await;

    // When the graceful sweep runs.
    system.shutdown_graceful(Duration::from_secs(5)).await;

    // Then BOTH paths' rows are in the database IMMEDIATELY after the
    // sweep returns (the flush happened inside the sweep, and the system
    // WAITED for it), leaving zero unflushed entries.
    let pool = db.pool();
    assert_eq!(db_event_count(&pool, "sweep/a").await, 1, "path a flushed");
    assert_eq!(db_event_count(&pool, "sweep/b").await, 1, "path b flushed");
}

#[tokio::test]
async fn shutdown_flush_failure_reaches_control_after_shutdown_returns() {
    // Given a system on a 60s interval whose events table refuses writes
    // (fault trigger installed BEFORE shutdown) with the control
    // closure wired to a channel.
    let db = TestDb::new();
    let pool = db.pool();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let args = JournalArgs::daow(
        pool.clone(),
        DaowConfig {
            flush_interval: Duration::from_secs(60),
        },
    )
    .with_control(Arc::new(move |msg| {
        let _ = tx.send(msg);
    }))
    .build()
    .await
    .expect("args");
    let system = ActorSystem::new(SystemConfig::production().with_journal(args));
    let path = Path::new("fail/1");
    spawn_accum(&system, &path).await;
    tick(&system, &path, 1).await;
    install_fault(&pool).await;

    // When the sweep runs (its store flush fails; the runtime IGNORES
    // flush's Result by design — the control closure is the surface).
    system.shutdown_graceful(Duration::from_secs(5)).await;

    // Then the failure is observable AFTER shutdown returned: the main
    // thread drains the channel and finds the sweep's `flush` failure
    // among the messages (the fault may also surface as `tick` from the
    // writer task racing shutdown on other runs — both name the fault).
    let mut saw_flush = false;
    let mut detail = String::new();
    // Bounded wait: the sweep's flush runs inside `shutdown_graceful`,
    // so its message is normally already queued when the call returns —
    // but the writer task may also race the fault, so poll briefly and
    // require the `flush` op specifically.
    for _ in 0..2_000 {
        while let Ok(StoreControlMessage::Error { op, detail: d }) = rx.try_recv() {
            detail = d;
            if op == "flush" {
                saw_flush = true;
            }
        }
        if saw_flush {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        saw_flush,
        "the sweep's flush failure reached the control channel (last detail: {detail:?})"
    );
    assert!(!detail.is_empty(), "the failure carries its rendering");

    // And the buffer was retained: once the fault clears, a later flush
    // (via a fresh store over the same pool) retries the entry — here we
    // prove the ENTRY was buffered by clearing the fault and flushing
    // through a second build.
    clear_fault(&pool).await;
    let retry = JournalArgs::daow(pool.clone(), DaowConfig::default())
        .build()
        .await
        .expect("rebuild");
    // (The retried entry lives in the DROPPED system's buffer; what the
    // retry build proves is the schema+pool stay usable. The retained-
    // buffer property is asserted directly in
    // `buffered_append_fails_the_write_and_the_tick_reports_and_recovers`.)
    retry
        .store
        .flush()
        .await
        .expect("pool usable after failure");
}

#[tokio::test]
async fn writer_task_exits_when_the_store_drops() {
    // Given a store with a running writer task (taken through the test
    // seam — sync Drop cannot await the join).
    let (store, handle) = test_store_with_writer(memory_pool(), DaowConfig::default()).await;

    // When the store drops.
    drop(store);
    // The writer wakes on its interval (first tick one full interval in),
    // so exit detection must span up to that wall-clock timer — a bounded
    // sleep poll, not yields (yields can't advance a timer).
    for _ in 0..2_000 {
        if handle.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // Then the writer exited (the Weak upgrade fails) — no task leak.
    assert!(handle.is_finished(), "writer task must exit on store drop");
}

#[tokio::test]
async fn concurrent_paths_flush_in_one_cycle() {
    // Given two actors appending concurrently to one store.
    let db = TestDb::new();
    let system = daow_system(&db).await;
    let a = Path::new("conc/a");
    let b = Path::new("conc/b");
    spawn_accum(&system, &a).await;
    spawn_accum(&system, &b).await;
    tokio::join!(tick(&system, &a, 1), tick(&system, &b, 7));

    // When the shutdown sweep flushes (one cycle, both paths).
    system.shutdown_graceful(Duration::from_secs(5)).await;

    // Then both paths' facts committed (one cycle covering both).
    let pool = db.pool();
    assert_eq!(db_event_count(&pool, "conc/a").await, 1);
    assert_eq!(db_event_count(&pool, "conc/b").await, 1);
}

#[tokio::test]
async fn snapshot_only_journal_restores_the_snapshot_alone() {
    // Given a journal whose only entry is a snapshot (idle snapshot at
    // genesis) flushed to the DB.
    let store = memory_store(DaowConfig::default()).await;
    let path = Path::new("snaponly");
    store
        .append_snapshot(&path, SeqNo::genesis(), json!({ "genesis": true }), 0)
        .await
        .expect("snapshot");
    store.flush().await.expect("flush");

    // When a fresh load reads the path.
    let replay = store.load(&path).await.expect("load").expect("journal");

    // Then the snapshot restores alone, with an empty tail.
    assert!(replay.snapshot.is_some(), "the snapshot survived");
    assert!(replay.tail.is_empty(), "no events: no tail");
    assert!(replay.events.is_empty(), "no events: no history");
}

#[tokio::test]
async fn scan_surfaces_facts_across_a_restart_boundary() {
    // Given one store that recorded two facts, flushed, and was dropped
    // (the restart), and a SECOND store over the same database that
    // holds one buffered fact.
    let db = TestDb::new();
    let pool = db.pool();
    let schema = trouper::schema::SchemaId::new("StockReserved");
    {
        let args = JournalArgs::daow(pool.clone(), DaowConfig::default())
            .build()
            .await
            .expect("build");
        args.store
            .append(&Path::new("scan/first"), &[event(1), event(2)])
            .await
            .expect("append");
        args.store.flush().await.expect("flush");
    }
    let args2 = JournalArgs::daow(pool.clone(), DaowConfig::default())
        .build()
        .await
        .expect("rebuild");
    args2
        .store
        .append(&Path::new("scan/second"), &[event(3)])
        .await
        .expect("buffered");

    // When scanning for the schema (DB rows from the first store's
    // committed facts ∪ the second store's buffered row).
    let found = args2.store.scan(&[schema]).await.expect("scan");

    // Then all three facts surface, ascending ingest order.
    assert_eq!(found.len(), 3, "cross-restart scan is complete");
    assert!(found.windows(2).all(|w| w[0].ingest_seq < w[1].ingest_seq));
}
