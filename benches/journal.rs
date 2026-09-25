//! Journal persistence benchmarks: the SQLite backend's price over a
//! live system.
//!
//! Every bench measures COMPLETE processing: the destination's INBOX
//! CURSOR advances exactly at the kernel's commit point, journal append
//! plus ack, and is single-writer per actor loop, so spinning on it —
//! `wait_committed`, yield-based, no timed sleep — proves the batch
//! committed.
//!
//! Batches 64 / 512 / 2048 (no single-message case: spawn + harness
//! overhead dominated it). Observation DISABLED (`production()`: no
//! handler installed). Criterion bodies hop into the runtime via
//! `rt.block_on` (the runtime lives in an `Arc` so `block_on` works from
//! anywhere outside it).
//!
//! Two media: `:memory:` SQLite (the pool's connection floor — pure
//! SQLite + pool cost, one connection by daow's `:memory:` rule) and an
//! on-disk SQLite file in a tempdir (WAL fsyncs included — the
//! deployment shape). The delta between the two IS the filesystem's
//! share of the flush price.
//!
//! `flush_price` isolates the store: it buffers 2048 events directly
//! (no actors, no system) and times ONE `flush()` cycle per iteration —
//! the durable-commit cost the writer task amortizes across ticks.

use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use trouper::actor::{ActorKind, ActorPath, EventSourcedActor};
use trouper::builder::spawn_es_builder;
use trouper::context::CmdCtx;
use trouper::envelope::{Events, IntoEvent};
use trouper::journal::{JournalArgs, JournalStore};
use trouper::journal_daow::DaowConfig;
use trouper::json::Json;
use trouper::schema::{ActorManifest, Command};
use trouper::system::{ActorSystem, SystemConfig};

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
        }
    }
}
impl trouper::actor::CommandHandler<Tick> for Accum {
    fn handle(&self, _cmd: Tick, _ctx: &mut CmdCtx<'_>) -> Events {
        Events::one(Ticked { n: 1 })
    }
}

/// Flush cadence: LONGER than any benchmark iteration so the measured
/// window is pure ack-path work (the buffered append) and the flush cost
/// lands outside it (in setup's shutdown). `flush_price` then prices the
/// flush itself, separately.
fn bench_config() -> DaowConfig {
    DaowConfig {
        flush_interval: Duration::from_secs(600),
    }
}

fn spawn_system(args: JournalArgs) -> (ActorSystem, Arc<tokio::runtime::Runtime>) {
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("rt"),
    );
    let system = ActorSystem::new(SystemConfig::production().with_journal(args));
    (system, rt)
}

/// Per-iteration entity paths: spawn is one-per-path, so every measured
/// iteration gets a fresh journal (iterations stay independent).
struct Iterations(std::sync::atomic::AtomicU64);
impl Iterations {
    fn next_path(&self, prefix: &str) -> ActorPath {
        let n = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ActorPath::new(format!("{prefix}-{n}"))
    }
}

/// Spins (yield-based, NO timed sleep) until `count` more messages have
/// COMMITTED to `path` since `base` — the push-driven completion signal
/// (the cursor advances exactly at journal append + ack). Must run
/// inside the runtime.
async fn wait_committed(system: &ActorSystem, base: u64, path: &ActorPath, count: u64) {
    let target = base + count;
    for _ in 0..200_000_000 {
        if system.inbox_cursor(path).map(|c| c.as_u64()).unwrap_or(0) >= target {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("commit wait stalled: {path} cursor < {target}");
}

async fn spawn_accum(system: &ActorSystem, path: &ActorPath) {
    spawn_es_builder::<Accum>(system)
        .at(path.clone())
        .handles::<Tick>()
        .emits::<Ticked>()
        .start();
    wait_committed_spawn(system, path).await;
}

/// Liveness probe: the entity's state is readable.
async fn wait_committed_spawn(system: &ActorSystem, path: &ActorPath) {
    for _ in 0..5_000 {
        if system
            .with_es_state::<Accum, _>(path, |a| a.count)
            .await
            .is_some()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("actor never went live: {path}");
}

// ── tell_acked through the daow backend ──────────────────────────────────

/// One producer, one fresh entity: full send→fold→journal-ack cycle with
/// the SQLite store installed. The ack resolves at the BUFFERED append
/// (never SQLite — that is the write-behind contract); flushes ride the
/// writer task outside the measured body. Media: `:memory:` and on-disk.
fn tell_acked_daow(c: &mut Criterion) {
    // :memory: — daow forces max_size=1, so writer and reads share one
    // connection; the pure SQLite floor.
    tell_acked_media(c, "memory", trouper_daow_memory_pool(), "bench/journal-mem");

    // On-disk — WAL fsyncs included: the deployment shape.
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("bench.db").display().to_string();
    let pool = daow::Pool::builder()
        .path(db_path)
        .max_size(2)
        .build()
        .expect("pool");
    tell_acked_media(c, "disk", pool, "bench/journal-disk");
}

/// One journal medium of `tell_acked`: system + journal build ONCE, then
/// per measured iteration a FRESH entity path is spawned in the untimed
/// `iter_batched` setup and the timed routine is only tells + the
/// commit-cursor wait (complete processing per Throughput::Elements).
fn tell_acked_media(c: &mut Criterion, label: &str, pool: daow::Pool, prefix: &str) {
    // Setup runtime: `JournalArgs::daow(..).build()` is async (pool probe
    // + migrations) and must run BEFORE the measured system exists. A
    // throwaway current-thread rt for those two awaits — nothing else.
    let args = {
        let rt0 = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("setup rt");
        rt0.block_on(JournalArgs::daow(pool, bench_config()).build())
            .expect("journal args")
    };
    let (system, rt) = spawn_system(args);
    let iterations = Iterations(std::sync::atomic::AtomicU64::new(0));

    let mut group = c.benchmark_group(format!("journal/tell_acked/{label}"));
    group.sample_size(30);
    for size in [64u64, 512, 2_048] {
        group.throughput(criterion::Throughput::Elements(size));
        group.bench_function(format!("{size}_messages"), |b| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let path = iterations.next_path(prefix);
                        spawn_accum(&system, &path).await;
                        let base = system.inbox_cursor(&path).map(|c| c.as_u64()).unwrap_or(0);
                        (path, base)
                    })
                },
                |(path, base)| {
                    rt.block_on(async {
                        for n in 0..size as i64 {
                            system
                                .tell(path.clone(), Tick { n })
                                .await
                                .expect("tell accepted");
                        }
                        wait_committed(&system, base, &path, size).await;
                    });
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
    rt.block_on(system.shutdown_graceful(Duration::from_secs(30)));
}

fn trouper_daow_memory_pool() -> daow::Pool {
    daow::Pool::builder()
        .path(":memory:")
        .build()
        .expect("pool")
}

fn disk_pool(dir: &tempfile::TempDir, name: &str) -> daow::Pool {
    let db_path = dir.path().join(name).display().to_string();
    daow::Pool::builder()
        .path(db_path)
        .max_size(2)
        .build()
        .expect("pool")
}

fn bench_store(pool: daow::Pool) -> Arc<dyn JournalStore> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    rt.block_on(JournalArgs::daow(pool, bench_config()).build())
        .expect("journal args")
        .store
}

async fn buffer_events(store: &dyn JournalStore, path: &ActorPath, start: i64, count: i64) {
    for n in start..start + count {
        let event = Ticked { n }.into_event();
        store.append(path, &[event]).await.expect("append");
    }
}

// ── pending flush: the durable-commit cost, isolated ─────────────────────

/// Buffers `size` events directly on the store (no actors) and times ONE
/// `flush()` per iteration — the SQLite commit the writer task amortizes
/// across ticks. One element = one event row committed; elem/s reads as
/// rows/s. Media: `:memory:` and on-disk (the delta IS the filesystem).
fn flush_price(c: &mut Criterion) {
    // The tempdirs outlive their pools (the pools keep the files open,
    // and criterion runs within this function's scope).
    let dirs = [
        tempfile::tempdir().expect("tempdir"),
        tempfile::tempdir().expect("tempdir"),
    ];
    for (name, pool) in [
        ("memory", trouper_daow_memory_pool()),
        ("disk", disk_pool(&dirs[1], "flush.db")),
    ] {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        let store = bench_store(pool);

        let mut group = c.benchmark_group("journal/flush_price");
        group.sample_size(30);
        for size in [512u64, 2_048] {
            group.throughput(criterion::Throughput::Elements(size));
            // `iter_batched`: SETUP buffers `size` events (the cheap
            // ack-path appends), the timed ROUTINE is exactly one
            // `flush()` — the durable commit the writer task amortizes.
            group.bench_function(format!("pending/{name}/{size}_events"), |b| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            let path = ActorPath::new("bench/flush-price");
                            store.purge(&path).await.expect("reset path");
                            for n in 0..size as i64 {
                                let event = Ticked { n }.into_event();
                                store.append(&path, &[event]).await.expect("append");
                            }
                        })
                    },
                    |()| {
                        rt.block_on(async {
                            // One durable commit of the buffered batch.
                            store.flush().await.expect("flush");
                        })
                    },
                    criterion::BatchSize::PerIteration,
                );
            });
        }
        group.finish();

        let mut backlog_group = c.benchmark_group(format!("journal/accumulated_backlog/{name}"));
        backlog_group.sample_size(30);
        backlog_group.throughput(criterion::Throughput::Elements(2_048));
        backlog_group.bench_function("2048_events", |b| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let path = ActorPath::new("bench/accumulated-backlog");
                        store.purge(&path).await.expect("reset path");
                        buffer_events(&*store, &path, 0, 2_048).await;
                    })
                },
                |()| {
                    rt.block_on(async {
                        store.flush().await.expect("flush accumulated backlog");
                    })
                },
                criterion::BatchSize::PerIteration,
            );
        });
        backlog_group.finish();

        let mut retained_group = c.benchmark_group(format!("journal/retained_history/{name}"));
        retained_group.sample_size(30);
        let retained = 2_048i64;
        let suffix = 64i64;
        retained_group.throughput(criterion::Throughput::Elements(suffix as u64));
        retained_group.bench_function("2048_retained_64_pending", |b| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let path = ActorPath::new("bench/retained-history");
                        store.purge(&path).await.expect("reset path");
                        buffer_events(&*store, &path, 0, retained).await;
                        store.flush().await.expect("flush retained prefix");
                        buffer_events(&*store, &path, retained, suffix).await;
                    })
                },
                |()| {
                    rt.block_on(async {
                        store.flush().await.expect("flush pending suffix");
                    })
                },
                criterion::BatchSize::PerIteration,
            );
        });
        retained_group.finish();

        let mut noop_group = c.benchmark_group(format!("journal/noop_flush/{name}"));
        noop_group.sample_size(30);
        noop_group.bench_function("after_1_event", |b| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let path = ActorPath::new("bench/noop-flush");
                        store.purge(&path).await.expect("reset path");
                        buffer_events(&*store, &path, 0, 1).await;
                        store.flush().await.expect("flush initial event");
                    })
                },
                |()| {
                    rt.block_on(async {
                        store.flush().await.expect("flush empty buffer");
                    })
                },
                criterion::BatchSize::PerIteration,
            );
        });
        noop_group.finish();
    }
}

// ── replay: cold activation and warmed-memory load ───────────────────────

/// Measures replay independently from flush. The persisted shape is 2,048
/// events plus a snapshot anchored after event 1,023. Cold activation builds
/// a fresh on-disk store for every Criterion sample and times only `load()`;
/// warmed replay seeds one store in setup and times repeated `load()` calls
/// against its already-resident authoritative path.
fn replay(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    for name in ["memory", "disk"] {
        let seed_pool = if name == "disk" {
            disk_pool(&dir, "replay.db")
        } else {
            trouper_daow_memory_pool()
        };
        let seed_store = bench_store(seed_pool);
        let path = ActorPath::new(format!("bench/replay/{name}"));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            seed_store.purge(&path).await.expect("reset path");
            buffer_events(&*seed_store, &path, 0, 2_048).await;
            seed_store
                .append_snapshot(
                    &path,
                    trouper::journal::SeqNo::new(1_023),
                    Json::of(&Accum::default()),
                    0,
                )
                .await
                .expect("append snapshot");
            seed_store.flush().await.expect("flush replay fixture");
        });

        if name == "disk" {
            let mut cold_group = c.benchmark_group("journal/replay/cold/disk");
            cold_group.sample_size(30);
            cold_group.throughput(criterion::Throughput::Elements(1));
            cold_group.bench_function("2048_events_plus_snapshot", |b| {
                b.iter_batched(
                    || bench_store(disk_pool(&dir, "replay.db")),
                    |store| {
                        rt.block_on(async {
                            let replay = store.load(&path).await.expect("load");
                            assert!(replay.is_some());
                        });
                    },
                    criterion::BatchSize::PerIteration,
                );
            });
            cold_group.finish();
        }

        let mut warm_group = c.benchmark_group(format!("journal/replay/warmed/{name}"));
        warm_group.sample_size(30);
        warm_group.throughput(criterion::Throughput::Elements(1));
        warm_group.bench_function("2048_events_plus_snapshot", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let replay = seed_store.load(&path).await.expect("load");
                    assert!(replay.is_some());
                });
            });
        });
        warm_group.finish();
    }
}

// ── accumulated shutdown: actor shutdown plus final store flush ──────────

/// Accumulates 2,048 acknowledged actor messages, then times graceful
/// shutdown. Runtime creation, actor startup, tells, and commit waiting are
/// setup; the timed routine includes actor teardown and the final journal
/// flush. Each sample owns its database so teardown cannot flush another
/// sample's retained history.
fn accumulated_shutdown(c: &mut Criterion) {
    for name in ["memory", "disk"] {
        let mut group = c.benchmark_group(format!("journal/accumulated_shutdown/{name}"));
        group.sample_size(10);
        group.throughput(criterion::Throughput::Elements(2_048));
        group.bench_function("2048_messages", |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let pool = if name == "disk" {
                        disk_pool(&dir, "shutdown.db")
                    } else {
                        trouper_daow_memory_pool()
                    };
                    let args = {
                        let setup_rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("setup rt");
                        setup_rt
                            .block_on(JournalArgs::daow(pool, bench_config()).build())
                            .expect("journal args")
                    };
                    let (system, rt) = spawn_system(args);
                    let path = ActorPath::new(format!("bench/accumulated-shutdown/{name}"));
                    rt.block_on(async {
                        spawn_accum(&system, &path).await;
                        let base = system.inbox_cursor(&path).map(|c| c.as_u64()).unwrap_or(0);
                        for n in 0..2_048i64 {
                            system
                                .tell(path.clone(), Tick { n })
                                .await
                                .expect("tell accepted");
                        }
                        wait_committed(&system, base, &path, 2_048).await;
                    });
                    (dir, system, rt)
                },
                |(dir, system, rt)| {
                    rt.block_on(system.shutdown_graceful(Duration::from_secs(30)));
                    drop(dir);
                },
                criterion::BatchSize::PerIteration,
            );
        });
        group.finish();
    }
}

criterion_group!(
    benches,
    tell_acked_daow,
    flush_price,
    replay,
    accumulated_shutdown
);
criterion_main!(benches);
