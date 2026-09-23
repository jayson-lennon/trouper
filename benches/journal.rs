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
use trouper::envelope::Events;
use trouper::journal::JournalArgs;
use trouper::journal_daow::DaowConfig;
use trouper::json::Json;
use trouper::schema::{ActorManifest, Command, Schema};
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
        Events::from_vec(vec![trouper::envelope::Event::from_json_view(
            Ticked::schema_id(),
            Json::of(&Ticked { n: 1 }),
        )])
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

// ── flush_price: the durable-commit cost, isolated ───────────────────────

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
    let disk_pool = |dir: &tempfile::TempDir, name: &str| {
        let db_path = dir.path().join(name).display().to_string();
        daow::Pool::builder()
            .path(db_path)
            .max_size(2)
            .build()
            .expect("pool")
    };
    for (name, pool) in [
        ("memory", trouper_daow_memory_pool()),
        ("disk", disk_pool(&dirs[1], "flush.db")),
    ] {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        let args = rt.block_on(async {
            JournalArgs::daow(pool, bench_config())
                .build()
                .await
                .expect("journal args")
        });
        let store = args.store;

        let mut group = c.benchmark_group("journal/flush_price");
        group.sample_size(30);
        for size in [512u64, 2_048] {
            group.throughput(criterion::Throughput::Elements(size));
            // `iter_batched`: SETUP buffers `size` events (the cheap
            // ack-path appends), the timed ROUTINE is exactly one
            // `flush()` — the durable commit the writer task amortizes.
            group.bench_function(format!("{name}/{size}_events"), |b| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            let path = ActorPath::new("bench/flush-price");
                            for n in 0..size as i64 {
                                let event = trouper::envelope::Event::from_json_view(
                                    Ticked::schema_id(),
                                    Json::of(&Ticked { n }),
                                );
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
    }
}

criterion_group!(benches, tell_acked_daow, flush_price);
criterion_main!(benches);
