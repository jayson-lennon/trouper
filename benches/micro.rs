//! Component-shaped benchmarks: the journal store's isolated costs, which
//! system-level benches cannot separate from routing and dispatch. These track the
//! planned journal-store restructure. The store is the in-memory
//! implementation (the only one); a durable backend would carry its own
//! cost on top of every number here.

use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};

use trouper::Json;
use trouper::actor::ActorPath;
use trouper::envelope::Event;
use trouper::journal::{InMemoryJournalStore, JournalStore, SeqNo};
use trouper::schema::SchemaId;

fn event(n: u64) -> Event {
    Event::from_json_view(
        SchemaId::new("BenchEvent"),
        Json::of(&serde_json::json!({ "n": n })),
    )
}

/// Builds a journal of `events` recorded events for `path` (sync seeding).
fn seed(store: &InMemoryJournalStore, path: &ActorPath, events: u64) {
    let batch: Vec<Event> = (0..events).map(event).collect();
    store.append_sync(path, &batch).expect("seed append");
}

/// in_memory_journal_append: the store's append cost at batch sizes 1 /
/// 8 / 64 — what every committed ES message pays inside the atomic
/// step. The name states the scope: this is the IN-MEMORY store; a
/// durable backend's cost rides on top.
fn in_memory_journal_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("micro/in_memory_journal_append");
    for batch in [1u64, 8, 64] {
        // ONE ELEMENT = one EVENT appended in the batch (not one
        // message): elem/s = events/s through the store, isolated from
        // the runtime.
        group.throughput(criterion::Throughput::Elements(batch));
        group.bench_function(format!("batch_{batch}"), |b| {
            let store = InMemoryJournalStore::new();
            let path = ActorPath::new("bench/append");
            let events: Vec<Event> = (0..batch).map(event).collect();
            b.iter(|| {
                store
                    .append_sync(&path, std::hint::black_box(&events))
                    .expect("append");
            });
        });
    }
    group.finish();
}

/// in_memory_journal_replay_restart: `load()` over a journaled path —
/// the dominant cost of a crash restart or cold re-activation (snapshot
/// + tail + full history clone), against the in-memory store.
fn in_memory_journal_replay_restart(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    let mut group = c.benchmark_group("micro/in_memory_journal_replay_restart");
    for history in [1_000u64, 10_000] {
        // ONE ELEMENT = one EVENT replayed by load() (snapshot + tail +
        // history): elem/s = replayed events/s — the restart-cost view.
        group.throughput(criterion::Throughput::Elements(history));
        group.bench_function(format!("journal_{history}"), |b| {
            let store = Arc::new(InMemoryJournalStore::new());
            let path = ActorPath::new("bench/replay");
            seed(&store, &path, history);
            let snapshot_seq = SeqNo::new(history / 2 - 1);
            let snapshot_state = Json::of(&serde_json::json!({ "folded": history }));
            rt.block_on(store.append_snapshot(&path, snapshot_seq, snapshot_state, 0))
                .expect("snapshot");
            b.iter(|| {
                let store = store.clone();
                let path = path.clone();
                rt.block_on(async move {
                    store.load(&path).await.expect("load");
                });
            });
        });
    }
    group.finish();
}

/// Keeps the bench binary honest about the seeded-journal cadence test
/// (a projector-style repeated load, close/chain-in-bench-style).
fn in_memory_journal_repeated_load(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    let store = Arc::new(InMemoryJournalStore::new());
    let path = ActorPath::new("bench/repeated");
    seed(&store, &path, 10_000);
    let mut group = c.benchmark_group("micro/in_memory_journal_replay_restart");
    // Same element rule as journal_N: one replayed EVENT (10k history).
    group.throughput(criterion::Throughput::Elements(10_000));
    group.bench_function("repeat_load_10k", |b| {
        b.iter(|| {
            let store = store.clone();
            let path = path.clone();
            rt.block_on(async move {
                store.load(&path).await.expect("load");
            });
        });
    });
    group.finish();
}

/// Unused guard: keeps `Duration` imported if bench shapes change.
const _: Option<Duration> = None;

criterion_group!(
    benches,
    in_memory_journal_append,
    in_memory_journal_replay_restart,
    in_memory_journal_repeated_load
);
criterion_main!(benches);
