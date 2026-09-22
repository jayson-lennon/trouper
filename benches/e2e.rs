//! Usage-shaped benchmarks: full send-message → done cycles against a
//! live multi-thread runtime, the shape a real deployment exercises.
//!
//! Every bench measures COMPLETE processing, never bare `tell` loops (a
//! tell resolves at channel accept; without a completion barrier the
//! number is a channel benchmark, not an actor one). Completion is
//! PUSH-DRIVEN, uniformly: a per-iteration FRESH entity's commit is
//! observed through its Acked tap fact (`wait_acked`, no timed sleep —
//! detection at scheduler scale), and `ask` benches settle on the
//! reply itself. Fresh entities keep criterion iterations independent —
//! no cross-iteration accounting.
//!
//! Message benches share one matrix: batches of 1, 64, and 512 (the
//! swarm keeps its declared 32×receivers shape; payload_size and
//! wide_tree are single-message by design — they price payload width,
//! not batching).
//!
//! Structure: setup runs inside `rt.block_on`; the measured `b.iter`
//! bodies hop into the runtime via `rt.block_on` (criterion's thread is
//! never a runtime worker; the runtime lives in an Arc so `block_on`
//! works from anywhere outside it).
//!
//! The runtime rides the TYPED PAYLOAD FABRIC: every `tell` wraps a live
//! value (zero serde on the dispatch path); the journal door memoizes
//! one compact encoding per payload. `payload_size` now measures the
//! fabric's per-message cost across body sizes — the old JSON-tree
//! waist (per-node walk + deep clone) no longer exists.

use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio::sync::Barrier;

use trouper::actor::{ActorKind, ActorPath, EventSourcedActor, Projector, ServiceActor};
use trouper::context::{CmdCtx, MsgCtx};
use trouper::inbox::OverloadPolicy;
use trouper::json::Json;
use trouper::schema::{ActorManifest, Command, Event, Schema};
use trouper::system::{ActorSystem, SnapshotCadence, SpawnOpts, SystemConfig};

// ---------------------------------------------------------------------------
// Fixtures: a counting entity, a heavy-payload command, an echo service.
// ---------------------------------------------------------------------------

#[derive(Clone, Command, serde::Serialize, serde::Deserialize)]
struct Tick {
    n: i64,
}

#[derive(Event, serde::Serialize, serde::Deserialize)]
struct Ticked {
    n: i64,
}

#[derive(Serialize, Deserialize, Default)]
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
    fn handle(&self, _cmd: Tick, _ctx: &mut CmdCtx<'_>) -> trouper::envelope::Events {
        trouper::envelope::Events::from_vec(vec![trouper::envelope::Event::from_json_view(
            Ticked::schema_id(),
            Json::of(&Ticked { n: 1 }),
        )])
    }
}

/// The read model the projection_read bench draws over: folds `Ticked`
/// facts into the same count the entity holds, plus a bounded tail
/// (the shape a UI summary actually renders — an aggregate and a few
/// recent rows).
#[derive(Serialize, Deserialize, Default)]
struct Ledger {
    count: u64,
    tail: Vec<u64>,
}

impl Projector for Ledger {
    fn apply(&mut self, event: &trouper::envelope::Event) {
        if event.schema.as_str() == "Ticked" {
            let n = event.payload_json()["n"].as_u64().unwrap_or(0);
            self.count += 1;
            self.tail.push(n);
            if self.tail.len() > 8 {
                self.tail.remove(0);
            }
        }
    }
}

/// A realistic message: a handful of typed fields, an id string, a small
/// tag list, and a bulk body. The payload_size bench sweeps `body` across
/// orders of magnitude — this is the shape application messages actually
/// have (a few fields, one bulk value), NOT a tree-node stress case.
#[derive(Clone, Command, serde::Serialize, serde::Deserialize)]
struct Chunk {
    kind: String,
    id: String,
    tags: Vec<String>,
    seq: u64,
    body: String,
}

#[derive(Event, serde::Serialize, serde::Deserialize)]
struct Chunked {
    bytes: usize,
}

/// A pathological message: `filler` serializes as a JSON array with one
/// node PER ELEMENT. A 1MB filler is a ~1M-node tree (~25x the serde cost
/// of a 1MB string) — kept for the wide_tree bench, which exists to show
/// the runtime's per-NODE overhead, never to represent a real payload.
#[derive(Clone, Command, serde::Serialize, serde::Deserialize)]
struct WideChunk {
    filler: Vec<u8>,
}

#[derive(Serialize, Deserialize, Default)]
struct Bytes {
    total: u64,
}

impl EventSourcedActor for Bytes {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<Chunk>()
            .emits::<Chunked>()
            .kind(ActorKind::EventSourced)
    }
    fn restore(_args: &Json) -> Self {
        Self::default()
    }
    fn apply(&mut self, event: &trouper::envelope::Event) {
        if event.schema.as_str() == "Chunked" {
            self.total += event.payload_json()["bytes"].as_u64().unwrap_or(0);
        }
    }
}
impl trouper::actor::CommandHandler<Chunk> for Bytes {
    fn handle(&self, cmd: Chunk, _ctx: &mut CmdCtx<'_>) -> trouper::envelope::Events {
        trouper::envelope::Events::from_vec(vec![trouper::envelope::Event::from_json_view(
            Chunked::schema_id(),
            Json::of(&Chunked {
                bytes: cmd.body.len(),
            }),
        )])
    }
}

/// An echo service: replies to `Ping` with a tiny ack — the lease-backed
/// ask RTT is a self-contained completion signal.
#[derive(Clone, Command, serde::Serialize, serde::Deserialize)]
struct Ping {
    filler: Vec<u8>,
}

#[derive(Clone, Event, serde::Serialize, serde::Deserialize)]
struct Pong {
    bytes: usize,
}

struct Echo;

impl ServiceActor for Echo {
    fn manifest() -> ActorManifest {
        // The reply is an outbound message: undeclared emits are
        // dead-lettered (UndeclaredEmit), so Pong MUST be declared here.
        ActorManifest::new()
            .handles::<Ping>()
            .emits::<Pong>()
            .kind(ActorKind::Service)
    }
    async fn start(
        _args: &Json,
    ) -> Result<Self, error_stack::Report<trouper::registry::RegistryError>> {
        Ok(Self)
    }
}
impl trouper::actor::MsgHandler<Ping> for Echo {
    async fn handle(&mut self, msg: Ping, ctx: &mut MsgCtx<'_>) {
        ctx.reply(Pong {
            bytes: msg.filler.len(),
        });
    }
}

fn spawn_system() -> (ActorSystem, Arc<Runtime>) {
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("rt"),
    );
    let system = ActorSystem::new(SystemConfig::production());
    (system, rt)
}

/// Spawns an `Accum` entity and waits for its state to be readable
/// (the loop task is live). Must run inside the runtime.
async fn spawn_accum(system: &ActorSystem, path: &ActorPath) {
    system.spawn_es::<Accum, _>(path.clone(), &Json::default(), SpawnOpts::default(), || {
        vec![Arc::new(
            trouper::actor::TypedEsAdapter::<Accum, Tick>::new::<Tick>(),
        )]
    });
    wait_entity_exists(system, path).await;
}

/// Waits (push-driven, NO timed sleep) until `count` Acked tap facts
/// for `path` appear at/after `from` — the scheduler-scale completion
/// signal. `Acked` is recorded at the kernel's commit point (journal
/// append + inbox ack), so this waits for COMPLETE processing, same
/// contract as the file-wide rule. Returns the next tap offset so
/// callers can chain windows. Must run inside the runtime.
///
/// The loop is PEEK-then-DRAIN: spin on the ring's O(1) next-offset
/// watermark and only call `tap_facts_from` once enough facts have
/// landed. Draining on every round would scan the whole retained ring
/// under the same mutex the commit point pushes with — the waiter
/// would throttle the very acks it waits for.
async fn wait_acked(system: &ActorSystem, from: u64, path: &ActorPath, count: u64) -> u64 {
    let mut cursor = from;
    let mut arrived = 0u64;
    for _ in 0..200_000_000 {
        if system.tap_next_offset() >= cursor + (count - arrived) {
            for fact in system.tap_facts_from(cursor) {
                cursor = fact.offset + 1;
                if matches!(&fact.kind, trouper::tap::FactKind::Acked { to, .. } if to == path) {
                    arrived += 1;
                    if arrived == count {
                        return cursor;
                    }
                }
            }
        }
        tokio::task::yield_now().await;
    }
    panic!("ack wait stalled: {arrived}/{count} for {path}");
}

/// Liveness probe: the entity's state is readable.
async fn wait_entity_exists(system: &ActorSystem, path: &ActorPath) {
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

/// Tells `count` commands at a JUST-SPAWNED entity and waits for the
/// acks (push-driven). The spawn must happen in the same iteration
/// (fresh path): telling a path nobody owns resolves to `Err`
/// immediately. Must run inside the runtime.
async fn drive_and_settle(
    system: &ActorSystem,
    cursor: &std::cell::Cell<u64>,
    path: &ActorPath,
    count: u64,
) {
    for n in 0..count as i64 {
        if let Err(envelope) = system.tell(path.clone(), Tick { n }).await {
            panic!("tell to {path} refused: {envelope:?}");
        }
    }
    cursor.set(wait_acked(system, cursor.get(), path, count).await);
}

/// Tells `count` commands at a PERSISTENT entity, waiting for exactly
/// `count` acks since the tap cursor captured BEFORE the tells (the
/// entity persists across iterations; the cursor makes the window
/// this-iteration-only). Must run inside the runtime.
async fn drive_and_settle_watermark(
    system: &ActorSystem,
    cursor: &std::cell::Cell<u64>,
    path: &ActorPath,
    count: u64,
) {
    for n in 0..count as i64 {
        if let Err(envelope) = system.tell(path.clone(), Tick { n }).await {
            panic!("tell to {path} refused: {envelope:?}");
        }
    }
    cursor.set(wait_acked(system, cursor.get(), path, count).await);
}

/// Per-iteration entity paths: criterion iterates each bench thousands of
/// times, and spawn is one-per-path — so every measured iteration gets a
/// fresh path (and therefore a fresh journal, keeping iterations
/// independent).
struct Iterations(std::sync::atomic::AtomicU64);
impl Iterations {
    fn next_path(&self, prefix: &str) -> ActorPath {
        let n = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ActorPath::new(format!("{prefix}-{n}"))
    }
}

/// Seeds a push-wait cursor: the offset just past the newest retained
/// tap fact (sync, unmeasured setup). Each iteration's `wait_acked`
/// advances the cursor it borrows, so completion windows never overlap
/// across iterations.
fn seed_cursor(system: &ActorSystem) -> std::cell::Cell<u64> {
    std::cell::Cell::new(
        system
            .tap_facts()
            .last()
            .map(|f| f.offset + 1)
            .unwrap_or(0),
    )
}

// ---------------------------------------------------------------------------
// tell_baseline: one producer, one fresh entity, full send→fold→ack cycle.
// ---------------------------------------------------------------------------

fn tell_baseline(c: &mut Criterion) {
    let (system, rt) = spawn_system();
    let iterations = Iterations(std::sync::atomic::AtomicU64::new(0));
    let cursor = seed_cursor(&system);

    let mut group = c.benchmark_group("e2e/tell_baseline");
    // ONE ELEMENT = one message, send→fold→ack COMPLETE (the file-wide
    // rule: never a bare tell). So criterion's elem/s reads as msg/s.
    // 1 = the true single-message floor (the push-wait detects its ack
    // at scheduler scale); 64 and 512 are the batch shapes.
    group.sample_size(30);
    for size in [1u64, 64, 512] {
        group.throughput(criterion::Throughput::Elements(size));
        group.bench_function(format!("{size}_messages"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let path = iterations.next_path("bench/tell-baseline");
                    spawn_accum(&system, &path).await;
                    drive_and_settle(&system, &cursor, &path, size).await;
                });
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// producer_scaling: P concurrent producers on one fresh entity — the
// global lock-contention ceiling (tracks the atomicbool/lock work).
// ---------------------------------------------------------------------------

fn producer_scaling(c: &mut Criterion) {
    let (system, rt) = spawn_system();
    let iterations = Iterations(std::sync::atomic::AtomicU64::new(0));
    let cursor = seed_cursor(&system);

    let mut group = c.benchmark_group("e2e/producer_scaling");
    // ONE ELEMENT = one message committed to the shared entity. elem/s
    // = msg/s. Matrix: P producers × batch size; the 1-message case
    // runs with ONE producer only (distributing a single message across
    // producers measures nothing).
    group.sample_size(20);

    // The single-message floor: one producer, one fresh entity, one
    // full send→fold→ack cycle.
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("producers_1_1_message", |b| {
        b.iter(|| {
            rt.block_on(async {
                let path = iterations.next_path("bench/scaling");
                spawn_accum(&system, &path).await;
                drive_and_settle(&system, &cursor, &path, 1).await;
            });
        });
    });

    for producers in [1usize, 2, 4, 8] {
        for size in [64u64, 512] {
            group.throughput(criterion::Throughput::Elements(size));
            group.bench_with_input(
                criterion::BenchmarkId::new(format!("{size}_messages"), producers),
                &producers,
                |b, &p| {
                    b.iter(|| {
                        rt.block_on(async {
                            // Fresh entity per iteration: each producer
                            // sends its share, completion is the exact
                            // ack count (push-wait on the shared
                            // cursor; acks may interleave across the
                            // producers, so they are COUNTED, never
                            // sequenced).
                            let path = iterations.next_path("bench/scaling");
                            spawn_accum(&system, &path).await;
                            let per = size / p as u64;
                            let barrier = Arc::new(Barrier::new(p));
                            let mut tasks = Vec::new();
                            for _ in 0..p {
                                let system = system.clone();
                                let path = path.clone();
                                let barrier = barrier.clone();
                                tasks.push(tokio::spawn(async move {
                                    barrier.wait().await;
                                    for _ in 0..per {
                                        system
                                            .tell(path.clone(), Tick { n: 1 })
                                            .await
                                            .expect("delivered");
                                    }
                                }));
                            }
                            for task in tasks {
                                task.await.expect("producer");
                            }
                            let next = wait_acked(&system, cursor.get(), &path, size).await;
                            cursor.set(next);
                        });
                    });
                },
            );
        }
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// payload_size: 100B / 10KB / 1MB commands — the JSON-waist clone tax
// (tracks the Arc<Json> improvement).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// payload_size: realistic application messages (a few typed fields, a
// small tag vec, one bulk body string), one tell per measured message.
// The reported time IS one tell→fold→ack cycle at that body size —
// comparable 1:1 against tell_baseline's per-message number.
// ---------------------------------------------------------------------------

fn payload_size(c: &mut Criterion) {
    let (system, rt) = spawn_system();
    let iterations = Iterations(std::sync::atomic::AtomicU64::new(0));
    let cursor = seed_cursor(&system);

    let mut group = c.benchmark_group("e2e/payload_size");
    group.sample_size(30);
    // 2KB is the common ceiling; large bodies are included because the
    // bench exists to show how cost scales past it.
    for (label, size) in [
        ("500B", 500usize),
        ("2KB", 2_000),
        ("64KB", 65_536),
        ("1MB", 1_000_000),
    ] {
        let chunk = realistic_chunk(size);
        // The wire size of one message (body + fields), for honest
        // MiB/s throughput. ONE BYTE-ELEMENT = one payload byte of one
        // committed message (Bytes throughput, not Elements — the time
        // is per message; criterion reports MiB/s from this).
        let wire = serde_json::to_vec(&chunk).expect("fixture size").len() as u64;
        group.throughput(criterion::Throughput::Bytes(wire));
        group.bench_function(label, |b| {
            b.iter(|| {
                rt.block_on(async {
                    let path = iterations.next_path("bench/chunk");
                    system.spawn_es::<Bytes, _>(
                        path.clone(),
                        &Json::default(),
                        SpawnOpts::default(),
                        || {
                            vec![Arc::new(
                                trouper::actor::TypedEsAdapter::<Bytes, Chunk>::new::<Chunk>(),
                            )]
                        },
                    );
                    system
                        .tell(path.clone(), chunk.clone())
                        .await
                        .expect("delivered");
                    // One message per iteration: its Acked fact IS the
                    // completion (push-wait, scheduler scale).
                    let next = wait_acked(&system, cursor.get(), &path, 1).await;
                    cursor.set(next);
                });
            });
        });
    }
    group.finish();
}

/// A realistic application message with `size` bytes of body: a few
/// typed fields and a small tag list around one bulk string.
fn realistic_chunk(body_bytes: usize) -> Chunk {
    Chunk {
        kind: "invoice.updated".into(),
        id: format!("inv_{:016x}", body_bytes),
        tags: ["prod", "eu-west", "retry-1"].map(String::from).to_vec(),
        seq: body_bytes as u64,
        body: "x".repeat(body_bytes),
    }
}

// ---------------------------------------------------------------------------
// wide_tree: a WIDE message (one huge array field). On the typed fabric
// the message is one live struct — the runtime never walks it per node,
// so this now measures the fabric + single-encode cost of an unusually
// wide value (kept as a shape extreme next to payload_size).
// ---------------------------------------------------------------------------

fn wide_tree(c: &mut Criterion) {
    let (system, rt) = spawn_system();
    let iterations = Iterations(std::sync::atomic::AtomicU64::new(0));
    let cursor = seed_cursor(&system);

    let mut group = c.benchmark_group("e2e/wide_tree");
    group.sample_size(20);
    for (label, elements) in [("100k_nodes", 100_000usize), ("1M_nodes", 1_000_000)] {
        let chunk = WideChunk {
            filler: vec![0u8; elements],
        };
        // Same Bytes rule as payload_size: one byte-element = one filler
        // byte of one committed wide message (criterion reports MiB/s).
        let wire = serde_json::to_vec(&chunk).expect("fixture size").len() as u64;
        group.throughput(criterion::Throughput::Bytes(wire));
        group.bench_function(label, |b| {
            b.iter(|| {
                rt.block_on(async {
                    let path = iterations.next_path("bench/wide");
                    system.spawn_es::<Wide, _>(
                        path.clone(),
                        &Json::default(),
                        SpawnOpts::default(),
                        || {
                            vec![Arc::new(
                                trouper::actor::TypedEsAdapter::<Wide, WideChunk>::new::<WideChunk>(
                                ),
                            )]
                        },
                    );
                    system
                        .tell(path.clone(), chunk.clone())
                        .await
                        .expect("delivered");
                    // One message per iteration: its Acked fact IS the
                    // completion (push-wait, scheduler scale).
                    let next = wait_acked(&system, cursor.get(), &path, 1).await;
                    cursor.set(next);
                });
            });
        });
    }
    group.finish();
}

#[derive(Serialize, Deserialize, Default)]
struct Wide {
    count: u64,
}

impl EventSourcedActor for Wide {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<WideChunk>()
            .emits::<Chunked>()
            .kind(ActorKind::EventSourced)
    }
    fn restore(_args: &Json) -> Self {
        Self::default()
    }
    fn apply(&mut self, event: &trouper::envelope::Event) {
        if event.schema.as_str() == "Chunked" {
            self.count += 1;
        }
    }
}
impl trouper::actor::CommandHandler<WideChunk> for Wide {
    fn handle(&self, _cmd: WideChunk, _ctx: &mut CmdCtx<'_>) -> trouper::envelope::Events {
        trouper::envelope::Events::one(Chunked { bytes: 0 })
    }
}

// ---------------------------------------------------------------------------
// fanout: one publisher, H `.handles` echo sinks — ask RTT per sink makes
// completion exact without counters (tracks per-handler clone/decode).
// ---------------------------------------------------------------------------

fn fanout(c: &mut Criterion) {
    let (system, rt) = spawn_system();

    let mut group = c.benchmark_group("e2e/fanout");
    // ONE ELEMENT = one ASK ROUNDTRIP (request delivered + reply
    // received through one of N echo services). NOT a broadcast fan-out
    // despite the name. elem/s = roundtrips/s. Matrix: H handlers ×
    // asks per iteration. Completion is the reply itself — already
    // push, no wait mechanism involved at any ask count.
    group.sample_size(20);
    for handlers in [1usize, 8, 64] {
        // Spawns are idempotent-free: each handler count gets its own
        // band of paths so groups never collide.
        let paths: Vec<ActorPath> = (0..handlers)
            .map(|index| ActorPath::new(format!("bench/echo-{handlers}/{index}")))
            .collect();
        rt.block_on(async {
            for path in &paths {
                system.spawn_service::<Echo, _>(
                    path.clone(),
                    &Json::default(),
                    SpawnOpts::default(),
                    || {
                        vec![Arc::new(
                            trouper::actor::TypedServiceAdapter::<Echo, Ping>::new::<Ping>(),
                        )]
                    },
                );
                // Liveness: the first ask roundtrip succeeds. Bounded by
                // WALL TIME (a failing ask burns its full timeout, so an
                // attempt-bounded loop would retry for hours).
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                loop {
                    match system
                        .ask(
                            path.clone(),
                            Ping { filler: Vec::new() },
                            Duration::from_secs(1),
                        )
                        .await
                    {
                        Ok(_) => break,
                        Err(e) => {
                            if tokio::time::Instant::now() >= deadline {
                                panic!("echo never went live: {e:?}");
                            }
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    }
                }
            }
        });

        for asks in [1u64, 64, 512] {
            group.throughput(criterion::Throughput::Elements(asks));
            group.bench_function(format!("handlers_{handlers}_{asks}_asks"), |b| {
                b.iter(|| {
                    rt.block_on(async {
                        for _ in 0..asks {
                            for path in &paths {
                                system
                                    .ask(
                                        path.clone(),
                                        Ping { filler: Vec::new() },
                                        Duration::from_secs(5),
                                    )
                                    .await
                                    .expect("echo");
                            }
                        }
                    });
                });
            });
        }
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// overload_block: sustained overload of a Block entity (post-D1 real
// backpressure); asserts the DLQ stays EMPTY — Block is lossless.
// ---------------------------------------------------------------------------

fn overload_block(c: &mut Criterion) {
    let (system, rt) = spawn_system();
    let path = ActorPath::new("bench/overloaded");
    let cursor = seed_cursor(&system);
    rt.block_on(async {
        system.spawn_es::<Accum, _>(
            path.clone(),
            &Json::default(),
            SpawnOpts {
                snapshot: SnapshotCadence::Off,
                mailbox_capacity: 64,
                mailbox_policy: OverloadPolicy::Block,
                high_watermark: None,
                passivation: None,
            },
            || {
                vec![Arc::new(
                    trouper::actor::TypedEsAdapter::<Accum, Tick>::new::<Tick>(),
                )]
            },
        );
        wait_entity_exists(&system, &path).await;
    });

    let mut group = c.benchmark_group("e2e/overload_block");
    // ONE ELEMENT = one message delivered into the Block inbox; elem/s
    // = msg/s. Sizes 64 and 512 fill the inbox from 16 producers
    // (32/producer and 4/producer respectively). The 1-message case
    // exercises the Block inbox UNFILLED — a plain, never-backpressured
    // tell — the no-backpressure datapoint the batch cases are measured
    // against. Lossless in every case: the dead-letter assert stays.
    group.sample_size(15);
    for (size, producers, per) in [(1u64, 1usize, 1u64), (64, 16, 4), (512, 16, 32)] {
        group.throughput(criterion::Throughput::Elements(size));
        group.bench_function(format!("producers_{producers}_{size}_messages"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let barrier = Arc::new(Barrier::new(producers));
                    let mut tasks = Vec::new();
                    for _ in 0..producers {
                        let system = system.clone();
                        let path = path.clone();
                        let barrier = barrier.clone();
                        tasks.push(tokio::spawn(async move {
                            barrier.wait().await;
                            for _ in 0..per {
                                system
                                    .tell(path.clone(), Tick { n: 1 })
                                    .await
                                    .expect("delivered");
                            }
                        }));
                    }
                    for task in tasks {
                        task.await.expect("producer");
                    }
                    // Completion: this iteration's `size` messages all
                    // acked (push-wait; the entity persists across
                    // iterations, the cursor keeps the window
                    // this-iteration-only).
                    let next = wait_acked(&system, cursor.get(), &path, size).await;
                    cursor.set(next);
                    assert_eq!(
                        system.dead_letter_count().await,
                        0,
                        "Block overload must be lossless (post-D1)"
                    );
                });
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// idle_fleet: throughput on ONE busy actor while 1k / 10k idle actors sit
// alongside — the polling-floor tax (tracks the polling-removal
// improvement). Both fleets drive the same 1/64/512-message cases; the
// idle fleet's cost is the wall-clock delta vs tell_baseline, never the
// element count.
// ---------------------------------------------------------------------------

/// Spawns `fleet` idle `Accum` entities under `prefix` (liveness-probed,
/// unmeasured) plus the busy entity the cases will drive. Must be called
/// inside the runtime.
async fn spawn_fleet(system: &ActorSystem, prefix: &str, fleet: usize, busy: &ActorPath) {
    for index in 0..fleet {
        let path = ActorPath::new(format!("{prefix}-{index}"));
        system.spawn_es::<Accum, _>(
            path.clone(),
            &Json::default(),
            SpawnOpts::default(),
            || {
                vec![Arc::new(
                    trouper::actor::TypedEsAdapter::<Accum, Tick>::new::<Tick>(),
                )]
            },
        );
    }
    for index in 0..fleet {
        let path = ActorPath::new(format!("{prefix}-{index}"));
        wait_entity_exists(system, &path).await;
    }
    spawn_accum(system, busy).await;
}

/// The 1/64/512-message cases on one busy entity, labeled
/// `{fleet_label}_idle_{size}_messages` (spawn is one-per-path, so the
/// fleet itself is fixture: the sample size drops for the 10k fleet to
/// keep total wall time sane, like the file's other heavy cases).
fn drive_fleet_cases(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    system: &ActorSystem,
    rt: &Arc<Runtime>,
    fleet_label: &str,
    busy: &ActorPath,
    sample_size: usize,
) {
    let cursor = seed_cursor(system);
    group.sample_size(sample_size);
    for size in [1u64, 64, 512] {
        group.throughput(criterion::Throughput::Elements(size));
        group.bench_function(format!("{fleet_label}_idle_{size}_messages"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    drive_and_settle_watermark(system, &cursor, busy, size).await;
                });
            });
        });
    }
}

fn idle_fleet(c: &mut Criterion) {
    let (system, rt) = spawn_system();

    let mut group = c.benchmark_group("e2e/idle_fleet");
    // ONE ELEMENT = one message committed to the BUSY entity; elem/s =
    // msg/s on the busy path.
    let busy_1k = ActorPath::new("bench/busy-1k");
    rt.block_on(spawn_fleet(&system, "bench/idle", 1_000, &busy_1k));
    drive_fleet_cases(&mut group, &system, &rt, "1k", &busy_1k, 20);

    // The same shape an order of magnitude wider: its own busy entity,
    // isolated from the 1k run.
    let busy_10k = ActorPath::new("bench/busy-10k");
    rt.block_on(spawn_fleet(&system, "bench/idle-10k", 10_000, &busy_10k));
    drive_fleet_cases(&mut group, &system, &rt, "10k", &busy_10k, 10);
    group.finish();
}

// ---------------------------------------------------------------------------
// swarm: many-to-many at scale — P producers driving R service sinks with
// round-robin fan. The registry/kernel tables and the poll floor scale with
// ACTOR COUNT, so this is where the 128–2048 fleet regime shows whether the
// global locks (or anything else) turn the curve into a cliff.
// ---------------------------------------------------------------------------

/// A counting service sink whose counter is handed over at start (the
/// swarm bench's receivers; one AtomicU64 per sink).
struct SwarmSink {
    received: Arc<std::sync::atomic::AtomicU64>,
}

static SWARM_SINKS: std::sync::Mutex<Vec<Arc<std::sync::atomic::AtomicU64>>> =
    std::sync::Mutex::new(Vec::new());

impl ServiceActor for SwarmSink {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }
    async fn start(
        _args: &Json,
    ) -> Result<Self, error_stack::Report<trouper::registry::RegistryError>> {
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        SWARM_SINKS.lock().expect("sinks").push(counter.clone());
        Ok(Self { received: counter })
    }
}
impl trouper::actor::MsgHandler<Tick> for SwarmSink {
    async fn handle(&mut self, _msg: Tick, _ctx: &mut MsgCtx<'_>) {
        self.received
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

fn swarm(c: &mut Criterion) {
    let (system, rt) = spawn_system();

    let mut group = c.benchmark_group("e2e/swarm");
    group.sample_size(10);

    // Square-ish (P, R) samples: spawn-once per R, sustained traffic.
    let shapes = [(32usize, 128usize), (128, 512), (512, 2048)];
    for (producers, receivers) in shapes {
        let paths: Vec<ActorPath> = (0..receivers)
            .map(|index| ActorPath::new(format!("bench/swarm-{receivers}/{index}")))
            .collect();
        let counters_before = SWARM_SINKS.lock().expect("sinks").len();

        rt.block_on(async {
            for path in &paths {
                system.spawn_service::<SwarmSink, _>(
                    path.clone(),
                    &Json::default(),
                    SpawnOpts::default(),
                    || {
                        vec![Arc::new(trouper::actor::TypedServiceAdapter::<
                            SwarmSink,
                            Tick,
                        >::new::<Tick>())]
                    },
                );
            }
        });
        // `start()` runs inside each spawned task, so the counters land in
        // the registry asynchronously: wait (bounded) for the full band.
        let counters: Vec<Arc<std::sync::atomic::AtomicU64>> = rt.block_on(async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                let ready = SWARM_SINKS.lock().expect("sinks").len() >= counters_before + receivers;
                if ready {
                    break SWARM_SINKS.lock().expect("sinks")[counters_before..].to_vec();
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("swarm sinks never registered");
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        });
        assert_eq!(counters.len(), receivers, "swarm counters misaligned");

        // One warmup tell per sink: proves the door is live before the
        // measured phase. Both the tell and the counter check are bounded
        // by wall time (spawn registration is async; a tell to an
        // unregistered path returns Err immediately).
        rt.block_on(async {
            // Tell every sink once (doors are registered; a slow loop just
            // answers later), then wait for the counters to prove liveness.
            // A shared fleet-wide deadline: per-sink budgets compound into
            // minutes at r2048 when the runtime is still spinning up loops.
            for path in &paths {
                system
                    .tell(path.clone(), Tick { n: 1 })
                    .await
                    .expect("door");
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            for (index, path) in paths.iter().enumerate() {
                let counter = counters[index].clone();
                while counter.load(std::sync::atomic::Ordering::Relaxed) == 0 {
                    if tokio::time::Instant::now() >= deadline {
                        panic!("swarm sink never went live: {path}");
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
        });

        // 32 tells per sink × receivers sinks.
        let total: u64 = 32 * receivers as u64;
        // ONE ELEMENT = one tell to one sink, commit complete. NOTE the
        // count is 32 × RECEIVERS (every producer drives every sink), so
        // elem/s here is aggregate many-to-many msg/s — NOT the rate of
        // one producer, and NOT producers × receivers.
        group.throughput(criterion::Throughput::Elements(total));
        group.bench_function(format!("p{producers}_r{receivers}"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    for counter in &counters {
                        counter.store(0, std::sync::atomic::Ordering::Relaxed);
                    }
                    let barrier = Arc::new(Barrier::new(producers));
                    let per_producer = receivers / producers;
                    let per_sink = 32u64;
                    let mut tasks = Vec::new();
                    for p in 0..producers {
                        let system = system.clone();
                        let paths = paths.clone();
                        let barrier = barrier.clone();
                        tasks.push(tokio::spawn(async move {
                            barrier.wait().await;
                            let mine = &paths[p * per_producer..(p + 1) * per_producer];
                            // Each producer owns a disjoint band of sinks;
                            // 32 tells per sink, round-robin across the band.
                            for round in 0..per_sink {
                                for path in mine {
                                    system
                                        .tell(path.clone(), Tick { n: round as i64 })
                                        .await
                                        .expect("delivered");
                                }
                            }
                        }));
                    }
                    for task in tasks {
                        task.await.expect("producer");
                    }
                    // Completion: every sink saw exactly its 32 tells
                    // (producers own disjoint bands, so 32 per sink).
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
                    loop {
                        let done = counters
                            .iter()
                            .all(|c| c.load(std::sync::atomic::Ordering::Relaxed) >= per_sink);
                        if done {
                            break;
                        }
                        if tokio::time::Instant::now() >= deadline {
                            panic!("swarm never settled at p{producers}/r{receivers}");
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    assert_eq!(
                        system.dead_letter_count().await,
                        0,
                        "swarm must be lossless"
                    );
                });
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// projection_read: frontend frame reads over a projector's live fold —
// the render loop's steady-state read cost. "Drawing" IS the closure:
// `try_with_projector_state::<Ledger, _>(path, |ledger| …)` runs over
// the live fold with zero copies (examples/try_with.rs demos the
// pattern). Two shapes:
//   hot_typed  — N live projectors, closure read only (the steady frame);
//   hot_json   — the JSON twin (`projector_state`), serialize + decode
//                per read — the typed seam's alternative, priced.
// ---------------------------------------------------------------------------

fn projection_read(c: &mut Criterion) {
    let (system, rt) = spawn_system();
    // FIXTURE (setup block_on ends before the group; each measured body
    // hops into the runtime fresh — the file's structure rule).
    rt.block_on(async {
        // N projectors, each consuming the entity's broadcast `Ticked`
        // copies. Entities are pre-warmed by one tick each, so every
        // read below is HOT (no wake path in the steady-state shape).
        const N: usize = 64;
        let (projectors, entities): (Vec<ActorPath>, Vec<ActorPath>) = (0..N)
            .map(|index| {
                (
                    ActorPath::new(format!("bench/proj-read-{index}")),
                    ActorPath::new(format!("bench/proj-src-{index}")),
                )
            })
            .unzip();
        for (projector, entity) in projectors.iter().zip(entities.iter()) {
            trouper::builder::spawn_projector_builder::<Ledger>(&system)
                .at(projector.clone())
                .args(Json::default())
                .consumes::<Ticked>()
                .start();
            system.spawn_es::<Accum, _>(
                entity.clone(),
                &Json::default(),
                SpawnOpts::default(),
                || {
                    vec![Arc::new(
                        trouper::actor::TypedEsAdapter::<Accum, Tick>::new::<Tick>(),
                    )]
                },
            );
        }
        // Warm: one tick per source; EVERY projector consumes EVERY
        // source's `Ticked` copy (standalone-projector consumption is by
        // schema, not source — examples/try_with.rs), so each fold ends
        // at exactly N. Waiting for N also proves all copies arrived.
        for entity in entities.iter() {
            system
                .tell(entity.clone(), Tick { n: 1 })
                .await
                .expect("delivered");
        }
        for projector in projectors.iter() {
            let mut settled = false;
            for _ in 0..30_000 {
                if system
                    .with_projector_state::<Ledger, _>(projector, |l| l.count)
                    .await
                    == Some(N as u64)
                {
                    settled = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            assert!(
                settled,
                "projector {projector} never folded all {N} sources"
            );
        }
    });

    let mut group = c.benchmark_group("e2e/projection_read");
    // ONE ELEMENT = one FRAME READ over a projector's live fold (64 per
    // iteration) — a state READ, no message involved. elem/s = reads/s.
    group.throughput(criterion::Throughput::Elements(64));
    group.sample_size(20);

    // HOT TYPED: one frame across the whole fleet (64 closure reads,
    // zero copies). Misses return None and count as a read of the
    // previous frame — the never-blocking contract; assert the fleet
    // stays readable so a broken projector can't fake a fast bench.
    group.bench_function("hot_typed_64_frames", |b| {
        b.iter(|| {
            rt.block_on(async {
                for index in 0..64usize {
                    let projector = ActorPath::new(format!("bench/proj-read-{index}"));
                    assert!(
                        system
                            .try_with_projector_state::<Ledger, _>(&projector, |l| (
                                l.count,
                                l.tail.len()
                            ))
                            .is_some(),
                        "projector {projector} must serve a typed frame"
                    );
                }
            });
        });
    });

    // HOT JSON: the same frame through the JSON twin (serialize +
    // decode per read) — the typed seam's alternative, priced.
    group.bench_function("hot_json_64_frames", |b| {
        b.iter(|| {
            rt.block_on(async {
                for index in 0..64usize {
                    let projector = ActorPath::new(format!("bench/proj-read-{index}"));
                    assert!(
                        system.projector_state(&projector).await.is_some(),
                        "projector {projector} must serve a JSON frame"
                    );
                }
            });
        });
    });

    // NOTE: there is deliberately no "send then wait for the fold to
    // advance" case here. A GUI never polls for catch-up — its render
    // loop just re-reads hot each frame (the hot cases above). End-to-end
    // "when does the edit appear on screen" composes from existing
    // benches: tell_baseline (commit) + broadcast/fanout (delivery) +
    // hot_typed (the draw).

    group.finish();
}

criterion_group!(
    benches,
    tell_baseline,
    producer_scaling,
    payload_size,
    wide_tree,
    fanout,
    overload_block,
    idle_fleet,
    projection_read,
    swarm
);
criterion_main!(benches);
