//! Usage-shaped benchmarks: full send-message → done cycles against a
//! live multi-thread runtime, the shape a real deployment exercises.
//!
//! Every bench measures COMPLETE processing, never bare `tell` loops (a
//! tell resolves at channel accept; without a completion barrier the
//! number is a channel benchmark, not an actor one). Completion comes
//! from a per-iteration FRESH entity whose fold is polled to an exact
//! count, or from a lease-backed `ask` roundtrip. Fresh entities keep
//! criterion iterations independent — no cross-iteration accounting.
//!
//! Structure: setup runs inside `rt.block_on`; the measured `b.iter`
//! bodies hop into the runtime via `rt.block_on` (criterion's thread is
//! never a runtime worker; the runtime lives in an Arc so `block_on`
//! works from anywhere outside it).
//!
//! Improvements these track (see .plans/perf-fixes/plan.md):
//! `producer_scaling` → lock/atomicbool improvements · `payload_size` →
//! Arc<Json> payloads · `idle_fleet` → polling removal.

use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio::sync::Barrier;

use trouper::actor::{ActorKind, ActorPath, EventSourcedActor, ServiceActor};
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
        if event.schema.as_str() == "Ticked@1" {
            self.count += 1;
        }
    }
}
impl trouper::actor::CommandHandler<Tick> for Accum {
    fn handle(&self, _cmd: Tick, _ctx: &mut CmdCtx<'_>) -> trouper::envelope::Events {
        trouper::envelope::Events::from_vec(vec![trouper::envelope::Event::new(
            Ticked::schema_id(),
            Json::of(&Ticked { n: 1 }),
        )])
    }
}

/// A command whose payload bulk is `filler` bytes — the payload_size bench
/// sweeps `filler` across orders of magnitude.
#[derive(Clone, Command, serde::Serialize, serde::Deserialize)]
struct Chunk {
    filler: Vec<u8>,
}

#[derive(Event, serde::Serialize, serde::Deserialize)]
struct Chunked {
    bytes: usize,
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
        if event.schema.as_str() == "Chunked@1" {
            self.total += event.payload["bytes"].as_u64().unwrap_or(0);
        }
    }
}
impl trouper::actor::CommandHandler<Chunk> for Bytes {
    fn handle(&self, cmd: Chunk, _ctx: &mut CmdCtx<'_>) -> trouper::envelope::Events {
        trouper::envelope::Events::from_vec(vec![trouper::envelope::Event::new(
            Chunked::schema_id(),
            Json::of(&Chunked {
                bytes: cmd.filler.len(),
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
    async fn start(_args: &Json) -> Result<Self, error_stack::Report<trouper::registry::RegistryError>> {
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

fn spawn_system() -> (ActorSystem, Arc<Runtime>) {    let rt = Arc::new(
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
        vec![Arc::new(trouper::actor::TypedEsAdapter::<Accum, Tick>::new::<Tick>())]
    });
    wait_entity_exists(system, path).await;
}

/// Waits until the entity's count reaches `count` (bounded; panics on
/// stall so a bench fails loudly instead of reporting a fast bogus time).
/// Must run inside the runtime.
async fn wait_entity(system: &ActorSystem, path: &ActorPath, count: u64) {
    for _ in 0..30_000 {
        if system
            .with_es_state::<Accum, _>(path, |a| a.count)
            .await
            .is_some_and(|c| c >= count)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("entity {path} never reached count {count}");
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

/// Drives `count` tells at a fresh entity and waits for the exact fold
/// count. Must run inside the runtime.
/// Tells `count` commands at a JUST-SPAWNED entity and waits for the
/// exact fold count. The spawn must happen in the same iteration (fresh
/// path): telling a path nobody owns resolves to `Err` immediately.
/// Must run inside the runtime.
async fn drive_and_settle(system: &ActorSystem, path: &ActorPath, count: u64) {
    for n in 0..count as i64 {
        if let Err(envelope) = system.tell(path.clone(), Tick { n }).await {
            panic!("tell to {path} refused: {envelope:?}");
        }
    }
    wait_entity(system, path, count).await;
}

/// Tells `count` commands at a PERSISTENT entity, waiting until its fold
/// has grown by exactly `count` from the watermark captured BEFORE the
/// tells. Must run inside the runtime.
async fn drive_and_settle_watermark(system: &ActorSystem, path: &ActorPath, count: u64) {
    let before = system
        .with_es_state::<Accum, _>(path, |a| a.count)
        .await
        .unwrap_or(0);
    for n in 0..count as i64 {
        if let Err(envelope) = system.tell(path.clone(), Tick { n }).await {
            panic!("tell to {path} refused: {envelope:?}");
        }
    }
    wait_entity(system, path, before + count).await;
}

/// Per-iteration entity paths: criterion iterates each bench thousands of
/// times, and spawn is one-per-path — so every measured iteration gets a
/// fresh path (and therefore a fresh journal, keeping iterations
/// independent).
struct Iterations(std::sync::atomic::AtomicU64);
impl Iterations {
    fn next_path(&self, prefix: &str) -> ActorPath {
        let n = self
            .0
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ActorPath::new(format!("{prefix}-{n}"))
    }
}

// ---------------------------------------------------------------------------
// tell_baseline: one producer, one fresh entity, full send→fold→ack cycle.
// ---------------------------------------------------------------------------

fn tell_baseline(c: &mut Criterion) {
    let (system, rt) = spawn_system();
    let iterations = Iterations(std::sync::atomic::AtomicU64::new(0));

    let mut group = c.benchmark_group("e2e/tell_baseline");
    group.throughput(criterion::Throughput::Elements(64));
    group.sample_size(30);
    group.bench_function("64_messages", |b| {
        b.iter(|| {
            rt.block_on(async {
                let path = iterations.next_path("bench/tell-baseline");
                spawn_accum(&system, &path).await;
                drive_and_settle(&system, &path, 64).await;
            });
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// producer_scaling: P concurrent producers on one fresh entity — the
// global lock-contention ceiling (tracks the atomicbool/lock work).
// ---------------------------------------------------------------------------

fn producer_scaling(c: &mut Criterion) {
    let (system, rt) = spawn_system();
    let iterations = Iterations(std::sync::atomic::AtomicU64::new(0));

    let mut group = c.benchmark_group("e2e/producer_scaling");
    group.throughput(criterion::Throughput::Elements(128));
    group.sample_size(20);
    for producers in [1usize, 2, 4, 8] {
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(producers),
            &producers,
            |b, &p| {
                b.iter(|| {
                    rt.block_on(async {
                        // Fresh entity per iteration: each producer sends
                        // its share, completion is the exact total count.
                        let path = iterations.next_path("bench/scaling");
                        spawn_accum(&system, &path).await;
                        let per = 128 / p;
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
                        wait_entity(&system, &path, 128).await;
                    });
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// payload_size: 100B / 10KB / 1MB commands — the JSON-waist clone tax
// (tracks the Arc<Json> improvement).
// ---------------------------------------------------------------------------

fn payload_size(c: &mut Criterion) {
    let (system, rt) = spawn_system();
    let iterations = Iterations(std::sync::atomic::AtomicU64::new(0));

    let mut group = c.benchmark_group("e2e/payload_size");
    group.sample_size(20);
    for (label, size) in [("100B", 100usize), ("10KB", 10_000), ("1MB", 1_000_000)] {
        let chunk = Chunk {
            filler: vec![0u8; size],
        };
        group.throughput(criterion::Throughput::Elements(16));
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
                    for _ in 0..16 {
                        system
                            .tell(path.clone(), chunk.clone())
                            .await
                            .expect("delivered");
                    }
                    for _ in 0..30_000 {
                        if system
                            .with_es_state::<Bytes, _>(&path, |b| b.total)
                            .await
                            .is_some_and(|t| t >= 16 * size as u64)
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                });
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// fanout: one publisher, H `.handles` echo sinks — ask RTT per sink makes
// completion exact without counters (tracks per-handler clone/decode).
// ---------------------------------------------------------------------------

fn fanout(c: &mut Criterion) {
    let (system, rt) = spawn_system();

    let mut group = c.benchmark_group("e2e/fanout");
    group.throughput(criterion::Throughput::Elements(32));
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
                        .ask(path.clone(), Ping { filler: Vec::new() }, Duration::from_secs(1))
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

        group.bench_function(format!("handlers_{handlers}"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..32 {
                        for path in &paths {
                            system
                                .ask(path.clone(), Ping { filler: Vec::new() }, Duration::from_secs(5))
                                .await
                                .expect("echo");
                        }
                    }
                });
            });
        });
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
                vec![Arc::new(trouper::actor::TypedEsAdapter::<Accum, Tick>::new::<Tick>())]
            },
        );
        wait_entity_exists(&system, &path).await;
    });

    let mut group = c.benchmark_group("e2e/overload_block");
    group.throughput(criterion::Throughput::Elements(512));
    group.sample_size(15);
    group.bench_function("16_producers_512_messages", |b| {
        b.iter(|| {
            rt.block_on(async {
                let before = system
                    .with_es_state::<Accum, _>(&path, |a| a.count)
                    .await
                    .unwrap_or(0);
                let barrier = Arc::new(Barrier::new(16));
                let mut tasks = Vec::new();
                for _ in 0..16 {
                    let system = system.clone();
                    let path = path.clone();
                    let barrier = barrier.clone();
                    tasks.push(tokio::spawn(async move {
                        barrier.wait().await;
                        for _ in 0..32 {
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
                // Completion: this iteration's 512 all folded, counted
                // from the pre-batch watermark (the entity persists
                // across iterations; a post-batch read would over-count
                // by whatever already folded).
                wait_entity(&system, &path, before + 512).await;
                assert_eq!(
                    system.dead_letter_count().await, 0,
                    "Block overload must be lossless (post-D1)"
                );
            });
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// idle_fleet: throughput on ONE busy actor while 1_000 idle actors poll —
// the polling-floor tax (tracks the polling-removal improvement).
// ---------------------------------------------------------------------------

fn idle_fleet(c: &mut Criterion) {
    let (system, rt) = spawn_system();
    rt.block_on(async {
        let fleet = 1_000usize;
        for index in 0..fleet {
            let path = ActorPath::new(format!("bench/idle-{index}"));
            system.spawn_es::<Accum, _>(path.clone(), &Json::default(), SpawnOpts::default(), || {
                vec![Arc::new(trouper::actor::TypedEsAdapter::<Accum, Tick>::new::<Tick>())]
            });
        }
        for index in 0..fleet {
            let path = ActorPath::new(format!("bench/idle-{index}"));
            wait_entity_exists(&system, &path).await;
        }
        // The measured body's target: one busy entity among the idlers.
        let busy = ActorPath::new("bench/busy");
        spawn_accum(&system, &busy).await;
    });

    let mut group = c.benchmark_group("e2e/idle_fleet");
    group.throughput(criterion::Throughput::Elements(64));
    group.sample_size(20);
    group.bench_function("1000_idle_producers_1", |b| {
        b.iter(|| {
            rt.block_on(async {
                let path = ActorPath::new("bench/busy");
                drive_and_settle_watermark(&system, &path, 64).await;
            });
        });
    });
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
    async fn start(_args: &Json) -> Result<Self, error_stack::Report<trouper::registry::RegistryError>> {
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
                        vec![Arc::new(
                            trouper::actor::TypedServiceAdapter::<SwarmSink, Tick>::new::<Tick>(),
                        )]
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
                system.tell(path.clone(), Tick { n: 1 }).await.expect("door");
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
                        system.dead_letter_count().await, 0,
                        "swarm must be lossless"
                    );
                });
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    tell_baseline,
    producer_scaling,
    payload_size,
    fanout,
    overload_block,
    idle_fleet,
    swarm
);
criterion_main!(benches);
