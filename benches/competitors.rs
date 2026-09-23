//! Framework tell comparison: the same prime-then-spam harness driving
//! trouper, kameo, and ractor service-actor tells. ONE producer actor
//! hot-loops `n` tells at ONE sink actor per measured leg — messages are
//! never injected at the edge; every send leaves a handler, the only
//! realistic actor-system shape.
//!
//! Mechanics (identical in every leg):
//!
//! 1. SETUP (untimed, `iter_batched`'s setup closure): the leg's module
//!    thread builds the framework's runtime and spawns producer + sink
//!    actors. (trouper-specific: the producer declares
//!    `.emits::<Tick>()` — its flush gate dead-letters undeclared
//!    outbound schemas, so the declaration IS part of the send path.) One real actor message (`Prime`) wakes the producer's
//!    handler, which acknowledges on the `prime` kanal channel, then
//!    parks awaiting the `start` kanal channel — the producer enters its
//!    handler exactly once and never receives another message. The bench
//!    waits for the prime ack, settles 50 ms (runtime catches up), and
//!    only then opens the timed window.
//! 2. TIMED (the entire criterion body — see `run`): `start_tx.send(())`
//!    wakes the parked handler, which hot-loops all `n` tells; the sink
//!    counts receives and fires `done_tx` at the target;
//!    `done_rx.recv()` returns.
//! 3. TEARDOWN: `bench_done_tx` releases the parked module thread, which
//!    drops the runtime; the leg is discarded (fresh leg per iteration).
//!
//! One throughput element = one fully processed message (send → handler
//! → sink count), per `Throughput::Elements(n)`.
//!
//! Mailbox shapes — each framework's supported set, honestly:
//!
//! | leg               | mailbox                                   |
//! |-------------------|-------------------------------------------|
//! | trouper           | bounded-64 (its default, Block policy)    |
//! | trouper-unbounded | 2^20-capacity Block (no true unbounded)   |
//! | kameo             | bounded-64 (its default)                  |
//! | kameo-unbounded   | native unbounded                          |
//! | ractor            | native unbounded (core has no bounded)    |
//!
//! Single-case runs:
//!
//! ```text
//! cargo bench --bench competitors -- --warm-up-time 1 --measurement-time 2 --sample-size 10 "competitors/trouper/64"
//! ```
//!
//! Adding a framework: copy one leg module (~70 lines, same two-function
//! shape: `start(bounded, n) -> Pair`, `run(pair, n)`) and add one
//! `bench_function`.

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

/// Message counts: small (mailbox-scale), medium, and large enough that
/// amortized per-send cost dominates.
const SIZES: [u64; 3] = [64, 512, 2_048];

/// Settle time after the prime ack: the runtime finishes spawn
/// bookkeeping so the timed window opens on a hot, quiet system.
const SETTLE: Duration = Duration::from_millis(50);

/// What a leg's `start` hands to `run`. `n` is baked into the actors at
/// start time (the sink's done target), so `run` is a pure signal
/// round-trip.
pub struct Pair {
    /// Harness → producer: "spam n tells now".
    pub start_tx: kanal::Sender<()>,
    /// Sink → harness: "target met".
    pub done_rx: kanal::Receiver<()>,
    /// Harness → module thread: "release the runtime".
    pub bench_done_tx: kanal::Sender<()>,
    /// The module thread parking its runtime; joined by `run`.
    pub thread: std::thread::JoinHandle<()>,
}

/// The timed body — byte-identical for every framework. `n` was recorded
/// at `start` (the actors carry the target); the routine is the start
/// signal, the done signal, and the release.
fn run(pair: Pair, n: u64) {
    let _ = n;
    pair.start_tx.send(()).expect("start signal");
    pair.done_rx.recv().expect("done signal");
    pair.bench_done_tx.send(()).expect("release thread");
    pair.thread.join().expect("module thread");
}

/// Waits (sync, on the bench thread) for a leg's prime ack — the sync
/// twin of the async handle the producer holds.
fn wait_prime(prime_rx: &kanal::Receiver<()>) {
    loop {
        match prime_rx.try_recv() {
            Ok(Some(())) => return,
            _ => std::thread::sleep(Duration::from_millis(1)),
        }
    }
}

// ── trouper ───────────────────────────────────────────────────────────────

mod trouper_leg {
    use super::{Pair, SETTLE, wait_prime};
    use std::time::Duration;

    use trouper::actor::{MsgHandler, ServiceActor};
    use trouper::inbox::OverloadPolicy;
    use trouper::prelude::*;
    use trouper::registry::RegistryError;

    #[derive(Clone, Command, serde::Serialize, serde::Deserialize)]
    struct Prime {
        sink: ActorPath,
    }

    #[derive(Clone, Command, serde::Serialize, serde::Deserialize)]
    struct Tick;

    struct Producer {
        prime_ack: Option<kanal::AsyncSender<()>>,
        start: Option<kanal::AsyncReceiver<()>>,
        target: u64,
    }

    impl ServiceActor for Producer {
        fn manifest() -> ActorManifest {
            ActorManifest::new().kind(ActorKind::Service)
        }
        async fn start(_args: &Json) -> Result<Self, error_stack::Report<RegistryError>> {
            // Unused: the spawn rides `.start_with` (kanal handles
            // cannot ride JSON args).
            unreachable!("trouper producer spawns via start_with")
        }
    }

    impl MsgHandler<Prime> for Producer {
        async fn handle(&mut self, msg: Prime, ctx: &mut MsgCtx<'_>) {
            self.prime_ack
                .take()
                .expect("prime ack handle")
                .send(())
                .await
                .expect("prime ack");
            // Park mid-handler until the timed window opens.
            self.start
                .take()
                .expect("start handle")
                .recv()
                .await
                .expect("start signal");
            // The spam: trouper's real actor-to-actor path. `ctx.send`
            // records the intent; the kernel flushes it after this
            // message (the prime) commits — the whole loop leaves
            // through this one handler execution.
            let sink = msg.sink;
            for _ in 0..self.target {
                ctx.send(Address::Path(sink.clone()), Tick, None);
            }
        }
    }

    struct Sink {
        seen: u64,
        done: Option<kanal::AsyncSender<()>>,
        target: u64,
    }

    impl ServiceActor for Sink {
        fn manifest() -> ActorManifest {
            ActorManifest::new().kind(ActorKind::Service)
        }
        async fn start(_args: &Json) -> Result<Self, error_stack::Report<RegistryError>> {
            // Unused: the spawn rides `.start_with`.
            unreachable!("trouper sink spawns via start_with")
        }
    }

    impl MsgHandler<Tick> for Sink {
        async fn handle(&mut self, _msg: Tick, _ctx: &mut MsgCtx<'_>) {
            self.seen += 1;
            if self.seen == self.target {
                self.done
                    .take()
                    .expect("done handle")
                    .send(())
                    .await
                    .expect("done signal");
            }
        }
    }

    /// Builds the runtime + both actors on a fresh thread, primes the
    /// producer (Prime → ack), settles, then parks. `bounded` picks the
    /// mailbox: `true` = the default bounded-64; `false` = the
    /// large-capacity Block stand-in for unbounded (capacity 2^20 is
    /// unbounded at these message counts).
    pub fn start(bounded: bool, n: u64) -> Pair {
        let (start_tx, start_rx) = kanal::bounded::<()>(1);
        let (prime_tx, prime_rx) = kanal::bounded::<()>(1);
        let (done_tx, done_rx) = kanal::bounded::<()>(1);
        let (release_tx, bench_done_rx) = kanal::bounded_async::<()>(1);
        let bench_done_tx = release_tx.clone_sync();

        let capacity = if bounded { 64 } else { 1 << 20 };
        let thread = std::thread::Builder::new()
            .name("trouper-leg".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("trouper leg rt");
                rt.block_on(async move {
                    let system = ActorSystem::new(SystemConfig::production());
                    let sink_path = ActorPath::new("bench/competitors-sink");
                    let producer_path = ActorPath::new("bench/competitors-producer");

                    trouper::builder::spawn_service_builder::<Sink>(&system)
                        .at(sink_path.clone())
                        .handles::<Tick>()
                        .mailbox(capacity, OverloadPolicy::Block)
                        .start_with({
                            let done = done_tx.clone().to_async();
                            move || {
                                let done = done.clone();
                                Box::pin(async move {
                                    Ok(Sink {
                                        seen: 0,
                                        done: Some(done),
                                        target: n,
                                    })
                                })
                            }
                        })
                        .start();

                    trouper::builder::spawn_service_builder::<Producer>(&system)
                        .at(producer_path.clone())
                        .handles::<Prime>()
                        .emits::<Tick>()
                        .mailbox(capacity, OverloadPolicy::Block)
                        .start_with({
                            let ack = prime_tx.clone().to_async();
                            let go = start_rx.clone().to_async();
                            move || {
                                let ack = ack.clone();
                                let go = go.clone();
                                Box::pin(async move {
                                    Ok(Producer {
                                        prime_ack: Some(ack),
                                        start: Some(go),
                                        target: n,
                                    })
                                })
                            }
                        })
                        .start();

                    // Liveness: both cells registered before priming.
                    while system.inbox_cursor(&producer_path).is_none() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    while system.inbox_cursor(&sink_path).is_none() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }

                    // ONE real actor message: the producer's handler
                    // acks on the prime channel, then parks on start.
                    system
                        .tell(
                            producer_path.clone(),
                            Prime {
                                sink: sink_path.clone(),
                            },
                        )
                        .await
                        .expect("prime tell");

                    // The module thread parks here for the whole timed
                    // window — returning would drop the runtime and
                    // close the channels mid-measurement.
                    bench_done_rx.recv().await.expect("bench done");
                });
            })
            .expect("trouper leg thread");

        wait_prime(&prime_rx);
        std::thread::sleep(SETTLE);

        Pair {
            start_tx,
            done_rx,
            bench_done_tx,
            thread,
        }
    }
}

// ── kameo ─────────────────────────────────────────────────────────────────

mod kameo_leg {
    use super::{Pair, SETTLE, wait_prime};

    use kameo::actor::{ActorRef, Spawn};
    use kameo::message::{Context, Message};
    use kameo::{Actor, mailbox};

    struct Tick;

    struct Sink {
        seen: u64,
        done: Option<kanal::AsyncSender<()>>,
        target: u64,
    }

    impl Actor for Sink {
        type Args = (kanal::AsyncSender<()>, u64);
        type Error = std::convert::Infallible;

        async fn on_start(
            (done, target): Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(Sink {
                seen: 0,
                done: Some(done),
                target,
            })
        }
    }

    impl Message<Tick> for Sink {
        type Reply = ();

        async fn handle(&mut self, _msg: Tick, _ctx: &mut Context<Self, Self::Reply>) {
            self.seen += 1;
            if self.seen == self.target {
                self.done
                    .take()
                    .expect("done handle")
                    .send(())
                    .await
                    .expect("done signal");
            }
        }
    }

    struct Prime;

    struct Producer {
        sink: ActorRef<Sink>,
        prime_ack: Option<kanal::AsyncSender<()>>,
        start: Option<kanal::AsyncReceiver<()>>,
        target: u64,
    }

    impl Actor for Producer {
        type Args = Self;
        type Error = std::convert::Infallible;

        async fn on_start(
            args: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(args)
        }
    }

    impl Message<Prime> for Producer {
        type Reply = ();

        async fn handle(&mut self, _msg: Prime, _ctx: &mut Context<Self, Self::Reply>) {
            self.prime_ack
                .take()
                .expect("prime ack handle")
                .send(())
                .await
                .expect("prime ack");
            // Park mid-handler until the timed window opens.
            self.start
                .take()
                .expect("start handle")
                .recv()
                .await
                .expect("start signal");
            // The spam: kameo's idiomatic tell.
            for _ in 0..self.target {
                self.sink
                    .tell(Tick)
                    .send()
                    .await
                    .expect("kameo tell delivered");
            }
        }
    }

    /// Builds the runtime + both actors on a fresh thread, primes the
    /// producer, settles, then parks. `bounded` picks the mailbox:
    /// `true` = the default bounded-64; `false` = native unbounded.
    pub fn start(bounded: bool, n: u64) -> Pair {
        let (start_tx, start_rx) = kanal::bounded::<()>(1);
        let (prime_tx, prime_rx) = kanal::bounded::<()>(1);
        let (done_tx, done_rx) = kanal::bounded::<()>(1);
        let (release_tx, bench_done_rx) = kanal::bounded_async::<()>(1);
        let bench_done_tx = release_tx.clone_sync();

        let thread = std::thread::Builder::new()
            .name("kameo-leg".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("kameo leg rt");
                rt.block_on(async move {
                    // Sink first, then the producer carrying its handle.
                    let sink_ref = if bounded {
                        <Sink as Spawn>::spawn((done_tx.clone().to_async(), n))
                    } else {
                        <Sink as Spawn>::spawn_with_mailbox(
                            (done_tx.clone().to_async(), n),
                            mailbox::unbounded(),
                        )
                    };

                    let producer_ref = <Producer as Spawn>::spawn(Producer {
                        sink: sink_ref,
                        prime_ack: Some(prime_tx.clone().to_async()),
                        start: Some(start_rx.clone().to_async()),
                        target: n,
                    });

                    // ONE real actor message: the producer's handler
                    // acks on the prime channel, then parks on start.
                    producer_ref.tell(Prime).send().await.expect("prime tell");

                    // The module thread parks here for the whole timed
                    // window — returning would drop the runtime and
                    // close the channels mid-measurement.
                    bench_done_rx.recv().await.expect("bench done");
                });
            })
            .expect("kameo leg thread");

        wait_prime(&prime_rx);
        std::thread::sleep(SETTLE);

        Pair {
            start_tx,
            done_rx,
            bench_done_tx,
            thread,
        }
    }
}

// ── ractor ────────────────────────────────────────────────────────────────

mod ractor_leg {
    use super::{Pair, SETTLE, wait_prime};

    use ractor::Actor;
    use ractor::ActorProcessingErr;
    use ractor::ActorRef;

    struct SinkActor;

    enum SinkMsg {
        Tick,
    }

    struct SinkState {
        seen: u64,
        done: Option<kanal::AsyncSender<()>>,
        target: u64,
    }

    // ractor's default-feature build uses native async trait methods
    // (`async-trait` is an opt-in feature): no attribute here, and no
    // manual `impl ractor::Message` (a blanket impl covers all types).
    impl Actor for SinkActor {
        type Msg = SinkMsg;
        type State = SinkState;
        type Arguments = (kanal::AsyncSender<()>, u64);

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            (done, target): Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(SinkState {
                seen: 0,
                done: Some(done),
                target,
            })
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            _msg: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            state.seen += 1;
            if state.seen == state.target {
                state
                    .done
                    .take()
                    .expect("done handle")
                    .send(())
                    .await
                    .expect("done signal");
            }
            Ok(())
        }
    }

    enum ProducerMsg {
        Prime(ActorRef<SinkMsg>),
    }

    struct ProducerActor;

    struct ProducerState {
        prime_ack: Option<kanal::AsyncSender<()>>,
        start: Option<kanal::AsyncReceiver<()>>,
        target: u64,
    }

    impl Actor for ProducerActor {
        type Msg = ProducerMsg;
        type State = ProducerState;
        type Arguments = (kanal::AsyncSender<()>, kanal::AsyncReceiver<()>, u64);

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            (ack, go, target): Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(ProducerState {
                prime_ack: Some(ack),
                start: Some(go),
                target,
            })
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            msg: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            let ProducerMsg::Prime(sink_ref) = msg;
            state
                .prime_ack
                .take()
                .expect("prime ack handle")
                .send(())
                .await
                .expect("prime ack");
            // Park mid-handler until the timed window opens.
            state
                .start
                .take()
                .expect("start handle")
                .recv()
                .await
                .expect("start signal");
            // The spam: ractor's idiomatic fire-and-forget.
            for _ in 0..state.target {
                sink_ref.cast(SinkMsg::Tick).expect("ractor cast delivered");
            }
            Ok(())
        }
    }

    /// Builds the runtime + both actors on a fresh thread, primes the
    /// producer, settles, then parks. ractor's core mailbox is
    /// unbounded only — `bounded` must be `false` (the parameter keeps
    /// the five legs' call sites identical).
    pub fn start(bounded: bool, n: u64) -> Pair {
        debug_assert!(!bounded, "ractor core has no bounded mailbox");
        let (start_tx, start_rx) = kanal::bounded::<()>(1);
        let (prime_tx, prime_rx) = kanal::bounded::<()>(1);
        let (done_tx, done_rx) = kanal::bounded::<()>(1);
        let (release_tx, bench_done_rx) = kanal::bounded_async::<()>(1);
        let bench_done_tx = release_tx.clone_sync();

        let thread = std::thread::Builder::new()
            .name("ractor-leg".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("ractor leg rt");
                rt.block_on(async move {
                    let (sink_ref, _sink_handle) =
                        Actor::spawn(None, SinkActor, (done_tx.clone().to_async(), n))
                            .await
                            .expect("ractor sink spawn");
                    let (_producer_ref, _producer_handle) = Actor::spawn(
                        None,
                        ProducerActor,
                        (prime_tx.clone().to_async(), start_rx.clone().to_async(), n),
                    )
                    .await
                    .expect("ractor producer spawn");

                    // ONE real actor message: the producer's handler
                    // acks on the prime channel, then parks on start.
                    _producer_ref
                        .cast(ProducerMsg::Prime(sink_ref))
                        .expect("prime cast");

                    // The module thread parks here for the whole timed
                    // window — returning would drop the runtime and
                    // close the channels mid-measurement.
                    bench_done_rx.recv().await.expect("bench done");
                });
            })
            .expect("ractor leg thread");

        wait_prime(&prime_rx);
        std::thread::sleep(SETTLE);

        Pair {
            start_tx,
            done_rx,
            bench_done_tx,
            thread,
        }
    }
}

// ── criterion wiring ──────────────────────────────────────────────────────

/// competitors/…: the five framework-mailbox shapes at each size.
fn competitors(c: &mut Criterion) {
    let mut group = c.benchmark_group("competitors");
    group.sample_size(20);
    for n in SIZES {
        group.throughput(criterion::Throughput::Elements(n));
        group.bench_function(BenchmarkId::new("trouper", n), |b| {
            b.iter_batched(
                || trouper_leg::start(true, n),
                |pair| run(pair, n),
                criterion::BatchSize::PerIteration,
            );
        });
        group.bench_function(BenchmarkId::new("trouper-unbounded", n), |b| {
            b.iter_batched(
                || trouper_leg::start(false, n),
                |pair| run(pair, n),
                criterion::BatchSize::PerIteration,
            );
        });
        group.bench_function(BenchmarkId::new("kameo", n), |b| {
            b.iter_batched(
                || kameo_leg::start(true, n),
                |pair| run(pair, n),
                criterion::BatchSize::PerIteration,
            );
        });
        group.bench_function(BenchmarkId::new("kameo-unbounded", n), |b| {
            b.iter_batched(
                || kameo_leg::start(false, n),
                |pair| run(pair, n),
                criterion::BatchSize::PerIteration,
            );
        });
        group.bench_function(BenchmarkId::new("ractor", n), |b| {
            b.iter_batched(
                || ractor_leg::start(false, n),
                |pair| run(pair, n),
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, competitors);
criterion_main!(benches);
