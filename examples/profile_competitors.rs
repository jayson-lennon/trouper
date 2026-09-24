//! Competitor-shape flamegraph harness: the EXACT trouper and ractor
//! legs from `benches/competitors.rs` (producer actor → sink actor, the
//! kanal prime/start/done protocol), driven once per process instead of
//! under criterion. The bench's ns/msg numbers (README: trouper bounded-64
//! ≈ 701 ns, ractor ≈ 228 ns at 50000) come from THIS shape — a raw edge
//! `tell` loop (examples/flame_tells.rs) cannot stand in for it.
//!
//! One runtime per invocation: `PROFILE_TROUPER=1` runs the trouper leg
//! (bounded-64, Block), `PROFILE_RACTOR=1` runs the ractor leg (unbounded,
//! cast). Running both in one process would put two multi-thread runtimes
//! on one core set and contaminate the sample accounting.
//!
//! ```text
//! RUSTFLAGS="-C force-frame-pointers=yes" \
//!   cargo build --profile release-debug --example profile_competitors
//! PROFILE_TROUPER=1 taskset -c 0-5 perf record -F 9999 --call-graph fp \
//!   -o /tmp/comp-trouper.data -- target/release-debug/examples/profile_competitors
//! perf script -i /tmp/comp-trouper.data | stackcollapse-perf.pl \
//!   > bench-accounting-trouper.folded
//! flamegraph --colors=green < bench-accounting-trouper.folded \
//!   > flame_competitors_trouper.svg
//! # likewise: PROFILE_RACTOR=1 → bench-accounting-ractor.folded (blue)
//! python3 tools/fold_accounting.py bench-accounting-trouper.folded \
//!   bench-accounting-ractor.folded --trouper-ns 701 --ractor-ns 228
//! ```

use std::time::{Duration, Instant};

use trouper::actor::{MsgHandler, ServiceActor};
use trouper::inbox::OverloadPolicy;
use trouper::prelude::*;
use trouper::registry::RegistryError;

/// Settle time after the prime ack: the runtime finishes spawn
/// bookkeeping so the timed window opens on a hot, quiet system.
/// (The bench value, verbatim.)
const SETTLE: Duration = Duration::from_millis(50);

/// The producers' spam target / the sinks' done target (the bench's `n`).
fn messages() -> u64 {
    std::env::var("MESSAGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000_000)
}

/// Sync poll on the bench thread for a leg's prime ack — the sync twin
/// of the async handle the producer holds. (The bench's `wait_prime`,
/// verbatim.)
fn wait_prime(prime_rx: &kanal::Receiver<()>) {
    loop {
        match prime_rx.try_recv() {
            Ok(Some(())) => return,
            _ => std::thread::sleep(Duration::from_millis(1)),
        }
    }
}

/// The timed body: the start signal, the done signal — byte-identical
/// timing shape to the bench's `run` (which additionally tears the leg
/// down; the profile run keeps nothing after).
///
/// Returns the elapsed time so the driver can print ns/msg.
fn timed_run(start_tx: &kanal::Sender<()>, done_rx: &kanal::Receiver<()>) -> Duration {
    let t0 = Instant::now();
    start_tx.send(()).expect("start signal");
    done_rx.recv().expect("done signal");
    t0.elapsed()
}

// ── trouper leg (benches/competitors.rs, verbatim) ────────────────────────

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
    async fn handle(&mut self, msg: &Prime, ctx: &mut MsgCtx<'_>) {
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
        let sink = msg.sink.clone();
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
    async fn handle(&mut self, _msg: &Tick, _ctx: &mut MsgCtx<'_>) {
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
/// producer (Prime → ack), settles, then parks until the run ends.
/// The bench leg's bounded variant: capacity 64, `OverloadPolicy::Block`.
fn trouper_leg(
    n: u64,
) -> (
    kanal::Sender<()>,
    kanal::Receiver<()>,
    kanal::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    let (start_tx, start_rx) = kanal::bounded::<()>(1);
    let (prime_tx, prime_rx) = kanal::bounded::<()>(1);
    let (done_tx, done_rx) = kanal::bounded::<()>(1);
    let (release_tx, bench_done_rx) = kanal::bounded_async::<()>(1);
    let bench_done_tx = release_tx.clone_sync();

    let capacity = 64;
    let policy = OverloadPolicy::Block;
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
                    .mailbox(capacity, policy)
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
                    .mailbox(capacity, policy)
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

    (start_tx, done_rx, bench_done_tx, thread)
}

// ── ractor leg (benches/competitors.rs, verbatim) ─────────────────────────

mod ractor_leg {
    use crate::{SETTLE, wait_prime};

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
    /// producer, settles, then parks until the run ends. ractor's core
    /// mailbox is unbounded only.
    pub fn start(
        n: u64,
    ) -> (
        kanal::Sender<()>,
        kanal::Receiver<()>,
        kanal::Sender<()>,
        std::thread::JoinHandle<()>,
    ) {
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

        (start_tx, done_rx, bench_done_tx, thread)
    }
}

// ── driver ────────────────────────────────────────────────────────────────

fn main() {
    let n = messages();
    let want_trouper = std::env::var("PROFILE_TROUPER").is_ok();
    let want_ractor = std::env::var("PROFILE_RACTOR").is_ok();
    if want_trouper == want_ractor {
        eprintln!(
            "set exactly one of PROFILE_TROUPER=1 / PROFILE_RACTOR=1 (one runtime per perf record)"
        );
        std::process::exit(2);
    }

    let (start_tx, done_rx, bench_done_tx, thread) = if want_trouper {
        trouper_leg(n)
    } else {
        ractor_leg::start(n)
    };

    let elapsed = timed_run(&start_tx, &done_rx);
    let rate = n as f64 / elapsed.as_secs_f64();
    let ns_per_msg = elapsed.as_nanos() as f64 / n as f64;
    println!("{n} msgs in {elapsed:.3?} → {rate:.0} msg/s ({ns_per_msg:.0} ns/msg)");

    // Release the parked module thread; it drops its runtime.
    bench_done_tx.send(()).expect("release thread");
    thread.join().expect("module thread");
}
