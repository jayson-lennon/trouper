//! Full-completion ES fan-out profiler for 0, 1, or 4 plain subscribers.
//!
//! The measured window includes both producing and consuming work: it opens
//! after warmup completion and closes only after the ES actor and every
//! subscriber have committed all measured messages. Use one subscriber count
//! per process so repeated `perf record` invocations do not share a runtime.
//!
//! ```text
//! RUSTFLAGS="-C force-frame-pointers=yes" \
//!   cargo build --profile release-debug --example profile_es_fanout
//! SUBSCRIBERS=4 MESSAGES=5000000 taskset -c 0-5 perf record -F 9999 \
//!   --call-graph fp -o /tmp/es-fanout-4.data -- \
//!   target/release-debug/examples/profile_es_fanout
//! perf script -i /tmp/es-fanout-4.data | stackcollapse-perf.pl \
//!   > bench-accounting-es-fanout-4.folded
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use trouper::actor::{
    ActorKind, ActorPath, CommandHandler, EventSourcedActor, MsgHandler, ServiceActor,
};
use trouper::context::{CmdCtx, MsgCtx};
use trouper::envelope::{Event as JournaledEvent, Events};
use trouper::json::Json;
use trouper::registry::RegistryError;
use trouper::schema::{ActorManifest, Command, Schema};
use trouper::system::{ActorSystem, SpawnOpts, SystemConfig};

const SETTLE: Duration = Duration::from_millis(100);
const DEFAULT_MESSAGES: u64 = 2_000_000;
const DEFAULT_WARMUP: u64 = 64_000;

#[derive(Debug, Clone, Serialize, Deserialize, Command)]
struct Tick {
    n: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, trouper::schema::Event)]
struct Ticked {
    n: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Accum {
    total: i64,
}

impl EventSourcedActor for Accum {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<Tick>()
            .emits::<Ticked>()
            .kind(ActorKind::EventSourced)
    }

    fn apply(&mut self, event: &JournaledEvent) {
        if event.schema == Ticked::schema_id() {
            self.total += 1;
        }
    }
}

impl CommandHandler<Tick> for Accum {
    fn handle(&self, cmd: Tick, _ctx: &mut CmdCtx<'_>) -> Events {
        Events::one(Ticked { n: cmd.n })
    }
}

struct Subscriber;

impl ServiceActor for Subscriber {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<Ticked>()
            .kind(ActorKind::Service)
    }

    async fn start(_args: &Json) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<Ticked> for Subscriber {
    async fn handle(&mut self, _event: &Ticked, _ctx: &mut MsgCtx<'_>) {}
}

fn env_count(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn subscriber_paths(count: usize) -> Vec<ActorPath> {
    (0..count)
        .map(|index| ActorPath::new(format!("profile/es-fanout/subscriber-{index}")))
        .collect()
}

fn spawn_subscribers(system: &ActorSystem, paths: &[ActorPath]) {
    for path in paths {
        system.spawn_service::<Subscriber, _>(
            path.clone(),
            &Json::default(),
            SpawnOpts::default(),
            || {
                vec![Arc::new(trouper::actor::TypedServiceAdapter::<
                    Subscriber,
                    Ticked,
                >::new::<Ticked>())]
            },
        );
    }
}

async fn wait_for_cursor(system: &ActorSystem, path: &ActorPath, target: u64) {
    while system.inbox_cursor(path).map(|cursor| cursor.as_u64()) != Some(target) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn wait_for_completion(system: &ActorSystem, paths: &[&ActorPath], target: u64) {
    for path in paths {
        wait_for_cursor(system, path, target).await;
    }
}

async fn run_workload(system: &ActorSystem, emitter: &ActorPath, messages: u64) {
    for n in 0..messages as i64 {
        let _ = system.tell(emitter.clone(), Tick { n }).await;
    }
}

async fn profile(
    system: &ActorSystem,
    emitter: &ActorPath,
    subscribers: &[ActorPath],
    messages: u64,
    warmup: u64,
) {
    let subscriber_refs = subscribers.iter().collect::<Vec<_>>();
    let mut watched = vec![emitter];
    watched.extend(subscriber_refs.iter().copied());

    run_workload(system, emitter, warmup).await;
    wait_for_completion(system, &watched, warmup).await;
    tokio::time::sleep(SETTLE).await;

    let target = warmup + messages;
    let started = Instant::now();
    run_workload(system, emitter, messages).await;
    wait_for_completion(system, &watched, target).await;
    let elapsed = started.elapsed();

    let rate = messages as f64 / elapsed.as_secs_f64();
    let ns_per_command = elapsed.as_nanos() as f64 / messages as f64;
    println!(
        "subscribers={} messages={messages} elapsed={elapsed:.3?} rate={rate:.0} cmd/s ns/cmd={ns_per_command:.0}",
        subscribers.len(),
    );
}

#[tokio::main]
async fn main() {
    let subscriber_count = env_count("SUBSCRIBERS", 1);
    if !matches!(subscriber_count, 0 | 1 | 4) {
        eprintln!("SUBSCRIBERS must be 0, 1, or 4");
        std::process::exit(2);
    }
    let messages = env_count("MESSAGES", DEFAULT_MESSAGES);
    let warmup = env_count("WARMUP", DEFAULT_WARMUP).min(messages);
    if messages == 0 {
        eprintln!("MESSAGES must be positive");
        std::process::exit(2);
    }

    let system = ActorSystem::new(SystemConfig::production());
    let emitter = ActorPath::new("profile/es-fanout/emitter");
    let subscribers = subscriber_paths(subscriber_count as usize);

    system.spawn_es::<Accum, _>(
        emitter.clone(),
        &Json::default(),
        SpawnOpts::default(),
        || {
            vec![Arc::new(
                trouper::actor::TypedEsAdapter::<Accum, Tick>::new::<Tick>(),
            )]
        },
    );
    spawn_subscribers(&system, &subscribers);

    while system.inbox_cursor(&emitter).is_none() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    wait_for_completion(&system, &[&emitter], 0).await;
    for subscriber in &subscribers {
        wait_for_cursor(&system, subscriber, 0).await;
    }

    profile(&system, &emitter, &subscribers, messages, warmup).await;
    let _ = system.shutdown_graceful(Duration::from_secs(2)).await;
}
