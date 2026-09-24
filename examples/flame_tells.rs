//! Flamegraph harness: sustained tells at steady state (no criterion).
//!
//! Mirrors `e2e fire_and_forget` (one warm entity, tell loop, no
//! completion wait) but the measured loop is the whole profile.
//!
//! ```text
//! RUSTFLAGS="-C force-frame-pointers=yes" \
//!   cargo build --profile release-debug --example flame_tells
//! perf record -F 9999 --call-graph fp -o /tmp/tells.data \
//!   -- target/release-debug/examples/flame_tells
//! perf script -i /tmp/tells.data | flamegraph --flamechart > tells.svg
//! PROFILE_SERVICE=1 runs the service-actor leg instead of ES.
//! ```

use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use trouper::actor::{ActorKind, ActorPath, EventSourcedActor, ServiceActor};
use trouper::context::{CmdCtx, MsgCtx};
use trouper::json::Json;
use trouper::schema::{ActorManifest, Command, Schema};
use trouper::system::{ActorSystem, SpawnOpts, SystemConfig};

#[derive(Debug, Clone, Serialize, Deserialize, Command)]
struct Tick {
    n: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, trouper::schema::Event)]
struct Ticked {
    n: i64,
}

#[derive(Serialize, Deserialize, Default)]
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
    fn apply(&mut self, event: &trouper::envelope::Event) {
        if event.schema.as_str() == "Ticked" {
            self.total += 1;
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

#[derive(Debug, Clone, Serialize, Deserialize, Command)]
struct Ping {
    n: i64,
}

struct Sink;

impl ServiceActor for Sink {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<Ping>()
            .kind(ActorKind::Service)
    }
    async fn start(
        _args: &Json,
    ) -> Result<Self, error_stack::Report<trouper::registry::RegistryError>> {
        Ok(Sink)
    }
}

impl trouper::actor::MsgHandler<Ping> for Sink {
    async fn handle(&mut self, _msg: &Ping, _ctx: &mut MsgCtx<'_>) {}
}

fn main() {
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("rt"),
    );
    let service_leg = std::env::var("PROFILE_SERVICE").is_ok();
    let measured: u64 = std::env::var("MESSAGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000_000);

    rt.block_on(async {
        let system = ActorSystem::new(SystemConfig::production());
        let path = ActorPath::new("flame/target");

        if service_leg {
            system.spawn_service::<Sink, _>(
                path.clone(),
                &Json::default(),
                SpawnOpts::default(),
                || {
                    vec![Arc::new(
                        trouper::actor::TypedServiceAdapter::<Sink, Ping>::new::<Ping>(),
                    )]
                },
            );
        } else {
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
        // Wait for the loop task to be live.
        for _ in 0..5_000 {
            let live = if service_leg {
                system.inbox_cursor(&path).is_some()
            } else {
                system
                    .with_es_state::<Accum, _>(&path, |a| a.total)
                    .await
                    .is_some()
            };
            if live {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(system.inbox_cursor(&path).is_some(), "spawn never settled");

        // Warmup so dispatch paths are hot before the measured window.
        let warm = 64_000.min(measured as i64);
        for n in 0..warm {
            let _ = system.tell(path.clone(), Tick { n }).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let t0 = Instant::now();
        if service_leg {
            for n in 0..measured as i64 {
                let _ = system.tell(path.clone(), Ping { n }).await;
            }
        } else {
            for n in 0..measured as i64 {
                let _ = system.tell(path.clone(), Tick { n }).await;
            }
        }
        let elapsed = t0.elapsed();
        let rate = measured as f64 / elapsed.as_secs_f64();
        println!(
            "{measured} tells in {elapsed:.3?} → {rate:.0} msg/s (leg: {})",
            if service_leg { "service" } else { "es" }
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let _ = system
            .shutdown_graceful(std::time::Duration::from_secs(2))
            .await;
    });
}
