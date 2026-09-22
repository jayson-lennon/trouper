//! The idle-burn probe: the deliverable this task exists for, measured.
//!
//! Spawns 5,000 idle ES actors with BOTH duties armed (passivation + time
//! snapshot, hours away so nothing fires), then measures process CPU over
//! a two-second window. The pre-change runtime polled each of those
//! actors 50×/s (the 20ms poll arm); this runtime wakes an actor only at
//! its due deadline — so with duties hours away the expected burn is
//! ~zero, under the fake-clock-free release build.
//!
//! Run: `cargo run --release --example idle_burn`

use serde::{Deserialize, Serialize};
use serde_json::json;
use trouper::actor::{EventSourcedActor, SnapshotCadence};
use trouper::prelude::Event;
use trouper::prelude::*;

#[derive(Deserialize, Serialize, Default, Clone)]
struct Idle {
    n: i64,
}
impl EventSourcedActor for Idle {
    fn manifest() -> trouper::schema::ActorManifest {
        trouper::schema::ActorManifest::new().kind(trouper::actor::ActorKind::EventSourced)
    }
    fn restore(_args: &Json) -> Self {
        Self::default()
    }
    fn apply(&mut self, _event: &Event) {}
}

/// utime + stime from /proc/self/stat, in clock ticks (CLK_TCK = 100).
fn cpu_ticks() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("stat");
    let fields: Vec<u64> = stat
        .rsplit_once(')')
        .expect("comm")
        .1
        .split_whitespace()
        .filter_map(|f| f.parse().ok())
        .collect();
    fields[11] + fields[12] // utime (14) + stime (15), 1-indexed after comm
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let system = ActorSystem::new(SystemConfig::production());
    for i in 0..5_000u32 {
        let path = ActorPath::new(format!("idle/{i}"));
        trouper::builder::spawn_es_builder::<Idle>(&system)
            .at(path)
            .args(json!({}))
            .passivate_after(std::time::Duration::from_secs(3_600))
            .snapshot(SnapshotCadence::Time(std::time::Duration::from_secs(3_600)))
            .start();
    }
    // Let the spawn burst (its own CPU) settle before the window opens.
    tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;

    // LIVENESS: the spawn burst left every loop + door task live; count
    // what the runtime owns so a fleet that silently died can't fake a
    // zero-burn result.
    {
        let live = tokio::runtime::Handle::try_current()
            .map(|h| h.metrics().num_alive_tasks())
            .unwrap_or(0);
        println!("  live tasks  : {live}");
    }
    let t0 = cpu_ticks();
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    let t1 = cpu_ticks();

    let cpu_seconds = (t1 - t0) as f64 / 100.0;
    println!("idle_burn: 5000 duty-armed idle actors, 10s wall window");
    println!("  cpu_seconds : {cpu_seconds:.3}");
    println!(
        "  per-actor   : {:.2} ms cpu/s ({:.0} wake-equivalents/s at 20ms poll)",
        cpu_seconds / 5_000.0 * 1_000.0,
        cpu_seconds / 5_000.0 * 50.0
    );
}
