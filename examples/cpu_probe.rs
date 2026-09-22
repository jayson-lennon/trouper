//! CPU probe: how much PROCESSOR time does the runtime burn to commit a
//! fixed load? 2_000 `Add` commands at one journaled counter entity,
//! with the wall clock and the process CPU clock (utime+stime from
//! /proc/self/stat) read just before and just after the send window.
//! The utilization ratio is what a deployment should expect under this
//! load shape — a multi-thread runtime may exceed 1.0 (more than one
//! core burning), and a number far below 1.0 means the run was mostly
//! waiting, not working.
//!
//! Run: `cargo run --release --example cpu_probe`

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::json;
use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::prelude::*;

/// Fixed load: every probe run commits exactly this many commands.
const MESSAGES: u64 = 2_000;

/// The counter folds the SUM of the command payloads (each `Add` carries
/// its sequence number), so "all N committed" is the exact arithmetic
/// sum 0+1+...+(N-1) — an all-or-nothing completion check.
const EXPECTED_TOTAL: i64 = ((MESSAGES * (MESSAGES - 1)) / 2) as i64;

/// Clock ticks per second (Linux `CLK_TCK`). libc is not in this
/// crate's dependency tree, so the Linux userspace default of 100 is
/// hardcoded — every stock kernel runs userspace at 100 Hz.
const CLK_TCK: f64 = 100.0;

// ---- A journaled counter (the entity under load) ------------------------

#[derive(Command, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "Add to the counter.")]
struct Add {
    n: i64,
}

#[derive(Event, Debug, Serialize, Deserialize)]
struct Added {
    n: i64,
}

#[derive(Default, Debug, Serialize, Deserialize)]
struct Counter {
    total: i64,
}
impl EventSourcedActor for Counter {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::EventSourced)
    }
    fn restore(_args: &Json) -> Self {
        Self::default()
    }
    fn apply(&mut self, event: &Event) {
        if event.schema.as_str() == "Added" {
            self.total += event.payload_json()["n"].as_i64().unwrap_or(0);
        }
    }
}
impl CommandHandler<Add> for Counter {
    fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> Events {
        Events::one(Added { n: cmd.n })
    }
}

/// Process CPU seconds: utime (field 14) + stime (field 15) of
/// /proc/self/stat, in CLK_TCK ticks. Linux only, plain std. The comm
/// field can contain spaces, so parsing starts after the LAST `)`;
/// token 0 of the remainder is field 3 (state), putting utime at token
/// 11 and stime at token 12.
fn cpu_seconds() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("linux /proc/self/stat");
    let after_comm = stat.rsplit_once(')').expect("stat shape").1;
    let mut fields = after_comm.split_whitespace();
    let utime: u64 = fields
        .clone()
        .nth(11)
        .expect("utime")
        .parse()
        .expect("utime ticks");
    let stime: u64 = fields.nth(12).expect("stime").parse().expect("stime ticks");
    (utime + stime) as f64 / CLK_TCK
}

#[tokio::main]
async fn main() {
    // The production shape (multi-thread runtime), as in benches/e2e.rs:
    // the probe wants the deployment's worker count, not a test clock.
    let system = ActorSystem::new(SystemConfig::production());
    system.register_schema::<Add>();
    system.register_schema::<Added>();

    let counter = trouper::builder::spawn_es_builder::<Counter>(&system)
        .at(ActorPath::new("probe/counter"))
        .args(json!({}))
        .handles::<Add>()
        .emits::<Added>()
        .start();

    let cpu_before = cpu_seconds();
    let wall = Instant::now();

    for n in 0..MESSAGES as i64 {
        system
            .tell(counter.clone(), Add { n })
            .await
            .expect("delivered");
    }
    // Commit completion is the typed fold reaching the exact expected
    // sum (public API; benches/e2e.rs settles the same way). There is
    // no public "await commit" handle, so this is a 1ms poll, bounded.
    wait_total(&system, &counter, EXPECTED_TOTAL).await;

    let wall_seconds = wall.elapsed().as_secs_f64();
    let cpu_seconds = cpu_seconds() - cpu_before;

    println!("== cpu_probe: {MESSAGES} Add commands at one ES counter ==");
    println!("messages       : {MESSAGES}");
    println!("wall_seconds   : {wall_seconds:.4}");
    println!("cpu_seconds    : {cpu_seconds:.4}");
    println!(
        "cpu_utilization: {:.2} (cpu/wall; >1.0 = multi-core burn)",
        cpu_seconds / wall_seconds
    );
}

/// Polls until the counter's fold reaches `total` (bounded; panics on
/// stall so the probe fails loudly instead of reporting a bogus window).
async fn wait_total(system: &ActorSystem, path: &ActorPath, total: i64) {
    for _ in 0..30_000 {
        if system.with_es_state::<Counter, _>(path, |c| c.total).await == Some(total) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("counter never folded to total {total}");
}
