//! Stateless pools: senders keep addressing the public path; the kernel
//! picks a worker per the pool's algo. Includes a takeover (a live plain
//! actor is stop-drained and replaced by the pool) and a round-robin
//! distribution check.
//!
//! Run: `cargo run --example pools`

use actor_runtime::actor::{CommandHandler, EventSourcedActor};
use actor_runtime::prelude::*;
use actor_runtime::tap::FactKind;
use actor_runtime::types::DeadLetterReason;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::Level;

#[derive(Deserialize)]
struct Work {
    n: i64,
}

impl Schema for Work {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Work".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

#[derive(Deserialize)]
struct WorkDone {
    #[allow(dead_code)]
    n: i64,
}

impl Schema for WorkDone {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "WorkDone".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
struct Worker {
    total: i64,
}

impl EventSourcedActor for Worker {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<Work>()
            .emits::<WorkDone>()
            .kind(ActorKind::EventSourced)
    }

    fn restore(_args: &serde_json::Value) -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &Event) {
        self.total += event.payload["n"].as_i64().unwrap_or(0);
    }
}

impl CommandHandler<Work> for Worker {
    fn handle(&self, cmd: Work, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        vec![Event::new(WorkDone::schema_id(), json!({ "n": cmd.n }))]
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::ERROR)
        .init();
    let system = Arc::new(ActorSystem::new(SystemConfig::production()));
    system.register_schema::<Work>();
    system.register_schema::<WorkDone>();

    // -- 1. Takeover: a plain actor holds "api"; the pool claims it --------
    println!("== 1. takeover ==");
    actor_runtime::builder::spawn_es_builder::<Worker>(&system)
        .at(ActorPath::new("api"))
        .args(json!({}))
        .handles::<Work>()
        .emits::<WorkDone>()
        .start();
    system
        .send(system.envelope(Work::schema_id(), ActorPath::new("api"), json!({ "n": 1 })))
        .await
        .expect("delivered to the plain actor");
    wait(|| async {
        system
            .inbox_cursor(&ActorPath::new("api"))
            .map(|c| c.as_u64())
            == Some(1)
    })
    .await;
    println!("   plain actor served one request at 'api'");

    system
        .install_pool(actor_runtime::pool::PoolSpec {
            public: ActorPath::new("api"),
            workers: 3,
            algo: actor_runtime::pool::PoolAlgo::RoundRobin,
            factory: Arc::new(|system, path, args| {
                actor_runtime::builder::spawn_es_builder::<Worker>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .handles::<Work>()
                    .emits::<WorkDone>()
                    .start();
            }),
            args: Some(json!({})),
            parent: Some(ActorPath::new("api-parent")),
            seed: 42,
        })
        .await
        .expect("takeover");
    println!("   pool took over 'api': 3 workers (children of api-parent)");

    // Senders did NOT change — same public path, different receivers.
    println!("== 2. round-robin distribution ==");
    for i in 0..6 {
        system
            .send(system.envelope(Work::schema_id(), ActorPath::new("api"), json!({ "n": i })))
            .await
            .expect("delivered to a worker");
    }
    wait(|| async {
        system
            .tap_facts()
            .iter()
            .filter(|f| {
                matches!(&f.kind,
                    FactKind::Delivered { to, .. }
                    if to.as_str().starts_with("api/worker-"))
            })
            .count()
            >= 6
    })
    .await;
    for worker in ["api/worker-0", "api/worker-1", "api/worker-2"] {
        let served = system
            .tap_facts()
            .iter()
            .filter(|f| matches!(&f.kind, FactKind::Delivered { to, .. } if to.as_str() == worker))
            .count();
        println!("   {worker} served {served} of 6 (round-robin rotation)");
    }

    // -- 3. Escalation parent + export --------------------------------------
    println!("== 3. declared topology ==");
    let export = system.export().await;
    for pool in &export.pools {
        println!(
            "   pool {} algo={} workers={:?} parent={:?}",
            pool.path, pool.algo, pool.workers, pool.spec_parent
        );
    }
    let dls = system.dead_letter_count().await;
    if dls > 0 {
        println!(
            "   takeover stop-drain flushed undelivered mail to the DLQ ({dls}); reasons: {:?}",
            system
                .tap_facts()
                .iter()
                .filter(|f| matches!(
                    &f.kind,
                    FactKind::DeadLettered {
                        reason: DeadLetterReason::InboxRefused,
                        ..
                    }
                ))
                .count()
        );
    }
    println!("pools example complete");
}

/// Polls `cond` until true (2s budget) — demo pacing helper.
async fn wait<F, Fut>(cond: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..1_000 {
        if cond().await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    panic!("demo condition never became true");
}
