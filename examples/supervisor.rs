//! Worker pools without a pool: a supervisor actor + `send_to_any`.
//!
//! The pool API is gone because it was thin sugar over things the actor
//! model already provides:
//! - **Lifecycle** — a supervisor owns its workers via [`trouper::supervision::ActorSpec`]
//!   (`system.spawn(spec)`); the engine restarts crashed workers within
//!   budget and escalates an `Escalated` control message to the declared
//!   parent when the budget exhausts. The supervisor is an ORDINARY
//!   actor whose handler owns the response policy (restart vs retire).
//! - **Distribution** — senders use `system.send_to_any::<M>()` /
//!   `ctx.send_to_any(&M)`; the route table rotates one copy per send
//!   across every worker that declared `.handles::<WorkJob>()`. No
//!   forwarding actor sits in the path.
//!
//! Notice `JobSupervisor` never touches a `WorkJob`: its declaration is
//! lifecycle-only. News and work flow through the same fabric; the
//! supervisor just answers for its children.
//!
//! Run: `cargo run --example supervisor`

use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use trouper::actor::{MsgHandler, ServiceActor};
use trouper::prelude::*;
use trouper::registry::RegistryError;
use trouper::supervision::{ActorSpec, Backoff, RestartBudget, RestartPolicy};

static LINES: std::sync::OnceLock<Arc<Mutex<Vec<String>>>> = std::sync::OnceLock::new();

fn log(line: impl Into<String>) {
    let lines = LINES.get_or_init(|| Arc::new(Mutex::new(Vec::new())));
    let line = line.into();
    println!("{line}");
    lines.lock().expect("lines lock").push(line);
}

/// The work unit: one copy goes to ONE worker per send_to_any.
#[derive(Debug, Serialize, Deserialize)]
struct WorkJob {
    id: u32,
}
impl Schema for WorkJob {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "WorkJob".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("id", FieldTy::Int)],
            description: Some("A unit of work for the worker pool.".into()),
        }
    }
}

/// A worker's completion announcement (published: news, not work).
#[derive(Debug, Serialize, Deserialize)]
struct JobDone {
    id: u32,
}
impl Schema for JobDone {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "JobDone".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("id", FieldTy::Int)],
            description: Some("A worker finished a job.".into()),
        }
    }
}

/// The escalation control message the engine SENDS to the declared
/// parent when a child's budget exhausts. It is an ordinary message —
/// the parent's handler owns the response.
#[derive(Debug, Deserialize)]
struct Escalated {
    escalated: String,
}
impl Schema for Escalated {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Escalated".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("escalated", FieldTy::Str)],
            description: Some("A supervised child exhausted its budget.".into()),
        }
    }
}

/// A worker: handles `WorkJob` and announces completion. Every worker
/// declares the SAME schema — that is what makes `send_to_any` rotate
/// across them.
struct Worker {
    id: &'static str,
}

impl Worker {
    fn new(id: u32) -> Self {
        // Box-leak keeps the handler `&'static str` for the demo's
        // lifetime; a real actor would own a String.
        Self {
            id: Box::leak(format!("worker-{id}").into_boxed_str()),
        }
    }
}

impl ServiceActor for Worker {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }

    async fn start(args: &serde_json::Value) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self::new(args["id"].as_u64().unwrap_or(0) as u32))
    }
}

impl MsgHandler<WorkJob> for Worker {
    async fn handle(&mut self, msg: WorkJob, ctx: &mut MsgCtx<'_>) {
        log(format!("[{}] finished job {}", self.id, msg.id));
        ctx.publish(&JobDone { id: msg.id });
    }
}

/// The lifecycle-only actor: spawns workers via `ActorSpec`, answers
/// escalations. It NEVER declares `WorkJob` — work bypasses it
/// entirely (send_to_any talks to the route table, not to a router
/// actor).
struct JobSupervisor;

impl ServiceActor for JobSupervisor {
    fn manifest() -> ActorManifest {
        ActorManifest::new().kind(ActorKind::Service)
    }

    async fn start(_args: &serde_json::Value) -> Result<Self, error_stack::Report<RegistryError>> {
        Ok(Self)
    }
}

impl MsgHandler<Escalated> for JobSupervisor {
    async fn handle(&mut self, msg: Escalated, _ctx: &mut MsgCtx<'_>) {
        // The policy point: budget exhausted ⇒ this demo RETIRES the
        // worker (a restart loop would re-enter the same budget). A
        // real system might page an operator, respawn at a new path,
        // or shed load — the actor decides, not the fabric.
        log(format!(
            "[supervisor] budget exhausted for {} — retiring it",
            msg.escalated
        ));
    }
}

#[tokio::main]
async fn main() {
    let system = ActorSystem::new(SystemConfig::production());

    // The supervisor first (it is the workers' declared parent).
    spawn_service_builder::<JobSupervisor>(&system)
        .at(ActorPath::new("job.supervisor"))
        .handles::<Escalated>()
        .start();

    // Three workers, each under its own ActorSpec with the supervisor
    // as the escalation target. Two of them will be healthy; the third
    // crashes every time to demonstrate the escalation path.
    for id in 0..3_u32 {
        let path = ActorPath::new(format!("job.worker-{id}"));
        let spec = ActorSpec {
            path: path.clone(),
            parent: Some(ActorPath::new("job.supervisor")),
            restart: RestartPolicy::Permanent,
            budget: RestartBudget::per(2, std::time::Duration::from_secs(10)),
            backoff: Backoff::default(),
            args: serde_json::json!({ "id": id }),
            spawn: Arc::new(
                move |sys: &ActorSystem, path: &ActorPath, args: &serde_json::Value| {
                    // The factory owns the actor type; the kernel owns the
                    // naming. Crashy workers (id 2) panic on every job.
                    if args["id"].as_u64() == Some(2) {
                        spawn_crashy(sys, path.clone(), args);
                    } else {
                        trouper::builder::spawn_service_builder::<Worker>(sys)
                            .at(path.clone())
                            .args(args.clone())
                            .handles::<WorkJob>()
                            .start();
                    }
                },
            ),
        };
        system.spawn(spec);
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Work flows via send_to_any: the route table rotates one copy per
    // send across the workers that declared WorkJob.
    for id in 0..6_u32 {
        let _ = system.send_to_any(&WorkJob { id }).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Now aim jobs at the crashy worker by NAME (tells bypass the
    // rotation): each crash consumes budget, the engine restarts it
    // twice, then escalates `Escalated` to the supervisor.
    for _ in 0..4_u32 {
        let _ = system
            .tell(ActorPath::new("job.worker-2"), WorkJob { id: 99 })
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    println!("--- pool report ---");
    let lines = LINES
        .get()
        .map(|l| l.lock().expect("lines lock").clone())
        .unwrap_or_default();
    let done: Vec<u32> = lines
        .iter()
        .filter(|l| l.starts_with("[worker-"))
        .filter_map(|l| l.rsplit(' ').next()?.parse().ok())
        .collect();
    let workers_used = lines
        .iter()
        .filter(|l| l.starts_with("[worker-"))
        .map(|l| l.split(']').next().unwrap_or(""))
        .collect::<std::collections::BTreeSet<_>>();
    let escalations = lines.iter().filter(|l| l.contains("retiring it")).count();
    println!("  jobs completed: {:?}", done);
    println!("  workers that processed jobs: {workers_used:?} (rotation, not a router actor)");
    println!("  escalations answered by the supervisor: {escalations}");
    assert_eq!(
        done.len(),
        4,
        "healthy workers finished their rotated jobs; the two that landed \
         on the crashy worker are LOST — a restart restores the worker, \
         not the work (at-most-once for in-flight jobs)"
    );
    assert!(
        workers_used.len() >= 2,
        "work spread across multiple workers"
    );
    assert_eq!(
        escalations, 1,
        "the crashy worker exhausted its budget exactly once and the \
         supervisor answered"
    );
    println!(
        "\nA pool is not a runtime feature: it is a supervisor owning lifecycles\n\
         (ActorSpec) plus senders choosing send_to_any. No forwarding actor exists."
    );
}

/// Spawns a worker that panics on its first job (the supervision
/// fixture): builder spawn with a locally-defined actor type.
fn spawn_crashy(sys: &ActorSystem, path: ActorPath, args: &serde_json::Value) {
    struct Crashy {
        _id: u32,
    }
    impl ServiceActor for Crashy {
        fn manifest() -> ActorManifest {
            ActorManifest::new().kind(ActorKind::Service)
        }
        async fn start(a: &serde_json::Value) -> Result<Self, error_stack::Report<RegistryError>> {
            Ok(Crashy {
                _id: a["id"].as_u64().unwrap_or(0) as u32,
            })
        }
    }
    impl MsgHandler<WorkJob> for Crashy {
        async fn handle(&mut self, _msg: WorkJob, _ctx: &mut MsgCtx<'_>) {
            panic!("crashy worker always dies");
        }
    }
    trouper::builder::spawn_service_builder::<Crashy>(sys)
        .at(path)
        .args(args.clone())
        .handles::<WorkJob>()
        .start();
}
