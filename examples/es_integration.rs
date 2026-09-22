//! ES in integration: the pending-request idiom, timers by ticker, and
//! facts as the ONLY channel out of an entity.
//!
//! The scenario: a `TransferService` (plain service actor) orchestrates
//! transfers between event-sourced `Account` entities. The service CANNOT
//! ask an entity — entities do not answer asks (refuse-to-lie) — so the
//! service records a pending entry and LISTENS FOR THE FACT: each
//! `Account` entity handles a command and ANNOUNCES by returning a
//! `TransferCompleted`/`TransferRejected` fact, which the kernel
//! broadcasts to every actor that declared `.handles` on it.
//!
//! Timers are a pattern, not a subsystem: a `Ticker` actor publishes a
//! `Tick` every 20ms (a tokio loop telling itself), and the transfer
//! service sweeps its pending table on every tick — any entry older than
//! 100ms times out. Cancellation races (a completion racing the sweep) are
//! resolved idempotently: both sides check the pending table under the
//! actor's single-threaded dispatch.
//!
//! Run: `cargo run --example es_integration`

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::Level;
use trouper::actor::{CommandHandler, EventSourcedActor, MsgHandler, ServiceActor};
use trouper::context::{CmdCtx, MsgCtx};
use trouper::prelude::*;

use trouper::tap::FactKind;

// ---- messages ------------------------------------------------------------

/// Transfer command to the service (its one receive declaration).
#[derive(Command, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "Move amount between two accounts.")]
struct TransferCmd {
    transfer_id: String,
    from: String,
    to: String,
    amount: i64,
}

/// Debit command addressed to an account entity (the `account` field is
/// the shard key: `accts/<account>`).
#[derive(Command, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "Adjust one account's balance.")]
struct AccountCmd {
    #[schema(shard_key)]
    account: String,
    delta: i64,
}

/// The completion fact an account returns after applying a debit.
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "A debit settled.")]
struct TransferCompleted {
    transfer_id: String,
    account: String,
    delta: i64,
    balance: i64,
}

/// The rejection fact: a debit that would overdraw the account.
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "A debit bounced.")]
struct TransferRejected {
    transfer_id: String,
    account: String,
    delta: i64,
    balance: i64,
}

/// The ticker's beat (published — news, not a work order).
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "The scheduler's heartbeat.")]
struct Tick {
    n: u64,
}

// ---- the Account entity ---------------------------------------------------

/// The event-sourced entity: a balance fold. Its decision function is
/// PURE — it returns facts, it cannot send, publish, reply, or stop
/// itself. Announcing IS returning.
#[derive(Serialize, Deserialize, Default)]
struct Account {
    balance: i64,
}

impl EventSourcedActor for Account {
    fn restore(args: &Json) -> Self {
        Account {
            balance: args["opening"].as_i64().unwrap_or(0),
        }
    }

    fn apply(&mut self, event: &Event) {
        if let Some(delta) = event.payload_json()["delta"].as_i64() {
            self.balance += delta;
        }
    }
}

impl CommandHandler<AccountCmd> for Account {
    fn handle(&self, cmd: AccountCmd, ctx: &mut CmdCtx<'_>) -> Events {
        let transfer_id = "n/a".to_owned(); // plain debits carry none
        let after = self.balance + cmd.delta;
        // THE DECISION IS THE ANNOUNCEMENT: return the fact, the kernel
        // broadcasts it to every .handles observer.
        let mut ev = Events::new();
        if after < 0 {
            ev.push_event(TransferRejected {
                transfer_id: transfer_id.clone(),
                account: cmd.account.clone(),
                delta: cmd.delta,
                balance: self.balance,
            });
        } else {
            ev.push_event(TransferCompleted {
                transfer_id,
                account: cmd.account.clone(),
                delta: cmd.delta,
                balance: after,
            });
        }
        let _ = ctx; // introspection only: lookup/handlers_of/recv_ts/self_path
        ev
    }
}

/// The account schema WITH the transfer id: the service sends this so
/// completions can be correlated back to the pending transfer.
#[derive(Command, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "A transfer debit against one account.")]
struct TransferDebit {
    #[schema(shard_key)]
    account: String,
    transfer_id: String,
    delta: i64,
}

impl CommandHandler<TransferDebit> for Account {
    fn handle(&self, cmd: TransferDebit, _ctx: &mut CmdCtx<'_>) -> Events {
        let after = self.balance + cmd.delta;
        // THE SCHEMA ID CARRIES THE VERDICT: a rejection is a
        // TransferRejected fact, a settlement a TransferCompleted one.
        let mut ev = Events::new();
        if after < 0 {
            ev.push_event(TransferRejected {
                transfer_id: cmd.transfer_id.clone(),
                account: cmd.account.clone(),
                delta: cmd.delta,
                balance: self.balance,
            });
        } else {
            ev.push_event(TransferCompleted {
                transfer_id: cmd.transfer_id.clone(),
                account: cmd.account.clone(),
                delta: cmd.delta,
                balance: after,
            });
        }
        ev
    }
}

// ---- the Ticker (a timer by pattern) --------------------------------------

/// Publishes a `Tick` every 20ms. The tokio loop tells the actor; the
/// actor publishes (declared). A cron would be the same shape with a
/// "which jobs are due" table.
#[derive(Serialize, Deserialize, Default)]
struct Ticker {
    count: u64,
}

impl ServiceActor for Ticker {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<TickBeat>()
            .emits::<Tick>()
            .kind(ActorKind::Service)
    }
    async fn start(
        _args: &Json,
    ) -> Result<Self, error_stack::Report<trouper::registry::RegistryError>> {
        Ok(Self::default())
    }
}

/// Internal beat: the tokio loop tells this to the ticker.
#[derive(Command, Debug, Clone, Serialize, Deserialize)]
#[schema(description = "Drive one ticker beat.")]
struct TickBeat;

impl MsgHandler<TickBeat> for Ticker {
    async fn handle(&mut self, _msg: TickBeat, ctx: &mut MsgCtx<'_>) {
        self.count += 1;
        ctx.publish(&Tick { n: self.count });
    }
}

// ---- the TransferService (the orchestrator) --------------------------------

/// One in-flight transfer.
#[derive(Serialize, Deserialize)]
struct Pending {
    debit_account: String,
    credit_account: String,
    amount: i64,
    stage: Stage,
    sent_ms: u64,
}

/// Which half of the transfer is outstanding.
#[derive(Serialize, Deserialize)]
enum Stage {
    /// The debit fact has not arrived yet.
    AwaitingDebit,
    /// Debit settled; the credit is in flight.
    AwaitingCredit,
}

/// The orchestrator: a plain service actor. It listens for completion
/// facts via `.handles` (an entity's only output channel) and sweeps
/// stale pendings on ticks (a timer by pattern).
#[derive(Serialize, Deserialize, Default)]
struct TransferService {
    pending: HashMap<String, Pending>,
}

impl ServiceActor for TransferService {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<TransferCmd>()
            .handles::<TransferCompleted>()
            .handles::<TransferRejected>()
            .handles::<Tick>()
            .emits::<TransferCompleted>() // the service RE-publishes settled transfers
            .kind(ActorKind::Service)
    }
    async fn start(
        _args: &Json,
    ) -> Result<Self, error_stack::Report<trouper::registry::RegistryError>> {
        Ok(Self::default())
    }
}

impl TransferService {
    fn sweep_stale(&mut self, now_ms: u64, ctx: &mut MsgCtx<'_>) {
        let stale: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, p)| now_ms.saturating_sub(p.sent_ms) > 100)
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            if let Some(p) = self.pending.remove(&id) {
                println!(
                    "  [{now_ms}] transfer {id} TIMED OUT ({} {} -> {} {} never settled)",
                    p.amount, p.debit_account, p.amount, p.credit_account
                );
                // Drop the record; a real system would notify or retry.
                let _ = ctx;
            }
        }
    }
}

impl MsgHandler<TransferCmd> for TransferService {
    async fn handle(&mut self, cmd: TransferCmd, ctx: &mut MsgCtx<'_>) {
        let now_ms = ctx.recv_ts().as_millis();
        println!("  [{now_ms}] transfer {} requested", cmd.transfer_id);
        self.pending.insert(
            cmd.transfer_id.clone(),
            Pending {
                debit_account: cmd.from.clone(),
                credit_account: cmd.to.clone(),
                amount: cmd.amount,
                stage: Stage::AwaitingDebit,
                sent_ms: now_ms,
            },
        );
        // Tell the debit half to the SET PATH (partition activation):
        // the shard key in the payload derives `accts/<from>`. NO ASK:
        // entities do not answer asks; the service listens for the
        // completion fact instead. A FROZEN counterparty's set was never
        // installed: the send dead-letters, no fact ever comes back, and
        // the tick sweep reclaims the pending.
        let set = if cmd.from == "frozen" {
            "frozen-accts"
        } else {
            "accts"
        };
        ctx.send(
            Address::Path(ActorPath::new(set)),
            &TransferDebit {
                account: cmd.from.clone(),
                transfer_id: cmd.transfer_id.clone(),
                delta: -cmd.amount,
            },
            None,
        );
    }
}

impl MsgHandler<TransferCompleted> for TransferService {
    async fn handle(&mut self, fact: TransferCompleted, ctx: &mut MsgCtx<'_>) {
        let Some(pending) = self.pending.get_mut(&fact.transfer_id) else {
            return; // not ours (or already swept): idempotent no-op
        };
        match &pending.stage {
            Stage::AwaitingDebit => {
                // The debit settled: launch the credit half.
                let credit = pending.credit_account.clone();
                pending.stage = Stage::AwaitingCredit;
                let transfer_id = fact.transfer_id.clone();
                println!(
                    "  [{}] debit settled on {} (balance {}); sending credit",
                    ctx.recv_ts().as_millis(),
                    fact.account,
                    fact.balance
                );
                ctx.send(
                    Address::Path(ActorPath::new("accts")),
                    &TransferDebit {
                        account: credit,
                        transfer_id,
                        delta: pending.amount,
                    },
                    None,
                );
            }
            Stage::AwaitingCredit => {
                // Both halves settled: complete the transfer.
                let pending = self.pending.remove(&fact.transfer_id).expect("checked");
                println!(
                    "  [{}] transfer {} COMPLETED ({} {} -> {} {})",
                    ctx.recv_ts().as_millis(),
                    fact.transfer_id,
                    pending.amount,
                    pending.debit_account,
                    pending.amount,
                    pending.credit_account
                );
                ctx.publish(&fact);
            }
        }
    }
}

impl MsgHandler<TransferRejected> for TransferService {
    async fn handle(&mut self, fact: TransferRejected, ctx: &mut MsgCtx<'_>) {
        if self.pending.remove(&fact.transfer_id).is_some() {
            println!(
                "  [{}] transfer {} REJECTED by {} (balance {}, needed {})",
                ctx.recv_ts().as_millis(),
                fact.transfer_id,
                fact.account,
                fact.balance,
                -fact.delta
            );
            let _ = ctx;
        }
    }
}

impl MsgHandler<Tick> for TransferService {
    async fn handle(&mut self, _tick: Tick, ctx: &mut MsgCtx<'_>) {
        self.sweep_stale(ctx.recv_ts().as_millis(), ctx);
    }
}

// ---- demo ------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::ERROR)
        .init();
    let system = ActorSystem::new(SystemConfig::production());

    // The partition spec validates against the schema table AT INSTALL —
    // but the Account builder registers its command schemas only when the
    // factory spawns the first entity. Declare the shard key up front.
    system.register_schema::<AccountCmd>();
    system.register_schema::<TransferDebit>();

    // The account partition set: senders address `accts/<key>` via the
    // public path; entities activate on demand, announce by fact.
    system
        .install_partition_set(trouper::pool::PartitionSpec {
            public: ActorPath::new("accts"),
            system: system.clone(),
            factory: Arc::new(|system, path, args| {
                trouper::builder::spawn_es_builder::<Account>(system)
                    .at(path.clone())
                    .args(args.clone())
                    .handles::<AccountCmd>()
                    .handles::<TransferDebit>()
                    .emits::<TransferCompleted>()
                    .emits::<TransferRejected>()
                    .start();
            }),
            key_field: "account".to_owned(),
            args_template: Some(trouper::json!({ "opening": 100 })),
            opts: SpawnOpts::default(),
        })
        .expect("partition set installs");

    // The orchestrator + the ticker (the tokio loop drives the beat).
    trouper::builder::spawn_service_builder::<TransferService>(&system)
        .at(ActorPath::new("transfers"))
        .args(json!({}))
        .handles::<TransferCmd>()
        .handles::<TransferCompleted>()
        .handles::<TransferRejected>()
        .handles::<Tick>()
        .emits::<TransferDebit>() // the debit half it tells
        .emits::<TransferCompleted>() // the settled transfers it re-publishes
        .start();
    trouper::builder::spawn_service_builder::<Ticker>(&system)
        .at(ActorPath::new("ticker"))
        .args(json!({}))
        .handles::<TickBeat>()
        .emits::<Tick>()
        .start();
    {
        let system = system.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let _ = system.tell(ActorPath::new("ticker"), TickBeat).await;
            }
        });
    }

    println!("== ES in integration: facts out, pendings in, ticks as timers ==\n");

    // 1. A happy transfer: alice (100) sends 30 to bob (100).
    system
        .tell(
            ActorPath::new("transfers"),
            TransferCmd {
                transfer_id: "t1".into(),
                from: "alice".into(),
                to: "bob".into(),
                amount: 30,
            },
        )
        .await
        .expect("delivered");
    wait(|| async {
        system.tap_facts().iter().any(|f| {
            matches!(&f.kind,
                FactKind::Delivered { schema, .. }
                    if *schema == TransferCompleted::schema_id())
        })
    })
    .await;
    println!("  -> t1 published by the service\n");

    // 2. A doomed transfer: carol (100) sends 500 to dave (100) — the
    //    debit bounces, the service learns by REJECTION FACT.
    system
        .tell(
            ActorPath::new("transfers"),
            TransferCmd {
                transfer_id: "t2".into(),
                from: "carol".into(),
                to: "dave".into(),
                amount: 500,
            },
        )
        .await
        .expect("delivered");
    wait(|| async {
        system.tap_facts().iter().any(|f| {
            matches!(&f.kind,
                FactKind::Delivered { schema, .. }
                    if *schema == TransferRejected::schema_id())
        })
    })
    .await;
    println!("  -> t2 rejected (overdraft)\n");

    // 3. A transfer against a FROZEN account: its set was never
    //    installed, so the debit dead-letters (Undeliverable, envelope
    //    retained in the DLQ) — no fact ever returns, and the tick sweep
    //    times the pending out (100ms window).
    system
        .tell(
            ActorPath::new("transfers"),
            TransferCmd {
                transfer_id: "t3".into(),
                from: "frozen".into(),
                to: "alice".into(),
                amount: 5,
            },
        )
        .await
        .expect("delivered");
    println!("  waiting for the tick sweep to time t3 out...\n");
    // Give the tick sweep a few beats (100ms timeout window + margin).
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The observation surface: the tap ring (facts about everything) and
    // the DLQ (frank's debit died there, envelope retained for resend).
    let dlq = system.dead_letter_count().await;
    let dropped = system
        .dead_letter_reasons()
        .await
        .iter()
        .filter(|r| r.starts_with("Undeliverable") || r.starts_with("Unresolvable"))
        .count();
    println!(
        "final: tap facts = {}, dlq = {} ({} 'never settles' drops)",
        system.tap_facts().len(),
        dlq,
        dropped
    );
    let drained = system.drain_dead_letters();
    for letter in &drained {
        println!(
            "  drained: {} dest={:?} reason={:?} (envelope retained — inspect, then resend deliberately)",
            letter.schema, letter.dest, letter.reason
        );
    }
    if !drained.is_empty() {
        println!("  (t3's debit sits here: inspect, then resend deliberately)");
    }

    println!(
        "\nEntities announced by RETURNING facts; the service never asked.\nTimers were a ticker + a sweep; cancels resolved idempotently."
    );
}

/// Polls a condition for up to 2s (demo pacing).
async fn wait<F, Fut>(mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..1_000 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("condition never became true");
}
