//! The delivery kernel: actor cells, front doors, routing, and the ES
//! atomic step.
//!
//! Identity model: the registry maps paths to slots; the system keeps the
//! per-actor cell (inbox + endpoint + handle). A restart swaps the endpoint
//! under the path — the inbox and its cursor persist, so senders holding
//! pre-crash handles never notice and undelivered messages redeliver.
//!
//! The atomic step (spec, exact order): peek → find entry → build ctx →
//! catch_unwind dispatch → journal.append → inbox.ack → apply → outbox
//! flush → emit fan-out → maybe snapshot. NO user code runs after ack;
//! steps after it are infallible kernel code, so the append+ack pair is
//! atomic in practice. A panicking handler leaves nothing appended, nothing
//! acked: the message stays queued for redelivery and the poisoned state is
//! never reused (restart rebuilds from the journal).
//!
//! Loop discipline (project skill): one loop per function; loop bodies are
//! named step functions.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwapOption;
use serde_json::Value as JsonValue;
use tokio::sync::{Notify, mpsc, watch};

use crate::actor::{CommandEntry, DynEsActor, DynServiceActor, MsgEntry};
use crate::context::{CmdCtx, Outbox, RuntimeView};
use crate::envelope::Event;
use crate::envelope::{Address, Envelope, TraceCtx};
use crate::inbox::Inbox;
use crate::journal::{Journal, JournalEntry, JournalError};
use crate::registry::{Endpoint, Registry};
use crate::types::{InboxOffset, Path, SchemaId, SeqNo};

/// How often an ES actor takes journal snapshots. Default: OFF.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnapshotPolicy {
    /// Never snapshot (replay is always full).
    #[default]
    Off,
    /// Snapshot every `n` events (taken BETWEEN messages, never mid-step).
    EveryN(u64),
}

/// An envelope the runtime could not deliver or decode.
///
/// Kept inspectable — dropped messages must stay observable, never silently
/// vanish. (Phase 6/7 redirect this onto the DLQ topic + tap facts.)
#[derive(Debug, Clone)]
pub struct DeadLetter {
    /// The undeliverable payload's schema.
    pub schema: SchemaId,
    /// Where it was headed.
    pub dest: Address,
    /// Why it died.
    pub reason: String,
    /// The trace of the hop that failed.
    pub trace: TraceCtx,
}

/// An ask lifecycle event (tap facts from Phase 7 read these).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AskOutcome {
    /// The callee replied in time.
    Replied,
    /// The timeout elapsed with no reply.
    Timeout,
    /// The ask failed outright (callee dead, slot lost).
    Failed,
}

#[derive(Debug, Clone)]
pub struct AskFact {
    /// Whether this opened or settled an ask.
    pub opened: bool,
    /// The settled outcome (None while open).
    pub outcome: Option<AskOutcome>,
    /// The callee's address.
    pub dest: Address,
    /// The ask's trace.
    pub trace: TraceCtx,
}

/// Actor tables beyond the registry: cells, journals, live ES state,
/// command entries, snapshot policies, crashes, and dead letters.
///
/// Guarded by one lock — these mutate together (spawn inserts into every
/// table; restart swaps state + endpoint as one observation).
pub struct KernelState {
    pub cells: HashMap<Path, Arc<ActorCell>>,
    pub journals: HashMap<Path, Journal>,
    pub es_state: HashMap<Path, Arc<tokio::sync::Mutex<Box<dyn DynEsActor>>>>,
    pub entries: HashMap<Path, Vec<Arc<dyn CommandEntry>>>,
    pub snapshot_policy: HashMap<Path, SnapshotPolicy>,
    /// Live service instances (service actors are not journaled).
    pub services: HashMap<Path, Arc<tokio::sync::Mutex<Box<dyn DynServiceActor>>>>,
    /// Reply-slot leases (the mechanism half of reply addresses).
    pub replies: crate::reply::ReplyTable,
    /// Ask lifecycle facts (the tap consumes these in Phase 7).
    pub ask_facts: Vec<AskFact>,
    /// Per-actor async message dispatch entries.
    pub msg_entries: HashMap<Path, Vec<Arc<dyn MsgEntry>>>,
    /// Spawn args (genesis rebuild needs them at restart time).
    pub genesis_args: HashMap<Path, JsonValue>,
    /// Paths whose loop died to a handler panic (awaiting supervision).
    pub crashed: HashSet<Path>,
    /// Envelopes that could not be delivered or decoded.
    pub dead_letters: Vec<DeadLetter>,
    /// Topic logs: bounded rings with per-subscriber cursors.
    pub topic_logs: HashMap<crate::types::Topic, crate::topics::TopicLog>,
    /// Topic publish facts (tap consumes in Phase 7).
    pub topic_facts: Vec<crate::topics::TopicPublishFact>,
    /// The global observation ring (drop-oldest).
    pub tap: crate::tap::TapRing,
    /// Supervised children: path → spec.
    pub specs: HashMap<Path, crate::supervision::ChildSpec>,
    /// Sliding-window failure records: path → window.
    pub failures: HashMap<Path, crate::supervision::FailureWindow>,
}

impl Default for KernelState {
    fn default() -> Self {
        Self::with_tap_capacity(4096)
    }
}

impl KernelState {
    /// A fresh state with a tap ring of the given capacity.
    pub fn with_tap_capacity(tap_capacity: usize) -> Self {
        Self {
            cells: HashMap::new(),
            journals: HashMap::new(),
            es_state: HashMap::new(),
            entries: HashMap::new(),
            snapshot_policy: HashMap::new(),
            services: HashMap::new(),
            replies: crate::reply::ReplyTable::default(),
            ask_facts: Vec::new(),
            msg_entries: HashMap::new(),
            genesis_args: HashMap::new(),
            crashed: HashSet::new(),
            dead_letters: Vec::new(),
            topic_logs: HashMap::new(),
            topic_facts: Vec::new(),
            tap: crate::tap::TapRing::new(tap_capacity),
            specs: HashMap::new(),
            failures: HashMap::new(),
        }
    }
}

/// Emits one fact onto the tap with the given clock's timestamp.
pub fn emit(
    tap_clock: &Mutex<KernelState>,
    clock: &crate::clock::ClockService,
    kind: crate::tap::FactKind,
) {
    let ts = clock.now();
    tap_clock.lock().expect("kernel lock").tap.push(ts, kind);
}

/// Kernel-facing handle for one running actor loop.
pub struct ActorHandle {
    /// The kill switch: signaled on graceful stop.
    pub shutdown: watch::Sender<bool>,
    /// The task join handle; aborted on hard remove.
    pub task: Option<tokio::task::JoinHandle<()>>,
}

/// Everything the runtime owns for one actor across restarts.
pub struct ActorCell {
    /// The actor's path (its identity).
    pub path: Path,
    /// The runtime-owned inbox (survives endpoint swaps).
    pub inbox: tokio::sync::Mutex<Inbox>,
    /// The mailbox front door registered in the registry slot.
    pub endpoint: ArcSwapOption<Endpoint>,
    /// The running loop's handle, when a task is live.
    pub handle: tokio::sync::Mutex<Option<ActorHandle>>,
    /// Wakes the actor loop when work arrives (latency optimization; the
    /// loop's poll backstop is the correctness guarantee).
    pub work: Arc<Notify>,
}

impl ActorCell {
    /// Creates a cell with a fresh inbox; the endpoint arrives on start.
    pub fn new(path: Path, inbox: Inbox) -> Self {
        Self {
            path,
            inbox: tokio::sync::Mutex::new(inbox),
            endpoint: ArcSwapOption::empty(),
            handle: tokio::sync::Mutex::new(None),
            work: Arc::new(Notify::new()),
        }
    }

    /// The cursor of the runtime-owned inbox (never resets).
    pub async fn cursor(&self) -> InboxOffset {
        self.inbox.lock().await.cursor()
    }
}

/// Everything one running ES loop needs; cloned per spawn/restart.
#[derive(Clone)]
pub struct EsLoop {
    /// The actor's path.
    pub path: Path,
    /// The actor's cell (inbox + front door).
    pub cell: Arc<ActorCell>,
    /// The shared routing table.
    pub registry: Arc<Mutex<Registry>>,
    /// The shared actor tables.
    pub kernel: Arc<Mutex<KernelState>>,
    /// The read-only view handed to handler contexts.
    pub view: Arc<dyn RuntimeView>,
    /// The injected clock (lease expiries, deterministic tests).
    pub clock: crate::clock::ClockService,
}

impl EsLoop {
    /// Spawns the front door + the ES loop for this actor.
    pub fn start(self, rx: mpsc::Receiver<Envelope>, shutdown: watch::Receiver<bool>) {
        let front_cell = self.cell.clone();
        let front_kernel = self.kernel.clone();
        tokio::spawn(front_door_loop(front_cell, front_kernel, rx));
        tokio::spawn(es_actor_loop(self, shutdown));
    }
}

/// Routes an envelope through the registry to its destination.
///
/// Returns the delivered path, or the envelope back for dead-lettering
/// when the destination does not resolve (slot/topic routing lands in
/// Phases 5–6).
pub async fn route(
    registry: &Mutex<Registry>,
    kernel: &Mutex<KernelState>,
    envelope: Envelope,
) -> Result<Path, Envelope> {
    let dest = envelope.dest.clone();
    match dest {
        Address::Path(ref path) => {
            let endpoint = {
                let registry = registry.lock().expect("registry lock");
                registry.resolve(path)
            };
            match endpoint {
                Some(endpoint) => deliver_with_retry(&endpoint, envelope.clone()).await?,
                None => return Err(envelope),
            }
            {
                let mut kernel = kernel.lock().expect("kernel lock");
                kernel.tap.push(
                    envelope.trace.causality_id.as_millis_ts(),
                    crate::tap::FactKind::Sent {
                        from: envelope.from.clone(),
                        dest: Address::Path(path.clone()),
                        schema: envelope.schema.clone(),
                        trace: envelope.trace,
                    },
                );
            }
            Ok(path.clone())
        }
        Address::Slot(_) => Err(envelope), // reply routing: ctx only
        Address::Topic(ref topic) => {
            publish_to_topic(kernel, registry, topic.clone(), envelope.clone()).await;
            {
                let mut kernel = kernel.lock().expect("kernel lock");
                kernel.tap.push(
                    envelope.trace.causality_id.as_millis_ts(),
                    crate::tap::FactKind::Sent {
                        from: envelope.from.clone(),
                        dest: Address::Topic(topic.clone()),
                        schema: envelope.schema.clone(),
                        trace: envelope.trace,
                    },
                );
            }
            let label = format!("topic:{topic}");
            Ok(Path::new(label.as_str()))
        }
    }
}

/// Dead-letters an envelope into the kernel's inspectable record.
///
/// `detail` is the human-readable elaboration (e.g. the decode error);
/// `reason` is the typed category.
pub fn dead_letter(
    kernel: &Mutex<KernelState>,
    envelope: &Envelope,
    reason: crate::types::DeadLetterReason,
    detail: &str,
) {
    let mut kernel = kernel.lock().expect("kernel lock");
    kernel.dead_letters.push(DeadLetter {
        schema: envelope.schema.clone(),
        dest: envelope.dest.clone(),
        reason: format!("{reason:?}: {detail}"),
        trace: envelope.trace,
    });
    // The DLQ is a REAL topic: the envelope is appended to the retained
    // `system.deadletters` log so a DLQ consumer can subscribe /
    // reset-cursor and re-consume it later.
    let log = kernel
        .topic_logs
        .entry(Registry::dead_letter_topic())
        .or_insert_with(|| crate::topics::TopicLog::new(256));
    log.append(envelope.clone());
    kernel.tap.push(
        envelope.trace.causality_id.as_millis_ts(),
        crate::tap::FactKind::DeadLettered {
            dest: envelope.dest.clone(),
            schema: envelope.schema.clone(),
            reason,
            trace: envelope.trace,
        },
    );
}

/// Pumps the DLQ topic once: offers every retained entry past each DLQ
/// subscriber's cursor. Called by the loops after dead-lettering.
pub async fn pump_dlq(registry: &Mutex<Registry>, kernel: &Mutex<KernelState>) {
    pump_topic(kernel, registry, &Registry::dead_letter_topic()).await;
}

/// Delivers to an endpoint, honoring Block by awaiting capacity.
async fn deliver_with_retry(endpoint: &Endpoint, envelope: Envelope) -> Result<(), Envelope> {
    use tokio::sync::mpsc::error::TrySendError::*;
    match endpoint.try_deliver(envelope.clone()) {
        Ok(()) => Ok(()),
        Err(Full(envelope)) => endpoint
            .deliver(envelope)
            .await
            .map_err(|send_err| send_err.0),
        Err(Closed(envelope)) => Err(envelope), // the slot's endpoint died mid-restart
    }
}

/// The front-door task: drains the mpsc into the runtime-owned inbox.
///
/// Only DropOld/DropNew refusals land here (the mpsc already backpressures
/// Block); refused/evicted messages are dead-lettered — never lost silently.
pub async fn front_door_loop(
    cell: Arc<ActorCell>,
    kernel: Arc<Mutex<KernelState>>,
    mut rx: mpsc::Receiver<Envelope>,
) {
    while let Some(envelope) = rx.recv().await {
        let accepted = {
            let mut inbox = cell.inbox.lock().await;
            match inbox.push(envelope.clone()) {
                Ok(_) => true,
                Err(refused) => {
                    if !refused.queued_anyway() {
                        dead_letter(
                            &kernel,
                            &envelope,
                            crate::types::DeadLetterReason::InboxRefused,
                            "inbox refused (overload/closed)",
                        );
                    } else {
                        let evicted = refused.into_envelope();
                        dead_letter(
                            &kernel,
                            &evicted,
                            crate::types::DeadLetterReason::InboxRefused,
                            "inbox evicted oldest (DropOld)",
                        );
                    }
                    false
                }
            }
        };
        if accepted {
            cell.work.notify_one();
        }
    }
}

/// The ES actor loop: the atomic step, forever, until shutdown or crash.
///
/// Idles with a notify + short poll backstop; the poll is deliberate — it
/// bounds wakeup latency without lost-wakeup races.
pub async fn es_actor_loop(loop_ctx: EsLoop, mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            break;
        }
        match step_es(&loop_ctx).await {
            Step::Work => continue,
            Step::Idle => {}
            Step::Crashed => break, // supervisor (Phase 8) takes over
        }
        let notified = loop_ctx.cell.work.notified();
        tokio::select! {
            _ = shutdown.changed() => {}
            _ = notified => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }
    }
    drain_inbox_on_stop(&loop_ctx).await;
}

/// What one atomic step concluded.
enum Step {
    /// A message was committed (or dead-lettered); loop continues.
    Work,
    /// No message ready; the loop may idle.
    Idle,
    /// The handler panicked; the loop must stop (state is poisoned).
    Crashed,
}

/// THE ATOMIC STEP — spec order, no deviations:
/// peek → find entry → build ctx → catch_unwind dispatch (decide only)
/// → journal.append → inbox.ack → apply → outbox flush → emit fan-out
/// → maybe snapshot.
async fn step_es(ctx: &EsLoop) -> Step {
    // 1. PEEK (clone, never consume: the ack is the commit point).
    let envelope = {
        let mut inbox = ctx.cell.inbox.lock().await;
        inbox.peek().cloned()
    };
    let Some(envelope) = envelope else {
        return Step::Idle;
    };
    {
        let mut kernel = ctx.kernel.lock().expect("kernel lock");
        kernel.tap.push(
            envelope.trace.causality_id.as_millis_ts(),
            crate::tap::FactKind::Delivered {
                to: ctx.path.clone(),
                schema: envelope.schema.clone(),
                trace: envelope.trace,
            },
        );
    }

    // 2. FIND the command entry for this schema.
    let entry = {
        let kernel = ctx.kernel.lock().expect("kernel lock");
        kernel.entries.get(&ctx.path).and_then(|entries| {
            entries
                .iter()
                .find(|e| e.schema() == envelope.schema)
                .cloned()
        })
    };
    let Some(entry) = entry else {
        // Unknown schema: dead-letter and ADVANCE the cursor (the message
        // can never be handled; redelivering it would be futile).
        dead_letter(
            &ctx.kernel,
            &envelope,
            crate::types::DeadLetterReason::UnknownSchema,
            "no entry for this schema",
        );
        pump_dlq(&ctx.registry, &ctx.kernel).await;
        ctx.cell.inbox.lock().await.ack();
        return Step::Work;
    };

    // 3+4. Build ctx, dispatch under catch_unwind. DECIDE ONLY: no state
    // mutation, no journal write, no ack inside the handler.
    let mut outbox = Outbox::new();
    let dispatch_result = {
        let state = ctx.state().await;
        let mut state = state.lock().await;
        let payload = envelope
            .as_json()
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let mut cmd_ctx = CmdCtx::new(
            &ctx.path,
            &envelope.trace,
            envelope.reply_to.as_ref(),
            ctx.view.as_ref(),
            &mut outbox,
        );

        std::panic::catch_unwind(AssertUnwindSafe(|| {
            entry.dispatch(state.as_mut(), &payload, &mut cmd_ctx)
        }))
    };

    let events = match dispatch_result {
        Ok(Ok(events)) => events,
        Ok(Err(report)) => {
            // Decode failure: dead-letter and advance (kernel bug only if
            // the schema registry and adapter disagree).
            let reason = format!("{report}");
            dead_letter(
                &ctx.kernel,
                &envelope,
                crate::types::DeadLetterReason::Decode,
                &reason,
            );
            pump_dlq(&ctx.registry, &ctx.kernel).await;
            ctx.cell.inbox.lock().await.ack();
            return Step::Work;
        }
        Err(poison) => {
            // PANIC: nothing appended, nothing acked, outbox discarded.
            // The state may be poisoned — mark crashed and stop; the
            // supervisor rebuilds from the journal (never reuses `state`).
            {
                let mut kernel = ctx.kernel.lock().expect("kernel lock");
                kernel.crashed.insert(ctx.path.clone());
                kernel.tap.push(
                    envelope.trace.causality_id.as_millis_ts(),
                    crate::tap::FactKind::Failed {
                        path: ctx.path.clone(),
                        error: "handler panic".to_owned(),
                    },
                );
            }
            let _ = poison;
            return Step::Crashed;
        }
    };

    // 5. JOURNAL APPEND (durable record first).
    let seqs = {
        let mut kernel = ctx.kernel.lock().expect("kernel lock");
        let journal = kernel.journals.entry(ctx.path.clone()).or_default();
        let seqs: Vec<_> = events
            .iter()
            .map(|ev| journal.append_event(ev.clone()))
            .collect();
        (journal.next_seq(), seqs)
    };

    // 6. ACK (the commit point: this message will never redeliver).
    ctx.cell.inbox.lock().await.ack();
    {
        let mut kernel = ctx.kernel.lock().expect("kernel lock");
        kernel.tap.push(
            envelope.trace.causality_id.as_millis_ts(),
            crate::tap::FactKind::Acked {
                to: ctx.path.clone(),
                schema: envelope.schema.clone(),
                trace: envelope.trace,
            },
        );
    }

    // 7. APPLY (the same fold replay uses; state may now lag the journal
    // only if the process dies before this line — rebuild covers that).
    {
        let state = ctx.state().await;
        let mut state = state.lock().await;
        for event in &events {
            state.apply_erased(event);
        }
    }

    // 8. OUTBOX FLUSH (deferred sends/replies, causality-linked).
    flush_outbox(ctx, outbox).await;

    // 9. EMIT FAN-OUT (events onto the manifest's emit topics; topics land
    // in Phase 6 — the named step exists so the order never changes).
    fan_out_emits(ctx, &events).await;

    // 10. MAYBE SNAPSHOT (policy EveryN, BETWEEN messages).
    maybe_snapshot(ctx, seqs).await;
    Step::Work
}

impl EsLoop {
    /// The live state shell.
    async fn state(&self) -> Arc<tokio::sync::Mutex<Box<dyn DynEsActor>>> {
        let kernel = self.kernel.lock().expect("kernel lock");
        kernel
            .es_state
            .get(&self.path)
            .cloned()
            .expect("es state present for a running loop")
    }
}

/// Flushes the outbox: sends route through the registry; failures dead-letter.
async fn flush_outbox(ctx: &EsLoop, mut outbox: Outbox) {
    for intent in outbox.drain() {
        match intent {
            crate::context::Intent::Send(envelope) => {
                if let Err(undeliverable) = route(&ctx.registry, &ctx.kernel, envelope).await {
                    dead_letter(
                        &ctx.kernel,
                        &undeliverable,
                        crate::types::DeadLetterReason::Unresolvable,
                        "destination unresolved",
                    );
                }
            }
            crate::context::Intent::Publish { topic, envelope } => {
                publish_to_topic(&ctx.kernel, &ctx.registry, topic, envelope).await;
            }
            crate::context::Intent::Reply {
                to,
                schema,
                payload,
                trace,
            } => {
                resolve_reply(&ctx.kernel, &ctx.registry, to, schema, payload, trace).await;
            }
            crate::context::Intent::Subscribe { path, topic } => {
                apply_subscribe(&ctx.kernel, &ctx.registry, &path, &topic);
            }
        }
    }
}

/// Performs one deferred subscription: the actor joins its topic at the
/// next offset ( Latest); the topic log is created on demand.
fn apply_subscribe(
    kernel: &Mutex<KernelState>,
    registry: &Mutex<Registry>,
    path: &Path,
    topic: &crate::types::Topic,
) {
    let policy = {
        let registry = registry.lock().expect("registry lock");
        registry.inbox_policy(path)
    };
    let mut kernel = kernel.lock().expect("kernel lock");
    let log = kernel
        .topic_logs
        .entry(topic.clone())
        .or_insert_with(|| crate::topics::TopicLog::new(256));
    log.subscribe(path.clone(), policy, crate::topics::CursorFrom::Latest);
}

/// The kernel's ask port: opens leases, routes request envelopes,
/// records ask facts. Handed to service contexts at dispatch time.
#[derive(Clone)]
pub struct KernelAskPort {
    /// The shared routing table.
    pub registry: Arc<Mutex<Registry>>,
    /// The shared actor tables (reply leases + ask facts live here).
    pub kernel: Arc<Mutex<KernelState>>,
    /// The clock for lease expiries.
    pub clock: crate::clock::ClockService,
}

impl crate::context::AskPort for KernelAskPort {
    fn ask_channel(
        &self,
        dest: Address,
        schema: SchemaId,
        payload: JsonValue,
        ttl: std::time::Duration,
    ) -> std::pin::Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (
                            crate::types::LeaseId,
                            tokio::sync::oneshot::Receiver<JsonValue>,
                        ),
                        error_stack::Report<crate::context::AskError>,
                    >,
                > + Send,
        >,
    > {
        let registry = self.registry.clone();
        let kernel = self.kernel.clone();
        let clock = self.clock.clone();
        Box::pin(async move {
            // Resolve the destination FIRST: an unresolvable ask fails fast.
            let endpoint = {
                let Address::Path(path) = &dest else {
                    return Err(error_stack::Report::new(
                        crate::context::AskError::Unresolved(format!("{dest:?}")),
                    ));
                };
                registry.lock().expect("registry lock").resolve(path)
            };
            let Some(endpoint) = endpoint else {
                return Err(error_stack::Report::new(
                    crate::context::AskError::Unresolved(format!("{dest:?}")),
                ));
            };

            // Open the lease and route the request envelope.
            let now = clock.now();
            let (lease, receiver) = {
                let mut kernel = kernel.lock().expect("kernel lock");
                let trace = crate::envelope::TraceCtx::root();
                let (lease, receiver) = kernel.replies.open(ttl, now);
                kernel.ask_facts.push(AskFact {
                    opened: true,
                    outcome: None,
                    dest: dest.clone(),
                    trace,
                });
                kernel.tap.push(
                    now,
                    crate::tap::FactKind::AskOpened {
                        from: Path::new("anonymous"),
                        dest: dest.clone(),
                        trace,
                    },
                );
                (lease, receiver)
            };
            let trace = crate::envelope::TraceCtx::root();
            let envelope =
                Envelope::json(schema, dest.clone(), payload, trace).reply_to(Address::Slot(lease));
            match deliver_with_retry(&endpoint, envelope).await {
                Ok(()) => Ok((lease, receiver)),
                Err(_) => Err(error_stack::Report::new(
                    crate::context::AskError::Unresolved(format!("{dest:?}")),
                )),
            }
        })
    }

    fn ask_settled(
        &self,
        lease: crate::types::LeaseId,
        dest: Address,
        outcome: AskOutcome,
        trace: TraceCtx,
    ) {
        let mut kernel = self.kernel.lock().expect("kernel lock");
        kernel.ask_facts.push(AskFact {
            opened: false,
            outcome: Some(outcome.clone()),
            dest: dest.clone(),
            trace,
        });
        kernel.tap.push(
            trace.causality_id.as_millis_ts(),
            crate::tap::FactKind::AskSettled { outcome, trace },
        );
        let _ = dest;
        // Drop the lease: settled (consumed) or timed out (late replies
        // land nowhere). The reply's `complete` already removed it on the
        // Replied path; removal here is idempotent.
        kernel.replies.cancel(&lease);
    }
}

/// Publishes one envelope onto a topic: appends to the bounded log,
/// pumps subscribers (each offered entries past its own cursor), and
/// records the publish fact. Never blocks on a slow subscriber — its
/// cursor simply falls behind.
async fn publish_to_topic(
    kernel: &Mutex<KernelState>,
    registry: &Mutex<Registry>,
    topic: crate::types::Topic,
    envelope: Envelope,
) {
    // Append + collect subscriber endpoints without holding locks across
    // delivery; pump_once owns per-subscriber cursor/backlog semantics.
    let offset = {
        let mut kernel = kernel.lock().expect("kernel lock");
        let log = kernel
            .topic_logs
            .entry(topic.clone())
            .or_insert_with(|| crate::topics::TopicLog::new(256));
        log.append(envelope.clone())
    };
    record_publish_facts(kernel, &topic, &envelope, offset);
    pump_topic(kernel, registry, &topic).await;
}

/// Records the publish facts (topic log fact + tap fact) for one append.
fn record_publish_facts(
    kernel: &Mutex<KernelState>,
    topic: &crate::types::Topic,
    envelope: &Envelope,
    offset: u64,
) {
    let mut kernel = kernel.lock().expect("kernel lock");
    kernel.topic_facts.push(crate::topics::TopicPublishFact {
        topic: topic.clone(),
        offset: crate::types::InboxOffset::new(offset),
        schema: envelope.schema.clone(),
        from: envelope.from.clone(),
        trace: envelope.trace,
    });
    kernel.tap.push(
        envelope.trace.causality_id.as_millis_ts(),
        crate::tap::FactKind::TopicPublished {
            topic: topic.clone(),
            schema: envelope.schema.clone(),
            trace: envelope.trace,
        },
    );
}

/// One pump pass over a topic: every subscriber is offered every RETAINED
/// entry past its own cursor (a reset cursor re-consumes here); only
/// accepted deliveries advance a cursor — a refused one stays behind
/// (slow-subscriber isolation, never blocking other subscribers).
async fn pump_topic(
    kernel: &Mutex<KernelState>,
    registry: &Mutex<Registry>,
    topic: &crate::types::Topic,
) {
    let targets: Vec<(Path, std::sync::Arc<Endpoint>)> = {
        let kernel = kernel.lock().expect("kernel lock");
        let registry = registry.lock().expect("registry lock");
        kernel
            .topic_logs
            .get(topic)
            .map(|log| log.subscribers())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|path| registry.resolve(&path).map(|endpoint| (path, endpoint)))
            .collect()
    };
    let mut kernel = kernel.lock().expect("kernel lock");
    let Some(log) = kernel.topic_logs.get_mut(topic) else {
        return;
    };
    let _delivered_skipped = log.pump_once(|path, entry, _policy| {
        let Some((_, endpoint)) = targets.iter().find(|(p, _)| p == path) else {
            return false;
        };
        endpoint.try_deliver(entry.clone()).is_ok()
    });
}

/// Resolves one reply: a slot goes straight to the asker's oneshot (the
/// mechanism); a path routes an ordinary envelope through the registry
/// (the durable name).
async fn resolve_reply(
    kernel: &Mutex<KernelState>,
    registry: &Mutex<Registry>,
    to: Address,
    schema: SchemaId,
    payload: JsonValue,
    trace: TraceCtx,
) {
    match to {
        Address::Slot(lease) => {
            // Mechanism: complete the lease if it is still live; a dead
            // (expired/pruned) slot just drops the reply — the asker is
            // gone, and the ask timed out on its side already.
            kernel
                .lock()
                .expect("kernel lock")
                .replies
                .complete(&lease, payload);
        }
        Address::Path(path) => {
            // Durable name: an ordinary envelope (any actor may have moved
            // on; unresolvable replies dead-letter like any send).
            let envelope = Envelope::json(schema, Address::Path(path.clone()), payload, trace);
            if let Err(undeliverable) = route(registry, kernel, envelope).await {
                dead_letter(
                    kernel,
                    &undeliverable,
                    crate::types::DeadLetterReason::Unresolvable,
                    "reply destination unresolved",
                );
            }
        }
        Address::Topic(_) => {
            // Replying onto a topic is not a reply; treat as a send to a
            // topic address (Phase 6 wires topic delivery).
        }
    }
}

/// Fans the committed events out to the manifest's emit topics. Position
/// in the atomic order is AFTER ack — replay never re-runs this (a
/// restart must not duplicate topic deliveries).
async fn fan_out_emits(ctx: &EsLoop, events: &[crate::envelope::Event]) {
    let topics = {
        let registry = ctx.registry.lock().expect("registry lock");
        let Some(info) = registry.lookup(&ctx.path) else {
            return;
        };
        info.manifest.emits_on_topics.clone()
    };
    if topics.is_empty() {
        return;
    }
    let cause = crate::envelope::TraceCtx::root();
    for topic in &topics {
        for event in events {
            let envelope = Envelope::json(
                event.schema.clone(),
                Address::Topic(topic.clone()),
                event.payload.clone(),
                cause,
            )
            .from(ctx.path.clone());
            publish_to_topic(&ctx.kernel, &ctx.registry, topic.clone(), envelope).await;
        }
    }
}

/// Takes the between-messages snapshot if the policy asks for one.
async fn maybe_snapshot(
    ctx: &EsLoop,
    (next_seq, seqs): (crate::types::SeqNo, Vec<crate::types::SeqNo>),
) {
    let policy = {
        let kernel = ctx.kernel.lock().expect("kernel lock");
        kernel
            .snapshot_policy
            .get(&ctx.path)
            .copied()
            .unwrap_or_default()
    };
    let SnapshotPolicy::EveryN(n) = policy else {
        return;
    };
    if n == 0 || seqs.is_empty() {
        return;
    }
    let last = *seqs.last().expect("non-empty");
    // Snapshot when the last committed event landed on an n-boundary.
    if last.as_u64() % n != n - 1 {
        return;
    }
    let state = ctx.state().await;
    let state = state.lock().await;
    if let Ok(blob) = state.capture_erased() {
        let mut kernel = ctx.kernel.lock().expect("kernel lock");
        let journal = kernel.journals.entry(ctx.path.clone()).or_default();
        journal.append_snapshot(last, blob);
        kernel.tap.push(
            ctx.clock.now(),
            crate::tap::FactKind::SnapshotTaken {
                path: ctx.path.clone(),
                seq: last,
            },
        );
    }
    let _ = next_seq;
}

/// Closes the inbox on stop; Phase 8 flushes undelivered entries to the DLQ.
async fn drain_inbox_on_stop(ctx: &EsLoop) {
    let mut inbox = ctx.cell.inbox.lock().await;
    inbox.close();
    drop(inbox);
    // Entries stay queued: restart reopens the inbox and redelivery
    // resumes from the cursor. (Graceful stop flushes to the DLQ — Phase 8.)
}

/// The service actor loop: pop → decode → dispatch (async, impure) →
/// drop the message. No journal, no cursor — service actors are at-most-once
/// by design (Phase 8 adds supervision around this loop).
pub async fn service_actor_loop(loop_ctx: ServiceLoop, mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            break;
        }
        match step_service(&loop_ctx).await {
            Step::Work => continue,
            Step::Idle => {}
            Step::Crashed => break,
        }
        let notified = loop_ctx.es.cell.work.notified();
        tokio::select! {
            _ = shutdown.changed() => {}
            _ = notified => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }
    }
    drain_inbox_on_stop(&loop_ctx.es).await;
}

/// Everything one running service loop needs.
#[derive(Clone)]
pub struct ServiceLoop {
    /// The ES-shaped plumbing the service loop shares (routing, cell).
    pub es: EsLoop,
}

/// One service step: peek → decode (sync) → dispatch (async) → ack.
///
/// Service messages are consumed on HANDOFF (ack before dispatch): there is
/// no journal to replay from, so redelivery after a crash would re-run
/// side effects — at-most-once semantics are the honest contract here.
async fn step_service(ctx: &ServiceLoop) -> Step {
    // 1. PEEK the envelope.
    let envelope = {
        let mut inbox = ctx.es.cell.inbox.lock().await;
        inbox.peek().cloned()
    };
    let Some(envelope) = envelope else {
        return Step::Idle;
    };

    {
        let mut kernel = ctx.es.kernel.lock().expect("kernel lock");
        kernel.tap.push(
            envelope.trace.causality_id.as_millis_ts(),
            crate::tap::FactKind::Delivered {
                to: ctx.es.path.clone(),
                schema: envelope.schema.clone(),
                trace: envelope.trace,
            },
        );
    }

    // 2. FIND the message entry.
    let entry = {
        let kernel = ctx.es.kernel.lock().expect("kernel lock");
        kernel.msg_entries.get(&ctx.es.path).and_then(|entries| {
            entries
                .iter()
                .find(|e| e.schema() == envelope.schema)
                .cloned()
        })
    };
    let Some(entry) = entry else {
        dead_letter(
            &ctx.es.kernel,
            &envelope,
            crate::types::DeadLetterReason::UnknownSchema,
            "no entry for this schema",
        );
        pump_dlq(&ctx.es.registry, &ctx.es.kernel).await;
        ctx.es.cell.inbox.lock().await.ack();
        return Step::Work;
    };

    // 3. DECODE (sync — decode failures dead-letter cleanly).
    let payload = envelope
        .as_json()
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let decoded = match entry.decode(&payload) {
        Ok(msg) => msg,
        Err(report) => {
            let reason = format!("{report}");
            dead_letter(
                &ctx.es.kernel,
                &envelope,
                crate::types::DeadLetterReason::Decode,
                &reason,
            );
            pump_dlq(&ctx.es.registry, &ctx.es.kernel).await;
            ctx.es.cell.inbox.lock().await.ack();
            return Step::Work;
        }
    };

    // 4. CONSUME (ack) — at-most-once handoff to the handler.
    ctx.es.cell.inbox.lock().await.ack();

    // 5. DISPATCH (async, impure) — spawned so handler panics surface as a
    // JoinHandle error instead of tearing down the loop task itself; the
    // loop awaits the handle, so one actor still processes one message at
    // a time (its inbox serializes).
    let path = ctx.es.path.clone();
    let service = {
        let kernel = ctx.es.kernel.lock().expect("kernel lock");
        kernel
            .services
            .get(&path)
            .cloned()
            .expect("service present for a running loop")
    };
    let view = ctx.es.view.clone();
    let entry = entry.clone();
    let ask_port = KernelAskPort {
        registry: ctx.es.registry.clone(),
        kernel: ctx.es.kernel.clone(),
        clock: ctx.es.clock.clone(),
    };
    let (outbox_tx, outbox_rx) = tokio::sync::oneshot::channel();
    let trace = envelope.trace;
    let reply_to = envelope.reply_to.clone();
    let handle = tokio::spawn(async move {
        let mut outbox = Outbox::new();
        let mut msg_ctx = crate::context::MsgCtx::new(
            &path,
            &trace,
            reply_to.as_ref(),
            view.as_ref(),
            &mut outbox,
            Some(&ask_port),
        );
        let mut service = service.lock().await;
        entry
            .dispatch(service.as_mut(), decoded, &mut msg_ctx)
            .await;
        let _ = outbox_tx.send(outbox);
    });
    let outbox = if handle.await.is_err() {
        // Handler panicked: mark crashed (supervision restarts via `start`).
        let mut kernel = ctx.es.kernel.lock().expect("kernel lock");
        kernel.crashed.insert(ctx.es.path.clone());
        return Step::Crashed;
    } else {
        outbox_rx.await.unwrap_or_default()
    };

    // 6. FLUSH deferred effects from the handler.
    flush_outbox(&ctx.es, outbox).await;
    Step::Work
}

/// Restarts a crashed ES actor (spec algorithm):
/// 1. rebuild state from the last snapshot (fast path) or genesis args,
/// 2. apply the journal tail after the snapshot seq (replay: apply only —
///    handlers never re-run, effects never re-fire),
/// 3. swap the slot endpoint to a fresh loop; the inbox and its cursor
///    persist, so pending messages (including the one that crashed the
///    actor) redeliver exactly once from where the cursor stopped.
///
/// The supervisor (Phase 8) wraps this with policy/budget/backoff checks.
///
/// # Errors
///
/// Propagates rebuild failures (a corrupt snapshot or undecodable state).
pub async fn restart_es(
    ctx: &EsLoop,
    genesis_args: &JsonValue,
) -> Result<(), error_stack::Report<JournalError>> {
    let (snapshot, snap_seq, tail) = {
        let kernel = ctx.kernel.lock().expect("kernel lock");
        let Some(journal) = kernel.journals.get(&ctx.path) else {
            return Ok(());
        };
        let snapshot = journal.last_snapshot().cloned();
        let snap_seq = snapshot.as_ref().map(|entry| entry.seq());
        let tail: Vec<Event> = journal
            .after(snap_seq.unwrap_or_else(SeqNo::before_genesis))
            .filter_map(|entry| entry.as_event().cloned())
            .collect();
        (snapshot, snap_seq, tail)
    };

    // Rebuild through the erased shell: the OLD instance is dropped, the
    // fresh one starts from snapshot-or-genesis plus the replay tail.
    let snapshot_state = match snapshot {
        Some(JournalEntry::Snapshot { state, .. }) => Some(state),
        _ => None,
    };
    // Lock order: resolve the state Arc WITHOUT holding the kernel's sync
    // guard across the await (a sync guard held across .await can deadlock
    // tasks that need the sync lock to make progress).
    let old = {
        let kernel = ctx.kernel.lock().expect("kernel lock");
        kernel
            .es_state
            .get(&ctx.path)
            .cloned()
            .expect("es state present at restart")
    };
    let fresh = {
        let old = old.lock().await;
        old.rebuild(genesis_args, snapshot_state, &tail)?
    };
    // Fresh instance swapped in; the poisoned one is gone.
    {
        let mut kernel = ctx.kernel.lock().expect("kernel lock");
        kernel
            .es_state
            .insert(ctx.path.clone(), Arc::new(tokio::sync::Mutex::new(fresh)));
        kernel.crashed.remove(&ctx.path);
        kernel.tap.push(
            ctx.clock.now(),
            crate::tap::FactKind::Spawned {
                path: ctx.path.clone(),
                kind: crate::types::ActorKind::EventSourced,
                restart: true,
            },
        );
    }

    // Fresh endpoint behind the SAME path: senders holding pre-crash
    // clones never notice (identity = path; slots are swapped, not dropped).
    let (tx, rx) = mpsc::channel(capacity_hint());
    let endpoint = Endpoint::new(tx);
    {
        let mut registry = ctx.registry.lock().expect("registry lock");
        registry
            .swap_endpoint(&ctx.path, endpoint)
            .expect("slot exists at restart");
    }

    // Redelivery: the cursor never moved; the crash-loop left the inbox
    // open-and-queued. Reopen and run a fresh loop over the SAME cell.
    {
        let mut inbox = ctx.cell.inbox.lock().await;
        inbox.reopen();
    }
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(front_door_loop(ctx.cell.clone(), ctx.kernel.clone(), rx));
    tokio::spawn(es_actor_loop(ctx.clone(), shutdown_rx));
    let _ = snap_seq;
    Ok(())
}

/// The front-door capacity for restarted endpoints.
fn capacity_hint() -> usize {
    64
}

/// The supervision engine: one task per supervised child, awaiting the
/// child's crash, then applying the spec — policy → budget → backoff →
/// restart, or stop + escalate.
pub async fn supervise_child(
    system: std::sync::Arc<crate::system::ActorSystem>,
    spec: crate::supervision::ChildSpec,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        // Wait for this child to crash (or the system to shut down).
        let mut crashed_seen = false;
        let watch = async {
            loop {
                let crashed = {
                    let kernel = system.kernel.lock().expect("kernel lock");
                    kernel.crashed.contains(&spec.path)
                };
                if crashed {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        };
        tokio::select! {
            _ = watch => { crashed_seen = true; }
            _ = shutdown.changed() => {
                if *shutdown.borrow() { return; }
            }
        }
        let _ = crashed_seen;

        // The child crashed. Interpret the spec.
        let now = system.clock.now().as_millis();
        let policy = spec.restart;
        if policy == crate::supervision::RestartPolicy::Never {
            escalate(&system, &spec, "policy Never (crashed)").await;
            return;
        }

        // Budget: record the failure first, then check the window.
        let budget_exhausted = {
            let mut kernel = system.kernel.lock().expect("kernel lock");
            let window = kernel.failures.entry(spec.path.clone()).or_default();
            window.record(now);
            window.prune(now, &spec.budget);
            window.exhausted(now, &spec.budget)
        };
        if budget_exhausted {
            escalate(&system, &spec, "restart budget exhausted").await;
            return;
        }

        // Backoff, then restart through the spec's spawn closure.
        let consecutive = {
            let kernel = system.kernel.lock().expect("kernel lock");
            kernel
                .failures
                .get(&spec.path)
                .map(|w| w.count(now, &spec.budget))
                .unwrap_or(1) as u32
        };
        let delay = spec.backoff.delay(consecutive);
        tokio::time::sleep(delay).await;
        // The spawn closure does a FULL spawn (slot insert included);
        // clear the dead instance's slot first so the path is free.
        {
            let mut registry = system.registry.lock().expect("registry lock");
            let _ = registry.remove_slot(&spec.path);
        }
        (spec.spawn)(&system, &spec.path, &spec.args);
        // The fresh instance is running (the spawn closure re-runs the
        // loop); clear the stale crash flag so the next wait observes a
        // NEW crash, not the one we just handled.
        {
            let mut kernel = system.kernel.lock().expect("kernel lock");
            kernel.crashed.remove(&spec.path);
        }
    }
}

/// Stops the child (slot + crash record) and escalates a control message
/// to the parent (or the system record when parentless). Emits the
/// Escalated fact.
async fn escalate(
    system: &std::sync::Arc<crate::system::ActorSystem>,
    spec: &crate::supervision::ChildSpec,
    reason: &str,
) {
    {
        let mut kernel = system.kernel.lock().expect("kernel lock");
        kernel.crashed.remove(&spec.path);
        kernel.tap.push(
            system.clock.now(),
            crate::tap::FactKind::Escalated {
                path: spec.path.clone(),
                reason: reason.to_owned(),
            },
        );
    }
    // Remove the child's slot (its identity leaves the registry; the
    // graceful-stop cascade arrives with the stop API in Phase 9).
    {
        let mut registry = system.registry.lock().expect("registry lock");
        let _ = registry.remove_slot(&spec.path);
    }
    let message = spec.escalation_message(reason);
    if let Some(parent) = &spec.parent {
        system
            .send(system.envelope(
                crate::types::SchemaId::new("Escalated", 1),
                parent.clone(),
                message,
            ))
            .await
            .ok();
    }
}
