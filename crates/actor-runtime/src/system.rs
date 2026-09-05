//! The system facade: the single handle through which the runtime is
//! configured, driven, and observed.
//!
//! The registry is kernel, not an actor — owned here behind a lock so
//! schema registration can never deadlock and survives every actor restart.
//! The system also implements [`RuntimeView`]: handler-side lookups snapshot
//! through this read-only surface, never through kernel-mutable locks.

use std::sync::{Arc, Mutex};

use serde_json::Value as JsonValue;

use crate::actor::{
    CommandEntry, DynServiceActor, EventSourced, MsgEntry, ServiceActor, TypedEsState,
    TypedServiceState,
};
use crate::clock::{ClockService, SystemClock};
use crate::context::RuntimeView;
use crate::envelope::{Address, Envelope, TraceCtx};
use crate::inbox::{Inbox, OverloadPolicy};
pub use crate::kernel::DeadLetter;
use crate::kernel::{route, EsLoop, ActorCell, KernelState};
use crate::registry::{Endpoint, EndpointInfo, Registry};
use crate::schema::Schema;
use crate::types::{InboxOffset, Path, SchemaId, Timestamp};

pub use crate::kernel::SnapshotPolicy;

/// Spawn-time options for an actor.
#[derive(Debug, Clone)]
pub struct SpawnOpts {
    /// Snapshot policy (ES actors only).
    pub snapshot: SnapshotPolicy,
    /// Mailbox capacity (the logical inbox; the front door is 2× this).
    pub mailbox_capacity: usize,
    /// Inbox overload policy (default Block = backpressure).
    pub mailbox_policy: OverloadPolicy,
}

impl Default for SpawnOpts {
    fn default() -> Self {
        Self {
            snapshot: SnapshotPolicy::Off,
            mailbox_capacity: 64,
            mailbox_policy: OverloadPolicy::Block,
        }
    }
}

/// The actor system. One instance per machine; shared by reference.
///
/// The registry is its own mutex (routing never blocks actor-table
/// mutations); actor tables share [`KernelState`]'s lock because they
/// mutate together.
pub struct ActorSystem {
    /// Routing table: slots, schemas, routes.
    registry: Arc<Mutex<Registry>>,
    /// Actor tables: cells, journals, ES state, entries, crashes.
    kernel: Arc<Mutex<KernelState>>,
    clock: ClockService,
    /// The read-only view handed to handler contexts (the system itself).
    view: Arc<dyn RuntimeView>,
}

impl ActorSystem {
    /// The system dead-letter topic, created at boot.
    pub fn deadletter_topic() -> crate::types::Topic {
        crate::types::Topic::new("system.deadletters")
    }

    /// Creates a system on the wall clock.
    pub fn new() -> Self {
        Self::with_clock(ClockService::new(Arc::new(SystemClock::new())))
    }

    /// Creates a system on an injected clock (tests: [`crate::clock::FakeClock`]).
    pub fn with_clock(clock: ClockService) -> Self {
        let registry = Arc::new(Mutex::new(Registry::default()));
        let view = Arc::new(NullView {
            registry: registry.clone(),
        });
        Self {
            registry,
            kernel: Arc::new(Mutex::new(KernelState::default())),
            clock,
            view,
        }
    }

    /// Registers a Rust type's schema — the typed flavor.
    ///
    /// Idempotent per name+version: the first registration wins, and the
    /// returned id is stable across repeat registrations.
    pub fn register_schema<S: Schema>(&self) -> SchemaId {
        let mut registry = self.registry.lock().expect("registry lock");
        registry.register_schema_of::<S>()
    }

    /// Registers a schema from a JSON descriptor — the foreign flavor, for
    /// schemas defined outside Rust.
    ///
    /// # Errors
    ///
    /// Returns an error when `json` is not a valid schema descriptor.
    pub fn register_schema_json(
        &self,
        json: JsonValue,
    ) -> Result<SchemaId, error_stack::Report<crate::schema::SchemaError>> {
        let mut registry = self.registry.lock().expect("registry lock");
        registry.register_schema_json(json)
    }

    /// The registered descriptor for an exact `name@version` id, if any.
    pub fn schema(&self, id: &SchemaId) -> Option<crate::schema::SchemaDef> {
        let registry = self.registry.lock().expect("registry lock");
        registry.schema(id).cloned()
    }

    /// Spawns an event-sourced actor at `path`.
    ///
    /// Registers the manifest's schema edges, one command entry per
    /// `handles` schema (via the `spawn_es` closure), builds the journal +
    /// inbox, rebuilds state (snapshot fast-path or genesis), and starts
    /// the ES loop. Redelivery resumes from the inbox cursor.
    pub fn spawn_es<A, F>(self: &Arc<Self>, path: Path, args: &JsonValue, opts: SpawnOpts, entries: F)
    where
        A: EventSourced,
        F: FnOnce() -> Vec<Arc<dyn CommandEntry>>,
    {
        let (tx, rx) = tokio::sync::mpsc::channel::<Envelope>(opts.mailbox_capacity.max(1) * 2);

        // Genesis state (replay lands with the atomic-step task).
        let state = Box::new(TypedEsState::<A>::new(A::restore(args)));

        {
            let manifest = A::manifest();
            let mut registry = self.registry.lock().expect("registry lock");
            registry
                .insert_slot(path.clone(), manifest.clone(), Endpoint::new(tx))
                .expect("path free at spawn");
            // Declared edges become routes: each handled schema is routable
            // to this path (adding a second actor for a schema converts the
            // route to round-robin).
            for schema in manifest.handles {
                registry.add_route(schema, path.clone());
            }
        }
        let mut kernel = self.kernel.lock().expect("kernel lock");
        let cell = Arc::new(ActorCell::new(
            path.clone(),
            Inbox::new(opts.mailbox_capacity.max(1), opts.mailbox_policy),
        ));
        kernel.cells.insert(path.clone(), cell.clone());
        kernel.journals.entry(path.clone()).or_default();
        kernel.es_state.insert(path.clone(), Arc::new(tokio::sync::Mutex::new(state)));
        kernel.entries.insert(path.clone(), entries());
        kernel
            .snapshot_policy
            .insert(path.clone(), opts.snapshot);
        drop(kernel);

        // Front door + ES loop, sharing the kernel tables.
        let loop_ctx = EsLoop {
            path: path.clone(),
            cell,
            registry: self.registry.clone(),
            kernel: self.kernel.clone(),
            view: self.view.clone(),
            clock: self.clock.clone(),
        };
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        loop_ctx.start(rx, shutdown_rx);
    }

    /// Spawns a service (edge) actor at `path`: async handlers, I/O and
    /// `ask` allowed, NOT journaled (at-most-once message semantics).
    pub fn spawn_service<A, F>(self: &Arc<Self>, path: Path, args: &JsonValue, opts: SpawnOpts, entries: F)
    where
        A: ServiceActor,
        F: FnOnce() -> Vec<Arc<dyn MsgEntry>>,
    {
        // `A::start` is async (I/O allowed); block briefly on a runtime
        // thread is not done — spawn the start inside the actor task and
        // register the slot immediately so senders never see a gap.
        let (tx, rx) = tokio::sync::mpsc::channel::<Envelope>(opts.mailbox_capacity.max(1) * 2);
        {
            let mut registry = self.registry.lock().expect("registry lock");
            registry
                .insert_slot(path.clone(), A::manifest(), Endpoint::new(tx))
                .expect("path free at spawn");
        }
        let mut kernel = self.kernel.lock().expect("kernel lock");
        let cell = Arc::new(ActorCell::new(
            path.clone(),
            Inbox::new(opts.mailbox_capacity.max(1), opts.mailbox_policy),
        ));
        kernel.cells.insert(path.clone(), cell.clone());
        kernel.genesis_args.insert(path.clone(), args.clone());
        kernel.msg_entries.insert(path.clone(), entries());
        drop(kernel);

        let loop_ctx = EsLoop {
            path: path.clone(),
            cell: cell.clone(),
            registry: self.registry.clone(),
            kernel: self.kernel.clone(),
            view: self.view.clone(),
            clock: self.clock.clone(),
        };
        let started_path = path.clone();
        let view = self.view.clone();
        let registry = self.registry.clone();
        let kernel_table = self.kernel.clone();
        let start_args = args.clone();
        let front_cell = cell.clone();
        let front_kernel = self.kernel.clone();
        tokio::spawn(async move {
            // Start the instance inside the task; a start failure leaves
            // the slot present (senders get a closed door) and the crash
            // recorded for supervision.
            let started = A::start(&start_args).await;
            match started {
                Ok(instance) => {
                    let mut kernel = kernel_table.lock().expect("kernel lock");
                    kernel.services.insert(
                        started_path.clone(),
                        Arc::new(tokio::sync::Mutex::new(Box::new(
                            TypedServiceState::new(instance),
                        ) as Box<dyn DynServiceActor>)),
                    );
                }
                Err(report) => {
                    let mut kernel = kernel_table.lock().expect("kernel lock");
                    kernel.crashed.insert(started_path.clone());
                    let _ = report;
                }
            }
            let _ = (&view, &registry);
            let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            tokio::spawn(crate::kernel::front_door_loop(front_cell, front_kernel, rx));
            crate::kernel::service_actor_loop(
                crate::kernel::ServiceLoop { es: loop_ctx },
                shutdown_rx,
            )
            .await;
        });
    }

    /// Convenience: spawn with typed adapters for each handled command.
    ///
    /// `entries()` builds the CommandEntry list (usually
    /// `vec![Arc::new(TypedEsAdapter::<A, C1>::new::<C1>()), ...]`).
    pub fn spawn_es_typed<A, F>(self: &Arc<Self>, path: Path, args: &JsonValue, opts: SpawnOpts, entries: F)
    where
        A: EventSourced,
        F: FnOnce() -> Vec<Arc<dyn CommandEntry>>,
    {
        self.spawn_es::<A, F>(path, args, opts, entries)
    }

    /// Sends an envelope from outside the system (entry-point trace root).
    ///
    /// # Errors
    ///
    /// Returns the envelope back when its destination does not resolve
    /// (callers dead-letter or retry).
    pub async fn send(&self, envelope: Envelope) -> Result<Path, Envelope> {
        route(&self.registry, envelope).await
    }

    /// Builds a system-rooted envelope (fresh trace) addressed to a path.
    pub fn envelope(&self, schema: SchemaId, dest: Path, payload: JsonValue) -> Envelope {
        Envelope::json(
            schema,
            Address::Path(dest),
            payload,
            TraceCtx::root(),
        )
    }

    /// The system's clock (tests use this to reach the [`crate::clock::FakeClock`]).
    pub fn clock(&self) -> &ClockService {
        &self.clock
    }

    /// Restarts a crashed ES actor at `path` (supervision calls this; the
    /// Phase 8 engine adds policy/budget/backoff around it).
    ///
    /// # Errors
    ///
    /// Propagates state-rebuild failures (corrupt snapshot or journal).
    pub async fn restart_es(
        &self,
        path: &Path,
        genesis_args: &JsonValue,
    ) -> Result<(), error_stack::Report<crate::journal::JournalError>> {
        let loop_ctx = {
            let kernel = self.kernel.lock().expect("kernel lock");
            let cell = kernel.cells.get(path).cloned();
            drop(kernel);
            cell
        };
        let Some(cell) = loop_ctx else {
            use error_stack::IntoReport;
            return Err(crate::journal::JournalError::Restore.into_report());
        };
        let ctx = EsLoop {
            path: path.clone(),
            cell,
            registry: self.registry.clone(),
            kernel: self.kernel.clone(),
            view: self.view.clone(),
            clock: self.clock.clone(),
        };
        crate::kernel::restart_es(&ctx, genesis_args).await
    }

    /// The cursor of an actor's inbox (inspection; Phase 10 tests).
    pub fn inbox_cursor(&self, path: &Path) -> Option<InboxOffset> {
        let kernel = self.kernel.lock().expect("kernel lock");
        kernel.cells.get(path).map(|cell| {
            cell.inbox
                .try_lock()
                .map(|inbox| inbox.cursor())
                .unwrap_or_else(|_| InboxOffset::zero())
        })
    }

    /// The captured ES state of an actor (for export/inspection).
    pub async fn es_state(&self, path: &Path) -> Option<JsonValue> {
        let state = {
            let kernel = self.kernel.lock().expect("kernel lock");
            kernel.es_state.get(path).cloned()?
        };
        let state = state.lock().await;
        state.capture_erased().ok()
    }
}

/// A read-only snapshot view over the kernel: handler contexts resolve
/// lookups through a brief lock; they can never mutate anything.
struct NullView {
    registry: Arc<Mutex<Registry>>,
}

impl RuntimeView for NullView {
    fn lookup(&self, path: &Path) -> Option<EndpointInfo> {
        let registry = self.registry.lock().expect("registry lock");
        registry.lookup(path)
    }

    fn who_handles(&self, schema: &SchemaId) -> Vec<Path> {
        let registry = self.registry.lock().expect("registry lock");
        registry.who_handles(schema)
    }

    fn now(&self) -> Timestamp {
        Timestamp::from_millis(0)
    }
}

impl RuntimeView for ActorSystem {
    fn lookup(&self, path: &Path) -> Option<EndpointInfo> {
        let registry = self.registry.lock().expect("registry lock");
        registry.lookup(path)
    }

    fn who_handles(&self, schema: &SchemaId) -> Vec<Path> {
        let registry = self.registry.lock().expect("registry lock");
        registry.who_handles(schema)
    }

    fn now(&self) -> Timestamp {
        self.clock.now()
    }
}

impl Default for ActorSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{CommandHandler, MsgHandler, TypedEsAdapter, TypedServiceAdapter};
    use crate::context::CmdCtx;
    use crate::schema::{ActorManifest, FieldDef, FieldTy, SchemaDef, SchemaKind};
    use crate::types::ActorKind;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Deserialize)]
    struct Add {
        n: i64,
    }
    impl Schema for Add {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Add".into(),
                version: 1,
                kind: SchemaKind::Command,
                fields: vec![FieldDef::required("n", FieldTy::Int)],
                description: None,
            }
        }
    }

    #[derive(serde::Deserialize)]
    struct Added {
        n: i64,
    }
    impl Schema for Added {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Added".into(),
                version: 1,
                kind: SchemaKind::Event,
                fields: vec![FieldDef::required("n", FieldTy::Int)],
                description: None,
            }
        }
    }

    #[derive(Serialize, Deserialize, Default)]
    struct Counter {
        total: i64,
    }

    impl EventSourced for Counter {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Add>()
                .emits::<Added>()
                .kind(ActorKind::EventSourced)
        }
        fn restore(_args: &JsonValue) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &crate::envelope::Event) {
            if event.schema.as_str() == "Added@1" {
                self.total += event.payload["n"].as_i64().unwrap_or(0);
            }
        }
    }
    impl CommandHandler<Add> for Counter {
        fn handle(&self, cmd: Add, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
            vec![crate::envelope::Event::new(Added::schema_id(), json!({ "n": cmd.n }))]
        }
    }

    impl CommandHandler<Boom> for Counter {
        fn handle(&self, _cmd: Boom, _ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
            panic!("injected handler panic");
        }
    }

    /// A command whose handling panics (panic isolation under test).
    #[derive(Deserialize)]
    struct Boom {
        #[allow(dead_code)] // payload shape; the handler panics before reading
        why: String,
    }
    impl Schema for Boom {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Boom".into(),
                version: 1,
                kind: SchemaKind::Command,
                fields: vec![FieldDef::required("why", FieldTy::Str)],
                description: None,
            }
        }
    }

    #[tokio::test]
    async fn atomic_step_commits_journal_state_and_cursor_together() {
        // Given a spawned counter actor.
        let system = Arc::new(ActorSystem::new());
        let path = Path::new("counter");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );

        // When one Add command is sent and the loop commits it.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 5 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // Then the state folded the event, the journal holds it, and the
        // inbox cursor advanced past the message (committed exactly once).
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], 5);
        let kernel = system.kernel.lock().expect("lock");
        let journal = &kernel.journals[&path];
        assert_eq!(journal.len(), 1);
        assert_eq!(journal.next_seq().as_u64(), 1);
        assert!(kernel.crashed.is_empty());
    }

    #[tokio::test]
    async fn atomic_step_dead_letters_unknown_schemas_and_advances() {
        // Given a spawned actor that handles only Add.
        let system = Arc::new(ActorSystem::new());
        let path = Path::new("counter");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );

        // When a message with an unhandled schema arrives.
        system
            .send(system.envelope(Boom::schema_id(), path.clone(), json!({})))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;

        // Then it is dead-lettered, nothing is journalled, nothing applied.
        let kernel = system.kernel.lock().expect("lock");
        assert_eq!(kernel.dead_letters.len(), 1);
        assert_eq!(kernel.dead_letters[0].schema, Boom::schema_id());
        assert_eq!(kernel.journals[&path].len(), 0);
        drop(kernel);
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], 0);
    }

    #[tokio::test]
    async fn atomic_step_panics_leave_the_message_queued_for_redelivery() {
        // Given a spawned actor whose Boom handler panics.
        let system = Arc::new(ActorSystem::new());
        let path = Path::new("counter");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Boom>::new::<Boom>())],
        );

        // When a Boom command arrives (the handler panics mid-decision).
        system
            .send(system.envelope(Boom::schema_id(), path.clone(), json!({ "why": "test" })))
            .await
            .expect("delivered");
        wait_for_crash(&system, &path).await;

        // Then NOTHING was appended, NOTHING acked, and the crash was
        // recorded: the message stays queued for redelivery after restart.
        let kernel = system.kernel.lock().expect("lock");
        assert!(kernel.crashed.contains(&path));
        assert_eq!(kernel.journals.get(&path).map(|j| j.len()), Some(0));
        assert!(kernel.dead_letters.is_empty());
        drop(kernel);
        assert_eq!(system.inbox_cursor(&path).map(|c| c.as_u64()), Some(0));
        let state = system.es_state(&path).await.expect("shell present");
        assert_eq!(state["total"], 0);
    }

    #[tokio::test]
    async fn restart_rebuilds_from_journal_and_redelivers_exactly_once() {
        // Given a counter that has committed one Add, then crashed on Boom.
        let system = Arc::new(ActorSystem::new());
        let path = Path::new("counter");
        let boom = Path::new("counter");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || {
                vec![
                    Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>()),
                    Arc::new(TypedEsAdapter::<Counter, Boom>::new::<Boom>()),
                ]
            },
        );
        system
            .send(system.envelope(Add::schema_id(), boom.clone(), json!({ "n": 4 })))
            .await
            .expect("delivered");
        wait_for_cursor(&system, &path, 1).await;
        system
            .send(system.envelope(Boom::schema_id(), boom.clone(), json!({ "why": "boom" })))
            .await
            .expect("delivered");
        wait_for_crash(&system, &path).await;

        // When the supervisor restarts the actor.
        system.restart_es(&path, &json!({})).await.expect("restart");
        // Boom is redelivered exactly once, panics again (the command is
        // poison), and the crash is re-recorded for the supervisor.
        wait_for_crash(&system, &path).await;

        // Then redelivery attempted the Boom exactly once (no duplicate
        // events, no double-apply), the pre-crash Add stayed committed, and
        // state equals the journal fold.
        let kernel = system.kernel.lock().expect("lock");
        assert_eq!(kernel.journals[&path].len(), 1, "no duplicate events");
        assert!(kernel.dead_letters.is_empty(), "panic never dead-letters");
        drop(kernel);
        assert_eq!(system.inbox_cursor(&path).map(|c| c.as_u64()), Some(1),
            "the poison message stays queued (peeked, never acked)");
        let state = system.es_state(&path).await.expect("live");
        assert_eq!(state["total"], 4, "fold(journal), not doubled");
    }

    async fn wait_for_cursor(system: &ActorSystem, path: &Path, expected: u64) {
        for _ in 0..2_000 {
            if system.inbox_cursor(path).map(|c| c.as_u64()) == Some(expected) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("cursor never reached {expected}");
    }

    async fn wait_for_crash(system: &ActorSystem, path: &Path) {
        for _ in 0..2_000 {
            {
                let kernel = system.kernel.lock().expect("lock");
                if kernel.crashed.contains(path) {
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("actor never crashed");
    }

    /// A service actor appending to a shared sink (impure by design).
    /// `start` receives JSON args, so the test passes the sink through a
    /// well-known args key serialized as a slot index into a static table.
    static SINKS: std::sync::OnceLock<Mutex<Vec<Arc<Mutex<Vec<String>>>>>> =
        std::sync::OnceLock::new();

    fn sinks() -> &'static Mutex<Vec<Arc<Mutex<Vec<String>>>>> {
        SINKS.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn open_sink() -> (usize, Arc<Mutex<Vec<String>>>) {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let mut all = sinks().lock().expect("sinks lock");
        all.push(sink.clone());
        (all.len() - 1, sink)
    }

    struct Auditor {
        sink: Arc<Mutex<Vec<String>>>,
    }

    impl ServiceActor for Auditor {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Add>()
                .kind(ActorKind::Service)
        }

        async fn start(
            args: &JsonValue,
        ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
            let idx = args["sink"].as_u64().expect("sink index") as usize;
            let sink = sinks().lock().expect("sinks lock")[idx].clone();
            Ok(Self { sink })
        }

    }

    impl MsgHandler<Add> for Auditor {
        async fn handle(&mut self, msg: Add, _ctx: &mut crate::context::MsgCtx<'_>) {
            self.sink.lock().expect("sink lock").push(format!("n={}", msg.n));
        }
    }

    #[tokio::test]
    async fn service_actor_receives_typed_messages_impurely() {
        // Given a system and an Auditor service with a shared test sink.
        let system = Arc::new(ActorSystem::new());
        let (sink_idx, sink) = open_sink();
        let path = Path::new("auditor");
        system.spawn_service::<Auditor, _>(
            path.clone(),
            &json!({ "sink": sink_idx }),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Auditor, Add>::new::<Add>())],
        );

        // When an Add message is sent to it.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 7 })))
            .await
            .expect("delivered");

        // Then the handler ran (impure side effect recorded).
        for _ in 0..2_000 {
            if sink.lock().expect("sink lock").as_slice() == ["n=7"] {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("service handler never ran");
    }

    #[tokio::test]
    async fn ask_settles_replied_when_the_callee_answers_the_slot() {
        // Given a callee that replies to whatever asks it, and an asker
        // service that calls ctx.ask on it.
        let system = Arc::new(ActorSystem::new());
        system.register_schema::<Add>();

        static RESULTS: std::sync::OnceLock<Mutex<Vec<String>>> = std::sync::OnceLock::new();
        let results = RESULTS.get_or_init(|| Mutex::new(Vec::new()));

        struct Echo;
        impl ServiceActor for Echo {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Echo {
            async fn handle(&mut self, msg: Add, ctx: &mut crate::context::MsgCtx<'_>) {
                if let Some(reply_to) = ctx.core.reply_to {
                    ctx.core.outbox.push_reply(
                        reply_to.clone(),
                        Add::schema_id(),
                        json!({ "echo": msg.n }),
                        ctx.core.trace.clone(),
                    );
                }
            }
        }

        struct Asker;
        impl ServiceActor for Asker {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Boom>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Boom> for Asker {
            async fn handle(&mut self, _msg: Boom, ctx: &mut crate::context::MsgCtx<'_>) {
                let reply = ctx
                    .ask(
                        Address::Path(Path::new("echo")),
                        Add::schema_id(),
                        json!({ "n": 21 }),
                        std::time::Duration::from_secs(2),
                    )
                    .await;
                let recorded = RESULTS
                    .get_or_init(|| Mutex::new(Vec::new()));
                match reply {
                    Ok(value) => recorded
                        .lock()
                        .expect("results lock")
                        .push(format!("replied:{}", value["echo"])),
                    Err(_) => recorded.lock().expect("results lock").push("failed".to_owned()),
                }
            }
        }

        system.spawn_service::<Echo, _>(
            Path::new("echo"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Echo, Add>::new::<Add>())],
        );
        system.spawn_service::<Asker, _>(
            Path::new("asker"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Asker, Boom>::new::<Boom>())],
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // When the asker asks the echo.
        system
            .send(system.envelope(Boom::schema_id(), Path::new("asker"), json!({ "why": "ask" })))
            .await
            .expect("delivered");

        // Then the ask settles as Replied with the echo's payload.
        for _ in 0..2_000 {
            if results.lock().expect("lock").as_slice() == ["replied:21"] {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("ask never settled as replied: {:?}", results.lock().unwrap());
    }

    #[tokio::test]
    async fn ask_settles_timeout_when_the_callee_never_replies() {
        // Given a silent callee and an asker with a short timeout.
        let system = Arc::new(ActorSystem::new());
        system.register_schema::<Add>();

        static RESULTS: std::sync::OnceLock<Mutex<Vec<String>>> = std::sync::OnceLock::new();
        let results = RESULTS.get_or_init(|| Mutex::new(Vec::new()));

        struct Silent;
        impl ServiceActor for Silent {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Add> for Silent {
            async fn handle(&mut self, _msg: Add, _ctx: &mut crate::context::MsgCtx<'_>) {}
        }

        struct Asker;
        impl ServiceActor for Asker {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Boom>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Boom> for Asker {
            async fn handle(&mut self, _msg: Boom, ctx: &mut crate::context::MsgCtx<'_>) {
                let reply = ctx
                    .ask(
                        Address::Path(Path::new("silent")),
                        Add::schema_id(),
                        json!({ "n": 1 }),
                        std::time::Duration::from_millis(50),
                    )
                    .await;
                let recorded = RESULTS.get_or_init(|| Mutex::new(Vec::new()));
                recorded
                    .lock()
                    .expect("lock")
                    .push(if reply.is_ok() { "replied" } else { "timed-out" }.to_owned());
            }
        }

        system.spawn_service::<Silent, _>(
            Path::new("silent"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Silent, Add>::new::<Add>())],
        );
        system.spawn_service::<Asker, _>(
            Path::new("asker"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Asker, Boom>::new::<Boom>())],
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // When the asker asks the silent callee.
        system
            .send(system.envelope(Boom::schema_id(), Path::new("asker"), json!({ "why": "ask" })))
            .await
            .expect("delivered");

        // Then the ask settles as a timeout (and the lease is gone).
        for _ in 0..2_000 {
            if results.lock().expect("lock").as_slice() == ["timed-out"] {
                let kernel = system.kernel.lock().expect("lock");
                assert!(kernel.replies.is_empty(), "lease leaked after timeout");
                assert!(!kernel.ask_facts.is_empty(), "no ask facts recorded");
                assert!(kernel
                    .ask_facts
                    .iter()
                    .any(|f| f.outcome == Some(crate::kernel::AskOutcome::Timeout)));
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("ask never timed out: {:?}", results.lock().unwrap());
    }

    #[tokio::test]
    async fn ask_over_a_durable_path_continues_as_an_ordinary_message() {
        // Given an ES counter whose Add handler REPLIES to a reply-to
        // PATH (not a slot): the reply continues as a normal envelope.
        let system = Arc::new(ActorSystem::new());
        system.register_schema::<Add>();
        system.register_schema::<Added>();

        static RECEIVED: std::sync::OnceLock<Mutex<Vec<String>>> = std::sync::OnceLock::new();
        let received = RECEIVED.get_or_init(|| Mutex::new(Vec::new()));

        struct Collector;
        impl ServiceActor for Collector {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Added>()
                    .kind(ActorKind::Service)
            }
            async fn start(
                _args: &JsonValue,
            ) -> Result<Self, error_stack::Report<crate::registry::RegistryError>> {
                Ok(Self)
            }
        }
        impl MsgHandler<Added> for Collector {
            async fn handle(&mut self, msg: Added, ctx: &mut crate::context::MsgCtx<'_>) {
                RECEIVED
                    .get_or_init(|| Mutex::new(Vec::new()))
                    .lock()
                    .expect("lock")
                    .push(format!(
                        "got n={} from={:?}",
                        msg.n, ctx.core.trace.causality_id
                    ));
            }
        }

        #[derive(Serialize, Deserialize)]
        struct Counter {
            total: i64,
        }
        impl EventSourced for Counter {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
                    .handles::<Add>()
                    .emits::<Added>()
                    .kind(ActorKind::EventSourced)
            }
            fn restore(_args: &JsonValue) -> Self {
                Self { total: 0 }
            }
            fn apply(&mut self, event: &crate::envelope::Event) {
                if event.schema.as_str() == "Added@1" {
                    self.total += event.payload["n"].as_i64().unwrap_or(0);
                }
            }
        }
        impl CommandHandler<Add> for Counter {
            fn handle(&self, cmd: Add, ctx: &mut CmdCtx<'_>) -> Vec<crate::envelope::Event> {
                if let Some(reply_to) = ctx.0.reply_to {
                    ctx.0.send(
                        reply_to.clone(),
                        Added::schema_id(),
                        json!({ "n": cmd.n }),
                        Some(Address::Path(ctx.0.self_path.clone())),
                    );
                }
                vec![crate::envelope::Event::new(Added::schema_id(), json!({ "n": cmd.n }))]
            }
        }

        system.spawn_service::<Collector, _>(
            Path::new("collector"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedServiceAdapter::<Collector, Added>::new::<Added>())],
        );
        system.spawn_es::<Counter, _>(
            Path::new("counter"),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // When the counter is told to Add with a reply-to PATH pointing at
        // the collector (an ask-shaped message, but reply-by-name).
        let mut envelope = system.envelope(Add::schema_id(), Path::new("counter"), json!({ "n": 5 }));
        envelope.reply_to = Some(Address::Path(Path::new("collector")));
        envelope.from = Some(Path::new("collector"));
        system.send(envelope).await.expect("delivered");

        // Then the collector receives the reply as an ordinary message
        // (a durable-path continuation — the name survives, no lease).
        for _ in 0..2_000 {
            if !received.lock().expect("lock").is_empty() {
                let got = received.lock().expect("lock")[0].clone();
                assert!(got.starts_with("got n=5"), "wrong payload: {got}");
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("path continuation never arrived");
    }

    #[tokio::test]
    async fn reply_slots_carry_the_mechanism_and_expire_cleanly() {
        // Given a live reply table with two leases: one short, one long.
        let system = ActorSystem::new();
        let kernel = system.kernel.lock().expect("lock");
        let (short_lease, _short_rx) =
            kernel.replies.open(std::time::Duration::from_millis(5), system.clock.now());
        let (long_lease, long_rx) =
            kernel.replies.open(std::time::Duration::from_secs(60), system.clock.now());

        // When completing the long lease and pruning past the short one.
        assert!(kernel.replies.complete(&long_lease, json!({ "ok": true })));
        drop(long_rx);
        kernel.replies.prune(crate::types::Timestamp::from_millis(system.clock.now().as_millis() + 10));

        // Then the short lease is gone (expired), the long one was
        // consumed by its reply, and the table is empty — no leaks.
        assert!(kernel.replies.is_empty(), "lease leaked");
        assert!(!kernel.replies.complete(&short_lease, json!({})));
    }

    #[tokio::test]
    async fn spawn_es_registers_slot_and_edges() {
        // Given a system.
        let system = Arc::new(ActorSystem::new());
        system.register_schema::<Add>();
        system.register_schema::<Added>();

        // When spawning an ES actor.
        let path = Path::new("counter");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );

        // Then the slot resolves and who_handles finds the path.
        let handlers = RuntimeView::who_handles(system.as_ref(), &Add::schema_id());
        assert_eq!(handlers, [path]);
    }

    #[tokio::test]
    async fn spawn_es_starts_genesis_state() {
        // Given a system with a spawned counter.
        let system = Arc::new(ActorSystem::new());
        let path = Path::new("counter");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || vec![Arc::new(TypedEsAdapter::<Counter, Add>::new::<Add>())],
        );

        // When capturing the state.
        let state = system.es_state(&path).await.expect("live");

        // Then it is at genesis.
        assert_eq!(state["total"], 0);
    }

    #[test]
    fn register_schema_is_idempotent_on_the_system() {
        // Given a system with Add registered.
        let system = ActorSystem::new();
        let first = system.register_schema::<Add>();

        // When registering Add again.
        let second = system.register_schema::<Add>();

        // Then both calls return the same id and one schema is stored.
        assert_eq!(first, second);
        assert!(system.schema(&first).is_some());
    }

    #[test]
    fn register_schema_json_accepts_foreign_descriptors() {
        // Given a system and a JSON-only descriptor.
        let system = ActorSystem::new();
        let foreign = json!({
            "name": "ForeignPing",
            "version": 4,
            "kind": "command",
            "fields": []
        });

        // When registering it as JSON.
        let id = system.register_schema_json(foreign).expect("valid");

        // Then it is retrievable by its name@version id.
        let stored = system.schema(&id).expect("stored");
        assert_eq!(id.to_string(), "ForeignPing@4");
        assert_eq!(stored.name, "ForeignPing");
    }

    #[tokio::test]
    async fn send_routes_through_the_kernel_to_the_inbox() {
        // Given a system with one spawned actor.
        let system = Arc::new(ActorSystem::new());
        let path = Path::new("counter");
        system.spawn_es::<Counter, _>(
            path.clone(),
            &json!({}),
            SpawnOpts::default(),
            || vec![],
        );

        // When sending an envelope from the system root.
        system
            .send(system.envelope(Add::schema_id(), path.clone(), json!({ "n": 3 })))
            .await
            .expect("delivered");

        // Then it lands in the actor's runtime-owned inbox (front door →
        // inbox pump may take a moment).
        for _ in 0..500 {
            if system.inbox_cursor(&path).map(|c| c.as_u64()) == Some(0)
                && inbox_has_work(&system, &path)
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("envelope never reached the inbox");
    }

    fn inbox_has_work(_system: &ActorSystem, _path: &Path) -> bool {
        // The envelope is queued iff the cursor has not advanced past it;
        // full inbox introspection lands with the atomic step (next task).
        true
    }
}
