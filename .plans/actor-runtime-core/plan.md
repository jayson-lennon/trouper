# Actor Runtime Core — Context-Rich Specification

Status: APPROVED PLAN (Socratic dialogue complete; all architectural questions settled).
This document is the authoritative implementation reference. A fresh agent must be able to implement from this document alone.

---

## Problem

We need a single-machine actor runtime whose *product* is the communication fabric a future canvas will render: a runtime-level schema registry, envelope/trace metadata, a tap stream of facts, journal-backed event-sourced actors, topics with per-subscriber cursors, declarative supervision, and dynamic add/remove of actors — with message schemas definable at runtime (not only in Rust) so external scripts/ports can join later. Kameo (cloned at `/mnt/zed/repos/third-party/kameo`, kept as reference only) was evaluated and rejected as a dependency: its core asset is static typed dispatch (`Message<T>`, `ActorRef<A>`, typed mailboxes), which we would have to erase at every joint; its supervised restart reuses the mailbox receiver but loses the in-flight message and offers no journal, no registry-as-kernel, no tap, no dynamic schemas.

## Solution

A small dynamic actor runtime on raw tokio (no kameo dependency):

- **Registry-as-kernel** (not an actor): paths → swappable endpoint slots, schema table, schema → handler-paths, topic → subscribers-with-cursors. Actor identity is its registered path; handles survive restarts because slots are swapped, not invalidated.
- **Runtime-internal envelopes** with trace context (`trace_id`, `causality_id`); users never construct envelopes.
- **Two-tier actor contract**:
  - `EventSourced` — pure decision functions: sync `handle(&self, cmd, ctx) -> Vec<Event>`, single `apply(&mut self, &Event)` used for BOTH live application and replay; seq-anchored in-memory journals holding `Event | Snapshot` entries; redelivery-on-restart from the inbox ack cursor; serde-bounded state with default `capture`/`restore_from` (override = sanctioned cache-hydration hook).
  - `ServiceActor` — impure by design: async handlers, I/O allowed, `ctx.ask` with mandatory timeout. Topic/edge consumers, port stand-ins, auditors.
- **JSON at the waist**: payloads cross the runtime boundary as `serde_json::Value`; typed Rust actors get generic adapters that erase/restore exactly once at spawn. Foreign (no-Rust-type) schemas register via JSON descriptors and land in the same tables.
- **Tap**: global in-memory ring of `Fact`s emitted at every waist crossing; subscribers hold offsets. JSON projection only at the tap boundary.
- **Declarative supervision**: restart policy + intensity budget + backoff as data, interpreted by one generic supervisor; escalation events are facts.
- `system.export()` produces the artifact the future canvas consumes (schemas, actors, manifests, declared vs observed edges, live ES state).

---

## Dialectical Outcomes (Why)

Decisions settled during the dialogue, with rejected alternatives:

1. **Own core, not kameo.** Round 1 recommended wrapping kameo (A); Round 2 flipped to owning a thin core after the user chose (a) journal-backed inboxes (log + offset semantics) and (b) runtime-registered schemas. Verdict from source inspection: wrapping means bypassing kameo's mailboxes (our journal replaces them), two queues per actor (kameo's signal channel + our inbox) with FIFO reordering hazards, and erasing its typed dispatch — its main value. Supervision (~2k lines in kameo) is re-implementable for one machine in a few hundred lines: policy, not mechanism. Rejected: fork kameo (own 18k lines + upstream drift); raw-tokio wrap of kameo (adapter gymnastics forever).
2. **Envelope is runtime-internal, not a user-facing struct.** Round 1 Q2 option B (literal `Envelope { payload: Box<dyn Any> }` in user land) rejected because it fights typed handlers. The runtime assembles envelope metadata at the send boundary; handlers see typed payloads + `ctx`.
3. **"Persist names, never mechanisms"** (from the source chat): reply slots/oneshot channels exist only inside the runtime; the log/tap records `causality` links and `reply_to` *as a path* only. A replayed log never contains a channel.
4. **Purity contract, physically enforced.** `EventSourced::handle` is sync and takes `&self` → no await (no ask/IO), no mutation outside `apply`. Deferred-effects outbox: `ctx.send/publish/reply` record intents during the handler; the runtime performs them AFTER journal-append + ack, so crash-redelivery never duplicates sends. `apply` is the single mutation site for live AND replay (replay must never re-run `handle`, never re-publish — events are facts that already happened; only commands are redelivered).
5. **`ask` only for ServiceActors.** An event-sourced handler is pure and cannot await. Anyone may send an "ask-shaped" command to an ES actor (envelope carries `reply_to`); the ES actor answers via `ctx.reply(...)` (reply-as-message continuation). Inline-await ask is reserved for service actors; timeouts are mandatory and produce facts.
6. **Journal + snapshot contract.** Snapshots are memoized fold output taken only BETWEEN messages (never mid-step), stored as journal entries (single container, trivial compaction later — rejected side-store). Restart: last snapshot fast-path else genesis `restore(args)`, then `apply` the tail. Inbox cursor and journal seq are independent axes. Snapshot capability via `EventSourced: Serialize + DeserializeOwned` supertraits with default `capture`/`restore_from` bodies (rolled into the trait per user decision; separate `Snapshotting` trait rejected). `restore_from(snap)` takes NO args — a snapshot is self-sufficient; args only matter at genesis. `#[serde(skip)]` caches are legitimate: override `restore_from` to hydrate (explicitly settled — no "plain-data rule", the override IS the hook). Default snapshot policy: OFF; `every_n(n)` opt-in per spawn.
7. **Dynamic waist, typed sugar.** The kernel stores only object-safe entries (`schema_id → dispatch closure over JSON`); `CommandHandler<C>`/`MsgHandler<M>` are generic adapters registered once per (actor, message) pair at spawn, decoding JSON → C at the actor edge. Foreign actors implement the erased twin directly. This preserves "arbitrary schemas defined outside Rust" while keeping typed ergonomics inside actors.
8. **Registry is kernel, not an actor** (from the source chat): bootstrap paradox, must never deadlock, must survive actor restarts. `ctx.lookup/who_handles/subscribe` are syscalls.
9. **Ports deferred, seam preserved.** External-process actors will later be ordinary actors reusing the JSON schema + manifest path; nothing in the core may assume "payload came from a Rust type."
10. **Tap ≠ delivery.** Delivery uses per-actor bounded inbox rings; the tap is a global observation ring that MAY drop under pressure. Never funnel delivery through the tap.
11. **Rust conventions** (project skill, binding on this codebase): aggressive newtypes for all IDs/counters with distinct semantics; `wherror::Error` + `error_stack::Report` with colocated error types; service traits + DI for anything external (notably the clock — needed for deterministic tests); context structs for subsystem capability bundles (`ActorContext`); one loop per function; BDD-style tests (Given/When/Then), one behavior per test, `rstest` for parameterized cases.

## Relevant Files (Where)

Workspace root `/mnt/zed/repos/actor-canvas` currently has: root `Cargo.toml` (package `actor-canvas`, empty deps), `src/lib.rs` (empty), empty `crates/` dir. Restructure to:

```
Cargo.toml                      # root: [workspace] members = ["crates/*"]; root package stays, depends on runtime
src/lib.rs                      # thin re-export of actor_runtime (umbrella crate)
crates/actor-runtime/
  Cargo.toml                    # name = "actor-runtime"
  src/
    lib.rs                      # module decls + prelude
    types.rs                    # newtypes: Path, Topic, SchemaId, TraceId, CausalityId, TraceCtx, SeqNo, InboxOffset, LeaseId, ActorKind, StopReason
    envelope.rs                 # Payload, Address, Envelope, Event
    schema.rs                   # Schema trait, SchemaDef, field descriptors, SchemaError
    registry.rs                 # Registry (slots, schemas, handlers, topics), RegistryError
    journal.rs                  # JournalEntry, Journal, JournalError
    inbox.rs                    # Inbox ring, OverloadPolicy, InboxError
    actor.rs                    # EventSourced, CommandHandler, ServiceActor, MsgHandler, ActorManifest, adapters (TypedEsAdapter, TypedServiceAdapter, ForeignEsAdapter)
    context.rs                  # ActorContext, CmdCtx, MsgCtx, Outbox
    kernel.rs                   # dispatch loops (es_actor_loop, service_actor_loop), atomic step, restart, spawn/stop plumbing
    topics.rs                   # TopicLog, cursor pumps, TopicError
    tap.rs                      # Fact, FactKind, TapRing, TapHandle, TapError
    supervision.rs              # RestartPolicy, RestartBudget, ChildSpec, Backoff, supervisor logic, SupervisionError
    system.rs                   # ActorSystem facade, SystemConfig, ExportTypes (SystemExport, ActorExport, Edge), export()
    clock.rs                    # ClockBackend trait, SystemClock, FakeClock (test)
  tests/                        # integration tests per test-case table (or #[cfg(test)] modules colocated)
```

Also: replace root `Cargo.toml` `[dependencies]` with workspace deps; root `src/lib.rs` re-exports. Demo lives in root package (`examples/demo.rs` or `tests/demo.rs`).

## Key Code Context (What)

Existing state of the repo (everything else is new):

```toml
# Cargo.toml (root, current)
[package]
name = "actor-canvas"
version = "0.1.0"
edition = "2024"

[dependencies]
```

`src/lib.rs` is empty; `crates/` is empty. Edition 2024 (root already set; match in member crates).

### The central traits (new code — the contract every actor will implement)

```rust
/// Event-sourced domain actor: pure, journaled, replayable.
pub trait EventSourced: Send + Sync + Serialize + DeserializeOwned + 'static {
    fn manifest() -> ActorManifest;
    /// Genesis state — fresh instance (used when no snapshot exists).
    fn restore(args: ActorArgs) -> Self;
    /// THE mutation. Live application AND replay — single code path.
    fn apply(&mut self, ev: &Event);
    /// Snapshot seam. Defaults = state IS the snapshot.
    /// Override to trim fields or hydrate `#[serde(skip)]` caches after decode.
    fn capture(&self) -> Result<JsonValue, Report<JournalError>> {
        serde_json::to_value(self).change_context(JournalError::Snapshot)
    }
    fn restore_from(snap: JsonValue) -> Result<Self, Report<JournalError>> {
        serde_json::from_value(snap).change_context(JournalError::Restore)
    }
}

/// Typed sugar over the erased dispatch table. Registered once per (Actor, Cmd) at spawn.
pub trait CommandHandler<C>: EventSourced {
    fn handle(&self, cmd: C, ctx: &mut CmdCtx) -> Vec<Event>;
}

/// Edge/service actor: impure by design; async, I/O and ask allowed; NOT journaled.
pub trait ServiceActor: Send + 'static {
    fn manifest() -> ActorManifest;
    fn start(args: ActorArgs) -> impl Future<Output = Result<Self, Report<RegistryError>>> + Send;
}
pub trait MsgHandler<M>: ServiceActor {
    fn handle(&mut self, msg: M, ctx: &mut MsgCtx) -> impl Future<Output = ()> + Send;
}
```

(`ServiceActor`/`MsgHandler` are likewise wrapped by a `DynServiceActor` shell + `MsgEntry` dispatch object at spawn — same pattern as the ES tier below.)

### Kernel-side erased dispatch (object-safe; what the registry actually stores)

Because `EventSourced` has serde supertraits it is NOT dyn-compatible — the kernel cannot hold `dyn EventSourced`. Instead, spawn wraps the typed actor in an object-safe shell; the kernel talks only to shells and entries:

```rust
/// Object-safe runtime shell around a typed ES actor. One per spawned actor instance.
pub trait DynEsActor: Send {
    fn apply_erased(&mut self, ev: &Event);                                  // forwards to A::apply
    fn capture_erased(&self) -> Result<JsonValue, Report<JournalError>>;     // forwards to A::capture
    /// Restore a fresh instance from a snapshot / genesis args.
    /// Returns Box so the poisoned instance is dropped, not reused.
    fn rebuild(&self, snapshot: Option<JsonValue>, args: &ActorArgs)
        -> Result<Box<dyn DynEsActor>, Report<JournalError>>;
}

pub trait CommandEntry: Send + Sync {
    fn schema(&self) -> SchemaId;
    /// Decode JSON → C, call the typed handler against the shell's state.
    fn dispatch(&self, actor: &mut dyn DynEsActor, cmd: JsonValue, ctx: &mut CmdCtx)
        -> Result<Vec<Event>, Report<DispatchError>>;
}
```

`TypedEsShell<A: EventSourced>` implements `DynEsActor` (holding `state: A`); `TypedEsAdapter<A, C>` implements `CommandEntry` (decode → `C::handle(state_ref, cmd, ctx)`). Foreign actors implement `DynEsActor` + `CommandEntry` directly — their state is `JsonValue` already, so `capture` is a clone and `rebuild` restores from JSON. Both land in the same registry tables. The adapter generics erase exactly once at spawn; the kernel never sees `C`. `serde_json::from_value::<C>` failures are `DispatchError::Decode` → dead-letter, not panic.

### Events and journal entries

```rust
pub struct Event { pub schema: SchemaId, pub payload: JsonValue }   // a fact that happened

pub enum JournalEntry {
    Event    { seq: SeqNo, event: Event },
    Snapshot { seq: SeqNo, state: JsonValue },
}
```

### Envelope + durable fact shapes

```rust
pub struct Envelope {
    pub schema: SchemaId,
    pub dest: Address,
    pub from: Option<Path>,          // logical name
    pub reply_to: Option<Address>,   // Path (durable) or Slot(LeaseId) (mechanism)
    pub trace: TraceCtx,             // trace_id + causality_id
    pub payload: Payload,            // Typed(Box<dyn Any + Send>) in-proc | Json(JsonValue) at waist
}
pub enum Address { Path(Path), Topic(Topic), Slot(LeaseId) }

pub enum JournalEntry {
    Event    { seq: SeqNo, schema: SchemaId, payload: JsonValue },
    Snapshot { seq: SeqNo, state: JsonValue },
}

pub struct Fact { pub ts: Timestamp, pub kind: FactKind }
pub enum FactKind {
    Sent { from: Option<Path>, to: Address, schema: SchemaId, trace: TraceCtx },
    Delivered { to: Path, schema: SchemaId, trace: TraceCtx },
    Acked { to: Path, trace: TraceCtx },
    AskOpened { from: Path, to: Address, trace: TraceCtx },
    AskSettled { outcome: AskOutcome, trace: TraceCtx },          // Replied | Timeout | Failed
    Spawned { path: Path, kind: ActorKind, restart: bool },
    Stopped { path: Path, reason: StopReason },
    Failed { path: Path, error: String },
    TopicPublished { topic: Topic, schema: SchemaId, trace: TraceCtx },
    SnapshotTaken { path: Path, seq: SeqNo },
    Escalated { path: Path, reason: String },
    DeadLettered { to: Address, schema: SchemaId, reason: DeadLetterReason },
}
```

### Handler-facing context (the syscall surface — see project skill: context-struct pattern)

```rust
pub struct CmdCtx<'a> {   // ES actors: sync, no ask, no I/O
    pub self_path: &'a Path,
    pub trace: &'a TraceCtx,           // metadata of the message being processed
    pub reply_to: Option<&'a Address>,
    // methods record intents into the outbox; performed post-ack:
    pub fn send(&mut self, dest: Address, payload: JsonValue, reply_to: Option<Address>);
    pub fn publish(&mut self, topic: Topic, payload: JsonValue);
    pub fn reply(&mut self, payload: JsonValue);      // tell to ctx.reply_to (if any)
    pub fn lookup(&self, path: &Path) -> Option<EndpointInfo>;
    pub fn who_handles(&self, schema: &SchemaId) -> Vec<Path>;
    pub fn recv_ts(&self) -> Timestamp;               // injected clock = deterministic
}
pub struct MsgCtx<'a> { /* superset; adds: */ 
    pub async fn ask(&mut self, dest: Address, payload: JsonValue, timeout: Duration) -> Result<JsonValue, Report<AskError>>;
    pub fn subscribe(&mut self, topic: Topic);
}
```

### The atomic step (heart of the system — exact ordering, ES path)

```rust
// NO user code runs after step 4; steps 5–10 are infallible kernel code, so the
// append+ack pair is atomic in practice (the panic window is step 4, inside catch_unwind).
1. env = inbox.peek(cursor)
2. decode payload (schema registry); unknown schema → DeadLettered, advance cursor
3. build CmdCtx { trace: env.trace, reply_to: env.reply_to, clock ts, outbox buffer }
4. events = catch_unwind(command_entry.dispatch(&mut shell, json, &mut ctx))
   - panic or Err → tap.Failed; NO append, NO ack, NO outbox flush → supervisor decides
5. journal.append(events)                    // seq per event
6. inbox.ack(cursor)
7. for ev in &events { shell.apply_erased(ev) }   // same fn replay uses
8. outbox.flush()                            // deferred sends/publishes/replies, causality-linked
9. fan out events to manifest's emits topics
10. tap.Acked; maybe snapshot (policy every_n, taken here — BETWEEN messages)
```

### Restart (ES)

```rust
// Journal and inbox are runtime-owned; crash destroys only the actor instance.
1. supervisor: policy check (budget N per window) → else tap.Escalated + stop
2. shell = match journal.last_snapshot() {
       Some(s) => shell.rebuild(Some(s.state), &args)?,   // fast path (hydration hook runs here)
       None    => shell.rebuild(None, &args)?,            // genesis: A::restore(args)
   }
3. for e in journal.after(snapshot_seq) { if let Event = e { shell.apply_erased(e) } }   // NEVER re-handle
4. swap slot endpoint to the new task; inbox cursor stays at acked + 1 → redeliver
   (identity = path; senders holding pre-crash handles never notice)
```

## Implementation Algorithm (How)

Phase-by-phase implementation logic:

### Phase 1 — Foundation
Convert root `Cargo.toml` to `[workspace] members = ["crates/*"]` (root package remains, edition 2024). Create `crates/actor-runtime` with the module skeleton. Define ALL newtypes in `types.rs` up front (project skill: aggressive newtypes): `Path(Arc<str>)`, `Topic(Arc<str>)`, `SchemaId(Arc<str>)` rendered `"name@version"`, `TraceId(Uuid)`/`CausalityId(Uuid)` (v7, time-ordered), `SeqNo(u64)`, `InboxOffset(u64)`, `LeaseId(Uuid)`, `Timestamp(u64 millis)`. Define `Envelope`, `Payload`, `Address`, `Event` (alias of JournalEntry::Event payload tuple), `TraceCtx`. JSON codec = `serde_json` directly (no abstraction yet). `clock.rs`: `ClockBackend` trait (`now_millis`), `SystemClock`, `FakeClock` (settable), following the service-trait pattern with an `Arc<dyn ClockBackend>` wrapper.

### Phase 2 — Schema registry
`schema.rs`: `Schema` trait for Rust types (`fn schema_def() -> SchemaDef`) — name, version, kind (Command/Event), field descriptors (`Vec<FieldDef { name, ty: FieldTy, unit: Option<..>, range: Option<..> }>`); `SchemaDef` is serde-serializable so foreign registration is `SchemaDef::from_json(...)`. Registry table: `schemas: HashMap<String, BTreeMap<u32, SchemaDef>>` keyed by name (version-sorted), lookup helpers `by_id`, `latest`. `ActorManifest::new(path).handles::<T>().emits::<T>().emits_on_topic(t).subscribes(t).kind(ActorKind::EventSourced|Service)` — manifest entries reference `SchemaId`s; both Rust-registered and JSON-registered schemas produce the same `SchemaId`s so declared edges are uniform. Registration happens at `ActorSystem::register_schema` (Rust: `T::schema_def()`; foreign: JSON) and is idempotent per name+version.

### Phase 3 — Delivery kernel
`inbox.rs`: bounded `VecDeque<Envelope>` + `OverloadPolicy { Block, DropNew, DropOld }` (default Block = backpressure), peek/ack by offset, push_front not needed (no priority). `registry.rs`: `slots: HashMap<Path, Slot>` where `Slot { manifest, endpoint: ArcSwapOption<Endpoint> }` (swap-on-restart; use `arc-swap` or a `RwLock<Option<_>>` — pick `arc-swap`), `handlers: HashMap<SchemaId, RoutePolicy>` where `RoutePolicy::Single(Path) | RoundRobin(Vec<Path>)`, `topics` table (filled in phase 6). `ActorContext` construction + outbox (`Vec<Intent>`; `Intent::{Send, Publish, Reply}` with pre-computed envelopes). `kernel.rs`: task per actor (`es_actor_loop`, `service_actor_loop` — ONE loop each per project skill, bodies factored into named step fns `process_next_es`, `flush_outbox`, `fan_out_events`). Send path: resolve `Address::Path` → slot → clone envelope into that inbox (respect policy; Block = `.send().await` on an internal mpsc front door); unresolvable → DLQ topic + `DeadLettered` fact. Trace: sends create `CausalityId` linked to current trace; system entry points start a fresh `TraceId`.

### Phase 4 — EventSourced tier
`journal.rs`: per-actor `Vec<JournalEntry>` (in-memory), `append_event`, `append_snapshot`, `last_snapshot`, `after(seq)`. `actor.rs`: the `EventSourced` + `CommandHandler` traits exactly as in Key Code Context; `TypedEsAdapter<A, C>` implementing `CommandEntry` (decode → typed handle → events). Spawn: `system.spawn_es::<A>(path, args, SpawnOpts { snapshot: SnapshotPolicy::Off | EveryN(u64), mailbox: (capacity, policy) })` — registers manifest edges, command entries per `C`, builds journal + inbox, starts loop. Atomic step per Key Code Context (preserve ordering EXACTLY; catch_unwind with `AssertUnwindSafe`, poison state must be discarded on panic — re-restore from journal on the supervisor path). Restart per Key Code Context algorithm.

### Phase 5 — ServiceActor tier
`TypedServiceAdapter<A, M>` implementing `MsgEntry` (async dispatch). Reply-slot table in registry: `LeaseId → (oneshot::Sender<JsonValue>, expires_at)`; `ctx.ask` = create lease + `AskOpened` fact + send envelope + `tokio::time::timeout` await + `AskSettled` fact (Timeout on timeout; lease pruned on expiry — a dead asker's slot must not leak). `ctx.reply` resolves `reply_to`: `Slot(id)` → oneshot (mechanism, dies with the ask), `Path(p)` → ordinary envelope (durable name). DLQ topic `system.deadletters` created at system boot.

### Phase 6 — Topics
`topics.rs`: `TopicLog { ring: VecDeque<(u64, Envelope)>, capacity }` + per-subscriber cursors `HashMap<Path, InboxOffset>`. One pump task per topic: wakes on publish, delivers new entries (offset > cursor) into subscriber inboxes as envelopes (topic-prefixed trace, causality linked to the publish), advances cursor ONLY on successful enqueue; a slow/blocked subscriber applies ITS inbox policy — pump never blocks forever (use try_send + policy). `reset_cursor(path, topic, to)` re-delivers from offset (record/replay seed). Removal of a subscriber cascades (phase 9).

### Phase 7 — Tap
`tap.rs`: `TapRing { ring: VecDeque<Fact>, capacity, drop: DropOldest }`, monotonically assigned fact offsets; `subscribe()` returns a `TapStream` holding an offset. Emit points (complete list): every send resolution (Sent), every hand-to-loop (Delivered), every ES ack (Acked), ask lifecycle (AskOpened/AskSettled), spawn/restart (Spawned{restart}), stop (Stopped), handler panic/error (Failed), publish (TopicPublished), snapshot (SnapshotTaken), escalation (Escalated), DLQ (DeadLettered). JSON projection: `impl From<&Fact> for JsonValue` — serialization ONLY here, never on the hot path.

### Phase 8 — Supervisor
`supervision.rs`: `ChildSpec { factory: BoxSpawnFn, restart_policy: RestartPolicy { Permanent | Transient | Never }, budget: RestartBudget { max: u32, window: Duration }, backoff: Backoff { base, max, factor } }` — pure data interpreted by kernel code registered via `system.spawn_child(parent_path, spec)` (registry stores parent→children + child→parent). On child failure: apply restart policy → check budget via sliding window → if allowed: backoff sleep → restart (Phase 4 algorithm, `Spawned{restart: true}` fact) → else: stop child, `Escalated` fact to parent's inbox as a control envelope. Graceful stop: `system.stop(path)` → signal drain (inbox accepts no new user envelopes) → await current message → stop children FIRST (recursive, timeout-bounded) → flush undelivered to DLQ → drop slot, cascade topic subscriptions, notify links (tap facts only — no user links in v1).

### Phase 9 — System control + export
`system.rs`: `ActorSystem::new(SystemConfig { clock: Arc<dyn ClockBackend>, tap_capacity, defaults })`; `spawn_es`, `spawn_service`, `spawn_child`, `stop`, `register_schema` (both flavors), `publish`, `send`, `export()`. Export: `SystemExport { schemas: Vec<SchemaDef>, actors: Vec<ActorExport { path, kind, manifest, state: Option<JsonValue> /* capture() for ES, per capability-not-policy */ }>, declared_edges: (from manifests), observed_edges: (aggregate tap: from→to schema counts) }` — serde-serializable; this is the canvas's future input.

### Phase 10 — Demo + tests
Demo (`examples/demo.rs` in root package): `Inventory` ES actor (ReserveStock/Restock commands; StockReserved/StockRejected events; fold), `AuditLog` service actor subscribing to `inventory.events` writing lines via ctx (in-memory sink), one foreign actor registered from pure JSON (no Rust type), exercising: add/remove, crash→restart→redelivery, snapshot fast path, topic re-consume, ask timeout, export printing declared vs observed edges.

## Phases (approved plan, expanded)

1. **Foundation** — workspace restructure; newtypes; envelope/event/fact types; JSON codec; clock service trait + fakes.
2. **Schema registry** — Schema trait + JSON descriptors; schema table; ActorManifest; dual registration path.
3. **Delivery kernel** — inbox rings; registry slots; dispatcher loops; ctx syscalls; outbox; send/tap plumbing; DLQ.
4. **EventSourced tier** — journal; traits + typed adapters; spawn with policies; atomic step; panic isolation; snapshot; restart.
5. **ServiceActor tier** — async actors; reply slots/leases; ctx.ask; AskSettled facts.
6. **Topics** — topic logs; per-subscriber cursors; pumps; cursor reset.
7. **Tap** — global fact ring; subscriptions; JSON projection at boundary only.
8. **Supervisor** — declarative child specs; budgets; backoff; escalation; graceful shutdown ordering.
9. **System control + export** — spawn/stop/remove API; cascade; export artifact.
10. **Demo + tests** — end-to-end demo; the full test-case table below.

## Acceptance Criteria

1. Actors are added/removed at runtime by path; removal cascades subscriptions and notifies links.
2. An ES actor that panics mid-message restarts: same path, pending inbox intact, state == fold(journal), failed message redelivered exactly once.
3. `EventSourced::handle` is sync and takes `&self` — no I/O or await possible; `apply` is the only mutation site, used for both live application and replay.
4. Restart restores from latest journal snapshot when present (`restore_from` + `apply` tail), else genesis; redelivery is independent of snapshots; `restore_from` override is the sanctioned cache-hydration hook.
5. Only service actors have `ctx.ask`; every ask has a timeout; timeouts appear as tap facts.
6. JSON-registered schemas (no Rust type) round-trip: register → send → handler decodes via schema id.
7. Topics deliver with independent per-subscriber cursors; reset re-delivers in order.
8. Every send/receive/ack/spawn/stop/failure emits a `Fact` with `trace_id`/`causality_id`; `system.export()` shows declared vs observed edges plus live ES state.
9. Graceful shutdown drains children before parents within timeouts.
10. No kameo dependency; tokio + serde only (plus dev-deps).

## Anti-Goals (Out of Scope)

- **No persistence to disk.** Journals, tap, topic logs are in-memory; whole-system restart is a cold start. (Later: tap dumper / WAL as subscribers + flush-policy knobs.)
- **No external-process port actor.** The JSON schema/manifest seam exists; the socket/stdio PortActor is a later phase.
- **No proc macros / derives.** Typed sugar is plain traits + generic adapters; `#[derive(ActorMsg)]` via linkme comes later over the same runtime contract.
- **No script tier (Lua/WASM).** Foreign actors are exercised via JSON descriptors in tests only.
- **No distributed anything**: no network transport, no libp2p/Zenoh/iceoryx2, no cluster.
- **No journal compaction** (only the `Snapshot` entry format that enables it later).
- **No canvas / rendering.** `export()` is the only deliverable toward it.
- **No kameo dependency**; kameo clone remains a reference only.
- **No selective receive / mailbox scanning** (FIFO per inbox; ReplyTo slots cover the use case).
- **No hot code swap.**

## Edge Cases & Gotchas

- **In-flight message vs journal double-apply**: events must be appended BEFORE ack; if a panic occurred between them, redelivery would double-apply. Avoided structurally: after the handler returns Ok, NO user code runs before append+ack complete (kernel-only, infallible steps) — preserve this invariant; anyone adding fallible/user-code steps between 5 and 6 breaks atomicity.
- **Panic safety**: `catch_unwind(AssertUnwindSafe(...))`; the actor state may be poisoned (partially borrowed) — on panic the supervisor path must REBUILD state from journal (or snapshot), never reuse the poisoned instance.
- **`apply` must not be called for the redelivered command's events twice**: the failed command produced NO events (nothing appended); acked commands' events are in the journal and are NOT re-produced (handle not re-run). Test explicitly.
- **Replay never re-publishes**: fan-out happens at flush time, not replay time. A restart must not duplicate topic deliveries.
- **Reply slot leaks**: leases carry expiry; prune on timeout AND on sweep; a dead asker's slot must not accumulate.
- **Topic pump blocking**: a slow subscriber must not stall the pump for others — pump uses try_send + per-subscriber inbox policy; cursor advances only on accepted delivery.
- **Cursor reset ≠ free**: re-consume re-invokes edge-actor handlers → duplicate side effects (at-least-once). Document; `ctx.trace_id()` in effects makes dedup possible.
- **Schema versioning**: `SchemaId` embeds version from day one (`name@version`); lookups by exact id, manifests pin versions; latest-version helper is convenience only.
- **Foreign actor without Rust types**: adapter must not require `DeserializeOwned` for state — foreign ES state is `JsonValue` already; the erased shell handles it (its `capture` is a clone).
- **Uuid v7 for trace/causality** keeps facts time-sortable for the future canvas; do not use v4.
- **ES state with `#[serde(skip)]`**: legal; restore hydrates via `restore_from` override. Do NOT add an `after_restore` hook (rejected in dialogue).
- **Loop discipline** (project skill): dispatcher loops = one loop per function, bodies factored into named step functions.
- **Errors** (project skill): `wherror::Error` + `error_stack::Report`, errors colocated per module, `# Errors` doc sections on public fallible fns. Handler panics are facts, not `Result`s.

## Navigation Anchors

- Entry point for the whole system: `crates/actor-runtime/src/system.rs` — `ActorSystem::new/spawn_es/spawn_service/spawn_child/stop/export`.
- The heart: `crates/actor-runtime/src/kernel.rs` — `es_actor_loop` / `service_actor_loop` + atomic step + restart.
- The contract: `crates/actor-runtime/src/actor.rs` — `EventSourced`, `CommandHandler`, `ServiceActor`, `MsgHandler`, adapters.
- The waist: `crates/actor-runtime/src/registry.rs` (slots/schemas/handlers/topics) + `schema.rs`.
- Reference (read-only, do NOT depend on): `/mnt/zed/repos/third-party/kameo` — `src/supervision.rs` (budget semantics), `src/links.rs` (drain/notify ordering), `src/console/registry.rs` (monitor/snapshot precedent).

## Dependency Mappings

New external deps for `crates/actor-runtime`:
- `tokio` (features: rt-multi-thread, sync, time, macros) — engine.
- `serde` (derive) + `serde_json` — schema defs, payloads, journals, snapshots, export.
- `uuid` (features: v7) — TraceId/CausalityId/LeaseId.
- `arc-swap` — swappable endpoint slots.
- `wherror` + `error-stack` — error handling per project skill.
- `derive_more` (features: debug) — service-wrapper Debug impls.
Dev-deps: `rstest`, `futures` (stream testing for tap).
Internal: root package `actor-canvas` depends on `actor-runtime` (path dep); root `src/lib.rs` re-exports; demo under root package.
No kameo. No tracing crate — the tap is the observability surface.

## Test Strategies

Per project skill: BDD Given/When/Then with `//` comments, one behavior per test, sentence names, `rstest` only for same-property cases, deterministic time via `FakeClock` (never `tokio::time` real sleeps in assertions — use `tokio::time::pause()` where needed). A deterministic test helper: `ActorSystem::test()` builder (FakeClock default, small tap ring) returning handles for driving + inspecting journals/tap.

Phase guidance (maps to the approved test-case table):
- Phase 1: unit tests for newtype Display/serialization round-trips; FakeClock determinism.
- Phase 2: `register_schema` from Rust and from JSON produce identical `SchemaId`s; version collision is idempotent; manifest edge extraction.
- Phase 3: bounded inbox policies (Block/DropNew/DropOld) each one test; send to removed path → DeadLettered fact; outbox flush ordering.
- Phase 4: `es_journal_and_fold`, `panic_redelivery` (inject panic via test command; assert no events for k, redelivered exactly once, k+1 queued), `snapshot_fast_path`, `snapshot_policy_off`, `restore_from_hydration` (`#[serde(skip)]` cache populated by override; fold-equivalence property via rstest over random k).
- Phase 5: `ask_timeout_is_fact` (FakeClock-advanced timeout; `AskSettled(Timeout)` fact; asker continues), `reply_to_continuation` (reply-to-path arrives as normal message; causality links pair), lease expiry pruning.
- Phase 6: `topic_cursor_reset`; independent cursors (subscriber A ahead of B); slow subscriber doesn't block others.
- Phase 7: `tap_causality_chain` (A→B→C: shared trace_id, chained causality_id); tap drop-oldest under pressure doesn't affect delivery.
- Phase 8: `restart_budget_escalation` (6 panics in 5s window, budget 5 → Escalated fact); `graceful_shutdown_order` (children drain before parent; undelivered → DLQ); Transient policy ignores normal exits.
- Phase 9: `subscription_cascade_on_remove`; `foreign_schema_roundtrip`; export contains declared vs observed edges + live ES state.
- Phase 10: demo runs the whole scenario green (also serves as the human-readable acceptance walkthrough).

## Record Updates (to be written to `.agents/RECORD.md` at END of implementation — not now)

Planned verbatim entries (verify against actual implementation before writing; if the implementation diverged, do NOT write a wrong entry — surface the divergence in the final summary instead):

- ADD: "All actor communication is mediated by the runtime: actors never hold channels directly; every send is routed by path or topic through the registry and emits a tap fact."
- ADD: "Event-sourced actors are pure decision functions (sync `handle(&self)` returning events) with a single `apply` used for both live state application and replay; all other actors may perform side effects and use `ask`."
- ADD: "The registry is kernel code, not an actor: path→endpoint slots, schema, type→handler, and topic→subscriber tables persist across actor restarts; actor identity is its registered path."
- ADD: "Message schemas are runtime data: Rust types and external JSON descriptors register into the same schema table; payloads cross the runtime boundary as JSON."
- ADD: "Event-sourced journals are in-memory, seq-anchored lists of `Event` and `Snapshot` entries; restart restores from the latest snapshot plus the tail, and command redelivery is independent of snapshots; journal persistence is deliberately out of scope."
- ADD: "External-process ports are planned as ordinary actors reusing the same schema/manifest tables (not yet implemented)."
