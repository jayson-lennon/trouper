# The Record

A curated list of factual, scoped statements asserting the application's **current** state. Authoritative for the present, never the future.

The planner consults this file before proposing a plan. If a feature **contradicts** an entry here, the contradiction is surfaced before the plan proceeds. If a feature **establishes a new high-level fact**, a verbatim entry is proposed for human approval as part of the plan.

## Format Rules

- **Factual.** Assert how things are _now_. Never future intent ("we will...", "should..."). Each entry is the current state of the application.
- **Scoped.** Name what each entry applies to — repo, app, frontend, or a named subsystem. An unscoped fact is ambiguous: is that the repo, or the app's supported VCS list? Always disambiguate.
- **High-level.** One-liners (a few sentences at most). Capture decisions and facts a planner needs, not implementation minutiae.
- **Single tag.** Each entry carries exactly one subsystem tag as a `(tag)` prefix: `- (tools) The bash tool runs...`. One entry, one tag. If you cannot decide between two tags for an entry, that is a signal to **re-evaluate the entry itself**, not to assign both.
- **Singular concept.** Each entry should be a single sentence and only concerned with a single concept. Prefer multiple entries versus combining many things into one.

## Templates

| Pattern     | Form                                                             |
| ----------- | ---------------------------------------------------------------- |
| State       | `[Scope] currently [does X / is Y].`                             |
| Persistence | `[Scope] persists [what] to [where].`                            |
| Flow        | `[Input/event] is handled by [actor/subsystem], which [action].` |
| Boundary    | `[Scope] is bounded by [constraint].`                            |

## Absence

A missing record, or an un-recorded area, simply means the list has no entry there yet. Absence is not a constraint — it is an open question, and a feature that fills a gap may establish the first entry for that area (proposed for human approval as part of the plan).

## Editing

Entries are added or amended **only with human approval**.

---

- (identity) trouper is a two-crate Rust workspace (edition 2024): `trouper`, the single-machine actor runtime, and `trouper_macros`, its proc-macro crate.
- (runtime) All actor communication is mediated by the runtime: actors never hold channels directly; every send is routed by path or schema through the registry and emits a tap fact.
- (runtime) Event-sourced actors are pure decision functions (sync `handle(&self)` returning events) with a single `apply` used for both live state application and replay; all other actors may perform side effects and use `ask`.
- (runtime) The registry is kernel code, not an actor: path→endpoint slots, schema, schema→handler route, partition, projector-set, and router rule tables persist across actor restarts; actor identity is its registered path.
- (runtime) Message schemas are runtime data: derived Rust types, hand-written impls, and external JSON descriptors register into the same schema table.
- (payloads) Envelope payloads are live typed values behind Arc<dyn PayloadValue>; handlers downcast and no JSON tree exists anywhere on the message path.
- (payloads) Every declared message type generates field-by-name reads and a lazy memoized JSON-text encoding from its derive.
- (payloads) Bytes exist only at two doors — erased ingress and the journal — wrapped in the PayloadBytes newtype.
- (schemas) Message identity is the schema name alone; schemas carry no version marker.
- (schemas) One Rust type per schema name is enforced at registration by TypeId uniqueness.
- (schemas) Message schemas are declared with the trouper Event/Command derive macros, which generate the SchemaDef from the struct's fields; hand-written Schema impls remain for complex or foreign descriptors.
- (runtime) Event-sourced journals are in-memory, seq-anchored lists of `Event` and `Snapshot` entries; restart restores from the latest snapshot plus the tail, and command redelivery is independent of snapshots.
- (runtime) Actor spawning is builder-based: typed actors declare `handles`/`emits` inline; foreign actors supply JSON schema plus handle/apply closures; positional spawn functions remain as alternative entry points.
- (runtime) Typed and foreign spawn builders register every declared handle and emit schema into the schema table; hand registration remains only where a def must exist before a builder runs (a partition set validates its shard key at install time).
- (runtime) Actor `manifest()` defaults to an empty manifest; builders supply the contract kind and edges, making builder declarations the single source of an actor's declared surface.
- (runtime) The ActorSystem exposes typed `tell` and `ask` entry points; ask is lease-backed with a mandatory timeout and settles with the same Replied/Timeout/Failed facts as in-actor asks.
- (runtime) Event emission is declaration-filtered: the kernel drops events whose schema the actor has not declared, before journal append, with a dead-letter fact and a tracing error; journals therefore contain only declared schemas.
- (runtime) Outbound actor messages are declaration-enforced: every intent an actor records (send, publish, send_to_any, reply) is gated at flush against the actor's .emits, and undeclared messages drop with an UndeclaredEmit dead letter plus a tracing error.
- (runtime) Partition sets are declarative specs resolved by the kernel at route time: senders keep addressing the public path; per-entity paths derive from a schema-declared shard key and entities activate on demand.
- (runtime) Service-actor asks are lease-backed: every ask carries a mandatory timeout, the reply slot is a runtime lease that dies with the ask, and outcomes (Replied/Timeout/Failed) are tap facts.
- (tap) The tap is a global bounded drop-oldest ring of facts that may drop under pressure; it is observation only — delivery never flows through it, and JSON projection happens only at the tap boundary (`Fact::to_json`).
- (tap) Each tap fact carries a monotonic `offset`; the ring exposes its retained `[floor, next)` range, and offset gaps signal eviction.
- (tap) The tap ring is the sole observation surface: facts are recorded to the bounded ring and read by the host; actors observe events only by declaring .handles on them.
- (runtime) `tracing` is the developer-diagnostic channel; the tap is the product fact stream.
- (runtime) `system.export()` returns a JSON-serializable `SystemExport` of the live system: schemas, actors (with ES state and inbox cursor), declared edges, observed edges, pools, partitions, and router rules; `SystemExport` round-trips through JSON losslessly.
- (runtime) A ReportState command makes a StateReporter actor emit a journaled StateReported event whose payload is the JSON SystemExport document.
- (runtime) Domain outcomes are events journaled like any other event; technical failures are handler panics, which supervision converts into restarts and `Failed`/`Escalated` tap facts.
- (runtime) All synchronous mutexes are parking_lot: lock() cannot fail, there is no poisoning, and a panic under a lock never wedges later lockers.
- (runtime) Replies are point-to-point: a reply with no reply_to is dropped silently, never broadcast; every outbound actor message requires a declared .emits and is dropped with an UndeclaredEmit dead letter otherwise.
- (runtime) Handler contexts (CmdCtx/MsgCtx) expose only tier-curated methods; the outbox, trace, and ask port are crate-private plumbing.
- (runtime) Handler effects are typed: ctx reply/publish/send take Message values (Schema + serde), derive the schema id from the type, and serialize at intent time; raw value escape hatches remain (\*\_json methods, Event::new, into_inner).
- (values) The runtime's public value type is trouper::Json, a newtype over the internal JSON tree; serde_json types do not appear in public signatures.
- (runtime) Command handlers return an Events buffer (inline for two events, heap beyond) and construct events from typed values via IntoEvent.
- (runtime) Event folds extract payloads by schema name via Event::as_fact::<T>() (downcast; a replayed event decodes from its bytes); unmatched schemas are ignored.
- (journal) Event payloads persist as compact JSON text written once at append; replay decodes bytes straight into the declared struct.
- (journal) Payload evolution follows the additive-fields contract; a payload that fails boundary decode dead-letters as Decode and fails rebuild loudly.
- (routing) A declared edge accepts any payload under its schema name, delivered as the registered type.
- (queries) Typed asks deliver typed replies through lease slots; a reply type mismatch surfaces as a named ask error.
- (runtime) Spawn arguments are provided as typed Serialize values on builders and decoded in restore via Json; partition entities receive the shard key merged as a "key" field.
- (contexts) MsgCtx exposes typed send/publish/send_to_any/ask/reply/stop_self, all emits-gated at flush; CmdCtx is pure introspection — an event-sourced entity announces only by returning facts from its decision.
- (lifecycle) A trouper actor's on_stop hook runs on graceful stop, self-stop, passivation, and the shutdown sweep; never on crash or hard shutdown.
- (lifecycle) Service actors receive an async on_stop(&mut self); event-sourced actors receive a sync on_stop(&self).
- (lifecycle) stop_self() records a deferred intent; the actor stops after the current message commits, flushing pending sends in order.
- (lifecycle) Passivation is declarative spawn config on the typed builders; the idle timer resets only on completed message steps.
- (lifecycle) Passivation drains already-queued messages after closing the inbox; external stop and self-stop dead-letter undelivered mail instead.
- (lifecycle) system.shutdown_graceful(deadline) runs a two-phase sweep: a barrier that refuses new sends and disables activation, restarts, and passivation, then a deadline-bounded parallel drain with on_stop hooks.
- (journal) All event-sourced journal reads and writes route through the async JournalStore trait; the in-memory store is the default and only implementation.
- (journal) The JournalStore append is awaited before the command's ack, backends may write through or buffer, and the runtime flushes the store once during the shutdown sweep.
- (supervision) A supervised child's restart engine exits when the child's spec is removed and stays suspended during the shutdown sweep.
- (supervision) Restart-budget exhaustion stops the child and delivers an Escalated message to its declared parent path; the parent is an ordinary actor whose handler owns the response.
- (partitions) Partition entities passivate per their factory's builder config and re-spawn on the next send to the public path.
- (routing) A recorded, declared event-sourced fact is broadcast by the kernel to every actor that declared .handles for it; zero handlers is a silent no-op.
- (routing) system.tell delivers one copy to the addressed path; system.send_to_any delivers one copy to one handler of the schema (round-robin); how a message arrived is invisible to the receiver's dispatch.
- (routing) deliver_schema_value uses the schema's declared kind as the erased bridge's default transport: Event schemas broadcast, Command schemas route to one handler.
- (routing) A re-spawned partition entity re-declares its handled schemas.
- (events) Event-sourced actors do not answer asks: system.ask to an ES path fails fast with AskError::Unresolved; consumers listen for facts.
- (journal) Journals accept only declared schemas: an undeclared recorded event dead-letters UndeclaredEvent before append. Dead letters retain their envelopes and are host-managed via drain_dead_letters; the runtime never redrives automatically.
- (supervision) An event-sourced entity owns no lifecycle intents: only passivation, external stop, or supervision ends one.
- (lifecycle) Passivation and stop remove the actor's in-memory state entry; only live actors hold state, and cold state returns by replay from the journal store.
- (projections) A projector is an event-sourced actor whose consumed facts are re-recorded into its own journal; the journaled origins are its checkpoint and catch-up seeds whatever the journal lacks.
- (projections) The journal store assigns every recorded fact a globally monotonic ingest_seq at append time; projectors fold in ingest_seq order and scans return origin-recorded entries only.
- (routing) A projector set declares consumption with a shard key: broadcast copies of a consumed schema resolve a key per copy and activate the owning projector on demand, like a told command.
- (journal) The store is told passivated(path) when an idle entity leaves memory; a failing hint is logged and never blocks passivation.
- (queries) projector_state wakes a projector-set entity if needed, awaits catch-up, and returns the complete fold; es_state is a best-effort capture of in-memory state only.
- (queries) with_es_state/with_projector_state read entity and projector state typed under the state lock with no serialization; try_ variants are sync and non-blocking.
- (queries) Typed reads cover live entities and projectors only: foreign actors read as JSON, passivated entities read None, and service-actor state is not a readable surface.
- (lifecycle) A projector records a CaughtUp tap fact when its catch-up completes.
- (docs) The trouper crate's public API documentation follows std rustdoc style — consumer-relevant statements only, no implementation narration — and cargo doc runs warning-free.
- (runtime) Actor inbox backpressure engages at the spawn-configured mailbox capacity: Block senders await room, and DropNew/DropOld refusals dead-letter through the front door.
- (lifecycle) A restarted actor's front door is created with the original spawn's mailbox capacity and policy.
- (queries) A set-owned projector wake completes on a kernel caught-up signal; the tap's CaughtUp fact remains the host-observable marker.
- (journal) A due-time snapshot check reads kernel-side anchors and loads the journal only when a snapshot is due.
- (runtime) Ask reply leases are bounded: failed request deliveries cancel the lease and expired slots are pruned.
- (bench) Criterion benches live in benches/e2e.rs (usage-shaped) and benches/micro.rs (component-shaped), run via cargo bench.
- (runtime) Message delivery wakes the actor's loop through a Notify signal fired by the front door; no fixed-interval polling exists on the message path.
- (runtime) Per-actor mutable bookkeeping lives on the actor cell as lock-free state; the kernel tables lock guards cross-actor state only (an ES message's happy path acquires it zero times; a send acquires it once for its Sent fact).
- (runtime) An idle actor's loop sleeps until its next due duty (snapshot cadence or passivation) or the next message, whichever comes first; an actor with no duties armed does not wake at all.
- (runtime) A supervised child's crash is signaled by a Notify on the child's cell; supervision engines wake on the signal and read the crash flag lock-free instead of polling.
- (bench) The idle_fleet bench includes a 10,000-idle-actor case, and examples/idle_burn.rs measures runtime CPU seconds for a duty-armed idle fleet (5,000 actors burn ~0.003 s CPU/s of wall vs ~0.28 s polled before this work).
