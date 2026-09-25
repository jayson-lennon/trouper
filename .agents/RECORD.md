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
- (runtime) All actor communication is mediated by the runtime: actors never hold channels directly; every send is routed by path or schema through the registry and emits a Sent observation when observation is enabled.
- (runtime) Event-sourced actors are pure decision functions (sync `handle(&self)` returning events) with a single `apply` used for both live state application and replay; all other actors may perform side effects and use `ask`.
- (runtime) The registry is kernel code, not an actor: path→endpoint slots, schema, schema→handler route, partition, projector-set, and router rule tables persist across actor restarts; actor identity is its registered path.
- (runtime) Message schemas are runtime data: derived Rust types, hand-written impls, and external JSON descriptors register into the same schema table.
- (payloads) Envelope payloads are unique boxes on the send path, promoted to shared Arc storage only when tee, fan-out, or journal retention needs multiple owners; handlers downcast and no JSON tree is walked on the message path.
- (payloads) Cloning a unique Payload deep-copies its erased value; cloning a shared Payload increments its Arc reference count.
- (payloads) Payload JSON and wire-encoding memoization lives in a side box allocated on the first cold read.
- (payloads) Service handlers borrow the live value (&M, zero copies) and replies/asks carry the payload end to end; serde exists only at the doors — the journal, erased ingress, and the public ask's lazy Json edge.
- (payloads) Every declared message type generates field-by-name reads and a lazy memoized JSON-text encoding from its derive.
- (payloads) Bytes exist only at two doors — erased ingress and the journal — wrapped in the PayloadBytes newtype.
- (schemas) Message identity is the schema name alone; schemas carry no version marker.
- (schemas) One Rust type per schema name is enforced at registration by TypeId uniqueness.
- (schemas) Message schemas are declared with the trouper Event/Command derive macros, which generate the SchemaDef from the struct's fields; hand-written Schema impls remain for complex or foreign descriptors.
- (runtime) Event-sourced journals are in-memory, seq-anchored lists of `Event` and `Snapshot` entries; restart restores from the latest snapshot plus the tail, and command redelivery is independent of snapshots.
- (runtime) Actor spawning is builder-based: typed actors declare `handles`/`emits` inline; foreign actors supply JSON schema plus handle/apply closures; positional spawn functions remain as alternative entry points.
- (runtime) Typed and foreign spawn builders register every declared handle and emit schema into the schema table; hand registration remains only where a def must exist before a builder runs (a partition set validates its shard key at install time).
- (runtime) Actor `manifest()` defaults to an empty manifest; builders supply the contract kind and edges, making builder declarations the single source of an actor's declared surface.
- (runtime) The ActorSystem exposes typed `tell` and `ask` entry points; ask is lease-backed with a mandatory timeout and settles with the same Replied/Timeout/Failed observations as in-actor asks.
- (runtime) Event emission is declaration-filtered: the kernel drops events whose schema the actor has not declared, before journal append, with a dead-letter and a tracing error; journals therefore contain only declared schemas.
- (runtime) Outbound actor messages are declaration-enforced: every intent an actor records (send, publish, send_to_any, reply) is gated at flush against the actor's .emits, and undeclared messages drop with an UndeclaredEmit dead letter plus a tracing error.
- (runtime) Partition sets are declarative specs resolved by the kernel at route time: senders keep addressing the public path; per-entity paths derive from a schema-declared shard key and entities activate on demand.
- (runtime) Service-actor asks are lease-backed: every ask carries a mandatory timeout, the reply slot is a runtime lease that dies with the ask, and outcomes (Replied/Timeout/Failed) are observations.
- (observability) Runtime observation is opt-in: a single user-installed handler closure (`ObservationHandler`); with no handler installed no observation messages are constructed. JSON projection exists only at the observation boundary (`Observation::to_json`).
- (observability) The handler can be installed or removed at runtime through the system API (`set_observation`/`clear_observation`), and a handler panic is isolated from the message path; observations carry a timestamp and a runtime-event kind.
- (observability) The handler is the sole observation surface: uncaptured observations are gone (no history — the DLQ is the only after-the-fact artifact the runtime keeps); actors observe events only by declaring .handles on them.
- (runtime) `tracing` is the developer-diagnostic channel; the observation handler is the product event stream.
- (runtime) `system.export()` returns a JSON-serializable `SystemExport` of the live system: schemas, actors (with ES state and inbox cursor), declared edges, observed edges, pools, partitions, and router rules; `SystemExport` round-trips through JSON losslessly.
- (runtime) A ReportState command makes a StateReporter actor emit a journaled StateReported event whose payload is the JSON SystemExport document.
- (runtime) Domain outcomes are events journaled like any other event; technical failures are handler panics, which supervision converts into restarts and `Failed`/`Escalated` observations.
- (runtime) All synchronous mutexes are parking_lot: lock() cannot fail, there is no poisoning, and a panic under a lock never wedges later lockers.
- (runtime) Replies are point-to-point: a reply with no reply_to is dropped silently, never broadcast; every outbound actor message requires a declared .emits and is dropped with an UndeclaredEmit dead letter otherwise.
- (runtime) Handler contexts (CmdCtx/MsgCtx) expose only tier-curated methods; the outbox, trace, and ask port are crate-private plumbing.
- (runtime) Handler effects are typed: ctx reply/publish/send take Message values by value (Schema + PayloadValue), derive the schema id from the type, and wrap the live value into the fabric at intent time (zero serde); raw value escape hatches remain (\*\_json methods, Event::new, into_inner).
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
- (journal) All event-sourced journal reads and writes route through the async JournalStore trait; the in-memory store is the default.
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
- (routing) Shard-key extraction reads the live payload's derive-generated field() (zero serde); wire bytes decode one field at the door as the fallback.
- (journal) The store is told passivated(path) when an idle entity leaves memory; a failing hint is logged and never blocks passivation.
- (queries) projector_state wakes a projector-set entity if needed, awaits catch-up, and returns the complete fold; es_state is a best-effort capture of in-memory state only.
- (queries) with_es_state/with_projector_state read entity and projector state typed under the state lock with no serialization; try_ variants are sync and non-blocking.
- (queries) Typed reads cover live entities and projectors only: foreign actors read as JSON, passivated entities read None, and service-actor state is not a readable surface.
- (lifecycle) A projector records a CaughtUp observation when its catch-up completes and observation is enabled.
- (docs) The trouper crate's public API documentation follows std rustdoc style — consumer-relevant statements only, no implementation narration — and cargo doc runs warning-free.
- (runtime) Actor inbox backpressure engages at the spawn-configured mailbox capacity: Block senders await room, and DropNew/DropOld refusals dead-letter through the front door.
- (lifecycle) A restarted actor's front door is created with the original spawn's mailbox capacity and policy.
- (runtime) The front-door task handles only refused deliveries: Block holds, dead letters, and the fallback notify; direct sends push the inbox and notify the loop themselves.
- (registry) An Endpoint couples the front-door channel sender with the destination's live cell; restarts swap the endpoint while the cell persists across restart.
- (queries) Projector set wakes complete on the kernel caught-up signal regardless of observation state; the CaughtUp observation is the host-observable marker when enabled.
- (journal) A due-time snapshot check reads kernel-side anchors and loads the journal only when a snapshot is due.
- (runtime) Ask reply leases are bounded: failed request deliveries cancel the lease and expired slots are pruned.
- (bench) Criterion benches live in benches/competitors.rs (framework tell comparison), benches/micro.rs, and benches/journal.rs, run via cargo bench.
- (bench) The competitors bench times one producer actor hot-looping n tells at one sink actor per framework (trouper, kameo, ractor) at n of 64, 512, 2048, and 50000; setup (runtime, actors, priming) is excluded from timing via iter_batched, and the timed body is only a kanal start signal and done signal.
- (bench) Competitor completion is harness-owned: the producer parks awaiting a kanal start channel inside its handler, and the sink fires a kanal done channel at the target count — the bench thread never touches actor handles.
- (bench) Mailbox shapes in the competitors bench follow each framework's supported set: kameo runs bounded-64 and unbounded, trouper runs bounded-64 and OverloadPolicy::Unbounded, and ractor runs unbounded.
- (bench) The journal bench times SQLite-backed tell commits at :memory: and on-disk media plus a direct store flush, with system and journal setup outside the timed body.
- (runtime) Broadcast fan-out to plain subscribers first attempts direct inbox delivery on the publisher's task and sends refused copies through the front-door channel.
- (runtime) Message delivery wakes the actor's loop through a Notify signal fired by the sender on the direct-delivery path and by the front door on its fallback path; no fixed-interval polling exists on the message path.
- (runtime) Per-actor mutable bookkeeping lives on the actor cell as lock-free state; the kernel tables lock guards cross-actor state only (with observation disabled an ES message's happy path acquires the tables zero times end to end).
- (runtime) An idle actor's loop sleeps until its next due duty (snapshot cadence or passivation) or the next message, whichever comes first; an actor with no duties armed does not wake at all.
- (runtime) A supervised child's crash is signaled by a Notify on the child's cell; supervision engines wake on the signal and read the crash flag lock-free instead of polling.
- (bench) examples/idle_burn.rs measures runtime CPU seconds for a duty-armed idle fleet (5,000 actors burn ~0.003 s CPU/s of wall vs ~0.28 s polled before this work).
- (bench) Bench completion is push-driven: benches spin-yield on the destination's inbox cursor — which advances exactly at the kernel commit point (journal append + ack), so cursor ≥ base+N proves N committed — and await ask replies; never poll actor state on a timed sleep.
- (bench) Criterion throughput Elements equal the number of messages each iteration fully processes (commits in the journal bench; counts at the sink in the competitors bench).
- (schemas) The Event/Command derive maps every non-shard-key field to FieldTy::Json without inspecting its Rust type; no field type is a compile error.
- (schemas) The Event/Command derive maps a #[schema(shard_key)] field to its real flat descriptor type, since shard keys are string or number values read at partition routing.
- (schemas) The derive's schema name is the struct ident and descriptor field names are the Rust field idents; renames are serde's concern only, and the shard-key field must not be serde-renamed.
- (runtime) An ES loop captures its state Arc at spawn and rebinds it at boot recovery, restart, and projector catch-up — the only moments the table's Arc is replaced; message steps read the cached state shell without the kernel tables lock (probe-counted: with observation off, a tell→fold→ack window acquires the tables zero times — the step's commit point reads cell state directly).
- (runtime) Emit-declaration gating on the message path reads a cell-local declaration mirror, seeded from the manifest at spawn and re-synced with the registry at the single declaration-mutation point (ActorSystemCore::declare_emits); the registry lock is off the step path.
- (journal) The optional daow feature enables a SQLite JournalStore backend over a daow Pool; building it runs a versioned, atomic journal-table migration chain and seeds the ingest counter from the stored maximum.
- (journal) The daow backend treats each active path's in-memory journal as authoritative and persists only the journal suffix beyond its last successful flush.
- (journal) The daow backend seeds a cold path from the append-only SQLite event table when it has no in-memory journal.
- (journal) Normal daow event persistence appends SQLite rows, with explicit host-requested purge as the only deletion path.
- (journal) The daow backend removes a passivated path buffer only after its entries commit to SQLite, and any retained buffer contributes to replay.
- (runtime) Journal stores install only at system construction, through SystemConfig's journal args (store plus an optional control-message closure); no post-construction setter exists.
- (journal) The control-message closure receives synchronous store messages (currently errors only); with none installed store errors are only traced.
- (bench) The journal bench (benches/journal.rs, requires the daow feature) measures the daow backend over in-memory and on-disk SQLite; tell_acked spawns a fresh entity per iteration in untimed setup, so the timed body is only tells plus the commit-cursor wait.
- (schemas) A schema's SchemaDef and SchemaId are cached per-type in statics; repeated reads allocate nothing.
- (runtime) Trace and causality ids are clock+counter generated (v7-layout, no getrandom); LeaseId shares the scheme.
- (runtime) Service actor handlers dispatch inline on the actor loop task; no task is spawned per message.
- (schemas) SchemaId holds a static string for derived schemas and allocates only for runtime-built names.
- (runtime) The actor inbox is a parking_lot mutex guarding a sync queue; senders and loops take it only for synchronous push/peek/commit bodies, never across an await.
- (runtime) Step loops resolve the cell's entry tables once per batch and dispatch by reference; the per-message entry lookup takes no lock and bumps no Arc.
- (runtime) A plain-path send acquires the registry once (rules, set probes, and endpoint resolve share one critical section); partition/projector destinations resolve after resolve_partition and may acquire again.
- (runtime) Step loops claim batches by moving envelopes out of claimed inbox slots (tombstones) and restore un-committed claims to their original offsets on crash or stop; no snapshot clones exist on the step path.
- (runtime) A Block-refused tell parks on a space-available notify fired at inbox commit; there is no poll-retry loop on the hold path.
- (runtime) An actor spawned with OverloadPolicy::Unbounded has no mailbox capacity: the inbox never refuses and the front door never engages; no pre-allocation occurs (the queue grows on demand).
- (runtime) The front-door channel is a kanal channel; Endpoint wraps its sender with the destination's live cell and an atomic pending count incremented on accepted sends and decremented by the door on every drain.
- (bench) The competitors profile rig (examples/profile_competitors.rs) mirrors the trouper and ractor competitors-bench legs under perf; folded stacks reduce to a sample-share accounting committed in bench-accounting.md.
- (bench) The competitors bench creates a fresh runtime and actors for every iteration, times only the interval from the start signal to sink completion, and excludes runtime construction, priming, settlement, release, and thread joins from that interval.
