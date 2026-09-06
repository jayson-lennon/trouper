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

- (identity) **actor-canvas** is a Rust workspace (edition 2024) whose `actor-runtime` crate is a single-machine actor runtime; the root `actor-canvas` package re-exports it.
- (runtime) All actor communication is mediated by the runtime: actors never hold channels directly; every send is routed by path or topic through the registry and emits a tap fact.
- (runtime) Event-sourced actors are pure decision functions (sync `handle(&self)` returning events) with a single `apply` used for both live state application and replay; all other actors may perform side effects and use `ask`.
- (runtime) The registry is kernel code, not an actor: path→endpoint slots, schema, type→handler, and topic→subscriber tables persist across actor restarts; actor identity is its registered path.
- (runtime) Message schemas are runtime data: Rust types and external JSON descriptors register into the same schema table; payloads cross the runtime boundary as JSON.
- (runtime) Event-sourced journals are in-memory, seq-anchored lists of `Event` and `Snapshot` entries; restart restores from the latest snapshot plus the tail, and command redelivery is independent of snapshots.
- (runtime) Actor spawning is builder-based: typed actors declare `handles`/`emits` inline; foreign actors supply JSON schema plus handle/apply closures; positional spawn functions remain as alternative entry points.
- (runtime) Typed and foreign spawn builders register every declared handle and emit schema into the schema table; hand registration remains only where a def must exist before a builder runs (a partition set validates its shard key at install time).
- (runtime) Actor `manifest()` defaults to an empty manifest; builders supply the contract kind and edges, making builder declarations the single source of an actor's declared surface.
- (runtime) The ActorSystem exposes typed `tell` and `ask` entry points; ask is lease-backed with a mandatory timeout and settles with the same Replied/Timeout/Failed facts as in-actor asks.
- (runtime) Event emission is declaration-filtered: the kernel drops events whose schema the actor has not declared, before journal append, with a dead-letter fact and a tracing error; journals therefore contain only declared schemas.
- (runtime) Pools and partition sets are declarative specs resolved by the kernel at route time, never forwarding actors: senders keep addressing the public path; partition sets derive per-entity paths from a schema-declared shard key and activate entities on demand.
- (runtime) Service-actor asks are lease-backed: every ask carries a mandatory timeout, the reply slot is a runtime lease that dies with the ask, and outcomes (Replied/Timeout/Failed) are tap facts.
- (tap) The tap is a global bounded drop-oldest ring of facts that may drop under pressure; it is observation only — delivery never flows through it, and JSON projection happens only at the tap boundary (`Fact::to_json`).
- (tap) Each tap fact carries a monotonic `offset`; the ring exposes its retained `[floor, next)` range, and offset gaps signal eviction.
- (tap) The `system.facts` topic is a subscribable mirror of the tap; facts are pumped to it after each record, and per-subscriber cursors detect gaps (at-most-once delivery of teed copies).
- (runtime) `tracing` is the developer-diagnostic channel; the tap is the product fact stream.
- (runtime) `system.export()` returns a JSON-serializable `SystemExport` of the live system: schemas, actors (with ES state and inbox cursor), declared edges, observed edges, pools, partitions, and router rules; `SystemExport` round-trips through JSON losslessly.
- (runtime) A ReportState command makes a StateReporter actor emit a journaled StateReported event whose payload is the JSON SystemExport document.
- (runtime) Domain outcomes are events journaled like any other event; technical failures are handler panics, which supervision converts into restarts and `Failed`/`Escalated` tap facts.
- (runtime) Replies are point-to-point: a reply with no reply_to is dropped silently, never broadcast; failures that must reach non-asking observers travel as published events on topics.
- (runtime) Handler contexts (CmdCtx/MsgCtx) expose only tier-curated methods; the outbox, trace, and ask port are crate-private plumbing.
- (canvas) System state is served and consumed as zenoh messages on the actor-runtime/state key (Config::default()); the canvas CLI queries it and prints the export.
- (canvas) The canvas GUI consumes the same zenoh state key as the CLI and renders the export as an interactive graph with pan, zoom, and cursor-anchored popups; the runtime serves it no differently than the CLI.
- (canvas) The canvas GUI lives in its own repo next to the SDK (../actor-canvas) and consumes actor-runtime and state-report by path dependency; this SDK carries no bevy anymore.
