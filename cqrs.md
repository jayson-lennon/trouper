# cqrs.md — projections, read models, and the ES/query boundary

> **Status:** design notes captured from the v0.5.0 planning dialectic. Nothing here is
> implemented yet. This is the seed spec for a future projections task, plus the
> query/read-model decisions that task must respect.

---

## 1. The taxonomy: three disciplines, one actor

Every trouper actor is the same thing (mailbox, `.handles`/`.emits`, messages in/out).
The domains differ only in *discipline*:

| Role | Discipline | Substrate | Eats | Emits |
|---|---|---|---|---|
| **Entity** | Event-sourced over commands | `EventSourcedActor` + partition set | Commands (`tell`) | Recorded facts (auto-broadcast per v0.5.0 decision 4B) |
| **Projector** | Event-sourced over facts | `EventSourcedActor` (future: + historical load) | Facts (`.handles` on the live tail; catch-up on cold start) | Its own fold as state; optionally output facts |
| **Service actor** | Impure edge | plain `ServiceActor` | Anything (facts, commands, asks) | Sends, publishes, replies — emits-gated |

This is the sentence actors.md already leads with: roles are compositions, not runtime
types. A projector is *not* a new runtime concept — it is an ES actor whose command
stream happens to be another entity's fact stream.

## 2. Why a projector must be an ES actor (the settled argument)

Bolting a projector onto a service actor means hand-rolling state persistence,
snapshotting, and rebuild-on-restart — i.e. re-implementing half of trouper's ES
machinery badly. Making the projector an `EventSourcedActor` gets all of it for free:

- **Fold** — `apply(&mut self, event)` *is* the projection update function.
- **Persistence** — the journal records what has been folded.
- **Rebuild** — restore + snapshot + tail replay rebuilds the read model after a crash.
- **Passivation** — idle projections leave the heap; memory-boundedness falls out.
- **Live tail** — v0.5.0 decision 4B: recorded, declared facts auto-broadcast to every
  `.handles` subscriber. A projector declares `.handles::<Deposited>()` and receives the
  stream with zero extra wiring.

The only missing piece is **historical load** (§5).

## 3. The query boundary: the four-quadrant table

The v0.5.0 decisions (ask fails fast into ES entities; projections end up ES) close the
query story into four non-overlapping verbs:

| You want... | You use |
|---|---|
| To command an entity | `tell` (or `send_to_any` for load-balanced sets) |
| To know what an entity/projector knows | `system.es_state(path)` — host reads the fold |
| To be *told* when something happens | `.handles::<Fact>()` on a service actor |
| To request/response with a service | `ask` (service tier only, never entities) |

Why `es_state` and not ask for reads: an ask is a *message* — it would have to be
handled by a decision function, journaled, answered. That mixes queries into the write
model, exactly what CQRS forbids. `es_state` is a **host-side read of already-
materialized state**: no message, no mailbox, no decision, no journal append. It is the
same category as reading a snapshot from the store — inspection, not interaction.
Queries never flow through the write model; they read the fold.

The ATM pattern (the canonical ES conversation), for reference:

```
ATM (service actor) --tell--> Account (entity): Deposit
ATM declares .handles::<DepositCompleted>() and waits (its own state + timeout policy)
Account records DepositCompleted -> 4B broadcast -> ATM's handler fires
Account frozen / rejected -> records DepositRejected (or nothing) -> ATM times out
```

No ask anywhere. The command's outcome is a *fact*; the sender listens for it. Anything
that must give up does so with its own timer/state discipline (§7).

## 4. `es_state` — the read model API as it exists today

```rust
system.es_state(&path) -> Option<JsonValue>   // system.rs:1687
```

Takes the ES state lock, calls `capture_erased()` (the same seam the snapshot writer
uses), returns the serialized fold. `None` if the path is not live or not ES.

- Built as a test/inspection helper; semantics are already right for read-model duty
  (host-side, no side effects, deterministic serialization of the fold).
- Known trade: it returns the **whole** serialized fold. No sub-field projection, no
  selector. Same trade as the snapshot format (state *is* the query result). If it ever
  hurts: a `project(path, selector)` read — still not ask.
- Possible ergonomic follow-up (small, separate): `es_state_as::<T>(path) -> Option<T>`
  deserializing into the typed state.
- Companion inspection APIs: `journal_schemas(path)`, `export()` (`PartitionExport`
  lists `path`, `key_field`, and activated `entities`).

### Path addressing of sharded entities (settled)

- Entity paths are deterministic: `format!("{public}/{key}")` — `accts` + `alice` →
  `accts/alice`. `ActorPath` is an opaque `Arc<str>`: no kind tag, no validation, no
  set-membership. A derived entity path is an ordinary path.
- `es_state(&ActorPath::new("accts/alice"))` works today (es_entity example does it).
- The partition set intercepts only the **public** path (`resolve_partition` at
  kernel.rs:508 fires when `dest` is the set's public path). Direct entity addressing
  bypasses key extraction and activation entirely: a passivated entity addressed
  directly dead-letters `Unresolvable` instead of waking.
- What the path *cannot* tell you: whether it is an entity. Membership is derivable
  from `export()` (which sets own which entity paths), not from the path itself.

## 5. The actual projections task (future work)

### 5.1 Historical load — the one missing primitive

Live tail is solved (4B). Cold start is not: a projector spawned today sees only the
future (`late_handler_receives_no_phantom_delivery`). Catching up requires a
`JournalStore` extension, roughly:

```rust
// shape TBD — dump events matching a schema set across all/selected journals
async fn scan(&self, schema: &SchemaId, after: Option<Cursor>) -> Vec<(ActorPath, Event)>;
```

Design questions that spec must answer:

- **Enumeration:** per-journal reads walked by the projector (via
  `PartitionExport.entities` — but that lists only *activated* entities; passivated
  entities' journals exist in the store yet are enumerated nowhere), or a store-level
  scan API that sees everything persisted?
- **Global order:** is there one? Per-entity journals are ordered; cross-entity order is
  not defined. A projection that must not care about cross-entity order folds
  commutatively (sums, sets). One that cares needs a merge rule (e.g. per-entity
  sequence interleaving by journal scan order) — decide explicitly, document loudly.
- **Cutover:** catch-up phase must end exactly where the live tail begins — the
  projector activates its `.handles` subscription only after folding history (or the
  runtime must buffer/dedupe the overlap). The standard answer: subscribe first,
  buffer live facts, fold history, drain buffer, go live. This ordering is the crux of
  the whole feature and should get its own test suite.

### 5.2 What the projector journals — the big fork (do not decide casually)

- **Option R — re-record consumed facts.** The projector's own journal holds every
  `Deposited` it folded. Rebuild is self-contained (restore + replay own journal).
  Cost: journal amplification — N accounts × M projectors copies.
- **Option C — checkpoint reference.** The projector journals only
  `"folded accounts/alice through seq 42"` markers. Compact. Cost: rebuild re-reads
  *source* journals — replay now depends on source retention, multi-source ordering,
  and a replay path that can drive the fold from external data (a new ES capability:
  replay input ≠ own journal).

Option C is compact but punches a hole in "an ES actor's journal fully determines its
state." Option R is dumb and bulletproof. Start with R; C is an optimization with a
real correctness surface.

### 5.3 Non-goals carried from the mainline dialectic

- No global event broker / bus. The one-machine scope statement rules out the
  Kafka-style log-per-stream machinery; projections compose from per-entity journals
  and message delivery, not from a central stream.
- No ask-based queries against entities or projectors — §3's table is the contract.
- No automatic observer replay for late `.handles` subscribers. Catch-up is explicit
  (projector cold start), never implicit (late subscriber silently backfilled).

## 6. Record-relevant statements (candidates when the projections task lands)

- `(events)` A recorded, declared ES fact is broadcast by the kernel to every actor that declared `.handles` for it; zero handlers is a silent no-op.
- `(events) Event-sourced actors do not answer asks: system.ask to an ES path fails fast with Unresolved; consumers listen for facts.`
- `(queries) es_state is the read surface for journaled state: a host-side capture of the fold with no message, no decision, and no journal write.`
- `(partitions) Entity paths derive deterministically as {public}/{key}; the set intercepts only the public path, and direct entity addressing bypasses key extraction and activation.`

## 7. Pending-request idiom (the timer-shaped hole, deliberately deferred)

The ATM pattern needs "give up eventually." The blessed idiom until (and unless) a
runtime scheduler exists — buildable today with zero runtime code:

1. Service actor holds `HashMap<ReqId, Pending>` (in-flight requests).
2. It `.handles` the completion fact (`DepositCompleted`) and removes the entry.
3. A ticker — a service actor publishing `Tick` on a tokio loop, or the host itself, gives it
   time-as-messages; on `Tick` it sweeps expired entries.

Why no `system.schedule(path, msg, delay)` yet: cancellation is inherently racy under
at-least-once delivery (the `Failed` may already be in flight when `DepositCompleted`
lands), so handlers must be idempotent *either way* — making the scheduler syntactic
sugar over this pattern for the common case, at the cost of a timer-wheel subsystem
(ClockService wiring, timeout-message schema, stopped-entity semantics, shutdown-sweep
interaction). Real need first; its own dialectic then.
