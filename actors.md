# actors.md — trouper is a single-machine actor runtime, on purpose

> **Status:** this document describes the API and behavior as of **v0.4.0** (the messaging
> unification). If a snippet doesn't compile, the doc or the code drifted — reconcile, don't
> paper over.
>
> **The one-line scope statement:** trouper implements the actor model **on one machine, in
> one process, with in-memory mailboxes**. It has no network, no cluster, no remote actors,
> no partial failure. Every design decision in this crate follows from that sentence. When
> reasoning about trouper, do not import reasoning from distributed systems — that material
> does not apply here, and applying it has repeatedly produced wrong designs (see §11).

---

## 0. Read this first: the one rule

> **The sender chooses topology. The receiver processes the message. The fabric carries it.**

Every property of trouper's messaging derives from that division:

- **Sender** — decides *how many* actors receive a message: one specific path (`tell`),
  one of the willing (`send_to_any`), or every one of the willing (`publish`).
- **Receiver** — declares one fact, "I process M" (`.handles::<M>()`), and handles it.
  It cannot know, and has no way to observe, which verb delivered the message.
- **Fabric** — resolves the sender's choice against the receiver's declaration. Nothing
  else. It never inspects what a message "means."

The north-star sketch (from the original design conversations):

```rust
system.tell(ActorPath::new("worker"), Kick {}).await?;   // sender: fire it somewhere

impl MsgHandler<Kick> for Worker {           // receiver: process by type
    async fn handle(&mut self, m: Kick, ctx: &mut MsgCtx) { /* whatever */ }
}

// Worker does not know — and must not be able to find out — whether the
// message came from a tell, a send_to_any, or a publish.
```

If a proposed change makes the receiver's behavior depend on *how* a message arrived,
or makes the sender's choice require cooperation from the receiver, the change violates
the model. Stop and re-derive.

---

## 1. What an actor is (and is not)

An actor is the unit of **state + concurrency**:

- It owns its state; no other actor can touch it. All mutation happens in its handler,
  one message at a time (its mailbox serializes).
- It has an **address**: a `path` — a virtual name in the registry (`"jobs/worker-2"`,
  `"accounts/acc-1"`). Paths are stable names; what sits behind a path can be replaced
  (a restart swaps the endpoint under the same identity).
- It communicates only by sending messages and, when asked, replying.
- It processes exactly one message at a time. No locks in domain code, ever.

An actor is **not**:

- A node in a cluster. There is no elsewhere.
- A long-lived connection to be health-checked. It is a name and a mailbox.
- A service with an SLA. If nothing sends to it, it does nothing.

**The oblivion contract.** Actors never assume a recipient exists. You may send to a
path with no live actor, or a schema nobody handles; the message is not lost silently —
it is dead-lettered, visibly (§10) — but the *sender* learns nothing and does nothing.
An actor's world model is: "some message forms exist; something *should* be listening;
my job is to emit and to handle." This is not pessimism, it is the model: knowledge of
who's out there is topology knowledge, and topology belongs to the composition layer
(the spawn site), not to message handlers.

---

## 2. Sending: three verbs, one mailbox

All application messaging is one of three sender verbs plus `ask`. There is no fourth
mechanism and there are no per-message flags.

| Verb | Destination | Delivered to | Zero takers |
|---|---|---|---|
| `tell(path, m)` / `ctx.send(Address::Path(p), &m, …)` | a path | that one path (what's behind it: an actor, or a partition-set public path) | envelope returns `Err` / dead-letters |
| `ctx.send_to_any(&m)` / `system.send_to_any::<M>(&m)` | the schema `M` | **one** of the actors that declared `.handles::<M>()` — round-robin; the fabric picks | dead-letters |
| `ctx.publish(&m)` / `system.publish(&m)` | the schema `M` (broadcast) | **every** actor that declared `.handles::<M>()`, one copy each | silent no-op — nothing delivered, nothing dead-lettered (a `Sent` fact records that the publish happened) |
| `ctx.ask(dest, &req, timeout)` / `system.ask(dest, req, timeout)` | a path | that path, plus a reply lease | error on the await |

```rust
// system-level (host code, tests, examples)
system.tell(ActorPath::new("jobs/router"), Kick {}).await?;
system.publish(&DocumentSaved { id }).await;          // news for every handler
system.send_to_any(&WorkJob::from(doc)).await?;       // one worker

// actor-level (inside `MsgCtx`) — same verbs, typed, deferred to post-ack flush:
ctx.send(Address::Path(ActorPath::new("fs.audit")), &audit_line, None); // point send
ctx.send_to_any(&ParseDocument::from(saved));          // one of the parse workers
ctx.publish(&DocumentSaved { id });                    // everyone handling it
ctx.reply(&SaveAck { bytes });                         // answer an ask (point-to-point)
```

Receiver side — identical for all four:

```rust
impl MsgHandler<ParseDocument> for ParseWorker {
    async fn handle(&mut self, job: ParseDocument, ctx: &mut MsgCtx) {
        // cannot tell (and must not care) whether this was told, sent-to-any, or published
    }
}
```

### `ask`

`ask` is **tell + a mandatory-timeout reply lease**. It exists on the system and — typed —
on `MsgCtx`:

- The timeout is mandatory. A wait-forever ask cannot be expressed.
- The reply slot is a lease that dies with the ask; a late reply lands nowhere, silently.
- Outcomes (`Replied` / `Timeout` / `Failed`) are tap facts, observable like everything else.
- A reply without an ask behind it (`reply_to` absent) is dropped — a reply is **never**
  broadcast. Publish is the news channel; reply is the answer channel.
- `ask` is typed on the request side: the schema id comes from `M`. The reply resolves as
  raw JSON — decoding it into a reply type is the caller's concern.

**Tier rule:** `ask` exists on `MsgCtx` (service actors) and not on `CmdCtx`
(event-sourced actors). This is not an omission: an ES decision function is a *sync,
pure* function — it cannot await, and a reply arriving mid-decision would make replay
non-deterministic. An ES actor that needs information receives it as a message.

---

## 3. Declaring: `.handles` is the whole truth

```rust
spawn_service_builder::<ParseWorker>(&system)
    .at(ActorPath::new("parse/worker-1"))
    .handles::<ParseDocument>()      // ← the ONLY receive-declaration in the crate
    .emits::<ParseDone>()
    .start();
```

`.handles::<M>()` means exactly: **"I process M."** It installs two entries together:

1. a **route entry** — makes the actor a candidate for one-of delivery
   (`send_to_any::<M>`, schema-addressed sends);
2. a **dispatch entry** — makes the actor's mailbox able to *run* an M
   (the kernel finds the M adapter and calls its handler).

Both come from one line. There is no way to be a fan-out target without being
dispatchable, or vice versa — which makes "the message arrived but nothing processed
it" structurally impossible for any declared handler.

History, so nobody re-derives the mistake: before v0.4.0 the two entries lived in two
independent tables fed by two declarations (`.handles` + `.subscribe`). Publish fanned
out over the subscribe table, whose members had *no dispatch entry*, so every broadcast
copy to a subscribe-only actor dead-lettered as `UnknownSchema`. Consumers had to declare
both lines to receive one message. That split was EDA vocabulary leaking into the fabric;
it is gone. Do not bring it back in any form (see §12).

`.emits::<E>()` is the **output** declaration: what facts this actor may announce. On ES
actors it gates which recorded events fan out (undeclared emits are dropped pre-journal).
It says nothing about how messages arrive.

**Schema kind (`Event` / `Command`) does not police the fabric.** The kind is metadata:
documentation in the schema table, and the default-transport hint for the erased-JSON
bridge (`deliver_schema_value`: Event-kind broadcasts, Command-kind routes to one
handler). Publishing a Command-kind schema works fine — with one handler it *is*
functionally a command. "Command vs event" is not a wire property; see §7.

---

## 4. Schema-addressing, plainly

Every message already carries a registered schema id (`ParseDocument@1`). The three verbs
differ only in what the envelope's destination says:

- `Path("parse/worker-2")` — aimed at a name;
- `Schema("ParseDocument@1")` + one-of — aimed at "whoever does this job";
- `Schema("ParseDocument@1")` + broadcast — aimed at "everyone who cares about this."

`send_to_any` is nothing new under the hood: it is the typed wrapper over the existing
schema-addressed send. The kernel resolves it through the route table — `Single` while
one actor handles M, rotating across the handler set once several do.

Receiver-side purity holds: the envelope is identical in shape, the handler is the same
code, and nothing in the message says which verb sent it.

---

## 5. Standard roles are compositions, not runtime features

There is no `Pool`, no `Router`, no `Supervisor` *type* in the runtime. Roles are
readable **at the spawn site** because the builder shows what an actor declares. Three
standard compositions cover nearly everything:

### Worker — "I do this job"

```rust
spawn_service_builder::<ParseWorker>(&system)
    .at(ActorPath::new("parse/worker-1"))
    .handles::<ParseDocument>()
    .emits::<ParseDone>()
    .start();
```

### Router — "I translate between domains" (stateless, on the hot path)

```rust
spawn_service_builder::<ParseRouter>(&system)
    .at(ActorPath::new("parse/router"))
    .handles::<DocumentSaved>()      // listens to news…
    .handles::<WorkerReady>()        // …and anything else relevant
    .start();

impl MsgHandler<DocumentSaved> for ParseRouter {
    async fn handle(&mut self, saved: DocumentSaved, ctx: &mut MsgCtx) {
        let job = ParseDocument::from(saved);   // domain → domain translation
        ctx.send_to_any(&job);                  // ONE parse worker; fabric picks
    }
}
```

The router holds **no worker list, no cursor, no liveness state** — the route table
already owns membership, rotation, and restart-pruning. It knows only the message forms.
If it addresses a worker that died mid-restart, the copy dead-letters (visible), and the
next message round-robins to a live one. Domain translation in, assignment out.

### Supervisor — "I own children and answer for them" (lifecycle-only)

```rust
// Domain types: the escalation message is an ordinary schema the engine
// SENDS to the declared parent when a child's budget exhausts — payload:
// { "escalated": <child path>, "reason": <why> }. `WorkerRetired` is
// plain domain code, not a runtime type.
#[derive(Debug, Deserialize)]
struct Escalated { escalated: String }

struct JobSupervisor { system: ActorSystem, restarts: HashMap<String, usize> }

impl MsgHandler<Escalated> for JobSupervisor {
    async fn handle(&mut self, e: Escalated, ctx: &mut MsgCtx) {
        // A child exhausted its restart budget. Domain decision:
        let n = self.restarts.entry(e.escalated.clone()).and_modify(|n| *n += 1).or_insert(1);
        if *n < 3 {
            self.system.spawn(worker_spec(&e.escalated, self.path.clone())); // re-arm
        } else {
            ctx.publish(&WorkerRetired { path: e.escalated.clone() });       // news for the org
        }
    }
}

// spawn the supervisor itself under supervision (who watches the watchmen — declared, not hidden):
system.spawn(ActorSpec {
    path: ActorPath::new("jobs/sup"),
    parent: None,
    restart: RestartPolicy::Transient,
    budget: RestartBudget::per(1, Duration::from_secs(60)),
    backoff: Backoff::default(),
    args: json!({}),
    spawn: Arc::new(|sys, path, args| {
        spawn_service_builder::<JobSupervisor>(sys).at(path.clone()).args(args.clone())
            .handles::<Escalated>()
            .emits::<WorkerRetired>()
            .start();
    }),
});

// workers, declared BY the supervisor's policy:
fn worker_spec(worker: &ActorPath, parent: ActorPath) -> ActorSpec {
    ActorSpec {
        path: worker.clone(),
        parent: Some(parent),                       // escalations land in JobSupervisor's inbox
        restart: RestartPolicy::Permanent,
        budget: RestartBudget::per(5, Duration::from_secs(10)),
        backoff: Backoff::default(),
        args: json!({}),
        spawn: Arc::new(|sys, path, args| {
            spawn_service_builder::<ParseWorker>(sys).at(path.clone()).args(args.clone())
                .handles::<ParseDocument>().start();
        }),
    }
}
```

**A pool is these three things, not a runtime type:** a supervisor (owns children),
workers (`.handles::<WorkJob>()`), and `send_to_any` (assignment). The pre-0.4.0
`install_pool` was removed because it was sugar over exactly this — its routing job is
the route table's existing round-robin, its spawning job is a `for` loop of
`system.spawn`, and its takeover-on-rescale is explicit domain code when needed. If you
find yourself reaching for pool infrastructure: write the supervisor.

---

## 6. There is no router bottleneck (do not optimize this)

Recurring failure mode, killed here with reasons: **the fear that a router actor in
front of workers is a scalability problem.** That fear comes from distributed-system
literature, where a hop is a network round-trip with serialization on both ends.

In trouper, a hop through a router is: dequeue an envelope from one in-memory `tokio`
mpsc, run a handler that constructs a new envelope (the payload is an already-built
`serde_json::Value` — copied in memory, never re-encoded), enqueue into another
in-memory mpsc. Microseconds. For any workload a single-process host application can
generate, the router is nowhere near the wall. The actual costs in this runtime are
handler work and mailbox contention — which exist with or without the router.

Consequences, stated as policy:

- Do not add pooling/batching/shortcut infrastructure to "avoid a hop."
- Do not inline routing into senders ("just send directly to workers") to save the hop —
  that trades a readable role for a hand-rolled worker registry in every sender.
- If throughput is ever genuinely bounded by a single router's mailbox, the answer is
  still composition: shard the domain (more routers, more paths), not fabric features.

---

## 7. Event sourcing: a state discipline, not a message kind

The fabric (§2) never changes for event-sourced actors — same verbs, same envelopes.
ES is a **per-actor discipline for state**:

```rust
struct Account { key: String, balance: i64 }

impl EventSourcedActor for Account {
    fn manifest() -> ActorManifest { ActorManifest::new().kind(ActorKind::EventSourced) }
    fn restore(args: &JsonValue) -> Self {                      // genesis + replay ctor
        Self { key: args["key"].as_str().unwrap().into(), balance: 0 }
    }
    fn apply(&mut self, event: &Event) {                        // fold: fact → state
        if event.schema.as_str() == "Credited@1" { self.balance += event.payload["delta"].as_i64().unwrap(); }
    }
}
impl CommandHandler<Credit> for Account {                       // input → facts I record
    fn handle(&self, cmd: Credit, _ctx: &mut CmdCtx) -> Vec<Event> {
        vec![Event::new(Credited::schema_id(), json!({ "delta": cmd.amount }))]
    }
}
```

- `handle` is **sync and pure**: input in, facts out. The kernel appends those facts to
  the journal (one batched `store.append` per message, **awaited before the ack**), then
  applies them to state. A crash before append = message redelivers; after ack = replay
  rebuilds. This is why `CmdCtx` has no `ask` (§2) and why ES handlers take `&self`.
- The distinction ES needs is **argument vs. return value of the handler** — the message
  that arrived vs. the facts recorded about it. That line is drawn by the *signature*,
  not by the wire.

**Events are provenance, not message types.** An ES "event" is a fact some ES actor
*recorded* — it can only come into existence inside one, because recording requires a
journal and an `apply`, which only the ES actor owns. Once recorded and applied, a
declared fact (`emits::<Credited>()`) also **fans out as an ordinary message**: any actor
— ES or plain service — that declares `.handles::<Credited>()` receives a copy. Service
actors have no events at all; their async handlers return nothing and nothing is
journaled for them.

**Role is relational.** `WorkDone` is an event for the actor that recorded it and a plain
input for every actor that handles it. The same type plays both roles depending on who is
looking. This is why messages carry **no `is_event` flag** — there is no fact of the
matter about a message "being an event" independent of the observer, and a flag would
re-import the receiver-side split that §3 removed. Consequences:

- Publishing a `DoWork` schema with exactly one listener **is** a command, functionally.
  Command/event is about how many actors listen — visible at the publish site — not a
  property stamped on the message.
- ES actors receive publishes like anyone else; an external event delivered to an ES
  handler is journalled like any other input (standard event-sourcing practice).

---

## 8. Partition sets: transparent passivation/activation of ES entities

This is the one messaging-adjacent feature that genuinely requires the kernel, because
**activation-on-demand means accepting mail for a path that does not exist yet** — no
actor can do that; only route-time interception can.

```rust
system.install_partition_set(PartitionSpec {
    public: ActorPath::new("accounts"),     // senders address ONLY this, forever
    system: system.clone(),                 // the set activates entities through it
    key_field: "account",                   // Credit's schema field marked ShardKey
    factory: Arc::new(|sys, path, args| {
        spawn_es_builder::<Account>(sys).at(path.clone()).args(args.clone())
            .handles::<Credit>()
            .passivate_after(Duration::from_secs(30))
            .start();
    }),
    args_template: None,                    // or a JSON seed merged with the shard key
    opts: SpawnOpts::default(),
})?;
```

Lifecycle of `accounts/acc-1`:

1. **Activation** — first `tell("accounts", Credit { account: "acc-1", .. })`. At route
   time the kernel extracts the shard key from the payload, derives the deterministic
   path `accounts/acc-1`, finds no live slot, calls the factory. ES spawn; message
   processed; `Credited` journalled; `apply` folds the balance.
2. **Passivation** — 30s with no completed message step → the runtime gracefully stops
   the entity (door closed, queued mail drained, `on_stop` run, tables removed). The
   journal stays in the store; memory is freed. The key lives on the **message** (routing)
   and in the **entity** (`restore` receives it merged into genesis args) — address on the
   envelope, name on the door.
3. **Re-activation** — the next `Credit` for `acc-1`, from any sender, to the same public
   path: factory called again, `restore` → snapshot + journal tail replayed through
   `apply` → balance restored → message processed. The re-spawned entity re-declares its
   handled schemas, so tell routing and publish fan-out both include it again.
   The sender never saw any of it.

Sharding ("same key → same entity, always") and memory-boundedness ("idle state leaves
the heap") fall out of this one mechanism. A **standalone** (non-partition) actor that
passivates simply stops — automatic re-activation for non-partition actors is parked,
by design, until a real need appears.

---

## 9. Persistence: the backend owns durability

- The default journal store is **in-memory**. Swapping backends means implementing the
  `JournalStore` trait and installing it at boot. Nothing else changes.
- **Append-before-ack:** every ES step awaits `store.append(path, &events)` — one batch
  per processed message — before the message is acknowledged. A backend may write
  through (durable immediately) or buffer in memory and assign seqs optimistically; the
  runtime doesn't care.
- **One flush:** the runtime calls `store.flush()` exactly once — during the graceful
  shutdown sweep, after every journal is final. It is a courtesy barrier for buffered
  backends, never a per-write contract.
- Therefore: **durability policy is the backend's promise.** A periodic-flush,
  high-throughput backend is explicitly anticipated: it buffers `append`s, flushes on a
  timer, treats the shutdown flush as its final barrier. A crash may lose the un-flushed
  tail — that is the backend's documented trade, the runtime's replay machinery stays
  correct over whatever the store retained.

---

## 10. Guarantees — the whole contract, in one place

**Delivery**

- Block backpressure is the default overload policy: a full mailbox makes the *sender
  wait*. Loss by overload is unrepresentable on the default policy; refusals under the
  other policies (`DropNew`/`DropOld`) dead-letter visibly.
- **Publish** = one copy per handler, in handler-declaration order; a closed endpoint
  (restart in flight) is skipped once, without failing the other deliveries, and without
  phantom delivery to the fresh instance.
- **Zero handlers of a published schema** = silent no-op (news nobody wanted is not an
  error). **Zero handlers of a `send_to_any`/tell** = the envelope comes back / DLQ.
- Restart swaps the endpoint under the same path identity; senders holding pre-crash
  paths never notice.

**Ordering** — per-sender per-mailbox FIFO. Publish fan-out walks the handler table in
declaration order. No global ordering across independent senders exists or should.

**Persistence** — ES: append-before-ack per message; replay = `restore` + snapshot +
tail. Service: consumed-on-handoff (at-most-once) — honest for stateless effectors.

**Observability** — every waist crossing is a tap fact (`Sent`, `Delivered`, `Acked`,
`Stopped`, `Escalated`, `DeadLettered`, …); refusals and unhandled schemas dead-letter
with a typed reason. "A missing consumer is a visible one-line diff": if an actor isn't
receiving a message, either its `.handles` line is missing (add it) or you've hit one of
the bounded races in §10's restart paragraph. It is never the transport.

---

## 11. Deliberately absent — and why (the anti-contamination section)

Trouper is not a distributed actor system, and it refuses to grow the parts that only
make sense over a network. If a suggestion below sounds reasonable, it is reasonable
*somewhere else*:

- **Location/remote transparency, node addresses, cluster membership, gossip** — one
  process. "Where" is not a question the fabric answers, because there is nowhere else.
- **Failure detectors, heartbeats, partial-failure handling** — the only failure is an
  in-process panic, and it is *known immediately* (supervision §5). Uncertainty about
  whether a peer died does not exist here.
- **At-least-once / exactly-once delivery guarantees across unreliable links** — the
  fabric is a function call through a queue; a message is delivered or it dead-letters.
  Redelivery exists in exactly one place: an ES actor's own inbox after a failed append.
- **Network backpressure / flow control / batching for the wire** — there is no wire
  (§6). Block policy is the backpressure; it is instantaneous and local.
- **Per-message delivery-mode flags** (`is_event`, `reliable`, `ordered`, `important`) —
  sender topology is a verb (§2); receiver identity is one declaration (§3). Flags are
  the split coming back through the window.
- **A router/pool/supervisor runtime type** — roles are compositions (§5). The runtime
  provides capability (routing table, supervision engines, partition activation), never
  domain roles.
- **Persistence in the fabric** — the fabric is in-memory by construction; durability
  begins and ends at the `JournalStore` backend boundary (§9).

---

## 12. Guidance for agents working on this code

**How to reason (the checklist):**

1. Is the proposed behavior a **sender-side topology choice**? Then it belongs in a verb
   (tell / send_to_any / publish) — not in declarations, not in message flags.
2. Is it a **receiver-side fact**? Then it belongs in `.handles` / handler code — and it
   must not be able to observe how the message arrived.
3. Is it **domain composition**? Then it belongs in a role actor at a spawn site
   (router, supervisor) — not in the fabric.
4. Is it **kernel capability that user code cannot do**? (Route-time activation of
   not-yet-existing paths is the canonical example — §8.) Only then does it belong in
   the runtime.

**Known traps — do not reintroduce:**

- A second receive-declaration ("subscribe", "observe", routing-mode enums on
  `.handles`). One declaration; the v0.4.0 unification exists because the split was wrong.
- Making publish consult only some handlers, or making handler dispatch depend on the
  delivery verb. The published-copy-needs-a-second-declaration hole is the bug this
  design exists to prevent.
- "Optimizing" the router hop (§6). Measure first; it will not be the router.
- Caching worker lists/cursors in domain code. The route table already does it; domain
  caches rot on restart.
- Adding `ask` (or any await) to `CmdCtx`. Sync-pure decision functions are the whole
  point of ES replay (§2, §7).
- An `is_event`/kind flag consulted by the fabric. Kind is metadata and the erased
  bridge's default hint — nothing more (§3, §7).
- Distributed-motivated features (§11). If the justification uses words like "cluster",
  "node", "network partition", "eventual", "exactly-once across machines" — stop.

**When debugging "a message didn't arrive":**

1. Does the receiver declare `.handles::<M>()`? If not — that's the one-line diff.
2. Was it a `publish` to a schema with zero handlers? Silent no-op by contract; check the
   `Sent` fact to confirm the publish happened.
3. Was the target mid-restart (one skipped copy) or stopped? Tap facts say so
   (`Stopped`, `Spawned { restart: true }`).
4. Check the DLQ (`dead_letter_count` / reasons). Every dead letter carries one of
   exactly eight typed reasons:
   - `Unresolvable` — no slot or route resolved (also what a zero-handler
     `send_to_any`/tell produces: no route to pick);
   - `UnknownSchema` — no handler registered for the payload's schema;
   - `Decode` — the payload did not decode against its registered schema;
   - `InboxRefused` — the destination inbox refused the envelope (overload/closed);
   - `StoppedWithMail` — the actor was stopped with undelivered inbox entries;
   - `ShuttingDown` — the graceful-shutdown barrier refused all new deliveries;
   - `UndeclaredEvent` — an ES actor recorded an event it never declared in `.emits`;
   - `ShardKeyMissing` — a partition-set command arrived without its shard key.
5. Only after all of the above: suspect the fabric. It will almost never be the fabric.

---

## 13. Glossary

- **Actor** — state + mailbox + behavior; processes one message at a time.
- **Path** — a virtual name for a mailbox (`"parse/worker-2"`). What's behind it can be
  replaced; the name persists.
- **Message** — a typed payload with a registered **schema** (`Name@version`). The only
  thing that crosses the fabric.
- **Event (ES)** — a fact an event-sourced actor *recorded* (the handler's return value).
  Provenance, not a wire property. After recording, it travels as an ordinary message.
- **Verb** — the sender's topology choice: `tell` (one path), `send_to_any` (one of the
  handlers of M, round-robin), `publish` (every handler of M). `ask` = tell + reply lease.
- **`.handles::<M>()`** — the single receive-declaration: "I process M." Installs route
  and dispatch entries together.
- **Route table** — the registry's schema→handlers mapping; source of truth for both
  one-of delivery and fan-out; owns membership, rotation, restart-pruning.
- **DLQ (dead-letter queue)** — where visibly-refused messages go, with a typed reason.
  The proof that "lost" is not a category in this runtime.
- **Supervision** — declared `ActorSpec` (policy/budget/backoff/parent) enforced by a
  runtime engine; budget exhaustion escalates as an `Escalated` *message* to the parent,
  an ordinary actor that owns the response.
- **Partition set** — kernel feature mapping a public path to per-key ES entities
  (`public/key`), activated on first message, passivated on idle, transparently
  re-activated by replay.
- **Journal / `JournalStore`** — an ES actor's facts, persisted through the store trait;
  append-before-ack, one flush at shutdown; durability policy belongs to the backend.
- **Passivation** — declarative idle stop (`passivate_after`); state leaves the heap and
  returns by replay on the next message (for partition entities).
