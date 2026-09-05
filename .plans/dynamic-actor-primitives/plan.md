# Dynamic Actor Primitives — Context-Rich Specification

Status: APPROVED PLAN (Socratic dialogue complete; all questions settled). Authoritative implementation reference; a fresh agent implements from this document alone. User amendments folded in: `SnapshotCadence` three-variant enum; **separate example file per feature** (the 805-line `examples/demo.rs` mega-file is retired to focused examples).

---

## Problem

The runtime has two spawn tiers (typed event-sourced, service) but lacks the primitives a canvas needs: per-entity actors (one per account/SKU), scale-out for overloaded actors, in-actor observation of system facts, and arbitrary event consumers. Additionally, `emits::<E>()` declarations are unverified claims (declared surface can silently diverge from reality — fatal for a canvas renderer), spawn APIs are unreadable (six positional parameters, anonymous closures, an `entries` closure full of adapter boilerplate), and `SnapshotPolicy` only supports message counts.

## Solution

Declarative **specs as registry data, interpreted by the kernel** — extending the existing registry-as-kernel thesis. No new actor *kinds*:

- **Partition sets (ES)**: senders address the public path; the kernel extracts the `ShardKey`-marked payload field, derives the entity path (`set/sku-123`), and activates the entity on demand from the shared factory. One journal per entity; senders never change.
- **Stateless pools**: path takeover (Group semantics); round-robin/random routing at slot-resolution time inside `route()` — no forwarding actor, no extra hop.
- **`system.facts` topic**: the tap mirrors facts into a subscribable topic; observers are ordinary actors (per-subscriber cursors, gap detection from fact offsets).
- **Router rules**: `(source?, schema?, dest?) → Tee | Inline` table. Landed as the mechanism with a seed rule set only; command snooping tiers come later.
- **Emit enforcement**: kernel filters undeclared events **before journal append**, emitting `DeadLettered(UndeclaredEvent)` + `tracing::error!` — journals contain only declared schemas.
- **Builder API**: typed (`spawn::<A>().at().handles::<C>().emits::<E>().emits_on_topic().snapshot_every().start()`), foreign (`.schema().handle().apply().start()`), installs (`install_pool`, `install_partition_set`). Positional spawns become deprecated wrappers.
- **`SnapshotCadence`**: `Off | Messages(Count) | Time(Duration)` (replaces `SnapshotPolicy::EveryN`).

## Dialectical Outcomes (Why)

1. **Pool interception at the kernel, not a pool actor (2A).** A pool is a routing decision, not a process: an actor in the forwarding path is itself a bottleneck/SPOF — the thing pooling exists to remove. Akka converged on this (`RoutedActorRef`); Orleans has no router at all (interception validates). Rejected: pool service actor (hop + serialization + death couples availability).
2. **Pool-path interception with schema-declared shard key (1A).** Senders address the public path forever; the key comes from a `FieldDef` marked `ShardKey`. Rejected: entity-addressed scheme (`set#key`) — forces key-extraction logic into every sender and creates a second addressing scheme for the canvas to render. Consequence: commands must carry the key; absence rejects without activation.
3. **Group semantics for pools** (Akka Pool-vs-Group): routees exist independently; the pool claims the public name via takeover. Workers are supervised children of the spec parent — escalation flows worker → pool's parent.
4. **Balance vs fan-out distinction.** Pools/partitions load-balance (each message → exactly one worker). Fan-out (all subscribers) remains topics-only. A copying pool would duplicate side effects (stateless) or fork journals (ES) — categorically wrong.
5. **ES pooling = entity partitioning, not cloning.** Cloning an ES actor means divergent states. The correct generalization is entities: many ES actors sharing one factory + declared surface, routing is key→entity determinism (path derivation is deterministic, so "same key → same entity" is structural — no consistent hashing needed in v1; hashing belongs to resharding, which is deferred). Rejected: journal replication (consensus/distribution — anti-goal).
6. **Filter model for emit enforcement, not fail-the-step (user's model, adopted over my strict model).** A deterministic handler that emits undeclared events will always do so — failing the step burns restart budget on a static condition redelivery can never heal. The declared surface is the *only* surface: undeclared events are dropped **pre-append** (journal purity: `fold(journal) = state` preserved, replay trivially consistent), with `DeadLettered(UndeclaredEvent)` fact + `tracing::error!`. Honest cost: a forgotten declaration silently swallows the event; the fact+log is the compensation. Commands are symmetric already (undeclared/undecodable → DLQ).
7. **Events vs commands for the alert use-case (projections).** Commands are attempts (may crash before ack); events are facts. Alerting/auditing belongs on the event stream → **projections are ordinary actors subscribing to topics** — zero new machinery, journaled, cursor-resettable. If the projection's own state must survive restarts, the observer is itself event-sourced. Command-level snooping is a *later* tier (tee/inline rules; teed copies are at-most-once and never an audit mechanism — to be recorded when exercised).
8. **Composition = placement tier.** After (projection/topic subscription), Beside (tee rule), In front (inline rule), System-level (`system.facts` topic). One mechanism per tier; loggers/alerters/snoopers are actors placed at a tier, never modifications of the things they watch.
9. **`tracing` vs tap vs observability — three channels.** `tracing` = developer diagnostics (errors, warnings, low-level detail; `tracing-subscriber` only as dev-dep for examples). Tap = product fact stream (untouched role). Observability = the future app (out of scope). The spec's old "no tracing crate" anti-goal is superseded by user decision; the record gains an entry stating the division.
10. **Builder over macros and positional soup.** Proc macros remain banned (anti-goal); positional parameters were first-draft ergonomics (anonymous closures, `Path` collision since renamed `ActorPath`, adapter boilerplate). Builder accumulates handles/emits/opts and resolves the double-typing (`TypedEsAdapter::<A, C>::new::<C>()` says everything twice — `.handles::<C>()` says it once).
11. **`SnapshotCadence` (user decision).** `Messages(Count)` is the industry default (recovery cost bounded deterministically; idle actors have short tails anyway). `Time(Duration)` serves steady-trickle actors that never reach N. `Off` for tiny-state actors. Time-based requires a wake source — the ES loop's existing 20ms poll arm is the hook (see Key Code Context).
12. **Defer passivation + live resharding.** Activation-on-demand is the core value; passivation and journal migration are the hard/expensive parts. Declared anti-goals; entities live until stopped.
13. **Takeover mail handling (v1).** Installing a pool over a live actor stop-drains it first (undelivered mail → DLQ, existing path). Re-routing queued mail into workers is a documented future refinement, not v1.
14. **`Random` pool algo without a new dependency**: seeded xorshift over an `AtomicU64`, no `rand` crate.

## Relevant Files (Where)

Workspace root `/mnt/zed/repos/actor-canvas`. All paths relative to it.

```
crates/actor-runtime/src/
  types.rs            # + SnapshotCadence (moved/replacing kernel::SnapshotPolicy), PoolAlgo
  schema.rs           # + FieldRole enum; FieldDef.role; ActorManifest unchanged shape
  registry.rs         # Registry: + pools: HashMap<ActorPath, PoolEntry>, partitions: HashMap<ActorPath, PartitionEntry>,
                      #   rules: Vec<Rule>; insert/resolve/install helpers
  kernel.rs           # route(): pool/partition/rule interception; es_actor_loop: Time-cadence wake;
                      #   step_es: emit filter between dispatch and append; fan_out_emits unchanged
  system.rs           # builder.rs types live in new file; spawn_es/spawn_es_foreign/spawn_service → deprecated wrappers;
                      #   install_pool/install_partition_set; SpawnOpts.snapshot: SnapshotCadence
  actor.rs            # unchanged (CommandEntry, TypedEsAdapter reused by builder)
  tap.rs              # FactKind: + Backpressured { path, depth }; Fact JSON gains "offset"
  topics.rs           # unchanged mechanics; system.facts topic created at boot
  builder.rs          # NEW: SpawnBuilder<A>, ForeignBuilder, install specs (PoolSpec, PartitionSpec)
  pool.rs             # NEW: PoolEntry/PartitionEntry/Rule/RuleAction types + router fns (xorshift Random)
  lib.rs              # + pub mod builder; pub mod pool; prelude additions
Cargo.toml (crate)    # + tracing (dep)
Cargo.toml (root/dev) # + tracing-subscriber (dev-dep, for examples)
examples/
  builder.rs          # NEW: typed + foreign builder spawns
  emit_contract.rs    # NEW: declared emits flow, undeclared dropped (fact + log)
  pools.rs            # NEW: install_pool takeover, distribution, escalation
  partitions.rs       # NEW: per-entity activation, key routing
  observers.rs        # NEW: system.facts observer with gap detection + DLQ re-driver
  demo.rs             # RETIRED → replaced by the five focused files (delete)
tests/ or #[cfg(test)] # per Test Strategies below
.agents/RECORD.md     # Record Updates at end of implementation ONLY
```

## Key Code Context (What)

Current state (verified; `ActorPath` rename already done by user).

`crates/actor-runtime/src/system.rs`:
```rust
pub struct SpawnOpts {
    pub snapshot: SnapshotPolicy,        // → SnapshotCadence
    pub mailbox_capacity: usize,
    pub mailbox_policy: OverloadPolicy,
}
pub fn spawn_es<A, F>(self: &Arc<Self>, path: ActorPath, args: &JsonValue,
    opts: SpawnOpts, entries: F) where A: EventSourcedActor, F: FnOnce() -> Vec<Arc<dyn CommandEntry>>
pub fn spawn_es_foreign(...)   // 6 positional params incl. two Arc<dyn Fn> closures
pub fn spawn_service<A, F>(...)
pub fn spawn_es_typed<A, F>(...)  // thin alias over spawn_es — fold into builder, remove

// spawn_es_erased body (the single funnel ALL ES spawns go through — builder targets this):
let mut registry = self.registry.lock().expect("registry lock");
registry.insert_slot(path.clone(), manifest.clone(), Endpoint::new(tx), opts.mailbox_policy)
    .expect("path free at spawn");
for schema in manifest.handles.clone() { registry.add_route(schema, path.clone()); }
// then kernel.cells/journals/es_state/entries/snapshot_policy inserts + Spawned fact
// + front door + es_actor_loop spawn
```

`crates/actor-runtime/src/kernel.rs`:
```rust
pub async fn route(registry: &Mutex<Registry>, kernel: &Mutex<KernelState>, envelope: Envelope)
    -> Result<ActorPath, Envelope>           // pool/partition/rule interception happens HERE
pub enum SnapshotPolicy { #[default] Off, EveryN(u64) }   // → types::SnapshotCadence
pub async fn es_actor_loop(loop_ctx: EsLoop, mut shutdown: watch::Receiver<bool>) {
    loop {
        match step_es(&loop_ctx).await {
            Step::Work => continue,
            Step::Idle => {}                  // ← Time-cadence snapshot check hooks here
            Step::Crashed => break,
        }
        let notified = loop_ctx.cell.work.notified();
        tokio::select! {
            _ = shutdown.changed() => {}
            _ = notified => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}  // ← wake source
        }
    }
    drain_inbox_on_stop(&loop_ctx).await;
}
async fn fan_out_emits(ctx: &EsLoop, events: &[crate::envelope::Event]) // reads manifest.emits_on_topics
```

The atomic step's emit-filter insertion point (between dispatch and append — journal purity depends on this exact position):
```
peek → decode → ctx → dispatch (catch_unwind) → [NEW: filter events vs manifest.emits]
→ journal.append → inbox.ack → apply → outbox.flush → fan_out_emits → tap.Acked / maybe snapshot
```

`crates/actor-runtime/src/schema.rs`:
```rust
pub struct FieldDef {
    pub name: String,
    pub ty: FieldTy,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub unit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub range: Option<Range>,
    // ... description
}                                            // + pub role: Option<FieldRole>
pub struct ActorManifest {
    pub handles: Vec<SchemaId>,              // routes + CommandEntry decode
    pub emits: Vec<SchemaId>,                // today: declarative only → becomes ENFORCED
    pub emits_on_topics: Vec<crate::types::Topic>,
    // kind, subscribes...
}
```

`crates/actor-runtime/src/registry.rs`:
```rust
pub struct Registry {
    schemas: SchemaTable,
    slots: HashMap<ActorPath, Slot>,
    routes: HashMap<SchemaId, RoutePolicy>,  // RoutePolicy::Single | RoundRobin already exists
    route_cursor: usize,
}   // + pools: HashMap<ActorPath, PoolEntry>, partitions: HashMap<ActorPath, PartitionEntry>, rules: Vec<Rule>
```

`crates/actor-runtime/src/tap.rs`: `Fact` ring with monotonic offsets; `impl From<&Fact> for JsonValue` (the boundary projection — facts-to-topic reuse this); `FactKind` gains `Backpressured { path: ActorPath, depth: u64 }`.

## Implementation Algorithm (How)

### Phase 1 — Emit enforcement
1. Move/rename `SnapshotPolicy` → `types::SnapshotCadence { #[default] Off, Messages(u64), Time(std::time::Duration) }` (user decision; do it first so the builder lands on the final type). Update `SpawnOpts`, `kernel.snapshot_policy` map, all uses. Time-cadence semantics: in `es_actor_loop`, on `Step::Idle`, check `last_snapshot.elapsed() >= cadence` before idling; the 20ms poll arm is the wake. Snapshot when due BETWEEN messages (existing rule).
2. Emit filter in `step_es`: after dispatch returns `Vec<Event>`, partition by `manifest.emits.contains(&ev.schema)`. Declared → append/ack/apply as today. Undeclared → `tracing::error!(actor = %path, schema = %ev.schema, "undeclared event dropped"); dead_letter(..., UndeclaredEvent, ...)` (new `DeadLetterReason::UndeclaredEvent`), **event dropped, not appended**. The step continues with the remainder (a partial decision is consistent: `apply` runs per appended event, so `state == fold(journal)` always).
3. Add `tracing` dep; add `tracing-subscriber` as workspace dev-dep (examples init it).
4. Update every test actor's manifest to declare what it emits (failures here are the feature working).

### Phase 2 — Builder API (`builder.rs`)
```rust
// Typed: one call says each type ONCE.
let h = system.spawn::<Inventory>().at("inventory")
    .args(json!({ "total": 100 }))
    .handles::<ReserveStock>()      // registers schema + CommandEntry + route
    .handles::<Restock>()
    .emits::<StockReserved>()       // schema + enforced emit edge
    .emits_on_topic("inventory.events")
    .snapshot(SnapshotCadence::Messages(100))
    .mailbox(capacity 64, OverloadPolicy::Block)
    .start()?;                       // → spawn_es_erased funnel
// Foreign: named methods replace anonymous closures; EventSourced vocabulary.
let f = system.spawn_foreign("tally")
    .schema(tally_schema).args(json!({"total": 0}))
    .handle(closure)                 // (state, cmd, ctx) -> Vec<Event>
    .apply(closure)                  // (state, ev)
    .start()?;
```
Builder internals: accumulate `handles: Vec<SchemaId>` + `entries: Vec<Arc<dyn CommandEntry>>` (constructing `TypedEsAdapter::<A, C>::new::<C>()` internally — the boilerplate dies here), `emits`, topics, `SpawnOpts` fields. `.start()` calls the existing `spawn_es_erased` / foreign funnel. Convert `spawn_es`/`spawn_service`/`spawn_es_foreign` to thin deprecated wrappers (keep tests compiling; mark `#[doc(hidden)]` + deprecation note; do not delete — external callers may exist).

### Phase 3 — Router rules + stateless pools (`pool.rs`, `registry.rs`, `kernel.rs`)
```rust
pub enum PoolAlgo { RoundRobin, Random }                       // Random: xorshift64 over AtomicU64 seed
pub struct PoolEntry { pub algo: PoolAlgo, pub workers: Vec<ActorPath>,
                       pub next: AtomicU64, pub spec_parent: Option<ActorPath> }
pub struct Rule { pub source: Option<ActorPath>, pub schema: Option<SchemaId>,
                  pub dest: ActorPath, pub action: RuleAction }
pub enum RuleAction { Tee(ActorPath) /* copy; original delivery untouched */,
                      Inline(ActorPath) /* interpose; interposer forwards */ }
```
- `install_pool(&system, spec: PoolSpec)`: if the public path is a live plain actor → graceful stop-drain (undelivered → DLQ, existing semantics; documented). Then one registry transaction: remove old slot, insert `PoolEntry`, spawn N workers via the spec factory as **children** (parent = spec parent or none), wire worker slots normally. Senders unchanged — they resolve the public path.
- `route()` interception order: rules matching (source/schema/dest) first (Tee duplicates the envelope — new causality id linked to the original trace, deliver both; Inline rewrites dest to the interposer), then `partitions`, then `pools` (algo picks worker; deliver to the worker's slot), then plain slots. Miss → existing DLQ.
- `Backpressured` fact: `SpawnOpts.high_watermark: Option<u64>` (default None); front door emits the fact when depth crosses it (rate-limited: only on crossing, not per message).
- v1 exercises Tee only as a seed example; Inline exists in the type + route path with a unit test (mechanism present, product use later).

### Phase 4 — Partition sets
1. `schema.rs`: `pub enum FieldRole { ShardKey }`; `FieldDef.role: Option<FieldRole>` (serde default None — old descriptors keep deserializing).
2. `PartitionSpec { public_path, factory, key_field: String, args_template }`. `install_partition_set` validates **at registration**: at least one command schema in the factory's manifest has the key field marked `ShardKey` (or `key_field` explicitly names a present field) → else reject with `RegistryError` (refuse-to-lie discipline).
3. `route()`: dest in `partitions` → read `key_field` from the envelope payload → `entity_path = ActorPath::new(format!("{public}/{key}"))` → if no slot, **activate**: call the factory (spawns an ES entity at that path with args `{key}` merged into the template; `Spawned` fact normal) → deliver to the entity slot. Key absent/unextractable → `DeadLettered(ShardKeyMissing)` (new reason), no activation.
4. Determinism: same key → same derived path → same entity/journal, structurally. Two keys → two entities with independent journals.

### Phase 5 — `system.facts` topic + observers
1. Boot creates `system.facts` topic log (like `system.deadletters`). `TapRing::push` additionally appends the fact's JSON projection (`From<&Fact> for JsonValue` already exists) into that topic log; the ring's drop-oldest rule is untouched. Fact JSON gains `"offset"` (the ring offset).
2. Observer pattern: ordinary `spawn_service` actor subscribing to `system.facts` (optionally with kind/path/schema filters — declared at subscription). Delivery = existing topic pump (try_send + per-subscriber policy; a slow observer never stalls the ring — drop happens at the ring, before the topic).
3. Gap detection: observer tracks last-seen fact `offset`; discontinuity (`new > last + 1`) → gap marker (observer writes it wherever it wants; the runtime only guarantees the offset sequence). This is what makes a disk logger "realtime with accounted-for losses".
4. DLQ re-driver: `ActorSystem::install_dlq_redriver()` — thin: spawns a service actor subscribed to `system.deadletters` that re-sends each dead letter's envelope to its recorded dest (from the DeadLetter JSON), tapping `Sent` facts like any sender. sugar over existing topic + cursor machinery.

### Phase 6 — Export + examples + docs
1. `SystemExport` gains: `pools: Vec<PoolExport { path, algo, workers }>`, `partitions: Vec<PartitionExport { path, key_field, entities: Vec<ActorPath> }>`, `rules: Vec<Rule>` (declared topology — the canvas's `source → pool → workers` lines draw from this; the observed router signature is already in tap facts: `Sent { dest: public }` → `Delivered { to: entity/worker }`).
2. Split `examples/demo.rs` into `builder.rs`, `emit_contract.rs`, `pools.rs`, `partitions.rs`, `observers.rs` (each focused, each inits tracing-subscriber, each prints its facts/export slice); delete `demo.rs`. Update the root README-lib docs if they reference the demo.
3. Annotate `.plans/*/plan.md` anti-goals if the runtime spec file carries them; ensure the deprecation docs on old spawn fns point at the builder.

## Acceptance Criteria

1. Emitting an undeclared event drops it pre-append with `DeadLettered(UndeclaredEvent)` + `tracing::error!`; declared events flow unchanged; journals contain only declared schemas (`fold(journal) = state` preserved even when a decision mixes declared + undeclared events).
2. `.handles()`/`.emits()` via builder register identical schema/edge/route/entry data as the positional spawns; positional spawns still work, marked deprecated. `SnapshotCadence::Messages(n)` behaves exactly like the old `EveryN(n)` (tests unchanged in outcome); `Time(Duration)` snapshots an idle actor; `Off` never snapshots.
3. Stateless pool: senders address the public path before/during/after install with zero changes; mail distributes per algo (round-robin rotation observed); workers are supervised children; escalation flows worker → spec parent; takeover stop-drains the replaced actor (undelivered → DLQ).
4. Partition set: two commands with different key values activate two entities, each with its own journal; same key always reaches the same entity (derived-path determinism); a command whose key is absent is dead-lettered without activation; spec registration fails when no command declares/has the shard key.
5. `system.facts` subscriber receives facts as messages; under ring pressure it observes an offset gap; a slow subscriber never stalls the ring (delivery unaffected for others).
6. Tee rule: monitor receives a copy with linked trace; primary delivery is unaffected (at-most-once property documented). Inline rule routes through the interposer.
7. Export shows declared pool/partition/rule topology; tap shows the router signature (public-path `Sent` → entity/worker-path `Delivered`, trace-linked); `Backpressured` fact fires on watermark crossing.
8. Five focused examples run green; `tracing` is a dependency of `actor-runtime` (`tracing-subscriber` dev-dep); all existing tests still pass; zero clippy warnings, fmt clean.

## Anti-Goals (Out of Scope)

- **Entity passivation** (idle entities live until stopped).
- **Live resharding / journal migration** (and consistent hashing — path derivation makes it unnecessary in v1).
- **Command tee/inline rules beyond the seed set** (mechanism lands in phase 3; product use + record entry come later).
- **Distribution/replication/consensus** of any kind (journal replication is categorically out).
- **Proc macros** (builder is plain Rust).
- **Autoscaling min/max workers** (PoolSpec takes N; scaling is a later canvas action).
- **Deleting positional spawns** (deprecated wrappers remain for compatibility).

## Edge Cases & Gotchas

- **Filter position is load-bearing**: the emit check must sit between dispatch and `journal.append`. Any later code that appends events without the check breaks journal purity (`fold(journal) = state` and replay consistency).
- **Partial decisions are legal**: dropping one undeclared event from a `[declared, undeclared]` return keeps state consistent because `apply` runs per appended event. Do not fail the step.
- **Time-cadence wake**: the ES loop idles in a `select!` with a 20ms poll arm — check `SnapshotCadence::Time` due-ness on the `Step::Idle` path there. Do not add a second timer task per actor.
- **Entity path collisions**: activation must use `insert_slot`'s fresh-path guarantee — a concurrent activation racing the same key must not clobber (double-activation guard: check-then-insert under the registry lock; loser of the race uses the winner's slot).
- **Key extraction is schema-aware, not stringly**: read `key_field` position/type from the schema def (numbers vs strings serialize differently in JSON).
- **Takeover mail**: installing over a live actor DLQs its undelivered mail (existing stop-drain). Re-routing into workers is a future refinement — document in `install_pool` docs.
- **Tee causality**: the copy gets a NEW causality id linked to the original trace (never reuse — two deliveries of one message must not look like a chain of two hops).
- **Facts topic vs ring offsets**: gaps appear when the RING drops (before mirroring); the topic log itself is bounded too — the observer's gap detection uses fact `offset`, not topic cursor position alone.
- **Deprecation, not deletion**: `spawn_es` etc. stay as wrappers; tests may keep using them where builder assertions aren't the point — but the emit-enforcement change will break any test actor with undeclared emits (fix manifests, don't weaken the filter).
- **Random algo determinism in tests**: seed the xorshift from the install call (injectable seed) so round-robin/random distribution tests are deterministic.

## Navigation Anchors

- Route interception (pools/partitions/rules): `crates/actor-runtime/src/kernel.rs` → `pub async fn route(...)` — the single delivery funnel.
- Emit filter: `kernel.rs` → `step_es` (atomic step), between dispatch and `journal.append`.
- Builder funnel target: `crates/actor-runtime/src/system.rs` → `spawn_es_erased` (all ES spawns go through it).
- Registry tables: `crates/actor-runtime/src/registry.rs` → `pub struct Registry` (+ `insert_slot`, `add_route`, `resolve`).
- Snapshot cadence: `kernel.rs` (`SnapshotPolicy` today) → moves to `types.rs` as `SnapshotCadence`; idle-wake hook in `es_actor_loop`.
- Facts→topic mirror: `crates/actor-runtime/src/tap.rs` → `TapRing::push` + `From<&Fact> for JsonValue`.
- Manifest/edge source of truth: `crates/actor-runtime/src/schema.rs` → `ActorManifest`, `FieldDef`.

## Dependency Mappings

New external deps:
- `tracing` — `crates/actor-runtime` (emit-enforcement errors, kernel diagnostics).
- `tracing-subscriber` — workspace dev-deps (examples init a subscriber).
Internal: builder.rs and pool.rs are new modules in `actor-runtime`; everything else reuses existing tokio/serde/uuid/arc-swap/wherror/error-stack/derive_more. No kameo. No `rand` (xorshift for Random).

## Test Strategies

Per existing project conventions: BDD Given/When/Then, one behavior per test, `FakeClock` for time, deterministic seeds for Random. Existing test fixtures (`ActorSystem::test()`, sink table, `wait_for`) are reused.

- Phase 1: `undeclared_emit_dropped_pre_append_with_trace_error` (no journal entry, no ack, fact, redelivery not poisoned); `mixed_decision_applies_declared_and_drops_undeclared` (fold-consistency); `snapshot_cadence_messages_matches_old_every_n` (existing snapshot tests renamed, outcomes unchanged); `snapshot_cadence_time_fires_on_idle` (FakeClock advance); `snapshot_cadence_off_never_snapshots`.
- Phase 2: `builder_registers_same_edges_as_positional_spawn` (schemas/routes/entries/manifest equality); `builder_emits_are_enforced_edges`; foreign builder parity with `spawn_es_foreign` (round-trip same tables); deprecated wrappers still function.
- Phase 3: `pool_takeover_is_invisible_to_senders` (same public path, distribution across workers under both algos); `pool_worker_escalates_to_spec_parent`; `tee_rule_copies_without_touching_delivery` (trace-linked copy); `inline_rule_interposes`; `backpressured_fact_fires_on_watermark_crossing`; takeover stop-drain → DLQ assertion.
- Phase 4: `partition_keys_activate_distinct_entities_with_separate_journals`; `partition_same_key_always_same_entity`; `partition_rejects_command_without_shard_key` (DeadLettered(ShardKeyMissing), no Spawned fact); `partition_spec_without_key_field_rejected` (install error); concurrent-activation race (two sends, same fresh key → one entity).
- Phase 5: `facts_subscriber_receives_and_detects_gap_under_pressure` (small ring + flood → offset discontinuity observed); `slow_facts_subscriber_never_stalls_ring` (other subscribers still served); `dlq_redriver_resends_dead_letters` (original dest receives, Sent facts appear).
- Phase 6: `export_shows_pool_partition_and_rule_topology`; examples run (`cargo run --example <each>`); existing suite green.

## Record Updates

Written to `.agents/RECORD.md` at END of implementation (via the Verification task), verified against the actual implementation first. If the implementation diverged from any entry, do NOT write a wrong entry — surface the divergence in the final summary instead. Planned verbatim entries:

- ADD: "Actor spawning is builder-based: typed actors declare `handles`/`emits` inline; foreign actors supply JSON schema plus handle/apply closures; positional spawn functions remain only as deprecated wrappers."
- ADD: "Event emission is declaration-filtered: the kernel drops events whose schema the actor has not declared, before journal append, with a dead-letter fact and a tracing error; journals therefore contain only declared schemas."
- ADD: "Pools and partition sets are declarative specs resolved by the kernel at route time, never forwarding actors: senders keep addressing the public path; partition sets derive per-entity paths from a schema-declared shard key and activate entities on demand; idle-entity passivation and live resharding are deliberately out of scope."
- ADD: "`tracing` is the developer-diagnostic channel (errors, warnings, low-level detail); the tap remains the product fact stream; product observability is a later, separate surface."
- ADD: "Runtime facts are consumable in-actor via the `system.facts` topic with per-subscriber cursors and gap detection; teed command copies (when introduced) are at-most-once and are not an audit mechanism."
