# ES in-memory journal path: where the per-message time goes

Sample-share accounting of the **in-memory-journaled ES cycle** — the
full `system.tell → route → claim → dispatch → decide → emit filter →
journal append → commit → apply (fold) → outbox flush → fan-out →
snapshot check` loop that every event-sourced message runs. Captured
with the same `release-debug` + frame-pointer perf rig as
`bench-accounting.md`; reduced by `tools/fold_accounting.py` (same
buckets). Raw evidence committed as `bench-accounting-es.folded`.

The ask was: **command processing and event application, in-memory
journal only, clear big wins only.** The store is exonerated; the costs
live in what surrounds it.

## Provenance

- Date: 2026-09-24. Cores 0-5 (`taskset` wraps perf + child), quiet box.
- Shape: `examples/flame_tells.rs` ES leg — edge `system.tell(Tick)` at
  one ES entity (`Accum`, `Tick → Ticked` one event per command,
  in-memory journal via `SystemConfig::production()` default,
  observation off). This is the exact committed-cycle shape: one event
  per command, no subscribers.
- Build: `RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile
  release-debug --example flame_tells`. 0% `[unknown]` frames.
- Capture: `PROFILE_ES=1 MESSAGES=5000000 taskset -c 0-5 perf record -F
  9999 --call-graph fp` → 5.737 s, 871,491 msg/s (**1,147 ns/msg**),
  138,956 samples.

## The cycle's cost map (inclusive, share of all samples)

| stage | share | ≈ ns/msg |
|---|---:|---:|
| `step_es` total (claim → decide → filter → append → commit → apply → fan-out) | 44.7% | 513 |
| route (send side: rules, set probes, endpoint resolve, push) | 21.1% | 242 |
| **fan_out + broadcast** | **14.9%** | **171** |
| handler decide (`Json::of` serde tree) | 8.1% | 93 |
| `flush_outbox` (empty outbox, gate check) | 2.7% | 31 |
| journal append (in-memory store) | 1.7% | 20 |
| **apply (the fold itself)** | **0.13%** | **1** |
| maybe_snapshot (cadence check, not due) | 0.07% | 1 |

Self-time buckets across the whole run: alloc 42.6%, trouper 21.9%,
sync-prims 18.8%, unwound-libc 13.1%, clock 1.9%, tokio 1.6%.

## Findings

### 1. The zero-subscriber fan-out is pure tax — ~171 ns/msg (14.9%)

Every recorded event is unconditionally materialized into a fresh
`Envelope` (three `SchemaId` clones, two `path.clone()` — `Arc<str>`
malloc/free pairs — plus the `RecordedOrigin` stamp) and pushed through
`broadcast()`, which takes the registry lock, allocates two `Vec`s, and
runs `projector_set_owning` — which does `path.strip_prefix(&format!("{}/{}",
…))`, i.e. a **`format!` allocation per candidate path per event** — and
a second registry lock for `projector_sets_consuming` — all *before*
learning there are zero handlers. In this shape (no consumers at all)
every one of those bytes is dropped immediately: the envelope dies at
the end of `broadcast`, its payload Arc never shared.

The registry could answer "does anyone consume `Ticked`?" with one
lock-held probe (`routes.get(schema).is_none() &&
projector_sets_consuming(schema).is_empty()`) and the whole stage
disappears for unsubscribed schemas. When subscribers exist, the
current construction is still one envelope per event per... no — one
envelope total, cloned per delivery; the fix is strictly the early-out.

**This is the biggest ES-specific lever.**

### 2. The decision's serde tree — ~93–165 ns/msg (8.1% + free side)

The example handler (and the natural-looking API) builds events as
`Event::from_json_view(Ticked::schema_id(), Json::of(&Ticked { n: 1 }))`.
`Json::of` runs `serde_json::to_value` — a `Value` tree (Object + String
key + i64) allocated and freed per command — and the typed value is
gone; the fabric then carries a JSON view instead of a live value. The
typed constructor `Events::one(Ticked { n: 1 })` wraps the live value
with zero serde (the same two Arcs the send path already pays).

A/B on this worktree (1M messages each, same rig):

| decision style | rate | ns/msg |
|---|---:|---:|
| `from_json_view` + `Json::of` | 956,342 msg/s | 1,046 |
| `Events::one(Ticked { n: 1 })` (typed) | 1,135,478 msg/s | **881** |

**−165 ns/msg (−16%)** from the handler choosing the typed constructor.
Nothing else changed. On the in-memory path the wire encoding is never
taken (only the daow door serializes, lazily, memoized), so the typed
value's "cost" is zero until some reader asks for JSON. The `apply`
side is unchanged (0.13% either way — the fold is not the cost).

Caveat: `Json::of` is user code; the runtime's job here is that the
typed path must stay the lazy one it is (payload → wire only at the
journal door that needs bytes), and that `from_json_view`'s doc should
say it pays a tree per call.

### 3. Allocation dominates the cycle — 42.6% self

The alloc self-time decomposes as: commit teardown (`ClaimGuard` drop →
Envelope/payload frees) 16.4%, route path clones 14.0%, **the edge
tell's own `Arc<Tick>`+`Arc<PayloadCell>` construction 12.6%**, the
`Json::of` tree 12.3%, **fan-out envelope construction 10.4%**, plus
scattered `Arc<str>` drops and `EventOrigin`/`PayloadCell` glue. The
unique-payload fast path and path-borrow fixes identified in
`bench-accounting.md` apply here identically — the ES cycle adds the
fan-out envelope and the journal's per-event `Event` clone (an Arc bump,
cheap) on top.

### 4. What is already fine

- **The in-memory store: 20 ns/msg append, 1.7%.** `Event` clone per
  append is an Arc bump; the journals lock is uncontended (single
  writer). Nothing to optimize there.
- **The fold (`apply`): ~1 ns/msg.** Borrowing the event by reference,
  zero copies. Any "faster event application" work aimed at `apply`
  would be optimizing nothing.
- **Claim/commit machinery: <1%** (claim 0.37%, commit_through 0.46%),
  same as the service leg.
- **Emit filter / snapshot cadence checks: ~0%.**
- The state lock (tokio `Mutex<Box<dyn DynEsActor>>`) is acquired once
  per batch for the decide pass and once for apply: 0.12% — not a
  per-message cost.

### 5. Where the rest lives

Route (242 ns/msg) is byte-for-byte the service path's cost — same
`Arc<str>` clone chain, same arc_swap debt probes, same single
registry critical section; `bench-accounting.md`'s items 2–3 cover it.
Sync-prims 18.8% here is mostly the ES loop's park path between
batches (44% of the bucket) — idle cost amortizing over the run, not
per-message work.

## The clear big wins, ranked

1. **Zero-subscriber fan-out early-out** (~171 ns/msg, −15%): one
   registry probe before materializing the broadcast envelope. ES-tier
   specific, mechanical, no contract change (subscribed schemas keep
   the exact current path).
2. **Typed event construction** (−165 ns/msg, −16% as measured): a
   handler-side choice the runtime should keep cheap and document;
   `from_json_view` should not be the example code's shape.
3. **Shared with the service path** (from `bench-accounting.md`):
   unique-payload fast path and path-clone elimination (~200+ ns/msg
   across both tiers).

Combined realistic floor for this shape: 1,147 → roughly 650–750
ns/msg without touching the store or the fold, matching the service
actor's 800 ns plus the now-cheap journal steps.

## Full-completion ES fan-out before/after

The direct-path change makes an accepted plain-subscriber broadcast copy
push straight into the destination inbox from the publishing ES task. A
refused copy still enters the existing front-door channel and retains
Block/DropNew/DropOld behavior. The front-door loop also moves its owned
envelope into the inbox after copying the `Copy` trace instead of cloning
the envelope to retain that trace.

`examples/profile_es_fanout.rs` measures one typed `Ticked` event per ES
command with the in-memory journal. Its timed window opens before the N
commands and closes only after the emitter and every subscriber have
committed N messages, so these are full producer-plus-consumer rates—not
enqueue-only timings.

| plain subscribers | before ns/cmd | after ns/cmd | delta | before cmd/s | after cmd/s |
|---:|---:|---:|---:|---:|---:|
| 0 | 932 | 909 | −2.5% | 1,073,342 | 1,100,701 |
| 1 | 1,621 | 1,240 | **−23.5%** | 616,889 | 806,454 |
| 4 | 2,665 | 2,101 | **−21.2%** | 375,182 | 476,033 |

The profiles corroborate the path change. With one subscriber,
`front_door_loop` fell from 28.1% to 9.4% of all samples; with four, from
24.9% to 3.3%. Within `broadcast` stacks specifically, kanal was 6.9% of
the before samples at four subscribers and 0.0% after; `direct_push` was
0.0% before and 32.5% after. All six captures had 0.00% `[unknown]`
frames. Folded evidence is committed as
`bench-accounting-es-fanout-{before,after}-{0,1,4}.folded`.

Reproduction (one leg per process, six pinned CPUs, 64,000 warmup commands,
and 1,000,000 measured commands):

```sh
RUSTFLAGS="-C force-frame-pointers=yes" \
  cargo build --profile release-debug --example profile_es_fanout
for subscribers in 0 1 4; do
  SUBSCRIBERS="$subscribers" MESSAGES=1000000 WARMUP=64000 \
    taskset -c 0-5 perf record -F 9999 --call-graph fp \
    -o "/tmp/es-fanout-${subscribers}.data" -- \
    target/release-debug/examples/profile_es_fanout
  perf script -i "/tmp/es-fanout-${subscribers}.data" \
    | stackcollapse-perf.pl \
    > "bench-accounting-es-fanout-${subscribers}.folded"
done
```
