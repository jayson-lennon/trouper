# trouper

[![Crates.io](https://img.shields.io/crates/v/trouper.svg)](https://img.shields.io/crates/v/trouper.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/license/mit)
[![Repository](https://img.shields.io/badge/repository-GitHub-black)](https://github.com/jayson-lennon/trouper)

A single-machine actor runtime.

This crate is _not_ yet ready for general use. Built specifically for [`jinn`](https://github.com/jayson-lennon/jinn).

## Benchmarks

Three criterion suites (`cargo bench --bench competitors`, `--bench
micro`, `--bench journal`) on the development machine (Intel i5-10400,
12 threads, Linux, release profile). Competitors numbers are criterion
means, median-of-5 taskset-pinned alternating runs (each framework's
leg re-measured in the same round — machine noise hits every leg); the
other suites are single quiet-machine runs. Treat them as shape and
scale, not absolute promises — rerun `cargo bench` locally for your
hardware. An element is always one fully processed message, one flush,
or one frame read — never a spawn or a harness step.

### competitors

The same prime-then-spam harness driving trouper, kameo, and ractor:
one producer actor hot-loops `n` tells at one sink actor. Every send
leaves a handler (actor→actor, the realistic shape) — nothing is
injected at the edge. The harness owns kanal control channels: the
producer parks inside its handler awaiting a start signal, the sink
fires a done signal at the target count, and the criterion timed body
is ONLY `start.send(())` → `done.recv()` (auditable in one 5-line
function, byte-identical for every framework). Runtime construction,
actor spawn, priming, and settle sit in `iter_batched`'s untimed setup;
one element = one fully processed message. Mailboxes follow each
framework's supported set:

| runtime           | mailbox                                    |
| ----------------- | ------------------------------------------ |
| trouper           | bounded-64 (its default, Block policy)     |
| trouper-unbounded | 2^20-capacity Block (no true unbounded)    |
| kameo             | bounded-64 (its default)                   |
| kameo-unbounded   | native unbounded                           |
| ractor            | native unbounded (core has no bounded API) |

trouper's producer declares `.emits::<Tick>()` at spawn — the flush
gate dead-letters undeclared outbound schemas, so that declaration is
part of its send path.

| Runtime           | tells | Criterion time | Per tell | Rate          |
| ----------------- | ----- | -------------: | -------: | ------------: |
| trouper           | 64    |      223.44 µs | 3,491 ns |   286,435 t/s |
|                   | 512   |      545.59 µs | 1,066 ns |   938,429 t/s |
|                   | 2048  |        1.74 ms |   849 ns | 1,177,673 t/s |
| trouper-unbounded | 64    |      210.64 µs | 3,291 ns |   303,833 t/s |
|                   | 512   |      603.77 µs | 1,179 ns |   848,012 t/s |
|                   | 2048  |        1.85 ms |   901 ns | 1,109,500 t/s |
| kameo             | 64    |      178.78 µs | 2,794 ns |   357,973 c/s |
|                   | 512   |      331.66 µs |   648 ns | 1,543,735 c/s |
|                   | 2048  |      821.85 µs |   401 ns | 2,491,929 c/s |
| kameo-unbounded   | 64    |      181.90 µs | 2,842 ns |   351,846 c/s |
|                   | 512   |      293.68 µs |   574 ns | 1,743,416 c/s |
|                   | 2048  |      724.92 µs |   354 ns | 2,825,142 c/s |
| ractor            | 64    |      178.36 µs | 2,787 ns |   358,833 c/s |
|                   | 512   |      265.03 µs |   518 ns | 1,931,886 c/s |
|                   | 2048  |      567.48 µs |   277 ns | 3,608,950 c/s |

Shape read: at small batches every framework is parked near the same
wake-up floor (~170-220 µs covers the producer's resume, the whole
in-flight batch, and the done signal). As the batch grows, per-message
cost separates: the unbounded kameo/ractor mailboxes climb toward
~2.5-3.6M c/s while trouper's journaled-style path — every tell is
schema-routed, outbox-recorded, and flush-gated, with the payload as a
live value end to end (zero serde on the message path; serde exists
only at the journal door) — holds ~850K-1.18M t/s at 2048. The
bounded leg is trouper's FASTEST at scale: each step claims the batch
by MOVING envelopes out of their inbox slots (tombstones hold the
positions — zero per-message clones, the old snapshot's refcount
traffic is gone), and a Block-refused tell parks on a space-available
notify instead of polling (the commit wakes it). Those two changes took
the bounded leg from ~476K to ~1.18M t/s at 2048 (~2.2-2.8× its
pre-claim median) while the unbounded leg and the kameo/ractor
controls sat flat (±7%). That is the architectural trade trouper makes
for its registry/journal guarantees, priced honestly against the plain
tell machines.

### micro

Component costs without a running system: in-memory journal append
(per-batch ns) and replay/restart costs. `cargo bench --bench micro`.

| Bench                            | Case            |      Mean |
| -------------------------------- | --------------- | -------: |
| in_memory_journal_append         | batch_1         | 71.40 ns |
|                                  | batch_8         | 263.50 ns |
|                                  | batch_64        | 1.76 µs |
| in_memory_journal_replay_restart | journal_1000    | 56.06 µs |
|                                  | journal_10000   | 569.14 µs |
|                                  | repeat_load_10k | 779.12 µs |

### journal

SQLite-backed tell commits through the daow journal
(`cargo bench --bench journal --features daow`). `tell_acked` prices
the full send→fold→journal-ack cycle per medium (`:memory:` = the pure
SQLite floor; disk = WAL fsyncs included); system + journal build once
and each iteration gets a fresh entity in untimed setup, so the timed
body is only the tells plus the commit-cursor wait.
`flush_price` prices one direct store flush at two batch sizes — the
write-behind drain the ack path defers.

Also see `examples/idle_burn.rs`, a CPU-seconds probe for idle fleets
(5,000 duty-armed idle actors burn ~0.003 CPU-seconds per second of
wall; ~100× less than a polled runtime).
