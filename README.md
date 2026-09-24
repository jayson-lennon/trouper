# trouper

[![Crates.io](https://img.shields.io/crates/v/trouper.svg)](https://crates.io/crates/trouper)
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

| leg               | mailbox                                    |
| ----------------- | ------------------------------------------ |
| trouper           | bounded-64 (its default, Block policy)     |
| trouper-unbounded | 2^20-capacity Block (no true unbounded)    |
| kameo             | bounded-64 (its default)                   |
| kameo-unbounded   | native unbounded                           |
| ractor            | native unbounded (core has no bounded API) |

trouper's producer declares `.emits::<Tick>()` at spawn — the flush
gate dead-letters undeclared outbound schemas, so that declaration is
part of its send path.

| Leg               | n    | Criterion time | Per message | Rate          |
| ----------------- | ---- | -------------: | ----------: | ------------: |
| trouper           | 64   |       220.0 µs |    3,438 ns |   290,856 msg/s |
|                   | 512  |        1.44 ms |    2,808 ns |   356,149 msg/s |
|                   | 2048 |        3.83 ms |    1,872 ns |   534,126 msg/s |
| trouper-unbounded | 64   |       216.3 µs |    3,380 ns |   295,872 msg/s |
|                   | 512  |       592.6 µs |    1,158 ns |   863,916 msg/s |
|                   | 2048 |        1.89 ms |      925 ns | 1,081,595 msg/s |
| kameo             | 64   |       186.0 µs |    2,907 ns |   344,012 msg/s |
|                   | 512  |       330.4 µs |      645 ns | 1,549,871 msg/s |
|                   | 2048 |       834.1 µs |      407 ns | 2,455,429 msg/s |
| kameo-unbounded   | 64   |       179.2 µs |    2,800 ns |   357,083 msg/s |
|                   | 512  |       293.2 µs |      573 ns | 1,746,487 msg/s |
|                   | 2048 |       722.1 µs |      353 ns | 2,836,251 msg/s |
| ractor            | 64   |       174.5 µs |    2,727 ns |   366,699 msg/s |
|                   | 512  |       268.7 µs |      525 ns | 1,905,329 msg/s |
|                   | 2048 |       572.4 µs |      279 ns | 3,578,230 msg/s |

Shape read: at small batches every framework is parked near the same
wake-up floor (~170-220 µs covers the producer's resume, the whole
in-flight batch, and the done signal). As the batch grows, per-message
cost separates: the unbounded kameo/ractor mailboxes climb toward
~2.5-3.6M msg/s while trouper's journaled-style path — every tell is
schema-routed, outbox-recorded, and flush-gated, with the payload as a
live value end to end (zero serde on the message path; serde exists
only at the journal door) — holds ~530K-1.08M msg/s at 2048 (the
bounded leg ~1.6× and the unbounded leg ~1.8× its pre-hot-path medians:
the inbox is a sync parking_lot lock, entry tables resolve once per
batch, and a plain-path send takes one registry critical section).
That is the architectural trade trouper makes for its registry/journal
guarantees, priced honestly against the plain tell machines.

### micro

Component costs without a running system: in-memory journal append
(per-batch ns) and replay/restart costs. `cargo bench --bench micro`.

| Bench                            | Case            |      Mean |
| -------------------------------- | --------------- | -------: |
| in_memory_journal_append         | batch_1         | 70.75 ns |
|                                  | batch_8         | 253.26 ns |
|                                  | batch_64        | 1.72 µs |
| in_memory_journal_replay_restart | journal_1000    | 54.04 µs |
|                                  | journal_10000   | 569.80 µs |
|                                  | repeat_load_10k | 756.43 µs |

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
