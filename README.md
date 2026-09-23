# trouper

[![Crates.io](https://img.shields.io/crates/v/trouper.svg)](https://crates.io/crates/trouper)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/license/mit)
[![Repository](https://img.shields.io/badge/repository-GitHub-black)](https://github.com/jayson-lennon/trouper)

A single-machine actor runtime.

This crate is _not_ yet ready for general use. Built specifically for [`jinn`](https://github.com/jayson-lennon/jinn).

## Benchmarks

Three criterion suites (`cargo bench --bench competitors`, `--bench
micro`, `--bench journal`) on the development machine (Intel i5-10400,
12 threads, Linux, release profile). Numbers are criterion means from a
quiet-machine run; treat them as shape and scale, not absolute
promises — rerun `cargo bench` locally for your hardware. An element is
always one fully processed message, one flush, or one frame read —
never a spawn or a harness step.

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
| trouper           | 64   |       316.6 µs |    4.90 µs |   202,163 msg/s |
|                   | 512  |        3.79 ms |    7.40 µs |   135,088 msg/s |
|                   | 2048 |        4.89 ms |    2.40 µs |   419,187 msg/s |
| trouper-unbounded | 64   |       338.5 µs |    5.30 µs |   189,069 msg/s |
|                   | 512  |       949.4 µs |    1.90 µs |   539,277 msg/s |
|                   | 2048 |        3.46 ms |    1.70 µs |   591,461 msg/s |
| kameo             | 64   |       267.0 µs |    4.20 µs |   239,696 msg/s |
|                   | 512  |       435.1 µs |     850 ns | 1,176,618 msg/s |
|                   | 2048 |       913.2 µs |     446 ns | 2,242,673 msg/s |
| kameo-unbounded   | 64   |       282.4 µs |    4.40 µs |   226,661 msg/s |
|                   | 512  |       395.0 µs |     772 ns | 1,296,045 msg/s |
|                   | 2048 |       791.4 µs |     386 ns | 2,587,901 msg/s |
| ractor            | 64   |       263.8 µs |    4.10 µs |   242,566 msg/s |
|                   | 512  |       376.4 µs |     735 ns | 1,360,147 msg/s |
|                   | 2048 |       677.4 µs |     331 ns | 3,023,506 msg/s |

Shape read: at small batches every framework is parked near the same
wake-up floor (~260-340 µs covers the producer's resume, the whole
in-flight batch, and the done signal). As the batch grows, per-message
cost separates: the unbounded kameo/ractor mailboxes climb toward
~2.6-3.0M msg/s while trouper's journaled-style path — every tell is
schema-routed, outbox-recorded, and flush-gated, with the payload as a
live value end to end (zero serde on the message path; serde exists
only at the journal door) — holds ~420-590K msg/s at 2048. That is the
architectural trade trouper makes for its registry/journal guarantees,
priced honestly against the plain tell machines.

### micro

Component costs without a running system: in-memory journal append
(per-batch ns) and replay/restart costs. `cargo bench --bench micro`.

| Bench                            | Case            |      Mean |
| -------------------------------- | --------------- | -------: |
| in_memory_journal_append         | batch_1         | 72.02 ns |
|                                  | batch_8         | 32.04 ns |
|                                  | batch_64        | 27.58 ns |
| in_memory_journal_replay_restart | journal_1000    | 53.62 µs |
|                                  | journal_10000   | 561.07 µs |
|                                  | repeat_load_10k | 743.88 µs |

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
