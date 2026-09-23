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
| trouper           | 64   |       318.5 µs |    4.98 µs  |   200,923 msg/s |
|                   | 512  |         3.97 ms |    7.75 µs |   129,063 msg/s |
|                   | 2048 |         4.92 ms |    2.40 µs |   416,096 msg/s |
| trouper-unbounded | 64   |       332.8 µs |    5.20 µs  |   192,308 msg/s |
|                   | 512  |       970.3 µs |    1.90 µs  |   527,699 msg/s |
|                   | 2048 |         3.45 ms |    1.69 µs |   593,014 msg/s |
| kameo             | 64   |       276.9 µs |    4.33 µs  |   231,106 msg/s |
|                   | 512  |       432.2 µs |    0.84 µs  | 1,184,539 msg/s |
|                   | 2048 |         1.27 ms |    0.62 µs | 1,618,594 msg/s |
| kameo-unbounded   | 64   |       267.9 µs |    4.19 µs  |   238,886 msg/s |
|                   | 512  |       749.5 µs |    1.46 µs  |   683,138 msg/s |
|                   | 2048 |       786.4 µs |    0.38 µs  | 2,604,278 msg/s |
| ractor            | 64   |       263.5 µs |    4.12 µs  |   242,872 msg/s |
|                   | 512  |       580.3 µs |    1.13 µs  |   882,339 msg/s |
|                   | 2048 |       785.3 µs |    0.38 µs  | 2,607,808 msg/s |

Shape read: at small batches every framework is parked near the same
wake-up floor (~260-330 µs covers the producer's resume, the whole
in-flight batch, and the done signal). As the batch grows, per-message
cost separates: the unbounded kameo/ractor mailboxes climb toward
~2.6M msg/s while trouper's journaled-style path — every tell is
schema-routed, outbox-recorded, and flush-gated — holds ~420-590K msg/s
at 2048. That is the architectural trade trouper makes for its
registry/journal guarantees, priced honestly against the plain tell
machines.

### micro

Component costs without a running system: in-memory journal append
(per-batch ns) and replay/restart costs. `cargo bench --bench micro`.

| Bench                            | Case            |      Mean |
| -------------------------------- | --------------- | -------: |
| in_memory_journal_append         | batch_1         | 71.79 ns |
|                                  | batch_8         | 32.55 ns |
|                                  | batch_64        | 27.79 ns |
| in_memory_journal_replay_restart | journal_1000    | 54.080 µs |
|                                  | journal_10000   | 566.30 µs |
|                                  | repeat_load_10k | 765.40 µs |

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
