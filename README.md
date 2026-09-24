# trouper

[![Crates.io](https://img.shields.io/crates/v/trouper.svg)](https://img.shields.io/crates/v/trouper.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/license/mit)
[![Repository](https://img.shields.io/badge/repository-GitHub-black)](https://github.com/jayson-lennon/trouper)

A single-machine actor runtime.

This crate is _not_ yet ready for general use. Built specifically for [`jinn`](https://github.com/jayson-lennon/jinn).

## Benchmarks

The competitor benchmark hot-loops a burst of actor-to-actor messages from one producer to one sink. Every iteration uses a fresh runtime and actor pair; only the start signal through sink completion leaks into the bench timing. Construction, priming, the 50 ms settlement, runtime release, and thread joining are excluded. Values are Criterion point estimates (2026-09-24, Intel Core i5-10400, cores 0–5).

`ractor` does not have a bounded API.

| Runtime                 | msg burst | Criterion time | Per message |             Rate |
| ----------------------- | --------- | -------------: | ----------: | ---------------: |
| trouper-tell-bounded-64 | 64        |       76.83 µs |    1,201 ns |   832,976 tell/s |
|                         | 512       |      334.65 µs |      654 ns | 1,529,940 tell/s |
|                         | 2048      |        1.16 ms |      565 ns | 1,770,563 tell/s |
|                         | 50000     |       28.77 ms |      575 ns | 1,737,825 tell/s |
| trouper-unbounded-tell  | 64        |       78.93 µs |    1,233 ns |   810,846 tell/s |
|                         | 512       |      354.76 µs |      693 ns | 1,443,227 tell/s |
|                         | 2048      |        1.29 ms |      628 ns | 1,592,438 tell/s |
|                         | 50000     |       32.61 ms |      652 ns | 1,533,196 tell/s |
| kameo-tell-bounded-64   | 64        |       53.93 µs |      843 ns | 1,186,792 tell/s |
|                         | 512       |      207.26 µs |      405 ns | 2,470,285 tell/s |
|                         | 2048      |      695.27 µs |      339 ns | 2,945,612 tell/s |
|                         | 50000     |       15.78 ms |      316 ns | 3,167,968 tell/s |
| kameo-unbounded-tell    | 64        |       48.54 µs |      758 ns | 1,318,410 tell/s |
|                         | 512       |      172.31 µs |      337 ns | 2,971,423 tell/s |
|                         | 2048      |      573.94 µs |      280 ns | 3,568,290 tell/s |
|                         | 50000     |       13.50 ms |      270 ns | 3,702,659 tell/s |
| ractor-unbounded-cast   | 64        |       43.14 µs |      674 ns | 1,483,386 cast/s |
|                         | 512       |      141.30 µs |      276 ns | 3,623,388 cast/s |
|                         | 2048      |      466.25 µs |      228 ns | 4,392,536 cast/s |
|                         | 50000     |       11.21 ms |      224 ns | 4,462,186 cast/s |

Reproduce with `taskset -c 0-5 cargo bench --bench competitors`.

Daow SQLite journal benches using the default 64-message mailbox with `:memory:` and on-disk
SQLite media, reported as Criterion point estimates (2026-09-24, Intel Core i5-10400, cores 0–5).

- `tell-acked` measures the complete flow of receiving a command, emitting an event, and then the journal receiving the event.
- `flush` measures the journal committing buffered events

Note: Journal implementations that ship with `trouper` buffer events in memory, so some data loss is possible between flushes. Committing to disk happens on a separate thread.

| Runtime                | msg burst   | Criterion time | Per message |           Rate |
| ---------------------- | ----------- | -------------: | ----------: | -------------: |
| daow-tell-acked-memory | 64          |       92.70 µs |    1,449 ns |  690,366 msg/s |
|                        | 512         |      568.69 µs |    1,111 ns |  900,307 msg/s |
|                        | 2048        |        2.14 ms |    1,043 ns |  958,436 msg/s |
| daow-tell-acked-disk   | 64          |       78.93 µs |    1,233 ns |  810,856 msg/s |
|                        | 512         |      575.35 µs |    1,124 ns |  889,901 msg/s |
|                        | 2048        |        2.13 ms |    1,040 ns |  961,222 msg/s |
| daow-flush-memory      | 512 events  |       26.71 ms |   52,166 ns | 19,169 event/s |
|                        | 2048 events |       71.43 ms |   34,878 ns | 28,671 event/s |
| daow-flush-disk        | 512 events  |       26.09 ms |   50,954 ns | 19,626 event/s |
|                        | 2048 events |       72.30 ms |   35,301 ns | 28,327 event/s |

Reproduce with `taskset -c 0-5 cargo bench --bench journal --features daow`.

Overhead of journal, per message:

| Bench                            | Case            |      Mean |
| -------------------------------- | --------------- | --------: |
| in_memory_journal_append         | batch_1         |  71.40 ns |
|                                  | batch_8         | 263.50 ns |
|                                  | batch_64        |   1.76 µs |
| in_memory_journal_replay_restart | journal_1000    |  56.06 µs |
|                                  | journal_10000   | 569.14 µs |
|                                  | repeat_load_10k | 779.12 µs |
