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

Daow SQLite journal benchmarks use `:memory:` and on-disk SQLite media. Values are Criterion point estimates (2026-09-25, Intel Core i5-10400, cores 0–5).

- `tell_acked` times command delivery, fold, journal append, and acknowledgement. SQLite is not on this path: daow acknowledges the buffered append.
- `pending` times one direct store flush of a fresh 512- or 2,048-event backlog. Fixture reset and appends are outside the timed body.
- `accumulated_backlog` times one direct flush of 2,048 events buffered since the previous reset.
- `retained_history` recreates and durably flushes 2,048 retained events, then times only the 64-event suffix flush.
- `noop_flush` times a second flush after the path is already durable.
- `replay` times `JournalStore::load()` for 2,048 events plus a snapshot. This is replay reconstruction, not a complete actor load: actor acquisition, state folding, and liveness are excluded. `cold` constructs a fresh on-disk store in setup and reads SQLite; `warmed` loads the authoritative in-memory journal.
- `accumulated_shutdown` accumulates 2,048 acknowledged actor messages in setup, then times graceful actor shutdown plus the final journal flush.

Because appends are buffered, a successful command acknowledgement does not yet imply SQLite durability. The periodic writer task and graceful-shutdown sweep provide the durable commit boundary.

| Bench                  | Media  | Size                 | Criterion time | Per event / message |            Rate |
| ---------------------- | ------ | -------------------- | -------------: | ------------------: | -------------: |
| tell_acked             | memory | 64 messages          |       78.02 µs |           1,219 ns |  820,270 msg/s |
|                       |        | 512 messages         |      477.24 µs |             932 ns | 1,072,800 msg/s |
|                       |        | 2,048 messages       |        1.94 ms |             946 ns | 1,057,000 msg/s |
| tell_acked             | disk   | 64 messages          |       84.33 µs |           1,318 ns |  758,960 msg/s |
|                       |        | 512 messages         |      503.32 µs |             983 ns | 1,017,200 msg/s |
|                       |        | 2,048 messages       |        1.93 ms |             944 ns | 1,059,700 msg/s |
| pending flush          | memory | 512 pending events   |      974.23 µs |           1,901 ns |  525,540 event/s |
|                       |        | 2,048 pending events |        4.15 ms |           2,025 ns |  493,720 event/s |
| pending flush          | disk   | 512 pending events   |      998.17 µs |           1,949 ns |  512,940 event/s |
|                       |        | 2,048 pending events |        4.40 ms |           2,149 ns |  465,310 event/s |
| accumulated backlog    | memory | 2,048 pending events |        4.77 ms |           2,327 ns |  429,720 event/s |
| accumulated backlog    | disk   | 2,048 pending events |        4.89 ms |           2,388 ns |  418,590 event/s |
| retained-history flush | memory | 2,048 retained + 64 |      226.32 µs |           3,537 ns |  282,790 event/s |
| retained-history flush | disk   | 2,048 retained + 64 |      260.23 µs |           4,066 ns |  245,940 event/s |
| no-op flush            | memory | 0 pending events     |      283.78 ns |                — |               — |
| no-op flush            | disk   | 0 pending events     |      300.13 ns |                — |               — |
| warmed replay          | memory | 2,048 + snapshot     |      403.95 µs |             197 ns |  2,475 replay/s |
| cold replay            | disk   | 2,048 + snapshot     |        2.01 ms |             984 ns |    496 replay/s |
| warmed replay          | disk   | 2,048 + snapshot     |      410.47 µs |             200 ns |  2,436 replay/s |
| accumulated shutdown   | memory | 2,048 messages       |        4.79 ms |           2,337 ns |  427,860 msg/s |
| accumulated shutdown   | disk   | 2,048 messages       |        5.08 ms |           2,479 ns |  403,310 msg/s |

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
