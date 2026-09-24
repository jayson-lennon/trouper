# trouper

[![Crates.io](https://img.shields.io/crates/v/trouper.svg)](https://img.shields.io/crates/v/trouper.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/license/mit)
[![Repository](https://img.shields.io/badge/repository-GitHub-black)](https://github.com/jayson-lennon/trouper)

A single-machine actor runtime.

This crate is _not_ yet ready for general use. Built specifically for [`jinn`](https://github.com/jayson-lennon/jinn).

## Benchmarks

Notes:

- `ractor` does not have a bounded API.

| Runtime                 | msg burst | Criterion time | Per message |             Rate |
| ----------------------- | --------- | -------------: | ----------: | ---------------: |
| trouper-tell-bounded-64 | 64        |      228.31 µs |    3,567 ns |   280,323 tell/s |
|                         | 512       |      545.17 µs |    1,065 ns |   939,155 tell/s |
|                         | 2048      |        1.58 ms |      770 ns | 1,298,889 tell/s |
|                         | 50000     |       35.04 ms |      701 ns | 1,426,790 tell/s |
| trouper-unbounded-tell  | 64        |      230.67 µs |    3,604 ns |   277,447 tell/s |
|                         | 512       |      573.17 µs |    1,119 ns |   893,273 tell/s |
|                         | 2048      |        1.65 ms |      808 ns | 1,238,208 tell/s |
|                         | 50000     |       39.07 ms |      781 ns | 1,279,704 tell/s |
| kameo-tell-bounded-64   | 64        |      204.39 µs |    3,194 ns |   313,120 tell/s |
|                         | 512       |      361.41 µs |      706 ns | 1,416,662 tell/s |
|                         | 2048      |      835.59 µs |      408 ns | 2,450,951 tell/s |
|                         | 50000     |       16.31 ms |      326 ns | 3,066,180 tell/s |
| kameo-unbounded-tell    | 64        |      191.05 µs |    2,985 ns |   334,985 tell/s |
|                         | 512       |      329.79 µs |      644 ns | 1,552,487 tell/s |
|                         | 2048      |      732.55 µs |      358 ns | 2,795,728 tell/s |
|                         | 50000     |       13.94 ms |      279 ns | 3,585,603 tell/s |
| ractor-unbounded-cast   | 64        |      179.11 µs |    2,799 ns |   357,318 cast/s |
|                         | 512       |      310.81 µs |      607 ns | 1,647,301 cast/s |
|                         | 2048      |      597.61 µs |      292 ns | 3,426,966 cast/s |
|                         | 50000     |       11.38 ms |      228 ns | 4,395,039 cast/s |

Daow SQLite journal benches @ default (64 messages) mailbox size.

- `tell-acked` measures the complete flow of receiving a command, emitting an event, and then the journal receiving the event.
- `flush` measures the journal committing buffered events

Note: Journal implementations that ship with `trouper` buffer events in memory, so some data loss is possible between flushes. Committing to disk happens on a separate thread.

| Runtime                | msg burst   | Criterion time | Per message |           Rate |
| ---------------------- | ----------- | -------------: | ----------: | -------------: |
| daow-tell-acked-memory | 64          |       96.74 µs |    1,512 ns |  661,570 msg/s |
|                        | 512         |       11.75 ms |   22,947 ns |   43,579 msg/s |
|                        | 2048        |       47.68 ms |   23,283 ns |   42,950 msg/s |
| daow-tell-acked-disk   | 64          |      130.76 µs |    2,043 ns |  489,432 msg/s |
|                        | 512         |       11.10 ms |   21,680 ns |   46,125 msg/s |
|                        | 2048        |       42.84 ms |   20,919 ns |   47,803 msg/s |
| daow-flush-memory      | 512 events  |       50.29 ms |   98,226 ns | 10,181 event/s |
|                        | 2048 events |       98.37 ms |   48,032 ns | 20,819 event/s |
| daow-flush-disk        | 512 events  |       37.61 ms |   73,453 ns | 13,614 event/s |
|                        | 2048 events |      133.65 ms |   65,257 ns | 15,324 event/s |

Overhead of journal, per message:

| Bench                            | Case            |      Mean |
| -------------------------------- | --------------- | --------: |
| in_memory_journal_append         | batch_1         |  71.40 ns |
|                                  | batch_8         | 263.50 ns |
|                                  | batch_64        |   1.76 µs |
| in_memory_journal_replay_restart | journal_1000    |  56.06 µs |
|                                  | journal_10000   | 569.14 µs |
|                                  | repeat_load_10k | 779.12 µs |
