# trouper

[![Crates.io](https://img.shields.io/crates/v/trouper.svg)](https://img.shields.io/crates/v/trouper.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/license/mit)
[![Repository](https://img.shields.io/badge/repository-GitHub-black)](https://github.com/jayson-lennon/trouper)

A single-machine actor runtime.

This crate is _not_ yet ready for general use. Built specifically for [`jinn`](https://github.com/jayson-lennon/jinn).

## Benchmarks

Notes:

- `ractor` does not have a bounded API.

| Runtime                 | msg burst | Criterion time | Per message |            Rate |
| ----------------------- | --------- | -------------: | ----------: | --------------: |
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

Overhead of journal, per message:

| Bench                            | Case            |      Mean |
| -------------------------------- | --------------- | --------: |
| in_memory_journal_append         | batch_1         |  71.40 ns |
|                                  | batch_8         | 263.50 ns |
|                                  | batch_64        |   1.76 µs |
| in_memory_journal_replay_restart | journal_1000    |  56.06 µs |
|                                  | journal_10000   | 569.14 µs |
|                                  | repeat_load_10k | 779.12 µs |
