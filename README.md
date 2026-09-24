# trouper

[![Crates.io](https://img.shields.io/crates/v/trouper.svg)](https://img.shields.io/crates/v/trouper.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/license/mit)
[![Repository](https://img.shields.io/badge/repository-GitHub-black)](https://github.com/jayson-lennon/trouper)

A single-machine actor runtime.

This crate is _not_ yet ready for general use. Built specifically for [`jinn`](https://github.com/jayson-lennon/jinn).

## Benchmarks

Notes:

- `ractor` does not have a bounded API.
- `trouper` does not have an unbounded API (a large capacity mailbox used instead).

| Runtime                 | messages | Criterion time | Per message |            Rate |
| ----------------------- | -------- | -------------: | ----------: | --------------: |
| trouper-tell-bounded-64 | 64       |      223.44 µs |    3,491 ns |   286,435 tell/s |
|                         | 512      |      545.59 µs |    1,066 ns |   938,429 tell/s |
|                         | 2048     |        1.74 ms |      849 ns | 1,177,673 tell/s |
| trouper-unbounded-tell  | 64       |      210.64 µs |    3,291 ns |   303,833 tell/s |
|                         | 512      |      603.77 µs |    1,179 ns |   848,012 tell/s |
|                         | 2048     |        1.85 ms |      901 ns | 1,109,500 tell/s |
| kameo-tell-bounded-64   | 64       |      178.78 µs |    2,794 ns |   357,973 tell/s |
|                         | 512      |      331.66 µs |      648 ns | 1,543,735 tell/s |
|                         | 2048     |      821.85 µs |      401 ns | 2,491,929 tell/s |
| kameo-unbounded-tell    | 64       |      181.90 µs |    2,842 ns |   351,846 tell/s |
|                         | 512      |      293.68 µs |      574 ns | 1,743,416 tell/s |
|                         | 2048     |      724.92 µs |      354 ns | 2,825,142 tell/s |
| ractor-unbounded-cast   | 64       |      178.36 µs |    2,787 ns |   358,833 cast/s |
|                         | 512      |      265.03 µs |      518 ns | 1,931,886 cast/s |
|                         | 2048     |      567.48 µs |      277 ns | 3,608,950 cast/s |

Overhead of journal, per message.
| Bench | Case | Mean |
| -------------------------------- | --------------- | --------: |
| in_memory_journal_append | batch_1 | 71.40 ns |
| | batch_8 | 263.50 ns |
| | batch_64 | 1.76 µs |
| in_memory_journal_replay_restart | journal_1000 | 56.06 µs |
| | journal_10000 | 569.14 µs |
| | repeat_load_10k | 779.12 µs |
