# trouper

[![Crates.io](https://img.shields.io/crates/v/trouper.svg)](https://crates.io/crates/trouper)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/license/mit)
[![Repository](https://img.shields.io/badge/repository-GitHub-black)](https://github.com/jayson-lennon/trouper)

A single-machine actor runtime.

This crate is _not_ yet ready for general use. Built specifically for [`jinn`](https://github.com/jayson-lennon/jinn).

## Benchmarks

Usage-shaped end-to-end cycles (`cargo bench --bench e2e`) and component
costs (`cargo bench --bench micro`), run with criterion on the
development machine (Intel i5-10400, Linux, release profile). Numbers
are the mean of the current baseline; treat them as shape and scale,
not absolute promises — rerun `cargo bench` locally for your hardware.

### e2e — full send → done cycles

| Bench | Case | Mean | Throughput |
|---|---|---:|---:|
| tell_baseline | 64_messages | 2.20 ms | 29.1 K elem/s |
| producer_scaling | 1 producer | 4.48 ms | 28.6 K elem/s |
| | 2 | 4.63 ms | 27.6 K elem/s |
| | 4 | 4.65 ms | 27.5 K elem/s |
| | 8 | 4.21 ms | 30.4 K elem/s |
| payload_size | 500 B | 1.23 ms | — |
| | 2 KB | 1.26 ms | — |
| | 64 KB | 1.32 ms | — |
| | 1 MB | 1.49 ms | — |
| wide_tree | 100k_nodes | 1.23 ms | — |
| | 1M_nodes | 1.31 ms | — |
| fanout | handlers_1 | 406 µs | — |
| | handlers_8 | 3.28 ms | — |
| | handlers_64 | 26.4 ms | — |
| overload_block | 16_producers_512_messages | 18.8 ms | 27.3 K elem/s |
| idle_fleet | 1000_idle_producers_1 | 2.16 ms | 29.7 K elem/s |
| | 10k_idle | 2.16 ms | 29.7 K elem/s |
| swarm | p32_r128 | 5.85 ms | 700 K elem/s |
| | p128_r512 | 20.9 ms | 785 K elem/s |
| | p512_r2048 | 106 ms | 618 K elem/s |
| projection_read | hot_typed_64_frames | 7.5 µs | 8.55 M elem/s |
| | hot_json_64_frames | 394 ms | 162 elem/s |

What the shapes mean:

- **tell_baseline** — one producer, one fresh entity, full
  send→fold→ack. The per-message floor: ~34 µs end to end.
- **producer_scaling** — P concurrent producers on one entity. Throughput
  holds as producers grow: per-actor bookkeeping is cell-local, so
  concurrency doesn't funnel through one lock.
- **payload_size** — the typed payload fabric's per-message cost across
  body sizes (payloads are `Arc`-shared; a copy is a refcount bump).
- **fanout** — one broadcast fanned to N handlers.
- **overload_block** — 16 producers against a `Block` inbox at capacity:
  lossless backpressure, senders paced.
- **idle_fleet** — one busy entity while 1k / 10k idle actors sit
  alongside. Flat vs tell_baseline: idle actors don't tax the busy path
  (an idle actor with no duties armed never wakes).
- **swarm** — P entities × R rounds, many-to-many at fleet scale.
- **projection_read** — frontend frame reads over a projector's live
  fold: the typed closure read (zero copies) vs the JSON twin
  (serialize + decode per read).

### micro — journal component costs

| Bench | Case | Mean |
|---|---|---:|
| journal_append | batch_1 | 72 ns |
| | batch_8 | 267 ns |
| | batch_64 | 1.70 µs |
| journal_replay_restart | journal_1000 | 51.9 µs |
| | journal_10000 | 595 µs |
| | repeat_load_10k | 738 µs |

Also see `examples/idle_burn.rs` — a CPU-seconds probe for idle fleets
(5,000 duty-armed idle actors burn ~0.003 CPU-seconds per second of
wall; ~100× less than a polled runtime).
