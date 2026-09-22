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
(The Rate column names the unit per bench — msg, ask roundtrip, or
frame read — rather than criterion's generic "elem"; map back to
criterion's `Elements` count when comparing against a local run.
Where the bench measures a BATCH per iteration, Mean is shown per
message — the per-batch time is Mean × the case's message count.)

### e2e — full send → done cycles

| Bench | Case | Mean | Rate |
|---|---|---:|---:|
| tell_baseline | 1 message | **34 µs** | 29,100 msg/s |
| producer_scaling | 1 producer | 4,480 µs | 28,600 msg/s |
| | 2 | 4,630 µs | 27,600 msg/s |
| | 4 | 4,650 µs | 27,500 msg/s |
| | 8 | 4,210 µs | 30,400 msg/s |
| payload_size | 500 B | 1,230 µs | — |
| | 2 KB | 1,260 µs | — |
| | 64 KB | 1,320 µs | — |
| | 1 MB | 1,490 µs | — |
| wide_tree | 100k_nodes | 1,230 µs | — |
| | 1M_nodes | 1,310 µs | — |
| fanout | handlers_1 | 406 µs | 78,800 roundtrips/s |
| | handlers_8 | 3,280 µs | 9,700 roundtrips/s |
| | handlers_64 | 26,400 µs | 1,200 roundtrips/s |
| overload_block | 16_producers_512_messages | 18,800 µs | 27,300 msg/s |
| idle_fleet | 1000_idle_producers_1 | 2,160 µs | 29,700 msg/s |
| | 10k_idle | 2,160 µs | 29,700 msg/s |
| swarm | p32_r128 | 5,850 µs | 700,000 msg/s |
| | p128_r512 | 20,900 µs | 785,000 msg/s |
| | p512_r2048 | 106,000 µs | 618,000 msg/s |
| projection_read | hot_typed_64_frames | 8 µs | 8,550,000 reads/s |
| | hot_json_64_frames | 394,000 µs | 162 reads/s |

What the shapes mean:

- **tell_baseline** — the floor: one message through send→fold→ack,
  ~34 µs. (64 per iteration; Mean shown per message.)
- **producer_scaling** — 1/2/4/8 producers on one entity. The rate
  holds as producers grow (no single-lock funnel).
- **payload_size** — one message per commit at 500 B → 1 MB payloads.
- **wide_tree** — one commit with a huge JSON-tree payload (100k / 1M
  nodes): 1,230 → 1,310 µs shows tree width barely matters.
- **fanout** — 32 ask roundtrips (request + reply); slower with more
  live echo services.
- **overload_block** — 16 producers into a full `Block` inbox:
  backpressure, no loss.
- **idle_fleet** — one busy entity among 1k / 10k idle actors. Same
  time as tell_baseline: idle actors cost nothing.
- **swarm** — many-to-many at fleet scale (32 tells × every receiver).
- **projection_read** — how fast a UI can read a projector's state:
  typed closure (8,550,000 reads/s) vs JSON (162 reads/s).

### micro — journal component costs

| Bench | Case | Mean |
|---|---|---:|
| journal_append | batch_1 | 72 ns |
| | batch_8 | 267 ns |
| | batch_64 | 1,695 ns |
| journal_replay_restart | journal_1000 | 51,868 ns |
| | journal_10000 | 595,087 ns |
| | repeat_load_10k | 738,226 ns |

Also see `examples/idle_burn.rs` — a CPU-seconds probe for idle fleets
(5,000 duty-armed idle actors burn ~0.003 CPU-seconds per second of
wall; ~100× less than a polled runtime).
