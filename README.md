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
(The "Criterion time" column is the raw mean per iteration, exactly as
`cargo bench` prints it; "Per message" divides by the case's element
count; "Rate" names the unit per bench — msg, ask roundtrip, or frame
read — instead of criterion's generic "elem".)

### e2e — full send → done cycles

| Bench | Case | Criterion time | Per message | Rate |
|---|---|---:|---:|---:|
| tell_baseline | 64_messages | 2.197 ms | **34.3 µs** | 29,126 msg/s |
| producer_scaling | 1_producer | 4.483 ms | 35.0 µs | 28,552 msg/s |
| | 2_producers | 4.630 ms | 36.2 µs | 27,646 msg/s |
| | 4_producers | 4.647 ms | 36.3 µs | 27,543 msg/s |
| | 8_producers | 4.212 ms | 32.9 µs | 30,386 msg/s |
| payload_size | 500_B | 1.227 ms | 1.227 ms | — |
| | 2_KB | 1.256 ms | 1.256 ms | — |
| | 64_KB | 1.324 ms | 1.324 ms | — |
| | 1_MB | 1.493 ms | 1.493 ms | — |
| wide_tree | 100k_nodes | 1.233 ms | 1.233 ms | — |
| | 1M_nodes | 1.306 ms | 1.306 ms | — |
| fanout | handlers_1 | 406.0 µs | 12.69 µs | 78,812 roundtrips/s |
| | handlers_8 | 3.283 ms | 102.6 µs | 9,747 roundtrips/s |
| | handlers_64 | 26.38 ms | 824.4 µs | 1,213 roundtrips/s |
| overload_block | 16_producers_512_messages | 18.75 ms | 36.62 µs | 27,304 msg/s |
| idle_fleet | 1000_idle_producers_1 | 2.157 ms | 33.71 µs | 29,669 msg/s |
| | 10k_idle | 2.158 ms | 33.72 µs | 29,656 msg/s |
| swarm | p32_r128 | 5.847 ms | 1.428 µs | 700,511 msg/s |
| | p128_r512 | 20.87 ms | 1.274 µs | 785,045 msg/s |
| | p512_r2048 | 106.1 ms | 1.619 µs | 617,817 msg/s |
| projection_read | hot_typed_64_frames | 7.485 µs | 117 ns | 8,550,000 reads/s |
| | hot_json_64_frames | 394.5 ms | 6.165 ms | 162.2 reads/s |

What the shapes mean:

- **tell_baseline** — the floor: one message through send→fold→ack,
  ~34 µs.
- **producer_scaling** — 1/2/4/8 producers on one entity. Per-message
  cost holds as producers grow (no single-lock funnel).
- **payload_size** — one message per commit at 500 B → 1 MB payloads.
- **wide_tree** — one commit with a huge JSON-tree payload (100k / 1M
  nodes): 1,230 → 1,310 µs shows tree width barely matters.
- **fanout** — ask roundtrips (request + reply); per-roundtrip cost
  with 1/8/64 live echo services.
- **overload_block** — 16 producers into a full `Block` inbox:
  backpressure, no loss.
- **idle_fleet** — one busy entity among 1k / 10k idle actors. Same
  per-message time as tell_baseline: idle actors cost nothing.
- **swarm** — many-to-many at fleet scale (32 tells × every receiver);
  per-message cost at 4k/64k/1M messages in flight.
- **projection_read** — how fast a UI can read a projector's state:
  typed closure vs JSON decode, per read.

### micro — journal component costs

| Bench | Case | Mean |
|---|---|---:|
| journal_append | batch_1 | 72 ns |
| | batch_8 | 267 ns |
| | batch_64 | 1.70 µs |
| journal_replay_restart | journal_1000 | 52 µs |
| | journal_10000 | 595 µs |
| | repeat_load_10k | 738 µs |

Also see `examples/idle_burn.rs` — a CPU-seconds probe for idle fleets
(5,000 duty-armed idle actors burn ~0.003 CPU-seconds per second of
wall; ~100× less than a polled runtime).
