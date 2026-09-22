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
not absolute promises; rerun `cargo bench` locally for your hardware.
(The "Criterion time" column is the raw mean per iteration, exactly as
`cargo bench` prints it; "Per message" divides by the case's element
count; "Rate" names the unit per bench (msg, ask roundtrip, or frame
read) instead of criterion's generic "elem". An element is always one
committed message, one ask roundtrip, or one frame read — never a
spawn or a harness step.)

### e2e

Message benches share one matrix — batches of 64, 512, and 2048 (2048
vs a Block inbox of 64 keeps producer-side blocking inside the measured
body) — and all completion is push-driven: benches spin-yield on the
destination's inbox cursor, which advances exactly at the kernel's
commit point (journal append + ack; cursor ≥ base+N proves N
committed), never poll actor state on a timed sleep. Ask benches
settle on the reply itself; swarm settles on hand-registered sink
counters. Benches run with observation DISABLED (the default) — the
`observation_price` group measures the enabled cost separately. There
is no single-message case: spawn + harness overhead dominated it.
Numbers across matrix entries are NOT comparable 1:1 (per-batch spawn
amortization differs); compare within a size. The swarm keeps its
declared 32×receivers shape; payload_size and wide_tree are
single-message by design (they price payload width, not batching).

| Bench            | Case                        | Criterion time | Per message |                Rate |
| ---------------- | --------------------------- | -------------: | ----------: | ------------------: |
| tell_acked       | 64_messages                 |       1.847 ms |     28.9 µs |        34,655 msg/s |
|                  | 512_messages                |      16.299 ms |     31.8 µs |        31,413 msg/s |
| fire_and_forget  | 64_messages                 |       1.430 ms |     22.3 µs |        44,756 msg/s |
|                  | 512_messages                |      14.822 ms |     28.9 µs |        34,544 msg/s |
| producer_scaling | 64_messages_1_producers     |       1.501 ms |     23.4 µs |        42,645 msg/s |
|                  | 64_messages_2_producers     |     668.089 µs |     10.4 µs |        95,796 msg/s |
|                  | 64_messages_4_producers     |     797.631 µs |     12.5 µs |        80,238 msg/s |
|                  | 64_messages_8_producers     |     952.633 µs |     14.9 µs |        67,182 msg/s |
|                  | 512_messages_1_producers    |      15.493 ms |     30.3 µs |        33,047 msg/s |
|                  | 512_messages_2_producers    |      16.144 ms |     31.5 µs |        31,715 msg/s |
|                  | 512_messages_4_producers    |      17.914 ms |     35.0 µs |        28,581 msg/s |
|                  | 512_messages_8_producers    |      18.083 ms |     35.3 µs |        28,314 msg/s |
| payload_size     | 500B                        |     308.262 µs |    308.3 µs |                   - |
|                  | 2KB                         |     806.425 µs |    806.4 µs |                   - |
|                  | 64KB                        |     211.301 µs |    211.3 µs |                   - |
|                  | 1MB                         |       1.329 ms |    1.329 ms |                   - |
| wide_tree        | 100k_nodes                  |     538.849 µs |    538.8 µs |                   - |
|                  | 1M_nodes                    |       1.247 ms |    1.247 ms |                   - |
| fanout           | handlers_1_1_asks           |      17.595 µs |     17.6 µs |  56,834 roundtrip/s |
|                  | handlers_1_64_asks          |     797.411 µs |     12.5 µs |  80,260 roundtrip/s |
|                  | handlers_1_512_asks         |       7.046 ms |     13.8 µs |  72,665 roundtrip/s |
|                  | handlers_8_1_asks           |     101.784 µs |    101.8 µs |   9,825 roundtrip/s |
|                  | handlers_8_64_asks          |       6.309 ms |     98.6 µs |  10,144 roundtrip/s |
|                  | handlers_8_512_asks         |      51.184 ms |    100.0 µs |  10,003 roundtrip/s |
|                  | handlers_64_1_asks          |     800.697 µs |    800.7 µs |   1,249 roundtrip/s |
|                  | handlers_64_64_asks         |      53.631 ms |    838.0 µs |   1,193 roundtrip/s |
|                  | handlers_64_512_asks        |     411.278 ms |    803.3 µs |   1,245 roundtrip/s |
|                  | 64_messages_16_producers    |     464.743 µs |      7.3 µs |       137,710 msg/s |
|                  | 512_messages_16_producers   |      16.792 ms |     32.8 µs |        30,490 msg/s |
|                  | 1k_idle_64_messages         |     466.120 µs |      7.3 µs |       137,304 msg/s |
|                  | 1k_idle_512_messages        |      13.810 ms |     27.0 µs |        37,075 msg/s |
|                  | 10k_idle_64_messages        |     434.367 µs |      6.8 µs |       147,341 msg/s |
|                  | 10k_idle_512_messages       |      14.146 ms |     27.6 µs |       36,195 msg/s |
| swarm            | p32_r128                    |       6.145 ms |      1.5 µs |       666,594 msg/s |
|                  | p128_r512                   |      28.256 ms |      1.7 µs |       579,846 msg/s |
|                  | p512_r2048                  |     107.689 ms |      1.6 µs |       608,570 msg/s |
| projection_read  | hot_typed_64_frames         |       7.358 µs |    115.0 ns |   8,698,142 read/s |
|                  | hot_json_64_frames          |     395.474 ms |    6.179 ms |          162 read/s |

What the shapes mean:

- **tell_acked**: the COMMIT floor, one message through
  send→fold→ack; batches price the per-message cost once the entity
  is warm.
- **fire_and_forget**: the SEND floor — the same tell loop with no
  completion wait: channel accept + route, resolved at the front
  door. This is what an event-driven producer waits before its own
  next message; the delta vs tell_acked is the fold+ack+observation
  cost. SUSTAINED LOOP ONLY: tells arrive faster than the entity can
  commit, so the inbox fills and backpressure engages (the same
  Block policy overload_block exercises); this measures the
  producer-side wait, not durability.
- **producer_scaling**: 1/2/4/8 producers on one entity at
  64/512/2048 messages. Per-message cost holds as producers grow.
- **payload_size**: one message per commit, 500 B to 1 MB payloads.
- **wide_tree**: one commit with a huge array payload (100k / 1M
  nodes); 539 µs → 1.25 ms shows width costs, but far less than a
  per-node walk would.
- **fanout**: ask roundtrips (request + reply) — 1/8/64 live echo
  services × 1/64/512 asks. Completion is the reply, no wait
  mechanism.
- **overload_block**: 16 producers into a `Block` inbox at
  64/512/2048 messages (backpressure, no loss); 2048 keeps the
  producers blocked inside the measured body.
- **idle_fleet**: one busy entity among 1k / 10k idle actors at
  64/512/2048 messages. Within noise of tell_acked; idle actors cost
  nothing.
- **swarm**: many-to-many at fleet scale (32 tells × every receiver),
  per-message cost at 4k/64k/1M messages in flight.
- **projection_read**: how fast a UI can read a projector's state,
  typed closure vs JSON decode, per read.
- **observation_price**: the same fire_and_forget body with a
  counting observation handler installed vs disabled — the price of
  watching the wire, paid ONLY when a handler is installed.

### micro

| Bench                          | Case            |    Mean |
| ------------------------------ | --------------- | ------: |
| in_memory_journal_append       | batch_1         | 71.2 ns |
|                                | batch_8         | 255.3 ns |
|                                | batch_64        | 1.711 µs |
| in_memory_journal_replay_rest… | journal_1000    | 52.7 µs |
|                                | journal_10000   | 592.5 µs |
|                                | repeat_load_10k | 784.2 µs |

Also see `examples/idle_burn.rs`, a CPU-seconds probe for idle fleets
(5,000 duty-armed idle actors burn ~0.003 CPU-seconds per second of
wall; ~100× less than a polled runtime).
