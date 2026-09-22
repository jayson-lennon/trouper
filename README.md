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

| Bench             | Case                       | Criterion time | Per message |               Rate |
| ----------------- | -------------------------- | ------------: | ---------: | ----------------: |
| tell_acked        | 64_messages                |      372.62 µs |      5.8 µs |      171,760 msg/s |
|                   | 512_messages               |      11.710 ms |     22.9 µs |       43,724 msg/s |
|                   | 2048_messages              |      47.936 ms |     23.4 µs |       42,723 msg/s |
| fire_and_forget   | 64_messages                |      611.23 µs |      9.6 µs |      104,710 msg/s |
|                   | 512_messages               |      8.4710 ms |     16.5 µs |       60,442 msg/s |
|                   | 2048_messages              |      43.909 ms |     21.4 µs |       46,642 msg/s |
| producer_scaling  | 64_messages/1              |      369.35 µs |      5.8 µs |      173,280 msg/s |
|                   | 64_messages/2              |      681.24 µs |     10.6 µs |       93,946 msg/s |
|                   | 64_messages/4              |      877.42 µs |     13.7 µs |       72,941 msg/s |
|                   | 64_messages/8              |      1.0473 ms |     16.4 µs |       61,109 msg/s |
|                   | 512_messages/1             |      12.912 ms |     25.2 µs |       39,652 msg/s |
|                   | 512_messages/2             |      17.082 ms |     33.4 µs |       29,972 msg/s |
|                   | 512_messages/4             |      19.247 ms |     37.6 µs |       26,602 msg/s |
|                   | 512_messages/8             |      20.312 ms |     39.7 µs |       25,206 msg/s |
|                   | 2048_messages/1            |      49.086 ms |     24.0 µs |       41,722 msg/s |
|                   | 2048_messages/2            |      70.452 ms |     34.4 µs |       29,070 msg/s |
|                   | 2048_messages/4            |      76.951 ms |     37.6 µs |       26,614 msg/s |
|                   | 2048_messages/8            |      80.647 ms |     39.4 µs |       25,395 msg/s |
| payload_size      | 500B                       |      871.15 µs |    871.2 µs |                  - |
|                   | 2KB                        |      842.30 µs |    842.3 µs |                  - |
|                   | 64KB                       |      421.56 µs |    421.6 µs |                  - |
|                   | 1MB                        |      1.0454 ms |    1.045 ms |                  - |
| wide_tree         | 100k_nodes                 |      809.56 µs |    809.6 µs |                  - |
|                   | 1M_nodes                   |      1.4102 ms |    1.410 ms |                  - |
| fanout            | handlers_1_1_asks          |      12.505 µs |     12.5 µs | 79,966 roundtrip/s |
|                   | handlers_1_64_asks         |      800.96 µs |     12.5 µs | 79,904 roundtrip/s |
|                   | handlers_1_512_asks        |      6.3559 ms |     12.4 µs | 80,556 roundtrip/s |
|                   | handlers_8_1_asks          |      102.94 µs |    102.9 µs |  9,715 roundtrip/s |
|                   | handlers_8_64_asks         |      6.4837 ms |    101.3 µs |  9,871 roundtrip/s |
|                   | handlers_8_512_asks        |      52.058 ms |    101.7 µs |  9,835 roundtrip/s |
|                   | handlers_64_1_asks         |      812.35 µs |    812.4 µs |  1,231 roundtrip/s |
|                   | handlers_64_64_asks        |      52.203 ms |    815.7 µs |  1,226 roundtrip/s |
|                   | handlers_64_512_asks       |      414.45 ms |    809.5 µs |  1,235 roundtrip/s |
| overload_block    | producers_16_64_messages   |      601.85 µs |      9.4 µs |      106,340 msg/s |
|                   | producers_16_512_messages  |      18.458 ms |     36.1 µs |       27,738 msg/s |
|                   | producers_16_2048_messages |      78.663 ms |     38.4 µs |       26,035 msg/s |
| idle_fleet        | 1k_idle_64_messages        |      207.17 µs |      3.2 µs |      308,920 msg/s |
|                   | 1k_idle_512_messages       |      11.762 ms |     23.0 µs |       43,530 msg/s |
|                   | 1k_idle_2048_messages      |      50.087 ms |     24.5 µs |       40,889 msg/s |
|                   | 10k_idle_64_messages       |      200.61 µs |      3.1 µs |      319,020 msg/s |
|                   | 10k_idle_512_messages      |      12.152 ms |     23.7 µs |       42,132 msg/s |
|                   | 10k_idle_2048_messages     |      51.413 ms |     25.1 µs |       39,835 msg/s |
| projection_read   | hot_typed_64_frames        |      7.4008 µs |    115.6 ns |   8,647,700 read/s |
|                   | hot_json_64_frames         |      396.24 ms |    6.191 ms |         161 read/s |
| swarm             | p32_r128                   |      4.5041 ms |      1.1 µs |      909,404 msg/s |
|                   | p128_r512                  |      14.578 ms |      0.9 µs |    1,123,900 msg/s |
|                   | p512_r2048                 |      75.192 ms |      1.1 µs |      871,590 msg/s |
| observation_price | 64_messages_off            |      453.45 µs |      7.1 µs |      141,140 msg/s |
|                   | 64_messages_on             |      826.82 µs |     12.9 µs |       77,405 msg/s |
|                   | 512_messages_off           |      7.3965 ms |     14.4 µs |       69,222 msg/s |
|                   | 512_messages_on            |      7.5446 ms |     14.7 µs |       67,863 msg/s |

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
  nodes); 810 µs → 1.41 ms shows width costs, but far less than a
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
  aggregate per-message cost at 4k/64k/1M messages in flight —
  sub-microsecond per message (service sinks, no journaling).
- **projection_read**: how fast a UI can read a projector's state,
  typed closure vs JSON decode, per read.
- **observation_price**: the same fire_and_forget body with a
  counting observation handler installed vs disabled — the price of
  watching the wire, paid ONLY when a handler is installed.

### micro

| Bench                            | Case            |      Mean |
| -------------------------------- | --------------- | -------: |
| in_memory_journal_append         | batch_1         | 70.184 ns |
|                                  | batch_8         | 246.98 ns |
|                                  | batch_64        | 1.6731 µs |
| in_memory_journal_replay_restart | journal_1000    | 50.536 µs |
|                                  | journal_10000   | 573.91 µs |
|                                  | repeat_load_10k | 744.56 µs |

Also see `examples/idle_burn.rs`, a CPU-seconds probe for idle fleets
(5,000 duty-armed idle actors burn ~0.003 CPU-seconds per second of
wall; ~100× less than a polled runtime).
