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
| tell_acked        | 64_messages                |      371.03 µs |     5.8 µs |      165,120 msg/s |
|                   | 512_messages               |      13.325 ms |    26.0 µs |       38,099 msg/s |
|                   | 2048_messages              |      55.081 ms |    26.9 µs |       36,754 msg/s |
| fire_and_forget   | 64_messages                |      338.52 µs |     5.3 µs |      167,250 msg/s |
|                   | 512_messages               |      10.875 ms |    21.2 µs |       46,612 msg/s |
|                   | 2048_messages              |      51.818 ms |    25.3 µs |       38,901 msg/s |
| producer_scaling  | 64_messages/1              |      334.30 µs |     5.2 µs |      171,420 msg/s |
|                   | 64_messages/2              |      682.18 µs |    10.7 µs |       88,483 msg/s |
|                   | 64_messages/4              |      1.0092 ms |    15.8 µs |       61,650 msg/s |
|                   | 64_messages/8              |      1.2644 ms |    19.8 µs |       49,398 msg/s |
|                   | 512_messages/1             |      13.173 ms |    25.7 µs |       38,591 msg/s |
|                   | 512_messages/2             |      16.907 ms |    33.0 µs |       30,150 msg/s |
|                   | 512_messages/4             |      19.497 ms |    38.1 µs |       26,007 msg/s |
|                   | 512_messages/8             |      20.570 ms |    40.2 µs |       24,678 msg/s |
|                   | 2048_messages/1            |      49.920 ms |    24.4 µs |       40,784 msg/s |
|                   | 2048_messages/2            |      68.094 ms |    33.2 µs |       29,787 msg/s |
|                   | 2048_messages/4            |      76.182 ms |    37.2 µs |       26,574 msg/s |
|                   | 2048_messages/8            |      80.548 ms |    39.3 µs |       25,192 msg/s |
| payload_size      | 500B                       |      845.48 µs |   845.5 µs |                  - |
|                   | 2KB                        |      1.4123 ms |   1.412 ms |                  - |
|                   | 64KB                       |      334.38 µs |   334.4 µs |                  - |
|                   | 1MB                        |      960.53 µs |   960.5 µs |                  - |
| wide_tree         | 100k_nodes                 |      941.64 µs |   941.6 µs |                  - |
|                   | 1M_nodes                   |      1.6095 ms |   1.610 ms |                  - |
| fanout            | handlers_1_1_asks          |      10.721 µs |    10.7 µs | 92,857 roundtrip/s |
|                   | handlers_1_64_asks         |      677.79 µs |    10.6 µs | 94,076 roundtrip/s |
|                   | handlers_1_512_asks        |      5.4277 ms |    10.6 µs | 93,881 roundtrip/s |
|                   | handlers_8_1_asks          |      85.384 µs |    85.4 µs | 11,613 roundtrip/s |
|                   | handlers_8_64_asks         |      5.4543 ms |    85.2 µs | 11,651 roundtrip/s |
|                   | handlers_8_512_asks        |      43.952 ms |    85.8 µs | 11,602 roundtrip/s |
|                   | handlers_64_1_asks         |      700.18 µs |   700.2 µs |  1,418 roundtrip/s |
|                   | handlers_64_64_asks        |      43.670 ms |   682.3 µs |  1,450 roundtrip/s |
|                   | handlers_64_512_asks       |      350.81 ms |   685.7 µs |  1,450 roundtrip/s |
| overload_block    | producers_16_64_messages   |      504.02 µs |     7.9 µs |      120,000 msg/s |
|                   | producers_16_512_messages  |      18.696 ms |    36.5 µs |       26,920 msg/s |
|                   | producers_16_2048_messages |      78.556 ms |    38.4 µs |       25,806 msg/s |
| idle_fleet        | 1k_idle_64_messages        |      230.65 µs |     3.6 µs |      258,160 msg/s |
|                   | 1k_idle_512_messages       |      13.015 ms |    25.4 µs |       38,466 msg/s |
|                   | 1k_idle_2048_messages      |      50.733 ms |    24.8 µs |       39,676 msg/s |
|                   | 10k_idle_64_messages       |      201.65 µs |     3.1 µs |      300,780 msg/s |
|                   | 10k_idle_512_messages      |      12.426 ms |    24.3 µs |       39,832 msg/s |
|                   | 10k_idle_2048_messages     |      53.943 ms |    26.3 µs |       37,083 msg/s |
| projection_read   | hot_typed_64_frames        |      7.5592 µs |   118.1 ns |   8,444,500 read/s |
|                   | hot_json_64_frames         |      391.15 ms |   6.112 ms |         163 read/s |
| swarm             | p32_r128                   |      3.2905 ms |     0.8 µs |    1,236,600 msg/s |
|                   | p128_r512                  |      13.233 ms |     0.8 µs |    1,232,800 msg/s |
|                   | p512_r2048                 |      62.710 ms |     1.2 µs |    1,031,900 msg/s |
| observation_price | 64_messages_off            |      370.92 µs |     5.8 µs |      153,390 msg/s |
|                   | 64_messages_on             |      651.19 µs |    10.2 µs |       92,899 msg/s |
|                   | 512_messages_off           |      10.894 ms |    21.3 µs |       46,127 msg/s |
|                   | 512_messages_on            |      10.763 ms |    21.0 µs |       46,729 msg/s |

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
| in_memory_journal_append         | batch_1         | 71.79 ns |
|                                  | batch_8         | 32.55 ns |
|                                  | batch_64        | 27.79 ns |
| in_memory_journal_replay_restart | journal_1000    | 54.080 µs |
|                                  | journal_10000   | 566.30 µs |
|                                  | repeat_load_10k | 765.40 µs |

Also see `examples/idle_burn.rs`, a CPU-seconds probe for idle fleets
(5,000 duty-armed idle actors burn ~0.003 CPU-seconds per second of
wall; ~100× less than a polled runtime).
