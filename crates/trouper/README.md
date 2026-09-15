# trouper

[![Crates.io](https://img.shields.io/crates/v/trouper.svg)](https://crates.io/crates/trouper)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/license/mit)
[![Repository](https://img.shields.io/badge/repository-GitHub-black)](https://github.com/jayson-lennon/trouper)

A single-machine actor runtime whose product is the communication fabric: a
runtime-level schema registry, envelope/trace metadata, a tap stream of facts,
journal-backed event-sourced actors, topics with per-subscriber cursors,
declarative supervision, and dynamic add/remove of actors.

## What it gives you

- **Schema registry** — every message type registers `(name, version)`; sends,
  publishes, and event emits are validated against declared edges.
- **Event-sourced actors** — sync `decide` handlers, journal-backed recovery
  (snapshot + tail replay), atomic commit ordering (append → ack → fold).
- **Service actors** — impure actors for I/O work, at-most-once by design.
- **Topics** — schema-named pub/sub with per-subscriber cursors and overload
  policies; slow subscribers never block publishers.
- **Supervision** — declarative restart policies, budgets, and backoff, with
  escalation to a parent.
- **Partition sets** — a public path fans out to keyed, on-demand entities
  (`jinn.discovery/<session-id>`); same key always reaches the same entity.
- **Lifecycle** — `on_stop` hooks, `ctx.stop_self()`, declarative idle
  passivation (`.passivate_after()`), and a coordinated
  `system.shutdown_graceful(deadline)` sweep.

Public error signatures use [`error-stack`](https://crates.io/crates/error-stack)
**0.8** (`Report<E>`).

## Example

```rust
use trouper::actor::{CommandHandler, EventSourcedActor};
use trouper::prelude::*;
use serde::Deserialize;

// A message: schema-registered so the fabric can route and validate it.
#[derive(Deserialize)]
struct Deposit {
    n: i64,
}

impl Schema for Deposit {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "Deposit".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![FieldDef::required("n", FieldTy::Int)],
            description: None,
        }
    }
}

// An event-sourced actor: pure decide, journaled, replayable.
#[derive(Debug, Default)]
struct Account {
    balance: i64,
}

impl EventSourcedActor for Account {
    fn genesis(_args: &serde_json::Value) -> Self {
        Self::default()
    }
}

impl CommandHandler<Deposit> for Account {
    type Error = std::convert::Infallible;
    // decide: pure — the kernel journals, acks, folds, then applies
    // deferred effects from `ctx` after commit.
    // (see the repository's examples/ for the full program)
}

# fn main() {}
```

See the `examples/` directory in the repository for complete, runnable
programs (accounts, pools, partitions, observers, and more).
