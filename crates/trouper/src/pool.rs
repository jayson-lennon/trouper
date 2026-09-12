//! Declarative pool, partition-set, and router-rule specs, resolved by the
//! kernel at route time — never forwarding actors.
//!
//! A pool is a routing decision, not a process: senders address the public
//! path and [`crate::kernel::route`] picks a worker per the pool's algo.
//! A partition set derives a per-entity path from a schema-declared shard
//! key and activates entities on demand from a shared factory. Router rules
//! place observers at a tier: `Tee` copies (at-most-once, never an audit
//! mechanism), `Inline` interposes.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::{ActorPath, SchemaId};

/// How a pool picks the worker for the next envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolAlgo {
    /// Workers rotate in registration order.
    RoundRobin,
    /// Pseudo-random pick (xorshift64) — decorrelates from senders.
    Random,
}

impl PoolAlgo {
    /// Picks a worker index from the counter state.
    pub fn pick(&self, len: usize, next: &AtomicU64) -> usize {
        match self {
            Self::RoundRobin => (next.fetch_add(1, Ordering::Relaxed) % len as u64) as usize,
            Self::Random => {
                // xorshift64: deterministic, dependency-free decorrelation.
                let mut x = next.load(Ordering::Relaxed);
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                next.store(x, Ordering::Relaxed);
                (x % len as u64) as usize
            }
        }
    }

    /// The algo's name (export/debug).
    pub fn name(&self) -> &'static str {
        match self {
            Self::RoundRobin => "round-robin",
            Self::Random => "random",
        }
    }
}

/// A stateless pool installed over a public path: the workers own the
/// slots, the pool owns the routing decision for the public name.
#[derive(Debug)]
pub struct PoolEntry {
    /// The worker-selection algorithm.
    pub algo: PoolAlgo,
    /// Worker paths (the only deliverable destinations).
    pub workers: Vec<ActorPath>,
    /// Rotation/PRNG state (seeded injectably for determinism).
    pub next: AtomicU64,
    /// The parent workers are spawned under (escalation target), if any.
    pub spec_parent: Option<ActorPath>,
}

/// Where a rule places an observer relative to the flow it watches.
#[derive(Debug, Clone)]
pub enum RuleAction {
    /// Deliver a COPY to the observer; the primary delivery is untouched.
    /// The copy carries a NEW causality id under the original's trace id
    /// (two deliveries of one message must not look like a chain of two
    /// hops). At-most-once: the copy is dropped if the observer's inbox
    /// is full — a teed copy is NOT an audit mechanism.
    Tee(ActorPath),
    /// Interpose the observer: it receives the envelope in the primary's
    /// place and is responsible for forwarding it.
    Inline(ActorPath),
}

/// A router rule: when an envelope matches (all `Some` criteria must
/// match), the action applies. `None` criteria are wildcards.
#[derive(Debug, Clone)]
pub struct Rule {
    /// Matches the ORIGINAL sender path (`from`), if declared.
    pub source: Option<ActorPath>,
    /// Matches the envelope's schema, if declared.
    pub schema: Option<SchemaId>,
    /// Matches the envelope's destination path, if declared.
    pub dest: Option<ActorPath>,
    /// What happens to a matching envelope.
    pub action: RuleAction,
}

/// Builds a pool entry with an explicit PRNG seed (tests: determinism).
pub fn pool_entry(
    algo: PoolAlgo,
    workers: Vec<ActorPath>,
    seed: u64,
    spec_parent: Option<ActorPath>,
) -> PoolEntry {
    PoolEntry {
        algo,
        workers,
        next: AtomicU64::new(seed.max(1)),
        spec_parent,
    }
}

/// The declarative pool spec handed to [`crate::system::ActorSystem::install_pool`].
///
/// The factory spawns ONE worker at the path the kernel gives it (a typed
/// builder/positional spawn inside a closure — the factory owns the actor
/// type, the kernel owns the naming).
#[derive(Clone)]
pub struct PoolSpec {
    /// The public path senders address (claimed by the pool entry).
    pub public: ActorPath,
    /// The worker count.
    pub workers: usize,
    /// The worker-selection algorithm.
    pub algo: PoolAlgo,
    /// Spawns one worker at the given path (slot included).
    #[allow(clippy::type_complexity)]
    pub factory: std::sync::Arc<
        dyn Fn(&std::sync::Arc<crate::system::ActorSystem>, &ActorPath, &serde_json::Value)
            + Send
            + Sync,
    >,
    /// Genesis args handed to the factory (the workers' shared config).
    pub args: Option<serde_json::Value>,
    /// The supervised parent workers are spawned under (escalation flows
    /// worker → parent); `None` = parentless workers.
    pub parent: Option<ActorPath>,
    /// The pool's PRNG seed (injectable for deterministic tests; any
    /// nonzero value in production).
    pub seed: u64,
}

impl std::fmt::Debug for PoolSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolSpec")
            .field("public", &self.public)
            .field("workers", &self.workers)
            .field("algo", &self.algo)
            .field("parent", &self.parent)
            .finish_non_exhaustive()
    }
}

/// A partition-set spec: per-entity actors derived from a schema-declared
/// shard key, activated on demand from ONE shared factory.
///
/// Senders address the public path forever; the kernel extracts the key,
/// derives `public/key`, and spawns the entity there on first sight.
#[derive(Clone)]
pub struct PartitionSpec {
    /// The public path senders address (the set's identity).
    pub public: ActorPath,
    /// The system handle the factory spawns entities through (captured at
    /// install so the router can activate without extra plumbing).
    pub system: std::sync::Arc<crate::system::ActorSystem>,
    /// Spawns ONE entity at the given path (an ES spawn — the entity owns
    /// its journal). The factory owns the actor type; the kernel owns the
    /// naming and the activation moment.
    #[allow(clippy::type_complexity)]
    pub factory: std::sync::Arc<
        dyn Fn(&std::sync::Arc<crate::system::ActorSystem>, &ActorPath, &serde_json::Value)
            + Send
            + Sync,
    >,
    /// The command field carrying the shard key (extracted per envelope).
    /// Must be marked [`crate::schema::FieldRole::ShardKey`] in at least one
    /// handled command schema — validated at install (refuse-to-lie).
    pub key_field: String,
    /// Genesis args template: the derived key is merged in as `"key"`.
    pub args_template: Option<serde_json::Value>,
    /// Spawn opts for activated entities (snapshot cadence, mailbox).
    pub opts: crate::system::SpawnOpts,
}

impl PartitionSpec {
    /// The genesis args for one entity: the template with the extracted
    /// shard key merged in as `"key"` (so `restore` seeds per-entity state).
    pub fn entity_args(&self, key: &str) -> serde_json::Value {
        let mut merged = match &self.args_template {
            Some(serde_json::Value::Object(map)) => map.clone(),
            _ => serde_json::Map::new(),
        };
        merged.insert("key".into(), serde_json::Value::String(key.to_owned()));
        serde_json::Value::Object(merged)
    }
}

impl std::fmt::Debug for PartitionSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionSpec")
            .field("public", &self.public)
            .field("key_field", &self.key_field)
            .finish_non_exhaustive()
    }
}

/// Extracts the shard-key string from a payload per the schema def.
///
/// Schema-aware, not stringly: the key field's declared type decides how
/// the JSON value renders into the entity path (ints vs strings).
pub fn extract_shard_key(
    schema: &crate::schema::SchemaDef,
    key_field: &str,
    payload: &serde_json::Value,
) -> Option<String> {
    let field_ty = schema
        .fields
        .iter()
        .find(|f| f.name == key_field)
        .map(|f| f.ty.clone());
    let value = payload.get(key_field)?;
    use crate::schema::FieldTy;
    let rendered = match field_ty {
        // Declared-int keys render bare (no quotes in the path).
        Some(FieldTy::Int) => {
            let n = value.as_i64()?;
            n.to_string()
        }
        Some(FieldTy::Float) => value.as_f64()?.to_string(),
        Some(FieldTy::Bool) => value.as_bool()?.to_string(),
        // Undeclared or string-typed keys render as strings.
        _ => {
            let s = value.as_str()?;
            s.to_owned()
        }
    };
    // A path segment must never be empty.
    if rendered.is_empty() {
        None
    } else {
        Some(rendered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_rotates_in_registration_order() {
        // Given a 3-worker pool seeded at zero.
        let entry = pool_entry(
            PoolAlgo::RoundRobin,
            vec![
                ActorPath::new("a"),
                ActorPath::new("b"),
                ActorPath::new("c"),
            ],
            0,
            None,
        );

        // When five picks are made.
        let picks: Vec<usize> = (0..5).map(|_| entry.algo.pick(3, &entry.next)).collect();

        // Then rotation wraps in order (fetch-add returns the PRE value, so
        // the first pick is worker 1; the 5th pick restarts the cycle).
        assert_eq!(picks, vec![1, 2, 0, 1, 2]);
    }

    #[test]
    fn random_is_deterministic_for_a_seed() {
        // Given two identically-seeded pools.
        let e1 = pool_entry(
            PoolAlgo::Random,
            vec![ActorPath::new("a"); 4],
            0xDEADBEEF,
            None,
        );
        let e2 = pool_entry(
            PoolAlgo::Random,
            vec![ActorPath::new("a"); 4],
            0xDEADBEEF,
            None,
        );

        // When several picks run on each.
        let p1: Vec<usize> = (0..8).map(|_| e1.algo.pick(4, &e1.next)).collect();
        let p2: Vec<usize> = (0..8).map(|_| e2.algo.pick(4, &e2.next)).collect();

        // Then the sequences are identical and in range.
        assert_eq!(p1, p2);
        assert!(p1.iter().all(|&i| i < 4));
    }

    #[test]
    fn pool_algo_names_render() {
        // Given each algo.
        // When its name renders.
        // Then the name is stable (export/debug contract).
        assert_eq!(PoolAlgo::RoundRobin.name(), "round-robin");
        assert_eq!(PoolAlgo::Random.name(), "random");
    }
}
