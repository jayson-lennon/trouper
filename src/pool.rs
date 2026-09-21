//! Declarative partition-set and router-rule specs, resolved by the
//! kernel at route time — never forwarding actors.
//!
//! A partition set derives a per-entity path from a schema-declared shard
//! key and activates entities on demand from a shared factory. Router
//! rules place observers at a tier: `Tee` copies (at-most-once, never an
//! audit mechanism), `Inline` interposes.
//!
//! (Worker pools are NOT a kernel construct here: spawn a supervisor
//! actor that owns the worker lifecycle and let senders use
//! `send_to_any` — the route table already round-robins one-of sends.)

use crate::actor::ActorPath;
use crate::schema::SchemaId;

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
    pub system: crate::system::ActorSystem,
    /// Spawns ONE entity at the given path (an ES spawn — the entity owns
    /// its journal). The factory owns the actor type; the kernel owns the
    /// naming and the activation moment.
    #[allow(clippy::type_complexity)]
    pub factory: std::sync::Arc<
        dyn Fn(&crate::system::ActorSystem, &ActorPath, &serde_json::Value) + Send + Sync,
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

/// A projector-set spec: per-key projectors derived from a CONSUMED fact's
/// shard key, activated on demand from ONE shared factory.
///
/// The per-key twin of [`PartitionSpec`] for read models: broadcast copies
/// of a consumed schema resolve `public/key` per copy and wake the owning
/// projector on demand — a declared consumption with a shard key is a
/// delivery obligation, so activation-on-declared-broadcast is the rule
/// (retroactive delivery to later-spawned declarants stays forbidden).
/// Passivation lives in `opts`: with it, each per-key projector is
/// evicted when idle and re-woken by the next broadcast; without it, the
/// entity lives until stopped.
#[derive(Clone)]
pub struct ProjectorSetSpec {
    /// The public path publishers ignore but broadcast copies resolve
    /// through (the set's identity).
    pub public: ActorPath,
    /// The system handle the factory spawns projectors through.
    pub system: crate::system::ActorSystem,
    /// Spawns ONE projector at the given path (a projector spawn — the
    /// projector owns its journal and its catch-up). The factory owns the
    /// read-model type and its consumed schemas; the kernel owns the
    /// naming and the activation moment.
    #[allow(clippy::type_complexity)]
    pub factory: std::sync::Arc<
        dyn Fn(&crate::system::ActorSystem, &ActorPath, &serde_json::Value) + Send + Sync,
    >,
    /// The consumed-fact field carrying the shard key (extracted per
    /// broadcast copy). Must be marked
    /// [`crate::schema::FieldRole::ShardKey`] in at least one consumed
    /// schema — validated at install (refuse-to-lie).
    pub key_field: String,
    /// Genesis args template: the derived key is merged in as `"key"`
    /// (a projector's fold genesis is `Default`, so this rides the
    /// manifest/export — the factory may consume it).
    pub args_template: Option<serde_json::Value>,
    /// Spawn opts for activated projectors (snapshot cadence, mailbox,
    /// passivation).
    pub opts: crate::system::SpawnOpts,
    /// The fact schemas this set consumes — every activated projector
    /// declares them. Validated at install: each must exist, be an Event,
    /// and carry `key_field` as its declared ShardKey.
    pub consumed: Vec<SchemaId>,
}

impl ProjectorSetSpec {
    /// The genesis args for one projector: the template with the
    /// extracted shard key merged in as `"key"`.
    pub fn entity_args(&self, key: &str) -> serde_json::Value {
        let mut merged = match &self.args_template {
            Some(serde_json::Value::Object(map)) => map.clone(),
            _ => serde_json::Map::new(),
        };
        merged.insert("key".into(), serde_json::Value::String(key.to_owned()));
        serde_json::Value::Object(merged)
    }
}

impl std::fmt::Debug for ProjectorSetSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectorSetSpec")
            .field("public", &self.public)
            .field("key_field", &self.key_field)
            .field("consumed", &self.consumed)
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
