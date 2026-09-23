//! The registry: the runtime's path, schema, and route tables.
//!
//! It is plain data behind a lock — never an actor — so routing can never
//! deadlock on it and registrations survive every actor restart. It holds
//! three tables: path→endpoint slots, the schema table, and schema→handler
//! routes. Actor identity is its registered path; handles survive restarts
//! because slots are swapped, never invalidated.

use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::actor::{ActorKind, ActorPath};
use crate::envelope::{Envelope, PayloadBytes};
use crate::json::Json;
use crate::schema::SchemaId;
use crate::schema::{ActorManifest, Schema, SchemaDef, SchemaError};

/// One Rust type per schema name, enforced process-wide.
///
/// `claim_schema_type` is called at every typed registration site (the
/// manifest builder's `.handles::<S>()`/`.emits::<S>()`, typed schema
/// registration). The first Rust type to claim a name owns it; a second,
/// different Rust type under the same name is a startup error — folds and
/// handlers match payloads by name and downcast to THE registered type, so
/// two candidate types under one name would be silent corruption.
///
/// Same-type re-claims (the idempotent re-registration case: spawning two
/// actors that both `.handles::<Add>()`) succeed.
pub(crate) fn claim_schema_type<S: Schema + 'static>()
-> Result<(), error_stack::Report<SchemaError>> {
    use std::collections::hash_map::Entry;
    static OWNERS: std::sync::OnceLock<parking_lot::Mutex<HashMap<String, std::any::TypeId>>> =
        std::sync::OnceLock::new();
    let owners = OWNERS.get_or_init(|| parking_lot::Mutex::new(HashMap::new()));
    let name = S::schema_def().name;
    let mut owners = owners.lock();
    match owners.entry(name) {
        Entry::Vacant(v) => {
            v.insert(std::any::TypeId::of::<S>());
            Ok(())
        }
        Entry::Occupied(o) if *o.get() == std::any::TypeId::of::<S>() => Ok(()),
        Entry::Occupied(o) => {
            use error_stack::IntoReport;
            Err(SchemaError::DuplicateType(o.key().clone())
                .into_report()
                .attach(format!(
                    "schema name already owned by another Rust type (registering {})",
                    std::any::type_name::<S>()
                )))
        }
    }
}

/// Decodes wire bytes into the LATEST registered type under `name` —
/// the erased-ingress door.
///
/// The bytes are JSON text (valid UTF-8 by contract); `from_slice` reads
/// them in place. The bound IS the invariant: `T` must be the exact type
/// that claimed the name (a wrong `T` here misses the owner's TypeId and
/// the decode runs against a foreign type — the caller's bug surfaces at
/// the handler's downcast, as a `Decode` dead letter, never as UB).
///
/// # Errors
///
/// Returns an error when the name has no registered descriptor
/// ([`SchemaError::InvalidDescriptor`]) or the bytes do not decode into
/// `T`.
pub fn decode_latest<T: Schema + serde::de::DeserializeOwned>(
    name: &SchemaId,
    bytes: &PayloadBytes,
) -> Result<T, error_stack::Report<SchemaError>> {
    use error_stack::{IntoReport, ResultExt};
    if name.as_str() != T::schema_id().as_str() {
        return Err(SchemaError::InvalidDescriptor.into_report().attach(format!(
            "decode_latest: requested {name} through type {}",
            std::any::type_name::<T>()
        )));
    }
    serde_json::from_slice::<T>(bytes.as_bytes()).change_context(SchemaError::InvalidDescriptor)
}

/// The deliverable front door of one running actor endpoint.
///
/// Senders clone this handle; a restart swaps in a fresh endpoint under the
/// same path, so pre-crash handles die quietly while the path keeps working.
/// The mpsc is the inbox's "Block" overload made concrete: its depth is the
/// spawn-configured capacity, so a full mailbox backpressures senders via
/// `.send().await` at exactly the configured bound.
///
/// The handle also carries the destination's LIVE CELL: a sender may push
/// straight into the cell's inbox and fire its wake itself (the direct
/// delivery path), falling back to the front-door channel only when the
/// inbox refuses. The cell persists across restarts — only the channel
/// swaps — so the coupling is valid for the endpoint's whole lifetime.
///
/// # Known hazard
///
/// True blocking means a cycle of actors whose mailboxes ALL fill up can
/// deadlock: every member is blocked sending while blocked flushes wait on
/// blocked peers. Size mailboxes so hot cycles cannot saturate every hop.
pub struct Endpoint {
    tx: mpsc::Sender<Envelope>,
    /// The channel's total capacity (`pending` derives from it).
    capacity: usize,
    /// The destination's live cell (its inbox is the direct-delivery
    /// target; its `work` notify is the direct wake).
    pub(crate) cell: std::sync::Arc<crate::kernel::ActorCell>,
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The cell is not Debug (it is loop-internal bookkeeping); the
        // channel side plus the live queue depth is what debug output
        // wants.
        f.debug_struct("Endpoint")
            .field("capacity", &self.capacity)
            .field("pending", &self.pending())
            .finish_non_exhaustive()
    }
}

impl Endpoint {
    /// Wraps the front-door sender and its destination's live cell.
    pub(crate) fn new(
        tx: mpsc::Sender<Envelope>,
        cell: std::sync::Arc<crate::kernel::ActorCell>,
    ) -> Self {
        Self {
            capacity: tx.max_capacity(),
            tx,
            cell,
        }
    }

    /// Envelopes accepted into the channel but not yet moved into the
    /// inbox by the front door (tests/reads: quiescence detection).
    ///
    /// Direct deliveries bypass the channel entirely, so this counts
    /// only the fallback door's in-flight backlog; the flush quiescence
    /// check pairs it with the inbox depth read (which covers both
    /// paths), never on its own.
    pub fn pending(&self) -> usize {
        self.capacity.saturating_sub(self.tx.capacity())
    }

    /// The channel's total capacity (tests: the D4 restart-capacity seam).
    #[cfg(test)]
    pub fn max_capacity(&self) -> usize {
        self.capacity
    }

    /// Attempts delivery without waiting: fails immediately when the inbox
    /// is full.
    ///
    /// # Errors
    ///
    /// Fails when the front door is full (`try_send`) or the endpoint is
    /// gone (receiver dropped mid-restart).
    // Large Err is deliberate: the caller recovers the undeliverable
    // envelope for dead-lettering (allowed workspace-wide in Cargo.toml).
    pub fn try_deliver(
        &self,
        envelope: Envelope,
    ) -> Result<(), mpsc::error::TrySendError<Envelope>> {
        self.tx.try_send(envelope)
    }

    /// Delivers an envelope, waiting for capacity (backpressure = Block).
    ///
    /// # Errors
    ///
    /// Fails when the endpoint is gone (receiver dropped mid-restart).
    pub async fn deliver(
        &self,
        envelope: Envelope,
    ) -> Result<(), mpsc::error::SendError<Envelope>> {
        self.tx.send(envelope).await
    }
}

/// A registered actor identity: its manifest plus a swappable endpoint.
#[derive(Debug)]
pub(crate) struct Slot {
    /// The actor's declared edges and contract kind.
    pub(crate) manifest: ActorManifest,
    /// The running endpoint; `None` while stopped (between restarts).
    pub(crate) endpoint: arc_swap::ArcSwapOption<Endpoint>,
    /// The inbox overload policy this actor spawned with.
    pub(crate) inbox_policy: crate::inbox::OverloadPolicy,
}

impl Slot {
    /// The actor kind this slot was spawned as.
    pub fn kind(&self) -> ActorKind {
        self.manifest
            .kind
            .expect("slots are always spawned with a manifest kind")
    }
}

/// What the caller sees from a successful lookup.
#[derive(Debug, Clone)]
pub struct EndpointInfo {
    /// The actor's path.
    pub path: ActorPath,
    /// The actor's contract kind.
    pub kind: ActorKind,
    /// A copy of the actor's manifest.
    pub manifest: ActorManifest,
}

/// How messages for one schema find their actor.
#[derive(Debug, Clone)]
pub enum RoutePolicy {
    /// Exactly one handler path.
    Single(ActorPath),
    /// Handlers rotate in registration order.
    RoundRobin(Vec<ActorPath>),
}

impl RoutePolicy {
    /// Picks the next path for this route.
    ///
    /// Round-robin state is a cursor carried by the caller (the registry),
    /// keeping this type pure data.
    fn pick(&self, cursor: &mut usize) -> Option<ActorPath> {
        match self {
            Self::Single(path) => Some(path.clone()),
            Self::RoundRobin(paths) if !paths.is_empty() => {
                let path = paths[*cursor % paths.len()].clone();
                *cursor = (*cursor + 1) % paths.len();
                Some(path)
            }
            Self::RoundRobin(_) => None,
        }
    }
}

/// The runtime's schema table: every message shape the system knows,
/// however it was defined. Keyed by schema name — identity is the name
/// alone.
#[derive(Debug, Default)]
pub struct SchemaTable {
    by_name: HashMap<String, SchemaDef>,
}

impl SchemaTable {
    /// Registers a descriptor; idempotent per name.
    ///
    /// Re-registering an identical (or even differing) descriptor under
    /// the same name keeps the first registration: schemas are agreed
    /// facts, not mutable config. Returns the schema's id either way.
    pub fn register(&mut self, def: SchemaDef) -> SchemaId {
        let id = def.id();
        self.by_name.entry(def.name.clone()).or_insert(def);
        id
    }

    /// Registers from a JSON descriptor — the foreign path.
    ///
    /// # Errors
    ///
    /// Returns an error when `json` is not a valid [`SchemaDef`].
    pub fn register_json(
        &mut self,
        json: Json,
    ) -> Result<SchemaId, error_stack::Report<SchemaError>> {
        let def = SchemaDef::from_json(json)?;
        Ok(self.register(def))
    }

    /// Registers a Rust type's schema — the typed path — claiming
    /// TypeId ownership of the name for `S`.
    ///
    /// # Errors
    ///
    /// [`SchemaError::DuplicateType`] when another Rust type already
    /// owns this schema name.
    pub fn register_typed<S: Schema + 'static>(
        &mut self,
    ) -> Result<SchemaId, error_stack::Report<SchemaError>> {
        use error_stack::ResultExt;
        let def = S::schema_def();
        let name = def.name.clone();
        let id = def.id();
        // TypeId ownership FIRST: a wrong second type never touches the
        // table, whether or not a descriptor already sat there.
        claim_schema_type::<S>().change_context(SchemaError::DuplicateType(format!(
            "{name} (registering {})",
            std::any::type_name::<S>()
        )))?;
        self.by_name.entry(name).or_insert(def);
        Ok(id)
    }

    /// The descriptor registered under this name.
    pub fn by_id(&self, id: &SchemaId) -> Option<&SchemaDef> {
        self.by_name.get(id.name())
    }

    /// The descriptor registered under a schema name.
    pub fn by_name(&self, name: &str) -> Option<&SchemaDef> {
        self.by_name.get(name)
    }

    /// The highest registered version of a schema name.
    pub fn latest(&self, name: &str) -> Option<&SchemaDef> {
        self.by_name.get(name)
    }

    /// Every registered descriptor, name-ordered (for export).
    pub fn all(&self) -> Vec<&SchemaDef> {
        let mut names: Vec<&String> = self.by_name.keys().collect();
        names.sort();
        names.into_iter().map(|name| &self.by_name[name]).collect()
    }

    /// The number of distinct schemas registered.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// Whether no schema is registered.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }
}

/// Errors surfaced by registry mutations.
#[derive(Debug, wherror::Error)]
#[error(debug)]
pub enum RegistryError {
    /// A slot already exists under this path.
    PathTaken(ActorPath),
    /// No slot exists under this path.
    UnknownPath(ActorPath),
    /// A pool/partition spec referenced no workers, or violated its own
    /// contract (e.g. no command schema carries the declared shard key).
    InvalidSpec,
}

/// All runtime tables: slots, schemas, routes, pools/partitions, and rules.
#[derive(Debug, Default)]
pub struct Registry {
    schemas: SchemaTable,
    slots: HashMap<ActorPath, Slot>,
    /// The one receive table: schema → handler paths. A declaration via
    /// `.handles::<M>()` lands here — whether the sender says tell,
    /// send_to_any, or publish is invisible to the receiver.
    routes: HashMap<SchemaId, RoutePolicy>,
    route_cursor: usize,
    /// Partition sets by PUBLIC path (entities own the real slots, derived
    /// from the set's path on demand).
    pub(crate) partitions: HashMap<ActorPath, crate::pool::PartitionSpec>,
    /// Projector sets by PUBLIC path (per-key projectors own the real
    /// slots, derived from the set's path on demand).
    pub(crate) projector_sets: HashMap<ActorPath, crate::pool::ProjectorSetSpec>,
    /// Router rules in declaration (priority) order.
    pub(crate) rules: Vec<crate::pool::Rule>,
}

impl Registry {
    /// Registers a schema descriptor; idempotent per name+version.
    pub fn register_schema(&mut self, def: SchemaDef) -> SchemaId {
        self.schemas.register(def)
    }

    /// Registers a schema from JSON; the foreign flavor.
    ///
    /// # Errors
    ///
    /// Returns an error when `json` is not a valid [`SchemaDef`].
    pub fn register_schema_json(
        &mut self,
        json: Json,
    ) -> Result<SchemaId, error_stack::Report<SchemaError>> {
        self.schemas.register_json(json)
    }

    /// Registers a Rust type's schema; the typed flavor. Claims TypeId
    /// ownership of the schema name for `S`.
    ///
    /// # Errors
    ///
    /// [`SchemaError::DuplicateType`] when another Rust type already owns
    /// `S`'s schema name (one type per name, enforced at registration).
    pub fn register_schema_of<S: Schema + 'static>(
        &mut self,
    ) -> Result<SchemaId, error_stack::Report<SchemaError>> {
        self.schemas.register_typed::<S>()
    }

    /// The descriptor for an exact schema id.
    pub fn schema(&self, id: &SchemaId) -> Option<&SchemaDef> {
        self.schemas.by_id(id)
    }

    /// Decodes wire bytes under `name` into `T` — the erased-ingress
    /// door, exposed for the kernel's typed fallbacks. `T` must be the
    /// schema name's registered type (the bound enforces the caller's
    /// static claim; see [`decode_latest`]).
    ///
    /// # Errors
    ///
    /// Propagates [`decode_latest`]'s failures (unknown name, bytes that
    /// do not decode into `T`).
    pub fn decode_latest_typed<T: Schema + serde::de::DeserializeOwned>(
        &self,
        name: &SchemaId,
        bytes: &PayloadBytes,
    ) -> Result<T, error_stack::Report<SchemaError>> {
        decode_latest::<T>(name, bytes)
    }

    /// A snapshot of every live slot for export: (path, manifest).
    pub fn slot_manifests(&self) -> Vec<(ActorPath, ActorManifest)> {
        self.slots
            .iter()
            .map(|(path, slot)| (path.clone(), slot.manifest.clone()))
            .collect()
    }

    /// The declared partition/rule topology (for export). Entities are the
    /// live slots derived from each set's public path.
    pub fn topology(
        &self,
    ) -> (
        Vec<crate::system::PartitionExport>,
        Vec<crate::system::RuleExport>,
    ) {
        let partitions = self
            .partitions
            .iter()
            .map(|(path, spec)| crate::system::PartitionExport {
                path: path.clone(),
                key_field: spec.key_field.clone(),
                entities: self
                    .slots
                    .keys()
                    .filter(|slot| slot.as_str().starts_with(&format!("{}/", path.as_str())))
                    .cloned()
                    .collect(),
            })
            .collect();
        let rules = self
            .rules
            .iter()
            .map(|rule| crate::system::RuleExport {
                source: rule.source.clone(),
                schema: rule.schema.clone(),
                dest: rule.dest.clone(),
                action: match rule.action {
                    crate::pool::RuleAction::Tee(_) => "tee".to_owned(),
                    crate::pool::RuleAction::Inline(_) => "inline".to_owned(),
                },
                observer: match &rule.action {
                    crate::pool::RuleAction::Tee(o) | crate::pool::RuleAction::Inline(o) => {
                        o.clone()
                    }
                },
            })
            .collect();
        (partitions, rules)
    }

    /// The shared schema table (for export).
    pub fn schemas(&self) -> &SchemaTable {
        &self.schemas
    }

    /// Inserts a slot; fails if the path is already taken (a live actor owns
    /// its path — remove it first).
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::PathTaken`] when the path is registered.
    pub fn insert_slot(
        &mut self,
        path: ActorPath,
        manifest: ActorManifest,
        endpoint: Endpoint,
        inbox_policy: crate::inbox::OverloadPolicy,
    ) -> Result<(), error_stack::Report<RegistryError>> {
        use error_stack::IntoReport;
        if self.slots.contains_key(&path) {
            return Err(RegistryError::PathTaken(path.clone())
                .into_report()
                .attach(format!("spawning over live path {path}")));
        }
        self.slots.insert(
            path,
            Slot {
                manifest,
                endpoint: arc_swap::ArcSwapOption::from_pointee(endpoint),
                inbox_policy,
            },
        );
        Ok(())
    }

    /// Swaps a slot's endpoint — the restart mechanism. Identity (the path,
    /// the manifest, the inbox cursor held by the runtime) persists.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::UnknownPath`] when no slot exists.
    pub fn swap_endpoint(
        &mut self,
        path: &ActorPath,
        endpoint: Endpoint,
    ) -> Result<(), error_stack::Report<RegistryError>> {
        use error_stack::ResultExt;
        let slot = self
            .slots
            .get_mut(path)
            .ok_or_else(|| RegistryError::UnknownPath(path.clone()))
            .attach(format!("restarting unknown path {path}"))?;
        slot.endpoint.store(Some(std::sync::Arc::new(endpoint)));
        Ok(())
    }

    /// Removes a slot entirely; returns its manifest.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::UnknownPath`] when no slot exists.
    pub fn remove_slot(
        &mut self,
        path: &ActorPath,
    ) -> Result<ActorManifest, error_stack::Report<RegistryError>> {
        use error_stack::ResultExt;
        let slot = self
            .slots
            .remove(path)
            .ok_or_else(|| RegistryError::UnknownPath(path.clone()))
            .attach(format!("removing unknown path {path}"))?;
        Ok(slot.manifest)
    }

    /// Declares an emit edge on a live slot (adds `schema` to the slot
    /// manifest's `emits`). Used by foreign spawns to declare the event
    /// schemas their decision closures produce — undeclared emits are
    /// dropped by the runtime, so this declaration is load-bearing.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::UnknownPath`] when no slot exists.
    pub fn declare_emits(
        &mut self,
        path: &ActorPath,
        schema: SchemaId,
    ) -> Result<(), error_stack::Report<RegistryError>> {
        use error_stack::ResultExt;
        let slot = self
            .slots
            .get_mut(path)
            .ok_or_else(|| RegistryError::UnknownPath(path.clone()))
            .attach(format!("declaring emits on unknown path {path}"))?;
        if !slot.manifest.emits.contains(&schema) {
            slot.manifest.emits.push(schema);
        }
        Ok(())
    }

    /// Appends a router rule (declaration order is priority order).
    pub fn add_rule(&mut self, rule: crate::pool::Rule) {
        self.rules.push(rule);
    }

    /// Installs a partition set: validates the spec against the schema
    /// table, then records it. Entities are not spawned
    /// here — activation happens on demand in the router.
    ///
    /// # Errors
    ///
    /// [`RegistryError::InvalidSpec`] when no command schema reachable
    /// from the spec declares the shard-key field the spec names: a
    /// partition set whose key can never be extracted would silently
    /// dead-letter every command, so it is rejected at install.
    pub fn install_partition_set(
        &mut self,
        spec: crate::pool::PartitionSpec,
    ) -> Result<(), error_stack::Report<RegistryError>> {
        use error_stack::IntoReport;
        // The key field must be declared (with the ShardKey role) on at
        // least one COMMAND schema whose route could reach this set. The
        // schema table is keyed by name; scan all command
        // schemas for a field with the declared name + role.
        let key_declared = self
            .schemas
            .all()
            .into_iter()
            .filter(|def| def.kind == crate::schema::SchemaKind::Command)
            .any(|def| {
                def.fields.iter().any(|f| {
                    f.name == spec.key_field && f.role == Some(crate::schema::FieldRole::ShardKey)
                })
            });
        if !key_declared {
            return Err(RegistryError::InvalidSpec.into_report().attach(format!(
                "partition set {}: no command schema declares field `{}` as ShardKey",
                spec.public, spec.key_field
            )));
        }
        self.partitions.insert(spec.public.clone(), spec);
        Ok(())
    }

    /// Installs a projector set: validates every consumed schema against
    /// the schema table, then records it. Projectors are
    /// not spawned here — activation happens on demand when a broadcast of
    /// a consumed schema crosses the fabric.
    ///
    /// # Errors
    ///
    /// [`RegistryError::InvalidSpec`] when a consumed schema does not
    /// exist, is not an Event, or no consumed schema declares the spec's
    /// key field as the ShardKey: a projector set whose key can never be
    /// extracted would silently dead-letter every broadcast copy, so it is
    /// rejected at install.
    pub fn install_projector_set(
        &mut self,
        spec: crate::pool::ProjectorSetSpec,
    ) -> Result<(), error_stack::Report<RegistryError>> {
        use error_stack::IntoReport;
        for schema in &spec.consumed {
            let def = self.schema(schema).ok_or_else(|| {
                RegistryError::InvalidSpec.into_report().attach(format!(
                    "projector set {}: consumed schema {schema} is not registered \
                         — declare it before installing",
                    spec.public
                ))
            })?;
            if def.kind != crate::schema::SchemaKind::Event {
                return Err(RegistryError::InvalidSpec.into_report().attach(format!(
                    "projector set {}: consumed schema {schema} is a Command — \
                         projectors fold facts, not commands",
                    spec.public
                )));
            }
        }
        // The key field must be declared (with the ShardKey role) on at
        // least one consumed EVENT schema — broadcast copies of it are the
        // set's activation trigger, and a copy without its key would
        // dead-letter every time.
        let key_declared = spec.consumed.iter().any(|schema| {
            self.schema(schema).is_some_and(|def| {
                def.fields.iter().any(|f| {
                    f.name == spec.key_field && f.role == Some(crate::schema::FieldRole::ShardKey)
                })
            })
        });
        if !key_declared {
            return Err(RegistryError::InvalidSpec.into_report().attach(format!(
                "projector set {}: no consumed schema declares field `{}` as ShardKey",
                spec.public, spec.key_field
            )));
        }
        self.projector_sets.insert(spec.public.clone(), spec);
        Ok(())
    }

    /// Every projector set consuming `schema` (the broadcast fan-out's
    /// activation candidates).
    pub fn projector_sets_consuming(
        &self,
        schema: &SchemaId,
    ) -> Vec<crate::pool::ProjectorSetSpec> {
        self.projector_sets
            .values()
            .filter(|spec| spec.consumed.contains(schema))
            .cloned()
            .collect()
    }

    /// The projector set owning `path` (a per-key projector derived from
    /// `public/key`), if any.
    pub fn projector_set_owning(&self, path: &ActorPath) -> Option<crate::pool::ProjectorSetSpec> {
        self.projector_sets
            .values()
            .find(|spec| {
                path.as_str()
                    .strip_prefix(&format!("{}/", spec.public.as_str()))
                    .is_some_and(|key| !key.is_empty() && !key.contains('/'))
            })
            .cloned()
    }

    /// Destination set for a schema's route (tests/canvas introspection).
    pub fn route_dests(&self, schema: &SchemaId) -> Vec<ActorPath> {
        self.routes
            .get(schema)
            .map(|policy| match policy {
                RoutePolicy::Single(path) => vec![path.clone()],
                RoutePolicy::RoundRobin(paths) => paths.clone(),
            })
            .unwrap_or_default()
    }

    /// Resolves a path to a deliverable endpoint, if the actor is running.
    pub fn resolve(&self, path: &ActorPath) -> Option<std::sync::Arc<Endpoint>> {
        self.slots.get(path)?.endpoint.load_full()
    }

    /// The inbox policy a path spawned with.
    pub fn inbox_policy(&self, path: &ActorPath) -> crate::inbox::OverloadPolicy {
        self.slots
            .get(path)
            .map(|s| s.inbox_policy)
            .unwrap_or(crate::inbox::OverloadPolicy::DropNew)
    }

    /// Snapshot info about a path, for `ctx.lookup`.
    pub fn lookup(&self, path: &ActorPath) -> Option<EndpointInfo> {
        let slot = self.slots.get(path)?;
        Some(EndpointInfo {
            path: path.clone(),
            kind: slot.kind(),
            manifest: slot.manifest.clone(),
        })
    }

    /// Whether a path is registered (running or mid-restart).
    pub fn is_registered(&self, path: &ActorPath) -> bool {
        self.slots.contains_key(path)
    }

    /// Routes `schema` to a handler path and registers the route.
    ///
    /// For [`RoutePolicy::Single`] the sole path wins; for round-robin the
    /// registry's shared cursor rotates. Projector-set members are not
    /// routable by schema: a per-key projector's only delivery obligation
    /// is its keyed copy (the broadcast set arm); a schema-addressed send
    /// has no key, so it must never land on a key-derived actor.
    pub fn route(&mut self, schema: &SchemaId) -> Option<ActorPath> {
        let policy = self.routes.get(schema)?;
        let candidate = policy.pick(&mut self.route_cursor)?;
        if self.projector_set_owning(&candidate).is_some() {
            // Rotating pick hit a projector member: scan for a plain
            // handler; none means the schema belongs to projectors only.
            let pool = self.handlers_of(schema);
            pool.iter()
                .find(|path| self.projector_set_owning(path).is_none())
                .cloned()
        } else {
            Some(candidate)
        }
    }

    /// Whether `path` declared `schema` in its emits — the kernel's emit
    /// gate, answered WITHOUT cloning the actor's manifest (a lock-held
    /// read of the live slot).
    pub fn declares_emit(&self, path: &ActorPath, schema: &SchemaId) -> bool {
        self.slots
            .get(path)
            .is_some_and(|slot| slot.manifest.emits.contains(schema))
    }

    /// The route pick and its live endpoint in ONE read (a send's single
    /// critical section; returns both `None`s when unrouted).
    pub fn route_resolved(
        &mut self,
        schema: &SchemaId,
    ) -> (Option<ActorPath>, Option<std::sync::Arc<Endpoint>>) {
        match self.route(schema) {
            Some(target) => {
                let endpoint = self.resolve(&target);
                (Some(target), endpoint)
            }
            None => (None, None),
        }
    }

    /// Declares (or extends) the route for a schema.
    ///
    /// Registering a second handler for a schema converts the route to
    /// round-robin over registration order — multiple independent actors
    /// sharing one schema is exactly the load-balancing case.
    pub fn add_route(&mut self, schema: SchemaId, path: ActorPath) {
        match self.routes.entry(schema) {
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(RoutePolicy::Single(path));
            }
            std::collections::hash_map::Entry::Occupied(mut o) => match o.get_mut() {
                RoutePolicy::Single(existing) => {
                    if *existing != path {
                        let first = existing.clone();
                        *o.get_mut() = RoutePolicy::RoundRobin(vec![first, path]);
                    }
                }
                RoutePolicy::RoundRobin(paths) => {
                    if !paths.contains(&path) {
                        paths.push(path);
                    }
                }
            },
        }
    }

    /// Drops every route pointing at `path` (slot removal cascade).
    pub fn drop_routes_of(&mut self, path: &ActorPath) {
        self.routes.retain(|_, policy| match policy {
            RoutePolicy::Single(single) => single != path,
            RoutePolicy::RoundRobin(paths) => {
                paths.retain(|p| p != path);
                !paths.is_empty()
            }
        });
    }

    /// Every actor that declared `.handles::<M>()` for `schema`, in
    /// registration order — the full target set for publish fan-out.
    pub fn handlers_of(&self, schema: &SchemaId) -> Vec<ActorPath> {
        match self.routes.get(schema) {
            Some(RoutePolicy::Single(path)) => vec![path.clone()],
            Some(RoutePolicy::RoundRobin(paths)) => paths.clone(),
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::TraceCtx;
    use crate::json;

    fn manifest(kind: ActorKind) -> ActorManifest {
        ActorManifest::new().kind(kind)
    }

    fn endpoint(capacity: usize) -> (mpsc::Receiver<Envelope>, Endpoint) {
        let (tx, rx) = mpsc::channel(capacity);
        let path = ActorPath::new("test.cell");
        let cell = std::sync::Arc::new(crate::kernel::ActorCell::new(
            path,
            crate::inbox::Inbox::new(capacity, crate::inbox::OverloadPolicy::DropNew),
            capacity,
            crate::inbox::OverloadPolicy::DropNew,
            1,
        ));
        (rx, Endpoint::new(tx, cell))
    }

    fn envelope(n: u32) -> Envelope {
        Envelope::from_bytes(
            SchemaId::new("Ping"),
            crate::envelope::Address::Path(ActorPath::new("a")),
            PayloadBytes::from(json!({ "n": n })),
            TraceCtx::root(),
        )
    }

    #[test]
    fn register_is_idempotent_per_name() {
        // Given a schema table with TestSchema already registered.
        let mut table = SchemaTable::default();
        let first = table.register(versioned_schema(1));

        // When registering TestSchema again.
        let second = table.register(versioned_schema(1));

        // Then the id is stable and only one entry exists.
        assert_eq!(first, second);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn duplicate_type_registration_fails_at_startup() {
        // Given TestSchema registered under its name.
        let mut table = SchemaTable::default();
        table
            .register_typed::<TestSchema>()
            .expect("first claim wins");

        // When a DIFFERENT Rust type registers the same schema name
        // (TestSchemaImpostor declares the same name in its def).
        let result = table.register_typed::<TestSchemaImpostor>();

        // Then registration fails with DuplicateType — one Rust type per
        // schema name, enforced before any message can exist.
        let report = result.expect_err("second type must be refused");
        assert!(
            matches!(report.current_context(), SchemaError::DuplicateType(_)),
            "expected DuplicateType, got {report:?}"
        );
        // And the impostor never touched the table.
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn same_type_reregistration_is_idempotent() {
        // Given TestSchema registered.
        let mut table = SchemaTable::default();
        let first = table.register_typed::<TestSchema>().expect("claim");

        // When the SAME type registers again (two actors, one schema).
        let second = table.register_typed::<TestSchema>().expect("re-claim");

        // Then both registrations agree and one entry exists.
        assert_eq!(first, second);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn latest_reports_the_named_schema() {
        // Given TestSchema registered under its name.
        let mut table = SchemaTable::default();
        table.register(versioned_schema(1));

        // When asking for the latest (the erased-ingress lookup).
        let latest = table.latest("TestSchema").expect("present");

        // Then it is the registered descriptor under its name-only id.
        assert_eq!(latest.id(), SchemaId::new("TestSchema"));
    }

    #[test]
    fn by_id_matches_by_name() {
        // Given TestSchema registered.
        let mut table = SchemaTable::default();
        table.register(versioned_schema(1));

        // When looking up by its name-only id — and by an unknown name.
        let exact = table.by_id(&SchemaId::new("TestSchema"));
        let missing = table.by_id(&SchemaId::new("TestSchemaUnknown"));

        // Then the name matches and the unknown name does not.
        assert!(exact.is_some());
        assert!(missing.is_none());
    }

    #[test]
    fn legacy_versioned_id_strings_lookup_by_name() {
        // Given TestSchema registered and a legacy `name@N` id string
        // (written before name-only identity).
        let mut table = SchemaTable::default();
        table.register(versioned_schema(1));

        // When parsing the legacy string and looking it up.
        let legacy = SchemaId::parse("TestSchema").expect("parses");
        let found = table.by_id(&legacy);

        // Then the version component is dropped and the name matches.
        assert!(found.is_some());
        assert_eq!(legacy.as_str(), "TestSchema");
    }

    #[test]
    fn rust_and_json_flavors_produce_identical_registrations() {
        // Given a typed schema and its hand-written JSON twin.
        let json_twin = json!({
            "name": "TestSchema",
            "kind": "command",
            "fields": []
        });

        // When registering each flavor into its own table.
        let mut typed = SchemaTable::default();
        let mut foreign = SchemaTable::default();
        let typed_id = typed.register(versioned_schema(1));
        let foreign_id = foreign.register_json(json_twin).expect("valid");

        // Then both tables hold identical descriptors under identical ids.
        assert_eq!(typed_id, foreign_id);
        assert_eq!(typed.by_id(&typed_id), foreign.by_id(&foreign_id));
    }

    #[test]
    fn register_json_rejects_invalid_descriptors() {
        // Given a garbage descriptor.
        let mut table = SchemaTable::default();

        // When registering it as JSON.
        let result = table.register_json(json!({ "name": ["nope"] }));

        // Then registration fails and the table stays empty.
        assert!(result.is_err());
        assert!(table.is_empty());
    }

    #[test]
    fn all_lists_every_schema_in_name_then_version_order() {
        // Given schemas registered out of order across two names.
        let mut table = SchemaTable::default();
        table.register(versioned_schema(1));
        table.register(other_schema());

        // When listing all schemas.
        let ids: Vec<String> = table.all().iter().map(|d| d.id().to_string()).collect();

        // Then they are sorted by name.
        assert_eq!(ids, ["OtherSchema", "TestSchema"]);
    }

    #[test]
    fn insert_slot_then_resolve_delivers_to_the_endpoint() {
        // Given a registry with one slot inserted.
        let mut registry = Registry::default();
        let path = ActorPath::new("inventory.west");
        let (_rx, ep) = endpoint(4);
        registry
            .insert_slot(
                path.clone(),
                manifest(ActorKind::EventSourced),
                ep,
                crate::inbox::OverloadPolicy::DropNew,
            )
            .expect("insert");

        // When resolving the path and delivering an envelope.
        let delivered = match registry.resolve(&path) {
            Some(ep) => ep.try_deliver(envelope(1)).is_ok(),
            None => false,
        };

        // Then delivery succeeds.
        assert!(delivered);
    }

    #[test]
    fn insert_slot_fails_when_path_is_taken() {
        // Given a registry with a slot at `dup`.
        let mut registry = Registry::default();
        let path = ActorPath::new("dup");
        let (_rx, ep) = endpoint(1);
        registry
            .insert_slot(
                path.clone(),
                manifest(ActorKind::Service),
                ep,
                crate::inbox::OverloadPolicy::DropNew,
            )
            .expect("insert");

        // When inserting another slot at the same path.
        let (_rx, ep2) = endpoint(1);
        let result = registry.insert_slot(
            path,
            manifest(ActorKind::Service),
            ep2,
            crate::inbox::OverloadPolicy::DropNew,
        );

        // Then it fails with PathTaken.
        assert!(matches!(
            result.expect_err("must fail").current_context(),
            RegistryError::PathTaken(_)
        ));
    }

    #[test]
    fn swap_endpoint_replaces_the_handle_under_the_same_identity() {
        // Given a slot whose first endpoint's receiver is dropped on swap.
        let mut registry = Registry::default();
        let path = ActorPath::new("inventory.west");
        let (rx1, ep1) = endpoint(4);
        registry
            .insert_slot(
                path.clone(),
                manifest(ActorKind::EventSourced),
                ep1,
                crate::inbox::OverloadPolicy::DropNew,
            )
            .expect("insert");
        let stale = registry.resolve(&path).expect("live");

        // When the actor "restarts": the old receiver dies (the task ended)
        // and a fresh endpoint is swapped in under the same path.
        drop(rx1); // the actor task ended: its receiver is gone
        let (_rx2, ep2) = endpoint(4);
        registry.swap_endpoint(&path, ep2).expect("swap");

        // Then the stale handle no longer delivers (its receiver is gone),
        // but the path still resolves to a fresh live endpoint.
        let fresh = registry.resolve(&path).expect("still registered");
        assert!(stale.try_deliver(envelope(1)).is_err());
        assert!(fresh.try_deliver(envelope(2)).is_ok());
    }

    #[test]
    fn remove_slot_makes_the_path_unresolvable() {
        // Given a registry with a slot.
        let mut registry = Registry::default();
        let path = ActorPath::new("temp");
        let (_rx, ep) = endpoint(1);
        registry
            .insert_slot(
                path.clone(),
                manifest(ActorKind::Service),
                ep,
                crate::inbox::OverloadPolicy::DropNew,
            )
            .expect("insert");

        // When removing the slot.
        registry.remove_slot(&path).expect("remove");

        // Then resolution fails and lookups return None.
        assert!(registry.resolve(&path).is_none());
        assert!(registry.lookup(&path).is_none());
        assert!(!registry.is_registered(&path));
    }

    #[test]
    fn lookup_reports_kind_and_manifest() {
        // Given a slot spawned with an event-sourced manifest.
        let mut registry = Registry::default();
        let path = ActorPath::new("inventory.west");
        let (ep_manifest, _rx, ep) = {
            let m = ActorManifest::new().kind(ActorKind::EventSourced);
            let (rx, ep) = endpoint(1);
            (m, rx, ep)
        };
        registry
            .insert_slot(
                path.clone(),
                ep_manifest,
                ep,
                crate::inbox::OverloadPolicy::DropNew,
            )
            .expect("insert");

        // When looking the path up.
        let info = registry.lookup(&path).expect("info");

        // Then the kind is visible.
        assert_eq!(info.kind, ActorKind::EventSourced);
    }

    #[test]
    fn route_returns_the_single_handler_path() {
        // Given a single route from a schema to an actor.
        let mut registry = Registry::default();
        let schema = SchemaId::new("Ping");
        let path = ActorPath::new("ponger");
        registry.add_route(schema.clone(), path.clone());

        // When routing the schema twice.
        let first = registry.route(&schema);
        let second = registry.route(&schema);

        // Then both route to the same sole path.
        assert_eq!(first, Some(path.clone()));
        assert_eq!(second, Some(path));
    }

    #[test]
    fn round_robin_rotates_across_registered_handlers() {
        // Given a schema routed to three handlers.
        let mut registry = Registry::default();
        let schema = SchemaId::new("Ping");
        for name in ["a", "b", "c"] {
            registry.add_route(schema.clone(), ActorPath::new(name));
        }

        // When routing four times.
        let picks: Vec<Option<String>> = (0..4)
            .map(|_| registry.route(&schema).map(|p| p.to_string()))
            .collect();

        // Then handlers rotate and wrap.
        assert_eq!(
            picks,
            [
                Some("a".into()),
                Some("b".into()),
                Some("c".into()),
                Some("a".into())
            ]
        );
    }

    #[test]
    fn handlers_of_lists_every_handler_of_a_schema() {
        // Given a schema routed to two handlers.
        let mut registry = Registry::default();
        let schema = SchemaId::new("Ping");
        registry.add_route(schema.clone(), ActorPath::new("a"));
        registry.add_route(schema.clone(), ActorPath::new("b"));

        // When asking who handles it.
        let handlers = registry.handlers_of(&schema);

        // Then both paths are listed.
        assert_eq!(handlers.len(), 2);
    }

    #[test]
    fn drop_routes_of_removes_only_the_removed_path() {
        // Given two schemas routed through `gone` and one through `kept`.
        let mut registry = Registry::default();
        let gone = ActorPath::new("gone");
        let kept = ActorPath::new("kept");
        registry.add_route(SchemaId::new("Ping"), gone.clone());
        registry.add_route(SchemaId::new("Ping"), kept.clone());
        registry.add_route(SchemaId::new("Pong"), gone.clone());

        // When dropping routes of `gone`.
        registry.drop_routes_of(&gone);

        // Then `kept` still handles Ping and Pong is unrouted.
        assert_eq!(registry.handlers_of(&SchemaId::new("Ping")), [kept]);
        assert!(registry.handlers_of(&SchemaId::new("Pong")).is_empty());
    }

    #[test]
    fn duplicate_route_declaration_keeps_registration_order() {
        // Given a schema routed to three handlers in non-alphabetical order.
        let mut registry = Registry::default();
        let schema = SchemaId::new("Ping");
        registry.add_route(schema.clone(), ActorPath::new("c"));
        registry.add_route(schema.clone(), ActorPath::new("a"));
        registry.add_route(schema.clone(), ActorPath::new("b"));

        // When listing the handlers.
        let handlers = registry.handlers_of(&schema);

        // Then they come back in registration order, not sorted order.
        assert_eq!(
            handlers,
            [
                ActorPath::new("c"),
                ActorPath::new("a"),
                ActorPath::new("b")
            ]
        );
    }

    #[test]
    fn drop_routes_of_cleans_empty_round_robin_entries() {
        // Given `gone` handling two schemas, `kept` handling one of them.
        let mut registry = Registry::default();
        let gone = ActorPath::new("gone");
        let kept = ActorPath::new("kept");
        registry.add_route(SchemaId::new("Ping"), gone.clone());
        registry.add_route(SchemaId::new("Ping"), kept.clone());
        registry.add_route(SchemaId::new("Pong"), gone.clone());

        // When dropping routes of `gone`.
        registry.drop_routes_of(&gone);

        // Then `kept` still handles Ping and Pong is unrouted.
        assert_eq!(registry.handlers_of(&SchemaId::new("Ping")), [kept]);
        assert!(registry.handlers_of(&SchemaId::new("Pong")).is_empty());
    }

    #[test]
    fn unresolvable_paths_resolve_to_none_for_dead_lettering() {
        // Given an empty registry.
        let registry = Registry::default();

        // When resolving a path no actor owns.
        let resolved = registry.resolve(&ActorPath::new("ghost"));

        // Then resolution is None — the kernel turns this into a
        // DeadLettered fact.
        assert!(resolved.is_none());
    }

    fn versioned_schema(_version: u32) -> SchemaDef {
        SchemaDef {
            name: "TestSchema".into(),
            kind: crate::schema::SchemaKind::Command,
            fields: vec![],
            description: None,
        }
    }

    fn other_schema() -> SchemaDef {
        SchemaDef {
            name: "OtherSchema".into(),
            kind: crate::schema::SchemaKind::Event,
            fields: vec![],
            description: None,
        }
    }

    struct TestSchema;
    impl Schema for TestSchema {
        fn schema_def() -> SchemaDef {
            versioned_schema(1)
        }
    }

    /// A different Rust type declaring the SAME schema name — the
    /// duplicate-registration hazard the TypeId claim exists to catch.
    struct TestSchemaImpostor;
    impl Schema for TestSchemaImpostor {
        fn schema_def() -> SchemaDef {
            versioned_schema(1)
        }
    }
}
