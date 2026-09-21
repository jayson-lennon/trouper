//! The registry: the runtime's path, schema, and route tables.
//!
//! It is plain data behind a lock — never an actor — so routing can never
//! deadlock on it and registrations survive every actor restart. It holds
//! three tables: path→endpoint slots, the schema table, and schema→handler
//! routes. Actor identity is its registered path; handles survive restarts
//! because slots are swapped, never invalidated.

use std::collections::{BTreeMap, HashMap};

use tokio::sync::mpsc;

use crate::actor::{ActorKind, ActorPath};
use crate::envelope::Envelope;
use crate::json::Json;
use crate::schema::SchemaId;
use crate::schema::{ActorManifest, Schema, SchemaDef, SchemaError};

/// The deliverable front door of one running actor endpoint.
///
/// Senders clone this handle; a restart swaps in a fresh endpoint under the
/// same path, so pre-crash handles die quietly while the path keeps working.
/// The mpsc is the inbox's "Block" overload made concrete: a full mailbox
/// backpressures senders via `.send().await`.
#[derive(Debug)]
pub struct Endpoint {
    tx: mpsc::Sender<Envelope>,
    /// The channel's total capacity (`pending` derives from it).
    capacity: usize,
}

impl Endpoint {
    /// Wraps the front-door sender.
    pub fn new(tx: mpsc::Sender<Envelope>) -> Self {
        Self {
            capacity: tx.max_capacity(),
            tx,
        }
    }

    /// Envelopes accepted into the channel but not yet moved into the
    /// inbox by the front door (tests/reads: quiescence detection).
    pub fn pending(&self) -> usize {
        self.capacity.saturating_sub(self.tx.capacity())
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
/// however it was defined.
///
/// Keyed by name with versions sorted ascending, so "latest" is the last
/// entry — versioning is embedded in [`SchemaId`] from day one.
#[derive(Debug, Default)]
pub struct SchemaTable {
    by_name: HashMap<String, BTreeMap<u32, SchemaDef>>,
}

impl SchemaTable {
    /// Registers a descriptor; idempotent per name+version.
    ///
    /// Re-registering an identical (or even differing) descriptor under the
    /// same name+version keeps the first registration: schemas are agreed
    /// facts, not mutable config. Returns the schema's id either way.
    pub fn register(&mut self, def: SchemaDef) -> SchemaId {
        let id = def.id();
        self.by_name
            .entry(def.name.clone())
            .or_default()
            .entry(def.version)
            .or_insert(def);
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

    /// Registers a Rust type's schema — the typed path.
    pub fn register_of<S: Schema>(&mut self) -> SchemaId {
        self.register(S::schema_def())
    }

    /// Looks a schema up by exact `name@version` id.
    pub fn by_id(&self, id: &SchemaId) -> Option<&SchemaDef> {
        let name = id.name();
        let version = id.version()?;
        self.by_name.get(name)?.get(&version)
    }

    /// The highest registered version of a schema name.
    pub fn latest(&self, name: &str) -> Option<&SchemaDef> {
        self.by_name.get(name)?.values().next_back()
    }

    /// Every registered descriptor, name-then-version ordered (for export).
    pub fn all(&self) -> Vec<&SchemaDef> {
        let mut names: Vec<&String> = self.by_name.keys().collect();
        names.sort();
        names
            .into_iter()
            .flat_map(|name| self.by_name[name].values())
            .collect()
    }

    /// The number of distinct schemas registered.
    pub fn len(&self) -> usize {
        self.by_name.values().map(BTreeMap::len).sum()
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

    /// Registers a Rust type's schema; the typed flavor.
    pub fn register_schema_of<S: Schema>(&mut self) -> SchemaId {
        self.schemas.register_of::<S>()
    }

    /// The descriptor for an exact schema id.
    pub fn schema(&self, id: &SchemaId) -> Option<&SchemaDef> {
        self.schemas.by_id(id)
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
        // schema table is keyed by `name@version`; scan all command
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
        (rx, Endpoint::new(tx))
    }

    fn envelope(n: u32) -> Envelope {
        Envelope::json(
            SchemaId::new("Ping", 1),
            crate::envelope::Address::Path(ActorPath::new("a")),
            json!({ "n": n }),
            TraceCtx::root(),
        )
    }

    #[test]
    fn register_is_idempotent_per_name_and_version() {
        // Given a schema table with ReserveStock@1 already registered.
        let mut table = SchemaTable::default();
        let first = table.register_of::<TestSchema>();

        // When registering ReserveStock@1 again.
        let second = table.register_of::<TestSchema>();

        // Then the id is stable and only one entry exists.
        assert_eq!(first, second);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn register_keeps_versions_sorted_and_latest_reports_highest() {
        // Given the schema registered at versions 1 and 3.
        let mut table = SchemaTable::default();
        table.register(versioned_schema(1));
        table.register(versioned_schema(3));

        // When asking for the latest.
        let latest = table.latest("TestSchema").expect("present");

        // Then it is version 3.
        assert_eq!(latest.id(), SchemaId::new("TestSchema", 3));
    }

    #[test]
    fn by_id_requires_exact_version_match() {
        // Given TestSchema at versions 1 and 2.
        let mut table = SchemaTable::default();
        table.register(versioned_schema(1));
        table.register(versioned_schema(2));

        // When looking up by id — exact and missing.
        let exact = table.by_id(&SchemaId::new("TestSchema", 1));
        let missing = table.by_id(&SchemaId::new("TestSchema", 9));

        // Then only the exact version is found.
        assert!(exact.is_some());
        assert!(missing.is_none());
    }

    #[test]
    fn rust_and_json_flavors_produce_identical_registrations() {
        // Given a typed schema and its hand-written JSON twin.
        let json_twin = json!({
            "name": "TestSchema",
            "version": 1,
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
        table.register(versioned_schema(2));
        table.register(other_schema());
        table.register(versioned_schema(1));

        // When listing all schemas.
        let ids: Vec<String> = table.all().iter().map(|d| d.id().to_string()).collect();

        // Then they are sorted by name, then version.
        assert_eq!(ids, ["OtherSchema@1", "TestSchema@1", "TestSchema@2"]);
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
        let schema = SchemaId::new("Ping", 1);
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
        let schema = SchemaId::new("Ping", 1);
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
        let schema = SchemaId::new("Ping", 1);
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
        registry.add_route(SchemaId::new("Ping", 1), gone.clone());
        registry.add_route(SchemaId::new("Ping", 1), kept.clone());
        registry.add_route(SchemaId::new("Pong", 1), gone.clone());

        // When dropping routes of `gone`.
        registry.drop_routes_of(&gone);

        // Then `kept` still handles Ping and Pong is unrouted.
        assert_eq!(registry.handlers_of(&SchemaId::new("Ping", 1)), [kept]);
        assert!(registry.handlers_of(&SchemaId::new("Pong", 1)).is_empty());
    }

    #[test]
    fn duplicate_route_declaration_keeps_registration_order() {
        // Given a schema routed to three handlers in non-alphabetical order.
        let mut registry = Registry::default();
        let schema = SchemaId::new("Ping", 1);
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
        registry.add_route(SchemaId::new("Ping", 1), gone.clone());
        registry.add_route(SchemaId::new("Ping", 1), kept.clone());
        registry.add_route(SchemaId::new("Pong", 1), gone.clone());

        // When dropping routes of `gone`.
        registry.drop_routes_of(&gone);

        // Then `kept` still handles Ping and Pong is unrouted.
        assert_eq!(registry.handlers_of(&SchemaId::new("Ping", 1)), [kept]);
        assert!(registry.handlers_of(&SchemaId::new("Pong", 1)).is_empty());
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

    fn versioned_schema(version: u32) -> SchemaDef {
        SchemaDef {
            name: "TestSchema".into(),
            version,
            kind: crate::schema::SchemaKind::Command,
            fields: vec![],
            description: None,
        }
    }

    fn other_schema() -> SchemaDef {
        SchemaDef {
            name: "OtherSchema".into(),
            version: 1,
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
}
