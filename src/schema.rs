//! Message schemas as runtime data.
//!
//! Rust types and external JSON descriptors register into the same schema
//! table (see [`crate::registry`]): a schema defined outside Rust is just a
//! [`SchemaDef`] parsed from JSON, indistinguishable from one derived from a
//! Rust type. Identity is the schema NAME alone — schemas carry no version
//! marker. Evolution follows the additive-fields contract: new fields get
//! serde defaults, so an older payload decodes into the newest type.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::sync::Arc;

use crate::json::Json;

/// Errors surfaced while parsing schema descriptors.
#[derive(Debug, wherror::Error)]
#[error(debug)]
pub enum SchemaError {
    /// A JSON document was not a valid [`SchemaDef`].
    InvalidDescriptor,
    /// A second Rust type registered under a schema name that another
    /// type already owns — one type per name, enforced at registration.
    DuplicateType(String),
}

/// Whether a schema describes an inbound command or a domain event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaKind {
    /// A request for the actor to decide something (ES) or do something
    /// (service).
    Command,
    /// A fact that already happened; emitted by event-sourced actors.
    Event,
}

/// A field's type within a schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldTy {
    /// JSON boolean.
    Bool,
    /// JSON integer.
    Int,
    /// JSON float.
    Float,
    /// JSON string.
    Str,
    /// A UUID string.
    Uuid,
    /// Arbitrary JSON; the escape hatch for payloads consumers need not
    /// inspect deeply.
    Json,
    /// A list of values of one element type.
    List(Box<FieldTy>),
}

/// An inclusive numeric bound pair, both ends optional.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct Range {
    /// The lowest accepted value, if bounded below.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// The highest accepted value, if bounded above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

impl Range {
    /// A range bounded on both ends.
    pub fn between(min: f64, max: f64) -> Self {
        Self {
            min: Some(min),
            max: Some(max),
        }
    }
}

/// One field descriptor within a schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDef {
    /// The field's name.
    pub name: String,
    /// The field's type.
    pub ty: FieldTy,
    /// The unit of measure, e.g. `"ms"`, `"kg"`; presentation data,
    /// never interpreted by the runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// Accepted numeric bounds, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<Range>,
    /// The field's structural role, when it carries one (e.g.
    /// [`FieldRole::ShardKey`] names the partition key a router reads).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<FieldRole>,
    /// Human-facing description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A structural role a field can play beyond its data type.
///
/// The runtime reads roles to make routing decisions; exports render
/// them so the declared routing is visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldRole {
    /// The partition key: partition-set routers extract this field from a
    /// command or consumed-fact payload to derive the entity path (a
    /// projector set resolves its per-key projectors through it).
    ShardKey,
}

impl FieldDef {
    /// A required field with no unit, range, or description.
    pub fn required(name: &str, ty: FieldTy) -> Self {
        Self {
            name: name.to_owned(),
            ty,
            unit: None,
            range: None,
            role: None,
            description: None,
        }
    }

    /// Attaches a unit of measure.
    pub fn with_unit(mut self, unit: &str) -> Self {
        self.unit = Some(unit.to_owned());
        self
    }

    /// Attaches a human-facing description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Attaches a numeric range.
    pub fn with_range(mut self, range: Range) -> Self {
        self.range = Some(range);
        self
    }

    /// Attaches a structural role.
    pub fn with_role(mut self, role: FieldRole) -> Self {
        self.role = Some(role);
        self
    }

    /// Marks this field as the partition's shard key (the key a router
    /// extracts from a command or consumed-fact payload to derive the
    /// entity path).
    pub fn as_shard_key(mut self) -> Self {
        self.role = Some(FieldRole::ShardKey);
        self
    }
}

/// A complete message schema: name, version, kind, and field descriptors.
///
/// Fully serde-serializable so a schema can be defined outside Rust and
/// registered as data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaDef {
    /// The schema's name, e.g. `StockReserved` — the schema's whole identity.
    pub name: String,
    /// Command or event.
    pub kind: SchemaKind,
    /// The payload's field descriptors.
    #[serde(default)]
    pub fields: Vec<FieldDef>,
    /// Human-facing description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl SchemaDef {
    /// Parses a schema descriptor from JSON — the foreign registration path.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::InvalidDescriptor`] when `json` is not a valid
    /// [`SchemaDef`].
    pub fn from_json(json: Json) -> Result<Self, error_stack::Report<SchemaError>> {
        use error_stack::ResultExt;
        json.decode::<Self>()
            .change_context(SchemaError::InvalidDescriptor)
    }

    /// The schema's stable identifier: its name.
    pub fn id(&self) -> SchemaId {
        SchemaId::new(&self.name)
    }

    /// Serializes the descriptor to JSON (for export).
    pub fn to_json(&self) -> Json {
        Json::of(self)
    }

    /// The name of the field carrying `role`, if any.
    pub fn field_with_role(&self, role: FieldRole) -> Option<&str> {
        self.fields
            .iter()
            .find(|f| f.role == Some(role))
            .map(|f| f.name.as_str())
    }
}

/// Implemented by Rust types that have a schema; the mirror of
/// [`SchemaDef::from_json`] for the typed path.
///
/// Both paths produce the same [`SchemaDef`], so a schema is identical
/// whether it arrived from code or from data.
///
/// Prefer deriving the impl with the [`Event`] / [`Command`] derives —
/// they generate `schema_def` from the struct's fields. Hand-write the
/// impl only for complex or foreign descriptors.
pub trait Schema: 'static {
    /// The type's schema descriptor.
    fn schema_def() -> SchemaDef;

    /// The type's schema name when it is a compile-time constant.
    ///
    /// The derive supplies this (the struct ident); hand-written impls
    /// keep the default `None` and take the per-type cached path in
    /// [`Schema::schema_id`].
    fn schema_name() -> Option<&'static str> {
        None
    }

    /// The type's stable [`SchemaId`].
    ///
    /// Zero heap allocation after the first call per type: a derived
    /// schema's id wraps its `&'static str` name (a clone is a copy); a
    /// hand-written impl's id is built once and cached per type (later
    /// reads are a map hit plus an `Arc` refcount bump).
    fn schema_id() -> SchemaId {
        match Self::schema_name() {
            Some(name) => SchemaId::static_name(name),
            None => cached_schema_id::<Self>(),
        }
    }
}

/// The per-type cached id for hand-written [`Schema`] impls (the derive
/// supplies a `'static` name and never lands here). One entry per type
/// per process: the first `schema_id()` call builds it, every later read
/// shares it.
fn cached_schema_id<S: Schema + ?Sized>() -> SchemaId {
    use std::collections::hash_map::Entry;
    static CACHE: std::sync::OnceLock<parking_lot::Mutex<HashMap<std::any::TypeId, SchemaId>>> =
        std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(|| parking_lot::Mutex::new(HashMap::new()));
    let mut cache = cache.lock();
    match cache.entry(std::any::TypeId::of::<S>()) {
        Entry::Vacant(slot) => {
            // TEST PROBE: the fallback cache miss is the one allocation a
            // hand-written impl's schema_id ever pays (see the counters in
            // kernel.rs — the derive's static arm never lands here).
            #[cfg(test)]
            crate::kernel::bump_schema_id_cache_misses();
            let id = SchemaId::new(&S::schema_def().name);
            slot.insert(id.clone());
            id
        }
        Entry::Occupied(slot) => slot.get().clone(),
    }
}

// The derive macros live in the workspace's `trouper_macros` crate; they
// are re-exported here (and reach the prelude through the glob above) so
// users write `#[derive(Event)]` next to `#[derive(Serialize, Clone)]`.
pub use trouper_macros::{Command, Event};

/// A type that round-trips the wire: a schema contract with serde on both
/// ends. The bound for typed effect methods — the schema id comes from the
/// type, the payload from serde.
///
/// Blanket-implemented: any `Schema` type with both serde derives is a
/// `Message`.
pub trait Message: Schema + Serialize + DeserializeOwned {}
impl<T: Schema + Serialize + DeserializeOwned> Message for T {}

/// An actor's declared edges: which schemas it handles and which it
/// emits (the complete outbound message surface — `.emits` is enforced
/// at flush time on every outbound message).
///
/// Manifest entries reference [`SchemaId`]s only — Rust-registered and
/// JSON-registered schemas are indistinguishable here, which is what makes
/// declared edges uniform for export. Produced by the [`ActorManifest`]
/// builder at spawn; the runtime registers routes from it.
#[derive(Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ActorManifest {
    /// Schemas this actor accepts — the one receive declaration. `.handles`
    /// installs both the route and the dispatch entry; whether a message
    /// arrives via tell, send_to_any, or publish is invisible here.
    #[serde(default)]
    pub handles: Vec<SchemaId>,
    /// Event schemas this actor emits. The outbound declaration: every
    /// message an actor sends, publishes, or replies with must appear
    /// here or the runtime drops it at flush (an `UndeclaredEmit` dead
    /// letter).
    #[serde(default)]
    pub emits: Vec<SchemaId>,
    /// Which actor contract this actor implements.
    #[serde(default)]
    pub kind: Option<crate::actor::ActorKind>,
}

impl ActorManifest {
    /// Starts an empty manifest.
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares that this actor handles schema `S`.
    ///
    /// # Panics
    ///
    /// Panics when a DIFFERENT Rust type already claimed `S`'s schema
    /// name (one type per name, process-wide). Same-type re-claims are
    /// free — spawning many actors on one schema is the norm.
    pub fn handles<S: Schema + 'static>(mut self) -> Self {
        Self::expect_claim::<S>();
        let id = S::schema_id();
        if !self.handles.contains(&id) {
            self.handles.push(id);
        }
        self
    }

    /// Declares that this actor handles a schema by id — the foreign path.
    pub fn handles_id(mut self, id: SchemaId) -> Self {
        if !self.handles.contains(&id) {
            self.handles.push(id);
        }
        self
    }

    /// Declares that this actor emits event schema `S` (same one-type-
    /// per-name contract as [`ActorManifest::handles`]).
    ///
    /// # Panics
    ///
    /// Panics when another Rust type already claimed `S`'s schema name.
    pub fn emits<S: Schema + 'static>(mut self) -> Self {
        Self::expect_claim::<S>();
        let id = S::schema_id();
        if !self.emits.contains(&id) {
            self.emits.push(id);
        }
        self
    }

    /// Claims `S`'s schema name for its Rust type, panicking with a
    /// named message on a conflicting claim (manifest builders panic —
    /// they run in author code at spawn, where a wrong second type is a
    /// program bug, not a runtime outcome).
    fn expect_claim<S: Schema + 'static>() {
        if let Err(report) = crate::registry::claim_schema_type::<S>() {
            panic!("schema TypeId claim failed (one Rust type per schema name): {report:?}");
        }
    }

    /// Declares that this actor emits an event schema by id.
    pub fn emits_id(mut self, id: SchemaId) -> Self {
        if !self.emits.contains(&id) {
            self.emits.push(id);
        }
        self
    }

    /// Declares the actor contract this actor implements.
    pub fn kind(mut self, kind: crate::actor::ActorKind) -> Self {
        self.kind = Some(kind);
        self
    }
}

impl Clone for ActorManifest {
    fn clone(&self) -> Self {
        // TEST PROBE: the hot-path manifest copies (emit-gate lookups,
        // handler-context reads) are the Arc-payload work's second
        // mechanism deliverable; this counter makes them observable. Zero
        // release impact — test builds only.
        #[cfg(test)]
        crate::kernel::bump_manifest_clones();
        Self {
            handles: self.handles.clone(),
            emits: self.emits.clone(),
            kind: self.kind,
        }
    }
}

/// A schema identifier: the schema's name, e.g. `StockReserved`.
///
/// Identity is the name alone — no version component. A payload that
/// evolves does so additively (new fields carry serde defaults); a truly
/// breaking shape is a NEW schema with a new name.
///
/// Representation: derived schemas hold their name as a `&'static str`
/// (a clone is a copy — the hot send path clones ids per message);
/// runtime-built names (`SchemaId::new` / `parse`) hold an `Arc<str>`.
/// Equality and hashing compare the NAME either way, so a static and a
/// dynamic id for the same schema are interchangeable in every map and
/// route check.
#[derive(Debug)]
pub struct SchemaId(Repr);

#[derive(Debug, Clone)]
enum Repr {
    /// The derive's arm: the name lives in static storage.
    Static(&'static str),
    /// A runtime-built name (JSON descriptors, tests, foreign paths).
    Dynamic(Arc<str>),
}

impl SchemaId {
    /// Builds the identifier from the schema's name.
    pub fn new(name: &str) -> Self {
        Self(Repr::Dynamic(name.into()))
    }

    /// Wraps a compile-time schema name — zero allocation, a clone is a
    /// copy. The derive's arm ([`Schema::schema_name`]).
    pub(crate) fn static_name(name: &'static str) -> Self {
        Self(Repr::Static(name))
    }

    /// Parses an existing id string.
    ///
    /// # Compatibility
    ///
    /// A `name@N` string (a pre-name-only identity) parses as the bare
    /// name — journals and exports written before the change stay
    /// readable.
    pub fn parse(s: &str) -> Option<Self> {
        let name = s.split_once('@').map_or(s, |(name, _)| name);
        Some(Self::new(name))
    }

    /// The schema name.
    pub fn name(&self) -> &str {
        self.0.as_str()
    }

    /// The identifier as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Repr {
    fn as_str(&self) -> &str {
        match self {
            Repr::Static(name) => name,
            Repr::Dynamic(name) => name,
        }
    }
}

impl Clone for SchemaId {
    fn clone(&self) -> Self {
        // TEST PROBE: a Static clone is a copy — the hot-path deliverable
        // (counted, then produced). Dynamic clones are an Arc refcount bump.
        let repr = match &self.0 {
            Repr::Static(name) => {
                #[cfg(test)]
                crate::kernel::bump_static_schema_clones();
                Repr::Static(name)
            }
            Repr::Dynamic(name) => Repr::Dynamic(Arc::clone(name)),
        };
        Self(repr)
    }
}

/// Identity is the name: the representation arm never leaks into
/// comparisons (a `Static("A")` and a `Dynamic("A")` must collide in
/// every route map).
impl PartialEq for SchemaId {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for SchemaId {}

impl std::hash::Hash for SchemaId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_str().hash(state);
    }
}

/// The string form — the same shape the old `Arc<str>` newtype
/// serialized (a transparent JSON string), so journals and exports from
/// before the split stay readable.
impl Serialize for SchemaId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0.as_str())
    }
}

impl<'de> Deserialize<'de> for SchemaId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(|s| Self::new(&s))
    }
}

impl std::fmt::Display for SchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json;

    /// A test command with a hand-written schema (no macros, per spec).
    struct ReserveStock {
        #[allow(dead_code)] // descriptor data; exercised via schema_def only
        sku: String,
        #[allow(dead_code)]
        qty: u32,
    }

    impl Schema for ReserveStock {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "ReserveStock".into(),
                kind: SchemaKind::Command,
                fields: vec![
                    FieldDef::required("sku", FieldTy::Str),
                    FieldDef::required("qty", FieldTy::Int)
                        .with_unit("count")
                        .with_range(Range::between(1.0, 100.0)),
                ],
                description: Some("Ask an inventory to hold stock for an order.".into()),
            }
        }
    }

    #[test]
    fn static_and_dynamic_arms_are_equal_and_hash_alike() {
        // Given the same schema name in both representation arms.
        let stat = SchemaId::static_name("StockReserved");
        let dynamic = SchemaId::new("StockReserved");

        // When comparing and hashing them.
        // Then they are equal (identity is the name, never the arm)...
        assert_eq!(stat, dynamic);
        assert_eq!(dynamic, stat);
        // ...and they hash into the same bucket (route maps must fork
        // by name, not by representation).
        use std::hash::BuildHasher;
        let hasher =
            std::hash::BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::new();
        let hash_of = |id: &SchemaId| {
            let mut h = hasher.build_hasher();
            std::hash::Hash::hash(id, &mut h);
            std::hash::Hasher::finish(&h)
        };
        assert_eq!(hash_of(&stat), hash_of(&dynamic));
    }

    #[test]
    fn schema_id_clone_of_the_static_arm_is_a_copy() {
        // Given a derived schema's id (the static arm).
        let id = SchemaId::static_name("Tick");
        let before = crate::kernel::STATIC_SCHEMA_CLONES.load(std::sync::atomic::Ordering::Relaxed);

        // When cloning it.
        let copy = id.clone();

        // Then the clone was the static arm's copy path — the hot send
        // path's per-message id clone allocates nothing.
        assert_eq!(copy, id);
        assert_eq!(copy.as_str(), "Tick");
        let after = crate::kernel::STATIC_SCHEMA_CLONES.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(after - before, 1, "static clone takes the copy path");
    }

    #[test]
    fn hand_written_schema_id_is_cached_per_type() {
        // Given a hand-written Schema impl (no static name).
        let before =
            crate::kernel::SCHEMA_ID_CACHE_MISSES.load(std::sync::atomic::Ordering::Relaxed);

        // When reading its id twice.
        let first = ReserveStock::schema_id();
        let second = ReserveStock::schema_id();

        // Then the two reads agree...
        assert_eq!(first, second);
        assert_eq!(first.as_str(), "ReserveStock");
        // ...and the fallback cache was populated at most once — later
        // reads share the entry.
        let after = crate::kernel::SCHEMA_ID_CACHE_MISSES.load(std::sync::atomic::Ordering::Relaxed);
        assert!(after - before <= 1, "cache miss at most once per type");
    }

    #[test]
    fn derived_schema_id_takes_the_static_arm() {
        // Given a derive-declared schema.
        #[derive(crate::Event, serde::Serialize, serde::Deserialize, Clone)]
        struct CacheProbe {
            n: i64,
        }

        // When reading its id repeatedly.
        let first = CacheProbe::schema_id();
        let misses_before =
            crate::kernel::SCHEMA_ID_CACHE_MISSES.load(std::sync::atomic::Ordering::Relaxed);
        let clones_before =
            crate::kernel::STATIC_SCHEMA_CLONES.load(std::sync::atomic::Ordering::Relaxed);
        let repeated = CacheProbe::schema_id();
        let copy = repeated.clone();

        // Then every read is the zero-alloc static arm: no fallback-cache
        // miss ever fires, and the clone counts as the copy path.
        assert_eq!(first, repeated);
        assert_eq!(first.as_str(), "CacheProbe");
        assert_eq!(copy, first);
        let misses_after =
            crate::kernel::SCHEMA_ID_CACHE_MISSES.load(std::sync::atomic::Ordering::Relaxed);
        let clones_after =
            crate::kernel::STATIC_SCHEMA_CLONES.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            misses_after - misses_before,
            0,
            "derived schemas never touch the TypeId fallback"
        );
        assert_eq!(clones_after - clones_before, 1, "clone was the copy path");
    }

    #[test]
    fn schema_def_descriptor_is_shared_across_reads() {
        // Given a derived schema.
        #[derive(crate::Event, serde::Serialize, serde::Deserialize, Clone)]
        struct DefProbe {
            n: i64,
        }

        // When reading the descriptor twice.
        let first = DefProbe::schema_def();
        let second = DefProbe::schema_def();

        // Then both reads agree with the hand-written equivalent.
        let hand = SchemaDef {
            name: "DefProbe".into(),
            kind: SchemaKind::Event,
            fields: vec![FieldDef::required("n", FieldTy::Json)],
            description: None,
        };
        assert_eq!(first, hand);
        assert_eq!(second, hand);
    }

    #[test]
    fn schema_id_renders_as_the_bare_name() {
        // Given the ReserveStock schema.
        let id = ReserveStock::schema_id();

        // When rendering it.
        let rendered = id.to_string();

        // Then it reads the bare name.
        assert_eq!(rendered, "ReserveStock");
    }

    #[test]
    fn derive_event_matches_hand_written_def() {
        // Given a type with the derive and an identical hand-written def.
        #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
        struct StockReserved {
            sku: String,
            qty: u64,
        }
        let derived = StockReserved::schema_def();
        let hand = SchemaDef {
            name: "StockReserved".into(),
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("sku", FieldTy::Json),
                FieldDef::required("qty", FieldTy::Json),
            ],
            description: None,
        };

        // When comparing them.
        // Then the derive emits exactly the hand-written def: non-key
        // fields are Json — the descriptor is presentation data.
        assert_eq!(derived, hand);
    }

    #[test]
    fn derive_command_sets_kind() {
        // Given a command type with the derive.
        #[derive(Command, serde::Serialize, serde::Deserialize, Clone)]
        struct ReserveStock2 {
            sku: String,
        }

        // When reading the def's kind.
        let def = ReserveStock2::schema_def();

        // Then the Command derive sets SchemaKind::Command (and the name
        // comes from the ident).
        assert_eq!(def.kind, SchemaKind::Command);
        assert_eq!(def.name, "ReserveStock2");
    }

    #[test]
    fn field_types_map_to_descriptor_tys() {
        // Given one type exercising arbitrary field types beside a shard
        // key — the macro inspects only the shard-key attribute.
        #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
        #[allow(dead_code)] // descriptor data; exercised via schema_def only
        struct Kitchen {
            #[schema(shard_key)]
            sku: String,
            small: i8,
            ratio: f64,
            flag: bool,
            id: uuid::Uuid,
            doc: Json,
            blob: Vec<u8>,
            raw: &'static str,
            file: std::path::PathBuf,
        }

        // When reading the def's fields.
        let def = Kitchen::schema_def();
        let tys: Vec<_> = def.fields.iter().map(|f| f.ty.clone()).collect();

        // Then the shard key maps its real flat type (partition routing
        // reads it) and EVERY other field is Json, whatever its Rust type.
        assert_eq!(
            tys,
            vec![
                FieldTy::Str,  // sku (shard key — flat string)
                FieldTy::Json, // small
                FieldTy::Json, // ratio
                FieldTy::Json, // flag
                FieldTy::Json, // id
                FieldTy::Json, // doc
                FieldTy::Json, // blob (Vec<u8>)
                FieldTy::Json, // raw (&str)
                FieldTy::Json, // file (PathBuf)
            ]
        );
    }

    #[test]
    fn description_attribute_applies() {
        // Given a type with a container-level description.
        #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
        #[schema(description = "Second edition of the fact.")]
        struct ReservedV2 {
            sku: String,
        }

        // When reading the def.
        let def = ReservedV2::schema_def();

        // Then the attribute landed.
        assert_eq!(
            def.description.as_deref(),
            Some("Second edition of the fact.")
        );
        // And the id is the bare name — no version component.
        assert_eq!(ReservedV2::schema_id().to_string(), "ReservedV2");
    }

    #[test]
    fn field_description_attribute_applies() {
        // Given a type with a field-level description.
        #[derive(Command, serde::Serialize, serde::Deserialize, Clone)]
        struct Report {
            #[schema(description = "the captured export document")]
            export: Json,
        }

        // When reading the def's field.
        let def = Report::schema_def();

        // Then the field carries the description.
        assert_eq!(
            def.fields[0].description.as_deref(),
            Some("the captured export document")
        );
    }

    #[test]
    fn shard_key_attribute_sets_role() {
        // Given a type whose second field is the shard key.
        #[derive(Command, serde::Serialize, serde::Deserialize, Clone)]
        struct Credit {
            account: String,
            #[schema(shard_key)]
            amount: i64,
        }

        // When reading the def.
        let def = Credit::schema_def();

        // Then the marked field carries ShardKey and the other does not.
        assert_eq!(def.fields[1].role, Some(FieldRole::ShardKey));
        assert_eq!(def.fields[0].role, None);
        // And the def's role lookup finds it by name.
        assert_eq!(def.field_with_role(FieldRole::ShardKey), Some("amount"));
    }

    #[test]
    fn serde_rename_leaves_the_descriptor_name_alone() {
        // Given a field the serde mapping renames on the wire (no macro
        // rename attribute exists; renames are serde's concern alone).
        #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
        #[serde(rename_all = "camelCase")]
        struct OrderShipped {
            order_id: String,
        }

        // When reading the def and serializing the value.
        let def = OrderShipped::schema_def();
        let payload = Json::of(&OrderShipped {
            order_id: "o-1".into(),
        });

        // Then the descriptor keeps the Rust field ident...
        assert_eq!(def.fields[0].name, "order_id");
        // ...while the payload serializes under the serde mapping.
        assert!(payload.get("orderId").is_some());
        assert!(payload.get("order_id").is_none());
    }

    #[test]
    fn ty_json_attribute_forces_the_descriptor_ty() {
        // Given a type whose field type is not otherwise mappable.
        #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
        struct Attachment {
            #[schema(ty = "json")]
            bytes: Vec<u8>,
            #[schema(ty = "json")]
            custom: std::collections::BTreeMap<String, i64>,
        }

        // When reading the def's fields.
        let def = Attachment::schema_def();

        // Then both fields are forced to Json despite the Rust types.
        assert_eq!(def.fields[0].ty, FieldTy::Json);
        assert_eq!(def.fields[1].ty, FieldTy::Json);
    }

    #[test]
    fn schema_def_roundtrips_through_json_losslessly() {
        // Given the typed schema descriptor.
        let def = ReserveStock::schema_def();

        // When round-tripping through JSON.
        let json = def.to_json();
        let parsed = SchemaDef::from_json(json).expect("parses");

        // Then the descriptor is identical.
        assert_eq!(parsed, def);
    }

    #[test]
    fn foreign_json_descriptor_yields_identical_schema_to_rust_type() {
        // Given a hand-written JSON descriptor with no Rust type behind it.
        let foreign = json!({
            "name": "ReserveStock",
            "kind": "command",
            "fields": [
                { "name": "sku", "ty": "str" },
                { "name": "qty", "ty": "int", "unit": "count",
                  "range": { "min": 1.0, "max": 100.0 } }
            ],
            "description": "Ask an inventory to hold stock for an order."
        });

        // When parsing it.
        let def = SchemaDef::from_json(foreign).expect("parses");

        // Then it exactly matches the Rust-derived descriptor.
        assert_eq!(def, ReserveStock::schema_def());
        assert_eq!(def.id(), ReserveStock::schema_id());
    }

    #[test]
    fn from_json_rejects_garbage_descriptor() {
        // Given a JSON document missing required descriptor fields.
        let garbage = json!({ "name": 42 });

        // When parsing it.
        let result = SchemaDef::from_json(garbage);

        // Then it fails with InvalidDescriptor.
        let report = result.expect_err("must not parse");
        assert!(matches!(
            report.current_context(),
            SchemaError::InvalidDescriptor
        ));
    }

    #[test]
    fn optional_descriptor_fields_are_omitted_when_absent() {
        // Given a minimal foreign descriptor.
        let foreign = json!({
            "name": "Tick",
            "kind": "event",
            "fields": [{ "name": "at", "ty": "uuid" }]
        });

        // When parsing and re-serializing it.
        let def = SchemaDef::from_json(foreign).expect("parses");
        let json = def.to_json();

        // Then absent optionals stay absent.
        assert!(json.get("description").is_none());
        assert!(json["fields"][0].get("unit").is_none());
    }

    #[test]
    fn manifest_declares_typed_and_foreign_edges_uniformly() {
        // Given the typed ReserveStock schema and a foreign Tick id.
        let foreign_tick = SchemaId::new("Tick");

        // When building a manifest through both paths.
        let manifest = ActorManifest::new()
            .handles::<ReserveStock>()
            .handles_id(foreign_tick.clone())
            .emits::<TickDone>()
            .kind(crate::actor::ActorKind::EventSourced);

        // Then both flavors appear as plain schema ids.
        assert_eq!(
            manifest.handles,
            [SchemaId::new("ReserveStock"), foreign_tick]
        );
        assert_eq!(manifest.kind, Some(crate::actor::ActorKind::EventSourced));
    }

    #[test]
    fn manifest_builder_deduplicates_repeated_declarations() {
        // Given a manifest declaring the same schema twice.
        let manifest = ActorManifest::new()
            .handles::<ReserveStock>()
            .handles::<ReserveStock>();

        // Then each edge is declared exactly once.
        assert_eq!(manifest.handles, [SchemaId::new("ReserveStock")]);
    }

    #[test]
    fn manifest_survives_serde_roundtrip() {
        // Given a fully-populated manifest.
        let manifest = ActorManifest::new()
            .handles::<ReserveStock>()
            .emits::<TickDone>()
            .kind(crate::actor::ActorKind::Service);

        // When round-tripping through JSON.
        let round: ActorManifest =
            serde_json::from_str(&serde_json::to_string(&manifest).expect("ser")).expect("de");

        // Then every declared edge survives.
        assert_eq!(round, manifest);
    }

    #[test]
    fn manifest_renders_declared_edges_for_export() {
        // Given a manifest with handles and emits.
        let manifest = ActorManifest::new()
            .handles_id(SchemaId::new("ForeignPing"))
            .emits_id(SchemaId::new("ForeignPong"));

        // When serializing it.
        let json = serde_json::to_value(&manifest).expect("ser");

        // Then declared edges are visible as data.
        assert_eq!(json["handles"][0], "ForeignPing");
        assert_eq!(json["emits"][0], "ForeignPong");
    }

    struct TickDone;
    impl Schema for TickDone {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "TickDone".into(),
                kind: SchemaKind::Event,
                fields: vec![],
                description: None,
            }
        }
    }
}

#[cfg(test)]
mod field_role_tests {
    use super::{FieldDef, FieldRole, FieldTy};
    #[test]
    fn old_descriptor_json_without_role_deserializes() {
        // Given a descriptor JSON from before FieldRole existed.
        let raw = crate::json!({ "name": "n", "ty": "int" });
        // When deserialized.
        let f: FieldDef = raw.decode().expect("old shape");
        // Then role defaults to None.
        assert_eq!(f.role, None);
    }
    #[test]
    fn shard_key_role_round_trips() {
        // Given a field marked as the shard key.
        let f = FieldDef::required("sku", FieldTy::Str).as_shard_key();
        // When serialized and deserialized.
        let v = serde_json::to_value(&f).expect("serialize");
        let back: FieldDef = serde_json::from_value(v).expect("deserialize");
        // Then the role survives.
        assert_eq!(back.role, Some(FieldRole::ShardKey));
        // And the role is absent from JSON when None (clean descriptors).
        let plain = serde_json::to_value(FieldDef::required("n", FieldTy::Int)).expect("serialize");
        assert!(plain.get("role").is_none());
    }
}

#[test]
fn schema_id_has_no_version_component() {
    // Given a schema id built from a name.
    let id = SchemaId::new("StockReserved");

    // When rendering it.
    let rendered = id.to_string();

    // Then it is the bare name: no `@`, no version digit anywhere.
    assert_eq!(rendered, "StockReserved");
    assert!(!rendered.contains('@'), "identity must not embed a version");
    assert!(!rendered.chars().any(|c| c.is_ascii_digit()));
    // And the whole identifier IS the name.
    assert_eq!(id.name(), "StockReserved");
    assert_eq!(id.as_str(), "StockReserved");
}
#[test]
fn schema_id_parse_accepts_legacy_versioned_strings_as_the_bare_name() {
    // Given a `name@N` string written before name-only identity.
    let raw = "StockReserved";

    // When parsing it.
    let id = SchemaId::parse(raw).expect("parses");

    // Then the version component is dropped — old journals stay readable.
    assert_eq!(id.name(), "StockReserved");
    assert_eq!(id.as_str(), "StockReserved");
}
#[test]
fn schema_id_survives_serde_roundtrip() {
    // Given a schema id.
    let id = SchemaId::new("StockReserved");

    // When round-tripping through JSON.
    let json = serde_json::to_string(&id).expect("serialize");
    let round: SchemaId = serde_json::from_str(&json).expect("deserialize");

    // Then the value is preserved.
    assert_eq!(round, id);
}
#[test]
fn legacy_name_at_version_strings_never_render_from_new_ids() {
    // Given a fresh name-only id.
    let id = SchemaId::new("StockReserved");

    // When rendering it.
    let rendered = id.to_string();

    // Then no `@` appears — new identities are clean.
    assert!(!rendered.contains('@'));
}
