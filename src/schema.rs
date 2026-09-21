//! Message schemas as runtime data.
//!
//! Rust types and external JSON descriptors register into the same schema
//! table (see [`crate::registry`]): a schema defined outside Rust is just a
//! [`SchemaDef`] parsed from JSON, indistinguishable from one derived from a
//! Rust type. `SchemaId`s embed their version (`name@version`) from day one.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

use crate::json::Json;

/// Errors surfaced while parsing schema descriptors.
#[derive(Debug, wherror::Error)]
#[error(debug)]
pub enum SchemaError {
    /// A JSON document was not a valid [`SchemaDef`].
    InvalidDescriptor,
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
    /// The schema's name, e.g. `StockReserved`.
    pub name: String,
    /// The schema's version; [`SchemaId`] embeds it.
    pub version: u32,
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

    /// The schema's stable identifier (`name@version`).
    pub fn id(&self) -> SchemaId {
        SchemaId::new(&self.name, self.version)
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
pub trait Schema {
    /// The type's schema descriptor.
    fn schema_def() -> SchemaDef;

    /// The type's stable [`SchemaId`].
    fn schema_id() -> SchemaId {
        let def = Self::schema_def();
        def.id()
    }
}

// The derive macros live in the workspace's `trouper_macros` crate; they
// are re-exported here (and reach the prelude through the glob above) so
// users write `#[derive(Event)]` next to `#[derive(Serialize)]`.
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
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
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
    pub fn handles<S: Schema>(mut self) -> Self {
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

    /// Declares that this actor emits event schema `S`.
    pub fn emits<S: Schema>(mut self) -> Self {
        let id = S::schema_id();
        if !self.emits.contains(&id) {
            self.emits.push(id);
        }
        self
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

/// A schema identifier of the form `name@version`, e.g. `StockReserved@1`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaId(Arc<str>);

impl SchemaId {
    /// Builds `name@version` from its parts.
    pub fn new(name: &str, version: u32) -> Self {
        Self(format!("{name}@{version}").into())
    }

    /// Parses an existing `name@version` string.
    ///
    /// Returns `None` when the string has no `@` separator.
    pub fn parse(s: &str) -> Option<Self> {
        s.split_once('@').map(|_| Self(s.into()))
    }

    /// The schema name (everything before `@`).
    pub fn name(&self) -> &str {
        self.0.split_once('@').map_or(&self.0, |(name, _)| name)
    }

    /// The schema version (everything after `@`), or `None` if unversioned.
    pub fn version(&self) -> Option<u32> {
        let (_, version) = self.0.split_once('@')?;
        version.parse().ok()
    }

    /// The identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
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
                version: 1,
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
    fn schema_id_renders_as_name_at_version() {
        // Given the ReserveStock schema.
        let id = ReserveStock::schema_id();

        // When rendering it.
        let rendered = id.to_string();

        // Then it reads name@version.
        assert_eq!(rendered, "ReserveStock@1");
    }

    #[test]
    fn derive_event_matches_hand_written_def() {
        // Given a type with the derive and an identical hand-written def.
        #[derive(Event, serde::Serialize, serde::Deserialize)]
        struct StockReserved {
            sku: String,
            qty: u64,
        }
        let derived = StockReserved::schema_def();
        let hand = SchemaDef {
            name: "StockReserved".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![
                FieldDef::required("sku", FieldTy::Str),
                FieldDef::required("qty", FieldTy::Int),
            ],
            description: None,
        };

        // When comparing them.
        // Then the derive emits exactly the hand-written def.
        assert_eq!(derived, hand);
    }

    #[test]
    fn derive_command_sets_kind() {
        // Given a command type with the derive.
        #[derive(Command, serde::Serialize, serde::Deserialize)]
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
        // Given one type exercising every supported field type.
        #[derive(Event, serde::Serialize, serde::Deserialize)]
        #[allow(dead_code)] // descriptor data; exercised via schema_def only
        struct Kitchen {
            small: i8,
            medium: i64,
            unsign: u32,
            big: u64,
            arch: usize,
            ratio: f64,
            flag: bool,
            label: String,
            id: uuid::Uuid,
            doc: Json,
            blob: Vec<u8>,
            raw: &'static str,
            file: std::path::PathBuf,
        }

        // When reading the def's fields.
        let def = Kitchen::schema_def();
        let tys: Vec<_> = def.fields.iter().map(|f| f.ty.clone()).collect();

        // Then each maps to the descriptor ty from the spec table.
        assert_eq!(
            tys,
            vec![
                FieldTy::Int,   // small
                FieldTy::Int,   // medium
                FieldTy::Int,   // unsign
                FieldTy::Int,   // big
                FieldTy::Int,   // arch
                FieldTy::Float, // ratio
                FieldTy::Bool,  // flag
                FieldTy::Str,   // label
                FieldTy::Uuid,  // id
                FieldTy::Json,  // doc
                FieldTy::Json,  // blob (Vec<u8>)
                FieldTy::Str,   // raw (&str)
                FieldTy::Str,   // file (PathBuf serializes as a string)
            ]
        );
    }

    #[test]
    fn version_and_description_attributes_apply() {
        // Given a type with container-level version and description.
        #[derive(Event, serde::Serialize, serde::Deserialize)]
        #[schema(version = 2, description = "Second edition of the fact.")]
        struct ReservedV2 {
            sku: String,
        }

        // When reading the def.
        let def = ReservedV2::schema_def();

        // Then both container attributes landed.
        assert_eq!(def.version, 2);
        assert_eq!(
            def.description.as_deref(),
            Some("Second edition of the fact.")
        );
        // And the id embeds the version.
        assert_eq!(ReservedV2::schema_id().to_string(), "ReservedV2@2");
    }

    #[test]
    fn field_description_attribute_applies() {
        // Given a type with a field-level description.
        #[derive(Command, serde::Serialize, serde::Deserialize)]
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
        #[derive(Command, serde::Serialize, serde::Deserialize)]
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
    fn rename_attribute_renames_descriptor_field_only() {
        // Given a type whose Rust field name differs from the wire name.
        #[derive(Event, serde::Serialize, serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OrderShipped {
            #[schema(rename = "orderId")]
            order_id: String,
        }

        // When reading the def.
        let def = OrderShipped::schema_def();

        // Then the descriptor uses the renamed wire name...
        assert_eq!(def.fields[0].name, "orderId");
        // ...while the payload still serializes under the serde mapping.
        let payload = Json::of(&OrderShipped {
            order_id: "o-1".into(),
        });
        assert!(payload.get("orderId").is_some());
        assert!(payload.get("order_id").is_none());
    }

    #[test]
    fn ty_json_attribute_forces_the_descriptor_ty() {
        // Given a type whose field type is not otherwise mappable.
        #[derive(Event, serde::Serialize, serde::Deserialize)]
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
            "version": 1,
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
            "version": 2,
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
        // Given the typed ReserveStock schema and a foreign Tick@1 id.
        let foreign_tick = SchemaId::new("Tick", 1);

        // When building a manifest through both paths.
        let manifest = ActorManifest::new()
            .handles::<ReserveStock>()
            .handles_id(foreign_tick.clone())
            .emits::<TickDone>()
            .kind(crate::actor::ActorKind::EventSourced);

        // Then both flavors appear as plain schema ids.
        assert_eq!(
            manifest.handles,
            [SchemaId::new("ReserveStock", 1), foreign_tick]
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
        assert_eq!(manifest.handles, [SchemaId::new("ReserveStock", 1)]);
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
            .handles_id(SchemaId::new("ForeignPing", 4))
            .emits_id(SchemaId::new("ForeignPong", 4));

        // When serializing it.
        let json = serde_json::to_value(&manifest).expect("ser");

        // Then declared edges are visible as data.
        assert_eq!(json["handles"][0], "ForeignPing@4");
        assert_eq!(json["emits"][0], "ForeignPong@4");
    }

    struct TickDone;
    impl Schema for TickDone {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "TickDone".into(),
                version: 1,
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
fn schema_id_renders_name_and_version() {
    // Given a name and version.
    let id = SchemaId::new("StockReserved", 1);

    // When displaying the id.
    let rendered = id.to_string();

    // Then it reads `name@version`.
    assert_eq!(rendered, "StockReserved@1");
}
#[test]
fn schema_id_parses_name_and_version_back_out() {
    // Given a rendered schema id string.
    let raw = "StockReserved@3";

    // When parsing it.
    let id = SchemaId::parse(raw).expect("parses");

    // Then name and version round-trip.
    assert_eq!(id.name(), "StockReserved");
    assert_eq!(id.version(), Some(3));
}
#[test]
fn schema_id_parse_rejects_unversioned_string() {
    // Given a string without an `@` separator.
    let raw = "StockReserved";

    // When parsing it.
    let parsed = SchemaId::parse(raw);

    // Then parsing fails.
    assert!(parsed.is_none());
}
#[test]
fn schema_id_survives_serde_roundtrip() {
    // Given a schema id.
    let id = SchemaId::new("StockReserved", 1);

    // When round-tripping through JSON.
    let json = serde_json::to_string(&id).expect("serialize");
    let round: SchemaId = serde_json::from_str(&json).expect("deserialize");

    // Then the value is preserved.
    assert_eq!(round, id);
}
