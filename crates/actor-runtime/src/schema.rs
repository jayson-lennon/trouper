//! Message schemas as runtime data.
//!
//! Rust types and external JSON descriptors register into the same schema
//! table (see [`crate::registry`]): a schema defined outside Rust is just a
//! [`SchemaDef`] parsed from JSON, indistinguishable from one derived from a
//! Rust type. `SchemaId`s embed their version (`name@version`) from day one.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::types::SchemaId;

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
    /// Arbitrary JSON; the escape hatch for payloads the canvas need not
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
    /// The unit of measure, e.g. `"ms"`, `"kg"`; presentation data for the
    /// canvas, never interpreted by the runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// Accepted numeric bounds, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<Range>,
    /// Human-facing description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl FieldDef {
    /// A required field with no unit, range, or description.
    pub fn required(name: &str, ty: FieldTy) -> Self {
        Self {
            name: name.to_owned(),
            ty,
            unit: None,
            range: None,
            description: None,
        }
    }

    /// Attaches a unit of measure.
    pub fn with_unit(mut self, unit: &str) -> Self {
        self.unit = Some(unit.to_owned());
        self
    }

    /// Attaches a numeric range.
    pub fn with_range(mut self, range: Range) -> Self {
        self.range = Some(range);
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
    pub fn from_json(json: JsonValue) -> Result<Self, error_stack::Report<SchemaError>> {
        use error_stack::ResultExt;
        serde_json::from_value(json).change_context(SchemaError::InvalidDescriptor)
    }

    /// The schema's stable identifier (`name@version`).
    pub fn id(&self) -> SchemaId {
        SchemaId::new(&self.name, self.version)
    }

    /// Serializes the descriptor to JSON — the export/canvas projection.
    pub fn to_json(&self) -> JsonValue {
        serde_json::to_value(self).unwrap_or(JsonValue::Null)
    }
}

/// Implemented by Rust types that have a schema; the mirror of
/// [`SchemaDef::from_json`] for the typed path.
///
/// Both paths produce the same [`SchemaDef`], so a schema is identical
/// whether it arrived from code or from data.
pub trait Schema {
    /// The type's schema descriptor.
    fn schema_def() -> SchemaDef;

    /// The type's stable [`SchemaId`].
    fn schema_id() -> SchemaId {
        let def = Self::schema_def();
        def.id()
    }
}

/// An actor's declared edges: which schemas it handles, which it emits, and
/// which topics it emits to or subscribes.
///
/// Manifest entries reference [`SchemaId`]s only — Rust-registered and
/// JSON-registered schemas are indistinguishable here, which is what makes
/// declared edges uniform for export. Produced by the [`ActorManifest`]
/// builder at spawn; the kernel registers routes from it.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ActorManifest {
    /// Command/message schemas this actor accepts.
    #[serde(default)]
    pub handles: Vec<SchemaId>,
    /// Event schemas this actor emits.
    #[serde(default)]
    pub emits: Vec<SchemaId>,
    /// Topics this actor publishes on.
    #[serde(default)]
    pub emits_on_topics: Vec<crate::types::Topic>,
    /// Topics this actor subscribes to.
    #[serde(default)]
    pub subscribes: Vec<crate::types::Topic>,
    /// Which actor contract this actor implements.
    #[serde(default)]
    pub kind: Option<crate::types::ActorKind>,
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

    /// Declares that this actor publishes on `topic`.
    pub fn emits_on_topic(mut self, topic: crate::types::Topic) -> Self {
        if !self.emits_on_topics.contains(&topic) {
            self.emits_on_topics.push(topic);
        }
        self
    }

    /// Declares that this actor subscribes to `topic`.
    pub fn subscribes(mut self, topic: crate::types::Topic) -> Self {
        if !self.subscribes.contains(&topic) {
            self.subscribes.push(topic);
        }
        self
    }

    /// Declares the actor contract this actor implements.
    pub fn kind(mut self, kind: crate::types::ActorKind) -> Self {
        self.kind = Some(kind);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
            .kind(crate::types::ActorKind::EventSourced);

        // Then both flavors appear as plain schema ids.
        assert_eq!(
            manifest.handles,
            [SchemaId::new("ReserveStock", 1), foreign_tick]
        );
        assert_eq!(manifest.kind, Some(crate::types::ActorKind::EventSourced));
    }

    #[test]
    fn manifest_builder_deduplicates_repeated_declarations() {
        // Given a manifest declaring the same schema twice.
        let manifest = ActorManifest::new()
            .handles::<ReserveStock>()
            .handles::<ReserveStock>()
            .emits_on_topic(crate::types::Topic::new("inventory.events"))
            .emits_on_topic(crate::types::Topic::new("inventory.events"));

        // Then each edge is declared exactly once.
        assert_eq!(manifest.handles, [SchemaId::new("ReserveStock", 1)]);
        assert_eq!(
            manifest.emits_on_topics,
            [crate::types::Topic::new("inventory.events")]
        );
    }

    #[test]
    fn manifest_survives_serde_roundtrip() {
        // Given a fully-populated manifest.
        let manifest = ActorManifest::new()
            .handles::<ReserveStock>()
            .emits::<TickDone>()
            .emits_on_topic(crate::types::Topic::new("inventory.events"))
            .subscribes(crate::types::Topic::new("commands.audit"))
            .kind(crate::types::ActorKind::Service);

        // When round-tripping through JSON.
        let round: ActorManifest =
            serde_json::from_str(&serde_json::to_string(&manifest).expect("ser")).expect("de");

        // Then every declared edge survives.
        assert_eq!(round, manifest);
    }

    #[test]
    fn manifest_renders_declared_edges_for_export() {
        // Given a manifest with handles and a subscription.
        let manifest = ActorManifest::new()
            .handles_id(SchemaId::new("ForeignPing", 4))
            .subscribes(crate::types::Topic::new("inventory.events"));

        // When serializing it.
        let json = serde_json::to_value(&manifest).expect("ser");

        // Then declared edges are visible as data.
        assert_eq!(json["handles"][0], "ForeignPing@4");
        assert_eq!(json["subscribes"][0], "inventory.events");
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
