//! The registry: kernel, not actor.
//!
//! Bootstrap paradox resolved by construction: the registry must never
//! deadlock and must survive every actor restart, so it is plain kernel data
//! owned by the [`crate::system::ActorSystem`] — path→endpoint slots, the
//! schema table, schema→handler routes, and topic→subscriber tables. Actor
//! identity is its registered path; handles survive restarts because slots
//! are swapped, never invalidated.
//!
//! This phase holds the schema table; slots, handler routes, and topics
//! arrive with the delivery kernel.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value as JsonValue;

use crate::schema::{Schema, SchemaDef, SchemaError};
use crate::types::SchemaId;

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
        json: JsonValue,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn reserve_stock_v(version: u32) -> SchemaDef {
        SchemaDef {
            name: "ReserveStock".into(),
            version,
            kind: crate::schema::SchemaKind::Command,
            fields: vec![crate::schema::FieldDef::required(
                "sku",
                crate::schema::FieldTy::Str,
            )],
            description: None,
        }
    }

    #[test]
    fn register_is_idempotent_per_name_and_version() {
        // Given a schema table with ReserveStock@1 already registered.
        let mut table = SchemaTable::default();
        let first = table.register(reserve_stock_v(1));

        // When registering ReserveStock@1 again.
        let second = table.register(reserve_stock_v(1));

        // Then the id is stable and only one entry exists.
        assert_eq!(first, second);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn register_keeps_versions_sorted_and_latest_reports_highest() {
        // Given ReserveStock registered at versions 1 and 3.
        let mut table = SchemaTable::default();
        table.register(reserve_stock_v(1));
        table.register(reserve_stock_v(3));

        // When asking for the latest ReserveStock.
        let latest = table.latest("ReserveStock").expect("present");

        // Then it is version 3.
        assert_eq!(latest.id(), SchemaId::new("ReserveStock", 3));
    }

    #[test]
    fn by_id_requires_exact_version_match() {
        // Given ReserveStock at versions 1 and 2.
        let mut table = SchemaTable::default();
        table.register(reserve_stock_v(1));
        table.register(reserve_stock_v(2));

        // When looking up by id — exact and missing.
        let exact = table.by_id(&SchemaId::new("ReserveStock", 1));
        let missing = table.by_id(&SchemaId::new("ReserveStock", 9));

        // Then only the exact version is found.
        assert!(exact.is_some());
        assert!(missing.is_none());
    }

    #[test]
    fn rust_and_json_flavors_produce_identical_registrations() {
        // Given a typed schema and its hand-written JSON twin.
        let json_twin = json!({
            "name": "ReserveStock",
            "version": 1,
            "kind": "command",
            "fields": [{ "name": "sku", "ty": "str" }]
        });

        // When registering each flavor into its own table.
        let mut typed_table = SchemaTable::default();
        let mut foreign_table = SchemaTable::default();
        let typed_id = typed_table.register(reserve_stock_v(1));
        let foreign_id = foreign_table
            .register_json(json_twin)
            .expect("valid descriptor");

        // Then both tables hold identical descriptors under identical ids.
        assert_eq!(typed_id, foreign_id);
        assert_eq!(
            typed_table.by_id(&typed_id),
            foreign_table.by_id(&foreign_id)
        );
    }

    #[test]
    fn register_json_rejects_invalid_descriptors() {
        // Given a garbage descriptor.
        let mut table = SchemaTable::default();
        let garbage = json!({ "name": ["not", "a", "schema"] });

        // When registering it as JSON.
        let result = table.register_json(garbage);

        // Then registration fails and the table stays empty.
        assert!(result.is_err());
        assert!(table.is_empty());
    }

    #[test]
    fn all_lists_every_schema_in_name_then_version_order() {
        // Given schemas registered out of order across two names.
        let mut table = SchemaTable::default();
        table.register(reserve_stock_v(2));
        table.register({
            let mut def = reserve_stock_v(1);
            def.name = "AuditNote".into();
            def
        });
        table.register(reserve_stock_v(1));

        // When listing all schemas.
        let ids: Vec<String> = table.all().iter().map(|def| def.id().to_string()).collect();

        // Then they are sorted by name, then version.
        assert_eq!(
            ids,
            ["AuditNote@1", "ReserveStock@1", "ReserveStock@2"]
        );
    }
}
