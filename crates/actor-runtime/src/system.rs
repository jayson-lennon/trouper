//! The system facade: the single handle through which the runtime is
//! configured and driven. Phase 9 completes spawn/stop/export; this phase
//! establishes the kernel data it guards.
//!
//! The registry is kernel, not an actor — owned here behind a lock so schema
//! registration can never deadlock and survives every actor restart.

use std::sync::Mutex;

use serde_json::Value as JsonValue;

use crate::clock::{ClockService, SystemClock};
use crate::registry::SchemaTable;
use crate::schema::Schema;
use crate::types::SchemaId;

/// The actor system. One instance per machine; clone-free shared handle
/// passed by reference.
pub struct ActorSystem {
    /// Kernel tables (schemas now; slots, routes, topics with the kernel).
    schemas: Mutex<SchemaTable>,
    /// The only source of time.
    clock: ClockService,
}

impl ActorSystem {
    /// Creates a system on the wall clock.
    pub fn new() -> Self {
        Self::with_clock(ClockService::new(std::sync::Arc::new(SystemClock::new())))
    }

    /// Creates a system on an injected clock (tests use [`crate::clock::FakeClock`]).
    pub fn with_clock(clock: ClockService) -> Self {
        Self {
            schemas: Mutex::new(SchemaTable::default()),
            clock,
        }
    }

    /// Registers a Rust type's schema — the typed flavor.
    ///
    /// Idempotent per name+version: the first registration wins, and the
    /// returned id is stable across repeat registrations.
    pub fn register_schema<S: Schema>(&self) -> SchemaId {
        let mut table = self
            .schemas
            .lock()
            .expect("schema table lock poisoned");
        table.register_of::<S>()
    }

    /// Registers a schema from a JSON descriptor — the foreign flavor, for
    /// schemas defined outside Rust.
    ///
    /// # Errors
    ///
    /// Returns an error when `json` is not a valid schema descriptor.
    pub fn register_schema_json(
        &self,
        json: JsonValue,
    ) -> Result<SchemaId, error_stack::Report<crate::schema::SchemaError>> {
        let mut table = self
            .schemas
            .lock()
            .expect("schema table lock poisoned");
        table.register_json(json)
    }

    /// The registered descriptor for an exact `name@version` id, if any.
    pub fn schema(&self, id: &SchemaId) -> Option<crate::schema::SchemaDef> {
        let table = self
            .schemas
            .lock()
            .expect("schema table lock poisoned");
        table.by_id(id).cloned()
    }

    /// The system's clock (tests use this to reach the [`FakeClock`]).
    ///
    /// [`FakeClock`]: crate::clock::FakeClock
    pub fn clock(&self) -> &ClockService {
        &self.clock
    }
}

impl Default for ActorSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldDef, FieldTy, SchemaDef, SchemaKind};
    use serde_json::json;

    struct Tick;
    impl Schema for Tick {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Tick".into(),
                version: 1,
                kind: SchemaKind::Event,
                fields: vec![FieldDef::required("at", FieldTy::Uuid)],
                description: None,
            }
        }
    }

    #[test]
    fn register_schema_is_idempotent_on_the_system() {
        // Given a system with Tick registered.
        let system = ActorSystem::new();
        let first = system.register_schema::<Tick>();

        // When registering Tick again.
        let second = system.register_schema::<Tick>();

        // Then both calls return the same id and one schema is stored.
        assert_eq!(first, second);
        assert!(system.schema(&first).is_some());
    }

    #[test]
    fn register_schema_json_accepts_foreign_descriptors() {
        // Given a system and a JSON-only descriptor.
        let system = ActorSystem::new();
        let foreign = json!({
            "name": "ForeignPing",
            "version": 4,
            "kind": "command",
            "fields": []
        });

        // When registering it as JSON.
        let id = system.register_schema_json(foreign).expect("valid");

        // Then it is retrievable by its name@version id.
        let stored = system.schema(&id).expect("stored");
        assert_eq!(id.to_string(), "ForeignPing@4");
        assert_eq!(stored.name, "ForeignPing");
    }
}
