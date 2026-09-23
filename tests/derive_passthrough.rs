//! Pass-through contract of the Event/Command derives: the macro never
//! inspects a field type for validity — every non-shard-key field's
//! descriptor is `FieldTy::Json`, the shard-key field maps its real flat
//! type, and renames belong to serde alone.

use trouper::prelude::*;

#[test]
fn exotic_field_types_derive_with_json_descriptors() {
    // Given a struct with field types no mapping table could name.
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    #[allow(dead_code)] // descriptor data; exercised via schema_def only
    struct Probe {
        maybe: Option<uuid::Uuid>,
        map: std::collections::HashMap<String, i64>,
        items: Vec<String>,
        origin: StreamOrigin,
    }
    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    #[serde(rename_all = "snake_case")]
    enum StreamOrigin {
        Kernel,
        Foreign,
    }

    // When reading the def's fields.
    let def = Probe::schema_def();
    let tys: Vec<_> = def.fields.iter().map(|f| f.ty.clone()).collect();

    // Then every field derives and maps to Json — no compile error, no
    // type-keyed rejection.
    assert_eq!(tys, vec![FieldTy::Json; 4]);
}

#[test]
fn shard_key_field_maps_its_flat_type_and_reads_back() {
    // Given a struct whose string shard key rides beside an exotic field.
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    #[allow(dead_code)] // descriptor data; exercised via schema_def only
    struct Shipped {
        #[schema(shard_key)]
        order_id: String,
        meta: std::collections::HashMap<String, i64>,
    }
    let payload = Shipped {
        order_id: "o-1".into(),
        meta: std::collections::HashMap::new(),
    };

    // When reading the def and the live value's fields.
    let def = Shipped::schema_def();

    // Then the key maps its real flat type (partition routing reads it)...
    assert_eq!(def.fields[0].ty, FieldTy::Str);
    // ...the exotic field is Json...
    assert_eq!(def.fields[1].ty, FieldTy::Json);
    // ...the key reads back by its Rust ident...
    assert_eq!(payload.field("order_id"), Some("o-1".into()));
    // ...and non-key fields read None — the shard key is the one runtime
    // reader.
    assert_eq!(payload.field("meta"), None);
}

#[test]
fn numeric_shard_key_maps_to_int_descriptor() {
    // Given a struct with a numeric shard key.
    #[derive(Command, serde::Serialize, serde::Deserialize, Clone)]
    struct Picked {
        #[schema(shard_key)]
        seq: u64,
    }
    let payload = Picked { seq: 7 };

    // When reading the descriptor and the live value's key.
    let def = Picked::schema_def();

    // Then the key maps Int and renders its Display form — the string the
    // partition path is built from.
    assert_eq!(def.fields[0].ty, FieldTy::Int);
    assert_eq!(payload.field("seq"), Some("7".into()));
}

#[test]
fn serde_renamed_non_shard_field_keeps_the_rust_ident_descriptor() {
    // Given a field the serde mapping renames on the wire.
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    struct OrderShipped {
        order_id: String,
    }

    // When reading the descriptor and serializing the value.
    let def = OrderShipped::schema_def();
    let payload = Json::of(&OrderShipped {
        order_id: "o-1".into(),
    });

    // Then the descriptor keeps the Rust ident (renames are serde's
    // concern alone)...
    assert_eq!(def.fields[0].name, "order_id");
    // ...while the payload serializes under the serde mapping.
    assert!(payload.get("orderId").is_some());
    assert!(payload.get("order_id").is_none());
}

#[test]
fn ty_override_accepts_all_descriptor_names() {
    // Given one struct per descriptor name, each overriding a field that
    // would otherwise map to Json.
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    struct ProbeBool {
        #[schema(ty = "bool")]
        v: Vec<String>,
    }
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    struct ProbeInt {
        #[schema(ty = "int")]
        v: Vec<String>,
    }
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    struct ProbeFloat {
        #[schema(ty = "float")]
        v: Vec<String>,
    }
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    struct ProbeStr {
        #[schema(ty = "str")]
        v: Vec<String>,
    }
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    struct ProbeUuid {
        #[schema(ty = "uuid")]
        v: Vec<String>,
    }
    #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
    struct ProbeJson {
        #[schema(ty = "json")]
        v: Vec<String>,
    }

    // When reading each override's descriptor ty.
    // Then every descriptor name is accepted and lands on its variant.
    assert_eq!(ProbeBool::schema_def().fields[0].ty, FieldTy::Bool);
    assert_eq!(ProbeInt::schema_def().fields[0].ty, FieldTy::Int);
    assert_eq!(ProbeFloat::schema_def().fields[0].ty, FieldTy::Float);
    assert_eq!(ProbeStr::schema_def().fields[0].ty, FieldTy::Str);
    assert_eq!(ProbeUuid::schema_def().fields[0].ty, FieldTy::Uuid);
    assert_eq!(ProbeJson::schema_def().fields[0].ty, FieldTy::Json);
}
