//! Proc-macro derives for the [trouper](https://crates.io/crates/trouper)
//! actor runtime.
//!
//! `#[derive(Event)]` and `#[derive(Command)]` emit exactly two impl
//! blocks for a named-field struct: `trouper::schema::Schema` (the
//! `SchemaDef` descriptor) and `trouper::envelope::PayloadValue`
//! (downcast dispatch, shard-key field reads, memoized JSON-text
//! encoding). That is all they do. The descriptor is presentation data —
//! the runtime reads only three facts from it: the schema name (identity
//! and routing), the kind (Event broadcast vs Command route-to-one), and
//! the shard-key field's name and flat type (partition path rendering).
//! The value side — every field's actual serialization — belongs to
//! serde entirely.
//!
//! See the `trouper` crate's `schema` module for the trait these impls
//! feed, and that crate's `examples/` directory for usage.

extern crate proc_macro;

mod expand;

use proc_macro::TokenStream;

/// Derives `trouper::schema::Schema` (the event-kind descriptor) and
/// `trouper::envelope::PayloadValue` for a named-field struct: the fact's
/// schema data plus its live-value behavior (downcast, shard-key field
/// read, one-serialization JSON encoding).
///
/// # How the derive maps your struct
///
/// The derive emits the two impl blocks and nothing else; any Rust field
/// type derives — `Option<T>`, enums, `HashMap`s, anything serde can
/// serialize. Every field's descriptor is
/// `FieldTy::Json` except the
/// `#[schema(shard_key)]` field, which maps its real flat type
/// (`Int`/`Float`/`Bool`/`Str`/`Uuid`) from the Rust type — partition
/// routing reads it. No field type is ever a compile error.
///
/// The schema name is the struct ident and descriptor field names are the
/// Rust field idents. Renames are serde's concern alone:
/// `#[serde(rename = "...")]` (or `rename_all`) on any field is safe and
/// invisible to this derive — EXCEPT on the shard-key field.
///
/// # Shard-key constraints
///
/// The `#[schema(shard_key)]` field must be a flat string or number type
/// (`String`, integers, floats, `bool`, `Uuid`) and must NOT be
/// serde-renamed: the runtime reads that exact key from the wire JSON by
/// the Rust field name to compute the entity's partition path, so a
/// rename (or a non-flat type like `PathBuf`) makes routing see the key
/// as absent and entity activation fails.
///
/// # Name registration
///
/// At spawn, `ActorManifest::handles::<T>()`/`emits::<T>()` TypeId-claim
/// the schema name (one Rust type per name, process-wide): a second,
/// DIFFERENT Rust type under a claimed name panics in the manifest
/// builder — an author bug. Same-type re-claims are free. Registering a
/// descriptor as JSON alone (`SchemaTable::register_json`) cannot panic:
/// the first descriptor under a name wins, and a Rust type may later
/// adopt that name.
#[proc_macro_derive(Event, attributes(schema))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    expand_derive(input, expand::Kind::Event)
}

/// Derives `trouper::schema::Schema` (the command-kind descriptor) and
/// `trouper::envelope::PayloadValue` for a named-field struct: the
/// command's schema data plus its live-value behavior (downcast,
/// shard-key field read, one-serialization JSON encoding).
///
/// # How the derive maps your struct
///
/// The derive emits the two impl blocks and nothing else; any Rust field
/// type derives — `Option<T>`, enums, `HashMap`s, anything serde can
/// serialize. Every field's descriptor is
/// `FieldTy::Json` except the
/// `#[schema(shard_key)]` field, which maps its real flat type
/// (`Int`/`Float`/`Bool`/`Str`/`Uuid`) from the Rust type — partition
/// routing reads it. No field type is ever a compile error.
///
/// The schema name is the struct ident and descriptor field names are the
/// Rust field idents. Renames are serde's concern alone:
/// `#[serde(rename = "...")]` (or `rename_all`) on any field is safe and
/// invisible to this derive — EXCEPT on the shard-key field.
///
/// # Shard-key constraints
///
/// The `#[schema(shard_key)]` field must be a flat string or number type
/// (`String`, integers, floats, `bool`, `Uuid`) and must NOT be
/// serde-renamed: the runtime reads that exact key from the wire JSON by
/// the Rust field name to compute the entity's partition path, so a
/// rename (or a non-flat type like `PathBuf`) makes routing see the key
/// as absent and entity activation fails.
///
/// # Name registration
///
/// At spawn, `ActorManifest::handles::<T>()`/`emits::<T>()` TypeId-claim
/// the schema name (one Rust type per name, process-wide): a second,
/// DIFFERENT Rust type under a claimed name panics in the manifest
/// builder — an author bug. Same-type re-claims are free. Registering a
/// descriptor as JSON alone (`SchemaTable::register_json`) cannot panic:
/// the first descriptor under a name wins, and a Rust type may later
/// adopt that name.
#[proc_macro_derive(Command, attributes(schema))]
pub fn derive_command(input: TokenStream) -> TokenStream {
    expand_derive(input, expand::Kind::Command)
}

fn expand_derive(input: TokenStream, kind: expand::Kind) -> TokenStream {
    match expand::generate(input.into(), kind) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
