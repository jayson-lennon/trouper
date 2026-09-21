//! Proc-macro derives for the [trouper](https://crates.io/crates/trouper)
//! actor runtime.
//!
//! `#[derive(Event)]` and `#[derive(Command)]` generate the `SchemaDef`
//! that the runtime's schema registry expects: name from the struct ident,
//! version 1 (override with `#[schema(version = N)]`), kind from which
//! derive was used, and field descriptors mapped from the struct's Rust
//! types.
//!
//! See the `trouper` crate's `schema` module for the trait these impls
//! feed, and that crate's `examples/` directory for usage.

extern crate proc_macro;

mod expand;

use proc_macro::TokenStream;

/// Derives `trouper::schema::Schema` for a named-field struct, marking it
/// as an event (fact) schema ([`SchemaKind::Event`](trouper::schema::SchemaKind::Event)).
#[proc_macro_derive(Event, attributes(schema))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    expand_derive(input, expand::Kind::Event)
}

/// Derives `trouper::schema::Schema` for a named-field struct, marking it
/// as a command schema ([`SchemaKind::Command`](trouper::schema::SchemaKind::Command)).
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
