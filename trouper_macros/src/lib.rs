//! Proc-macro derives for the [trouper](https://crates.io/crates/trouper)
//! actor runtime.
//!
//! `#[derive(Event)]` and `#[derive(Command)]` generate the `SchemaDef`
//! that the runtime's schema registry expects: name from the struct ident
//! (schemas carry NO version — evolution is additive), description from
//! `#[schema(description = "...")]`, kind from which derive was used, and
//! field descriptors mapped from the struct's Rust types. They also
//! generate the runtime's `PayloadValue` impl: downcast dispatch,
//! field-by-name reads for routers, and a memoized compact JSON-text
//! encoding for the journal door.
//!
//! See the `trouper` crate's `schema` module for the trait these impls
//! feed, and that crate's `examples/` directory for usage.

extern crate proc_macro;

mod expand;

use proc_macro::TokenStream;

/// Derives `trouper::schema::Schema` (the event-kind descriptor) and
/// `trouper::envelope::PayloadValue` for a named-field struct: the fact's
/// schema data plus its live-value behavior (downcast, field reads,
/// one-serialization JSON encoding).
#[proc_macro_derive(Event, attributes(schema))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    expand_derive(input, expand::Kind::Event)
}

/// Derives `trouper::schema::Schema` (the command-kind descriptor) and
/// `trouper::envelope::PayloadValue` for a named-field struct: the
/// command's schema data plus its live-value behavior.
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
