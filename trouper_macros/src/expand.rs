//! Derive expansion: parse the derive input and generate the
//! `trouper::schema::Schema` impl plus the `trouper::envelope::PayloadValue`
//! impl (downcast dispatch, field-by-name reads, memoized JSON-text
//! encoding) for the derived struct.
//!
//! The derive's only job is emitting those two impl blocks; the descriptor
//! they produce is presentation data. Exactly three facts in it are
//! load-bearing at runtime: the schema name (identity and routing), the
//! kind (Event broadcast vs Command route-to-one), and the shard-key
//! field's name and flat type (partition path rendering). Accordingly the
//! derive inspects exactly one thing per struct — which field (if any) is
//! the `#[schema(shard_key)]` — and maps EVERY other field to
//! `FieldTy::Json` regardless of its Rust type. No field type is a
//! compile error; the value side belongs to serde entirely.

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, Type, TypePath};

/// Which derive marker was used — selects the `SchemaKind` of the
/// generated `SchemaDef`.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
    Event,
    Command,
}

impl Kind {
    /// The `::trouper::schema::SchemaKind` variant name for this derive.
    fn variant(self) -> &'static str {
        match self {
            Kind::Event => "Event",
            Kind::Command => "Command",
        }
    }
}

/// Generates the `Schema` impl for the derived struct: name from the
/// struct ident, description from the `#[schema(...)]` container
/// attribute (default: no description), kind from the derive, and field
/// descriptors — `FieldTy::Json` for every field except the shard key,
/// which maps its real flat type from the Rust type.
///
/// Also generates the `PayloadValue` impl: `as_any` (the downcast
/// dispatch core), `field` (a string read of the shard-key field only —
/// the one runtime reader is partition-key extraction), and
/// `to_json_bytes` (compact JSON text, memoized in a `OnceLock` so the
/// value serializes at most once no matter how many readers ask — the
/// journal door's single-serialization contract).
pub fn generate(input: TokenStream, kind: Kind) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let ident = &input.ident;

    // Container #[schema(...)] attributes: `description = "..."`. A
    // `version = N` attribute is REJECTED with a named error: schemas carry
    // no version — evolution is additive (new fields get serde defaults).
    let mut description: Option<String> = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("schema") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("version") {
                Err(meta.error(
                    "schemas carry no version; drop `version = N` — evolution is \
                     additive (new fields get serde defaults), breaking shapes get a \
                     new schema name",
                ))
            } else if meta.path.is_ident("description") {
                if description.is_some() {
                    return Err(meta.error("duplicate `description` in #[schema(...)]"));
                }
                let lit: syn::LitStr = meta.value()?.parse()?;
                description = Some(lit.value());
                Ok(())
            } else {
                Err(meta
                    .error("unknown #[schema(...)] container attribute; supported: description"))
            }
        })?;
    }

    // Named-field structs (or unit structs = zero-field schemas);
    // everything else is a spanned error. Iterate over `fields`.
    let field_list: Vec<syn::Field> = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => fields.named.iter().cloned().collect(),
            Fields::Unit => Vec::new(),
            _ => {
                return Err(syn::Error::new_spanned(
                    &data.fields,
                    "Event/Command derive requires a named-field struct",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "Event/Command derive requires a named-field struct",
            ));
        }
    };
    let fields = &field_list;

    // Schemas are concrete wire types; generics are rejected.
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "Event/Command derive does not support generic types; schemas are concrete wire types",
        ));
    }

    // At most ONE field may be the shard key.
    let has_shard_key = |f: &syn::Field| {
        let mut found = false;
        for attr in &f.attrs {
            if !attr.path().is_ident("schema") {
                continue;
            }
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("shard_key") {
                    found = true;
                }
                Ok(())
            });
        }
        found
    };
    let shard_keys: Vec<String> = fields
        .iter()
        .filter(|f| has_shard_key(f))
        .map(|f| f.ident.as_ref().expect("named fields").to_string())
        .collect();
    if shard_keys.len() > 1 {
        return Err(syn::Error::new(
            input.ident.span(),
            format!(
                "only one field may be #[schema(shard_key)]; found {}: {}",
                shard_keys.len(),
                shard_keys.join(", ")
            ),
        ));
    }

    // One pass per field: a `FieldDef` descriptor expression for every
    // field, plus a `field()` match arm for the shard key only. Field
    // types are never inspected for validity — every non-key field is
    // `FieldTy::Json`.
    let mut field_defs: Vec<TokenStream> = Vec::new();
    let mut field_reads: Vec<TokenStream> = Vec::new();
    for f in fields {
        let (field_def, is_shard_key) = field_def_expr(f)?;
        field_defs.push(field_def);
        if !is_shard_key {
            continue;
        }
        // The shard-key arm reads under the Rust ident (raw-ident `r#`
        // prefixes stripped) — the name the runtime looks up in the wire
        // JSON. A serde rename on the shard key breaks that lookup;
        // documented as unsupported for this one field.
        let field_name = f.ident.as_ref().expect("named fields").to_string();
        let desc_name = field_name.strip_prefix("r#").unwrap_or(&field_name);
        let desc_lit = syn::LitStr::new(desc_name, f.span());
        let rust_ident = quote::format_ident!("{}", desc_name);
        let read_expr = field_string_read(&f.ty);
        field_reads.push(quote! {
            #desc_lit => (#read_expr)(&self.#rust_ident),
        });
    }

    let kind_variant = quote::format_ident!("{}", kind.variant());
    let name = ident.to_string();
    let name_static = syn::LitStr::new(&name, ident.span());
    let description = match description {
        Some(text) => quote! { ::std::option::Option::Some(::std::string::String::from(#text)) },
        None => quote! { ::std::option::Option::None },
    };

    // The `field()` match arms: descriptor name → string form.
    let field_match_arms = &field_reads;

    Ok(quote! {
        impl ::trouper::schema::Schema for #ident {
            fn schema_name() -> ::std::option::Option<&'static ::std::primitive::str> {
                ::std::option::Option::Some(#name_static)
            }

            fn schema_def() -> ::trouper::schema::SchemaDef {
                // The descriptor is a per-type static: built once (first
                // read), every later read clones the shared `Arc` — the
                // schema id rides the static-name arm, so the hot path
                // never touches this either.
                static DEF: ::std::sync::LazyLock<::trouper::schema::SchemaDef> =
                    ::std::sync::LazyLock::new(|| ::trouper::schema::SchemaDef {
                        name: ::std::string::String::from(#name),
                        kind: ::trouper::schema::SchemaKind::#kind_variant,
                        fields: ::std::vec![#(#field_defs),*],
                        description: #description,
                    });
                DEF.clone()
            }
        }

        impl ::trouper::envelope::PayloadValue for #ident {
            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }

            fn field(&self, name: &str) -> ::std::option::Option<::std::string::String> {
                match name {
                    #(#field_match_arms)*
                    _ => ::std::option::Option::None,
                }
            }

            fn to_json_bytes(&self) -> ::std::sync::Arc<[::std::primitive::u8]> {
                ::trouper::envelope::payload_value_json_bytes(self)
            }
        }
    })
}

/// The string-read EXPRESSION GENERATOR for the shard-key field's type:
/// returns code that turns `&T` into `Option<String>`, mirroring the
/// descriptor type table. Non-stringable types read as `None` — the
/// shard-key read then reports the key as absent.
fn field_string_read(ty: &Type) -> TokenStream {
    if path_ends_with(ty, "PathBuf") {
        quote! { |v: &#ty| ::std::option::Option::Some(v.to_string_lossy().into_owned()) }
    } else if path_ends_with(ty, "Uuid") {
        quote! { |v: &#ty| ::std::option::Option::Some(v.to_string()) }
    } else if path_ends_with(ty, "String") {
        quote! { |v: &#ty| ::std::option::Option::Some(v.clone()) }
    } else if ty_is_stringish(ty) {
        // `&str` (and any other Deref-to-str stringish type): copy out.
        quote! { |v: &#ty| ::std::option::Option::Some(v.to_string()) }
    } else if ty_is_numeric(ty) || path_is(ty, "bool") {
        // Numbers and bools render their Display form (the canonical
        // stringification for a shard key).
        quote! { |v: &#ty| ::std::option::Option::Some(v.to_string()) }
    } else {
        // Json-typed fields (objects, byte blobs) have no canonical
        // string form — the shard-key read reports them as absent.
        quote! { |_: &#ty| ::std::option::Option::None }
    }
}

/// Whether the field type is string-valued (`String`, `&str`, `Uuid`,
/// `PathBuf`) — read as-is per the descriptor table.
fn ty_is_stringish(ty: &Type) -> bool {
    fn check(ty: &Type) -> Option<bool> {
        match ty {
            Type::Path(TypePath { qself: None, path }) => {
                let pivot = path.segments.last()?.ident.to_string();
                Some(matches!(pivot.as_str(), "String" | "Uuid" | "PathBuf"))
            }
            Type::Reference(ty_ref) => match &*ty_ref.elem {
                Type::Path(TypePath { qself: None, path }) if path.is_ident("str") => Some(true),
                other => check(other),
            },
            Type::Group(group) => check(&group.elem),
            Type::Paren(inner) => check(&inner.elem),
            _ => None,
        }
    }
    check(ty).unwrap_or(false)
}

/// Whether the field type is numeric per the descriptor table.
fn ty_is_numeric(ty: &Type) -> bool {
    fn check(ty: &Type) -> Option<bool> {
        match ty {
            Type::Path(TypePath { qself: None, path }) => {
                let pivot = path.segments.last()?.ident.to_string();
                Some(matches!(
                    pivot.as_str(),
                    "i8" | "i16"
                        | "i32"
                        | "i64"
                        | "u8"
                        | "u16"
                        | "u32"
                        | "u64"
                        | "usize"
                        | "isize"
                        | "f32"
                        | "f64"
                ))
            }
            Type::Reference(ty_ref) => check(&ty_ref.elem),
            Type::Group(group) => check(&group.elem),
            Type::Paren(inner) => check(&inner.elem),
            _ => None,
        }
    }
    check(ty).unwrap_or(false)
}

/// Builds the `FieldDef` constructor expression for one struct field,
/// applying its `#[schema(...)]` attributes, and reports whether the
/// field is the shard key.
///
/// Accepted keys (each at most once per field):
/// - `shard_key` (flag) → `FieldRole::ShardKey`
/// - `ty = "bool"|"int"|"float"|"str"|"uuid"|"json"` → force that
///   descriptor `FieldTy`
/// - `description = "..."` → `FieldDef.description`
///
/// No rename attribute: descriptor names are the Rust field idents;
/// renames are serde's concern alone.
fn field_def_expr(f: &syn::Field) -> syn::Result<(TokenStream, bool)> {
    let mut description: Option<String> = None;
    let mut shard_key = false;
    let mut ty_override: Option<&'static str> = None;

    for attr in &f.attrs {
        if !attr.path().is_ident("schema") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("shard_key") {
                if shard_key {
                    return Err(meta.error("duplicate `shard_key` in #[schema(...)]"));
                }
                shard_key = true;
                Ok(())
            } else if meta.path.is_ident("ty") {
                if ty_override.is_some() {
                    return Err(meta.error("duplicate `ty` in #[schema(...)]"));
                }
                let lit: syn::LitStr = meta.value()?.parse()?;
                match lit.value().as_str() {
                    "bool" => ty_override = Some("Bool"),
                    "int" => ty_override = Some("Int"),
                    "float" => ty_override = Some("Float"),
                    "str" => ty_override = Some("Str"),
                    "uuid" => ty_override = Some("Uuid"),
                    "json" => ty_override = Some("Json"),
                    _ => {
                        return Err(syn::Error::new(
                            lit.span(),
                            format!(
                                "unsupported #[schema(ty = {:?})]; supported: \"bool\", \"int\", \
                                 \"float\", \"str\", \"uuid\", \"json\"",
                                lit.value()
                            ),
                        ));
                    }
                }
                Ok(())
            } else if meta.path.is_ident("description") {
                if description.is_some() {
                    return Err(meta.error("duplicate `description` in #[schema(...)]"));
                }
                let lit: syn::LitStr = meta.value()?.parse()?;
                description = Some(lit.value());
                Ok(())
            } else {
                Err(meta.error(
                    "unknown #[schema(...)] field attribute; supported: shard_key, ty, description",
                ))
            }
        })?;
    }

    // Descriptor `FieldTy`: the `ty` override wins; the shard key maps its
    // real flat type (partition routing reads it); every other field is
    // `Json` — the descriptor is presentation data, not a compile contract.
    let variant = ty_override.unwrap_or(if shard_key {
        flat_ty_variant(&f.ty)
    } else {
        "Json"
    });
    let variant = quote::format_ident!("{}", variant);
    let ty_expr = quote! { ::trouper::schema::FieldTy::#variant };

    // Descriptor name: the Rust field name (raw-ident `r#` prefixes are
    // stripped — descriptors are wire names).
    let field_name = f.ident.as_ref().expect("named fields").to_string();
    let desc_name = field_name.strip_prefix("r#").unwrap_or(&field_name);

    // Builder chain: required → role → description.
    let role_expr = if shard_key {
        quote! { .as_shard_key() }
    } else {
        quote! {}
    };
    let desc_expr = match description {
        Some(text) => quote! { .with_description(#text) },
        None => quote! {},
    };

    Ok((
        quote! {
            ::trouper::schema::FieldDef::required(#desc_name, #ty_expr)
                #role_expr
                #desc_expr
        },
        shard_key,
    ))
}

/// The descriptor `FieldTy` VARIANT NAME for the shard-key field's Rust
/// type. The pivot is the LAST path segment (references, `Group`, and
/// `Paren` unwrap to their inner type): integers → `Int`, `f32/f64` →
/// `Float`, `bool` → `Bool`, `str/String` → `Str`, `Uuid` → `Uuid`,
/// anything else → `Json`. Total by design — no shard-key type is an
/// error — but only flat strings and numbers actually read back at
/// partition routing.
fn flat_ty_variant(ty: &Type) -> &'static str {
    match pivot_ident(ty).as_deref() {
        Some("i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" | "usize" | "isize") => {
            "Int"
        }
        Some("f32" | "f64") => "Float",
        Some("bool") => "Bool",
        Some("str" | "String") => "Str",
        Some("Uuid") => "Uuid",
        _ => "Json",
    }
}

/// The last path segment's ident, unwrapping references and invisible
/// delimiter groups (`&String` → `String`).
fn pivot_ident(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(TypePath { qself: None, path }) => {
            path.segments.last().map(|s| s.ident.to_string())
        }
        Type::Reference(ty_ref) => pivot_ident(&ty_ref.elem),
        Type::Group(group) => pivot_ident(&group.elem),
        Type::Paren(inner) => pivot_ident(&inner.elem),
        _ => None,
    }
}

/// True when the type is exactly the named primitive path (e.g. `u8`).
fn path_is(ty: &Type, name: &str) -> bool {
    matches!(ty, Type::Path(TypePath { qself: None, path }) if path.is_ident(name))
}

/// True when the type's LAST path segment is `name` (matches both `Uuid`
/// and `uuid::Uuid`, `PathBuf` and `std::path::PathBuf`).
fn path_ends_with(ty: &Type, name: &str) -> bool {
    match ty {
        Type::Path(TypePath { qself: None, path }) => path
            .segments
            .last()
            .map(|s| s.ident == name)
            .unwrap_or(false),
        Type::Group(group) => path_ends_with(&group.elem, name),
        Type::Paren(inner) => path_ends_with(&inner.elem, name),
        _ => false,
    }
}
