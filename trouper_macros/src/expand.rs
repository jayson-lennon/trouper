//! Derive expansion: parse the derive input and generate the
//! `trouper::schema::Schema` impl plus the `trouper::envelope::PayloadValue`
//! impl (downcast dispatch, field-by-name reads, memoized JSON-text
//! encoding) for the derived struct.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, GenericArgument, Path, PathArguments, Type, TypePath};

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
/// attribute (default: no description), kind from the
/// derive, fields mapped from the Rust types.
///
/// Also generates the `PayloadValue` impl: `as_any` (the downcast
/// dispatch core), `field` (string reads of declared fields by their
/// DESCRIPTOR name — the rename-aware one serde respects), and
/// `to_json_bytes` (compact JSON text, memoized in a `OnceLock` so the
/// value serializes at most once no matter how many readers ask — the
/// journal door's single-serialization contract).
///
/// `#[schema(...)]` FIELD attributes are rejected; the error says so
/// explicitly so early adopters aren't surprised.
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
                Err(meta.error(
                    "unknown #[schema(...)] container attribute; supported: description",
                ))
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

    // Map each Rust field type to its descriptor `FieldDef` expression,
    // honoring the field-level `#[schema(...)]` attributes:
    // `shard_key`, `rename = "..."`, `ty = "json"`, `description = "..."`.
    let mut field_defs: Vec<TokenStream> = Vec::new();
    let mut field_reads: Vec<TokenStream> = Vec::new();
    for f in fields {
        let name = f.ident.as_ref().expect("named fields");
        let field_name = name.to_string();
        field_defs.push(field_def_expr(f, &field_name)?);
        // The RUNTIME field name — the descriptor name the rename produces
        // (serde serializes under the serde rename; this derive's rename
        // attribute mirrors it). Struct field access goes through the
        // Rust ident.
        let rust_ident = quote::format_ident!(
            "{}",
            field_name.strip_prefix("r#").unwrap_or(&field_name)
        );
        let read_expr = field_string_read(&f.ty);
        let desc_name = descriptor_name(f, &field_name)?;
        let desc_lit = syn::LitStr::new(&desc_name, f.span());
        field_reads.push(quote! {
            #desc_lit => (#read_expr)(&self.#rust_ident),
        });
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

    let kind_variant = quote::format_ident!("{}", kind.variant());
    let name = ident.to_string();
    let description = match description {
        Some(text) => quote! { ::std::option::Option::Some(::std::string::String::from(#text)) },
        None => quote! { ::std::option::Option::None },
    };

    // The `field()` match arms: descriptor name → string form.
    let field_match_arms = &field_reads;

    Ok(quote! {
        impl ::trouper::schema::Schema for #ident {
            fn schema_def() -> ::trouper::schema::SchemaDef {
                ::trouper::schema::SchemaDef {
                    name: ::std::string::String::from(#name),
                    kind: ::trouper::schema::SchemaKind::#kind_variant,
                    fields: ::std::vec![#(#field_defs),*],
                    description: #description,
                }
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

/// The string-read EXPRESSION GENERATOR for one field type: returns code
/// that turns `&T` into `Option<String>`, mirroring the descriptor type
/// table. Unsupported (non-stringable) descriptor types read as `None`.
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
    } else if ty_is_numeric(ty) {
        quote! { |v: &#ty| ::std::option::Option::Some(v.to_string()) }
    } else if path_is(ty, "bool") {
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
                    "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" | "usize"
                        | "isize" | "f32" | "f64"
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

/// The field's DESCRIPTOR name (the rename, or the Rust name with raw
///-ident prefixes stripped) — the same name `field()` matches against.
/// Re-parses the attributes fully (same grammar as `field_def_expr`) so
/// no key is left unconsumed.
fn descriptor_name(f: &syn::Field, rust_name: &str) -> syn::Result<String> {
    let mut rename: Option<String> = None;
    for attr in &f.attrs {
        if !attr.path().is_ident("schema") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                if rename.is_some() {
                    return Err(meta.error("duplicate `rename` in #[schema(...)]"));
                }
                let lit: syn::LitStr = meta.value()?.parse()?;
                rename = Some(lit.value());
                Ok(())
            } else if meta.path.is_ident("shard_key") {
                // A flag (no value), consumed by `field_def_expr`.
                Ok(())
            } else if meta.path.is_ident("ty") || meta.path.is_ident("description") {
                // Consumed by `field_def_expr`; skip its value.
                let _ = meta.value()?.parse::<syn::Expr>();
                Ok(())
            } else {
                Err(meta.error(
                    "unknown #[schema(...)] field attribute; supported: shard_key, rename, ty, description",
                ))
            }
        })?;
    }
    Ok(rename.unwrap_or_else(|| {
        rust_name
            .strip_prefix("r#")
            .unwrap_or(rust_name)
            .to_string()
    }))
}

/// Builds the `FieldDef` constructor expression for one struct field,
/// applying its `#[schema(...)]` attributes.
///
/// Accepted keys (each at most once per field):
/// - `shard_key` (flag) → `FieldRole::ShardKey`
/// - `rename = "x"` → the DESCRIPTOR field name only (serde untouched)
/// - `ty = "json"` → force `FieldTy::Json` (escape hatch for exotic types)
/// - `description = "..."` → `FieldDef.description`
fn field_def_expr(f: &syn::Field, rust_name: &str) -> syn::Result<TokenStream> {
    let mut rename: Option<String> = None;
    let mut description: Option<String> = None;
    let mut shard_key = false;
    let mut force_json = false;

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
            } else if meta.path.is_ident("rename") {
                if rename.is_some() {
                    return Err(meta.error("duplicate `rename` in #[schema(...)]"));
                }
                let lit: syn::LitStr = meta.value()?.parse()?;
                rename = Some(lit.value());
                Ok(())
            } else if meta.path.is_ident("ty") {
                if force_json {
                    return Err(meta.error("duplicate `ty` in #[schema(...)]"));
                }
                let lit: syn::LitStr = meta.value()?.parse()?;
                if lit.value() != "json" {
                    return Err(syn::Error::new(
                        lit.span(),
                        format!(
                            "unsupported #[schema(ty = {:?})]; the only override is \"json\"",
                            lit.value()
                        ),
                    ));
                }
                force_json = true;
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
                    "unknown #[schema(...)] field attribute; supported: shard_key, rename, ty, description",
                ))
            }
        })?;
    }

    // Descriptor name: the rename, or the Rust field name (raw-ident `r#`
    // prefixes are stripped — descriptors are wire names).
    let desc_name = rename.unwrap_or_else(|| {
        rust_name
            .strip_prefix("r#")
            .unwrap_or(rust_name)
            .to_string()
    });

    // The field's `FieldTy`: forced override, else the type mapping.
    let ty_expr = if force_json {
        quote! { ::trouper::schema::FieldTy::Json }
    } else {
        map_field_ty(&f.ty, rust_name)?
    };

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

    Ok(quote! {
        ::trouper::schema::FieldDef::required(#desc_name, #ty_expr)
            #role_expr
            #desc_expr
    })
}

/// Maps one Rust field type to the `::trouper::schema::FieldTy` expression.
///
/// The pivot is the LAST path segment of the type:
/// - `i64/u64/i32/u32/i16/u16/i8/u8/isize/usize` → `Int`
/// - `f32/f64` → `Float`
/// - `bool` → `Bool`
/// - `String`/`&str` → `Str`
/// - any path ending in `Uuid` → `Uuid`
/// - `Json`/`Value` → `Json`; `PathBuf` → `Str` (serde string);
///   `Vec<u8>`/`[u8]` → `Json`
/// - unknown → spanned error naming the field and the supported set.
fn map_field_ty(ty: &Type, field_name: &str) -> syn::Result<TokenStream> {
    match ty {
        Type::Path(TypePath { qself: None, path }) => map_path_ty(path, ty, field_name),
        Type::Reference(ty_ref) => {
            // `&str` → Str; other references delegate to the referent.
            if let Type::Path(TypePath { qself: None, path }) = &*ty_ref.elem
                && path.is_ident("str")
            {
                return Ok(quote! { ::trouper::schema::FieldTy::Str });
            }
            map_field_ty(&ty_ref.elem, field_name)
        }
        Type::Slice(elem) => {
            // `[u8]` → Json (bytes ride as opaque JSON payloads); other
            // slices delegate to their element type.
            if path_is(&elem.elem, "u8") {
                return Ok(quote! { ::trouper::schema::FieldTy::Json });
            }
            map_field_ty(&elem.elem, field_name)
        }
        Type::Group(group) => {
            // Invisible-delimiter groups (macro-emitted types) delegate.
            map_field_ty(&group.elem, field_name)
        }
        Type::Paren(inner) => map_field_ty(&inner.elem, field_name),
        _ => Err(unsupported_ty_error(ty, field_name)),
    }
}

/// Maps a bare path type. `Vec<u8>` is special-cased to `Json` (the
/// `contents: Vec<u8>` byte-payload precedent) BEFORE the generic pivot
/// match; `Vec<T>` otherwise delegates to `T`.
fn map_path_ty(path: &Path, ty: &Type, field_name: &str) -> syn::Result<TokenStream> {
    // Vec<u8> → Json (raw bytes serialize as a JSON array of numbers).
    if let Some(segment) = path.segments.last()
        && segment.ident == "Vec"
        && let PathArguments::AngleBracketed(args) = &segment.arguments
        && let Some(GenericArgument::Type(inner)) = args.args.first()
    {
        if path_is(inner, "u8") {
            return Ok(quote! { ::trouper::schema::FieldTy::Json });
        }
        return map_field_ty(inner, field_name);
    }

    let pivot = path
        .segments
        .last()
        .map(|s| s.ident.to_string())
        .unwrap_or_default();
    let field_ty = match pivot.as_str() {
        "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" | "usize" | "isize" => "Int",
        "f32" | "f64" => "Float",
        "bool" => "Bool",
        "String" => "Str",
        "Uuid" => "Uuid",
        // PathBuf serializes as a JSON string (serde), so the truthful
        // descriptor is Str — matching the hand-written impls this
        // derive replaces.
        "PathBuf" => "Str",
        "Json" | "Value" => "Json",
        _ => return Err(unsupported_ty_error(ty, field_name)),
    };
    let variant = quote::format_ident!("{}", field_ty);
    Ok(quote! { ::trouper::schema::FieldTy::#variant })
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

/// The spanned "unsupported type" error: names the field, shows the type,
/// lists the supported set, and points at the `#[schema(ty)]` escape hatch.
fn unsupported_ty_error(ty: &Type, field_name: &str) -> syn::Error {
    let ty_render = ty.to_token_stream().to_string();
    syn::Error::new(
        ty.span(),
        format!(
            "unsupported schema field type `{ty_render}` for field `{field_name}`; \
             supported: integers, floats, bool, String, uuid::Uuid, Json \
             (or override with #[schema(ty = \"json\")])"
        ),
    )
}
