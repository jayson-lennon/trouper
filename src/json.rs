//! [`Json`] — the runtime's public value type: a newtype over the
//! underlying JSON tree.
//!
//! `serde_json` types never appear in the runtime's public signatures: spawn
//! args, event payloads, snapshot blobs, and export documents all cross the
//! API as [`Json`]. The wrapper forwards every read (`Deref` to the tree:
//! indexing, `as_i64`, ...) and serde round-trips transparently, so journals
//! and snapshots written by earlier versions load unchanged. Anything the
//! wrapper does not cover is one [`Json::into_inner`] away.
//!
//! The representation is stable: shard-key extraction reads
//! `payload[key_field]` on the live value, and journals, foreign actors,
//! export, and args-merge all depend on key-value semantics.

use serde::{Deserialize, Serialize};

/// A JSON value at the runtime's public boundary.
///
/// Reads go through `Deref` (so `args["key"]`, `value.as_i64()`, and every
/// other `serde_json::Value` accessor work as-is); construction comes from
/// literals via the [`json!`](crate::json!) macro, from any [`Serialize`]
/// value via [`Json::of`], or from a raw tree via `From`.
#[derive(Debug, PartialEq, Default)]
pub struct Json(pub(crate) serde_json::Value);

impl Clone for Json {
    fn clone(&self) -> Self {
        // TEST PROBE: the test-build clone counter exists to make deep-tree
        // copies observable to the mechanism tests (zero release impact —
        // this arm compiles to the plain clone below).
        #[cfg(test)]
        crate::kernel::bump_deep_clones();
        Self(self.0.clone())
    }
}

impl Json {
    /// Builds a `Json` from any serializable value.
    ///
    /// # Panics
    ///
    /// Panics (named) if serialization fails — for the runtime's value
    /// domain it cannot; a failure here is a programming bug. Prefer the
    /// [`json!`](crate::json!) macro or `From` conversions at literal
    /// sites.
    pub fn of<T: Serialize>(value: &T) -> Self {
        match serde_json::to_value(value) {
            Ok(v) => Self(v),
            Err(e) => panic!("Json::of: failed to serialize value: {e}"),
        }
    }

    /// Decodes this value into the typed `T`.
    ///
    /// # Errors
    ///
    /// Returns the underlying `serde_json` error when the tree does not
    /// match `T`'s shape.
    pub fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        // Borrows: `&Value` IS a serde `Deserializer`, so the decode reads
        // the tree in place — it never copies it (only `T`'s own fields
        // allocate).
        T::deserialize(&self.0)
    }

    /// Unwraps the raw JSON tree — the escape hatch for code that must own
    /// the `serde_json` value itself.
    pub fn into_inner(self) -> serde_json::Value {
        self.0
    }
}

impl std::ops::Deref for Json {
    type Target = serde_json::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Json {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<serde_json::Value> for Json {
    fn as_ref(&self) -> &serde_json::Value {
        &self.0
    }
}

impl From<serde_json::Value> for Json {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}

impl From<&str> for Json {
    fn from(value: &str) -> Self {
        Self(serde_json::Value::String(value.to_owned()))
    }
}

impl From<String> for Json {
    fn from(value: String) -> Self {
        Self(serde_json::Value::String(value))
    }
}

impl From<i64> for Json {
    fn from(value: i64) -> Self {
        Self(serde_json::Value::from(value))
    }
}

impl From<u64> for Json {
    fn from(value: u64) -> Self {
        Self(serde_json::Value::from(value))
    }
}

impl From<f64> for Json {
    fn from(value: f64) -> Self {
        Self(serde_json::Value::from(value))
    }
}

impl From<bool> for Json {
    fn from(value: bool) -> Self {
        Self(serde_json::Value::Bool(value))
    }
}

impl From<serde_json::Map<String, serde_json::Value>> for Json {
    fn from(value: serde_json::Map<String, serde_json::Value>) -> Self {
        Self(serde_json::Value::Object(value))
    }
}

impl PartialEq<serde_json::Value> for Json {
    fn eq(&self, other: &serde_json::Value) -> bool {
        self.0 == *other
    }
}

impl PartialEq<&serde_json::Value> for Json {
    fn eq(&self, other: &&serde_json::Value) -> bool {
        &self.0 == *other
    }
}

impl PartialEq<Json> for serde_json::Value {
    fn eq(&self, other: &Json) -> bool {
        *self == other.0
    }
}

impl PartialEq<Json> for &serde_json::Value {
    fn eq(&self, other: &Json) -> bool {
        **self == other.0
    }
}

impl Serialize for Json {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        serde_json::Value::deserialize(deserializer).map(Self)
    }
}

/// Build a [`Json`] value with `json!`-literal syntax.
///
/// Re-exported from `serde_json` so user code never names that crate: the
/// macro's output (`serde_json::Value`) converts into [`Json`] via `From`.
#[macro_export]
macro_rules! json {
    ($($t:tt)*) => {
        $crate::json_inner!(serde_json::json!($($t)*))
    };
}

/// Internal bridge: converts the `serde_json` literal into [`Json`].
#[doc(hidden)]
#[macro_export]
macro_rules! json_inner {
    ($v:expr) => {
        $crate::Json::from($v)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a JSON object with `n` string keys (a tree big enough that a
    /// deep clone is unmissable to the probe).
    fn big_tree(n: usize) -> Json {
        let map: serde_json::Map<String, serde_json::Value> = (0..n)
            .map(|i| (format!("k{i}"), serde_json::Value::from(i as u64)))
            .collect();
        Json::from(serde_json::Value::Object(map))
    }

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
    struct Wide {
        k0: u64,
    }

    #[test]
    fn decode_borrows_not_clones() {
        // Given a wide JSON tree (10k keys — a deep-clone would copy every
        // node) and a probe baseline.
        let tree = big_tree(10_000);

        // When decoding it into the typed shape.
        let (decoded, clones) =
            crate::kernel::deep_clones_in_window(|| -> Result<Wide, _> { tree.decode() });

        // Then the decode is CORRECT...
        assert_eq!(decoded.expect("decode").k0, 0);
        // ...and copied ZERO payload trees: decode borrows (the RED run
        // fails here — today it clones the whole tree through
        // `from_value`).
        assert!(
            clones == 0,
            "decode must not clone the payload tree (cloned {clones} trees)"
        );
    }

    #[test]
    fn json_clone_is_a_deep_tree_copy_today_probe_counts_it() {
        // Given the probe counts deep tree copies (RED evidence for the
        // mechanism: the counter sees real clones, so the fan-out test's
        // zero-clone assertion is meaningful).
        let tree = big_tree(1_000);

        // When cloning a Json (an explicit, deliberate copy).
        let (_copy, clones) = crate::kernel::deep_clones_in_window(|| tree.clone());

        // Then exactly one deep copy was counted.
        assert_eq!(clones, 1, "Json::clone is one deep tree copy today");
    }

    #[test]
    fn json_reexport_covers_json_macro_and_into_inner() {
        // Given the crate's json! macro building a value at the boundary.
        let value = crate::json!({ "n": 3, "why": "because" });

        // When reading it back through the wrapper.
        // Then the literal reads like a serde_json value (Deref) and the
        // escape hatch hands over an interoperable raw tree.
        assert_eq!(value["n"], 3);
        assert_eq!(value["why"].as_str(), Some("because"));
        let raw: serde_json::Value = value.into_inner();
        assert_eq!(raw, serde_json::json!({ "n": 3, "why": "because" }));
    }

    #[test]
    fn json_of_and_decode_roundtrip_a_typed_value() {
        // Given a typed value converted at the boundary.
        let args = Json::of(&Genesis {
            key: "a".into(),
            on_hand: 4,
        });

        // When decoding it back into the type.
        let decoded: Genesis = args.decode().expect("decode");

        // Then the round trip preserves it.
        assert_eq!(decoded.key, "a");
        assert_eq!(decoded.on_hand, 4);
    }

    #[test]
    fn json_survives_serde_roundtrip_transparently() {
        // Given a Json value.
        let value = crate::json!({ "seq": 12, "name": "j" });

        // When round-tripping through serde (the journal/snapshot path).
        let text = serde_json::to_string(&value).expect("serialize");
        let round: Json = serde_json::from_str(&text).expect("deserialize");

        // Then it is byte-equivalent (no wrapper-shaped surprises on disk).
        assert_eq!(round, value);
    }

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
    struct Genesis {
        key: String,
        on_hand: i64,
    }

    /// Compile-level proof that the actor surface needs no `serde_json`
    /// naming: args, folds, and decisions all run on trouper types.
    mod public_api_hides_serde_json {
        use crate::actor::EventSourcedActor;
        use crate::prelude::*;

        #[derive(Event, serde::Serialize, serde::Deserialize, Clone)]
        struct Pong {
            n: i64,
        }

        #[derive(serde::Serialize, serde::Deserialize, Default)]
        struct Pinger {
            seen: i64,
        }
        impl EventSourcedActor for Pinger {
            fn manifest() -> ActorManifest {
                ActorManifest::new()
            }
            fn restore(args: &Json) -> Self {
                Self {
                    seen: args["start"].as_i64().unwrap_or(0),
                }
            }
            fn apply(&mut self, event: &Event) {
                if let Some(p) = event.as_fact::<Pong>() {
                    self.seen = p.n;
                }
            }
        }

        #[test]
        fn restores_folds_and_decides_without_naming_serde_json() {
            // Given spawn args built by the crate's json! macro.
            let args: Json = crate::json!({ "start": 9 });
            let mut actor = Pinger::restore(&args);

            // When folding a typed fact and building a typed decision.
            let event = Pong { n: 12 }.into_event();
            actor.apply(&event);
            let decision = Events::one(Pong { n: 4 });

            // Then everything round-trips on the public API alone.
            assert_eq!(actor.seen, 12);
            assert_eq!(decision.len(), 1);
            assert_eq!(decision[0].payload_json()["n"], 4);
        }
    }
}
