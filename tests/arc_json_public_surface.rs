//! Public-surface guarantees that outlive the Arc-payload change: `Json`
//! keeps its value semantics, `Envelope::json` keeps its signature, and
//! handlers still see `&Json`.

use trouper::{json, json::Json};

#[test]
fn json_is_clone_partialeq_default_with_deref() {
    // Given a Json value at the public boundary.
    let value: Json = json!({ "k": 41 });

    // Then it clones (independently), compares by value, defaults, and
    // reads through Deref like the underlying tree.
    let copy = value.clone();
    assert_eq!(copy, value);
    assert_eq!(Json::default(), Json::from(serde_json::Value::Null));
    assert_eq!(value["k"], 41);
}

#[test]
fn decode_matches_the_tree_without_consuming_it() {
    // Given a Json tree.
    let value: Json = json!({ "n": 9 });

    // When decoding it twice.
    #[derive(serde::Deserialize, PartialEq, Eq, Debug)]
    struct N {
        n: i64,
    }
    let a: N = value.decode().expect("decode a");
    let b: N = value.decode().expect("decode b");

    // Then both decodes succeed on the borrowed tree — the value is still
    // readable afterwards.
    assert_eq!(a, N { n: 9 });
    assert_eq!(b, a);
    assert_eq!(value["n"], 9);
}
