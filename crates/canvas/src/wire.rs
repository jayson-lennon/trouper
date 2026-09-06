//! The client's view of the wire: the request line it writes, and how
//! it classifies the reply envelopes it accepts. Versioned NDJSON,
//! strict v1 — the server side of this contract lives in
//! `canvas_server::protocol`; serialization for outbound messages lives
//! HERE and nowhere else on the client.
//!
//! A client must not panic on any server reply, including error kinds
//! or versions it does not know — everything unrecognized maps to a
//! [`ReplyKind`] and surfaces as [`CanvasError::Protocol`].

use serde_json::Value;

/// The protocol version this client speaks. A reply with a different
/// version is `unknown_version`, never a downgrade dance.
pub const PROTOCOL_VERSION: u64 = 1;

/// The outbound snapshot request, as one NDJSON line (without the
/// newline).
pub const REQUEST_LINE: &str = r#"{"v":1,"kind":"snapshot_request"}"#;

/// What a reply envelope turned out to be.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplyKind {
    /// `{"v":1,"kind":"snapshot","export":{...}}`.
    Snapshot,
    /// `{"v":1,"kind":"error","code":...,"detail":...}` (any code — the
    /// code string rides through to the caller).
    Error,
    /// A known shape speaking a different protocol version.
    Unknown(u64),
}

impl ReplyKind {
    /// The `code` string used when a reply is not even valid JSON.
    pub const UNPARSEABLE: &'static str = "unparseable";
    /// The `code` string used when the reply version is not v1.
    pub const UNKNOWN_VERSION: &'static str = "unknown_version";
}

/// Classifies one parsed reply envelope.
///
/// Structural errors — not an object, missing/absent `kind` — degrade
/// to [`ReplyKind::Unknown`] with the received `v` (or 0) so the caller
/// can report what it actually got without panicking.
pub fn classify(envelope: &Value) -> ReplyKind {
    let version = envelope.get("v").and_then(Value::as_u64).unwrap_or(0);
    match envelope.get("kind").and_then(Value::as_str) {
        Some("snapshot") if version == PROTOCOL_VERSION => ReplyKind::Snapshot,
        Some("error") if version == PROTOCOL_VERSION => ReplyKind::Error,
        _ => ReplyKind::Unknown(version),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_line_is_the_versioned_snapshot_request() {
        // Given the outbound request line.
        // When parsing it back.
        let parsed: Value = serde_json::from_str(REQUEST_LINE).expect("valid JSON");
        // Then it is a v1 snapshot_request.
        assert_eq!(parsed["v"], 1);
        assert_eq!(parsed["kind"], "snapshot_request");
    }

    #[test]
    fn snapshot_envelope_classifies_as_snapshot() {
        // Given a v1 snapshot envelope.
        let envelope = json!({"v": 1, "kind": "snapshot", "export": {}});
        // When classifying.
        // Then it is a Snapshot.
        assert_eq!(classify(&envelope), ReplyKind::Snapshot);
    }

    #[test]
    fn error_envelope_of_any_code_classifies_as_error() {
        // Given v1 error envelopes with known and unknown codes.
        let known = json!({"v": 1, "kind": "error", "code": "malformed", "detail": "x"});
        let unknown_code = json!({"v": 1, "kind": "error", "code": "shrug", "detail": "y"});
        // When classifying.
        // Then both are Errors (codes ride through, nothing panics).
        assert_eq!(classify(&known), ReplyKind::Error);
        assert_eq!(classify(&unknown_code), ReplyKind::Error);
    }

    #[test]
    fn future_version_reply_classifies_as_unknown_with_its_version() {
        // Given a v2 snapshot envelope.
        let envelope = json!({"v": 2, "kind": "snapshot", "export": {}});
        // When classifying.
        // Then it is Unknown(2) — reportable, never a panic.
        assert_eq!(classify(&envelope), ReplyKind::Unknown(2));
    }

    #[test]
    fn garbage_envelope_classifies_as_unknown_without_panicking() {
        // Given envelopes that are objects but not recognized shapes.
        let no_kind = json!({"v": 1});
        let not_object = json!(["array"]);
        // When classifying.
        // Then both degrade to Unknown with the received version (0 when
        // absent).
        assert_eq!(classify(&no_kind), ReplyKind::Unknown(1));
        assert_eq!(classify(&not_object), ReplyKind::Unknown(0));
    }
}
