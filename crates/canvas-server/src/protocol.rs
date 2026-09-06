//! The wire protocol: versioned envelopes, one JSON object per
//! `\n`-terminated line (NDJSON).
//!
//! Both directions carry `v` (integer protocol version) and `kind` (a
//! string discriminator). v1 is a stateless request/response pair:
//!
//! ```text
//! {"v":1,"kind":"snapshot_request"}
//! {"v":1,"kind":"snapshot","export":{ ...full SystemExport JSON... }}
//! {"v":1,"code":"unknown_version|malformed|internal","detail":"..."}
//! ```
//!
//! A peer must tolerate a trailing partial line at EOF (discard it).
//! There is no negotiation: a request whose `v` is not [`PROTOCOL_VERSION`]
//! is answered with `unknown_version`, not a downgrade dance. The
//! envelope's `v` + `kind` reserve space so a v2 fact stream is additive,
//! not a protocol break. No line-length cap in v1 — a `SystemExport` with
//! many entities is one long line; use a generous read buffer.
//!
//! Serialization lives HERE and nowhere else (the same rule as
//! [`actor_runtime::tap::Fact::to_json`], the tap boundary).

use actor_runtime::system::SystemExport;
use serde::{Deserialize, Serialize};

/// The only version this build speaks. Anything else is an
/// [`ErrorCode::UnknownVersion`] reply.
pub const PROTOCOL_VERSION: u64 = 1;

/// The inbound request (what a client writes to the server).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRequest {
    /// Protocol version; strict — see [`PROTOCOL_VERSION`].
    pub v: u64,
    /// Discriminator; must be `"snapshot_request"`.
    pub kind: String,
}

/// A validated, version-matched request.
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    /// `{"v":1,"kind":"snapshot_request"}`.
    Snapshot,
}

impl Request {
    /// Parses one NDJSON line into a request.
    ///
    /// A version mismatch and a bad shape are distinct failures (the
    /// server maps them to different [`ErrorCode`]s), and a mismatch
    /// reports the received version in its payload.
    pub fn parse(line: &str) -> Result<Self, Reject> {
        let value: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| Reject::Malformed(format!("not valid JSON: {e}")))?;
        let request: SnapshotRequest = serde_json::from_value(value)
            .map_err(|e| Reject::Malformed(format!("not a snapshot_request envelope: {e}")))?;
        if request.v != PROTOCOL_VERSION {
            return Err(Reject::UnknownVersion(request.v));
        }
        if request.kind != "snapshot_request" {
            return Err(Reject::Malformed(format!(
                "unknown kind {:?} (expected \"snapshot_request\")",
                request.kind
            )));
        }
        Ok(Self::Snapshot)
    }
}

/// Why [`Request::parse`] refused a line.
#[derive(Debug, Clone, PartialEq)]
pub enum Reject {
    /// JSON that is not a request envelope (or a known kind).
    Malformed(String),
    /// A well-formed envelope speaking a different protocol version.
    UnknownVersion(u64),
}

impl Reject {
    /// The error code a reply carries for this rejection.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Malformed(_) => ErrorCode::Malformed,
            Self::UnknownVersion(_) => ErrorCode::UnknownVersion,
        }
    }

    /// Human-readable detail for the reply payload.
    pub fn detail(&self) -> String {
        match self {
            Self::Malformed(detail) => detail.clone(),
            Self::UnknownVersion(v) => {
                format!("server speaks protocol v{PROTOCOL_VERSION}, request carried v{v}")
            }
        }
    }
}

/// Machine-readable reason inside an `error` reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The request spoke a different protocol version.
    UnknownVersion,
    /// The request was not valid JSON, or not a known envelope shape.
    Malformed,
    /// The server failed to produce a reply (e.g. serialization).
    Internal,
}

/// The outbound snapshot reply. `export` rides as a JSON document —
/// the wire is untyped JSON, so a future renderer in another language
/// stays possible.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotReply {
    /// Protocol version; see [`PROTOCOL_VERSION`].
    pub v: u64,
    /// Discriminator; always `"snapshot"`.
    pub kind: &'static str,
    /// The full `SystemExport` JSON document.
    pub export: serde_json::Value,
}

impl SnapshotReply {
    /// Wraps an export in a versioned reply envelope.
    pub fn new(export: SystemExport) -> Result<Self, serde_json::Error> {
        let export = serde_json::to_value(&export)?;
        Ok(Self {
            v: PROTOCOL_VERSION,
            kind: "snapshot",
            export,
        })
    }
}

/// The outbound error reply.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorReply {
    /// Protocol version; see [`PROTOCOL_VERSION`].
    pub v: u64,
    /// Discriminator; always `"error"`.
    pub kind: &'static str,
    /// Machine-readable reason.
    pub code: ErrorCode,
    /// Human-readable detail.
    pub detail: String,
}

impl ErrorReply {
    /// Builds a versioned error reply.
    pub fn new(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            kind: "error",
            code,
            detail: detail.into(),
        }
    }

    /// Wraps an internal failure (a reply could not be produced).
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, detail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actor_runtime::system::SystemExport;

    /// An export with every section present but empty — enough to prove
    /// envelope wrapping without a live system.
    fn empty_export() -> SystemExport {
        SystemExport {
            schemas: Vec::new(),
            actors: Vec::new(),
            declared_edges: Vec::new(),
            observed_edges: Vec::new(),
            pools: Vec::new(),
            partitions: Vec::new(),
            rules: Vec::new(),
        }
    }

    #[rstest::rstest]
    #[case(r#"{"v":1,"kind":"snapshot_request"}"#)]
    fn canonical_request_parses(#[case] line: &str) {
        // Given a canonical v1 request line.
        // When parsing.
        // Then it is accepted as a Snapshot request.
        assert_eq!(Request::parse(line), Ok(Request::Snapshot));
    }

    #[test]
    fn wrong_version_rejects_with_the_received_version() {
        // Given a v2 request line.
        // When parsing.
        // Then the rejection is UnknownVersion(2).
        let line = r#"{"v":2,"kind":"snapshot_request"}"#;
        assert_eq!(Request::parse(line), Err(Reject::UnknownVersion(2)));
        // And its detail names both versions.
        assert!(Request::parse(line)
            .unwrap_err()
            .detail()
            .contains("v1"));
    }

    #[test]
    fn wrong_version_field_shape_rejects_as_malformed() {
        // Given an envelope whose `v` is a string, not an integer.
        // When parsing.
        // Then the rejection is Malformed (the shape is wrong before the
        // version is even comparable).
        let line = r#"{"v":"1","kind":"snapshot_request"}"#;
        assert!(matches!(
            Request::parse(line),
            Err(Reject::Malformed(_))
        ));
    }

    #[rstest::rstest]
    #[case("hello")]
    #[case("[]")]
    #[case(r#"{"v":1}"#)]
    #[case(r#"{"v":1,"kind":"facts_subscribe"}"#)]
    #[case("")]
    fn garbage_rejects_as_malformed(#[case] line: &str) {
        // Given a line that is not JSON, or JSON that is not a known
        // request envelope.
        // When parsing.
        // Then the rejection is Malformed.
        assert!(matches!(Request::parse(line), Err(Reject::Malformed(_))));
    }

    #[test]
    fn snapshot_reply_carries_version_kind_and_export() {
        // Given an empty system export.
        let export = empty_export();

        // When wrapping it in a reply.
        let reply = SnapshotReply::new(export).expect("serializable");

        // Then the envelope is versioned and kinded, and the export rode
        // through as a JSON object.
        assert_eq!(reply.v, PROTOCOL_VERSION);
        assert_eq!(reply.kind, "snapshot");
        assert!(reply.export.is_object());
    }

    #[test]
    fn error_reply_serializes_with_snake_case_code() {
        // Given an error reply.
        // When serializing.
        let line = serde_json::to_string(&ErrorReply::new(
            ErrorCode::UnknownVersion,
            "server speaks protocol v1, request carried v9",
        ))
        .expect("serializable");

        // Then the code is the snake_case wire spelling.
        assert!(line.contains(r#""code":"unknown_version""#));
    }
}
