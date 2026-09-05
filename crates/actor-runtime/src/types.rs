//! Identity and scalar newtypes shared across the runtime.
//!
//! Every primitive that carries domain meaning is wrapped: a [`Path`] is
//! never passed where a [`Topic`] is expected, a [`SeqNo`] is never compared
//! to an [`InboxOffset`], and IDs are UUIDs — never strings.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A registered actor identity, e.g. `inventory.west`.
///
/// Actor identity IS its path: handles survive restarts because the registry
/// maps the path to a swappable endpoint slot.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Path(Arc<str>);

/// A pub/sub topic name, e.g. `inventory.events`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Topic(Arc<str>);

/// A schema identifier of the form `name@version`, e.g. `StockReserved@1`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaId(Arc<str>);

/// Identifies a whole causal conversation (a request and everything it caused).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TraceId(Uuid);

/// Identifies one hop's position within a [`TraceId`] conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CausalityId(Uuid);

/// Sequence number of a journal entry within one actor's journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SeqNo(u64);

/// Position within one actor's inbox; independent of the journal's [`SeqNo`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InboxOffset(u64);

/// Lease for a reply slot (an `ask` in flight); a runtime-internal mechanism,
/// never persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseId(Uuid);

/// Milliseconds since the Unix epoch, always sourced from the injected clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(u64);

/// Which of the two actor contracts an actor implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ActorKind {
    /// Pure, journaled, replayable — implements [`crate::actor::EventSourced`].
    EventSourced,
    /// Impure by design: async handlers, I/O and `ask` allowed.
    Service,
}

/// Why an actor's endpoint ceased to exist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StopReason {
    /// Finished on its own or was stopped gracefully via the system.
    Normal,
    /// A handler panicked and the supervisor declined to restart it.
    Crashed,
    /// The restart budget was exhausted; escalated to the parent.
    Escalated,
}

/// Why an envelope was dead-lettered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeadLetterReason {
    /// No slot or route resolved for the destination.
    Unresolvable,
    /// No handler is registered for the payload's schema.
    UnknownSchema,
    /// The payload did not decode against its registered schema.
    Decode,
    /// The destination inbox refused the envelope (overload/closed).
    InboxRefused,
}

impl Path {
    /// Creates a path from a string.
    pub fn new(s: impl Into<Arc<str>>) -> Self {
        Self(s.into())
    }

    /// The path as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Topic {
    /// Creates a topic from a string.
    pub fn new(s: impl Into<Arc<str>>) -> Self {
        Self(s.into())
    }

    /// The topic as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl SchemaId {
    /// Builds `name@version` from its parts.
    pub fn new(name: &str, version: u32) -> Self {
        Self(format!("{name}@{version}").into())
    }

    /// Parses an existing `name@version` string.
    ///
    /// Returns `None` when the string has no `@` separator.
    pub fn parse(s: &str) -> Option<Self> {
        s.split_once('@').map(|_| Self(s.into()))
    }

    /// The schema name (everything before `@`).
    pub fn name(&self) -> &str {
        self.0.split_once('@').map_or(&self.0, |(name, _)| name)
    }

    /// The schema version (everything after `@`), or `None` if unversioned.
    pub fn version(&self) -> Option<u32> {
        let (_, version) = self.0.split_once('@')?;
        version.parse().ok()
    }

    /// The identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TraceId {
    /// Generates a fresh, time-ordered (v7) trace id.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl std::fmt::Display for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

impl CausalityId {
    /// Generates a fresh, time-ordered (v7) causality id.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl std::fmt::Display for CausalityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl CausalityId {
    /// The millisecond timestamp embedded in the v7 uuid (fact ts fallback).
    pub fn as_millis_ts(self) -> crate::types::Timestamp {
        let ms = self
            .0
            .get_timestamp()
            .map(|t| t.to_unix().0 * 1_000 + u64::from(t.to_unix().1) / 1_000_000)
            .unwrap_or(0);
        Timestamp::from_millis(ms)
    }
}

impl Default for CausalityId {
    fn default() -> Self {
        Self::new()
    }
}

impl LeaseId {
    /// Generates a fresh lease id.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for LeaseId {
    fn default() -> Self {
        Self::new()
    }
}

impl SeqNo {
    /// The sequence of the first journal entry.
    pub fn genesis() -> Self {
        Self(0)
    }

    /// The sequence "before genesis": replaying `after(this)` yields every
    /// event, including the first.
    pub fn before_genesis() -> Self {
        Self(u64::MAX)
    }

    /// Wraps a raw sequence value.
    pub fn new(v: u64) -> Self {
        Self(v)
    }

    /// The raw sequence value.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Whether this is the [`before_genesis`] sentinel (compare by value,
    /// since `u64::MAX` is unreachable by honest counting).
    pub fn is_before_genesis(self) -> bool {
        self.0 == u64::MAX
    }
}

impl InboxOffset {
    /// The offset of the next never-delivered inbox entry.
    pub fn zero() -> Self {
        Self(0)
    }

    /// Wraps a raw offset value.
    pub fn new(v: u64) -> Self {
        Self(v)
    }

    /// The raw offset value.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl Timestamp {
    /// Wraps raw epoch milliseconds (from the injected clock).
    pub fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// Raw epoch milliseconds.
    pub fn as_millis(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for SeqNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for InboxOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for LeaseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_survives_serde_roundtrip() {
        // Given an actor path.
        let path = Path::new("inventory.west");

        // When round-tripping through JSON.
        let json = serde_json::to_string(&path).expect("serialize");
        let round: Path = serde_json::from_str(&json).expect("deserialize");

        // Then the value is preserved as a bare string.
        assert_eq!(json, "\"inventory.west\"");
        assert_eq!(round, path);
    }

    #[test]
    fn path_displays_as_bare_name() {
        // Given an actor path.
        let path = Path::new("inventory.west");

        // When displaying it.
        let rendered = path.to_string();

        // Then only the name is shown.
        assert_eq!(rendered, "inventory.west");
    }

    #[test]
    fn topic_survives_serde_roundtrip() {
        // Given a topic.
        let topic = Topic::new("inventory.events");

        // When round-tripping through JSON.
        let json = serde_json::to_string(&topic).expect("serialize");
        let round: Topic = serde_json::from_str(&json).expect("deserialize");

        // Then the value is preserved.
        assert_eq!(round, topic);
    }

    #[test]
    fn schema_id_renders_name_and_version() {
        // Given a name and version.
        let id = SchemaId::new("StockReserved", 1);

        // When displaying the id.
        let rendered = id.to_string();

        // Then it reads `name@version`.
        assert_eq!(rendered, "StockReserved@1");
    }

    #[test]
    fn schema_id_parses_name_and_version_back_out() {
        // Given a rendered schema id string.
        let raw = "StockReserved@3";

        // When parsing it.
        let id = SchemaId::parse(raw).expect("parses");

        // Then name and version round-trip.
        assert_eq!(id.name(), "StockReserved");
        assert_eq!(id.version(), Some(3));
    }

    #[test]
    fn schema_id_parse_rejects_unversioned_string() {
        // Given a string without an `@` separator.
        let raw = "StockReserved";

        // When parsing it.
        let parsed = SchemaId::parse(raw);

        // Then parsing fails.
        assert!(parsed.is_none());
    }

    #[test]
    fn schema_id_survives_serde_roundtrip() {
        // Given a schema id.
        let id = SchemaId::new("StockReserved", 1);

        // When round-tripping through JSON.
        let json = serde_json::to_string(&id).expect("serialize");
        let round: SchemaId = serde_json::from_str(&json).expect("deserialize");

        // Then the value is preserved.
        assert_eq!(round, id);
    }

    #[test]
    fn trace_id_is_version_7() {
        // Given a freshly generated trace id.
        let id = TraceId::new();

        // When asking for its UUID version.
        let version = id.0.get_version_num();

        // Then it is v7 (time-ordered, for the canvas).
        assert_eq!(version, 7);
    }

    #[test]
    fn trace_id_survives_serde_roundtrip() {
        // Given a trace id.
        let id = TraceId::new();

        // When round-tripping through JSON.
        let json = serde_json::to_string(&id).expect("serialize");
        let round: TraceId = serde_json::from_str(&json).expect("deserialize");

        // Then the value is preserved.
        assert_eq!(round, id);
    }

    #[test]
    fn causality_id_is_version_7() {
        // Given a freshly generated causality id.
        let id = CausalityId::new();

        // When asking for its UUID version.
        let version = id.0.get_version_num();

        // Then it is v7.
        assert_eq!(version, 7);
    }

    #[test]
    fn seqno_orders_numerically_and_roundtrips() {
        // Given two sequence numbers.
        let earlier = SeqNo::genesis();
        let later = SeqNo::new(7);

        // When comparing and round-tripping through JSON.
        let ordered = earlier < later;
        let round: SeqNo =
            serde_json::from_str(&serde_json::to_string(&later).expect("ser")).expect("de");

        // Then ordering follows the numeric value and the value survives.
        assert!(ordered);
        assert_eq!(round, later);
    }

    #[test]
    fn inbox_offset_orders_numerically() {
        // Given two inbox offsets.
        let earlier = InboxOffset::zero();
        let later = InboxOffset::new(3);

        // When comparing them.
        let ordered = earlier < later;

        // Then ordering follows the numeric value.
        assert!(ordered);
    }

    #[test]
    fn timestamp_roundtrips_millis() {
        // Given raw epoch milliseconds.
        let ts = Timestamp::from_millis(1_756_000_000_000);

        // When reading them back.
        let millis = ts.as_millis();

        // Then the value is preserved.
        assert_eq!(millis, 1_756_000_000_000);
    }

    #[test]
    fn stop_reason_survives_serde_roundtrip() {
        // Given a stop reason.
        let reason = StopReason::Escalated;

        // When round-tripping through JSON.
        let json = serde_json::to_string(&reason).expect("serialize");
        let round: StopReason = serde_json::from_str(&json).expect("deserialize");

        // Then the variant is preserved.
        assert_eq!(round, reason);
    }
}
