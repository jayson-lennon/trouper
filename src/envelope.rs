//! Runtime-internal envelopes and the message shapes that ride in them.
//!
//! Users never construct envelopes: the runtime assembles the metadata at the
//! send boundary. Handlers see typed payloads plus a context. Payloads cross
//! the runtime boundary as JSON; the typed arm exists only for in-process
//! zero-copy fast paths, erased exactly once at spawn.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::actor::ActorPath;
use crate::clock::Timestamp;
use crate::reply::LeaseId;
use crate::schema::SchemaId;

/// Where a message is headed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Address {
    /// A named actor's inbox.
    Path(ActorPath),
    /// A reply slot for an in-flight `ask`; a mechanism, dies with the ask.
    Slot(LeaseId),
    /// Any handler of the schema: the kernel picks one per send
    /// (RoundRobin across registered handlers; Single while only one).
    Schema(SchemaId),
}

/// Trace metadata carried by every envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceCtx {
    /// Identifies the whole causal conversation.
    pub trace_id: TraceId,
    /// Identifies this hop's position within the conversation.
    pub causality_id: CausalityId,
}

impl TraceCtx {
    /// Starts a fresh conversation with a fresh causality root.
    pub fn root() -> Self {
        Self {
            trace_id: TraceId::new(),
            causality_id: CausalityId::new(),
        }
    }

    /// The metadata for a hop caused by this one: same trace, new causality.
    pub fn caused(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            causality_id: CausalityId::new(),
        }
    }
}

/// What an envelope carries.
///
/// Clonable so the kernel can peek a message without consuming it: the
/// envelope stays at the inbox cursor until acked, which is what makes
/// redelivery possible. The typed arm is an `Arc` clone (cheap).
#[derive(Clone)]
pub enum Payload {
    /// RESERVED in-proc fast path, not yet crossed by production code:
    /// every runtime boundary is JSON today (the "JSON waist" decision),
    /// so no adapter constructs this arm yet. Kept as the seam for a
    /// future zero-copy path; downstream code must still handle it (see
    /// [`Payload::into_json`] treating it as an error).
    Typed(std::sync::Arc<dyn std::any::Any + Send + Sync>),
    /// The waist representation: plain JSON.
    Json(JsonValue),
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Address::Path(path) => write!(f, "{path}"),
            Address::Slot(lease) => write!(f, "slot({lease})"),
            Address::Schema(schema) => write!(f, "schema({schema})"),
        }
    }
}

impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Typed(_) => f.write_str("Payload::Typed(<erased>)"),
            Self::Json(value) => f.debug_tuple("Payload::Json").field(value).finish(),
        }
    }
}

/// A domain event: schema-tagged JSON, appended to journals and folded into
/// state. Events are facts that already happened.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// The event's schema.
    pub schema: SchemaId,
    /// The event's JSON payload.
    pub payload: JsonValue,
}

impl Event {
    /// Creates an event from a schema id and JSON payload.
    pub fn new(schema: SchemaId, payload: JsonValue) -> Self {
        Self { schema, payload }
    }
}

/// The runtime-internal message wrapper. Not serializable as a whole: the
/// durable parts (trace, schema, JSON payload) are copied out at the waist.
#[derive(Debug, Clone)]
pub struct Envelope {
    /// The payload's schema.
    pub schema: SchemaId,
    /// Where the message is headed.
    pub dest: Address,
    /// The logical sending path, when the sender is an actor.
    pub from: Option<ActorPath>,
    /// Where a reply should go: a durable [`Address::Path`] or a mechanism
    /// [`Address::Slot`].
    pub reply_to: Option<Address>,
    /// Trace metadata of this hop.
    pub trace: TraceCtx,
    /// The message body.
    pub payload: Payload,
}

impl Envelope {
    /// Assembles a JSON envelope — the waist representation.
    pub fn json(schema: SchemaId, dest: Address, payload: JsonValue, trace: TraceCtx) -> Self {
        Self {
            schema,
            dest,
            from: None,
            reply_to: None,
            trace,
            payload: Payload::Json(payload),
        }
    }

    /// The JSON view of the payload (the waist representation).
    pub fn payload_json(&self) -> &JsonValue {
        match &self.payload {
            Payload::Json(value) => value,
            Payload::Typed(_) => &JsonValue::Null,
        }
    }

    /// Assembles a typed envelope for the RESERVED in-proc fast path.
    /// Production code crosses the schema waist as JSON; this constructor
    /// exists for that future path and for the erased-payload unit test.
    #[doc(hidden)]
    pub fn typed<T: Send + Sync + 'static>(
        schema: SchemaId,
        dest: Address,
        payload: T,
        trace: TraceCtx,
    ) -> Self {
        Self {
            schema,
            dest,
            from: None,
            reply_to: None,
            trace,
            payload: Payload::Typed(std::sync::Arc::new(payload)),
        }
    }

    /// Sets the logical sender.
    pub fn from(mut self, from: ActorPath) -> Self {
        self.from = Some(from);
        self
    }

    /// Sets the reply destination.
    pub fn reply_to(mut self, reply_to: Address) -> Self {
        self.reply_to = Some(reply_to);
        self
    }

    /// The payload as JSON, if it is already at the waist.
    pub fn as_json(&self) -> Option<&JsonValue> {
        match &self.payload {
            Payload::Json(value) => Some(value),
            Payload::Typed(_) => None,
        }
    }

    /// Takes the payload as JSON, encoding the typed arm at the schema waist.
    ///
    /// Typed payloads require the type to be JSON-encodable; this is the
    /// single encode point for the fast path.
    pub fn into_json(self) -> Result<JsonValue, Payload> {
        match self.payload {
            Payload::Json(value) => Ok(value),
            typed @ Payload::Typed(_) => Err(typed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn address_survives_serde_roundtrip_for_each_variant() {
        // Given one address of each variant.
        let addresses = [
            Address::Path(ActorPath::new("inventory.west")),
            Address::Slot(LeaseId::new()),
        ];

        for address in addresses {
            // When round-tripping through JSON.
            let json = serde_json::to_string(&address).expect("serialize");
            let round: Address = serde_json::from_str(&json).expect("deserialize");

            // Then the variant and value are preserved.
            assert_eq!(round, address);
        }
    }

    #[test]
    fn trace_ctx_caused_shares_trace_and_renews_causality() {
        // Given a trace context.
        let parent = TraceCtx::root();

        // When deriving a caused hop.
        let child = parent.caused();

        // Then the trace id is shared and the causality id is fresh.
        assert_eq!(child.trace_id, parent.trace_id);
        assert_ne!(child.causality_id, parent.causality_id);
    }

    #[test]
    fn trace_ctx_survives_serde_roundtrip() {
        // Given a trace context.
        let trace = TraceCtx::root();

        // When round-tripping through JSON.
        let round: TraceCtx =
            serde_json::from_str(&serde_json::to_string(&trace).expect("ser")).expect("de");

        // Then both ids are preserved.
        assert_eq!(round, trace);
    }

    #[test]
    fn json_envelope_roundtrips_payload_and_metadata() {
        // Given a JSON envelope with sender and reply-to set.
        let envelope = Envelope::json(
            SchemaId::new("ReserveStock", 1),
            Address::Path(ActorPath::new("inventory.west")),
            json!({ "sku": "widget", "qty": 2 }),
            TraceCtx::root(),
        )
        .from(ActorPath::new("storefront"))
        .reply_to(Address::Path(ActorPath::new("storefront")));

        // When reading the payload back out.
        let payload = envelope.as_json().expect("json payload");

        // Then the metadata and payload survive intact.
        assert_eq!(envelope.schema.as_str(), "ReserveStock@1");
        assert_eq!(
            envelope.from.as_ref().map(|p| p.as_str()),
            Some("storefront")
        );
        assert_eq!(
            envelope.reply_to,
            Some(Address::Path(ActorPath::new("storefront")))
        );
        assert_eq!(payload["sku"], "widget");
    }

    #[test]
    fn typed_envelope_holds_erased_payload() {
        // Given a typed envelope.
        let envelope = Envelope::typed(
            SchemaId::new("Tick", 1),
            Address::Path(ActorPath::new("a")),
            42u32,
            TraceCtx::root(),
        );

        // When inspecting the payload.
        let rendered = format!("{envelope:?}");

        // Then it is opaque — never leakable across the waist.
        assert!(rendered.contains("<erased>"));
        assert!(envelope.as_json().is_none());
    }
}

/// Identifies a whole causal conversation (a request and everything it caused).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TraceId(Uuid);

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
/// Identifies one hop's position within a [`TraceId`] conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CausalityId(Uuid);

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
    pub fn as_millis_ts(self) -> crate::clock::Timestamp {
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
