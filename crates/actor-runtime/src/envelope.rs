//! Runtime-internal envelopes and the message shapes that ride in them.
//!
//! Users never construct envelopes: the runtime assembles the metadata at the
//! send boundary. Handlers see typed payloads plus a context. Payloads cross
//! the runtime boundary as JSON; the typed arm exists only for in-process
//! zero-copy fast paths, erased exactly once at spawn.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::types::{CausalityId, LeaseId, Path, SchemaId, Topic, TraceId};

/// Where a message is headed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Address {
    /// A named actor's inbox.
    Path(Path),
    /// A topic's fan of subscriber inboxes.
    Topic(Topic),
    /// A reply slot for an in-flight `ask`; a mechanism, dies with the ask.
    Slot(LeaseId),
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
    /// In-process typed fast path; erased to JSON at the schema waist.
    Typed(std::sync::Arc<dyn std::any::Any + Send + Sync>),
    /// The waist representation: plain JSON.
    Json(JsonValue),
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
    pub from: Option<Path>,
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

    /// Assembles a typed envelope for the in-process fast path.
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
    pub fn from(mut self, from: Path) -> Self {
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
            Address::Path(Path::new("inventory.west")),
            Address::Topic(Topic::new("inventory.events")),
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
            Address::Path(Path::new("inventory.west")),
            json!({ "sku": "widget", "qty": 2 }),
            TraceCtx::root(),
        )
        .from(Path::new("storefront"))
        .reply_to(Address::Path(Path::new("storefront")));

        // When reading the payload back out.
        let payload = envelope.as_json().expect("json payload");

        // Then the metadata and payload survive intact.
        assert_eq!(envelope.schema.as_str(), "ReserveStock@1");
        assert_eq!(envelope.from.as_ref().map(|p| p.as_str()), Some("storefront"));
        assert_eq!(
            envelope.reply_to,
            Some(Address::Path(Path::new("storefront")))
        );
        assert_eq!(payload["sku"], "widget");
    }

    #[test]
    fn typed_envelope_holds_erased_payload() {
        // Given a typed envelope.
        let envelope =
            Envelope::typed(SchemaId::new("Tick", 1), Address::Path(Path::new("a")), 42u32, TraceCtx::root());

        // When inspecting the payload.
        let rendered = format!("{envelope:?}");

        // Then it is opaque — never leakable across the waist.
        assert!(rendered.contains("<erased>"));
        assert!(envelope.as_json().is_none());
    }
}

