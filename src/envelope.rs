//! Runtime-internal envelopes and the message shapes that ride in them.
//!
//! Users never construct envelopes: the runtime assembles the metadata at the
//! send boundary. Handlers see typed payloads plus a context.
//!
//! The fabric invariant: a payload is a LIVE TYPED VALUE behind a shared,
//! memoizing cell (`Arc<dyn PayloadValue>`). Handlers downcast; nothing on
//! the message path walks a JSON tree. Serde exists at exactly two doors —
//! erased ingress ([`PayloadBytes`] from outside the system) and the journal
//! (the payload's memoized compact encoding) — and every payload value
//! serializes at most once, however many readers ask.

use std::ops::Deref;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use uuid::Uuid;

use crate::actor::ActorPath;
use crate::clock::Timestamp;
use crate::json::Json;
use crate::reply::LeaseId;
use crate::schema::{Schema, SchemaId};

/// Where a message is headed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Address {
    /// A named actor's inbox. Survives restarts.
    Path(ActorPath),
    /// A reply slot for an in-flight `ask`; lives only as long as the
    /// asker awaits the reply.
    Slot(LeaseId),
    /// Any handler of the schema: the runtime picks one per send
    /// (round-robin across registered handlers; single while only one).
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

/// What an envelope carries: one erased, shared payload.
///
/// Clonable by refcount — a clone of an envelope bumps the payload's
/// `Arc`, never a copy of the value. The many simultaneous owners (the
/// channel copy and the inbox cursor; the fan-out copies; the journal
/// entry and the outbox intent) all read ONE value.
///
/// The fabric holds live typed values (downcast via [`AnyPayload`]) OR
/// wire bytes that arrived erased (decoded on demand); serde exists only
/// at the doors — the journal and erased ingress. There is no `Bytes`
/// fabric arm: a message that is not in memory is not a message.
#[derive(Clone)]
pub struct Payload(Arc<PayloadCell>);

/// The payload and its memoized JSON view. The view is interior-mutable
/// through the shared `Arc`: the FIRST reader of a `Bytes` payload pays
/// the decode (a debug-render path, never the typed hot path), every
/// later reader — including readers on other threads — shares it.
struct PayloadCell {
    inner: AnyPayload,
    json_view: std::sync::OnceLock<Json>,
    /// The memoized wire encoding (the journal door's input). Live
    /// values serialize at most once no matter how many readers ask;
    /// `Bytes` payloads pass their bytes through and never populate it.
    wire: std::sync::OnceLock<Arc<[u8]>>,
}

impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0.inner {
            AnyPayload::Value(value) => f
                .debug_tuple("Payload::Value")
                .field(&value.as_any().type_id())
                .finish(),
            AnyPayload::Bytes(bytes) => f
                .debug_tuple("Payload::Bytes")
                .field(&bytes.as_str())
                .finish(),
            AnyPayload::Json(view) => f.debug_tuple("Payload::Json").field(view).finish(),
        }
    }
}

impl Default for Payload {
    fn default() -> Self {
        Self::json_view(Json::default())
    }
}

impl Payload {
    /// Wraps a live value (the typed send edge's constructor).
    pub(crate) fn value<T: PayloadValue>(value: T) -> Self {
        Self(Arc::new(PayloadCell {
            inner: AnyPayload::Value(Arc::new(value)),
            json_view: std::sync::OnceLock::new(),
            wire: std::sync::OnceLock::new(),
        }))
    }

    /// Wraps wire bytes (the erased-ingress constructor).
    pub(crate) fn bytes(bytes: PayloadBytes) -> Self {
        Self(Arc::new(PayloadCell {
            inner: AnyPayload::Bytes(bytes),
            json_view: std::sync::OnceLock::new(),
            wire: std::sync::OnceLock::new(),
        }))
    }

    /// Wraps a JSON tree as a payload (the internal Json-flavored seams:
    /// reply forwarding to schema/path addresses). Live values remain the
    /// normal shape; this is the escape hatch for payloads that already
    /// exist as trees.
    pub(crate) fn json_view(view: Json) -> Self {
        Self(Arc::new(PayloadCell {
            inner: AnyPayload::Json(view),
            json_view: std::sync::OnceLock::new(),
            wire: std::sync::OnceLock::new(),
        }))
    }

    /// Wraps a shared payload — a refcount bump (the fan-out's
    /// constructor: one value across N handlers).
    pub(crate) fn shared(source: &Payload) -> Self {
        Self(Arc::clone(&source.0))
    }

    /// The payload union erased for downcast dispatch.
    pub(crate) fn inner(&self) -> &AnyPayload {
        &self.0.inner
    }

    /// The JSON view, materializing (and memoizing) through the bytes
    /// when needed. DLQ rendering and foreign-fold surfaces read here;
    /// the typed hot path never does.
    pub(crate) fn json(&self) -> &Json {
        self.0.json_view.get_or_init(|| match &self.0.inner {
            AnyPayload::Json(view) => view.clone(),
            other => Json(serde_json::from_slice(&other.json_text()).unwrap_or_default()),
        })
    }

    /// The wire encoding — serde at most once per payload VALUE (the
    /// memoization lives in the shared cell: one serialize, every later
    /// reader shares the bytes). The journal door's input.
    pub(crate) fn json_text(&self) -> Arc<[u8]> {
        if let AnyPayload::Bytes(bytes) = &self.0.inner {
            return Arc::clone(&bytes.0); // bytes ARE the wire encoding
        }
        self.0.wire.get_or_init(|| self.0.inner.json_text()).clone()
    }

    /// The wire encoding as [`PayloadBytes`] (the journal entry's shape).
    pub(crate) fn wire_bytes(&self) -> PayloadBytes {
        PayloadBytes(self.json_text())
    }

    /// A field's value as a string (the shard-key read).
    pub(crate) fn field(&self, name: &str) -> Option<String> {
        self.0.inner.field(name)
    }
}

/// The payload union INSIDE the shared cell — the three ways a message
/// body can exist. Everything below `Payload` is crate-private; handlers
/// see typed reads (`Payload`-level accessors) and never the union.
pub(crate) enum AnyPayload {
    /// A live value of the schema's registered Rust type. The common
    /// case: every typed `tell`/`publish`/event build lands here.
    Value(Arc<dyn PayloadValue>),
    /// Wire bytes that arrived without a live value (erased ingress) —
    /// the payload is the LATEST registered type's bytes; the decode to
    /// `Json` (and the type) happens on demand at the readers.
    Bytes(PayloadBytes),
    /// A JSON view (the memoized materialization of `Bytes`).
    Json(Json),
}

impl AnyPayload {
    /// Downcasts to the schema's registered type. A `Bytes` payload
    /// decodes into `T` on demand — replayed/erased deliveries join the
    /// live path HERE, the only shape-changing transition in the system.
    pub(crate) fn downcast_ref<T: Schema + serde::de::DeserializeOwned + Clone + 'static>(
        &self,
    ) -> Option<T> {
        match self {
            AnyPayload::Value(value) => value.as_any().downcast_ref::<T>().cloned(),
            AnyPayload::Bytes(bytes) => {
                bump_serde_calls();
                serde_json::from_slice::<T>(bytes.as_bytes()).ok()
            }
            // The erased Json-flavored seams (send_json/ask_json/reply):
            // the handler's type decodes from the tree — borrowed, the
            // same cost the old dispatch paid.
            AnyPayload::Json(view) => {
                bump_serde_calls();
                T::deserialize(&view.0).ok()
            }
        }
    }

    /// Borrows the live value of the schema's registered type — the
    /// borrowed-dispatch accessor. Unlike [`Self::downcast_ref`] (which
    /// always yields an OWNED `T`), a live value is lent as `&T` with
    /// zero copies; the service dispatch awaits the handler on this
    /// borrow, rooted at the kernel's loop-local batch.
    ///
    /// Only the `Value` arm can answer: a `Bytes`/`Json` payload has no
    /// live value to lend (a borrow cannot be materialized from wire
    /// bytes) and yields `None` — replayed/erased deliveries take the
    /// owned [`Self::downcast_ref`] decode instead, the door.
    pub(crate) fn value_ref<T: 'static>(&self) -> Option<&T> {
        match self {
            AnyPayload::Value(value) => value.as_any().downcast_ref::<T>(),
            AnyPayload::Bytes(_) | AnyPayload::Json(_) => None,
        }
    }

    /// A field's value as a string (the shard-key read). Live values
    /// answer from the derive-generated match; bytes decode the one
    /// field through the tree (a router-path read, off the hot fold).
    pub(crate) fn field(&self, name: &str) -> Option<String> {
        match self {
            AnyPayload::Value(value) => value.field(name),
            AnyPayload::Bytes(bytes) => {
                bump_serde_calls();
                let view: Json = serde_json::from_slice(bytes.as_bytes()).ok()?;
                view.get(name).and_then(json_string_of)
            }
            AnyPayload::Json(view) => view.get(name).and_then(json_string_of),
        }
    }

    /// The wire encoding (serde once). The journal door's input.
    pub(crate) fn json_text(&self) -> Arc<[u8]> {
        match self {
            AnyPayload::Value(value) => value.to_json_bytes(),
            AnyPayload::Bytes(bytes) => Arc::clone(&bytes.0),
            AnyPayload::Json(view) => {
                bump_serde_calls();
                Arc::from(serde_json::to_vec(view).expect("Json tree always serializes"))
            }
        }
    }
}

/// A field's string form for shard-key extraction (the `Json` fallback
/// path mirrors the typed `field` contract: strings as-is, numbers and
/// bools stringified, everything else unreadable).
fn json_string_of(value: &serde_json::Value) -> Option<String> {
    use serde_json::Value;
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The runtime's questions to a live payload value, generated by the
/// derive for every declared message type. `std::any::Any` alone cannot
/// answer them: shard-key reads and journal encoding need the schema's
/// field names, which only the type knows.
///
/// The `Clone` supertrait serves handler-side retention: the fabric
/// itself never copies (a payload is an `Arc`), but a handler that
/// stashes or re-sends a message clones its value explicitly — the
/// bound guarantees every declared message type can. It sits behind
/// `CloneablePayloadValue` (`Clone` requires `Self: Sized`) so the
/// erased cell keeps its vtable shape and every public signature is
/// unchanged.
pub trait PayloadValue: Send + Sync + 'static {
    /// The value as `Any` — the downcast dispatch core.
    fn as_any(&self) -> &dyn std::any::Any;
    /// A declared field's value as a string; `None` when the field is
    /// absent or its type has no string form (objects, lists of them).
    fn field(&self, name: &str) -> Option<String>;
    /// The value's compact JSON-text encoding. The derive memoizes this
    /// (one serialization per value, shared by every later reader).
    fn to_json_bytes(&self) -> Arc<[u8]>;

    /// A copy of the erased value (a dyn-safe `Clone`). Returns a
    /// `Box` of the concrete type behind `self` — the handler-side
    /// retention constructor, the only way to copy a value out of the
    /// fabric's shared `Arc` without knowing its type.
    fn clone_value(&self) -> Box<dyn PayloadValue>;
}

/// The `Self: Sized` companion of [`PayloadValue`]: blanket-implemented
/// for every `PayloadValue` that is also `Clone` (every declared message
/// type — the derive's contract requires it), so generic code (the
/// send edge, typed replies, `downcast_ref`) keeps the plain `Clone`
/// bound, and the erased cell gets a dyn-safe hook to the same copy.
pub trait CloneablePayloadValue: PayloadValue + Clone {
    fn clone_value_boxed(&self) -> Box<dyn PayloadValue> {
        Box::new(self.clone())
    }
}

impl<T: PayloadValue + Clone> CloneablePayloadValue for T {}

impl<T: PayloadValue> PayloadValue for Arc<T> {
    fn as_any(&self) -> &dyn std::any::Any {
        (**self).as_any()
    }
    fn field(&self, name: &str) -> Option<String> {
        (**self).field(name)
    }
    fn to_json_bytes(&self) -> Arc<[u8]> {
        (**self).to_json_bytes()
    }
    fn clone_value(&self) -> Box<dyn PayloadValue> {
        (**self).clone_value()
    }
}

/// The ONLY byte type in the system: compact JSON text (valid UTF-8 by
/// construction — every producer is a serde JSON serializer). Bytes exist
/// at exactly two doors — erased ingress and the journal — and never ride
/// the fabric as a fabric arm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadBytes(pub(crate) Arc<[u8]>);

impl Serialize for PayloadBytes {
    /// Serde: the bytes serialize AS their JSON text (a JSON string) —
    /// the journal row stays queryable, not base64 soup.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PayloadBytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Ok(PayloadBytes(Arc::from(text.into_bytes())))
    }
}

impl PayloadBytes {
    /// Wraps already-encoded bytes (journal replay, bench fixtures).
    ///
    /// The caller guarantees the bytes are valid JSON text —
    /// [`PayloadBytes::assert_json_text`] is the debug-gate for the
    /// boundary constructors; replay trusts its own writer.
    pub fn from_bytes(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self(bytes.into())
    }

    /// The bytes as a byte slice (serde_json's `from_slice` input).
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// The bytes as UTF-8 text (they are valid JSON by contract).
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or("<invalid utf8>")
    }

    /// Debug gate for the erased boundary: garbage bytes die loudly at
    /// the door instead of silently inside a handler fold. Runs the
    /// parse; test builds keep it always (the door cost is once per
    /// erased envelope, not per hop).
    pub(crate) fn assert_json_text(&self) {
        if serde_json::from_slice::<serde_json::Value>(&self.0).is_err() {
            panic!(
                "PayloadBytes: erased ingress delivered non-JSON bytes: {:?}",
                self.as_str().get(..120).unwrap_or("<..>")
            );
        }
    }
}

impl From<Json> for Payload {
    fn from(view: Json) -> Self {
        Self::json_view(view)
    }
}

impl From<&Json> for PayloadBytes {
    fn from(value: &Json) -> Self {
        bump_serde_calls();
        Self(Arc::from(
            serde_json::to_vec(&value).expect("Json tree always serializes"),
        ))
    }
}

impl From<Json> for PayloadBytes {
    fn from(value: Json) -> Self {
        Self::from(&value)
    }
}

/// Counts one serde boundary crossing (test probe — see
/// `kernel::bump_manifest_clones` for the pattern).
#[allow(dead_code)] // called from cfg(test) probe paths; kept compiled
pub(crate) fn bump_serde_calls() {
    #[cfg(test)]
    crate::kernel::bump_serde_calls();
}

/// The derive's `to_json_bytes` body: compact JSON text, counted once per
/// real serialization (the derive memoizes at the call site via a
/// `OnceLock` wrapper — this helper runs at most once per value).
pub fn payload_value_json_bytes<T: Serialize>(value: &T) -> Arc<[u8]> {
    bump_serde_calls();
    Arc::from(serde_json::to_vec(value).expect("schema payload serialization cannot fail"))
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

/// A domain event: schema-tagged payload, appended to journals and folded
/// into state. Events are facts that already happened. The payload rides
/// the fabric as a live value (serde once — at the journal door).
#[derive(Debug, Clone)]
pub struct Event {
    /// The event's schema.
    pub schema: SchemaId,
    /// The event's payload (live value, bytes from replay, or a memoized
    /// JSON view).
    pub payload: Payload,
}

impl PartialEq for Event {
    /// Event equality compares the JSON views — the semantic content.
    /// (Two `Value` payloads of the same type compare through their
    /// encodings; PartialEq on the erased `Arc` cannot.)
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema && self.payload_json() == other.payload_json()
    }
}

impl Event {
    /// Creates an event from a typed fact (the fold-friendly
    /// constructor). The fact is wrapped, never serialized.
    pub fn new<T: Into<Payload>>(schema: SchemaId, fact: T) -> Self {
        Self {
            schema,
            payload: fact.into(),
        }
    }

    /// Creates an event from wire bytes (journal replay's constructor).
    pub fn from_bytes(schema: SchemaId, bytes: PayloadBytes) -> Self {
        Self {
            schema,
            payload: Payload::bytes(bytes),
        }
    }

    /// Creates an event from a JSON tree (the internal Json-flavored
    /// seams: projector re-records, foreign folds).
    pub fn from_json_view(schema: SchemaId, view: Json) -> Self {
        Self {
            schema,
            payload: Payload::json_view(view),
        }
    }

    /// Creates an event that ADOPTS an existing payload (an `Arc` bump,
    /// never a copy) — the consume-entry seam: a projector's consumed
    /// command re-enters its journal as a fact without a single payload
    /// copy.
    pub fn with_shared_payload(schema: SchemaId, payload: Payload) -> Self {
        Self { schema, payload }
    }

    /// Extracts the typed fact `T`, matching by schema NAME.
    ///
    /// The typed fold for [`crate::actor::EventSourcedActor::apply`] and
    /// [`crate::actor::Projector::apply`]: an event whose schema is not
    /// `T`'s (another fact type) yields `None` without touching the
    /// payload; a matching event downcasts (a live value: a TypeId
    /// recognition and an `Arc` read, no decode) — or decodes from bytes
    /// (a replayed event), the system's only shape-changing transition.
    ///
    /// Unmatched schemas are simply ignored — one fold can consume several
    /// fact types by stacking `as_fact` calls.
    pub fn as_fact<T: Schema + serde::de::DeserializeOwned + Clone + 'static>(&self) -> Option<T> {
        if self.schema != T::schema_id() {
            return None;
        }
        self.payload.inner().downcast_ref::<T>()
    }

    /// Whether this event's schema is exactly `T`'s (the name).
    ///
    /// A pure name comparison: no clone, no decode. Use it to skip
    /// work, or when the payload shape is read dynamically instead of
    /// decoded into a type.
    pub fn is<T: Schema>(&self) -> bool {
        self.schema == T::schema_id()
    }

    /// The event's JSON view — materialized on demand (see
    /// [`Envelope::payload_json`]).
    pub fn payload_json(&self) -> &Json {
        self.payload.json()
    }
}

/// The runtime's message wrapper. Not serializable as a whole: the durable
/// parts (trace, schema, JSON payload) are copied out when crossing the
/// runtime boundary.
#[derive(Debug, Clone)]
pub struct Envelope {
    /// The payload's schema.
    pub schema: SchemaId,
    /// Where the message is headed.
    pub dest: Address,
    /// The logical sending path, when the sender is an actor.
    pub from: Option<ActorPath>,
    /// Where a reply should go: a durable [`Address::Path`] or a
    /// short-lived reply slot ([`Address::Slot`]).
    pub reply_to: Option<Address>,
    /// Trace metadata of this hop.
    pub trace: TraceCtx,
    /// The message body.
    pub payload: Payload,
    /// Where this copy's fact was recorded — `(source journal, seq)` —
    /// when the message is a recorded fact (broadcast copies of consumed
    /// events). A projector stamps its journal entries with it: the
    /// checkpoint's source identity for live-delivered copies, so a
    /// restart never re-seeds (never double-folds) a fact it folded live.
    /// `None` for everything else (commands, host sends).
    pub(crate) recorded_origin: Option<RecordedOrigin>,
}

/// The fact-stamp a broadcast copy carries (see
/// [`Envelope::recorded_origin`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedOrigin {
    /// The journal the fact was recorded in.
    pub journal: ActorPath,
    /// The fact's seq inside that journal.
    pub seq: crate::journal::SeqNo,
}

impl Envelope {
    /// Assembles a typed envelope: `value` becomes the message body with
    /// NO serialization — the fabric carries the live value, and serde
    /// happens only at the doors (journal append, journal replay).
    pub fn json<T: Into<Payload>>(
        schema: SchemaId,
        dest: Address,
        value: T,
        trace: TraceCtx,
    ) -> Self {
        Self::raw(schema, dest, value.into(), trace)
    }

    /// Assembles a payload-SHARING envelope (a `Payload` Arc bump, never a
    /// copy of the value) — the kernel's constructor for tee/fan-out copies
    /// of an existing message.
    pub(crate) fn shared_from(
        schema: SchemaId,
        dest: Address,
        source: &Envelope,
        trace: TraceCtx,
    ) -> Self {
        let mut copy = Self::raw(schema, dest, Payload::shared(&source.payload), trace);
        copy.recorded_origin = source.recorded_origin.clone();
        copy
    }

    /// The bare constructor: schema, destination, payload, trace. The
    /// metadata fields default; setters chain the rest.
    pub(crate) fn raw(schema: SchemaId, dest: Address, payload: Payload, trace: TraceCtx) -> Self {
        Self {
            schema,
            dest,
            from: None,
            reply_to: None,
            trace,
            payload,
            recorded_origin: None,
        }
    }

    /// Assembles a JSON-text-payload envelope — the erased boundary:
    /// ingress callers (foreign bridges, command emulations) hold wire
    /// bytes, never live Rust values.
    ///
    /// # Panics
    ///
    /// Panics when `bytes` is not valid JSON text — the erased boundary's
    /// contract is that bytes were produced by a serializer; garbage bytes
    /// are a caller bug, not a runtime value (garbage IN the fabric dies
    /// loudly HERE, never inside a handler fold).
    pub fn from_bytes(
        schema: SchemaId,
        dest: Address,
        bytes: PayloadBytes,
        trace: TraceCtx,
    ) -> Self {
        bytes.assert_json_text();
        Self::raw(schema, dest, Payload::bytes(bytes), trace)
    }

    /// Assembles a bytes-payload envelope from an owned JSON tree — the
    /// convenience form of [`Envelope::from_bytes`]: the tree serializes
    /// to compact text once, at the door.
    pub fn from_bytes_wrapped(
        schema: SchemaId,
        dest: Address,
        payload: impl Into<Json>,
        trace: TraceCtx,
    ) -> Self {
        Self::from_bytes(schema, dest, PayloadBytes::from(payload.into()), trace)
    }

    /// The payload's JSON view — materialized ON DEMAND for the readers
    /// that genuinely need a tree (DLQ rendering, foreign fold surfaces).
    /// A `json` view is memoized into the payload's `OnceLock`, so the
    /// first reader pays the decode and every later reader shares it.
    pub fn payload_json(&self) -> &Json {
        self.payload.json()
    }

    /// Stamps this copy as a recorded fact from `(journal, seq)` (the
    /// broadcast fan-out does this; projectors read it back into their
    /// checkpoint).
    pub fn with_recorded_origin(mut self, journal: ActorPath, seq: crate::journal::SeqNo) -> Self {
        self.recorded_origin = Some(RecordedOrigin { journal, seq });
        self
    }

    /// The copy's fact stamp, if it carries one.
    pub fn recorded_origin(&self) -> Option<&RecordedOrigin> {
        self.recorded_origin.as_ref()
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

    /// The payload as JSON, if a JSON view is already materialized (no
    /// on-demand decode — readers that need a tree use
    /// [`Envelope::payload_json`]).
    pub fn as_json(&self) -> Option<&Json> {
        match self.payload.inner() {
            AnyPayload::Json(view) => Some(view),
            _ => None,
        }
    }

    /// The payload's wire encoding (bytes), for the journal door and the
    /// erased bridge. Value payloads serialize here ONCE — callers that
    /// only peek (never crossing a door) never pay this.
    pub fn to_payload_bytes(&self) -> PayloadBytes {
        PayloadBytes(self.payload.json_text())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json;

    #[test]
    fn address_survives_serde_roundtrip_for_each_variant() {
        // Given one address of each variant.
        let lease = LeaseId::new();
        let addresses = [
            Address::Path(ActorPath::new("inventory.west")),
            Address::Slot(lease),
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
    fn trace_id_minting_pays_one_clock_read_and_no_getrandom() {
        // Given the id-minting probes (each `next_uuid_shaped_id` bumps
        // the clock-read counter once and the getrandom-skipped counter
        // once; a real `Uuid::now_v7` call would skip the bump).
        crate::kernel::ID_CLOCK_READS.store(0, std::sync::atomic::Ordering::Relaxed);
        crate::kernel::GETRANDOM_SKIPPED.store(0, std::sync::atomic::Ordering::Relaxed);
        const ROOTS: u64 = 1_000;

        // When minting ROOTS trace contexts: root() pays 2 ids (trace +
        // causality), each caused() hop renews causality and pays 1 more.
        for _ in 0..ROOTS {
            let parent = TraceCtx::root();
            let _child = parent.caused();
        }

        // Then getrandom was never paid: every mint is counted as skipped.
        assert_eq!(
            crate::kernel::GETRANDOM_SKIPPED.load(std::sync::atomic::Ordering::Relaxed),
            3 * ROOTS,
            "every id mint must be getrandom-free"
        );
        // And exactly one clock read backed each id.
        assert_eq!(
            crate::kernel::ID_CLOCK_READS.load(std::sync::atomic::Ordering::Relaxed),
            3 * ROOTS,
            "each id costs exactly one vDSO clock read"
        );
    }

    #[test]
    fn json_envelope_roundtrips_payload_and_metadata() {
        // Given a bytes-payload envelope (the erased boundary) with
        // sender and reply-to set.
        let envelope = Envelope::from_bytes(
            SchemaId::new("ReserveStock"),
            Address::Path(ActorPath::new("inventory.west")),
            PayloadBytes::from(Json::of(&json!({ "sku": "widget", "qty": 2 }))),
            TraceCtx::root(),
        )
        .from(ActorPath::new("storefront"))
        .reply_to(Address::Path(ActorPath::new("storefront")));

        // When reading the payload view back out.
        let payload = envelope.payload_json();

        // Then the metadata and payload survive intact.
        assert_eq!(envelope.schema.as_str(), "ReserveStock");
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
    fn bytes_payload_view_is_memoized() {
        // Given a bytes-payload envelope.
        let envelope = Envelope::from_bytes(
            SchemaId::new("Tick"),
            Address::Path(ActorPath::new("a")),
            PayloadBytes::from(Json::of(&json!({ "n": 42 }))),
            TraceCtx::root(),
        );

        // When reading the JSON view twice.
        let first = envelope.payload_json();
        let second = envelope.payload_json();

        // Then both reads agree — and the Debug shape renders the bytes.
        assert_eq!(first, second);
        assert_eq!(first["n"], 42);
    }
}

/// Identifies a whole causal conversation (a request and everything it caused).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TraceId(Uuid);

impl TraceId {
    /// Generates a fresh, time-ordered (v7-shaped) trace id.
    pub fn new() -> Self {
        Self(next_uuid_shaped_id())
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
    /// Generates a fresh, time-ordered (v7-shaped) causality id.
    pub fn new() -> Self {
        Self(next_uuid_shaped_id())
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

/// The id generator behind [`TraceId::new`], [`CausalityId::new`], and
/// [`crate::reply::LeaseId::new`]: a v7-SHAPED uuid assembled from one
/// coarse clock read plus a process-unique counter — no `getrandom`, no
/// float math, per id.
///
/// Layout (RFC 9562 v7, so `Display`/parse/`get_timestamp` stay valid):
/// bits 0..48 carry the unix millisecond (time-ordered at ms grain,
/// `as_millis_ts` reads it back), bits 48..52 the `7` version nibble,
/// bits 52..54 the RFC-4122 variant, and the remaining 62 bits a
/// process-unique counter. The counter mixes into the HIGH free bits so
/// consecutive ids differ early in the rendered string, not only at the
/// tail. Uniqueness holds within a process (one atomic counter feeds
/// every id kind); across restarts the fresh clock value separates ids —
/// ids never persist (journals carry schema + payload only; ids ride
/// envelopes and observations), so no durable collision surface exists.
///
/// Layout map (uuid bytes are big-endian, `from_u64_pair(high, low)`):
/// `high` bits 63..16 = unix ms (48), bits 15..12 = version `0x7`,
/// bits 11..0 = rand_a (12) — carrying the counter's top 12 bits.
/// `low` bits 63..62 = variant `0b10`, bits 61..0 = rand_b (62) —
/// carrying the counter's low 62 bits. The full 62-bit counter is
/// recoverable from (rand_a, rand_b), so ids never collide within a
/// process, and consecutive ids differ in the rendered string's middle
/// groups, not only at the tail.
fn next_uuid_shaped_id() -> Uuid {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let raw_millis = {
        #[cfg(test)]
        crate::kernel::bump_id_clock_reads();
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            // A clock before the epoch degrades to 0: ids stay unique by
            // counter, only their time order is lost.
            .unwrap_or(0)
    };
    // The monotonic guard: a clock step backwards (NTP correction) would
    // break v7's time-ordered property; hold the high-water mark instead.
    static LAST_MILLIS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let millis = {
        let mut last = LAST_MILLIS.load(std::sync::atomic::Ordering::Relaxed);
        loop {
            if raw_millis > last {
                match LAST_MILLIS.compare_exchange_weak(
                    last,
                    raw_millis,
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                ) {
                    Ok(_) => break raw_millis,
                    Err(now) => last = now,
                }
            } else {
                break last; // step-back or same ms: keep the high-water mark
            }
        }
    };
    let counter =
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) & 0x3FFF_FFFF_FFFF_FFFF;
    let high = (millis << 16) | 0x7000 | ((counter >> 50) & 0xFFF);
    let low = 0x8000_0000_0000_0000 | (counter & 0x3FFF_FFFF_FFFF_FFFF);
    #[cfg(test)]
    // TEST PROBE: every id minted here is one `Uuid::now_v7` NOT paid —
    // no getrandom, no float pow (the tell-path deliverable).
    crate::kernel::bump_getrandom_skipped();
    Uuid::from_u64_pair(high, low)
}

/// The lease-id entry into the shared generator (the ask path's
/// constructor; pub(crate) so `reply.rs` shares the one counter).
pub(crate) fn next_lease_id() -> Uuid {
    next_uuid_shaped_id()
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
fn trace_ids_are_unique_across_threads() {
    // Given eight tasks each minting 100k ids (the shared atomic counter
    // is the only uniqueness mechanism — this is the collision probe).
    let handle = |task: usize| {
        std::thread::spawn(move || {
            let mut ids = std::collections::HashSet::with_capacity(100_000);
            for n in 0..100_000_u64 {
                // Alternate kinds: ONE counter feeds trace, causality,
                // and lease ids, so the guarantee is shared-counter-wide.
                let id = match (task + n as usize) % 3 {
                    0 => TraceId::new().0.as_u128(),
                    1 => CausalityId::new().0.as_u128(),
                    _ => lease_probe().as_u128(),
                };
                ids.insert(id);
            }
            ids
        })
    };
    let handles: Vec<_> = (0..8).map(handle).collect();
    let mut all = std::collections::HashSet::new();
    for h in handles {
        let part = h.join().expect("thread");
        assert_eq!(part.len(), 100_000, "no duplicate within one task");
        all.extend(part);
    }

    // Then all 800k ids are distinct across every kind and task.
    assert_eq!(all.len(), 800_000);
}

#[test]
fn counter_ids_read_back_their_millisecond_timestamp() {
    // Given a freshly generated causality id (the observation ts source).
    let before_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64;
    let id = CausalityId::new();
    let after_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64;

    // When reading the embedded timestamp back.
    let ts = id.as_millis_ts().as_millis();

    // Then it falls inside the generation window (the packed millis is
    // the REAL clock — observation timestamps stay truthful).
    assert!(
        (before_ms..=after_ms).contains(&ts),
        "ts {ts} outside {before_ms}..{after_ms}"
    );
}

#[test]
fn legacy_v7_uuid_strings_decode_into_counter_ids() {
    // Given a REAL v7 uuid written by a pre-change runtime (an old
    // observation stream, a serialized envelope in flight).
    let legacy = "018f6a2c-9f6b-7000-8000-3b3a9d6e4f21";
    let round: TraceId = serde_json::from_str(&format!("\"{legacy}\"")).expect("decode");

    // When rendering it back.
    let rendered = round.to_string();

    // Then the wire string survives untouched — the serde boundary never
    // minted a new id.
    assert_eq!(rendered, legacy);
}

#[test]
fn counter_ids_keep_the_rfc_variant_bits() {
    // Given a freshly generated id of each kind.
    let trace = TraceId::new().0;
    let causality = CausalityId::new().0;
    let lease = lease_probe();

    // When inspecting the variant nibble.
    // Then every id is a valid RFC 4122 uuid (parsers never choke).
    for id in [trace, causality, lease] {
        assert_eq!(id.get_version_num(), 7);
        let variant_nibble = (id.as_u128() >> 62) & 0b11;
        assert_eq!(variant_nibble, 0b10, "RFC 4122 variant");
    }
}

/// A lease id's uuid (the field is private to `reply`; this probe reads
/// the shared generator's output for it).
#[cfg(test)]
fn lease_probe() -> Uuid {
    crate::reply::lease_id_probe()
}

#[test]
fn counter_ids_stay_time_ordered_within_a_millisecond() {
    // Given a burst of ids minted back-to-back (same ms almost surely).
    let mut prev = TraceId::new().0.as_u128();

    // When minting 10k more.
    for _ in 0..10_000 {
        let next = CausalityId::new().0.as_u128();
        // Then ordering never goes backwards (the v7 property the canvas
        // and the monotonic millis guard both rely on).
        assert!(next > prev, "ids must strictly increase");
        prev = next;
    }
}

// ---------------------------------------------------------------------------
// Events — a compact buffer of journal-ready events (§5)
// ---------------------------------------------------------------------------

/// The events a command handler decided on, ready to journal.
///
/// Construct from typed facts with [`Events::one`] and
/// [`Events::push_event`]; conversion through [`IntoEvent`] is automatic.
/// The runtime appends these to the journal in order.
///
/// The buffer is append-only: [`Events::push`] exists for events built by
/// hand, [`Events::push_event`] for typed facts.
#[derive(Debug, Clone, Default)]
pub struct Events(SmallVec<[Event; 2]>);

impl Events {
    /// An empty buffer.
    pub fn new() -> Self {
        Self(SmallVec::new())
    }

    /// A buffer holding exactly one event, built from a typed fact.
    ///
    /// The common shape of an event-sourced decision: the handler returns
    /// `Events::one(Deposited { n })`. Panics if serializing the fact fails
    /// — see [`IntoEvent`].
    pub fn one(e: impl IntoEvent) -> Self {
        let mut ev = Self::new();
        ev.push_event(e);
        ev
    }

    /// Appends an event built from a typed fact.
    ///
    /// Panics if serializing the fact fails — see [`IntoEvent`].
    pub fn push_event(&mut self, e: impl IntoEvent) {
        self.0.push(e.into_event());
    }

    /// Appends a hand-built [`Event`].
    pub fn push(&mut self, e: Event) {
        self.0.push(e);
    }

    /// Adopts an already-allocated `Vec` of events without copying them.
    ///
    /// The bridge for actors implemented outside this crate's typed
    /// dispatch (foreign actors whose fold closures return `Vec`).
    pub fn from_vec(v: Vec<Event>) -> Self {
        Self(SmallVec::from_vec(v))
    }

    /// The events as a plain slice.
    pub fn as_slice(&self) -> &[Event] {
        &self.0
    }
}

impl Deref for Events {
    type Target = [Event];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for Events {
    type Item = Event;
    type IntoIter = smallvec::IntoIter<[Event; 2]>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<Event> for Events {
    fn from_iter<T: IntoIterator<Item = Event>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl Extend<Event> for Events {
    fn extend<T: IntoIterator<Item = Event>>(&mut self, iter: T) {
        self.0.extend(iter);
    }
}

// ---------------------------------------------------------------------------
// IntoEvent — typed facts become journal-ready events (§6)
// ---------------------------------------------------------------------------

/// A typed fact that knows how to become a journal-ready [`Event`].
///
/// Implemented automatically for every type that is a [`Schema`] and
/// [`PayloadValue`] — i.e. for every declared message type (the derive
/// generates both). Call sites never name this trait; they pass typed
/// facts straight to [`Events::one`] or [`Events::push_event`]. Building
/// an event WRAPS the value — no serialization on this path; serde
/// happens once, when the journal door asks for the wire encoding.
pub trait IntoEvent: Schema + Into<Payload> + Sized {
    /// Builds the event, deriving the schema id from the type.
    fn into_event(self) -> Event {
        Event {
            schema: Self::schema_id(),
            payload: self.into(),
        }
    }
}

impl<T: Schema + PayloadValue> IntoEvent for T {}

impl<T: PayloadValue> From<T> for Payload {
    fn from(value: T) -> Self {
        Payload::value(value)
    }
}

#[cfg(test)]
mod events_tests {
    use super::*;
    use crate::actor::CommandEntry as _;
    use crate::actor::CommandHandler;
    use crate::actor::EventSourcedActor as _;
    use crate::context::CmdCtx;
    use crate::json;
    use serde::Deserialize;

    #[derive(crate::schema::Event, Serialize, Deserialize, Clone)]
    struct Deposited {
        n: i64,
    }

    #[test]
    fn value_ref_borrows_the_live_value_without_a_copy() {
        // Given a payload wrapping a live value.
        let payload = Payload::value(Deposited { n: 5 });

        // When borrowing the typed value.
        let fact: Option<&Deposited> = payload.inner().value_ref::<Deposited>();

        // Then the borrow reads the wrapped value — zero clones, zero serde.
        assert_eq!(fact.expect("live borrow").n, 5);
    }

    #[test]
    fn value_ref_returns_none_for_the_wrong_type() {
        // Given a payload wrapping a live Deposited.
        let payload = Payload::value(Deposited { n: 5 });

        // When borrowing it as a different live type.
        let fact: Option<&StockReserved> = payload.inner().value_ref::<StockReserved>();

        // Then nothing comes back (a TypeId miss, no decode).
        assert!(fact.is_none());
    }

    #[test]
    fn value_ref_cannot_lend_from_bytes_or_json_views() {
        // Given a bytes payload (replay shape) and a Json view payload.
        let bytes = Payload::bytes(PayloadBytes::from(Json::of(&Deposited { n: 9 })));
        let view = Payload::json_view(json!({ "n": 9 }));

        // When borrowing the declared type from each.
        let from_bytes: Option<&Deposited> = bytes.inner().value_ref::<Deposited>();
        let from_view: Option<&Deposited> = view.inner().value_ref::<Deposited>();

        // Then neither lends a borrow — a wire shape has no live value;
        // those deliveries take the owned downcast decode instead.
        assert!(from_bytes.is_none());
        assert!(from_view.is_none());
    }

    #[test]
    fn events_stays_inline_for_two_and_spills_past() {
        // Given an empty buffer.
        let mut events = Events::new();

        // When filling it up to the inline capacity.
        events.push_event(Deposited { n: 1 });
        events.push_event(Deposited { n: 2 });

        // Then two events stay inline (no heap allocation).
        assert!(!events.0.spilled(), "two events must stay inline");

        // When pushing a third event.
        events.push_event(Deposited { n: 3 });

        // Then the buffer spilled to the heap transparently and kept every
        // event in order.
        assert!(events.0.spilled(), "past two events must spill");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].payload_json()["n"], 1);
        assert_eq!(events[2].payload_json()["n"], 3);
    }

    #[test]
    fn into_event_derives_schema_id_and_payload_from_type() {
        // Given a typed fact.
        let fact = Deposited { n: 5 };

        // When converting it into an event.
        let event = fact.into_event();

        // Then the schema id is derived from the type and the
        // payload is the serialized fact.
        assert_eq!(event.schema, Deposited::schema_id());
        assert_eq!(event.schema.as_str(), "Deposited");
        assert_eq!(event.payload_json()["n"], 5);
    }

    #[test]
    fn as_fact_yields_the_typed_fact_on_exact_schema_name() {
        // Given an event built from a live fact.
        let event = Deposited { n: 7 }.into_event();

        // When extracting the typed fact.
        let fact: Option<Deposited> = event.as_fact();

        // Then the fact comes back with its data — a downcast, no decode.
        assert_eq!(fact.expect("as_fact").n, 7);
    }

    #[test]
    fn as_fact_downcasts_replayed_bytes_by_name() {
        // Given an event RECORDED as bytes (replay shape) under the same
        // schema name.
        let bytes = PayloadBytes::from(Json::of(&Deposited { n: 9 }));
        let event = Event::from_bytes(Deposited::schema_id(), bytes);

        // When extracting the typed fact.
        let fact: Option<Deposited> = event.as_fact();

        // Then the bytes decode into the declared struct — replayed
        // deliveries join the live fold path.
        assert_eq!(fact.expect("decode").n, 9);
    }

    #[test]
    fn as_fact_returns_none_for_other_facts() {
        // Given an event under a DIFFERENT schema name.
        let event = Event::from_json_view(SchemaId::new("Withdrawn"), json!({ "n": 9 }));

        // When asking it for a Deposited.
        let fact: Option<Deposited> = event.as_fact();

        // Then nothing comes back: folds stack safely by name.
        assert!(fact.is_none());
    }

    #[test]
    fn is_matches_without_cloning_or_decoding() {
        // Given two events: one Deposited, one Withdrawn.
        let deposited = Deposited { n: 1 }.into_event();
        let withdrawn = Event::from_json_view(SchemaId::new("Withdrawn"), json!({ "n": 1 }));

        // When asking each whether it IS a Deposited.
        let deposited_is = deposited.is::<Deposited>();
        let withdrawn_is = withdrawn.is::<Deposited>();

        // Then only the exact-schema event matches (pure comparison).
        assert!(deposited_is);
        assert!(!withdrawn_is);
    }

    #[test]
    fn into_event_wraps_without_serializing() {
        // Given a typed fact.
        let fact = Deposited { n: 3 };

        // When converting it into an event.
        let event = fact.into_event();

        // Then the payload IS the value (downcast back, no bytes needed).
        let fact: Option<Deposited> = event.as_fact();
        assert_eq!(fact.expect("live value").n, 3);
    }

    #[test]
    fn dlq_payload_json_still_renders_for_inspection() {
        // Given an event built from a live fact.
        let event = Deposited { n: 11 }.into_event();

        // When rendering its JSON view (the DLQ materializer path).
        let view = event.payload_json();

        // Then the tree is inspectable.
        assert_eq!(view["n"], 11);
    }

    #[test]
    fn payload_bytes_from_json_roundtrips_text() {
        // Given a Json tree.
        let tree = json!({ "k": "v" });

        // When converting to wire bytes and back.
        let bytes = PayloadBytes::from(tree.clone());
        let round: Json = serde_json::from_slice(bytes.as_bytes()).expect("parse");

        // Then the bytes ARE the compact JSON text of the tree.
        assert_eq!(bytes.as_str(), r#"{"k":"v"}"#);
        assert_eq!(round, tree);
    }

    #[test]
    fn dispatch_journals_exactly_the_events_handle_returned() {
        // Given a counter whose handler returns one typed fact, driven
        // through the erased dispatch the kernel uses.
        use crate::context::{Outbox, RuntimeView};
        struct NullView;
        impl RuntimeView for NullView {
            fn lookup(&self, _path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
                None
            }
            fn handlers_of(&self, _schema: &SchemaId) -> Vec<ActorPath> {
                Vec::new()
            }
            fn now(&self) -> crate::clock::Timestamp {
                crate::clock::Timestamp::from_millis(0)
            }
        }
        let mut state = crate::actor::TypedEsState::new(Counter::restore(&json!({})));
        let adapter = crate::actor::TypedEsAdapter::<Counter, ReserveStock>::new::<ReserveStock>();
        let trace = TraceCtx::root();
        let path = ActorPath::new("t");
        let mut outbox = Outbox::new();
        let mut ctx = CmdCtx::new(&path, &trace, None, &NullView, &mut outbox);

        // When dispatching the command.
        let events = adapter
            .dispatch(
                &mut state,
                &Payload::value(ReserveStock { qty: 4 }),
                &mut ctx,
            )
            .expect("dispatch");

        // Then the buffer holds exactly the decided events, in order, ready
        // to append as a slice (deref to &[Event], no conversion).
        assert_eq!(events.len(), 1);
        let appended: &[Event] = &events;
        assert_eq!(appended[0].schema, StockReserved::schema_id());
        assert_eq!(appended[0].payload_json()["qty"], 4);
    }

    /// Minimal counter state for the dispatch test above (same shape as the
    /// actor.rs doc fixture).
    #[derive(Serialize, Deserialize, Default, Clone)]
    struct Counter {
        count: i64,
    }
    impl crate::actor::EventSourcedActor for Counter {
        fn manifest() -> crate::schema::ActorManifest {
            crate::schema::ActorManifest::new()
        }
        fn restore(_args: &Json) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &Event) {
            if event.schema == StockReserved::schema_id() {
                self.count += event.payload_json()["qty"].as_i64().unwrap_or(0);
            }
        }
    }
    impl CommandHandler<ReserveStock> for Counter {
        fn handle(&self, cmd: ReserveStock, _ctx: &mut CmdCtx<'_>) -> Events {
            Events::one(StockReserved { qty: cmd.qty })
        }
    }

    #[derive(crate::schema::Command, Serialize, Deserialize, Clone)]
    struct ReserveStock {
        qty: i64,
    }

    #[derive(crate::schema::Event, Serialize, Deserialize, Clone)]
    struct StockReserved {
        qty: i64,
    }
}
