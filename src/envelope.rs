//! Runtime-internal envelopes and the message shapes that ride in them.
//!
//! Users never construct envelopes: the runtime assembles the metadata at the
//! send boundary. Handlers see typed payloads plus a context. Payloads cross
//! the runtime boundary as JSON; the typed arm exists only for in-process
//! zero-copy fast paths, erased exactly once at spawn.

use std::ops::Deref;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use smallvec::SmallVec;
use uuid::Uuid;

use crate::actor::ActorPath;
use crate::clock::Timestamp;
use crate::reply::LeaseId;
use crate::schema::{Schema, SchemaId};

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

    /// Decodes the payload into the typed fact `T`, matching by schema id
    /// (`name@version`, exact — version-pinned).
    ///
    /// The typed fold for [`crate::actor::EventSourcedActor::apply`] and
    /// [`crate::actor::Projector::apply`]: an event whose schema is not
    /// `T`'s (another fact type, or another VERSION of this one) yields
    /// `None` without touching the payload; a matching event decodes. The
    /// payload is cloned (one value copy per decoded event — the same cost
    /// command dispatch already pays).
    ///
    /// Unmatched schemas are simply ignored — one fold can consume several
    /// fact types by stacking `decode` calls.
    pub fn decode<T: Schema + serde::de::DeserializeOwned>(&self) -> Option<T> {
        if self.schema != T::schema_id() {
            return None;
        }
        serde_json::from_value(self.payload.clone()).ok()
    }

    /// Whether this event's schema is exactly `T`'s (`name@version`).
    ///
    /// A pure schema-id comparison: no clone, no decode. Use it to skip
    /// work, or when the payload shape is read dynamically instead of
    /// decoded into a type.
    pub fn is<T: Schema>(&self) -> bool {
        self.schema == T::schema_id()
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
    /// Where this copy's fact was recorded — `(source journal, seq)` —
    /// when the message IS a recorded fact (broadcast copies of consumed
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
    /// Assembles a JSON envelope — the waist representation.
    pub fn json(schema: SchemaId, dest: Address, payload: JsonValue, trace: TraceCtx) -> Self {
        Self {
            schema,
            dest,
            from: None,
            reply_to: None,
            trace,
            payload: Payload::Json(payload),
            recorded_origin: None,
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
            recorded_origin: None,
        }
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

// ---------------------------------------------------------------------------
// Events — a compact buffer of journal-ready events (§5)
// ---------------------------------------------------------------------------

/// The events a command handler decided on, ready to journal.
///
/// Backed by a [`SmallVec`]: up to two events stay inline with no heap
/// allocation, which covers almost every event-sourced decision. Overflowing
/// past two spills to the heap transparently. Handler code never names the
/// backing type — construct with [`Events::new`], [`Events::one`], and
/// [`Events::push_event`], and convert typed facts with [`IntoEvent`].
///
/// The kernel appends these to the journal in order; conversion to a plain
/// slice happens through `Deref`, so [`crate::journal::JournalStore::append`]
/// takes `&[Event]` with no allocation on the path.
///
/// There is no `DerefMut<Target = [Event]>`: once a decision is made the
/// buffer is append-only. [`Events::push`] is the raw escape hatch for
/// events built by hand; prefer [`Events::push_event`].
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
    /// `Events::one(Deposited { n })` instead of building an [`Event`] by
    /// hand. Panics if serializing the fact fails — see [`IntoEvent`].
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

    /// Appends a hand-built [`Event`] (raw escape hatch).
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
/// [`Serialize`] — i.e. for every declared message type. Call sites never
/// name this trait; they pass typed facts straight to [`Events::one`] or
/// [`Events::push_event`].
///
/// # Panics
///
/// [`IntoEvent::into_event`] panics if serializing the payload fails. For
/// the runtime's value domain (strings, numbers, bools, nulls, and
/// compositions of them) serialization cannot fail, so a failure here means
/// a programming bug — a failing invariant is a crash, not a value. Use
/// [`IntoEvent::try_into_event`] at the boundary of hand-rolled `Serialize`
/// impls that can fail.
pub trait IntoEvent: Schema + Serialize + Sized {
    /// Builds the event, deriving the schema id from the type.
    ///
    /// # Panics
    ///
    /// Panics with a named message if serializing the payload fails.
    fn into_event(self) -> Event {
        self.try_into_event()
            .unwrap_or_else(|e| panic!("IntoEvent: failed to serialize payload for {}: {e}", Self::schema_id()))
    }

    /// Builds the event, reporting serialization failure instead of
    /// panicking.
    ///
    /// # Errors
    ///
    /// Returns the underlying `serde_json` error if the payload cannot be
    /// serialized.
    fn try_into_event(self) -> Result<Event, serde_json::Error> {
        Ok(Event::new(
            Self::schema_id(),
            serde_json::to_value(&self)?,
        ))
    }
}

impl<T: Schema + Serialize> IntoEvent for T {}

#[cfg(test)]
mod events_tests {
    use super::*;
    use crate::actor::CommandHandler;
    use crate::actor::CommandEntry as _;
    use crate::actor::EventSourcedActor as _;
    use crate::context::CmdCtx;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Serialize, Deserialize)]
    struct Deposited {
        n: i64,
    }
    impl Schema for Deposited {
        fn schema_def() -> crate::schema::SchemaDef {
            crate::schema::SchemaDef {
                name: "Deposited".into(),
                version: 1,
                kind: crate::schema::SchemaKind::Event,
                fields: vec![],
                description: None,
            }
        }
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
        assert_eq!(events[0].payload["n"], 1);
        assert_eq!(events[2].payload["n"], 3);
    }

    #[test]
    fn into_event_derives_schema_id_and_payload_from_type() {
        // Given a typed fact.
        let fact = Deposited { n: 5 };

        // When converting it into an event.
        let event = fact.into_event();

        // Then the schema id is derived from the type (name@version) and the
        // payload is the serialized fact.
        assert_eq!(event.schema, Deposited::schema_id());
        assert_eq!(event.schema.as_str(), "Deposited@1");
        assert_eq!(event.payload["n"], 5);
    }

    #[test]
    fn decode_yields_the_typed_fact_on_exact_schema_id() {
        // Given an event recorded as `Deposited@1`.
        let event = Deposited { n: 7 }.into_event();

        // When decoding it into the typed fact.
        let decoded: Option<Deposited> = event.decode();

        // Then the fact comes back with its data.
        assert_eq!(decoded.expect("decode").n, 7);
    }

    #[test]
    fn decode_returns_none_on_version_mismatch() {
        // Given an event recorded as `Deposited@2` (a v2 payload shape).
        let event = Event::new(SchemaId::new("Deposited", 2), json!({ "n": 9, "cents": 0 }));

        // When decoding it through the v1 type.
        let decoded: Option<Deposited> = event.decode();

        // Then nothing decodes: version-pinned, no silent corruption.
        assert!(decoded.is_none(), "v2 payload must not decode as v1");
    }

    #[test]
    fn is_matches_without_cloning_or_decoding() {
        // Given two events: one `Deposited@1`, one `Withdrawn@1`.
        let deposited = Deposited { n: 1 }.into_event();
        let withdrawn = Event::new(SchemaId::new("Withdrawn", 1), json!({ "n": 1 }));

        // When asking each whether it IS a Deposited.
        let deposited_is = deposited.is::<Deposited>();
        let withdrawn_is = withdrawn.is::<Deposited>();

        // Then only the exact-schema event matches (pure comparison).
        assert!(deposited_is);
        assert!(!withdrawn_is);
    }

    /// A fact whose serialization fails (injected via a hand-rolled impl).
    #[derive(Clone, Copy)]
    struct Unserializable {
        bad: f64,
    }
    impl Serialize for Unserializable {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::Error as _;
            Err(S::Error::custom("injected serialize failure"))
        }
    }
    impl Schema for Unserializable {
        fn schema_def() -> crate::schema::SchemaDef {
            crate::schema::SchemaDef {
                name: "Unserializable".into(),
                version: 1,
                kind: crate::schema::SchemaKind::Event,
                fields: vec![],
                description: None,
            }
        }
    }

    #[test]
    fn try_into_event_reports_serialize_failure() {
        // Given a fact whose serialization fails.
        let fact = Unserializable { bad: f64::NAN };

        // When converting with the fallible escape hatch.
        let result = fact.try_into_event();

        // Then the failure is reported, not panicked.
        assert!(result.is_err(), "injected failure must surface as Err");

        // When converting with the named-panic path.
        // Then the panic names the schema.
        let panicked = std::panic::catch_unwind(move || fact.into_event());
        let msg = panicked
            .err()
            .and_then(|p| p.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        assert!(
            msg.contains("Unserializable@1"),
            "panic must name the schema, got: {msg}"
        );
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
            .dispatch(&mut state, &json!({ "qty": 4 }), &mut ctx)
            .expect("dispatch");

        // Then the buffer holds exactly the decided events, in order, ready
        // to append as a slice (deref to &[Event], no conversion).
        assert_eq!(events.len(), 1);
        let appended: &[Event] = &events;
        assert_eq!(appended[0].schema, StockReserved::schema_id());
        assert_eq!(appended[0].payload["qty"], 4);
    }

    /// Minimal counter state for the dispatch test above (same shape as the
    /// actor.rs doc fixture).
    #[derive(Serialize, Deserialize, Default)]
    struct Counter {
        count: i64,
    }
    impl crate::actor::EventSourcedActor for Counter {
        fn manifest() -> crate::schema::ActorManifest {
            crate::schema::ActorManifest::new()
        }
        fn restore(_args: &JsonValue) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &Event) {
            if event.schema == StockReserved::schema_id() {
                self.count += event.payload["qty"].as_i64().unwrap_or(0);
            }
        }
    }
    impl CommandHandler<ReserveStock> for Counter {
        fn handle(&self, cmd: ReserveStock, _ctx: &mut CmdCtx<'_>) -> Events {
            Events::one(StockReserved { qty: cmd.qty })
        }
    }

    #[derive(Serialize, Deserialize)]
    struct ReserveStock {
        qty: i64,
    }
    impl Schema for ReserveStock {
        fn schema_def() -> crate::schema::SchemaDef {
            crate::schema::SchemaDef {
                name: "ReserveStock".into(),
                version: 1,
                kind: crate::schema::SchemaKind::Command,
                fields: vec![],
                description: None,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    struct StockReserved {
        qty: i64,
    }
    impl Schema for StockReserved {
        fn schema_def() -> crate::schema::SchemaDef {
            crate::schema::SchemaDef {
                name: "StockReserved".into(),
                version: 1,
                kind: crate::schema::SchemaKind::Event,
                fields: vec![],
                description: None,
            }
        }
    }
}
