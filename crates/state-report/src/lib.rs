//! Serves and consumes system state as zenoh messages, and serves control
//! commands over the same mesh.
//!
//! One key family — [`STATE_KEY`] — carries the whole story: [`install`]
//! declares a queryable that answers every query with a *fresh* export
//! document (captured, journaled through a `StateReporter` actor, then
//! replied), and [`fetch`] queries that key and decodes the export. Both
//! sides are zenoh peers on `Config::default()` — no addresses, no ports.
//!
//! A second key family — [`CONTROL_KEY`] — carries the control plane:
//! [`install_control`] declares a queryable that dispatches
//! [`ControlRequest`]s to an app-registered [`ControlRouter`], answering
//! each with exactly one [`ControlReply`] (result or legible error), and
//! [`send_command`] is the client side. The transport is generic; the
//! command set is an allow-list: only names an app registered at startup
//! can ever run.
//!
//! Test isolation: zenoh's default peer discovery puts every session on
//! the machine (and network) into one mesh, so concurrent tests must not
//! share the production keys. [`StateKey::scoped`] and [`ControlKey::scoped`]
//! derive per-test island keys (`trouper/{state,control}/<scope>`);
//! a query on one island only ever reaches queryables declared on that
//! same island.
//!
//! Freshness contract: a reply is only sent after the reporter's journaled
//! `seq` has advanced past its pre-query value, so what `fetch` decodes is
//! a report of the system as of that query, not the previous one.
//!
//! ```no_run
//! use serde_json::json;
//! use state_report::{ControlCommand, ControlReply, ControlRequest, ControlRouter};
//!
//! // A command is a struct + an impl + one registration line — nothing else.
//! struct Ping;
//!
//! impl ControlCommand for Ping {
//!     fn name(&self) -> &'static str { "Ping" }
//!
//!     async fn execute(
//!         &self,
//!         _system: &trouper::system::ActorSystem,
//!         args: &serde_json::Value,
//!     ) -> Result<serde_json::Value, String> {
//!         let text = args.get("text").and_then(|t| t.as_str()).unwrap_or("ping");
//!         Ok(json!({ "pong": text }))
//!     }
//! }
//!
//! # async fn demo(system: trouper::system::ActorSystem) -> Result<(), Box<dyn std::error::Error>> {
//! // App startup: the allow-list is chosen once, here.
//! let _bridge = state_report::install_control(
//!     system.clone(),
//!     ControlRouter::new().with(Ping),
//! ).await?;
//!
//! // Any client, any process:
//! let reply = state_report::send_command(ControlRequest {
//!     command: "Ping".into(),
//!     args: json!({ "text": "hello" }),
//! }).await?;
//! assert!(matches!(reply, ControlReply::Ok { .. }));
//! # Ok(())
//! # }
//! ```

use std::borrow::Cow;
use std::time::Duration;
use tokio::time::Instant;
use trouper::prelude::*;
use trouper::state_report::ReportState;
use trouper::system::{ActorSystem, SystemExport};

/// Re-exported so consumers of [`install_on`]/[`fetch_on`] can name
/// session types (e.g. to close a bridge session) without adding their
/// own zenoh dependency.
pub use zenoh;

/// The zenoh key every state query travels on.
pub const STATE_KEY: &str = "trouper/state";

/// The zenoh key every control command travels on.
pub const CONTROL_KEY: &str = "trouper/control";

/// The zenoh key a control bridge serves and clients send commands to.
///
/// Production uses [`ControlKey::production`]; tests derive per-test
/// islands with [`ControlKey::scoped`] — the same isolation contract as
/// [`StateKey::scoped`], and for the same reason: control bridges on one
/// mesh must never answer commands meant for another.
#[derive(Debug, Clone)]
pub struct ControlKey(String);

impl ControlKey {
    /// The production key, [`CONTROL_KEY`].
    pub fn production() -> Self {
        Self(CONTROL_KEY.to_string())
    }

    /// A namespaced island key, `trouper/control/<scope>`.
    pub fn scoped(scope: &str) -> Self {
        Self(format!("{CONTROL_KEY}/{scope}"))
    }

    /// The key expression as zenoh sees it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The zenoh key a bridge serves and clients query.
///
/// Production uses [`StateKey::production`]; tests derive per-test
/// islands with [`StateKey::scoped`] so concurrent test processes sharing
/// one zenoh mesh never answer each other's queries.
#[derive(Debug, Clone)]
pub struct StateKey(String);

impl StateKey {
    /// The production key, [`STATE_KEY`].
    pub fn production() -> Self {
        Self(STATE_KEY.to_string())
    }

    /// A namespaced island key, `trouper/state/<scope>`.
    ///
    /// Keys are matched exactly (no wildcards here), so queries on one
    /// scope only reach queryables on the same scope — even though the
    /// underlying zenoh sessions all discover each other.
    pub fn scoped(scope: &str) -> Self {
        Self(format!("{STATE_KEY}/{scope}"))
    }

    /// The key expression as zenoh sees it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// How long the bridge waits for the reporter's `seq` to advance after
/// injecting a `ReportState` command.
const FRESH_BUDGET: Duration = Duration::from_secs(2);

/// Zenoh's per-query timeout on the fetching side.
const GET_TIMEOUT: Duration = Duration::from_millis(1_000);

/// The total fetching budget (including discovery warm-up retries).
const FETCH_BUDGET: Duration = Duration::from_secs(3);

/// Everything that can go wrong while serving or fetching state.
#[derive(Debug, wherror::Error)]
pub enum StateBridgeError {
    /// A zenoh operation failed.
    #[error("zenoh operation failed: {0}")]
    Zenoh(String),
    /// No actor exists at the reporter path — the bridge has nothing to
    /// journal reports through.
    #[error("no actor at {0}: spawn a StateReporter there before installing the bridge")]
    NoReporter(ActorPath),
    /// The fetch budget elapsed before a usable reply arrived.
    #[error("no reply within the {0:?} fetch budget")]
    Timeout(Duration),
    /// The reply payload did not decode into a `SystemExport`.
    #[error("reply was not a decodable SystemExport: {0}")]
    Payload(String),
}

/// Everything that can go wrong on the control client's side — the
/// command transport. Command *outcomes* (including rejections) travel in
/// the [`ControlReply`] itself, not here; these are failures to reach or
/// hear a bridge at all.
#[derive(Debug, wherror::Error)]
pub enum ControlBridgeError {
    /// A zenoh operation failed.
    #[error("zenoh operation failed: {0}")]
    Zenoh(String),
    /// No decodable reply arrived within the send budget.
    #[error("no reply within the {0:?} send budget")]
    Timeout(Duration),
    /// The reply payload did not decode into a `ControlReply`.
    #[error("reply was not a decodable ControlReply: {0}")]
    Payload(String),
}

/// Declares the state queryable for `system` and serves queries until the
/// session is dropped (or the process exits).
///
/// Every query is answered with a fresh [`SystemExport`]: the bridge reads
/// the reporter's `seq`, captures `system.export()`, injects a
/// [`ReportState`] command, waits for the journaled report to land, then
/// replies with the export document. When no reply can be produced (e.g.
/// the reporter stopped), the query is dropped unanswered.
///
/// # Errors
///
/// Returns [`StateBridgeError::NoReporter`] when `reporter` names no live
/// actor, or [`StateBridgeError::Zenoh`] when the session or queryable
/// cannot be created.
pub async fn install(
    system: ActorSystem,
    reporter: ActorPath,
) -> Result<zenoh::Session, StateBridgeError> {
    install_on(StateKey::production(), system, reporter).await
}

/// [`install`] on an explicit [`StateKey`] — the test seam for per-test
/// island keys.
///
/// # Errors
///
/// As [`install`].
pub async fn install_on(
    key: StateKey,
    system: ActorSystem,
    reporter: ActorPath,
) -> Result<zenoh::Session, StateBridgeError> {
    // Given the system already contains the reporter actor.
    if system.inbox_cursor(&reporter).is_none() {
        return Err(StateBridgeError::NoReporter(reporter));
    }

    // When the zenoh session and queryable are declared.
    let session = zenoh::open(zenoh::Config::default()).await.map_err(zoh)?;
    let queryable = session
        .declare_queryable(key.as_str())
        .complete(true)
        .await
        .map_err(zoh)?;

    // Then queries are served for as long as this task lives (it owns the
    // queryable, and its session clone keeps the transport alive even if
    // the caller lets their handle go).
    tokio::spawn(serve_loop(
        session.clone(),
        key,
        queryable,
        system,
        reporter,
    ));
    Ok(session)
}

/// Serves state queries one at a time until the queryable's channel closes.
async fn serve_loop(
    session: zenoh::Session,
    key: StateKey,
    queryable: zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>,
    system: ActorSystem,
    reporter: ActorPath,
) {
    while let Ok(query) = queryable.recv_async().await {
        serve_query(&session, &key, &system, &reporter, query).await;
    }
}

/// Answers one query with a fresh export, or stays silent when the report
/// cannot be produced and journaled.
async fn serve_query(
    _session: &zenoh::Session,
    key: &StateKey,
    system: &ActorSystem,
    reporter: &ActorPath,
    query: zenoh::query::Query,
) {
    // The freshness watermark: whatever the reporter recorded BEFORE us.
    let before = reported_seq(system, reporter).await;

    let export = system.export().await;
    let doc = match serde_json::to_value(&export) {
        Ok(doc) => doc,
        Err(error) => {
            tracing::error!("export serialization failed: {error}");
            return;
        }
    };

    let command = serde_json::json!({ "export": doc });
    if let Err(undelivered) = system
        .send(system.envelope(ReportState::schema_id(), reporter.clone(), command))
        .await
    {
        tracing::error!(
            "ReportState to {reporter} was not delivered (dest: {})",
            undelivered.dest
        );
        return;
    }

    // Only reply once the report has actually been journaled.
    if !wait_for_seq(system, reporter, before).await {
        tracing::error!("reporter at {reporter} never recorded the report; not replying");
        return;
    }

    let body = match serde_json::to_string(&doc) {
        Ok(body) => body,
        Err(error) => {
            tracing::error!("report serialization failed: {error}");
            return;
        }
    };
    if let Err(error) = query.reply(key.as_str(), body).await {
        tracing::error!("zenoh reply failed: {error}");
        return;
    }
    tracing::info!("served a state query");
}

/// The reporter's current journaled report count (`seq`).
async fn reported_seq(system: &ActorSystem, reporter: &ActorPath) -> u64 {
    system
        .es_state(reporter)
        .await
        .and_then(|state| state["seq"].as_u64())
        .unwrap_or(0)
}

/// Polls until the reporter's `seq` advances past `before`.
async fn wait_for_seq(system: &ActorSystem, reporter: &ActorPath, before: u64) -> bool {
    let deadline = Instant::now() + FRESH_BUDGET;
    while Instant::now() < deadline {
        if reported_seq(system, reporter).await > before {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    false
}

/// Fetches one fresh export from whichever bridge is answering
/// [`STATE_KEY`], retrying within the budget to ride out zenoh's
/// peer-discovery warm-up.
///
/// # Errors
///
/// Returns [`StateBridgeError::Timeout`] when nothing usable arrives within
/// [`FETCH_BUDGET`], [`StateBridgeError::Zenoh`] for transport failures, or
/// [`StateBridgeError::Payload`] when a reply does not decode.
pub async fn fetch() -> Result<SystemExport, StateBridgeError> {
    fetch_on(StateKey::production()).await
}

/// [`fetch`] from an explicit [`StateKey`] — the test seam for per-test
/// island keys.
///
/// # Errors
///
/// As [`fetch`].
pub async fn fetch_on(key: StateKey) -> Result<SystemExport, StateBridgeError> {
    let session = zenoh::open(zenoh::Config::default()).await.map_err(zoh)?;
    // Whatever happens, leave the mesh gracefully — an abruptly vanished
    // peer can poison routing state in every other session on the network.
    let result = fetch_all(&session, &key).await;
    if let Err(error) = session.close().await {
        tracing::warn!("fetch session close failed: {error}");
    }
    result
}

/// The retry loop of [`fetch_on`] against an open session.
async fn fetch_all(
    session: &zenoh::Session,
    key: &StateKey,
) -> Result<SystemExport, StateBridgeError> {
    let deadline = Instant::now() + FETCH_BUDGET;
    while Instant::now() < deadline {
        match first_export(session, key, deadline).await {
            Ok(export) => return Ok(export),
            Err(FetchMiss::Retry) => continue,
            Err(FetchMiss::Fatal(error)) => return Err(error),
        }
    }
    Err(StateBridgeError::Timeout(FETCH_BUDGET))
}

/// Why one query attempt produced no export: worth another try, or fatal.
enum FetchMiss {
    /// The query closed without a usable reply — scouting may still be
    /// settling; try again.
    Retry,
    /// Stop fetching and surface this.
    Fatal(StateBridgeError),
}

/// One query attempt: sends the query and decodes the first usable reply.
async fn first_export(
    session: &zenoh::Session,
    key: &StateKey,
    deadline: Instant,
) -> Result<SystemExport, FetchMiss> {
    let replies = session
        .get(key.as_str())
        .timeout(GET_TIMEOUT)
        .await
        .map_err(zoh)
        .map_err(FetchMiss::Fatal)?;
    loop {
        match tokio::time::timeout_at(deadline, replies.recv_async()).await {
            Ok(Ok(reply)) => match decode(reply) {
                Ok(doc) => return Ok(doc),
                // Not ours / not decodable: keep the loop open for the
                // next reply until the budget runs out.
                Err(error @ StateBridgeError::Payload(_)) => {
                    tracing::warn!("undecodable reply on the state key: {error}")
                }
                Err(error) => return Err(FetchMiss::Fatal(error)),
            },
            // The query completed with nothing decodable in it.
            Ok(Err(_closed)) => return Err(FetchMiss::Retry),
            // The whole budget is gone.
            Err(_elapsed) => return Err(FetchMiss::Fatal(StateBridgeError::Timeout(FETCH_BUDGET))),
        }
    }
}

/// Decodes one zenoh reply into a `SystemExport`.
fn decode(reply: zenoh::query::Reply) -> Result<SystemExport, StateBridgeError> {
    let sample = reply.result().map_err(|error| {
        let detail = error
            .payload()
            .try_to_string()
            .map(Cow::into_owned)
            .unwrap_or_default();
        StateBridgeError::Zenoh(format!("queryable replied with an error: {detail}"))
    })?;
    let text = sample
        .payload()
        .try_to_string()
        .map_err(|error| StateBridgeError::Zenoh(format!("payload was not utf-8: {error}")))?;
    serde_json::from_str(text.as_ref())
        .map_err(|error| StateBridgeError::Payload(error.to_string()))
}

/// Converts a zenoh error into the bridge's Zenoh variant.
fn zoh<E: std::fmt::Display>(error: E) -> StateBridgeError {
    StateBridgeError::Zenoh(error.to_string())
}

/// Converts a zenoh error into the control bridge's Zenoh variant.
fn czoh<E: std::fmt::Display>(error: E) -> ControlBridgeError {
    ControlBridgeError::Zenoh(error.to_string())
}

/// The wire envelope of every control command.
///
/// `command` selects among the app-registered allow-list; `args` is the
/// command's argument document, decoded (and shape-checked) by the
/// command's own implementation — the transport never inspects it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ControlRequest {
    /// The registered command name (the [`ControlRouter`] key).
    pub command: String,
    /// The command's arguments, as JSON.
    pub args: serde_json::Value,
}

impl ControlRequest {
    /// A request with no arguments (`Null` — commands treat it as "none").
    pub fn bare(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            args: serde_json::Value::Null,
        }
    }
}

/// What the control bridge replies with: exactly one of a result or an
/// error. A dropped command surfaces as a client-side timeout instead —
/// silence is never a command outcome.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ControlReply {
    /// The command ran; this is its result document.
    Ok {
        /// The command's result.
        result: serde_json::Value,
    },
    /// The command did not run, or ran and failed; this is why.
    Err {
        /// A legible reason.
        error: String,
    },
}

impl ControlReply {
    /// The result of a successful command, for tests and callers that
    /// treat any error as fatal.
    pub fn result(self) -> Option<serde_json::Value> {
        match self {
            ControlReply::Ok { result } => Some(result),
            ControlReply::Err { .. } => None,
        }
    }
}

/// A control-plane operation: typed arguments, privileged execution.
///
/// The transport is generic — this trait is the entire extension surface.
/// A command owns its argument decoding (shape errors are its own typed
/// outcome, reported before any effect) and receives only the
/// [`ActorSystem`] handle that app code is already trusted with.
///
/// Extension story: a struct + this impl + one
/// [`ControlRouter::with`] registration line. Nothing else changes.
///
/// (`execute` uses RPITIT so impls can be plain `async fn` bodies; that
/// makes the trait not dyn-compatible, so the router stores commands
/// behind the dyn-compatible [`DynCommand`] carrier instead of
/// `dyn ControlCommand` — see [`ControlRouter::with`].)
pub trait ControlCommand: Send + Sync {
    /// The registered name — the wire's `command` field. Reserved: `List`
    /// (see [`LIST_COMMAND`]).
    fn name(&self) -> &'static str;

    /// Decodes `args` and runs the command against `system`.
    ///
    /// Returning `Err` turns into a [`ControlReply::Err`] — the command
    /// should put a legible reason in it, since it is what the operator
    /// sees.
    fn execute(
        &self,
        system: &ActorSystem,
        args: &serde_json::Value,
    ) -> impl Future<Output = Result<serde_json::Value, String>> + Send;
}

/// The dyn-compatible carrier the router stores: same surface as
/// [`ControlCommand`], with a boxed future instead of RPITIT. Not part
/// of the public extension story — commands implement [`ControlCommand`];
/// the blanket impls below make every such command (or a shared
/// `Arc` of one) usable as a carrier.
pub trait DynCommand: Send + Sync {
    /// As [`ControlCommand::name`].
    fn name(&self) -> &'static str;

    /// As [`ControlCommand::execute`].
    fn execute<'a>(
        &'a self,
        system: &'a ActorSystem,
        args: &'a serde_json::Value,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
    >;
}

impl<T: ControlCommand + ?Sized> DynCommand for T {
    fn name(&self) -> &'static str {
        ControlCommand::name(self)
    }

    fn execute<'a>(
        &'a self,
        system: &'a ActorSystem,
        args: &'a serde_json::Value,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
    > {
        Box::pin(ControlCommand::execute(self, system, args))
    }
}

impl<C: DynCommand + ?Sized> DynCommand for std::sync::Arc<C> {
    fn name(&self) -> &'static str {
        (**self).name()
    }

    fn execute<'a>(
        &'a self,
        system: &'a ActorSystem,
        args: &'a serde_json::Value,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
    > {
        (**self).execute(system, args)
    }
}

/// The reserved command name for asking a bridge what it can do.
pub const LIST_COMMAND: &str = "List";

/// The app-side allow-list: only registered names are invocable.
///
/// Built at startup with [`ControlRouter::with`] — each `with` rejects
/// duplicates and the reserved [`LIST_COMMAND`] immediately, so a name
/// collision fails fast instead of silently shadowing.
#[derive(Default)]
pub struct ControlRouter {
    commands: std::collections::HashMap<String, std::sync::Arc<dyn DynCommand>>,
}

impl std::fmt::Debug for ControlRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlRouter")
            .field("commands", &self.names())
            .finish()
    }
}

impl ControlRouter {
    /// An empty router.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `command`, rejecting duplicate names and the reserved
    /// [`LIST_COMMAND`].
    ///
    /// # Panics
    ///
    /// Panics on a duplicate or reserved name — registration is app
    /// startup code, and a collision is a programming error, not runtime
    /// input.
    #[must_use]
    pub fn with(mut self, command: impl DynCommand + 'static) -> Self {
        self.register(command);
        self
    }

    /// Registers `command` on an existing router.
    ///
    /// # Panics
    ///
    /// As [`ControlRouter::with`].
    pub fn register(&mut self, command: impl DynCommand + 'static) {
        let name = command.name();
        assert!(
            name != LIST_COMMAND,
            "control command name `{name}` is reserved"
        );
        let previous = self
            .commands
            .insert(name.to_string(), std::sync::Arc::new(command));
        assert!(
            previous.is_none(),
            "control command name `{name}` registered twice"
        );
    }

    /// The registered command names, sorted for stable output.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.commands.keys().cloned().collect();
        names.sort();
        names
    }

    /// The router's immutable, registration-closed form — what a bridge
    /// serves. Late registration is impossible by design.
    pub fn finish(self) -> FinishedRouter {
        FinishedRouter(self)
    }

    /// Dispatches one request: look up, run, and always answer.
    ///
    /// The lookup happens before anything runs, so an unknown command
    /// never reaches a handler; `List` is answered from the router
    /// itself.
    pub async fn dispatch(&self, system: &ActorSystem, request: ControlRequest) -> ControlReply {
        if request.command == LIST_COMMAND {
            return ControlReply::Ok {
                result: serde_json::json!({ "commands": self.names() }),
            };
        }
        let Some(command) = self.commands.get(&request.command) else {
            return ControlReply::Err {
                error: format!("no such command: {}", request.command),
            };
        };
        match command.execute(system, &request.args).await {
            Ok(result) => ControlReply::Ok { result },
            Err(error) => ControlReply::Err { error },
        }
    }
}

/// A [`ControlRouter`] whose registration phase is over: the allow-list
/// is fixed for the life of the bridge. Built by [`ControlRouter::finish`].
#[derive(Default)]
pub struct FinishedRouter(ControlRouter);

impl std::fmt::Debug for FinishedRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FinishedRouter").field(&self.0).finish()
    }
}

/// The budget for one control round trip (send → bridge dispatch →
/// reply). Pool installation runs inside this window, so it must cover
/// the slowest registered command; a pool spawn is milliseconds.
const SEND_BUDGET: Duration = Duration::from_secs(10);

/// Declares the control queryable for `system` and serves commands until
/// the session is dropped (or the process exits).
///
/// # Errors
///
/// As [`install_control_on`].
pub async fn install_control(
    system: ActorSystem,
    router: ControlRouter,
) -> Result<zenoh::Session, ControlBridgeError> {
    install_control_on(ControlKey::production(), system, router).await
}

/// [`install_control`] on an explicit [`ControlKey`] — the test seam for
/// per-test island keys.
///
/// Every query is answered with exactly one [`ControlReply`]: unknown
/// commands, undecodable envelopes, and failed handlers all produce an
/// error reply rather than silence. The router is consumed into its
/// [`finished`](ControlRouter::finish) form; duplicate/reserved names
/// have already failed fast by then.
///
/// # Errors
///
/// Returns [`ControlBridgeError::Zenoh`] when the session or queryable
/// cannot be created.
pub async fn install_control_on(
    key: ControlKey,
    system: ActorSystem,
    router: ControlRouter,
) -> Result<zenoh::Session, ControlBridgeError> {
    let router = std::sync::Arc::new(router.finish());
    // When the zenoh session and queryable are declared.
    let session = zenoh::open(zenoh::Config::default()).await.map_err(czoh)?;
    let queryable = session
        .declare_queryable(key.as_str())
        .complete(true)
        .await
        .map_err(czoh)?;

    // Then commands are served for as long as this task lives (it owns
    // the queryable, and its session clone keeps the transport alive even
    // if the caller lets their handle go).
    tokio::spawn(control_loop(key, queryable, system, router));
    Ok(session)
}

/// Serves control commands one at a time until the queryable's channel
/// closes.
async fn control_loop(
    key: ControlKey,
    queryable: zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>,
    system: ActorSystem,
    router: std::sync::Arc<FinishedRouter>,
) {
    while let Ok(query) = queryable.recv_async().await {
        serve_command(&key, &system, &router, query).await;
    }
}

/// Answers one control query: decode the envelope, dispatch, reply once.
///
/// Every failure mode below the transport produces an error reply — a
/// command that cannot run must still be heard, because silence on a
/// mutating command reads as success.
async fn serve_command(
    key: &ControlKey,
    system: &ActorSystem,
    router: &FinishedRouter,
    query: zenoh::query::Query,
) {
    let reply = match query.payload() {
        // The envelope is the transport's to decode; a malformed one is
        // an error reply, not a dropped query.
        None => ControlReply::Err {
            error: "malformed request: query had no payload".to_string(),
        },
        Some(payload) => match payload.try_to_string() {
            Err(error) => ControlReply::Err {
                error: format!("malformed request: payload was not utf-8: {error}"),
            },
            Ok(text) => match serde_json::from_str::<ControlRequest>(text.as_ref()) {
                Ok(request) => router.0.dispatch(system, request).await,
                Err(error) => ControlReply::Err {
                    error: format!("malformed request: {error}"),
                },
            },
        },
    };

    let body = match serde_json::to_string(&reply) {
        Ok(body) => body,
        Err(error) => {
            // Unreachable for these types, but silence is still not an
            // option: answer with the plainest legible error.
            tracing::error!("control reply serialization failed: {error}");
            r#"{"outcome":"err","error":"bridge could not serialize its reply"}"#.to_string()
        }
    };
    if let Err(error) = query.reply(key.as_str(), body).await {
        tracing::error!("zenoh reply failed: {error}");
    }
}

/// Sends one control command to whichever bridge is answering `key` and
/// decodes the first reply.
///
/// Unlike [`fetch_on`], this is strictly one-shot: no discovery retries,
/// because a command is not idempotent and a timeout must mean "nothing
/// answered in time", not "maybe it ran twice".
///
/// # Errors
///
/// Returns [`ControlBridgeError::Timeout`] when no reply arrives within
/// [`SEND_BUDGET`], [`ControlBridgeError::Zenoh`] for transport failures,
/// or [`ControlBridgeError::Payload`] when the reply does not decode into
/// a [`ControlReply`]. A command that *ran and failed* is not an error
/// here — it is a [`ControlReply::Err`].
pub async fn send_command_on(
    key: ControlKey,
    request: ControlRequest,
) -> Result<ControlReply, ControlBridgeError> {
    let session = zenoh::open(zenoh::Config::default()).await.map_err(czoh)?;
    // Whatever happens, leave the mesh gracefully — an abruptly vanished
    // peer can poison routing state in every other session on the network.
    let result = first_reply(&session, &key, request).await;
    if let Err(error) = session.close().await {
        tracing::warn!("send_command session close failed: {error}");
    }
    result
}

/// [`send_command_on`] on the production [`CONTROL_KEY`].
///
/// # Errors
///
/// As [`send_command_on`].
pub async fn send_command(request: ControlRequest) -> Result<ControlReply, ControlBridgeError> {
    send_command_on(ControlKey::production(), request).await
}

/// One control round trip against an open session: query with payload,
/// take the first decodable reply.
async fn first_reply(
    session: &zenoh::Session,
    key: &ControlKey,
    request: ControlRequest,
) -> Result<ControlReply, ControlBridgeError> {
    let payload = serde_json::to_vec(&request)
        .map_err(|error| ControlBridgeError::Payload(error.to_string()))?;
    let deadline = Instant::now() + SEND_BUDGET;
    let replies = session
        .get(key.as_str())
        .payload(payload)
        .timeout(SEND_BUDGET)
        .await
        .map_err(czoh)?;
    match tokio::time::timeout_at(deadline, replies.recv_async()).await {
        Ok(Ok(reply)) => decode_reply(reply),
        // The query completed with nothing in it, or the budget elapsed.
        Ok(Err(_)) | Err(_) => Err(ControlBridgeError::Timeout(SEND_BUDGET)),
    }
}

/// Decodes one zenoh reply into a [`ControlReply`].
fn decode_reply(reply: zenoh::query::Reply) -> Result<ControlReply, ControlBridgeError> {
    let sample = reply.result().map_err(|error| {
        let detail = error
            .payload()
            .try_to_string()
            .map(Cow::into_owned)
            .unwrap_or_default();
        ControlBridgeError::Zenoh(format!("queryable replied with an error: {detail}"))
    })?;
    let text = sample
        .payload()
        .try_to_string()
        .map_err(|error| ControlBridgeError::Zenoh(format!("payload was not utf-8: {error}")))?;
    serde_json::from_str(text.as_ref())
        .map_err(|error| ControlBridgeError::Payload(error.to_string()))
}

/// A factory closure: spawns ONE worker at the path the kernel gives it.
/// The factory owns the actor type; the kernel owns the naming.
pub type PoolFactory =
    std::sync::Arc<dyn Fn(&ActorSystem, &ActorPath, &serde_json::Value) + Send + Sync>;

/// The code half of a scalable pool, bound by the app at startup.
///
/// A factory closure cannot cross a wire — so the app fixes everything
/// that is *code or policy* (the actor type behind the factory, the
/// public name, the algo, supervision, seed, genesis args) here, and the
/// wire carries only the variable half: `workers`.
#[derive(Clone)]
pub struct PoolBlueprint {
    /// The public path senders address (claimed by the pool).
    pub public: ActorPath,
    /// The worker-selection algorithm.
    pub algo: trouper::pool::PoolAlgo,
    /// The supervised parent workers spawn under (escalation flows
    /// worker → parent); `None` = parentless workers.
    pub parent: Option<ActorPath>,
    /// The pool's PRNG seed (injectable for deterministic tests).
    pub seed: u64,
    /// Genesis args handed to the factory (the workers' shared config).
    pub args: Option<serde_json::Value>,
    /// Spawns ONE worker at the path the kernel gives it — a typed
    /// builder/positional spawn inside a closure.
    pub factory: PoolFactory,
}

impl std::fmt::Debug for PoolBlueprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolBlueprint")
            .field("public", &self.public)
            .field("algo", &self.algo)
            .field("parent", &self.parent)
            .field("seed", &self.seed)
            .field("args", &self.args)
            .finish_non_exhaustive()
    }
}

/// The app-side registry of scalable pool kinds: kind name → blueprint.
#[derive(Debug, Default, Clone)]
pub struct Blueprints(std::collections::HashMap<String, PoolBlueprint>);

impl Blueprints {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `blueprint` under `kind`.
    #[must_use]
    pub fn with_kind(mut self, kind: impl Into<String>, blueprint: PoolBlueprint) -> Self {
        self.0.insert(kind.into(), blueprint);
        self
    }

    /// The blueprint registered under `kind`, if any.
    pub fn get(&self, kind: &str) -> Option<&PoolBlueprint> {
        self.0.get(kind)
    }

    /// The registered kind names, sorted for stable output.
    pub fn kinds(&self) -> Vec<String> {
        let mut kinds: Vec<String> = self.0.keys().cloned().collect();
        kinds.sort();
        kinds
    }
}

/// The wire shape of a `ScalePool` command's arguments.
#[derive(Debug, serde::Deserialize)]
pub struct ScalePoolArgs {
    /// Which registered blueprint to (re)install.
    pub kind: String,
    /// The worker count to scale to.
    pub workers: usize,
}

/// Grows (or re-installs) a registered pool kind to a worker count.
///
/// Holds its [`Blueprints`] so it is self-contained: constructing it at
/// app startup and registering it is the whole integration. Installing
/// over a live pool is a takeover — all workers are stop-drained and
/// respawned (worker identities do not survive a rescale; in-flight
/// envelopes land in the DLQ per the stop contract).
pub struct ScalePoolCmd {
    blueprints: Blueprints,
}

impl ScalePoolCmd {
    /// Binds the registry this command scales from.
    pub fn new(blueprints: Blueprints) -> Self {
        Self { blueprints }
    }
}

impl ControlCommand for ScalePoolCmd {
    fn name(&self) -> &'static str {
        "ScalePool"
    }

    async fn execute(
        &self,
        system: &ActorSystem,
        args: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        // Decode the wire's variable half before anything runs.
        let args: ScalePoolArgs = serde_json::from_value(args.clone()).map_err(|error| {
            format!("args must be {{\"kind\": <string>, \"workers\": <int ≥ 1>}}: {error}")
        })?;
        if args.workers == 0 {
            return Err("workers must be >= 1".to_string());
        }
        let blueprint = self.blueprints.get(&args.kind).ok_or_else(|| {
            format!(
                "no such pool kind: {} (registered: {:?})",
                args.kind,
                self.blueprints.kinds()
            )
        })?;

        // A rescale of a LIVE pool must not spawn over the old workers'
        // paths (the install takeover stop-drains only the public path's
        // holder — the workers are separate registrations). Drain the
        // current generation first, discovered from the live export.
        drain_pool_workers(system, &blueprint.public).await;

        let spec = trouper::pool::PoolSpec {
            public: blueprint.public.clone(),
            workers: args.workers,
            algo: blueprint.algo,
            factory: blueprint.factory.clone(),
            args: blueprint.args.clone(),
            parent: blueprint.parent.clone(),
            seed: blueprint.seed,
        };
        system
            .install_pool(spec)
            .await
            .map_err(|report| format!("pool install failed at {}: {report}", blueprint.public))?;
        Ok(serde_json::json!({
            "kind": args.kind,
            "public": blueprint.public.as_str(),
            "workers": args.workers,
            "algo": blueprint.algo.name(),
        }))
    }
}

/// Stop-drains the workers of a live pool at `public`, if one exists.
///
/// `install_pool`'s takeover handles only the public path itself; the
/// workers own their own slots and would make a re-install die on
/// `PathTaken`. Discovers the live worker list from the export so the
/// runtime's pool entry stays the source of truth.
async fn drain_pool_workers(system: &ActorSystem, public: &ActorPath) {
    let workers: Vec<ActorPath> = system
        .export()
        .await
        .pools
        .iter()
        .filter(|pool| pool.path == *public)
        .flat_map(|pool| pool.workers.iter().cloned())
        .collect();
    for worker in workers {
        system.stop(&worker).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A minimal command for exercising the router without any system
    /// effects: validates `{"text": …}` first, then "acts" (echoes it
    /// back and counts the act). Validation happens before the counted
    /// effect, mirroring how real commands must decode before they do
    /// anything.
    struct Echo {
        acts: AtomicU32,
    }

    impl Echo {
        fn new() -> Self {
            Self {
                acts: AtomicU32::new(0),
            }
        }

        fn act_count(&self) -> u32 {
            self.acts.load(Ordering::Relaxed)
        }
    }

    impl ControlCommand for Echo {
        fn name(&self) -> &'static str {
            "Echo"
        }

        async fn execute(
            &self,
            _system: &ActorSystem,
            args: &serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            // Decode first — a shape error must never reach the effect.
            let text = args
                .get("text")
                .and_then(|t| t.as_str())
                .ok_or_else(|| "args must be {\"text\": <string>}".to_string())?;
            self.acts.fetch_add(1, Ordering::Relaxed);
            Ok(json!({ "echoed": text }))
        }
    }

    /// A second distinct command, so `List` output is proven to reflect
    /// the actual registration set.
    struct Noop;

    impl ControlCommand for Noop {
        fn name(&self) -> &'static str {
            "Noop"
        }

        async fn execute(
            &self,
            _system: &ActorSystem,
            _args: &serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            Ok(json!({}))
        }
    }

    fn test_system() -> ActorSystem {
        ActorSystem::new(SystemConfig::production())
    }

    fn dispatch(router: &ControlRouter, request: ControlRequest) -> ControlReply {
        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(router.dispatch(&test_system(), request))
    }

    #[test]
    fn registered_command_round_trips_to_its_result() {
        // Given a router with the Echo command registered.
        let echo = std::sync::Arc::new(Echo::new());
        let router = ControlRouter::new().with(echo.clone());

        // When dispatching a well-formed Echo request.
        let reply = dispatch(
            &router,
            ControlRequest {
                command: "Echo".into(),
                args: json!({ "text": "ping" }),
            },
        );

        // Then the reply carries the command's own result.
        assert_eq!(
            reply.result(),
            Some(json!({ "echoed": "ping" })),
            "a registered command's Ok reply is its handler result"
        );
        // And the handler ran exactly once.
        assert_eq!(echo.act_count(), 1);
    }

    #[test]
    fn unknown_command_yields_error_reply_without_running_anything() {
        // Given a router with Echo registered.
        let echo = std::sync::Arc::new(Echo::new());
        let router = ControlRouter::new().with(echo.clone());

        // When dispatching a name nothing registered.
        let reply = dispatch(&router, ControlRequest::bare("Nope"));

        // Then the reply is an error naming the unknown command.
        match &reply {
            ControlReply::Err { error } => {
                assert!(
                    error.contains("no such command") && error.contains("Nope"),
                    "error should name the unknown command, got: {error}"
                );
            }
            other => panic!("expected Err reply, got {other:?}"),
        }
        // And no handler ran.
        assert_eq!(echo.act_count(), 0, "unknown names never reach a handler");
    }

    #[test]
    fn malformed_args_yield_error_reply_and_handler_never_acts() {
        // Given a router with Echo registered, and a request whose args
        // have the wrong shape.
        let echo = std::sync::Arc::new(Echo::new());
        let router = ControlRouter::new().with(echo.clone());

        // When dispatching Echo with args lacking the `text` field.
        let reply = dispatch(
            &router,
            ControlRequest {
                command: "Echo".into(),
                args: json!({ "wrong": true }),
            },
        );

        // Then the reply is a decode error from the command itself.
        match &reply {
            ControlReply::Err { error } => {
                assert!(
                    error.contains("text"),
                    "error should describe the expected shape, got: {error}"
                );
            }
            other => panic!("expected Err reply, got {other:?}"),
        }
        // And the handler decoded the args but never acted.
        assert_eq!(
            echo.act_count(),
            0,
            "a decode failure must not cause effects"
        );
    }

    #[test]
    fn list_returns_registered_command_names() {
        // Given a router with two commands registered.
        let router = ControlRouter::new().with(Echo::new()).with(Noop);

        // When dispatching the reserved List command.
        let reply = dispatch(&router, ControlRequest::bare(LIST_COMMAND));

        // Then the reply lists exactly the registered names, sorted.
        assert_eq!(
            reply.result(),
            Some(json!({ "commands": ["Echo", "Noop"] })),
            "List reflects the registration set"
        );
    }

    #[test]
    fn list_is_reserved_against_user_registration() {
        // Given a command named like the reserved List.
        struct List;
        impl ControlCommand for List {
            fn name(&self) -> &'static str {
                LIST_COMMAND
            }
            async fn execute(
                &self,
                _system: &ActorSystem,
                _args: &serde_json::Value,
            ) -> Result<serde_json::Value, String> {
                Ok(json!({}))
            }
        }

        // When registering it.
        // Then registration panics — the name is reserved.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = ControlRouter::new().with(List);
        }));
        assert!(result.is_err(), "registering `List` must fail fast");
    }

    #[test]
    fn duplicate_registration_fails_fast() {
        // Given a router with Noop registered.
        // When registering Noop again.
        // Then the second registration panics instead of shadowing.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = ControlRouter::new().with(Noop).with(Noop);
        }));
        assert!(result.is_err(), "duplicate registration must fail fast");
    }

    #[test]
    fn control_key_scoping_families_from_state() {
        // Given/When deriving keys for the same scope name.
        // Then control islands live under the control family, never the
        // state family.
        assert_eq!(ControlKey::scoped("t").as_str(), "trouper/control/t");
        assert_eq!(StateKey::scoped("t").as_str(), "trouper/state/t");
        assert_eq!(ControlKey::production().as_str(), CONTROL_KEY);
    }

    // ----- ScalePool over a real system (tests 5-6) ------------------------

    use trouper::actor::{CommandHandler, EventSourcedActor};
    use trouper::tap::FactKind;

    /// The harness command: `{"n": int}`.
    #[derive(serde::Deserialize)]
    struct Probe {
        n: i64,
    }

    impl Schema for Probe {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "ScaleProbe".into(),
                version: 1,
                kind: SchemaKind::Command,
                fields: vec![FieldDef::required("n", FieldTy::Int)],
                description: None,
            }
        }
    }

    /// The harness event: `{"n": int}`.
    #[derive(serde::Deserialize)]
    struct Probed {
        #[allow(dead_code)] // payload shape; assertions read the tap, not it
        n: i64,
    }

    impl Schema for Probed {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "ScaleProbed".into(),
                version: 1,
                kind: SchemaKind::Event,
                fields: vec![FieldDef::required("n", FieldTy::Int)],
                description: None,
            }
        }
    }

    /// The harness worker: totals `Probe` counts (content is irrelevant —
    /// the assertions read the delivery tap).
    #[derive(serde::Serialize, serde::Deserialize, Default)]
    struct ProbeWorker {
        total: i64,
    }

    impl EventSourcedActor for ProbeWorker {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Probe>()
                .emits::<Probed>()
                .kind(ActorKind::EventSourced)
        }
        fn restore(_args: &serde_json::Value) -> Self {
            Self::default()
        }
        fn apply(&mut self, event: &Event) {
            self.total += event.payload["n"].as_i64().unwrap_or(0);
        }
    }

    impl CommandHandler<Probe> for ProbeWorker {
        fn handle(&self, cmd: Probe, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
            vec![Event::new(Probed::schema_id(), json!({ "n": cmd.n }))]
        }
    }

    /// A fresh system with the harness schemas registered and a `Probe`
    /// blueprint named `probe` at the public path `scale.me` (RoundRobin,
    /// seed 42, parentless).
    fn probe_blueprint_system() -> ActorSystem {
        let system = test_system();
        system.register_schema::<Probe>();
        system.register_schema::<Probed>();
        system
    }

    fn probe_blueprints() -> Blueprints {
        probe_blueprints_under(None)
    }

    /// The `probe` blueprint with an optional supervised parent.
    fn probe_blueprints_under(parent: Option<ActorPath>) -> Blueprints {
        Blueprints::new().with_kind(
            "probe",
            PoolBlueprint {
                public: ActorPath::new("scale.me"),
                algo: trouper::pool::PoolAlgo::RoundRobin,
                parent,
                seed: 42,
                args: Some(json!({})),
                factory: std::sync::Arc::new(|system, path, args| {
                    trouper::builder::spawn_es_builder::<ProbeWorker>(system)
                        .at(path.clone())
                        .args(args.clone())
                        .handles::<Probe>()
                        .emits::<Probed>()
                        .start();
                }),
            },
        )
    }

    /// Sends `count` probes to a pool's public path (any blueprint —
    /// they differ only in supervision).
    async fn send_probes_to(system: &ActorSystem, public: &str, count: usize) {
        for i in 0..count {
            system
                .send(system.envelope(
                    Probe::schema_id(),
                    ActorPath::new(public),
                    json!({ "n": i }),
                ))
                .await
                .expect("probe delivered to the pool");
        }
    }

    async fn send_probes(system: &ActorSystem, count: usize) {
        send_probes_to(system, "scale.me", count).await;
    }

    /// Counts `Delivered` facts addressed to a specific worker path.
    fn delivered_to(system: &ActorSystem, path: &str) -> usize {
        system
            .tap_facts()
            .iter()
            .filter(|f| matches!(&f.kind, FactKind::Delivered { to, .. } if to.as_str() == path))
            .count()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scale_pool_takes_over_a_plain_path_and_round_robins() {
        // Given a live system where a plain (non-pool) actor holds the
        // blueprint's public path and has served one request.
        let system = probe_blueprint_system();
        trouper::builder::spawn_es_builder::<ProbeWorker>(&system)
            .at(ActorPath::new("scale.me"))
            .args(json!({}))
            .handles::<Probe>()
            .emits::<Probed>()
            .start();
        system
            .send(system.envelope(
                Probe::schema_id(),
                ActorPath::new("scale.me"),
                json!({ "n": 99 }),
            ))
            .await
            .expect("probe delivered to the plain actor");
        while system
            .inbox_cursor(&ActorPath::new("scale.me"))
            .map(|c| c.as_u64())
            != Some(1)
        {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        // When scaling the kind through the router (the command under
        // test is exercised exactly as the wire exercises it).
        let router = ControlRouter::new().with(ScalePoolCmd::new(probe_blueprints()));
        let reply = router
            .dispatch(
                &system,
                ControlRequest {
                    command: "ScalePool".into(),
                    args: json!({ "kind": "probe", "workers": 3 }),
                },
            )
            .await;

        // Then the reply is the factual summary of the new pool.
        assert_eq!(
            reply.result(),
            Some(json!({
                "kind": "probe",
                "public": "scale.me",
                "workers": 3,
                "algo": "round-robin",
            })),
            "ScalePool replies with the pool summary"
        );

        // And the public path is now a pool routing to three workers.
        send_probes(&system, 6).await;
        while delivered_to(&system, "scale.me/worker-0")
            + delivered_to(&system, "scale.me/worker-1")
            + delivered_to(&system, "scale.me/worker-2")
            < 6
        {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        for worker in [
            "scale.me/worker-0",
            "scale.me/worker-1",
            "scale.me/worker-2",
        ] {
            assert_eq!(
                delivered_to(&system, worker),
                2,
                "round-robin distributes evenly across {worker}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scale_pool_reinstall_replaces_workers_and_resumes_routing() {
        // Given a system whose pool at `scale.me` runs three workers and
        // has served three probes.
        let system = probe_blueprint_system();
        let router = ControlRouter::new().with(ScalePoolCmd::new(probe_blueprints()));
        let first = router
            .dispatch(
                &system,
                ControlRequest {
                    command: "ScalePool".into(),
                    args: json!({ "kind": "probe", "workers": 3 }),
                },
            )
            .await;
        assert!(first.result().is_some(), "first scale succeeds");
        send_probes(&system, 3).await;
        while (0..3)
            .map(|i| delivered_to(&system, &format!("scale.me/worker-{i}")))
            .sum::<usize>()
            < 3
        {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        // The per-worker delivery counts generation one leaves behind.
        // A rescale respawns workers at the SAME paths (`public/worker-N`
        // — the name is derived from the public path, not a unique id),
        // so later assertions must be deltas, not totals.
        let before: Vec<usize> = (0..3)
            .map(|i| delivered_to(&system, &format!("scale.me/worker-{i}")))
            .collect();

        // When scaling the SAME kind again (the takeover path over a live
        // pool — the rescale case).
        let reply = router
            .dispatch(
                &system,
                ControlRequest {
                    command: "ScalePool".into(),
                    args: json!({ "kind": "probe", "workers": 2 }),
                },
            )
            .await;

        // Then the rescale succeeds.
        assert_eq!(
            reply.result(),
            Some(json!({
                "kind": "probe",
                "public": "scale.me",
                "workers": 2,
                "algo": "round-robin",
            })),
            "a rescale of a live pool succeeds"
        );

        // And routing resumes — across the new pool's two slots only.
        // Worker-2 is outside the new pool: its delivery count must stay
        // frozen at the generation-one value, proving the old slot list
        // is gone. Workers 0/1 each serve exactly 2 of the 4 new probes,
        // proving round-robin over the NEW entry (not a flat-ish spread
        // a stale 3-slot entry would produce).
        send_probes(&system, 4).await;
        while delivered_to(&system, "scale.me/worker-0")
            + delivered_to(&system, "scale.me/worker-1")
            < before[0] + before[1] + 4
        {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert_eq!(
            delivered_to(&system, "scale.me/worker-0") - before[0],
            2,
            "worker-0 serves 2 of the 4 post-rescale probes"
        );
        assert_eq!(
            delivered_to(&system, "scale.me/worker-1") - before[1],
            2,
            "worker-1 serves 2 of the 4 post-rescale probes"
        );
        assert_eq!(
            delivered_to(&system, "scale.me/worker-2"),
            before[2],
            "worker-2 is outside the rescaled pool: nothing new delivered there"
        );
    }

    /// Spawns a plain `ProbeWorker` at the blueprint's public path — the
    /// pre-existing topology the rejection tests must not disturb.
    fn spawn_plain_holder(system: &ActorSystem) {
        trouper::builder::spawn_es_builder::<ProbeWorker>(system)
            .at(ActorPath::new("scale.me"))
            .args(json!({}))
            .handles::<Probe>()
            .emits::<Probed>()
            .start();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scale_pool_rejects_zero_workers_and_leaves_the_holder_serving() {
        // Given a live system where a plain actor holds the blueprint's
        // public path.
        let system = probe_blueprint_system();
        spawn_plain_holder(&system);
        let router = ControlRouter::new().with(ScalePoolCmd::new(probe_blueprints()));

        // When dispatching a scale request with workers = 0.
        let reply = router
            .dispatch(
                &system,
                ControlRequest {
                    command: "ScalePool".into(),
                    args: json!({ "kind": "probe", "workers": 0 }),
                },
            )
            .await;

        // Then the reply rejects the count legibly.
        match &reply {
            ControlReply::Err { error } => {
                assert!(
                    error.contains("workers must be >= 1"),
                    "expected a workers rejection, got: {error}"
                );
            }
            other => panic!("expected Err reply, got {other:?}"),
        }
        // And the existing holder still serves — nothing was installed
        // over it.
        send_probes(&system, 1).await;
        while system
            .inbox_cursor(&ActorPath::new("scale.me"))
            .map(|c| c.as_u64())
            != Some(1)
        {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scale_pool_rejects_unknown_kinds_and_leaves_the_holder_serving() {
        // Given a live system where a plain actor holds the blueprint's
        // public path.
        let system = probe_blueprint_system();
        spawn_plain_holder(&system);
        let router = ControlRouter::new().with(ScalePoolCmd::new(probe_blueprints()));

        // When dispatching a scale request for an unregistered kind.
        let reply = router
            .dispatch(
                &system,
                ControlRequest {
                    command: "ScalePool".into(),
                    args: json!({ "kind": "nope", "workers": 2 }),
                },
            )
            .await;

        // Then the reply names the kind AND lists what is registered.
        match &reply {
            ControlReply::Err { error } => {
                assert!(
                    error.contains("no such pool kind: nope"),
                    "expected the unknown kind named, got: {error}"
                );
                assert!(
                    error.contains("probe"),
                    "expected the registered kinds listed, got: {error}"
                );
            }
            other => panic!("expected Err reply, got {other:?}"),
        }
        // And the existing holder still serves — nothing was installed.
        send_probes(&system, 1).await;
        while system
            .inbox_cursor(&ActorPath::new("scale.me"))
            .map(|c| c.as_u64())
            != Some(1)
        {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scale_pool_honors_a_supervised_parent_blueprint() {
        // Given a blueprint whose workers are supervised children of a
        // live parent actor (the pools example's escalation topology).
        let system = probe_blueprint_system();
        trouper::builder::spawn_es_builder::<ProbeWorker>(&system)
            .at(ActorPath::new("scale.parent"))
            .args(json!({}))
            .handles::<Probe>()
            .emits::<Probed>()
            .start();
        let router = ControlRouter::new().with(ScalePoolCmd::new(probe_blueprints_under(Some(
            ActorPath::new("scale.parent"),
        ))));

        // When scaling through the router.
        let reply = router
            .dispatch(
                &system,
                ControlRequest {
                    command: "ScalePool".into(),
                    args: json!({ "kind": "probe", "workers": 2 }),
                },
            )
            .await;

        // Then the install succeeds and the reply is the usual summary.
        assert_eq!(
            reply.result(),
            Some(json!({
                "kind": "probe",
                "public": "scale.me",
                "workers": 2,
                "algo": "round-robin",
            })),
            "a supervised blueprint scales like any other"
        );

        // And the workers serve as pool slots — routing works under the
        // supervised topology (escalation wiring itself is the runtime's
        // own tested concern; the command must simply not break it).
        send_probes(&system, 2).await;
        while delivered_to(&system, "scale.me/worker-0")
            + delivered_to(&system, "scale.me/worker-1")
            < 2
        {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        // And the supervision link is observable: stopping a worker
        // notifies the blueprint's parent (workers were spawned as its
        // children).
        system.stop(&ActorPath::new("scale.me/worker-0")).await;
        while !system.tap_facts().iter().any(|f| {
            matches!(
                &f.kind,
                FactKind::LinkNotified { parent, child }
                    if parent.as_str() == "scale.parent"
                        && child.as_str() == "scale.me/worker-0"
            )
        }) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }
}
