//! Serves and consumes system state as zenoh messages.
//!
//! One key family — [`STATE_KEY`] — carries the whole story: [`install`]
//! declares a queryable that answers every query with a *fresh* export
//! document (captured, journaled through a `StateReporter` actor, then
//! replied), and [`fetch`] queries that key and decodes the export. Both
//! sides are zenoh peers on `Config::default()` — no addresses, no ports.
//!
//! Test isolation: zenoh's default peer discovery puts every session on
//! the machine (and network) into one mesh, so concurrent tests must not
//! share the production key. [`StateKey::scoped`] derives per-test island
//! keys (`actor-runtime/state/<scope>`); a query on one island only ever
//! reaches queryables declared on that same island.
//!
//! Freshness contract: a reply is only sent after the reporter's journaled
//! `seq` has advanced past its pre-query value, so what `fetch` decodes is
//! a report of the system as of that query, not the previous one.

use actor_runtime::prelude::*;
use actor_runtime::state_report::ReportState;
use actor_runtime::system::{ActorSystem, SystemExport};
use std::borrow::Cow;
use std::time::Duration;
use tokio::time::Instant;

/// Re-exported so consumers of [`install_on`]/[`fetch_on`] can name
/// session types (e.g. to close a bridge session) without adding their
/// own zenoh dependency.
pub use zenoh;

/// The zenoh key every state query travels on.
pub const STATE_KEY: &str = "actor-runtime/state";

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

    /// A namespaced island key, `actor-runtime/state/<scope>`.
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
    system: std::sync::Arc<ActorSystem>,
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
    system: std::sync::Arc<ActorSystem>,
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
    system: std::sync::Arc<ActorSystem>,
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
    system: &std::sync::Arc<ActorSystem>,
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
