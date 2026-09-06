//! canvas: connects to a running system's canvas server and consumes a
//! whole-system snapshot.
//!
//! The library seam is [`connect_snapshot`]: one address, one timeout,
//! one `SystemExport` back (or a [`CanvasError`] naming what failed).
//! The `canvas` binary wraps it: connect-or-abort — on any failure it
//! prints a legible stderr message and exits non-zero before any GUI
//! startup path (no GUI exists yet, and none may be stubbed here).
//!
//! Wire protocol: versioned NDJSON envelopes, strict v1 (see
//! [`wire`]). The wire is untyped JSON; this crate deserializes the
//! snapshot into `actor_runtime`'s `SystemExport`.

pub mod wire;

use crate::wire::ReplyKind;
use actor_runtime::system::SystemExport;
use actor_runtime::types::ActorKind;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// Everything that can go wrong between "connect" and "snapshot in
/// hand". Every message names the address involved — the binary prints
/// these verbatim to stderr before aborting.
#[derive(Debug, wherror::Error)]
pub enum CanvasError {
    /// The TCP connection itself failed (refused, unroutable, ...).
    #[error("could not connect to {addr}: {source}")]
    Connect {
        /// The address that refused the connection.
        addr: SocketAddr,
        /// The underlying OS error.
        #[source]
        source: std::io::Error,
    },
    /// The deadline passed before the exchange completed.
    #[error("timed out after {timeout:?} talking to {addr}")]
    Timeout {
        /// The address that went quiet.
        addr: SocketAddr,
        /// The budget that elapsed.
        timeout: Duration,
    },
    /// The server hung up before sending a reply line.
    #[error("connection to {addr} closed before a reply arrived")]
    Closed {
        /// The address that closed the connection.
        addr: SocketAddr,
    },
    /// The server answered with an error envelope (or an envelope this
    /// client cannot interpret — including a different protocol version).
    #[error("{addr} replied with an error ({code}): {detail}")]
    Protocol {
        /// The address that produced the reply.
        addr: SocketAddr,
        /// The envelope's error code (or a client-side discriminator).
        code: String,
        /// The envelope's human-readable detail.
        detail: String,
    },
    /// The snapshot document did not decode into a `SystemExport`.
    #[error("snapshot from {addr} was not a decodable SystemExport: {source}")]
    Payload {
        /// The address that sent the payload.
        addr: SocketAddr,
        /// The deserialization error.
        #[source]
        source: serde_json::Error,
    },
}

/// An open, ready connection: the request has been written. The write
/// half is kept only so the connection (and its request) stays alive
/// while the reply is read.
struct Connected {
    #[allow(dead_code)]
    writer: tokio::net::tcp::OwnedWriteHalf,
    lines: tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
}

/// Connects to a canvas server at `addr` and fetches one whole-system
/// snapshot, giving up after `timeout` in total across connect+request
/// and again on the reply.
///
/// # Errors
///
/// - [`CanvasError::Connect`] when the TCP connection fails.
/// - [`CanvasError::Timeout`] when connecting or reading outlives
///   `timeout`.
/// - [`CanvasError::Closed`] when the server hangs up before replying.
/// - [`CanvasError::Protocol`] when the reply is an error envelope, a
///   different protocol version, or not a known envelope at all.
/// - [`CanvasError::Payload`] when a snapshot document fails to decode.
pub async fn connect_snapshot(addr: SocketAddr, timeout: Duration) -> Result<SystemExport, CanvasError> {
    let mut connected = open(addr, timeout).await?;
    let line = read_reply_line(&mut connected, addr, timeout).await?;
    decode_reply(&line, addr)
}

/// Opens the connection and writes the snapshot request.
async fn open(addr: SocketAddr, timeout: Duration) -> Result<Connected, CanvasError> {
    let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| CanvasError::Timeout { addr, timeout })?
        .map_err(|source| CanvasError::Connect { addr, source })?;
    let (reader, mut writer) = stream.into_split();
    let write = async {
        writer.write_all(wire::REQUEST_LINE.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await
    };
    write
        .await
        .map_err(|source| CanvasError::Connect { addr, source })?;
    Ok(Connected {
        writer,
        lines: BufReader::new(reader).lines(),
    })
}

/// Reads the next non-empty reply line (empty lines are never replies).
async fn read_reply_line(
    connected: &mut Connected,
    addr: SocketAddr,
    timeout: Duration,
) -> Result<String, CanvasError> {
    loop {
        let line = tokio::time::timeout(timeout, connected.lines.next_line())
            .await
            .map_err(|_| CanvasError::Timeout { addr, timeout })?
            .map_err(|_| CanvasError::Closed { addr })?;
        match line {
            Some(line) if line.trim().is_empty() => continue,
            Some(line) => return Ok(line),
            None => return Err(CanvasError::Closed { addr }),
        }
    }
}

/// Decodes one reply line into a snapshot — or the error it carries.
fn decode_reply(line: &str, addr: SocketAddr) -> Result<SystemExport, CanvasError> {
    let envelope: serde_json::Value = serde_json::from_str(line).map_err(|source| {
        CanvasError::Protocol {
            addr,
            code: ReplyKind::UNPARSEABLE.to_owned(),
            detail: format!("reply was not valid JSON: {source}"),
        }
    })?;
    match wire::classify(&envelope) {
        ReplyKind::Snapshot => {
            serde_json::from_value(envelope["export"].clone()).map_err(|source| {
                CanvasError::Payload { addr, source }
            })
        }
        ReplyKind::Error => {
            let code = envelope["code"].as_str().unwrap_or("unknown").to_owned();
            let detail = envelope["detail"].as_str().unwrap_or("").to_owned();
            Err(CanvasError::Protocol { addr, code, detail })
        }
        ReplyKind::Unknown(version) => Err(CanvasError::Protocol {
            addr,
            code: ReplyKind::UNKNOWN_VERSION.to_owned(),
            detail: format!(
                "client speaks protocol v{}, reply carried v{version}",
                wire::PROTOCOL_VERSION
            ),
        }),
    }
}

/// The one-glance digest of a snapshot: counts per export section, with
/// actor counts split by contract kind. Derived by [`SnapshotSummary::of`],
/// rendered by [`SnapshotSummary::render`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotSummary {
    /// Registered schemas (all versions).
    pub schemas: usize,
    /// Live actors of either kind.
    pub actors: usize,
    /// Live journaled actors.
    pub es: usize,
    /// Live edge/service actors.
    pub service: usize,
    /// Manifest-declared edges.
    pub declared_edges: usize,
    /// Traffic-aggregated edges.
    pub observed_edges: usize,
    /// Declared stateless pools.
    pub pools: usize,
    /// Declared partition sets.
    pub partitions: usize,
    /// Activated entities across all partition sets.
    pub entities: usize,
    /// Declared router rules.
    pub rules: usize,
}

impl SnapshotSummary {
    /// Counts every export section of `export`.
    pub fn of(export: &SystemExport) -> Self {
        let es = export
            .actors
            .iter()
            .filter(|a| a.kind == ActorKind::EventSourced)
            .count();
        Self {
            schemas: export.schemas.len(),
            actors: export.actors.len(),
            es,
            service: export.actors.len() - es,
            declared_edges: export.declared_edges.len(),
            observed_edges: export.observed_edges.len(),
            pools: export.pools.len(),
            partitions: export.partitions.len(),
            entities: export.partitions.iter().map(|p| p.entities.len()).sum(),
            rules: export.rules.len(),
        }
    }

    /// The two summary lines (as printed by the binary, without the
    /// leading `connected to <addr>` line or the JSON dump).
    pub fn render(&self) -> String {
        format!(
            "schemas: {}  actors: {} (ES: {}, service: {})  declared_edges: {}  observed_edges: {}\n\
             pools: {}  partitions: {} ({} entities)  rules: {}",
            self.schemas,
            self.actors,
            self.es,
            self.service,
            self.declared_edges,
            self.observed_edges,
            self.pools,
            self.partitions,
            self.entities,
            self.rules,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actor_runtime::schema::{ActorManifest, SchemaDef, SchemaKind};
    use actor_runtime::system::{ActorExport, PartitionExport};
    use actor_runtime::types::{ActorPath, InboxOffset};

    /// An export with known, nonzero content where the summary has
    /// something to count: 5 schemas, one actor of each kind, a partition
    /// set with 3 activated entities. Everything else stays empty (which
    /// exercises the zero case of every remaining counter).
    fn sample_export() -> SystemExport {
        let actor = |kind: ActorKind, path: &str| ActorExport {
            path: ActorPath::new(path),
            kind,
            manifest: ActorManifest::new(),
            state: None,
            cursor: None,
        };
        SystemExport {
            schemas: (0..5)
                .map(|i| SchemaDef {
                    name: format!("S{i}"),
                    version: 1,
                    kind: SchemaKind::Command,
                    fields: Vec::new(),
                    description: None,
                })
                .collect(),
            actors: vec![
                actor(ActorKind::EventSourced, "es/one"),
                actor(ActorKind::Service, "svc/two"),
            ],
            declared_edges: Vec::new(),
            observed_edges: Vec::new(),
            pools: Vec::new(),
            partitions: vec![PartitionExport {
                path: ActorPath::new("accounts"),
                key_field: "account".to_owned(),
                entities: vec![
                    ActorPath::new("accounts/acme"),
                    ActorPath::new("accounts/globex"),
                    ActorPath::new("accounts/initech"),
                ],
            }],
            rules: Vec::new(),
        }
    }

    #[test]
    fn summary_counts_each_export_section() {
        // Given a snapshot with known section sizes.
        let export = sample_export();

        // When summarizing.
        let summary = SnapshotSummary::of(&export);

        // Then every section count matches.
        assert_eq!(summary.schemas, 5);
        assert_eq!(summary.actors, 2);
        assert_eq!(summary.es, 1);
        assert_eq!(summary.service, 1);
        assert_eq!(summary.declared_edges, 0);
        assert_eq!(summary.observed_edges, 0);
        assert_eq!(summary.pools, 0);
        assert_eq!(summary.partitions, 1);
        assert_eq!(summary.entities, 3);
        assert_eq!(summary.rules, 0);
    }

    #[test]
    fn render_shows_counts_in_two_lines() {
        // Given a summary of a known snapshot.
        let summary = SnapshotSummary::of(&sample_export());

        // When rendering.
        let text = summary.render();

        // Then both lines carry the counts in order.
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("schemas: 5"));
        assert!(lines[0].contains("actors: 2 (ES: 1, service: 1)"));
        assert!(lines[1].contains("partitions: 1 (3 entities)"));
        assert!(lines[1].contains("rules: 0"));
    }

    #[test]
    fn empty_export_summarizes_to_all_zeroes() {
        // Given an export with every section empty.
        let export = SystemExport {
            schemas: Vec::new(),
            actors: Vec::new(),
            declared_edges: Vec::new(),
            observed_edges: Vec::new(),
            pools: Vec::new(),
            partitions: Vec::new(),
            rules: Vec::new(),
        };

        // When summarizing.
        let summary = SnapshotSummary::of(&export);

        // Then every count is zero (and rendering never divides by a
        // zero actor count).
        assert_eq!(summary.actors, 0);
        assert_eq!(summary.es, 0);
        assert_eq!(summary.service, 0);
        assert_eq!(summary.entities, 0);
        assert!(summary.render().contains("actors: 0"));
    }

    /// Silence the unused-import lint when InboxOffset is not needed by
    /// the cases above (kept: it documents the cursor type on
    /// ActorExport).
    #[test]
    fn inbox_offset_type_is_u64_backed() {
        // Given an offset.
        let offset = InboxOffset::new(7);
        // Then its raw value round-trips.
        assert_eq!(offset.as_u64(), 7);
    }
}
