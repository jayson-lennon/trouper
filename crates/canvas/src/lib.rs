//! canvas: queries a running system's state over zenoh and consumes the
//! export document.
//!
//! The library seam is [`fetch_export`]: one call, one fresh
//! `SystemExport` back (or a [`StateError`] naming what failed). All
//! transport lives in `state-report` — this crate is a pure projection.
//! The `canvas` binary wraps it: query-or-abort — on any failure it
//! prints a legible stderr message and exits non-zero before any GUI
//! startup path (no GUI exists yet, and none may be stubbed here).

use actor_runtime::system::SystemExport;
use state_report::StateBridgeError;

/// Everything that can go wrong between "query" and "export in hand".
/// The binary prints these verbatim to stderr before aborting.
#[derive(Debug, wherror::Error)]
pub enum StateError {
    /// A zenoh operation failed.
    #[error("zenoh query failed: {0}")]
    Zenoh(String),
    /// No bridge answered within the fetch budget.
    #[error("no system answered the state query; is one running with a bridge installed?")]
    Timeout,
    /// A reply did not decode into a `SystemExport`.
    #[error("reply was not a decodable SystemExport: {0}")]
    Payload(String),
}

impl From<StateBridgeError> for StateError {
    fn from(error: StateBridgeError) -> Self {
        match error {
            StateBridgeError::Zenoh(detail) => Self::Zenoh(detail),
            StateBridgeError::Timeout(_) => Self::Timeout,
            StateBridgeError::NoReporter(path) => {
                Self::Payload(format!("no StateReporter actor at {path}"))
            }
            StateBridgeError::Payload(detail) => Self::Payload(detail),
        }
    }
}

/// Queries [`state_report::STATE_KEY`] and decodes the fresh
/// `SystemExport` document the answering system reports.
///
/// # Errors
///
/// - [`StateError::Zenoh`] when the transport fails.
/// - [`StateError::Timeout`] when nothing answers within the budget.
/// - [`StateError::Payload`] when a reply fails to decode.
pub async fn fetch_export() -> Result<SystemExport, StateError> {
    Ok(state_report::fetch().await?)
}

/// The one-glance digest of an export: counts per export section, with
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
            .filter(|a| a.kind == actor_runtime::types::ActorKind::EventSourced)
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
        let actor = |kind: actor_runtime::types::ActorKind, path: &str| ActorExport {
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
                actor(actor_runtime::types::ActorKind::EventSourced, "es/one"),
                actor(actor_runtime::types::ActorKind::Service, "svc/two"),
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
        // Given an export with known section sizes.
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
        // Given a summary of a known export.
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
