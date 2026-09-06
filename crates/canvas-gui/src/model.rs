//! The pure scene model: `SystemExport` → drawable graph.
//!
//! No bevy here. Everything in this module is data-in/data-out so the
//! mapping is unit-testable against a captured export (see
//! [`crate::fixture`]). Rendering decisions (boxes, stubs vs arcs) are
//! documented on the fields they affect.

use actor_runtime::system::{DeclaredEdge, EdgeDirection, SystemExport};
use std::collections::HashSet;

/// The identity of a drawable node (an actor path, or a pseudo-node id
/// like `topic:system.facts`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub String);

/// Which of the two actor contracts a node implements — or that it is a
/// pseudo-node (a topic anchor) rather than an actor at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    /// Journaled, replayable (blue badge).
    EventSourced,
    /// Impure by design (orange badge).
    Service,
    /// A topic anchor created so edges have endpoints; not an actor.
    Topic,
}

impl NodeKind {
    /// The short display name ("ES", "Service", "topic").
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::EventSourced => "ES",
            Self::Service => "Service",
            Self::Topic => "topic",
        }
    }
}

/// The contract lines shown inside a node box and in its popup.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ManifestInfo {
    /// Schemas the actor accepts.
    pub handles: Vec<String>,
    /// Schemas the actor produces.
    pub emits: Vec<String>,
    /// Topics the actor subscribes to.
    pub subscribes: Vec<String>,
}

/// One drawable actor (or topic pseudo-node).
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// The node's identity.
    pub id: NodeId,
    /// Badge kind (drives color).
    pub kind: NodeKind,
    /// The display path ("api/worker-0", "system.facts").
    pub path: String,
    /// The declared contract lines.
    pub manifest: ManifestInfo,
    /// Live ES state (ES actors only).
    pub state: Option<serde_json::Value>,
    /// The inbox ack cursor (ES progress).
    pub cursor: Option<u64>,
}

/// What a container groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerKind {
    /// A stateless pool (workers behind a public path).
    Pool,
    /// A partition set (entities behind a public path).
    Partition,
}

/// A grouping box (pool or partition). Members are drawn inside it;
/// the container's public path is deliberately NOT a node — the runtime
/// section describes routing, not an actor (see the plan's rules
/// decision).
#[derive(Debug, Clone, PartialEq)]
pub struct Container {
    /// The container's identity ("pool:api", "part:accounts").
    pub id: NodeId,
    /// Which section produced it (drives color).
    pub kind: ContainerKind,
    /// The public path ("api", "accounts").
    pub path: String,
    /// The header line ("pool · api · round-robin").
    pub label: String,
    /// Member node ids in export order.
    pub members: Vec<NodeId>,
}

/// What an edge represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    /// A declared topic subscription (the only declared arc we draw;
    /// per-actor Handles/Emits contracts render inside node boxes).
    Subscription,
    /// Observed traffic from the tap.
    Observed,
}

/// One drawable arc between two resolved endpoints.
#[derive(Debug, Clone, PartialEq)]
pub struct Edge {
    /// The source node.
    pub from: NodeId,
    /// The destination node.
    pub to: NodeId,
    /// The arc label ("Any@1 · Subscribes", "Work@1 ×3").
    pub label: String,
    /// What the arc represents (drives style).
    pub kind: EdgeKind,
}

/// One router rule rendered as legend text (rules point at public paths
/// and observers, not always at nodes — geometry would be fake).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleSummary {
    /// The legend line ("tee Work@1 @api → watchdog").
    pub text: String,
}

/// The whole drawable scene: the model half of the canvas.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SceneGraph {
    /// Actor nodes plus topic pseudo-nodes.
    pub nodes: Vec<Node>,
    /// Pool/partition grouping boxes.
    pub containers: Vec<Container>,
    /// Subscription and observed arcs.
    pub edges: Vec<Edge>,
    /// Rule legend lines.
    pub rules: Vec<RuleSummary>,
}

impl SceneGraph {
    /// Looks up a node by id.
    #[must_use]
    pub fn node(&self, id: &NodeId) -> Option<&Node> {
        self.nodes.iter().find(|node| &node.id == id)
    }
}

/// Maps a fetched export into the drawable scene.
///
/// Mapping decisions (see the plan's dialectical outcomes):
/// - every actor becomes a node (workers and entities included);
/// - every pool/partition becomes a container over its members;
/// - declared Handles/Emits stay INSIDE node boxes (12 stubs would be
///   noise); declared topic subscriptions become arcs;
/// - observed edges become arcs where both endpoints resolve;
/// - rules become legend lines (they point at public paths, which are
///   not nodes).
#[must_use]
pub fn from_export(export: &SystemExport) -> SceneGraph {
    let actor_nodes = actor_nodes(export);
    let actor_ids: HashSet<NodeId> = actor_nodes.iter().map(|n| n.id.clone()).collect();
    let topic_names = subscribed_topics(export);

    SceneGraph {
        nodes: {
            let mut nodes = actor_nodes;
            nodes.extend(topic_nodes(&topic_names));
            nodes
        },
        containers: containers(export),
        edges: edges(export, &actor_ids, &topic_names),
        rules: rules(export),
    }
}

/// One node per actor, in export order.
fn actor_nodes(export: &SystemExport) -> Vec<Node> {
    export
        .actors
        .iter()
        .map(|actor| Node {
            id: NodeId(actor.path.as_str().to_owned()),
            kind: match actor.kind {
                actor_runtime::types::ActorKind::EventSourced => NodeKind::EventSourced,
                actor_runtime::types::ActorKind::Service => NodeKind::Service,
            },
            path: actor.path.as_str().to_owned(),
            manifest: ManifestInfo {
                handles: actor
                    .manifest
                    .handles
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect(),
                emits: actor
                    .manifest
                    .emits
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect(),
                subscribes: actor
                    .manifest
                    .subscribes
                    .iter()
                    .map(|topic| topic.as_str().to_owned())
                    .collect(),
            },
            state: actor.state.clone(),
            cursor: actor.cursor,
        })
        .collect()
}

/// Topic names that at least one subscription arc points at, in
/// first-appearance order (deterministic).
fn subscribed_topics(export: &SystemExport) -> Vec<String> {
    let mut topics: Vec<String> = Vec::new();
    for edge in &export.declared_edges {
        if edge.direction == EdgeDirection::Subscribes {
            let Some(topic) = &edge.topic else {
                continue;
            };
            let name = topic.as_str().to_owned();
            if !topics.contains(&name) {
                topics.push(name);
            }
        }
    }
    topics
}

/// One pseudo-node per subscribed topic.
fn topic_nodes(topics: &[String]) -> Vec<Node> {
    topics
        .iter()
        .map(|name| Node {
            id: NodeId(format!("topic:{name}")),
            kind: NodeKind::Topic,
            path: name.clone(),
            manifest: ManifestInfo::default(),
            state: None,
            cursor: None,
        })
        .collect()
}

/// One container per pool, then per partition, in export order.
fn containers(export: &SystemExport) -> Vec<Container> {
    let mut result: Vec<Container> = Vec::new();
    for pool in &export.pools {
        result.push(Container {
            id: NodeId(format!("pool:{}", pool.path.as_str())),
            kind: ContainerKind::Pool,
            path: pool.path.as_str().to_owned(),
            label: format!("pool · {} · {}", pool.path.as_str(), pool.algo),
            members: pool
                .workers
                .iter()
                .map(|w| NodeId(w.as_str().to_owned()))
                .collect(),
        });
    }
    for partition in &export.partitions {
        result.push(Container {
            id: NodeId(format!("part:{}", partition.path.as_str())),
            kind: ContainerKind::Partition,
            path: partition.path.as_str().to_owned(),
            label: format!(
                "partitions · {} · key={}",
                partition.path.as_str(),
                partition.key_field
            ),
            members: partition
                .entities
                .iter()
                .map(|e| NodeId(e.as_str().to_owned()))
                .collect(),
        });
    }
    result
}

/// Subscription arcs first (declared order), then observed arcs (export
/// order). Declared Handles/Emits deliberately produce no arcs. An
/// observed edge without a resolvable source (system-entry sends) or
/// destination is dropped — geometry must not invent endpoints.
fn edges(export: &SystemExport, actor_ids: &HashSet<NodeId>, topics: &[String]) -> Vec<Edge> {
    let mut result: Vec<Edge> = Vec::new();
    for declared in &export.declared_edges {
        if let Some(edge) = subscription_edge(declared, actor_ids, topics) {
            result.push(edge);
        }
    }
    for observed in &export.observed_edges {
        if let Some(edge) = observed_edge(observed, actor_ids, topics) {
            result.push(edge);
        }
    }
    result
}

/// A declared subscription becomes an arc to the topic pseudo-node.
fn subscription_edge(
    declared: &DeclaredEdge,
    actor_ids: &HashSet<NodeId>,
    topics: &[String],
) -> Option<Edge> {
    if declared.direction != EdgeDirection::Subscribes {
        return None;
    }
    let topic = declared.topic.as_ref()?;
    let name = topic.as_str();
    if !topics.iter().any(|known| known == name) {
        return None;
    }
    let from = NodeId(declared.actor.as_str().to_owned());
    if !actor_ids.contains(&from) {
        return None;
    }
    Some(Edge {
        from,
        to: NodeId(format!("topic:{name}")),
        label: format!("{} · Subscribes", declared.schema),
        kind: EdgeKind::Subscription,
    })
}

/// An observed edge becomes an arc where both endpoints resolve;
/// `to` is "path:<p>" or "topic:<t>".
fn observed_edge(
    observed: &actor_runtime::system::ObservedEdge,
    actor_ids: &HashSet<NodeId>,
    topics: &[String],
) -> Option<Edge> {
    let from = NodeId(observed.from.as_deref()?.to_owned());
    if !actor_ids.contains(&from) {
        return None;
    }
    let to = if let Some(path) = observed.to.strip_prefix("path:") {
        NodeId(path.to_owned())
    } else if let Some(topic) = observed.to.strip_prefix("topic:") {
        NodeId(format!("topic:{topic}"))
    } else {
        return None;
    };
    let resolved = actor_ids.contains(&to)
        || topics
            .iter()
            .any(|name| NodeId(format!("topic:{name}")) == to);
    if !resolved {
        return None;
    }
    Some(Edge {
        from,
        to,
        label: format!("{} ×{}", observed.schema, observed.count),
        kind: EdgeKind::Observed,
    })
}

/// One legend line per rule, in declaration order.
fn rules(export: &SystemExport) -> Vec<RuleSummary> {
    export
        .rules
        .iter()
        .map(|rule| {
            let schema = rule
                .schema
                .as_ref()
                .map(std::string::ToString::to_string)
                .unwrap_or_else(|| "*".into());
            let dest = rule
                .dest
                .as_ref()
                .map(|path| path.as_str().to_owned())
                .unwrap_or_else(|| "*".into());
            let source = rule
                .source
                .as_ref()
                .map(|path| format!("from {} ", path.as_str()))
                .unwrap_or_default();
            RuleSummary {
                text: format!(
                    "{source}{} {schema} @{dest} → {}",
                    rule.action,
                    rule.observer.as_str()
                ),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_maps_to_expected_scene() {
        // Given the captured demo export.
        let export = crate::fixture::fixture();

        // When mapping it to a scene.
        let scene = from_export(&export);

        // Then the node count is the 6 actors plus the one topic
        // pseudo-node.
        assert_eq!(scene.nodes.len(), 6 + 1);
        // And the kind split is 5 ES, 1 service, 1 topic anchor.
        let es = scene
            .nodes
            .iter()
            .filter(|n| n.kind == NodeKind::EventSourced)
            .count();
        let service = scene
            .nodes
            .iter()
            .filter(|n| n.kind == NodeKind::Service)
            .count();
        assert_eq!(es, 5);
        assert_eq!(service, 1);
        // And there are two containers with the right members.
        assert_eq!(scene.containers.len(), 2);
        let pool = scene
            .containers
            .iter()
            .find(|c| c.kind == ContainerKind::Pool)
            .expect("pool container");
        assert_eq!(pool.label, "pool · api · round-robin");
        assert_eq!(
            pool.members,
            vec![NodeId("api/worker-0".into()), NodeId("api/worker-1".into())]
        );
        let partition = scene
            .containers
            .iter()
            .find(|c| c.kind == ContainerKind::Partition)
            .expect("partition container");
        assert_eq!(partition.label, "partitions · accounts · key=account");
        // And the watchdog subscription arc exists with its label.
        let subs: Vec<&Edge> = scene
            .edges
            .iter()
            .filter(|e| e.kind == EdgeKind::Subscription)
            .collect();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].from, NodeId("watchdog".into()));
        assert_eq!(subs[0].to, NodeId("topic:system.facts".into()));
        assert_eq!(subs[0].label, "Any@1 · Subscribes");
        // And the topic pseudo-node exists and is not an actor.
        let topic = scene
            .node(&NodeId("topic:system.facts".into()))
            .expect("topic node");
        assert_eq!(topic.kind, NodeKind::Topic);
        // And one rule summary carries the tee rule.
        assert_eq!(scene.rules.len(), 1);
        assert_eq!(scene.rules[0].text, "tee Work@1 @api → watchdog");
    }

    #[test]
    fn unresolved_edge_endpoints_are_dropped() {
        // Given the demo export whose rules reference the pool public
        // path (not a node) and whose observed section is empty.
        let export = crate::fixture::fixture();

        // When mapping to a scene.
        let scene = from_export(&export);

        // Then no arc exists for the rule's `api` destination (rules
        // are legend lines, not edges).
        let api_is_node = scene
            .nodes
            .iter()
            .any(|n| n.path == "api" && n.kind != NodeKind::Topic);
        assert!(!api_is_node);
        // And with an empty observed section, only the subscription
        // arc remains.
        assert_eq!(scene.edges.len(), 1);
        // And observed endpoints that cannot resolve are dropped by
        // construction — exercised directly with a synthetic edge.
        let synthetic = actor_runtime::system::ObservedEdge {
            from: Some("api/worker-0".into()),
            to: "path:ghost".into(),
            schema: actor_runtime::types::SchemaId::parse("Work@1").expect("schema id"),
            count: 1,
        };
        let actor_ids: HashSet<NodeId> = scene
            .nodes
            .iter()
            .filter(|n| n.kind != NodeKind::Topic)
            .map(|n| n.id.clone())
            .collect();
        assert!(observed_edge(&synthetic, &actor_ids, &["system.facts".into()]).is_none());
    }
}
