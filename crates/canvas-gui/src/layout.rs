//! Deterministic layout: `SceneGraph` → world-space placements.
//!
//! Pure and windowless. The algorithm is section-based (containers on
//! the left, standalone actors in a column to their right, topic
//! pseudo-nodes below) with no randomness, so two layouts of the same
//! graph are identical — the seam a future force-directed pass would
//! replace.
//!
//! Hit-testing lives here too: it consumes placements, and keeping it
//! in this module avoids a layout→view dependency cycle.

use std::collections::HashMap;

use crate::model::{NodeId, SceneGraph};
use crate::view::Vec2;

/// Node box size in world units.
pub const NODE_SIZE: Vec2 = Vec2::new(220.0, 90.0);
/// Padding around a container's member grid.
const CONTAINER_PAD: f32 = 24.0;
/// Height of a container's label band (above the member grid).
const CONTAINER_LABEL: f32 = 28.0;
/// Gaps between member cells.
const CELL_GAP: Vec2 = Vec2::new(16.0, 16.0);
/// Member grid columns.
const GRID_COLS: usize = 2;
/// Horizontal gap between containers.
const CONTAINER_GAP: f32 = 80.0;
/// Vertical gap in the standalone column.
const STANDALONE_GAP: f32 = 40.0;
/// Gap between the sections and the topic pseudo-nodes.
const TOPIC_GAP: f32 = 60.0;

/// Where and how big one drawable thing is (world-space center + size).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Placement {
    /// World-space center.
    pub pos: Vec2,
    /// World-space full size.
    pub size: Vec2,
}

impl Placement {
    /// The axis-aligned bounds: `(min, max)`.
    #[must_use]
    pub fn bounds(&self) -> (Vec2, Vec2) {
        let half = self.size * 0.5;
        (self.pos - half, self.pos + half)
    }

    /// True when `point` is inside the rect.
    #[must_use]
    pub fn contains(&self, point: Vec2) -> bool {
        let (min, max) = self.bounds();
        point.x >= min.x && point.x <= max.x && point.y >= min.y && point.y <= max.y
    }
}

/// Clipped endpooints of one drawable arc.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeGeometry {
    /// The source node id.
    pub from: NodeId,
    /// The destination node id.
    pub to: NodeId,
    /// Segment start (on the source box border).
    pub a: Vec2,
    /// Segment end (on the destination box border).
    pub b: Vec2,
}

/// Everything rendering needs to know about where things are.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LayoutOutput {
    /// Node placements by id (actors and topic pseudo-nodes).
    pub nodes: HashMap<NodeId, Placement>,
    /// Container placements by id.
    pub containers: HashMap<NodeId, Placement>,
    /// Drawable arcs with clipped endpoints.
    pub edges: Vec<EdgeGeometry>,
    /// Scene bounding rect `(min, max)` for camera framing.
    pub bounds: (Vec2, Vec2),
}

impl LayoutOutput {
    /// The topmost node under `world`, or `None` for empty space.
    /// Later nodes draw above earlier ones, so the scan is reversed.
    #[must_use]
    pub fn hit_node<'a>(&'a self, order: &'a [NodeId], world: Vec2) -> Option<&'a NodeId> {
        order
            .iter()
            .rev()
            .find(|id| self.nodes.get(*id).is_some_and(|p| p.contains(world)))
    }
}

/// Lays out the whole graph deterministically.
#[must_use]
pub fn layout(graph: &SceneGraph) -> LayoutOutput {
    let mut out = LayoutOutput::default();
    place_containers(graph, &mut out);
    let standalone_column_x = place_standalones(graph, &mut out);
    place_topics(graph, standalone_column_x, &mut out);
    place_edges(graph, &mut out);
    compute_bounds(&mut out);
    out
}

/// Lays containers left-to-right with their member grids inside; the
/// cursor it returns is where the next section starts.
fn place_containers(graph: &SceneGraph, out: &mut LayoutOutput) -> f32 {
    let mut cursor_x = 0.0_f32;
    for container in &graph.containers {
        let count = container.members.len();
        let rows = count.div_ceil(GRID_COLS).max(1);
        let inner_w = GRID_COLS as f32 * NODE_SIZE.x + (GRID_COLS - 1) as f32 * CELL_GAP.x;
        let inner_h = rows as f32 * NODE_SIZE.y + (rows - 1) as f32 * CELL_GAP.y;
        let size = Vec2::new(
            inner_w + 2.0 * CONTAINER_PAD,
            inner_h + 2.0 * CONTAINER_PAD + CONTAINER_LABEL,
        );
        let center = Vec2::new(cursor_x + size.x * 0.5, 0.0);
        out.containers
            .insert(container.id.clone(), Placement { pos: center, size });

        let content_left = center.x - size.x * 0.5 + CONTAINER_PAD;
        let content_top = center.y + size.y * 0.5 - CONTAINER_PAD - CONTAINER_LABEL;
        for (index, member) in container.members.iter().enumerate() {
            let col = index % GRID_COLS;
            let row = index / GRID_COLS;
            let pos = Vec2::new(
                content_left + col as f32 * (NODE_SIZE.x + CELL_GAP.x) + NODE_SIZE.x * 0.5,
                content_top - row as f32 * (NODE_SIZE.y + CELL_GAP.y) - NODE_SIZE.y * 0.5,
            );
            out.nodes.insert(
                member.clone(),
                Placement {
                    pos,
                    size: NODE_SIZE,
                },
            );
        }
        cursor_x += size.x + CONTAINER_GAP;
    }
    cursor_x
}

/// Lays standalone actors (not inside any container) in a vertical
/// column right of the containers; returns the column's center x so
/// topic pseudo-nodes align under it.
fn place_standalones(graph: &SceneGraph, out: &mut LayoutOutput) -> f32 {
    let in_container: Vec<&NodeId> = graph
        .containers
        .iter()
        .flat_map(|c| c.members.iter())
        .collect();
    let column_left = out
        .containers
        .values()
        .map(|p| p.bounds().1.x)
        .fold(f32::NEG_INFINITY, f32::max);
    let column_left = if column_left.is_finite() {
        column_left + CONTAINER_GAP
    } else {
        0.0
    };
    let top_max = out
        .nodes
        .values()
        .map(|p| p.bounds().1.y)
        .fold(f32::NEG_INFINITY, f32::max);
    let mut top = if top_max.is_finite() {
        top_max
    } else {
        NODE_SIZE.y * 0.5
    };
    let column_center_x = column_left + NODE_SIZE.x * 0.5;
    for node in &graph.nodes {
        if node.kind == crate::model::NodeKind::Topic || in_container.contains(&&node.id) {
            continue;
        }
        let pos = Vec2::new(column_center_x, top - NODE_SIZE.y * 0.5);
        out.nodes.insert(
            node.id.clone(),
            Placement {
                pos,
                size: NODE_SIZE,
            },
        );
        top -= NODE_SIZE.y + STANDALONE_GAP;
    }
    column_center_x
}

/// Lays topic pseudo-nodes in a column below everything laid so far,
/// aligned with the standalone actors' column.
fn place_topics(graph: &SceneGraph, column_center_x: f32, out: &mut LayoutOutput) {
    let lowest = out
        .nodes
        .values()
        .chain(out.containers.values())
        .map(|p| p.bounds().0.y)
        .fold(f32::INFINITY, f32::min);
    let mut top = if lowest.is_finite() {
        lowest - TOPIC_GAP
    } else {
        -TOPIC_GAP
    };
    for node in &graph.nodes {
        if node.kind != crate::model::NodeKind::Topic {
            continue;
        }
        let pos = Vec2::new(column_center_x, top - NODE_SIZE.y * 0.5);
        out.nodes.insert(
            node.id.clone(),
            Placement {
                pos,
                size: NODE_SIZE,
            },
        );
        top -= NODE_SIZE.y + STANDALONE_GAP;
    }
}

/// Clipped center-to-center segments for every resolvable edge.
fn place_edges(graph: &SceneGraph, out: &mut LayoutOutput) {
    for edge in &graph.edges {
        let (Some(from), Some(to)) = (out.nodes.get(&edge.from), out.nodes.get(&edge.to)) else {
            continue;
        };
        if let Some((a, b)) =
            crate::view::clip_segment_between_boxes(from.pos, from.size, to.pos, to.size)
        {
            out.edges.push(EdgeGeometry {
                from: edge.from.clone(),
                to: edge.to.clone(),
                a,
                b,
            });
        }
    }
}

/// The bounding rect over every placement, with a small margin; the
/// empty scene is a degenerate rect at the origin.
fn compute_bounds(out: &mut LayoutOutput) {
    let margin = 40.0;
    let mut min = Vec2::splat(f32::INFINITY);
    let mut max = Vec2::splat(f32::NEG_INFINITY);
    for placement in out.nodes.values().chain(out.containers.values()) {
        let (p_min, p_max) = placement.bounds();
        min = min.min(p_min);
        max = max.max(p_max);
    }
    if !min.x.is_finite() {
        min = Vec2::ZERO;
        max = Vec2::ZERO;
    }
    out.bounds = (min - Vec2::splat(margin), max + Vec2::splat(margin));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::fixture;
    use crate::model::from_export;

    fn demo_layout() -> (crate::model::SceneGraph, LayoutOutput) {
        let graph = from_export(&fixture());
        let out = layout(&graph);
        (graph, out)
    }

    #[test]
    fn layout_is_deterministic() {
        // Given the demo scene.
        let graph = from_export(&fixture());

        // When laying it out twice.
        let first = layout(&graph);
        let second = layout(&graph);

        // Then both layouts place every node identically.
        assert_eq!(first.nodes, second.nodes);
        assert_eq!(first.containers, second.containers);
        assert_eq!(first.edges, second.edges);
    }

    #[rstest::rstest]
    #[case::pool("pool:api")]
    #[case::partition("part:accounts")]
    fn container_members_do_not_overlap(#[case] container_id: &str) {
        // Given the laid-out demo scene.
        let (graph, out) = demo_layout();
        let container = graph
            .containers
            .iter()
            .find(|c| c.id.0 == container_id)
            .expect("container exists");
        let container_rect = out.containers[&container.id];

        // When checking every member against the container and each
        // other.
        // Then each member lies fully inside the container.
        for member in &container.members {
            let p = out.nodes[member];
            let (min, max) = p.bounds();
            let (c_min, c_max) = container_rect.bounds();
            assert!(
                min.x >= c_min.x && min.y >= c_min.y && max.x <= c_max.x && max.y <= c_max.y,
                "{member:?} escapes its container"
            );
        }
        // And no two members overlap.
        for (index, a) in container.members.iter().enumerate() {
            for b in &container.members[index + 1..] {
                let (a_min, a_max) = out.nodes[a].bounds();
                let (b_min, b_max) = out.nodes[b].bounds();
                let separated = a_max.x <= b_min.x
                    || b_max.x <= a_min.x
                    || a_max.y <= b_min.y
                    || b_max.y <= a_min.y;
                assert!(separated, "{a:?} and {b:?} overlap");
            }
        }
    }

    #[test]
    fn hit_test_returns_topmost_node() {
        // Given a layout where two nodes overlap and the standalone
        // node draws later (front).
        let mut out = LayoutOutput::default();
        let back_id = NodeId("back".into());
        let front_id = NodeId("front".into());
        out.nodes.insert(
            back_id.clone(),
            Placement {
                pos: Vec2::new(0.0, 0.0),
                size: NODE_SIZE,
            },
        );
        out.nodes.insert(
            front_id.clone(),
            Placement {
                pos: Vec2::new(20.0, 0.0),
                size: NODE_SIZE,
            },
        );
        let order = vec![back_id.clone(), front_id.clone()];

        // When hit-testing the overlap region.
        let hit = out.hit_node(&order, Vec2::new(10.0, 0.0));

        // Then the front node wins.
        assert_eq!(hit, Some(&front_id));

        // When hit-testing empty space.
        let miss = out.hit_node(&order, Vec2::new(500.0, 500.0));

        // Then nothing is hit.
        assert_eq!(miss, None);
    }

    #[test]
    fn demo_scene_produces_expected_geometry() {
        // Given the laid-out demo scene.
        let (graph, out) = demo_layout();

        // When reading its geometry.
        // Then every node and container got a placement.
        assert_eq!(out.nodes.len(), graph.nodes.len());
        assert_eq!(out.containers.len(), graph.containers.len());
        // And the single subscription arc has clipped endpoints on the
        // two boxes' borders (watchdog above the topic pseudo-node, so
        // the segment enters vertically).
        assert_eq!(out.edges.len(), 1);
        let edge = &out.edges[0];
        let from = out.nodes[&edge.from];
        let to = out.nodes[&edge.to];
        for endpoint in [edge.a, edge.b] {
            let on_from = from.contains(endpoint);
            let on_to = to.contains(endpoint);
            assert!(
                on_from || on_to,
                "endpoint {endpoint:?} touches neither box"
            );
        }
        // And the bounds cover every placement with margin.
        let (min, max) = out.bounds;
        for placement in out.nodes.values() {
            assert!(placement.pos.x > min.x && placement.pos.x < max.x);
        }
    }
}
