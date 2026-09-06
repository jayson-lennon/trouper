//! Bevy rendering: drain fetch results, (re)build scene entities from
//! the laid-out scene, and apply the view transform to the camera.
//!
//! This module owns no logic of its own — model, layout, and view math
//! are pure; these systems only copy prepared data into entities.

use bevy::asset::Assets;
use bevy::camera::Camera2d;
use bevy::camera::Projection;
use bevy::color::Color;
use bevy::ecs::change_detection::ResMut;
use bevy::ecs::component::Component;
use bevy::ecs::entity::Entity;
use bevy::ecs::query::With;
use bevy::ecs::resource::Resource;
use bevy::ecs::schedule::IntoScheduleConfigs as _;
use bevy::ecs::system::Commands;
use bevy::ecs::system::Local;
use bevy::ecs::system::Query;
use bevy::ecs::system::Res;
use bevy::math::Quat;
use bevy::math::Vec3;
use bevy::math::primitives::Rectangle;
use bevy::mesh::Mesh2d;
use bevy::render::mesh::Mesh;
use bevy::sprite::Text2d;
use bevy::sprite_render::ColorMaterial;
use bevy::sprite_render::MeshMaterial2d;
use bevy::text::TextColor;
use bevy::text::TextFont;
use bevy::transform::components::Transform;

use crate::fetch::ExportMsg;
use crate::layout::layout;
use crate::model::Node;
use crate::model::NodeId;
use crate::model::NodeKind;
use crate::model::SceneGraph;
use crate::model::from_export;
use crate::view::Vec2;
use crate::view::ViewTransform;

/// The fetch plumbing, as a bevy resource. The receiver half is
/// `Mutex`-wrapped because bevy resources must be `Sync`.
#[derive(Resource)]
pub struct FetchChannels {
    /// The sender the GUI uses to request refreshes.
    pub commands: std::sync::mpsc::Sender<crate::fetch::FetchCommand>,
    /// Fresh results from the fetch thread.
    pub results: std::sync::Mutex<std::sync::mpsc::Receiver<ExportMsg>>,
}

impl std::fmt::Debug for FetchChannels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchChannels").finish_non_exhaustive()
    }
}

/// Node z, above edges and containers.
const Z_NODE: f32 = 0.0;
/// Edge z, above the container fills but below nodes.
const Z_EDGE: f32 = -5.0;
/// Container fill z, below everything drawn.
const Z_CONTAINER: f32 = -10.0;
/// Edge thickness in world units.
const EDGE_WIDTH: f32 = 2.5;

/// Marker for every entity that belongs to the drawn scene (dropped
/// and rebuilt together when the scene version changes).
#[derive(Component)]
pub struct SceneEntity;

/// The latest fetched scene and its layout, rebuilt as a unit.
#[derive(Resource)]
pub struct SceneState {
    /// The mapped scene graph.
    pub graph: SceneGraph,
    /// The deterministic layout of `graph`.
    pub layout: crate::layout::LayoutOutput,
    /// Node ids in draw order (back to front), for hit-testing.
    pub hit_order: Vec<NodeId>,
    /// Bumped on every applied export so the renderer rebuilds once.
    pub version: u64,
    /// Human-readable status (waiting / fetched / fetch error).
    pub status: String,
    /// When the displayed export was fetched (drives the age label).
    pub fetched_at: Option<std::time::Instant>,
}

impl Default for SceneState {
    fn default() -> Self {
        Self {
            graph: SceneGraph::default(),
            layout: crate::layout::LayoutOutput::default(),
            hit_order: Vec::new(),
            version: 0,
            status: "waiting for first export…".into(),
            fetched_at: None,
        }
    }
}

/// The current selection (written by the interaction module).
#[derive(Resource, Default)]
pub struct Selection {
    /// The selected node, if any.
    pub node: Option<NodeId>,
    /// Where the selecting click happened (screen px).
    pub cursor_px: Option<Vec2>,
}

/// Fills [`SceneState`] from the newest fetch result.
pub fn drain_fetch(mut state: ResMut<SceneState>, channels: Res<FetchChannels>) {
    let Ok(results) = channels.results.lock() else {
        return;
    };
    while let Ok(message) = results.try_recv() {
        match message.export {
            Ok(export) => {
                let graph = from_export(&export);
                let laid = layout(&graph);
                let hit_order = hit_order_of(&graph);
                let status = format!(
                    "fetched {} actors · {} pools · {} partitions",
                    export.actors.len(),
                    export.pools.len(),
                    export.partitions.len()
                );
                bevy::log::info!(
                    "scene v{} applied: {status}, {} rules, {} drawn arcs",
                    state.version + 1,
                    graph.rules.len(),
                    laid.edges.len()
                );
                *state = SceneState {
                    graph,
                    layout: laid,
                    hit_order,
                    version: state.version + 1,
                    status,
                    fetched_at: Some(message.at),
                };
            }
            Err(error) => {
                state.status = format!("fetch failed: {error}");
            }
        }
    }
}

/// Node ids in draw order: container members first (back), then
/// standalone actors, then topic pseudo-nodes (front) — matching the
/// layout's build order so hit tests agree with what's on top.
fn hit_order_of(graph: &SceneGraph) -> Vec<NodeId> {
    let mut order: Vec<NodeId> = graph
        .containers
        .iter()
        .flat_map(|container| container.members.clone())
        .collect();
    for node in &graph.nodes {
        if node.kind != NodeKind::Topic && !order.contains(&node.id) {
            order.push(node.id.clone());
        }
    }
    for node in &graph.nodes {
        if node.kind == NodeKind::Topic {
            order.push(node.id.clone());
        }
    }
    order
}

/// Drops stale scene entities and spawns the new ones whenever the
/// scene version changed (bevy `despawn` recurses into children, so
/// labels go with their boxes).
pub fn build_scene(
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
    state: Res<SceneState>,
    existing: Query<Entity, With<SceneEntity>>,
    mut last_version: Local<u64>,
) {
    if state.version == *last_version {
        return;
    }
    *last_version = state.version;
    for entity in &existing {
        commands.entity(entity).despawn();
    }
    if state.version == 0 {
        return;
    }
    let mesh = meshes.add(Rectangle::default());
    let container_material = materials.add(Color::srgba(0.16, 0.19, 0.24, 0.30));
    let edge_material = materials.add(Color::srgb(0.55, 0.58, 0.62));
    let es_material = materials.add(Color::srgb(0.26, 0.46, 0.83));
    let service_material = materials.add(Color::srgb(0.86, 0.55, 0.20));
    let topic_material = materials.add(Color::srgb(0.36, 0.62, 0.38));

    for container in &state.graph.containers {
        let Some(placement) = state.layout.containers.get(&container.id) else {
            continue;
        };
        // The layout reserves CONTAINER_LABEL at the container top.
        let label_offset = Vec2::new(0.0, placement.size.y * 0.5 - 14.0);
        spawn_box(
            &mut commands,
            &mesh,
            container_material.clone(),
            *placement,
            Z_CONTAINER,
        );
        spawn_label(
            &mut commands,
            container.label.clone(),
            16.0,
            Color::srgb(0.85, 0.87, 0.92),
            placement.pos + label_offset,
        );
    }

    for edge in &state.layout.edges {
        let delta = edge.b - edge.a;
        let length = delta.length();
        if length < f32::EPSILON {
            continue;
        }
        let angle = delta.y.atan2(delta.x);
        let mid = Vec2::new((edge.a.x + edge.b.x) * 0.5, (edge.a.y + edge.b.y) * 0.5);
        commands.spawn((
            SceneEntity,
            Mesh2d(mesh.clone()),
            MeshMaterial2d(edge_material.clone()),
            Transform::from_xyz(mid.x, mid.y, Z_EDGE)
                .with_rotation(Quat::from_rotation_z(angle))
                .with_scale(Vec3::new(length, EDGE_WIDTH, 1.0)),
        ));
    }

    for node in &state.graph.nodes {
        let Some(placement) = state.layout.nodes.get(&node.id) else {
            continue;
        };
        let material = match node.kind {
            NodeKind::EventSourced => es_material.clone(),
            NodeKind::Service => service_material.clone(),
            NodeKind::Topic => topic_material.clone(),
        };
        spawn_box(&mut commands, &mesh, material, *placement, Z_NODE);
        spawn_label(
            &mut commands,
            node.path.clone(),
            19.0,
            Color::srgb(0.94, 0.95, 0.97),
            placement.pos + Vec2::new(0.0, 18.0),
        );
        spawn_label(
            &mut commands,
            node_summary(node),
            13.0,
            Color::srgb(0.75, 0.78, 0.83),
            placement.pos + Vec2::new(0.0, -4.0),
        );
        if let Some(cursor_note) = node.cursor.map(|cursor| format!("cursor {cursor}")) {
            spawn_label(
                &mut commands,
                cursor_note,
                12.0,
                Color::srgb(0.60, 0.64, 0.70),
                placement.pos + Vec2::new(0.0, -26.0),
            );
        }
    }
}

/// Spawns an independent label entity (never a child of the scaled
/// box, so text keeps its glyph size) at an absolute world position.
fn spawn_label(
    commands: &mut Commands,
    text: String,
    font_size: f32,
    color: Color,
    world_pos: Vec2,
) {
    commands.spawn((
        SceneEntity,
        Text2d::new(text),
        TextFont::from_font_size(font_size),
        TextColor(color),
        Transform::from_xyz(world_pos.x, world_pos.y, Z_NODE + 1.0),
    ));
}

/// Spawns one scaled quad (container fill, node box) with the given
/// material; labels are separate `SceneEntity` entities so they never
/// inherit the box's scale.
fn spawn_box<'a>(
    commands: &'a mut Commands,
    mesh: &bevy::asset::Handle<Mesh>,
    material: bevy::asset::Handle<ColorMaterial>,
    placement: crate::layout::Placement,
    z: f32,
) -> bevy::ecs::system::EntityCommands<'a> {
    commands.spawn((
        SceneEntity,
        Mesh2d(mesh.clone()),
        MeshMaterial2d(material),
        Transform::from_xyz(placement.pos.x, placement.pos.y, z).with_scale(Vec3::new(
            placement.size.x,
            placement.size.y,
            1.0,
        )),
    ))
}

/// The one-line summary inside a node box.
fn node_summary(node: &Node) -> String {
    match node.kind {
        NodeKind::Topic => "topic".into(),
        _ => format!(
            "{} · handles {} · emits {}",
            node.kind.label(),
            node.manifest.handles.len(),
            node.manifest.emits.len()
        ),
    }
}

/// Pushes the view transform into the 2D camera.
pub fn apply_view(
    view: Res<ViewTransform>,
    mut cameras: Query<(&mut Transform, &mut Projection), With<Camera2d>>,
) {
    let Ok((mut transform, mut projection)) = cameras.single_mut() else {
        return;
    };
    let bevy::camera::Projection::Orthographic(orthographic) = &mut *projection else {
        return;
    };
    transform.translation = Vec3::new(view.pan.x, view.pan.y, 0.0);
    orthographic.scale = 1.0 / view.zoom;
}

/// Registers the render systems.
pub fn plugin(app: &mut bevy::app::App) {
    app.init_resource::<SceneState>()
        .init_resource::<Selection>()
        .init_resource::<ViewTransform>()
        .add_systems(
            bevy::app::Update,
            (drain_fetch, build_scene, apply_view).chain(),
        );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::fixture;
    use bevy::ecs::system::RunSystemOnce;
    use bevy::ecs::world::World;

    use crate::fetch::FetchCommand;

    /// The demo export with the reporter's `seq` field set, mimicking
    /// what a later fetch returns as the reporter journals reports.
    fn demo_export_with_seq(seq: i64) -> actor_runtime::system::SystemExport {
        let mut export = fixture();
        let reporter = export
            .actors
            .iter_mut()
            .find(|actor| actor.path.as_str() == "state/reporter")
            .expect("reporter in fixture");
        if let Some(object) = reporter
            .state
            .as_mut()
            .and_then(|state| state.as_object_mut())
        {
            object.insert("seq".into(), serde_json::json!(seq));
        }
        export
    }

    #[test]
    fn drain_fetch_applies_export_into_scene_state() {
        // Given a world with the fetch channels holding one canned
        // export (seq = 3).
        let mut world = World::default();
        let (command_tx, command_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        result_tx
            .send(ExportMsg {
                export: Ok(demo_export_with_seq(3)),
                at: std::time::Instant::now(),
            })
            .expect("send");
        world.insert_resource(SceneState::default());
        world.insert_resource(FetchChannels {
            commands: command_tx,
            results: std::sync::Mutex::new(result_rx),
        });
        let _ = command_rx; // receiver dropped by the fetch thread in prod

        // When draining the fetch results.
        world.run_system_once(drain_fetch).expect("system runs");

        // Then the scene state holds the mapped graph with the seq
        // visible in the reporter's stored state, and the version
        // advanced.
        let state = world.resource::<SceneState>();
        assert_eq!(state.version, 1);
        assert_eq!(state.graph.nodes.len(), 7);
        let reporter = state
            .graph
            .node(&NodeId("state/reporter".into()))
            .expect("reporter node");
        assert_eq!(reporter.state.as_ref().expect("state")["seq"], 3);
        assert!(state.fetched_at.is_some());
    }

    #[test]
    fn drain_fetch_bumps_version_on_refresh() {
        // Given a world that already applied one export (seq = 0) and
        // a second, fresher export waiting in the channel (seq = 7).
        let mut world = World::default();
        let (command_tx, _command_rx) = std::sync::mpsc::channel::<FetchCommand>();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        result_tx
            .send(ExportMsg {
                export: Ok(demo_export_with_seq(0)),
                at: std::time::Instant::now(),
            })
            .expect("send");
        world.insert_resource(SceneState::default());
        world.insert_resource(FetchChannels {
            commands: command_tx,
            results: std::sync::Mutex::new(result_rx),
        });
        world.run_system_once(drain_fetch).expect("system runs");

        // When the refresh result arrives and drains again.
        result_tx
            .send(ExportMsg {
                export: Ok(demo_export_with_seq(7)),
                at: std::time::Instant::now(),
            })
            .expect("send");
        world.run_system_once(drain_fetch).expect("system runs");

        // Then the version bumped and the reporter's seq advanced.
        let state = world.resource::<SceneState>();
        assert_eq!(state.version, 2);
        let reporter = state
            .graph
            .node(&NodeId("state/reporter".into()))
            .expect("reporter");
        assert_eq!(reporter.state.as_ref().expect("state")["seq"], 7);
    }
}
