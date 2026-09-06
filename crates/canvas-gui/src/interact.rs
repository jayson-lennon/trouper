//! Input handling and egui UI: pan, zoom, selection, popups, refresh.

use bevy::app::Update;
use bevy::ecs::change_detection::ResMut;
use bevy::ecs::query::With;
use bevy::ecs::resource::Resource;
use bevy::ecs::system::Local;
use bevy::ecs::system::Query;
use bevy::ecs::system::Res;
use bevy::input::ButtonInput;
use bevy::input::keyboard::KeyCode;
use bevy::input::mouse::AccumulatedMouseMotion;
use bevy::input::mouse::AccumulatedMouseScroll;
use bevy::input::mouse::MouseButton;
use bevy::window::PrimaryWindow;
use bevy::window::Window;
use bevy_egui::EguiContexts;
use bevy_egui::egui;

use crate::fetch::FetchCommand;
use crate::model::NodeKind;
use crate::render::FetchChannels;
use crate::render::SceneState;
use crate::render::Selection;
use crate::view;
use crate::view::MAX_ZOOM;
use crate::view::MIN_ZOOM;
use crate::view::ViewTransform;

/// Movement (px) under which a press-release counts as a click, not a
/// drag.
const CLICK_THRESHOLD_PX: f32 = 4.0;
/// Zoom multiplier per wheel line.
const ZOOM_STEP: f32 = 1.1;

/// What the user is doing with the left button this frame.
#[derive(Resource, Default)]
struct DragState {
    /// Whether the left button is currently held.
    held: bool,
    /// Total accumulated motion (px) since the press.
    traveled: f32,
}

/// Frames the camera onto the scene bounds once the first export
/// arrives.
fn frame_camera_once(
    state: Res<SceneState>,
    windows: Query<&Window, With<PrimaryWindow>>,
    mut view: ResMut<ViewTransform>,
    mut framed: Local<bool>,
) {
    if *framed || state.version == 0 {
        return;
    }
    let Ok(window) = windows.single() else {
        return;
    };
    let (min, max) = state.layout.bounds;
    let span = max - min;
    let fit_zoom = {
        let zoom_x = window.width() / span.x.max(1.0);
        let zoom_y = window.height() / span.y.max(1.0);
        zoom_x.min(zoom_y)
    };
    *view = ViewTransform {
        pan: min + span * 0.5,
        zoom: fit_zoom.clamp(MIN_ZOOM, MAX_ZOOM),
    };
    *framed = true;
}

/// Pan (drag), zoom (wheel, cursor-anchored), click selection, and
/// refresh (R key here; button in the legend window).
#[allow(clippy::too_many_arguments)]
fn handle_input(
    mouse_buttons: Res<ButtonInput<MouseButton>>,
    keys: Res<ButtonInput<KeyCode>>,
    motion: Res<AccumulatedMouseMotion>,
    scroll: Res<AccumulatedMouseScroll>,
    windows: Query<&Window, With<PrimaryWindow>>,
    mut drag: ResMut<DragState>,
    mut view: ResMut<ViewTransform>,
    mut selection: ResMut<Selection>,
    state: Res<SceneState>,
    channels: Res<FetchChannels>,
    mut contexts: EguiContexts,
) {
    let Ok(window) = windows.single() else {
        return;
    };
    let Some(cursor_px) = window.cursor_position() else {
        return;
    };
    let cursor = view::Vec2::new(cursor_px.x, cursor_px.y);
    let egui_wants_pointer = contexts
        .ctx_mut()
        .map(|ctx| ctx.egui_wants_pointer_input())
        .unwrap_or(false);
    if egui_wants_pointer {
        return;
    }

    if keys.just_pressed(KeyCode::Escape) {
        selection.node = None;
        selection.cursor_px = None;
    }
    if keys.just_pressed(KeyCode::KeyR) {
        let _ = channels.commands.send(FetchCommand::Refresh);
    }

    if mouse_buttons.just_pressed(MouseButton::Left) {
        drag.held = true;
        drag.traveled = 0.0;
    }
    if drag.held {
        drag.traveled += motion.delta.length();
        let delta = view::Vec2::new(motion.delta.x, motion.delta.y) * view.zoom;
        view.pan = view.pan - delta;
    }
    if mouse_buttons.just_released(MouseButton::Left) {
        drag.held = false;
        if drag.traveled < CLICK_THRESHOLD_PX {
            let world = view.screen_to_world(cursor, window_size(window));
            selection.node = state.layout.hit_node(&state.hit_order, world).cloned();
            if selection.node.is_some() {
                selection.cursor_px = Some(cursor);
            } else {
                selection.cursor_px = None;
            }
        }
    }

    if scroll.delta.y != 0.0 {
        let next = (view.zoom * ZOOM_STEP.powf(scroll.delta.y)).clamp(MIN_ZOOM, MAX_ZOOM);
        *view = view.zoom_at(cursor, window_size(window), next);
    }
}

/// The window size as our view math expects it.
fn window_size(window: &Window) -> view::Vec2 {
    view::Vec2::new(window.width(), window.height())
}

/// Registers the interaction systems: world-space input on Update,
/// egui windows in the primary context pass.
pub fn plugin(app: &mut bevy::app::App) {
    use bevy::ecs::schedule::IntoScheduleConfigs as _;
    use bevy_egui::EguiPrimaryContextPass;
    app.init_resource::<DragState>()
        .add_systems(Update, (frame_camera_once, handle_input))
        .add_systems(EguiPrimaryContextPass, (ui_popups, ui_legend).chain());
}

/// Cursor-anchored popup for the selection, plus the rules legend and
/// status window. Runs in the egui pass.
fn ui_popups(mut contexts: EguiContexts, selection: Res<Selection>, state: Res<SceneState>) {
    let Some(node) = selection.node.as_ref().and_then(|id| state.graph.node(id)) else {
        return;
    };
    if let Ok(ctx) = contexts.ctx_mut() {
        let screen = ctx.input(|input| input.viewport_rect());
        let anchored = selection.cursor_px.unwrap_or(view::Vec2::ZERO);
        let pos = egui::Pos2::new(
            anchored.x.clamp(8.0, (screen.width() - 380.0).max(8.0)),
            anchored.y.clamp(8.0, (screen.height() - 320.0).max(8.0)),
        );
        egui::Window::new(format!("actor · {}", node.path))
            .current_pos(pos)
            .show(ctx, |ui| {
                kind_line(ui, node.kind);
                contract_lines(ui, &node.manifest.handles, "handles");
                contract_lines(ui, &node.manifest.emits, "emits");
                if !node.manifest.subscribes.is_empty() {
                    contract_lines(ui, &node.manifest.subscribes, "subscribes");
                }
                if let Some(cursor) = node.cursor {
                    ui.label(format!("inbox cursor: {cursor}"));
                }
                if let Some(pretty) = node
                    .state
                    .as_ref()
                    .and_then(|value| serde_json::to_string_pretty(value).ok())
                {
                    ui.separator();
                    egui::ScrollArea::vertical()
                        .max_height(180.0)
                        .show(ui, |ui| {
                            ui.monospace(pretty);
                        });
                }
            });
    }
}

/// The legend window: rules, status, controls, refresh button.
fn ui_legend(mut contexts: EguiContexts, state: Res<SceneState>, channels: Res<FetchChannels>) {
    if let Ok(ctx) = contexts.ctx_mut() {
        egui::Window::new("actor canvas")
            .anchor(egui::Align2::LEFT_BOTTOM, [12.0, -12.0])
            .resizable(false)
            .show(ctx, |ui| {
                let age = state
                    .fetched_at
                    .map(|at| format!(" · {}s ago", at.elapsed().as_secs()))
                    .unwrap_or_default();
                ui.label(format!("{}{age}", state.status));
                ui.separator();
                ui.heading("rules");
                for rule in &state.graph.rules {
                    ui.monospace(&rule.text);
                }
                ui.separator();
                ui.label("ES = event-sourced · Service = impure · topic = anchor");
                ui.label("drag: pan · scroll: zoom · click: details · R: refresh");
                if ui.button("refresh").clicked() {
                    let _ = channels.commands.send(FetchCommand::Refresh);
                }
            });
    }
}

/// The colored kind badge line inside a popup.
fn kind_line(ui: &mut egui::Ui, kind: NodeKind) {
    let (name, color) = match kind {
        NodeKind::EventSourced => ("EventSourced", egui::Color32::from_rgb(0x42, 0x75, 0xd4)),
        NodeKind::Service => ("Service", egui::Color32::from_rgb(0xdb, 0x8c, 0x33)),
        NodeKind::Topic => ("topic", egui::Color32::from_rgb(0x5c, 0x9e, 0x61)),
    };
    ui.colored_label(color, name);
}

/// One contract list ("handles: Work@1, …"), or nothing when empty.
fn contract_lines(ui: &mut egui::Ui, schemas: &[String], label: &str) {
    if schemas.is_empty() {
        return;
    }
    ui.label(format!("{label}: {}", schemas.join(", ")));
}
