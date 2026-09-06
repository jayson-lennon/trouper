//! Binary entry point for the canvas GUI.
//!
//! Hosting responsibilities only — topology mapping lives in [`crate::model`],
//! positions in [`crate::layout`], view math in [`crate::view`].

use bevy::prelude::*;
use bevy::render::RenderPlugin;
use bevy::window::WindowPlugin;
use bevy::winit::WinitPlugin;
use bevy_egui::EguiContexts;
use bevy_egui::EguiPlugin;
use bevy_egui::EguiPrimaryContextPass;

#[cfg(test)]
mod fixture;
mod layout;
mod model;
mod view;

fn spawn_camera(mut commands: Commands) {
    // Given the scaffold camera shell.
    // When the window opens.
    // Then a 2D camera exists for scene entities to render into.
    commands.spawn(Camera2d);
}

fn egui_shell(mut contexts: EguiContexts) {
    // Given the scaffold egui shell.
    // When the primary context renders its pass each frame.
    // Then the context is acquirable and the empty frame draws without error.
    // (Popups and the legend join this schedule in later phases.)
    let _ctx = contexts.ctx_mut();
}

fn main() {
    App::new()
        .add_plugins((
            DefaultPlugins
                .set(WindowPlugin {
                    primary_window: Some(Window {
                        title: "actor canvas".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .set(RenderPlugin::default())
                .set(WinitPlugin::default()),
            EguiPlugin::default(),
        ))
        .add_systems(Startup, spawn_camera)
        .add_systems(EguiPrimaryContextPass, egui_shell)
        .run();
}
