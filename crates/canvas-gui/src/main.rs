//! Binary entry point for the canvas GUI.
//!
//! Hosting responsibilities only — topology mapping lives in [`crate::model`],
//! positions in [`crate::layout`], view math in [`crate::view`].

use bevy::prelude::*;
use bevy::render::RenderPlugin;
use bevy::window::WindowPlugin;
use bevy::winit::WinitPlugin;
use bevy_egui::EguiPlugin;

fn spawn_camera(mut commands: Commands) {
    // Given the scaffold camera shell.
    // When the window opens.
    // Then a 2D camera exists for scene entities to render into.
    commands.spawn(Camera2d);
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
        .run();
}
