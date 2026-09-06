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

mod fetch;
#[cfg(test)]
mod fixture;
mod layout;
mod model;
mod render;
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
    // Given the GUI shell.
    // When the fetch thread is started and the render plugin wired.
    // Then an initial refresh is requested so the first export
    // arrives as soon as zenoh answers.
    let (command_tx, command_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let fetcher = fetch::spawn_fetch_thread(command_rx, result_tx);
    command_tx
        .send(fetch::FetchCommand::Refresh)
        .expect("fetch thread alive");

    let mut app = App::new();
    app.add_plugins((
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
    .insert_resource(render::FetchChannels {
        commands: command_tx,
        results: std::sync::Mutex::new(result_rx),
    })
    .add_systems(Startup, spawn_camera);
    render::plugin(&mut app);
    app.add_systems(EguiPrimaryContextPass, egui_shell);
    app.run();
    let _ = fetcher.join();
}
