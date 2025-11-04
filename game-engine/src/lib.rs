mod plugins;

use plugins::preview_plugin::PreviewPlugin;

use bevy::prelude::*;
use bevy_rapier3d::prelude::*;

pub fn start_engine() {
    App::new()
        .add_plugins(DefaultPlugins)
        .add_plugins(RapierPhysicsPlugin::<NoUserData>::default())
        .add_plugins(RapierDebugRenderPlugin::default())
        .add_plugins(PreviewPlugin)
        .add_systems(Startup, spawn_gltf)
        .run();
}

fn spawn_gltf(mut commands: Commands, asset_server: Res<AssetServer>) {
    commands.spawn((
        // This is equivalent to "models/FlightHelmet/FlightHelmet.gltf#Scene0"
        // The `#Scene0` label here is very important because it tells 
        // bevy to load the first scene in the glTF file.
        // If this isn't specified bevy doesn't know which part of the glTF file to load.
        SceneRoot(
            asset_server.load(
                GltfAssetLabel::Scene(0).from_asset(
                    "Bolid_Tor_test.glb"))),
    ));
}


