use bevy::prelude::*;
use bevy::input::mouse::AccumulatedMouseMotion;
use std::{f32::consts::FRAC_PI_2, ops::Range};

pub struct PreviewPlugin;

impl Plugin for PreviewPlugin{
    fn build(&self, app: &mut App){
        app.init_resource::<CameraSettings>();
        app.add_systems(Startup,setup);
        app.add_systems(Update, rotation);
        app.add_systems(Update, movement);
    }
}

#[derive(Debug, Resource)]
struct CameraSettings {
    pub pitch_speed: f32,
    pub pitch_range: Range<f32>,
    pub yaw_speed: f32,

    pub move_speed: f32,
}

impl Default for CameraSettings {
    fn default() -> Self {
        // Limiting pitch stops some unexpected rotation past 90° up or down.
        let pitch_limit = FRAC_PI_2 - 0.01;
        Self {
            pitch_speed: 0.003,
            pitch_range: -pitch_limit..pitch_limit,
            yaw_speed: 0.004,
            move_speed: 100.0,
        }
    }
}

fn setup(mut commands: Commands) {
    commands.spawn((
        Name::new("Camera"),
        Camera3d::default(),
        Transform::from_xyz(5.0, 5.0, 5.0).looking_at(Vec3::ZERO, Vec3::Y),
    ));
}

fn movement(
    mut camera: Single<&mut Transform, With<Camera>>,
    camera_settings: Res<CameraSettings>,
    keyboard_input: Res<ButtonInput<KeyCode>>,
    time: Res<Time>,
){
    let mut direction=Vec3::ZERO;
    if keyboard_input.pressed(KeyCode::KeyA){
        direction+=Vec3::new(-1.0,0.0,0.0);
    }
    if keyboard_input.pressed(KeyCode::KeyD){
        direction+=Vec3::new(1.0,0.0,0.0);
    }
    if keyboard_input.pressed(KeyCode::KeyW){
        direction+=Vec3::new(0.0,0.0,-1.0);
    }
    if keyboard_input.pressed(KeyCode::KeyS){
        direction+=Vec3::new(0.0,0.0,1.0);
    }
    if keyboard_input.pressed(KeyCode::ControlLeft){
        direction+=Vec3::new(0.0,-1.0,0.0);
    }
    if keyboard_input.pressed(KeyCode::ShiftLeft){
        direction+=Vec3::new(0.0,1.0,0.0);
    }
    
    if direction.length()>0.0{
        direction=direction.normalize();
    }

    direction=camera.rotation*direction;
    camera.translation+=direction*camera_settings.move_speed*time.delta_secs();
}

fn rotation(
    mut camera: Single<&mut Transform, With<Camera>>,
    camera_settings: Res<CameraSettings>,
    mouse_motion: Res<AccumulatedMouseMotion>
) {
    let delta = mouse_motion.delta;

    let delta_pitch = -delta.y * camera_settings.pitch_speed;
    let delta_yaw = -delta.x * camera_settings.yaw_speed;

    let (yaw, pitch, roll) = camera.rotation.to_euler(EulerRot::YXZ);

    let pitch = (pitch + delta_pitch).clamp(
        camera_settings.pitch_range.start,
        camera_settings.pitch_range.end,
    );
    let yaw = yaw + delta_yaw;

    camera.rotation = Quat::from_euler(EulerRot::YXZ, yaw, pitch, roll);
}
