//! Transport-layer mapping helpers.

use boink::model::{CarState, Controls, Gear};
use proto::race::v1::{
    GetCarStateResponse, Quaternion, SetControlsRequest, Vector3, WheelAngles, WheelSpeeds,
};

/// Convert gRPC controls request into engine controls.
pub(crate) fn proto_to_controls(req: &SetControlsRequest) -> Controls {
    Controls {
        throttle: req.throttle,
        brake: req.brake,
        steer: req.steering,
    }
}

/// Convert engine `CarState` into a gRPC response payload.
pub(crate) fn car_state_to_proto(car_id: u64, state: CarState) -> GetCarStateResponse {
    let front_left_speed = *state.wheel_speeds.get(0).unwrap_or(&0.0);
    let front_right_speed = *state.wheel_speeds.get(1).unwrap_or(&0.0);
    let rear_left_speed = *state.wheel_speeds.get(2).unwrap_or(&0.0);
    let rear_right_speed = *state.wheel_speeds.get(3).unwrap_or(&0.0);

    let front_left_angle = *state.wheel_angles.get(0).unwrap_or(&0.0);
    let front_right_angle = *state.wheel_angles.get(1).unwrap_or(&0.0);

    if state.wheel_speeds.len() < 4 || state.wheel_angles.len() < 2 {
        tracing::warn!(
            wheel_speeds_len = state.wheel_speeds.len(),
            wheel_angles_len = state.wheel_angles.len(),
            "engine returned incomplete wheel data; defaulting to zeros"
        );
    }

    GetCarStateResponse {
        car_id,
        position: Some(Vector3 {
            x: state.position.x,
            y: state.position.y,
            z: state.position.z,
        }),
        orientation: Some(Quaternion {
            x: state.orientation.x,
            y: state.orientation.y,
            z: state.orientation.z,
            w: state.orientation.w,
        }),
        gear: match state.gear {
            Gear::Reverse => -1,
            Gear::Neutral => 0,
            Gear::Forward(n) => n as i32,
        },
        speed_mps: state.speed,
        engine_rpm: state.engine_rpm,
        throttle_applied: state.throttle_applied,
        brake_applied: state.brake_applied,
        wheel_speeds: Some(WheelSpeeds {
            front_left_rps: front_left_speed,
            front_right_rps: front_right_speed,
            rear_left_rps: rear_left_speed,
            rear_right_rps: rear_right_speed,
        }),
        wheel_steering: Some(WheelAngles {
            front_left_rad: front_left_angle,
            front_right_rad: front_right_angle,
        }),
    }
}
