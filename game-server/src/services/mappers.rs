//! Transport-layer mapping helpers.

use boink::model::{CarState, Controls, Gear};
use proto::race::v1::{
    CarKinematics, CarParticipantState, CarRenderState, FrontendCarFullState, Quaternion,
    SetControlsRequest, Vector3, WheelAngles, WheelSpeeds,
};

/// Convert gRPC controls request into engine controls.
pub(crate) fn proto_to_controls(req: &SetControlsRequest) -> Controls {
    Controls {
        throttle: req.throttle,
        brake: req.brake,
        steer: req.steering,
    }
}

fn wheel_speeds_from_state(state: &CarState) -> WheelSpeeds {
    let front_left_speed = *state.wheel_speeds.get(0).unwrap_or(&0.0);
    let front_right_speed = *state.wheel_speeds.get(1).unwrap_or(&0.0);
    let rear_left_speed = *state.wheel_speeds.get(2).unwrap_or(&0.0);
    let rear_right_speed = *state.wheel_speeds.get(3).unwrap_or(&0.0);

    if state.wheel_speeds.len() < 4 {
        tracing::warn!(
            wheel_speeds_len = state.wheel_speeds.len(),
            "engine returned incomplete wheel speed data; defaulting to zeros"
        );
    }

    WheelSpeeds {
        front_left_rps: front_left_speed,
        front_right_rps: front_right_speed,
        rear_left_rps: rear_left_speed,
        rear_right_rps: rear_right_speed,
    }
}

fn wheel_angles_from_state(state: &CarState) -> WheelAngles {
    let front_left_angle = *state.wheel_angles.get(0).unwrap_or(&0.0);
    let front_right_angle = *state.wheel_angles.get(1).unwrap_or(&0.0);

    if state.wheel_angles.len() < 2 {
        tracing::warn!(
            wheel_angles_len = state.wheel_angles.len(),
            "engine returned incomplete wheel angle data; defaulting to zeros"
        );
    }

    WheelAngles {
        front_left_rad: front_left_angle,
        front_right_rad: front_right_angle,
    }
}

fn kinematics_from_state(state: &CarState) -> CarKinematics {
    CarKinematics {
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
    }
}

fn participant_state_from_state(
    state: &CarState,
    last_applied_client_seq: u64,
) -> CarParticipantState {
    CarParticipantState {
        last_applied_client_seq,
        speed_mps: state.speed,
        engine_rpm: state.engine_rpm,
        gear: match state.gear {
            Gear::Reverse => -1,
            Gear::Neutral => 0,
            Gear::Forward(n) => n as i32,
        },
        throttle_applied: state.throttle_applied,
        brake_applied: state.brake_applied,
        wheel_angles: Some(wheel_angles_from_state(state)),
    }
}

fn render_state_from_state(state: &CarState) -> CarRenderState {
    CarRenderState {
        wheel_speeds: Some(wheel_speeds_from_state(state)),
    }
}

/// Convert engine `CarState` into frontend spectator/participant full state.
pub(crate) fn frontend_full_state(
    car_id: u64,
    state: CarState,
    last_applied_client_seq: u64,
) -> FrontendCarFullState {
    FrontendCarFullState {
        car_id,
        kinematics: Some(kinematics_from_state(&state)),
        telemetry: Some(participant_state_from_state(
            &state,
            last_applied_client_seq,
        )),
        render: Some(render_state_from_state(&state)),
    }
}
