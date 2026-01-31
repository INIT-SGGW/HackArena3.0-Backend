//! Transport-layer mapping helpers.

use boink::model::{Controls, Gear, VehicleState};
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

fn wheel_speeds_from_state(state: &VehicleState) -> WheelSpeeds {
    WheelSpeeds {
        front_left_rps: state.wheel_speeds[0],
        front_right_rps: state.wheel_speeds[1],
        rear_left_rps: state.wheel_speeds[2],
        rear_right_rps: state.wheel_speeds[3],
    }
}

fn wheel_angles_from_state(_state: &VehicleState) -> WheelAngles {
    WheelAngles {
        front_left_rad: 0.0,
        front_right_rad: 0.0,
    }
}

fn kinematics_from_state(state: &VehicleState) -> CarKinematics {
    CarKinematics {
        position: Some(Vector3 {
            x: state.chassis_position.x,
            y: state.chassis_position.y,
            z: state.chassis_position.z,
        }),
        orientation: Some(Quaternion {
            x: state.vehicle_orientation.x,
            y: state.vehicle_orientation.y,
            z: state.vehicle_orientation.z,
            w: state.vehicle_orientation.w,
        }),
    }
}

fn participant_state_from_state(
    state: &VehicleState,
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

fn render_state_from_state(state: &VehicleState) -> CarRenderState {
    CarRenderState {
        wheel_speeds: Some(wheel_speeds_from_state(state)),
    }
}

/// Convert engine `VehicleState` into frontend spectator/participant full state.
pub(crate) fn frontend_full_state(
    car_id: u64,
    state: VehicleState,
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
