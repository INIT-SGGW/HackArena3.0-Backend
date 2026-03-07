//! Transport-layer mapping helpers.

use boink::model::{
    Controls, Gear, GearShift as EngineGearShift, TrackData as EngineTrackData, VehicleState,
};
use proto::race::v1::{
    CarKinematics, CarParticipantState, CarRenderState, CenterlineSample, FrontendCarFullState,
    GearShift as ProtoGearShift, ParticipantOpponentState, ParticipantSelfState, Quaternion,
    SetControlsDevRequest, SetControlsRequest, TrackData as ProtoTrackData, Vector3, WheelAngles,
    WheelSpeeds,
};
use tonic::Status;

/// Convert engine `Vec3` into proto `Vector3`.
pub(crate) fn vec3_to_proto(v: boink::model::Vec3) -> Vector3 {
    Vector3 {
        x: v.x,
        y: v.y,
        z: v.z,
    }
}

/// Convert gRPC controls request into engine controls.
pub(crate) fn proto_to_controls(req: &SetControlsRequest) -> Result<Controls, Status> {
    controls_from_proto(req.throttle, req.brake, req.steering, req.gear_shift)
}

/// Convert gRPC dev-controls request into engine controls.
pub(crate) fn proto_dev_to_controls(req: &SetControlsDevRequest) -> Result<Controls, Status> {
    controls_from_proto(req.throttle, req.brake, req.steering, req.gear_shift)
}

/// Convert engine gear-shift response into protobuf enum value.
pub(crate) fn engine_gear_shift_to_proto(shift: EngineGearShift) -> i32 {
    match shift {
        EngineGearShift::None => ProtoGearShift::None as i32,
        EngineGearShift::Upshift => ProtoGearShift::Upshift as i32,
        EngineGearShift::Downshift => ProtoGearShift::Downshift as i32,
    }
}

fn controls_from_proto(
    throttle: f32,
    brake: f32,
    steering: f32,
    raw_gear_shift: i32,
) -> Result<Controls, Status> {
    Ok(Controls::new(
        throttle,
        brake,
        steering,
        proto_gear_shift_to_engine(raw_gear_shift)?,
    ))
}

fn proto_gear_shift_to_engine(raw_gear_shift: i32) -> Result<EngineGearShift, Status> {
    let gear_shift = ProtoGearShift::try_from(raw_gear_shift)
        .map_err(|_| Status::invalid_argument("invalid gear_shift"))?;

    Ok(match gear_shift {
        ProtoGearShift::Unspecified | ProtoGearShift::None => EngineGearShift::None,
        ProtoGearShift::Upshift => EngineGearShift::Upshift,
        ProtoGearShift::Downshift => EngineGearShift::Downshift,
    })
}

fn wheel_speeds_from_state(state: &VehicleState) -> WheelSpeeds {
    WheelSpeeds {
        front_left_radps: state.wheel_speeds[0],
        front_right_radps: state.wheel_speeds[1],
        rear_left_radps: state.wheel_speeds[2],
        rear_right_radps: state.wheel_speeds[3],
    }
}

fn wheel_angles_from_state(_state: &VehicleState) -> WheelAngles {
    WheelAngles {
        front_left_rad: 0.0,
        front_right_rad: 0.0,
    }
}

pub(crate) fn participant_kinematics_from_state(state: &VehicleState) -> CarKinematics {
    CarKinematics {
        position: Some(vec3_to_proto(state.chassis_position)),
        orientation: Some(Quaternion {
            x: state.vehicle_orientation.x,
            y: state.vehicle_orientation.y,
            z: state.vehicle_orientation.z,
            w: state.vehicle_orientation.w,
        }),
    }
}

pub(crate) fn participant_telemetry_from_state(
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
        kinematics: Some(participant_kinematics_from_state(&state)),
        telemetry: Some(participant_telemetry_from_state(
            &state,
            last_applied_client_seq,
        )),
        render: Some(render_state_from_state(&state)),
    }
}

/// Convert engine `VehicleState` into participant self state.
pub(crate) fn participant_self_state(
    car_id: u64,
    state: VehicleState,
    last_applied_client_seq: u64,
) -> ParticipantSelfState {
    ParticipantSelfState {
        car_id,
        kinematics: Some(participant_kinematics_from_state(&state)),
        telemetry: Some(participant_telemetry_from_state(
            &state,
            last_applied_client_seq,
        )),
    }
}

/// Convert engine `VehicleState` into participant opponent state.
pub(crate) fn participant_opponent_state(
    car_id: u64,
    state: VehicleState,
) -> ParticipantOpponentState {
    ParticipantOpponentState {
        car_id,
        kinematics: Some(participant_kinematics_from_state(&state)),
    }
}

/// Convert engine `TrackData` into proto `TrackData`.
pub(crate) fn track_data_to_proto(track: EngineTrackData) -> ProtoTrackData {
    let centerline_samples = track
        .centerline_samples
        .into_iter()
        .map(|sample| CenterlineSample {
            s_m: sample.s_m,
            position: Some(vec3_to_proto(sample.position)),
            tangent: Some(vec3_to_proto(sample.tangent)),
            normal: Some(vec3_to_proto(sample.normal)),
            right: Some(vec3_to_proto(sample.right)),
            left_width_m: sample.left_width_m,
            right_width_m: sample.right_width_m,
            curvature_1pm: sample.curvature_1pm,
            grade_rad: sample.grade_rad,
            bank_rad: sample.bank_rad,
        })
        .collect();

    ProtoTrackData {
        map_id: track.map_id,
        lap_length_m: track.lap_length_m,
        centerline_samples,
    }
}
