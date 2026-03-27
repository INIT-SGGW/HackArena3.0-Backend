//! Transport-layer mapping helpers.

use boink::model::{
    Controls, GHOST_MODE_BLOCKER_EXIT_DELAY_RUNNING, GHOST_MODE_BLOCKER_EXIT_SPEED_NOT_MET,
    GHOST_MODE_BLOCKER_IN_PIT, GHOST_MODE_BLOCKER_LAPS_REQUIREMENT_NOT_MET,
    GHOST_MODE_BLOCKER_OVERLAP_EXIT_DELAY_RUNNING, GHOST_MODE_BLOCKER_VEHICLE_OVERLAP_ACTIVE, Gear,
    GearShift as EngineGearShift, GhostModePhase as EngineGhostModePhase, GhostModeRuntimeState,
    GroundType, TrackData as EngineTrackData, TyreType, VehicleState,
};
use proto::race::v1::{
    CarKinematics, CarParticipantState, CarRenderState, CenterlineSample, CommandCooldownState,
    FrontendCarFullState, GearShift as ProtoGearShift, GhostModeBlocker as ProtoGhostModeBlocker,
    GhostModePhase as ProtoGhostModePhase, GhostModeState, GroundType as ProtoGroundType,
    GroundWidth as ProtoGroundWidth, ParticipantOpponentState, ParticipantSelfState,
    PitEntrySource as ProtoPitEntrySource, PitHistoryEntry as ProtoPitHistoryEntry,
    PitHistoryState as ProtoPitHistoryState, PitRuntimeState, PitstopData as ProtoPitstopData,
    Quaternion, SetControlsDevRequest, TireSlipPerWheel, TireTemperaturePerWheel,
    TireType as ProtoTireType, TireWearPerWheel, TrackData as ProtoTrackData, Vector3, WheelSpeeds,
    participant_client_message::Payload as ParticipantClientPayload,
};
use tonic::Status;

use super::race::runtime_store::{
    RuntimeControlInputSnapshot, RuntimePitEntrySource, RuntimePitStateSnapshot, RuntimePitTireType,
};

/// Convert engine `Vec3` into proto `Vector3`.
pub(crate) fn vec3_to_proto(v: boink::model::Vec3) -> Vector3 {
    Vector3 {
        x: v.x,
        y: v.y,
        z: v.z,
    }
}

/// Convert gRPC dev-controls request into engine controls.
pub(crate) fn proto_dev_to_controls(req: &SetControlsDevRequest) -> Result<Controls, Status> {
    controls_from_proto(
        req.throttle,
        req.brake,
        0.5,
        0.0,
        req.steering,
        req.gear_shift,
    )
}

/// Convert participant bidi controls payload into engine controls.
pub(crate) fn proto_participant_controls_to_controls(
    payload: &ParticipantClientPayload,
) -> Result<Option<(u64, Controls)>, Status> {
    match payload {
        ParticipantClientPayload::Controls(value) => Ok(Some((
            value.client_seq,
            controls_from_proto(
                value.throttle,
                value.brake,
                value.brake_balancer,
                value.differential_lock,
                value.steering,
                value.gear_shift,
            )?,
        ))),
        ParticipantClientPayload::Init(_)
        | ParticipantClientPayload::BackToTrack(_)
        | ParticipantClientPayload::EmergencyPitstop(_)
        | ParticipantClientPayload::SetNextPitTireType(_) => Ok(None),
    }
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
    brake_balancer: f32,
    differential_lock: f32,
    steering: f32,
    raw_gear_shift: i32,
) -> Result<Controls, Status> {
    Ok(Controls::new(
        throttle,
        brake,
        brake_balancer,
        differential_lock,
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

fn ghost_mode_phase_to_proto(phase: EngineGhostModePhase) -> i32 {
    match phase {
        EngineGhostModePhase::Inactive => ProtoGhostModePhase::Inactive as i32,
        EngineGhostModePhase::PendingEnter => ProtoGhostModePhase::Inactive as i32,
        EngineGhostModePhase::Active => ProtoGhostModePhase::Active as i32,
        EngineGhostModePhase::PendingExit => ProtoGhostModePhase::PendingExit as i32,
    }
}

fn ghost_mode_blockers_to_proto(blockers_mask: u32) -> Vec<i32> {
    let mut blockers = Vec::new();
    if (blockers_mask & GHOST_MODE_BLOCKER_LAPS_REQUIREMENT_NOT_MET) != 0 {
        blockers.push(ProtoGhostModeBlocker::LapsRequirementNotMet as i32);
    }
    if (blockers_mask & GHOST_MODE_BLOCKER_EXIT_SPEED_NOT_MET) != 0 {
        blockers.push(ProtoGhostModeBlocker::ExitSpeedNotMet as i32);
    }
    if (blockers_mask & GHOST_MODE_BLOCKER_EXIT_DELAY_RUNNING) != 0 {
        blockers.push(ProtoGhostModeBlocker::ExitDelayRunning as i32);
    }
    if (blockers_mask & GHOST_MODE_BLOCKER_VEHICLE_OVERLAP_ACTIVE) != 0 {
        blockers.push(ProtoGhostModeBlocker::VehicleOverlapActive as i32);
    }
    if (blockers_mask & GHOST_MODE_BLOCKER_OVERLAP_EXIT_DELAY_RUNNING) != 0 {
        blockers.push(ProtoGhostModeBlocker::OverlapExitDelayRunning as i32);
    }
    if (blockers_mask & GHOST_MODE_BLOCKER_IN_PIT) != 0 {
        blockers.push(ProtoGhostModeBlocker::InPit as i32);
    }
    blockers
}

fn ghost_mode_state_from_runtime(runtime: &GhostModeRuntimeState) -> GhostModeState {
    GhostModeState {
        can_collide_now: runtime.can_collide_now,
        phase: ghost_mode_phase_to_proto(runtime.phase),
        blockers: ghost_mode_blockers_to_proto(runtime.blockers_mask),
        exit_delay_remaining_ms: runtime.exit_delay_remaining_ms,
    }
}

pub(crate) fn participant_telemetry_from_state(
    state: &VehicleState,
    last_applied_client_seq: u64,
    pit_state: &RuntimePitStateSnapshot,
) -> CarParticipantState {
    let (tire_type, tire_wear, tire_temperature_celsius, tire_slip) =
        tire_telemetry_from_state(state);

    CarParticipantState {
        last_applied_client_seq,
        speed_mps: state.speed,
        engine_rpm: state.engine_rpm,
        gear: match state.gear {
            Gear::Reverse => -1,
            Gear::Neutral => 0,
            Gear::Forward(n) => n as i32,
        },
        ghost_mode: Some(ghost_mode_state_from_runtime(&state.ghost_mode_runtime)),
        pitstop_zone_flags: state.pitstop_state.zone_mask,
        wheels_in_pitstop: state.pitstop_state.wheels_in_pitstop as u32,
        tire_type,
        tire_wear,
        tire_temperature_celsius,
        pit_runtime: Some(pit_runtime_from_snapshot(pit_state)),
        pit_history: Some(pit_history_from_snapshot(pit_state)),
        next_pit_tire_type: runtime_tire_type_to_proto(pit_state.next_pit_tire_type),
        tire_slip,
        command_cooldowns: Some(command_cooldowns_from_snapshot(pit_state)),
    }
}

fn render_state_from_state(state: &VehicleState) -> CarRenderState {
    CarRenderState {
        wheel_speeds: Some(wheel_speeds_from_state(state)),
        front_left_wheel_orientation_rad: state.front_wheel_orientation_rad[0],
        front_right_wheel_orientation_rad: state.front_wheel_orientation_rad[1],
    }
}

/// Convert engine `VehicleState` into frontend spectator/participant full state.
pub(crate) fn frontend_full_state(
    car_id: u64,
    state: VehicleState,
    last_applied_client_seq: u64,
    pit_state: &RuntimePitStateSnapshot,
    controls_input: RuntimeControlInputSnapshot,
    current_lap_elapsed_ms: Option<u32>,
    last_lap_time_ms: Option<u32>,
    best_lap_time_ms: Option<u32>,
) -> FrontendCarFullState {
    FrontendCarFullState {
        car_id,
        kinematics: Some(participant_kinematics_from_state(&state)),
        telemetry: Some(participant_telemetry_from_state(
            &state,
            last_applied_client_seq,
            pit_state,
        )),
        render: Some(render_state_from_state(&state)),
        input_throttle: controls_input.input_throttle,
        input_brake: controls_input.input_brake,
        current_brake_balancer: controls_input.current_brake_balancer,
        current_differential_lock: controls_input.current_differential_lock,
        frontend_next_pit_tire_override: runtime_tire_type_to_proto(
            pit_state.frontend_next_pit_tire_override,
        ),
        current_lap_elapsed_ms,
        last_lap_time_ms,
        best_lap_time_ms,
    }
}

/// Convert engine `VehicleState` into participant self state.
pub(crate) fn participant_self_state(
    car_id: u64,
    state: VehicleState,
    last_applied_client_seq: u64,
    pit_state: &RuntimePitStateSnapshot,
) -> ParticipantSelfState {
    ParticipantSelfState {
        car_id,
        kinematics: Some(participant_kinematics_from_state(&state)),
        telemetry: Some(participant_telemetry_from_state(
            &state,
            last_applied_client_seq,
            pit_state,
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
        ghost_mode: Some(ghost_mode_state_from_runtime(&state.ghost_mode_runtime)),
    }
}

/// Convert engine `TrackData` into proto `TrackData`.
pub(crate) fn track_data_to_proto(track: EngineTrackData) -> ProtoTrackData {
    let centerline_samples = track
        .centerline_samples
        .into_iter()
        .map(centerline_sample_to_proto)
        .collect();
    let pitstop_data = track.pitstop_data;
    let pitstop_data = Some(ProtoPitstopData {
        enter_centerline_samples: pitstop_data
            .enter_centerline_samples
            .into_iter()
            .map(centerline_sample_to_proto)
            .collect(),
        fix_centerline_samples: pitstop_data
            .fix_centerline_samples
            .into_iter()
            .map(centerline_sample_to_proto)
            .collect(),
        exit_centerline_samples: pitstop_data
            .exit_centerline_samples
            .into_iter()
            .map(centerline_sample_to_proto)
            .collect(),
        length_m: pitstop_data.length_m,
    });

    ProtoTrackData {
        map_id: track.map_id,
        lap_length_m: track.lap_length_m,
        centerline_samples,
        pitstop_data,
    }
}

fn centerline_sample_to_proto(sample: boink::model::CenterlineSample) -> CenterlineSample {
    CenterlineSample {
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
        max_left_width_m: sample.max_left_width_m,
        max_right_width_m: sample.max_right_width_m,
        left_grounds: sample
            .left_grounds
            .into_iter()
            .map(ground_width_to_proto)
            .collect(),
        right_grounds: sample
            .right_grounds
            .into_iter()
            .map(ground_width_to_proto)
            .collect(),
    }
}

fn tire_telemetry_from_state(
    state: &VehicleState,
) -> (
    i32,
    Option<TireWearPerWheel>,
    Option<TireTemperaturePerWheel>,
    Option<TireSlipPerWheel>,
) {
    let no_wear_signal = state.tyre_health.iter().all(|v| *v == 0.0);
    let no_temp_signal = state.tyre_temperature_celsius.iter().all(|v| *v == 0.0);
    let all_finite = state
        .tyre_health
        .iter()
        .chain(state.tyre_temperature_celsius.iter())
        .chain(state.tyre_slip.iter())
        .all(|v| v.is_finite());

    if !all_finite || (no_wear_signal && no_temp_signal) {
        return (ProtoTireType::Unspecified as i32, None, None, None);
    }

    let tire_type = match state.tyre_type {
        TyreType::Hard => ProtoTireType::Hard as i32,
        TyreType::Soft => ProtoTireType::Soft as i32,
        TyreType::Wet => ProtoTireType::Wet as i32,
    };
    let tire_wear = Some(TireWearPerWheel {
        front_left: state.tyre_health[0],
        front_right: state.tyre_health[1],
        rear_left: state.tyre_health[2],
        rear_right: state.tyre_health[3],
    });
    let tire_temperature = Some(TireTemperaturePerWheel {
        front_left_celsius: state.tyre_temperature_celsius[0],
        front_right_celsius: state.tyre_temperature_celsius[1],
        rear_left_celsius: state.tyre_temperature_celsius[2],
        rear_right_celsius: state.tyre_temperature_celsius[3],
    });
    let tire_slip = Some(TireSlipPerWheel {
        front_left: state.tyre_slip[0],
        front_right: state.tyre_slip[1],
        rear_left: state.tyre_slip[2],
        rear_right: state.tyre_slip[3],
    });

    (tire_type, tire_wear, tire_temperature, tire_slip)
}

fn pit_runtime_from_snapshot(snapshot: &RuntimePitStateSnapshot) -> PitRuntimeState {
    PitRuntimeState {
        pit_request_active: snapshot.pit_request_active,
        emergency_lock_remaining_ms: snapshot.emergency_lock_remaining_ms,
        last_pit_time_ms: snapshot.last_pit_time_ms,
        last_pit_source: runtime_pit_source_to_proto(snapshot.last_pit_source),
        last_pit_lap: snapshot.last_pit_lap,
    }
}

fn pit_history_from_snapshot(snapshot: &RuntimePitStateSnapshot) -> ProtoPitHistoryState {
    ProtoPitHistoryState {
        entries: snapshot
            .history
            .iter()
            .map(|entry| ProtoPitHistoryEntry {
                pit_time_ms: entry.pit_time_ms,
                lap: entry.lap,
                source: runtime_pit_source_to_proto(entry.source),
                tire_type_after: runtime_tire_type_to_proto(entry.tire_type_after),
                tire_type_before: runtime_tire_type_to_proto(entry.tire_type_before),
                tire_wear_before_repair: Some(tire_wear_array_to_proto(
                    entry.tire_wear_before_repair,
                )),
                tire_temperature_before_celsius: Some(tire_temperature_array_to_proto(
                    entry.tire_temperature_before_celsius,
                )),
                tire_temperature_after_celsius: Some(tire_temperature_array_to_proto(
                    entry.tire_temperature_after_celsius,
                )),
                bot_slot_before: entry.bot_slot_before,
                bot_slot_after: entry.bot_slot_after,
            })
            .collect(),
    }
}

fn command_cooldowns_from_snapshot(snapshot: &RuntimePitStateSnapshot) -> CommandCooldownState {
    CommandCooldownState {
        back_to_track_remaining_ms: snapshot.back_to_track_remaining_ms,
        emergency_pitstop_remaining_ms: snapshot.emergency_pitstop_remaining_ms,
    }
}

fn runtime_tire_type_to_proto(tire_type: RuntimePitTireType) -> i32 {
    match tire_type {
        RuntimePitTireType::Unspecified => ProtoTireType::Unspecified as i32,
        RuntimePitTireType::Hard => ProtoTireType::Hard as i32,
        RuntimePitTireType::Soft => ProtoTireType::Soft as i32,
        RuntimePitTireType::Wet => ProtoTireType::Wet as i32,
    }
}

fn runtime_pit_source_to_proto(source: RuntimePitEntrySource) -> i32 {
    match source {
        RuntimePitEntrySource::BotDecision => ProtoPitEntrySource::BotDecision as i32,
        RuntimePitEntrySource::Requested => ProtoPitEntrySource::Requested as i32,
        RuntimePitEntrySource::Emergency => ProtoPitEntrySource::Emergency as i32,
    }
}

fn tire_wear_array_to_proto(values: [f32; 4]) -> TireWearPerWheel {
    TireWearPerWheel {
        front_left: values[0],
        front_right: values[1],
        rear_left: values[2],
        rear_right: values[3],
    }
}

fn tire_temperature_array_to_proto(values: [f32; 4]) -> TireTemperaturePerWheel {
    TireTemperaturePerWheel {
        front_left_celsius: values[0],
        front_right_celsius: values[1],
        rear_left_celsius: values[2],
        rear_right_celsius: values[3],
    }
}

fn ground_width_to_proto(ground: boink::model::GroundWidth) -> ProtoGroundWidth {
    ProtoGroundWidth {
        width_m: ground.width,
        ground_type: ground_type_to_proto(ground.ground_type) as i32,
    }
}

fn ground_type_to_proto(ground: GroundType) -> ProtoGroundType {
    match ground {
        GroundType::Asphalt => ProtoGroundType::Asphalt,
        GroundType::Grass => ProtoGroundType::Grass,
        GroundType::Sand | GroundType::Gravel => ProtoGroundType::Gravel,
        GroundType::Wall => ProtoGroundType::Wall,
        GroundType::Kerb => ProtoGroundType::Kerb,
    }
}
