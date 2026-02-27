//! Sandbox admin service mapping helpers.

use prost_types::Timestamp;
use proto::race::v1::{
    GhostModeConditionLogic, GhostModeSettings as ProtoGhostModeSettings, RuntimeTimeOfDayPreset,
    SandboxConfig as ProtoSandboxConfig, SandboxConfigInput as ProtoSandboxConfigInput,
};
use tonic::Status;

use crate::db::repos::sandbox_config::{
    GhostModeSettingsRecord, SandboxConfigInputRecord, SandboxConfigRecord,
};

/// Maps protobuf input payload into repository input shape.
pub fn sandbox_input_from_proto(
    input: &ProtoSandboxConfigInput,
) -> Result<SandboxConfigInputRecord, Status> {
    let time_of_day_preset = RuntimeTimeOfDayPreset::try_from(input.time_of_day_preset)
        .map_err(|_| Status::invalid_argument("invalid time_of_day_preset"))?;
    if matches!(time_of_day_preset, RuntimeTimeOfDayPreset::Unspecified) {
        return Err(Status::invalid_argument(
            "time_of_day_preset must be specified",
        ));
    }

    let ghost_mode = match input.ghost_mode.as_ref() {
        Some(ghost) => Some(ghost_mode_from_proto(ghost)?),
        None => None,
    };

    if input.sandbox_name.trim().is_empty() {
        return Err(Status::invalid_argument("sandbox_name must be non-empty"));
    }
    if input.map_id.trim().is_empty() {
        return Err(Status::invalid_argument("map_id must be non-empty"));
    }

    Ok(SandboxConfigInputRecord {
        sandbox_name: input.sandbox_name.clone(),
        map_id: input.map_id.clone(),
        time_of_day_preset,
        ghost_mode,
    })
}

/// Maps persisted record into protobuf response payload.
pub fn sandbox_to_proto(record: SandboxConfigRecord) -> ProtoSandboxConfig {
    ProtoSandboxConfig {
        sandbox_id: record.sandbox_id,
        config: Some(sandbox_input_to_proto(record.config)),
    }
}

/// Maps persisted input record to protobuf shape.
pub fn sandbox_input_to_proto(input: SandboxConfigInputRecord) -> ProtoSandboxConfigInput {
    ProtoSandboxConfigInput {
        sandbox_name: input.sandbox_name,
        map_id: input.map_id,
        time_of_day_preset: input.time_of_day_preset as i32,
        ghost_mode: input.ghost_mode.map(ghost_mode_to_proto),
    }
}

fn ghost_mode_from_proto(
    proto: &ProtoGhostModeSettings,
) -> Result<GhostModeSettingsRecord, Status> {
    let condition_logic = GhostModeConditionLogic::try_from(proto.condition_logic)
        .map_err(|_| Status::invalid_argument("invalid ghost_mode.condition_logic"))?;
    if matches!(condition_logic, GhostModeConditionLogic::Unspecified) {
        return Err(Status::invalid_argument(
            "ghost_mode.condition_logic must be specified",
        ));
    }

    if !proto.min_speed_enter_mps.is_finite() || proto.min_speed_enter_mps < 0.0 {
        return Err(Status::invalid_argument(
            "ghost_mode.min_speed_enter_mps must be finite and >= 0",
        ));
    }
    if !proto.min_speed_exit_mps.is_finite() || proto.min_speed_exit_mps < 0.0 {
        return Err(Status::invalid_argument(
            "ghost_mode.min_speed_exit_mps must be finite and >= 0",
        ));
    }

    Ok(GhostModeSettingsRecord {
        enabled: proto.enabled,
        min_speed_enter_mps: proto.min_speed_enter_mps,
        min_speed_exit_mps: proto.min_speed_exit_mps,
        enter_delay_ms: proto.enter_delay_ms,
        exit_delay_ms: proto.exit_delay_ms,
        min_completed_laps: proto.min_completed_laps,
        condition_logic,
        overlap_exit_delay_ms: proto.overlap_exit_delay_ms,
    })
}

fn ghost_mode_to_proto(record: GhostModeSettingsRecord) -> ProtoGhostModeSettings {
    ProtoGhostModeSettings {
        enabled: record.enabled,
        min_speed_enter_mps: record.min_speed_enter_mps,
        min_speed_exit_mps: record.min_speed_exit_mps,
        enter_delay_ms: record.enter_delay_ms,
        exit_delay_ms: record.exit_delay_ms,
        min_completed_laps: record.min_completed_laps,
        condition_logic: record.condition_logic as i32,
        overlap_exit_delay_ms: record.overlap_exit_delay_ms,
    }
}

/// Returns current UTC timestamp.
pub fn utc_now_timestamp() -> Timestamp {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    Timestamp {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}
