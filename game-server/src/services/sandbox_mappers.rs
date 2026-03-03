//! Sandbox admin service mapping helpers.

use boink::model::{
    GhostModeConditionLogic as EngineGhostModeConditionLogic,
    GhostModeSettings as EngineGhostModeSettings,
};
use prost_types::Timestamp;
use proto::race::v1::{
    AdminPendingSandboxOperation, AdminSandboxRuntimeInfo,
    GhostModeSettings as ProtoGhostModeSettings, PublicSandboxRuntimeInfo, RuntimeTimeOfDayPreset,
    SandboxConfig as ProtoSandboxConfig, SandboxConfigInput as ProtoSandboxConfigInput,
};
use tonic::Status;

use crate::db::repos::sandbox_config::{
    GhostModeSettingsRecord, SandboxConfigInputRecord, SandboxConfigRecord,
};
use crate::runtime::engine_worker::{EnginePendingSandboxActivation, EngineRuntimeTimeOfDayPreset};

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
    if !proto.enter_speed_max_mps.is_finite() || proto.enter_speed_max_mps < 0.0 {
        return Err(Status::invalid_argument(
            "ghost_mode.enter_speed_max_mps must be finite and >= 0",
        ));
    }
    if !proto.exit_speed_min_mps.is_finite() || proto.exit_speed_min_mps < 0.0 {
        return Err(Status::invalid_argument(
            "ghost_mode.exit_speed_min_mps must be finite and >= 0",
        ));
    }
    if proto.enter_speed_max_mps > proto.exit_speed_min_mps {
        return Err(Status::invalid_argument(
            "ghost_mode.enter_speed_max_mps must be <= ghost_mode.exit_speed_min_mps",
        ));
    }
    Ok(GhostModeSettingsRecord {
        enabled: proto.enabled,
        enter_speed_max_mps: proto.enter_speed_max_mps,
        exit_speed_min_mps: proto.exit_speed_min_mps,
        enter_delay_ms: proto.enter_delay_ms,
        exit_delay_ms: proto.exit_delay_ms,
        until_completed_laps: proto.until_completed_laps,
        vehicle_overlap_exit_delay_ms: proto.vehicle_overlap_exit_delay_ms,
    })
}

fn ghost_mode_to_proto(record: GhostModeSettingsRecord) -> ProtoGhostModeSettings {
    ProtoGhostModeSettings {
        enabled: record.enabled,
        enter_speed_max_mps: record.enter_speed_max_mps,
        exit_speed_min_mps: record.exit_speed_min_mps,
        enter_delay_ms: record.enter_delay_ms,
        exit_delay_ms: record.exit_delay_ms,
        until_completed_laps: record.until_completed_laps,
        vehicle_overlap_exit_delay_ms: record.vehicle_overlap_exit_delay_ms,
    }
}

/// Maps runtime time-of-day preset from engine worker to protobuf.
pub fn runtime_time_of_day_preset_to_proto(
    preset: EngineRuntimeTimeOfDayPreset,
) -> RuntimeTimeOfDayPreset {
    match preset {
        EngineRuntimeTimeOfDayPreset::Unspecified => RuntimeTimeOfDayPreset::Unspecified,
        EngineRuntimeTimeOfDayPreset::Morning => RuntimeTimeOfDayPreset::Morning,
        EngineRuntimeTimeOfDayPreset::Noon => RuntimeTimeOfDayPreset::Noon,
        EngineRuntimeTimeOfDayPreset::Evening => RuntimeTimeOfDayPreset::Evening,
        EngineRuntimeTimeOfDayPreset::Night => RuntimeTimeOfDayPreset::Night,
    }
}

/// Maps runtime time-of-day preset from protobuf to engine worker.
pub fn runtime_time_of_day_preset_from_proto(
    preset: RuntimeTimeOfDayPreset,
) -> EngineRuntimeTimeOfDayPreset {
    match preset {
        RuntimeTimeOfDayPreset::Unspecified => EngineRuntimeTimeOfDayPreset::Unspecified,
        RuntimeTimeOfDayPreset::Morning => EngineRuntimeTimeOfDayPreset::Morning,
        RuntimeTimeOfDayPreset::Noon => EngineRuntimeTimeOfDayPreset::Noon,
        RuntimeTimeOfDayPreset::Evening => EngineRuntimeTimeOfDayPreset::Evening,
        RuntimeTimeOfDayPreset::Night => EngineRuntimeTimeOfDayPreset::Night,
    }
}

/// Maps persisted sandbox record into runtime payload.
pub fn admin_sandbox_runtime_info_from_record(
    record: SandboxConfigRecord,
    active_time_of_day_preset: RuntimeTimeOfDayPreset,
) -> AdminSandboxRuntimeInfo {
    AdminSandboxRuntimeInfo {
        sandbox_id: record.sandbox_id,
        sandbox_name: record.config.sandbox_name,
        map_id: record.config.map_id,
        active_time_of_day_preset: active_time_of_day_preset as i32,
        ghost_mode: record.config.ghost_mode.map(ghost_mode_to_proto),
        started_at_utc: None,
        closes_at_utc: None,
    }
}

/// Maps persisted sandbox record into public runtime payload.
pub fn public_sandbox_runtime_info_from_record(
    record: SandboxConfigRecord,
    active_time_of_day_preset: RuntimeTimeOfDayPreset,
    active_player_count: u32,
) -> PublicSandboxRuntimeInfo {
    PublicSandboxRuntimeInfo {
        sandbox_id: record.sandbox_id,
        sandbox_name: record.config.sandbox_name,
        map_id: record.config.map_id,
        active_time_of_day_preset: active_time_of_day_preset as i32,
        ghost_mode: record.config.ghost_mode.map(ghost_mode_to_proto),
        active_player_count,
    }
}

/// Finds sandbox record by stable sandbox identifier.
pub fn find_sandbox_by_id(
    sandboxes: &[SandboxConfigRecord],
    sandbox_id: &str,
) -> Option<SandboxConfigRecord> {
    sandboxes
        .iter()
        .find(|entry| entry.sandbox_id == sandbox_id)
        .cloned()
}

/// Returns disabled/default ghost mode settings for engine runtime.
pub fn default_engine_ghost_mode_settings() -> EngineGhostModeSettings {
    EngineGhostModeSettings {
        enabled: false,
        min_speed_enter_mps: 0.0,
        min_speed_exit_mps: 0.0,
        enter_delay_ms: 0,
        exit_delay_ms: 0,
        min_completed_laps: 0,
        condition_logic: EngineGhostModeConditionLogic::Or,
        overlap_exit_delay_ms: 0,
    }
}

/// Maps persisted ghost-mode settings into engine runtime shape.
pub fn engine_ghost_mode_settings_from_record(
    record: Option<&GhostModeSettingsRecord>,
) -> EngineGhostModeSettings {
    let Some(record) = record else {
        return default_engine_ghost_mode_settings();
    };

    EngineGhostModeSettings {
        enabled: record.enabled,
        min_speed_enter_mps: record.enter_speed_max_mps,
        min_speed_exit_mps: record.exit_speed_min_mps,
        enter_delay_ms: record.enter_delay_ms,
        exit_delay_ms: record.exit_delay_ms,
        min_completed_laps: record.until_completed_laps,
        condition_logic: EngineGhostModeConditionLogic::Or,
        overlap_exit_delay_ms: record.vehicle_overlap_exit_delay_ms,
    }
}

/// Maps runtime pending sandbox activation metadata to protobuf.
pub fn pending_sandbox_operation_to_proto(
    pending: EnginePendingSandboxActivation,
) -> AdminPendingSandboxOperation {
    AdminPendingSandboxOperation {
        activate: pending.activate,
        sandbox_id: pending.sandbox_id,
        execute_at_utc: Some(unix_ms_to_timestamp(pending.execute_at_unix_ms)),
    }
}

/// Converts protobuf timestamp to unix milliseconds.
pub fn timestamp_to_unix_ms(timestamp: &Timestamp) -> Result<i64, Status> {
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(Status::invalid_argument(
            "timestamp nanos must be in range 0..1_000_000_000",
        ));
    }

    let seconds_ms = timestamp
        .seconds
        .checked_mul(1000)
        .ok_or_else(|| Status::out_of_range("timestamp seconds overflow"))?;
    let nanos_ms = i64::from(timestamp.nanos / 1_000_000);
    seconds_ms
        .checked_add(nanos_ms)
        .ok_or_else(|| Status::out_of_range("timestamp overflow"))
}

/// Converts unix milliseconds to protobuf timestamp.
pub fn unix_ms_to_timestamp(ms: i64) -> Timestamp {
    let seconds = ms.div_euclid(1000);
    let nanos = (ms.rem_euclid(1000) as i32) * 1_000_000;
    Timestamp { seconds, nanos }
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
