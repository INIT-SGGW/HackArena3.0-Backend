use proto::race::v1::GhostModeSettings as ProtoGhostModeSettings;
use tonic::Status;

const MAX_SANDBOX_NAME_LEN_CHARS: usize = 64;
const MAX_GHOST_DELAY_MS: u32 = 600_000;
const MAX_GHOST_UNTIL_COMPLETED_LAPS: u32 = 100_000;

pub(super) fn validate_sandbox_name_and_map_id(
    sandbox_name: &str,
    map_id: &str,
) -> Result<(), Status> {
    let sandbox_name = sandbox_name.trim();
    if sandbox_name.is_empty() {
        return Err(Status::invalid_argument("sandbox_name must be non-empty"));
    }
    if sandbox_name.chars().count() > MAX_SANDBOX_NAME_LEN_CHARS {
        return Err(Status::invalid_argument(format!(
            "sandbox_name must be at most {MAX_SANDBOX_NAME_LEN_CHARS} characters"
        )));
    }

    let map_id = map_id.trim();
    if map_id.is_empty() {
        return Err(Status::invalid_argument("map_id must be non-empty"));
    }
    if !map_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(Status::invalid_argument(
            "map_id contains invalid characters",
        ));
    }

    Ok(())
}

pub(super) fn validate_ghost_mode(proto: &ProtoGhostModeSettings) -> Result<(), Status> {
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
    if proto.enter_delay_ms > MAX_GHOST_DELAY_MS {
        return Err(Status::invalid_argument(format!(
            "ghost_mode.enter_delay_ms must be <= {MAX_GHOST_DELAY_MS}"
        )));
    }
    if proto.exit_delay_ms > MAX_GHOST_DELAY_MS {
        return Err(Status::invalid_argument(format!(
            "ghost_mode.exit_delay_ms must be <= {MAX_GHOST_DELAY_MS}"
        )));
    }
    if proto.vehicle_overlap_exit_delay_ms > MAX_GHOST_DELAY_MS {
        return Err(Status::invalid_argument(format!(
            "ghost_mode.vehicle_overlap_exit_delay_ms must be <= {MAX_GHOST_DELAY_MS}"
        )));
    }
    if proto.until_completed_laps > MAX_GHOST_UNTIL_COMPLETED_LAPS {
        return Err(Status::invalid_argument(format!(
            "ghost_mode.until_completed_laps must be <= {MAX_GHOST_UNTIL_COMPLETED_LAPS}"
        )));
    }

    Ok(())
}
