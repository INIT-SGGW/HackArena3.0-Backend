use std::path::Path;

use tonic::Status;

use crate::local::sandbox_config_store::LocalSandboxSpawnModeRecord;

pub(crate) fn ensure_supported_spawn_mode(
    spawn_mode: LocalSandboxSpawnModeRecord,
) -> Result<(), Status> {
    match spawn_mode {
        LocalSandboxSpawnModeRecord::StartLine => Ok(()),
        LocalSandboxSpawnModeRecord::RandomOnTrack
        | LocalSandboxSpawnModeRecord::InPit
        | LocalSandboxSpawnModeRecord::RandomStartSlot => Err(Status::unimplemented(
            "spawn mode is not implemented yet (supported: START_LINE)",
        )),
    }
}

pub(crate) async fn validate_map_id_track_exists(
    tracks_dir: &Path,
    map_id: &str,
) -> Result<(), Status> {
    if map_id.trim().is_empty() {
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

    let track_path = tracks_dir.join(format!("{map_id}.glb"));
    match tokio::fs::try_exists(&track_path).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(Status::not_found(format!(
            "track not found for map_id: {map_id}"
        ))),
        Err(err) => Err(Status::internal(format!(
            "failed to validate track existence for map_id {map_id}: {err}"
        ))),
    }
}
