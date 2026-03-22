use std::path::Path;

use tonic::Status;

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
