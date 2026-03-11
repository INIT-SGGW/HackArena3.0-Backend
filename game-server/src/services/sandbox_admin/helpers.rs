use tonic::Status;
use uuid::Uuid;

use crate::db::repos::sandbox_config::{SandboxConfigInputRecord, SandboxConfigRepoError};

pub(super) fn sandbox_id_v5(config: &SandboxConfigInputRecord, expected_revision: u64) -> String {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    let payload = format!(
        "expected_revision={};sandbox_name={};map_id={};time_of_day_preset={};ts_ns={}",
        expected_revision,
        config.sandbox_name,
        config.map_id,
        config.time_of_day_preset as i32,
        duration.as_nanos(),
    );
    Uuid::new_v5(&Uuid::NAMESPACE_OID, payload.as_bytes()).to_string()
}

pub(super) fn map_repo_error_to_status(err: SandboxConfigRepoError) -> Status {
    match err {
        SandboxConfigRepoError::RevisionMismatch { .. } => {
            Status::failed_precondition(err.to_string())
        }
        SandboxConfigRepoError::AlreadyExists { .. } => Status::already_exists(err.to_string()),
        SandboxConfigRepoError::NotFound { .. } => Status::not_found(err.to_string()),
        SandboxConfigRepoError::InvalidTimeOfDayPreset => Status::invalid_argument(err.to_string()),
        SandboxConfigRepoError::Sqlx(_)
        | SandboxConfigRepoError::StateMissing
        | SandboxConfigRepoError::PartialGhostData { .. }
        | SandboxConfigRepoError::NumericOutOfRange { .. }
        | SandboxConfigRepoError::RevisionOverflow => Status::internal(err.to_string()),
    }
}
