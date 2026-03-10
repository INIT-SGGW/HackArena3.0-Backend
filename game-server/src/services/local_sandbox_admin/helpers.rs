use tonic::Status;
use uuid::Uuid;

use crate::local::sandbox_config_store::{
    LocalSandboxConfigInputRecord, LocalSandboxConfigRecord, LocalSandboxConfigStoreError,
};

use super::mappers::{local_spawn_mode_to_proto, local_time_of_day_mode_to_proto};

pub(crate) fn local_sandbox_id_v5(
    config: &LocalSandboxConfigInputRecord,
    expected_revision: u64,
) -> String {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    let payload = format!(
        "expected_revision={};sandbox_name={};map_id={};mode={};spawn_mode={};ts_ns={}",
        expected_revision,
        config.sandbox_name,
        config.map_id,
        local_time_of_day_mode_to_proto(config.time_of_day.mode) as i32,
        local_spawn_mode_to_proto(config.spawn_mode) as i32,
        duration.as_nanos(),
    );
    Uuid::new_v5(&Uuid::NAMESPACE_OID, payload.as_bytes()).to_string()
}

pub(crate) fn map_store_err(err: LocalSandboxConfigStoreError) -> Status {
    match err {
        LocalSandboxConfigStoreError::RevisionMismatch { .. } => {
            Status::failed_precondition(err.to_string())
        }
        LocalSandboxConfigStoreError::AlreadyExists { .. } => {
            Status::already_exists(err.to_string())
        }
        LocalSandboxConfigStoreError::NotFound { .. } => Status::not_found(err.to_string()),
        LocalSandboxConfigStoreError::InvalidConfig { .. } => {
            Status::invalid_argument(err.to_string())
        }
        LocalSandboxConfigStoreError::Io(_) | LocalSandboxConfigStoreError::Serde(_) => {
            Status::internal(err.to_string())
        }
    }
}

pub(crate) fn find_local_sandbox_by_id(
    sandboxes: &[LocalSandboxConfigRecord],
    sandbox_id: &str,
) -> Option<LocalSandboxConfigRecord> {
    sandboxes
        .iter()
        .find(|entry| entry.sandbox_id == sandbox_id)
        .cloned()
}
