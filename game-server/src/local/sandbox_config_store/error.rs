use thiserror::Error;

/// Errors returned by local sandbox config store.
#[derive(Debug, Error)]
pub enum LocalSandboxConfigStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("revision mismatch: expected {expected}, actual {actual}")]
    RevisionMismatch { expected: u64, actual: u64 },
    #[error("local sandbox config already exists: {sandbox_id}")]
    AlreadyExists { sandbox_id: String },
    #[error("local sandbox config not found: {sandbox_id}")]
    NotFound { sandbox_id: String },
    #[error("invalid local sandbox config: {message}")]
    InvalidConfig { message: String },
}
