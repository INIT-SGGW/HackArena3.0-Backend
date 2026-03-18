use std::path::{Path, PathBuf};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::config::Config;

/// Input payload for staging a participant source archive.
#[derive(Debug, Clone)]
pub struct UploadStageInput {
    pub team_id: String,
    pub upload_id: String,
    pub original_file_name: String,
    pub archive_bytes: Bytes,
}

/// Metadata produced after successful ZIP staging.
#[derive(Debug, Clone)]
pub struct StagedUpload {
    pub team_id: String,
    pub upload_id: String,
    pub original_file_name: String,
    pub file_size: u64,
    pub sha256_hex: String,
    pub staged_path: PathBuf,
}

/// File-system based upload staging implementation.
#[derive(Debug, Clone)]
pub struct FsUploadStager {
    root_dir: PathBuf,
    max_upload_size_bytes: u64,
}

impl FsUploadStager {
    /// Creates stager from server configuration.
    pub fn from_config(cfg: &Config) -> Self {
        Self {
            root_dir: cfg.build_uploads_root.clone(),
            max_upload_size_bytes: cfg.build_upload_max_size_bytes,
        }
    }

    /// Ensures that the staging root directory exists.
    pub async fn ensure_root_dir(&self) -> Result<(), UploadStagingError> {
        fs::create_dir_all(&self.root_dir)
            .await
            .map_err(|source| UploadStagingError::Io {
                op: "create_dir_all",
                path: self.root_dir.clone(),
                source,
            })
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn max_upload_size_bytes(&self) -> u64 {
        self.max_upload_size_bytes
    }

    /// Validates and stores uploaded archive as `<root>/<team_id>/<upload_id>.zip`.
    pub async fn stage_zip(
        &self,
        input: UploadStageInput,
    ) -> Result<StagedUpload, UploadStagingError> {
        validate_safe_id(&input.team_id, "team_id")?;
        validate_safe_id(&input.upload_id, "upload_id")?;
        validate_zip_file_name(&input.original_file_name)?;

        let file_size = input.archive_bytes.len() as u64;
        if file_size == 0 {
            return Err(UploadStagingError::EmptyArchive);
        }
        if file_size > self.max_upload_size_bytes {
            return Err(UploadStagingError::UploadTooLarge {
                size_bytes: file_size,
                max_size_bytes: self.max_upload_size_bytes,
            });
        }

        let team_dir = self.root_dir.join(&input.team_id);
        fs::create_dir_all(&team_dir)
            .await
            .map_err(|source| UploadStagingError::Io {
                op: "create_dir_all",
                path: team_dir.clone(),
                source,
            })?;

        let staged_path = team_dir.join(format!("{}.zip", input.upload_id));
        let mut staged_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged_path)
            .await
            .map_err(|source| match source.kind() {
                std::io::ErrorKind::AlreadyExists => UploadStagingError::AlreadyExists {
                    team_id: input.team_id.clone(),
                    upload_id: input.upload_id.clone(),
                    staged_path: staged_path.clone(),
                },
                _ => UploadStagingError::Io {
                    op: "open",
                    path: staged_path.clone(),
                    source,
                },
            })?;

        if let Err(source) = staged_file.write_all(input.archive_bytes.as_ref()).await {
            let _ = fs::remove_file(&staged_path).await;
            return Err(UploadStagingError::Io {
                op: "write_all",
                path: staged_path.clone(),
                source,
            });
        }
        if let Err(source) = staged_file.flush().await {
            let _ = fs::remove_file(&staged_path).await;
            return Err(UploadStagingError::Io {
                op: "flush",
                path: staged_path.clone(),
                source,
            });
        }
        drop(staged_file);

        let sha256_hex = sha256_hex(input.archive_bytes.as_ref());
        Ok(StagedUpload {
            team_id: input.team_id,
            upload_id: input.upload_id,
            original_file_name: input.original_file_name,
            file_size,
            sha256_hex,
            staged_path,
        })
    }
}

#[derive(Debug, Error)]
pub enum UploadStagingError {
    #[error("{field} must be non-empty and contain only [A-Za-z0-9_-]")]
    InvalidIdentifier { field: &'static str },
    #[error("original_file_name must be a plain file name without path segments")]
    InvalidOriginalFileName,
    #[error("archive must have .zip extension")]
    InvalidArchiveExtension,
    #[error("archive payload is empty")]
    EmptyArchive,
    #[error("archive is too large: {size_bytes} bytes (max {max_size_bytes} bytes)")]
    UploadTooLarge {
        size_bytes: u64,
        max_size_bytes: u64,
    },
    #[error(
        "upload already exists for team `{team_id}` and upload `{upload_id}` at `{staged_path}`"
    )]
    AlreadyExists {
        team_id: String,
        upload_id: String,
        staged_path: PathBuf,
    },
    #[error("filesystem {op} failed for `{path}`: {source}")]
    Io {
        op: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
}

fn validate_safe_id(value: &str, field: &'static str) -> Result<(), UploadStagingError> {
    let value = value.trim();
    if value.is_empty()
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(UploadStagingError::InvalidIdentifier { field });
    }
    Ok(())
}

fn validate_zip_file_name(file_name: &str) -> Result<(), UploadStagingError> {
    let file_name = file_name.trim();
    if file_name.is_empty() {
        return Err(UploadStagingError::InvalidOriginalFileName);
    }

    let is_plain_file_name = Path::new(file_name)
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name == file_name)
        .unwrap_or(false);
    if !is_plain_file_name {
        return Err(UploadStagingError::InvalidOriginalFileName);
    }

    if !file_name.to_ascii_lowercase().ends_with(".zip") {
        return Err(UploadStagingError::InvalidArchiveExtension);
    }

    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        out.push_str(&format!("{:02x}", b));
    }
    out
}
