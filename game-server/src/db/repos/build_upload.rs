//! Build upload repository for staged source archives.

use sqlx::PgPool;
use thiserror::Error;

/// Persisted staged upload metadata row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildUploadRecord {
    pub upload_id: String,
    pub team_id: String,
    pub requested_by_subject: String,
    pub original_file_name: String,
    pub file_size_bytes: u64,
    pub sha256_hex: String,
    pub staged_path: String,
    pub created_at_ms: i64,
}

/// Repository error surface for build upload persistence.
#[derive(Debug, Error)]
pub enum BuildUploadRepoError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error("persisted numeric value is out of range for upload: {upload_id} ({field})")]
    NumericOutOfRange {
        upload_id: String,
        field: &'static str,
    },
}

/// Repository for staged upload metadata.
#[derive(Clone)]
pub struct BuildUploadRepo {
    pool: PgPool,
}

impl BuildUploadRepo {
    /// Creates a repository backed by the provided Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a staged upload metadata row.
    pub async fn insert_upload(
        &self,
        upload: &BuildUploadRecord,
    ) -> Result<(), BuildUploadRepoError> {
        let file_size_bytes_i64 = i64::try_from(upload.file_size_bytes).map_err(|_| {
            BuildUploadRepoError::NumericOutOfRange {
                upload_id: upload.upload_id.clone(),
                field: "file_size_bytes",
            }
        })?;

        sqlx::query!(
            r#"
            INSERT INTO build_uploads (
                upload_id,
                team_id,
                requested_by_subject,
                original_file_name,
                file_size_bytes,
                sha256_hex,
                staged_path,
                created_at_ms
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            "#,
            &upload.upload_id,
            &upload.team_id,
            &upload.requested_by_subject,
            &upload.original_file_name,
            file_size_bytes_i64,
            &upload.sha256_hex,
            &upload.staged_path,
            upload.created_at_ms,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Reads one upload by stable `upload_id`.
    pub async fn get_upload(
        &self,
        upload_id: &str,
    ) -> Result<Option<BuildUploadRecord>, BuildUploadRepoError> {
        let row = sqlx::query!(
            r#"
            SELECT
                upload_id,
                team_id,
                requested_by_subject,
                original_file_name,
                file_size_bytes,
                sha256_hex,
                staged_path,
                created_at_ms
            FROM build_uploads
            WHERE upload_id = $1
            "#,
            upload_id,
        )
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };

        let file_size_bytes = u64::try_from(row.file_size_bytes).map_err(|_| {
            BuildUploadRepoError::NumericOutOfRange {
                upload_id: row.upload_id.clone(),
                field: "file_size_bytes",
            }
        })?;

        Ok(Some(BuildUploadRecord {
            upload_id: row.upload_id,
            team_id: row.team_id,
            requested_by_subject: row.requested_by_subject,
            original_file_name: row.original_file_name,
            file_size_bytes,
            sha256_hex: row.sha256_hex,
            staged_path: row.staged_path,
            created_at_ms: row.created_at_ms,
        }))
    }
}
