//! Build submission repository for persisted backend orchestration metadata.

use sqlx::{PgPool, Row};
use thiserror::Error;

/// Persisted submission/build mapping row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildSubmissionRecord {
    pub submission_id: String,
    pub upload_id: String,
    pub team_id: String,
    pub requested_by_subject: String,
    pub original_file_name: String,
    pub file_size_bytes: u64,
    pub sha256_hex: String,
    pub staged_path: String,
    pub builder_build_id: Option<String>,
    pub cancellation_requested: bool,
    pub retry_of_submission_id: Option<String>,
    /// Cached builder status value (proto enum i32), optional.
    pub last_known_builder_status: Option<i32>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub last_synced_at_ms: Option<i64>,
}

/// Repository error surface for build submission persistence.
#[derive(Debug, Error)]
pub enum BuildSubmissionRepoError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error("submission not found: {submission_id}")]
    NotFound { submission_id: String },
    #[error("persisted numeric value is out of range for submission: {submission_id} ({field})")]
    NumericOutOfRange {
        submission_id: String,
        field: &'static str,
    },
}

/// Repository for build submission metadata and builder mapping.
#[derive(Clone)]
pub struct BuildSubmissionRepo {
    pool: PgPool,
}

impl BuildSubmissionRepo {
    /// Creates a repository backed by the provided Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a new submission metadata row.
    pub async fn insert_submission(
        &self,
        submission: &BuildSubmissionRecord,
    ) -> Result<(), BuildSubmissionRepoError> {
        let file_size_bytes_i64 = i64::try_from(submission.file_size_bytes).map_err(|_| {
            BuildSubmissionRepoError::NumericOutOfRange {
                submission_id: submission.submission_id.clone(),
                field: "file_size_bytes",
            }
        })?;

        sqlx::query(
            r#"
            INSERT INTO build_submissions (
                submission_id,
                upload_id,
                team_id,
                requested_by_subject,
                original_file_name,
                file_size_bytes,
                sha256_hex,
                staged_path,
                builder_build_id,
                cancellation_requested,
                retry_of_submission_id,
                last_known_builder_status,
                created_at_ms,
                updated_at_ms,
                last_synced_at_ms
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
            )
            "#,
        )
        .bind(&submission.submission_id)
        .bind(&submission.upload_id)
        .bind(&submission.team_id)
        .bind(&submission.requested_by_subject)
        .bind(&submission.original_file_name)
        .bind(file_size_bytes_i64)
        .bind(&submission.sha256_hex)
        .bind(&submission.staged_path)
        .bind(&submission.builder_build_id)
        .bind(submission.cancellation_requested)
        .bind(&submission.retry_of_submission_id)
        .bind(submission.last_known_builder_status)
        .bind(submission.created_at_ms)
        .bind(submission.updated_at_ms)
        .bind(submission.last_synced_at_ms)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Reads one submission by its stable identifier.
    pub async fn get_submission(
        &self,
        submission_id: &str,
    ) -> Result<Option<BuildSubmissionRecord>, BuildSubmissionRepoError> {
        let row = sqlx::query(
            r#"
            SELECT
                submission_id,
                upload_id,
                team_id,
                requested_by_subject,
                original_file_name,
                file_size_bytes,
                sha256_hex,
                staged_path,
                builder_build_id,
                cancellation_requested,
                retry_of_submission_id,
                last_known_builder_status,
                created_at_ms,
                updated_at_ms,
                last_synced_at_ms
            FROM build_submissions
            WHERE submission_id = $1
            "#,
        )
        .bind(submission_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => decode_submission_row(row).map(Some),
            None => Ok(None),
        }
    }

    /// Lists submissions for a team, newest first.
    pub async fn list_submissions_for_team(
        &self,
        team_id: &str,
        limit: u32,
    ) -> Result<Vec<BuildSubmissionRecord>, BuildSubmissionRepoError> {
        let rows = sqlx::query(
            r#"
            SELECT
                submission_id,
                upload_id,
                team_id,
                requested_by_subject,
                original_file_name,
                file_size_bytes,
                sha256_hex,
                staged_path,
                builder_build_id,
                cancellation_requested,
                retry_of_submission_id,
                last_known_builder_status,
                created_at_ms,
                updated_at_ms,
                last_synced_at_ms
            FROM build_submissions
            WHERE team_id = $1
            ORDER BY created_at_ms DESC
            LIMIT $2
            "#,
        )
        .bind(team_id)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(decode_submission_row(row)?);
        }
        Ok(out)
    }

    /// Attaches builder build-id to an existing submission.
    pub async fn set_builder_build_id(
        &self,
        submission_id: &str,
        builder_build_id: &str,
        updated_at_ms: i64,
    ) -> Result<(), BuildSubmissionRepoError> {
        let affected = sqlx::query(
            r#"
            UPDATE build_submissions
            SET builder_build_id = $1, updated_at_ms = $2
            WHERE submission_id = $3
            "#,
        )
        .bind(builder_build_id)
        .bind(updated_at_ms)
        .bind(submission_id)
        .execute(&self.pool)
        .await?
        .rows_affected();

        if affected == 0 {
            return Err(BuildSubmissionRepoError::NotFound {
                submission_id: submission_id.to_string(),
            });
        }
        Ok(())
    }

    /// Updates cached builder status and sync timestamp for a submission.
    pub async fn update_cached_builder_status(
        &self,
        submission_id: &str,
        last_known_builder_status: Option<i32>,
        last_synced_at_ms: i64,
        updated_at_ms: i64,
    ) -> Result<(), BuildSubmissionRepoError> {
        let affected = sqlx::query(
            r#"
            UPDATE build_submissions
            SET
                last_known_builder_status = $1,
                last_synced_at_ms = $2,
                updated_at_ms = $3
            WHERE submission_id = $4
            "#,
        )
        .bind(last_known_builder_status)
        .bind(last_synced_at_ms)
        .bind(updated_at_ms)
        .bind(submission_id)
        .execute(&self.pool)
        .await?
        .rows_affected();

        if affected == 0 {
            return Err(BuildSubmissionRepoError::NotFound {
                submission_id: submission_id.to_string(),
            });
        }
        Ok(())
    }

    /// Marks submission cancellation request flag.
    pub async fn mark_cancellation_requested(
        &self,
        submission_id: &str,
        updated_at_ms: i64,
    ) -> Result<(), BuildSubmissionRepoError> {
        let affected = sqlx::query(
            r#"
            UPDATE build_submissions
            SET cancellation_requested = TRUE, updated_at_ms = $1
            WHERE submission_id = $2
            "#,
        )
        .bind(updated_at_ms)
        .bind(submission_id)
        .execute(&self.pool)
        .await?
        .rows_affected();

        if affected == 0 {
            return Err(BuildSubmissionRepoError::NotFound {
                submission_id: submission_id.to_string(),
            });
        }
        Ok(())
    }
}

fn decode_submission_row(
    row: sqlx::postgres::PgRow,
) -> Result<BuildSubmissionRecord, BuildSubmissionRepoError> {
    let submission_id: String = row.try_get("submission_id")?;
    let file_size_bytes_i64: i64 = row.try_get("file_size_bytes")?;
    let file_size_bytes = u64::try_from(file_size_bytes_i64).map_err(|_| {
        BuildSubmissionRepoError::NumericOutOfRange {
            submission_id: submission_id.clone(),
            field: "file_size_bytes",
        }
    })?;

    Ok(BuildSubmissionRecord {
        submission_id,
        upload_id: row.try_get("upload_id")?,
        team_id: row.try_get("team_id")?,
        requested_by_subject: row.try_get("requested_by_subject")?,
        original_file_name: row.try_get("original_file_name")?,
        file_size_bytes,
        sha256_hex: row.try_get("sha256_hex")?,
        staged_path: row.try_get("staged_path")?,
        builder_build_id: row.try_get("builder_build_id")?,
        cancellation_requested: row.try_get("cancellation_requested")?,
        retry_of_submission_id: row.try_get("retry_of_submission_id")?,
        last_known_builder_status: row.try_get("last_known_builder_status")?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
        last_synced_at_ms: row.try_get("last_synced_at_ms")?,
    })
}
