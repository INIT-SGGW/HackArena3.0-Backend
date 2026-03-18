use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use proto::hackarena::build::v1::{
    BuildJob, BuildStatus, CancelBuildRequest, DockerTemplateReference, GetBuildRequest,
    ListBuildsRequest, SubmitBuildRequest,
};
use thiserror::Error;

use crate::db::repos::build_submission::{BuildSubmissionRecord, BuildSubmissionRepo};

use super::{
    BuildGrpcClient, BuildGrpcClientError, FsUploadStager, UploadStageInput, UploadStagingError,
};

/// Input for backend-orchestrated build submission.
#[derive(Debug, Clone)]
pub struct SubmitBuildUploadInput {
    pub submission_id: String,
    pub upload_id: String,
    pub team_id: String,
    pub requested_by_subject: String,
    pub original_file_name: String,
    pub archive_bytes: Bytes,
    pub docker_template: Option<DockerTemplateReference>,
    pub retry_of_submission_id: Option<String>,
}

/// Result of orchestrated submission.
#[derive(Debug, Clone)]
pub struct SubmitBuildUploadResult {
    pub submission_id: String,
    pub upload_id: String,
    pub build: BuildJob,
    /// True when submission already existed and backend returned deduplicated build state.
    pub deduplicated: bool,
}

/// Input for backend-orchestrated cancellation.
#[derive(Debug, Clone)]
pub struct CancelSubmissionInput {
    pub submission_id: String,
    pub team_id: String,
    pub requested_by_subject: String,
    pub reason: String,
}

/// Internal orchestrator for backend <-> builder submission flow.
#[derive(Clone)]
pub struct BuildSubmissionService {
    repo: BuildSubmissionRepo,
    stager: FsUploadStager,
    client: BuildGrpcClient,
}

impl BuildSubmissionService {
    pub fn new(repo: BuildSubmissionRepo, stager: FsUploadStager, client: BuildGrpcClient) -> Self {
        Self {
            repo,
            stager,
            client,
        }
    }

    /// Stages upload (when needed), persists metadata, submits build and stores builder mapping.
    pub async fn submit_build_upload(
        &self,
        input: SubmitBuildUploadInput,
    ) -> Result<SubmitBuildUploadResult, BuildSubmissionServiceError> {
        validate_required(&input.submission_id, "submission_id")?;
        validate_required(&input.upload_id, "upload_id")?;
        validate_required(&input.team_id, "team_id")?;
        validate_required(&input.requested_by_subject, "requested_by_subject")?;

        if let Some(existing) = self.repo.get_submission(&input.submission_id).await? {
            ensure_team_match(&existing, &input.team_id)?;
            if let Some(build_id) = existing.builder_build_id.clone() {
                let build = self
                    .fetch_build_and_cache(&input.submission_id, &build_id)
                    .await?;
                return Ok(SubmitBuildUploadResult {
                    submission_id: existing.submission_id,
                    upload_id: existing.upload_id,
                    build,
                    deduplicated: true,
                });
            }
            if existing.upload_id != input.upload_id {
                return Err(BuildSubmissionServiceError::SubmissionAlreadyExists {
                    submission_id: input.submission_id,
                });
            }

            let idempotency_key = make_idempotency_key(
                &existing.team_id,
                &existing.submission_id,
                &existing.upload_id,
                &existing.sha256_hex,
            );
            let build = self
                .submit_to_builder(
                    &existing.team_id,
                    &existing.submission_id,
                    &existing.upload_id,
                    &input.requested_by_subject,
                    input.docker_template,
                    idempotency_key,
                )
                .await?;

            self.repo
                .set_builder_build_id(&existing.submission_id, &build.build_id, now_ms())
                .await?;
            self.repo
                .update_cached_builder_status(
                    &existing.submission_id,
                    Some(build.status),
                    now_ms(),
                    now_ms(),
                )
                .await?;

            return Ok(SubmitBuildUploadResult {
                submission_id: existing.submission_id,
                upload_id: existing.upload_id,
                build,
                deduplicated: false,
            });
        }

        let staged = self
            .stager
            .stage_zip(UploadStageInput {
                team_id: input.team_id.clone(),
                upload_id: input.upload_id.clone(),
                original_file_name: input.original_file_name.clone(),
                archive_bytes: input.archive_bytes,
            })
            .await?;

        let created_at = now_ms();
        self.repo
            .insert_submission(&BuildSubmissionRecord {
                submission_id: input.submission_id.clone(),
                upload_id: input.upload_id.clone(),
                team_id: input.team_id.clone(),
                requested_by_subject: input.requested_by_subject.clone(),
                original_file_name: input.original_file_name,
                file_size_bytes: staged.file_size,
                sha256_hex: staged.sha256_hex.clone(),
                staged_path: staged.staged_path.display().to_string(),
                builder_build_id: None,
                cancellation_requested: false,
                retry_of_submission_id: input.retry_of_submission_id,
                last_known_builder_status: None,
                created_at_ms: created_at,
                updated_at_ms: created_at,
                last_synced_at_ms: None,
            })
            .await?;

        let idempotency_key = make_idempotency_key(
            &input.team_id,
            &input.submission_id,
            &input.upload_id,
            &staged.sha256_hex,
        );
        let build = self
            .submit_to_builder(
                &input.team_id,
                &input.submission_id,
                &input.upload_id,
                &input.requested_by_subject,
                input.docker_template,
                idempotency_key,
            )
            .await?;

        self.repo
            .set_builder_build_id(&input.submission_id, &build.build_id, now_ms())
            .await?;
        self.repo
            .update_cached_builder_status(
                &input.submission_id,
                Some(build.status),
                now_ms(),
                now_ms(),
            )
            .await?;

        Ok(SubmitBuildUploadResult {
            submission_id: input.submission_id,
            upload_id: input.upload_id,
            build,
            deduplicated: false,
        })
    }

    /// Reads submission from repo and fetches authoritative build state from builder.
    pub async fn get_submission_build(
        &self,
        team_id: &str,
        submission_id: &str,
    ) -> Result<BuildJob, BuildSubmissionServiceError> {
        validate_required(team_id, "team_id")?;
        validate_required(submission_id, "submission_id")?;

        let submission = self
            .repo
            .get_submission(submission_id)
            .await?
            .ok_or_else(|| BuildSubmissionServiceError::SubmissionNotFound {
                submission_id: submission_id.to_string(),
            })?;
        ensure_team_match(&submission, team_id)?;
        let build_id = submission.builder_build_id.ok_or_else(|| {
            BuildSubmissionServiceError::BuilderBuildIdMissing {
                submission_id: submission_id.to_string(),
            }
        })?;

        self.fetch_build_and_cache(submission_id, &build_id).await
    }

    /// Lists builds for team from authoritative builder API.
    pub async fn list_team_builds(
        &self,
        team_id: &str,
        status: Option<BuildStatus>,
        limit: u32,
    ) -> Result<Vec<BuildJob>, BuildSubmissionServiceError> {
        validate_required(team_id, "team_id")?;
        let effective_limit = if limit == 0 { 50 } else { limit };
        let effective_status = status.unwrap_or(BuildStatus::Unspecified) as i32;
        let response = self
            .client
            .list_builds(ListBuildsRequest {
                team_id: team_id.to_string(),
                status: effective_status,
                limit: effective_limit,
            })
            .await?;
        Ok(response.builds)
    }

    /// Cancels build for submission scoped to team ownership.
    pub async fn cancel_submission(
        &self,
        input: CancelSubmissionInput,
    ) -> Result<BuildJob, BuildSubmissionServiceError> {
        validate_required(&input.submission_id, "submission_id")?;
        validate_required(&input.team_id, "team_id")?;
        validate_required(&input.requested_by_subject, "requested_by_subject")?;

        let submission = self
            .repo
            .get_submission(&input.submission_id)
            .await?
            .ok_or_else(|| BuildSubmissionServiceError::SubmissionNotFound {
                submission_id: input.submission_id.clone(),
            })?;
        ensure_team_match(&submission, &input.team_id)?;

        let build_id = submission.builder_build_id.ok_or_else(|| {
            BuildSubmissionServiceError::BuilderBuildIdMissing {
                submission_id: input.submission_id.clone(),
            }
        })?;
        let reason = if input.reason.trim().is_empty() {
            "cancelled by backend orchestration".to_string()
        } else {
            input.reason
        };

        let response = self
            .client
            .cancel_build(CancelBuildRequest {
                build_id,
                requested_by_subject: input.requested_by_subject,
                reason,
            })
            .await?;
        let build =
            response
                .build
                .ok_or(BuildSubmissionServiceError::BuilderResponseMissingBuild {
                    operation: "CancelBuild",
                })?;

        self.repo
            .mark_cancellation_requested(&input.submission_id, now_ms())
            .await?;
        self.repo
            .update_cached_builder_status(
                &input.submission_id,
                Some(build.status),
                now_ms(),
                now_ms(),
            )
            .await?;
        Ok(build)
    }

    async fn submit_to_builder(
        &self,
        team_id: &str,
        submission_id: &str,
        upload_id: &str,
        requested_by_subject: &str,
        docker_template: Option<DockerTemplateReference>,
        idempotency_key: String,
    ) -> Result<BuildJob, BuildSubmissionServiceError> {
        let response = self
            .client
            .submit_build(SubmitBuildRequest {
                team_id: team_id.to_string(),
                submission_id: submission_id.to_string(),
                upload_id: upload_id.to_string(),
                docker_template,
                idempotency_key,
                requested_by_subject: requested_by_subject.to_string(),
            })
            .await?;

        let build =
            response
                .build
                .ok_or(BuildSubmissionServiceError::BuilderResponseMissingBuild {
                    operation: "SubmitBuild",
                })?;
        if build.build_id.trim().is_empty() {
            return Err(BuildSubmissionServiceError::BuilderResponseMissingBuildId {
                submission_id: submission_id.to_string(),
            });
        }
        Ok(build)
    }

    async fn fetch_build_and_cache(
        &self,
        submission_id: &str,
        build_id: &str,
    ) -> Result<BuildJob, BuildSubmissionServiceError> {
        let response = self
            .client
            .get_build(GetBuildRequest {
                build_id: build_id.to_string(),
            })
            .await?;
        let build =
            response
                .build
                .ok_or(BuildSubmissionServiceError::BuilderResponseMissingBuild {
                    operation: "GetBuild",
                })?;
        self.repo
            .update_cached_builder_status(submission_id, Some(build.status), now_ms(), now_ms())
            .await?;
        Ok(build)
    }
}

#[derive(Debug, Error)]
pub enum BuildSubmissionServiceError {
    #[error("{field} must be non-empty")]
    InvalidInput { field: &'static str },
    #[error("submission does not belong to team `{team_id}`")]
    TeamMismatch { team_id: String },
    #[error("submission already exists: {submission_id}")]
    SubmissionAlreadyExists { submission_id: String },
    #[error("submission not found: {submission_id}")]
    SubmissionNotFound { submission_id: String },
    #[error("builder build_id is not attached to submission: {submission_id}")]
    BuilderBuildIdMissing { submission_id: String },
    #[error("builder response for {operation} did not contain build")]
    BuilderResponseMissingBuild { operation: &'static str },
    #[error("builder response did not contain build_id for submission: {submission_id}")]
    BuilderResponseMissingBuildId { submission_id: String },
    #[error(transparent)]
    Repo(#[from] crate::db::repos::build_submission::BuildSubmissionRepoError),
    #[error(transparent)]
    Staging(#[from] UploadStagingError),
    #[error(transparent)]
    BuilderClient(#[from] BuildGrpcClientError),
}

fn validate_required(value: &str, field: &'static str) -> Result<(), BuildSubmissionServiceError> {
    if value.trim().is_empty() {
        return Err(BuildSubmissionServiceError::InvalidInput { field });
    }
    Ok(())
}

fn ensure_team_match(
    submission: &BuildSubmissionRecord,
    team_id: &str,
) -> Result<(), BuildSubmissionServiceError> {
    if submission.team_id == team_id {
        return Ok(());
    }
    Err(BuildSubmissionServiceError::TeamMismatch {
        team_id: team_id.to_string(),
    })
}

fn make_idempotency_key(
    team_id: &str,
    submission_id: &str,
    upload_id: &str,
    sha256_hex: &str,
) -> String {
    format!("{team_id}:{submission_id}:{upload_id}:{sha256_hex}")
}

fn now_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration
            .as_millis()
            .min(i64::MAX as u128)
            .try_into()
            .unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}
