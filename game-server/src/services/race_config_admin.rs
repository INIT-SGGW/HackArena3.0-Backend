//! gRPC RaceConfigAdminService implementation.

mod mappers;

use std::sync::Arc;

use proto::race::v1::race_config_admin_service_server::RaceConfigAdminService;
use proto::race::v1::{
    CreateRaceConfigRequest, CreateRaceConfigResponse, DeleteRaceConfigRequest,
    DeleteRaceConfigResponse, GetRaceConfigsRequest, GetRaceConfigsResponse,
    UpdateRaceConfigRequest, UpdateRaceConfigResponse,
};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use self::mappers::{race_input_from_proto, race_to_proto, repo_schedule_to_domain};
use crate::auth::auth_claims::TokenValidator;
use crate::db::repos::race_config::{
    RaceConfigInputRecord, RaceConfigRecord, RaceConfigRepo, RaceConfigRepoError,
};
use crate::domain::race_config::{RaceConfigDomainError, validate_schedule};
use crate::services::public_menu_service::UpcomingRacesCacheInvalidation;

/// RaceConfigAdmin service.
#[derive(Clone)]
pub struct RaceConfigAdminServiceImpl {
    repo: RaceConfigRepo,
    token_validator: Arc<TokenValidator>,
    upcoming_invalidation: UpcomingRacesCacheInvalidation,
}

impl RaceConfigAdminServiceImpl {
    pub(crate) fn with_repo(
        repo: RaceConfigRepo,
        token_validator: Arc<TokenValidator>,
        upcoming_invalidation: UpcomingRacesCacheInvalidation,
    ) -> Self {
        Self {
            repo,
            token_validator,
            upcoming_invalidation,
        }
    }

    async fn require_admin(&self, metadata: &MetadataMap) -> Result<(), Status> {
        let is_admin = self.token_validator.is_admin(metadata).await?;
        if !is_admin {
            return Err(Status::permission_denied("admin role required"));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl RaceConfigAdminService for RaceConfigAdminServiceImpl {
    async fn get_race_configs(
        &self,
        request: Request<GetRaceConfigsRequest>,
    ) -> Result<Response<GetRaceConfigsResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let snapshot = self
            .repo
            .get_snapshot()
            .await
            .map_err(map_repo_error_to_status)?;

        Ok(Response::new(GetRaceConfigsResponse {
            revision: snapshot.revision,
            races: snapshot.races.into_iter().map(race_to_proto).collect(),
        }))
    }

    async fn create_race_config(
        &self,
        request: Request<CreateRaceConfigRequest>,
    ) -> Result<Response<CreateRaceConfigResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        let config = request
            .config
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("config is required"))
            .and_then(race_input_from_proto)?;
        let race_id = race_id_v5(&config, request.expected_revision);
        let race = RaceConfigRecord { race_id, config };

        let snapshot = self
            .repo
            .get_snapshot()
            .await
            .map_err(map_repo_error_to_status)?;
        ensure_expected_revision(request.expected_revision, snapshot.revision)?;

        let mut candidate = snapshot.races;
        candidate.push(race.clone());
        sort_by_start_time(&mut candidate);
        validate_candidate_schedule(&candidate)?;

        let revision = self
            .repo
            .create_config(request.expected_revision, &race)
            .await
            .map_err(map_repo_error_to_status)?;
        self.upcoming_invalidation
            .invalidate_for_change(None, Some(race.config.starts_at_ms));

        Ok(Response::new(CreateRaceConfigResponse {
            revision,
            race: Some(race_to_proto(race)),
        }))
    }

    async fn update_race_config(
        &self,
        request: Request<UpdateRaceConfigRequest>,
    ) -> Result<Response<UpdateRaceConfigResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        if request.race_id.trim().is_empty() {
            return Err(Status::invalid_argument("race_id must be non-empty"));
        }

        let config = request
            .config
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("config is required"))
            .and_then(race_input_from_proto)?;

        let snapshot = self
            .repo
            .get_snapshot()
            .await
            .map_err(map_repo_error_to_status)?;
        ensure_expected_revision(request.expected_revision, snapshot.revision)?;

        let mut candidate = snapshot.races;
        let Some(entry) = candidate
            .iter_mut()
            .find(|entry| entry.race_id == request.race_id)
        else {
            return Err(Status::not_found(format!(
                "race config not found: {}",
                request.race_id
            )));
        };
        let previous_starts_at_ms = Some(entry.config.starts_at_ms);
        entry.config = config.clone();
        sort_by_start_time(&mut candidate);
        validate_candidate_schedule(&candidate)?;

        let race = RaceConfigRecord {
            race_id: request.race_id,
            config,
        };
        let revision = self
            .repo
            .update_config(request.expected_revision, &race)
            .await
            .map_err(map_repo_error_to_status)?;
        self.upcoming_invalidation
            .invalidate_for_change(previous_starts_at_ms, Some(race.config.starts_at_ms));

        Ok(Response::new(UpdateRaceConfigResponse {
            revision,
            race: Some(race_to_proto(race)),
        }))
    }

    async fn delete_race_config(
        &self,
        request: Request<DeleteRaceConfigRequest>,
    ) -> Result<Response<DeleteRaceConfigResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        if request.race_id.trim().is_empty() {
            return Err(Status::invalid_argument("race_id must be non-empty"));
        }

        let snapshot = self
            .repo
            .get_snapshot()
            .await
            .map_err(map_repo_error_to_status)?;
        ensure_expected_revision(request.expected_revision, snapshot.revision)?;
        let deleted_starts_at_ms = snapshot
            .races
            .iter()
            .find(|entry| entry.race_id == request.race_id)
            .map(|entry| entry.config.starts_at_ms);
        if deleted_starts_at_ms.is_none() {
            return Err(Status::not_found(format!(
                "race config not found: {}",
                request.race_id
            )));
        }

        let revision = self
            .repo
            .delete_config(request.expected_revision, &request.race_id)
            .await
            .map_err(map_repo_error_to_status)?;
        self.upcoming_invalidation
            .invalidate_for_change(deleted_starts_at_ms, None);

        Ok(Response::new(DeleteRaceConfigResponse {
            revision,
            race_id: request.race_id,
        }))
    }
}

fn ensure_expected_revision(expected_revision: u64, current_revision: u64) -> Result<(), Status> {
    if expected_revision == current_revision {
        return Ok(());
    }

    Err(Status::failed_precondition(format!(
        "race config revision mismatch: expected {expected_revision}, actual {current_revision}"
    )))
}

fn sort_by_start_time(entries: &mut [RaceConfigRecord]) {
    entries.sort_by(|a, b| {
        a.config
            .starts_at_ms
            .cmp(&b.config.starts_at_ms)
            .then_with(|| a.race_id.cmp(&b.race_id))
    });
}

fn validate_candidate_schedule(entries: &[RaceConfigRecord]) -> Result<(), Status> {
    let domain_entries = repo_schedule_to_domain(entries)?;
    validate_schedule(&domain_entries).map_err(map_domain_error_to_status)
}

fn map_domain_error_to_status(err: RaceConfigDomainError) -> Status {
    Status::invalid_argument(err.to_string())
}

fn map_repo_error_to_status(err: RaceConfigRepoError) -> Status {
    match err {
        RaceConfigRepoError::RevisionMismatch { .. } => {
            Status::failed_precondition(err.to_string())
        }
        RaceConfigRepoError::AlreadyExists { .. } => Status::already_exists(err.to_string()),
        RaceConfigRepoError::NotFound { .. } => Status::not_found(err.to_string()),
        RaceConfigRepoError::InvalidStartPlacementMode
        | RaceConfigRepoError::InvalidTimeOfDayPreset => Status::invalid_argument(err.to_string()),
        RaceConfigRepoError::Sqlx(_)
        | RaceConfigRepoError::StateMissing
        | RaceConfigRepoError::NumericOutOfRange { .. }
        | RaceConfigRepoError::RevisionOverflow => Status::internal(err.to_string()),
    }
}

fn race_id_v5(config: &RaceConfigInputRecord, expected_revision: u64) -> String {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    let payload = format!(
        "expected_revision={};race_name={};starts_at_ms={};race_duration_sec={};map_id={};start_placement_mode={};time_of_day_preset={};ts_ns={}",
        expected_revision,
        config.race_name,
        config.starts_at_ms,
        config.race_duration_sec,
        config.map_id,
        config.start_placement_mode as i32,
        config.time_of_day_preset as i32,
        duration.as_nanos(),
    );
    Uuid::new_v5(&Uuid::NAMESPACE_OID, payload.as_bytes()).to_string()
}
