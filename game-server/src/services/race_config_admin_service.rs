//! gRPC RaceConfigAdminService implementation.

use proto::race::v1::race_config_admin_service_server::RaceConfigAdminService;
use proto::race::v1::{
    GetRaceConfigScheduleRequest, GetRaceConfigScheduleResponse, ReplaceRaceConfigScheduleRequest,
    ReplaceRaceConfigScheduleResponse,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::domain::race_config::{
    RaceConfigDomainError, RaceConfigInput, validate_draft_schedule, validate_schedule,
};
#[cfg(feature = "official")]
use crate::services::race_config_mappers::{domain_schedule_to_repo, repo_schedule_to_domain};
use crate::services::race_config_mappers::{draft_input_from_proto, schedule_entry_to_proto};

#[cfg(feature = "official")]
use crate::db::repos::race_config::RaceConfigRepo;

/// RaceConfigAdmin service backed by persisted race config schedule.
#[derive(Clone)]
pub struct RaceConfigAdminServiceImpl {
    #[cfg(feature = "official")]
    repo: Option<RaceConfigRepo>,
}

impl RaceConfigAdminServiceImpl {
    #[cfg(feature = "official")]
    pub fn with_repo(repo: RaceConfigRepo) -> Self {
        Self { repo: Some(repo) }
    }
}

impl Default for RaceConfigAdminServiceImpl {
    fn default() -> Self {
        Self {
            #[cfg(feature = "official")]
            repo: None,
        }
    }
}

#[tonic::async_trait]
impl RaceConfigAdminService for RaceConfigAdminServiceImpl {
    async fn get_race_config_schedule(
        &self,
        _request: Request<GetRaceConfigScheduleRequest>,
    ) -> Result<Response<GetRaceConfigScheduleResponse>, Status> {
        let entries = self.load_schedule().await?;
        let races = entries.into_iter().map(schedule_entry_to_proto).collect();
        Ok(Response::new(GetRaceConfigScheduleResponse { races }))
    }

    async fn replace_race_config_schedule(
        &self,
        request: Request<ReplaceRaceConfigScheduleRequest>,
    ) -> Result<Response<ReplaceRaceConfigScheduleResponse>, Status> {
        let request = request.into_inner();

        let draft_entries: Vec<_> = request
            .races
            .iter()
            .map(draft_input_from_proto)
            .collect::<Result<_, Status>>()?;
        validate_draft_schedule(&draft_entries).map_err(map_domain_error_to_status)?;

        let persisted_schedule = persisted_schedule_from_draft(&draft_entries);
        validate_schedule(&persisted_schedule).map_err(map_domain_error_to_status)?;
        self.replace_schedule(&persisted_schedule).await?;

        let races = persisted_schedule
            .into_iter()
            .map(schedule_entry_to_proto)
            .collect();
        Ok(Response::new(ReplaceRaceConfigScheduleResponse { races }))
    }
}

impl RaceConfigAdminServiceImpl {
    async fn load_schedule(
        &self,
    ) -> Result<Vec<crate::domain::race_config::ScheduleEntry>, Status> {
        #[cfg(feature = "official")]
        {
            let repo = self.repo.as_ref().ok_or_else(|| {
                Status::failed_precondition("race config admin service is not configured")
            })?;
            let rows = repo.get_schedule().await.map_err(|err| {
                Status::internal(format!("failed to load race config schedule: {err}"))
            })?;
            return Ok(repo_schedule_to_domain(rows));
        }

        #[cfg(not(feature = "official"))]
        {
            Err(Status::unimplemented(
                "race config admin service is available only in official backend",
            ))
        }
    }

    async fn replace_schedule(
        &self,
        entries: &[crate::domain::race_config::ScheduleEntry],
    ) -> Result<(), Status> {
        #[cfg(feature = "official")]
        {
            let repo = self.repo.as_ref().ok_or_else(|| {
                Status::failed_precondition("race config admin service is not configured")
            })?;
            let rows = domain_schedule_to_repo(entries);
            return repo.replace_schedule(&rows).await.map_err(|err| {
                Status::internal(format!("failed to replace race config schedule: {err}"))
            });
        }

        #[cfg(not(feature = "official"))]
        {
            let _ = entries;
            Err(Status::unimplemented(
                "race config admin service is available only in official backend",
            ))
        }
    }
}

fn persisted_schedule_from_draft(
    draft: &[RaceConfigInput],
) -> Vec<crate::domain::race_config::ScheduleEntry> {
    draft
        .iter()
        .enumerate()
        .map(|(idx, input)| crate::domain::race_config::ScheduleEntry {
            race_id: race_id_v5(input, idx),
            config: input.clone(),
        })
        .collect()
}

fn race_id_v5(input: &RaceConfigInput, idx: usize) -> String {
    let map_version = input
        .map_version
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string());
    let payload = format!(
        "idx={idx};race_name={};starts_at_ms={};ends_at_ms={};map_id={};map_version={};start_placement_mode={};points_multiplier_fixed={};time_of_day_preset={}",
        input.race_name,
        input.starts_at_ms,
        input.ends_at_ms,
        input.map_id,
        map_version,
        input.start_placement_mode as i32,
        input.points_multiplier_fixed,
        input.time_of_day_preset as i32
    );
    Uuid::new_v5(&Uuid::NAMESPACE_OID, payload.as_bytes()).to_string()
}

fn map_domain_error_to_status(err: RaceConfigDomainError) -> Status {
    Status::invalid_argument(err.to_string())
}
