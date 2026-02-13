//! gRPC WeatherAdminService implementation.

#[cfg(feature = "official")]
use std::sync::Arc;

use proto::weather::v1::weather_admin_service_server::WeatherAdminService;
use proto::weather::v1::{ForecastPreset, ReplaceWeatherScheduleResponse};
use proto::weather::v1::{
    GetWeatherScheduleRequest, GetWeatherScheduleResponse, ReplaceWeatherScheduleRequest,
    SimulateForecastRequest, SimulateForecastResponse,
};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::config::AppEnv;
use crate::domain::weather::{
    WeatherDomainError, project_forecast, unspecified_policy_for_env, validate_schedule,
};
use crate::services::weather_mappers::{
    forecast_point_to_proto, schedule_entry_from_proto, schedule_entry_to_proto, timestamp_to_ms,
};

#[cfg(feature = "official")]
use crate::db::repos::weather::WeatherRepo;
#[cfg(feature = "official")]
use crate::auth::keycloak::{TokenValidator, parse_bearer_token};

/// WeatherAdmin service backed by global weather schedule.
#[derive(Clone)]
pub struct WeatherAdminServiceImpl {
    #[cfg(feature = "official")]
    repo: Option<WeatherRepo>,
    #[cfg(feature = "official")]
    keycloak_validator: Option<Arc<TokenValidator>>,
    app_env: AppEnv,
}

impl WeatherAdminServiceImpl {
    #[cfg(feature = "official")]
    pub fn with_repo(
        repo: WeatherRepo,
        app_env: AppEnv,
        keycloak_validator: Arc<TokenValidator>,
    ) -> Self {
        Self {
            repo: Some(repo),
            keycloak_validator: Some(keycloak_validator),
            app_env,
        }
    }
}

impl Default for WeatherAdminServiceImpl {
    fn default() -> Self {
        Self {
            #[cfg(feature = "official")]
            repo: None,
            #[cfg(feature = "official")]
            keycloak_validator: None,
            app_env: AppEnv::Development,
        }
    }
}

#[tonic::async_trait]
impl WeatherAdminService for WeatherAdminServiceImpl {
    async fn get_weather_schedule(
        &self,
        request: Request<GetWeatherScheduleRequest>,
    ) -> Result<Response<GetWeatherScheduleResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let entries = self.load_schedule().await?;
        let entries = entries.into_iter().map(schedule_entry_to_proto).collect();
        Ok(Response::new(GetWeatherScheduleResponse { entries }))
    }

    async fn replace_weather_schedule(
        &self,
        request: Request<ReplaceWeatherScheduleRequest>,
    ) -> Result<Response<ReplaceWeatherScheduleResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        let entries: Vec<_> = request
            .entries
            .iter()
            .map(schedule_entry_from_proto)
            .collect::<Result<_, Status>>()?;

        validate_schedule(&entries, unspecified_policy_for_env(self.app_env))
            .map_err(map_domain_error_to_status)?;

        self.replace_schedule(&entries).await?;
        Ok(Response::new(ReplaceWeatherScheduleResponse {}))
    }

    async fn simulate_forecast(
        &self,
        request: Request<SimulateForecastRequest>,
    ) -> Result<Response<SimulateForecastResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        let spec = request
            .spec
            .ok_or_else(|| Status::invalid_argument("spec is required"))?;
        let start_time = spec
            .start_time
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("spec.start_time is required"))?;
        let start_ms = timestamp_to_ms(start_time)?;
        let preset = ForecastPreset::try_from(spec.preset)
            .map_err(|_| Status::invalid_argument("invalid forecast preset"))?;
        if matches!(preset, ForecastPreset::Unspecified) {
            return Err(Status::invalid_argument(
                "forecast preset must be specified",
            ));
        }

        let entries: Vec<_> = request
            .draft_entries
            .iter()
            .map(schedule_entry_from_proto)
            .collect::<Result<_, Status>>()?;

        validate_schedule(&entries, unspecified_policy_for_env(self.app_env))
            .map_err(map_domain_error_to_status)?;

        // TODO(weather): support stochastic projection based on spec.stochastic/spec.seed.
        let points =
            project_forecast(&entries, start_ms, preset).map_err(map_domain_error_to_status)?;
        let points = points.into_iter().map(forecast_point_to_proto).collect();
        Ok(Response::new(SimulateForecastResponse { points }))
    }
}

impl WeatherAdminServiceImpl {
    async fn require_admin(&self, metadata: &MetadataMap) -> Result<(), Status> {
        #[cfg(feature = "official")]
        {
            let token = parse_bearer_token(metadata)?
                .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
            let validator = self.keycloak_validator.as_ref().ok_or_else(|| {
                Status::failed_precondition("weather admin service auth is not configured")
            })?;
            let is_admin = validator.is_admin(&token).await?;
            if !is_admin {
                return Err(Status::permission_denied("admin role required"));
            }
            return Ok(());
        }

        #[cfg(not(feature = "official"))]
        {
            let _ = metadata;
            Err(Status::unimplemented(
                "weather admin service is available only in official backend",
            ))
        }
    }

    async fn load_schedule(&self) -> Result<Vec<crate::domain::weather::ScheduleEntry>, Status> {
        #[cfg(feature = "official")]
        {
            let repo = self.repo.as_ref().ok_or_else(|| {
                Status::failed_precondition("weather admin service is not configured")
            })?;
            return repo.get_schedule().await.map_err(|err| {
                Status::internal(format!("failed to load weather schedule: {err}"))
            });
        }

        #[cfg(not(feature = "official"))]
        {
            Err(Status::unimplemented(
                "weather admin service is available only in official backend",
            ))
        }
    }

    async fn replace_schedule(
        &self,
        entries: &[crate::domain::weather::ScheduleEntry],
    ) -> Result<(), Status> {
        #[cfg(feature = "official")]
        {
            let repo = self.repo.as_ref().ok_or_else(|| {
                Status::failed_precondition("weather admin service is not configured")
            })?;
            return repo.replace_schedule(entries).await.map_err(|err| {
                Status::internal(format!("failed to replace weather schedule: {err}"))
            });
        }

        #[cfg(not(feature = "official"))]
        {
            let _ = entries;
            Err(Status::unimplemented(
                "weather admin service is available only in official backend",
            ))
        }
    }
}

fn map_domain_error_to_status(err: WeatherDomainError) -> Status {
    match err {
        WeatherDomainError::UnspecifiedPreset
        | WeatherDomainError::UnspecifiedNotTail { .. }
        | WeatherDomainError::NonIncreasingTimestamp { .. } => {
            Status::invalid_argument(err.to_string())
        }
        WeatherDomainError::TimestampOverflow => Status::out_of_range(err.to_string()),
    }
}
