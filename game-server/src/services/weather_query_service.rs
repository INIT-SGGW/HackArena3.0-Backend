//! gRPC WeatherQueryService implementation.

use std::time::{SystemTime, UNIX_EPOCH};

use proto::weather::v1::weather_query_service_server::WeatherQueryService;
use proto::weather::v1::{
    ForecastPreset, ForecastUpdateEvent, GetForecastNowRequest, GetForecastNowResponse,
    GetWeatherNowRequest, GetWeatherNowResponse, StreamForecastUpdatesRequest,
    StreamWeatherUpdatesRequest, WeatherNow, WeatherType, WeatherUpdateEvent,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::domain::weather::{
    WeatherDomainError, align_start_to_preset_slot, project_forecast, weather_type_at,
};
use crate::services::weather_mappers::forecast_point_to_proto;

#[cfg(feature = "official")]
use crate::db::repos::weather::WeatherRepo;

/// WeatherQuery service backed by global weather schedule.
#[derive(Clone, Default)]
pub struct WeatherQueryServiceImpl {
    #[cfg(feature = "official")]
    repo: Option<WeatherRepo>,
}

impl WeatherQueryServiceImpl {
    #[cfg(feature = "official")]
    pub fn with_repo(repo: WeatherRepo) -> Self {
        Self { repo: Some(repo) }
    }
}

#[tonic::async_trait]
impl WeatherQueryService for WeatherQueryServiceImpl {
    type StreamWeatherUpdatesStream = ReceiverStream<Result<WeatherUpdateEvent, Status>>;
    type StreamForecastUpdatesStream = ReceiverStream<Result<ForecastUpdateEvent, Status>>;

    async fn get_weather_now(
        &self,
        _request: Request<GetWeatherNowRequest>,
    ) -> Result<Response<GetWeatherNowResponse>, Status> {
        let now_ms = current_time_ms();
        let schedule = self.load_schedule().await?;
        let weather_type = weather_type_at(&schedule, now_ms).unwrap_or(WeatherType::Unspecified);

        Ok(Response::new(GetWeatherNowResponse {
            now: Some(WeatherNow {
                r#type: weather_type as i32,
            }),
        }))
    }

    async fn stream_weather_updates(
        &self,
        _request: Request<StreamWeatherUpdatesRequest>,
    ) -> Result<Response<Self::StreamWeatherUpdatesStream>, Status> {
        Err(Status::unimplemented(
            "weather query service not implemented yet",
        ))
    }

    async fn get_forecast_now(
        &self,
        request: Request<GetForecastNowRequest>,
    ) -> Result<Response<GetForecastNowResponse>, Status> {
        let preset = extract_preset(request.into_inner())?;
        let now_ms = current_time_ms();
        let start_ms =
            align_start_to_preset_slot(now_ms, preset).map_err(map_domain_error_to_status)?;

        let schedule = self.load_schedule().await?;
        let points =
            project_forecast(&schedule, start_ms, preset).map_err(map_domain_error_to_status)?;
        let response_points = points.into_iter().map(forecast_point_to_proto).collect();

        Ok(Response::new(GetForecastNowResponse {
            points: response_points,
        }))
    }

    async fn stream_forecast_updates(
        &self,
        _request: Request<StreamForecastUpdatesRequest>,
    ) -> Result<Response<Self::StreamForecastUpdatesStream>, Status> {
        Err(Status::unimplemented(
            "weather query service not implemented yet",
        ))
    }
}

impl WeatherQueryServiceImpl {
    async fn load_schedule(&self) -> Result<Vec<crate::domain::weather::ScheduleEntry>, Status> {
        #[cfg(feature = "official")]
        {
            let repo = self.repo.as_ref().ok_or_else(|| {
                Status::failed_precondition("weather query service is not configured")
            })?;
            return repo.get_schedule().await.map_err(|err| {
                Status::internal(format!("failed to load weather schedule: {err}"))
            });
        }

        #[cfg(not(feature = "official"))]
        {
            Err(Status::unimplemented(
                "weather query service is available only in official backend",
            ))
        }
    }
}

fn extract_preset(request: GetForecastNowRequest) -> Result<ForecastPreset, Status> {
    let spec = request
        .spec
        .ok_or_else(|| Status::invalid_argument("spec is required"))?;
    let preset = ForecastPreset::try_from(spec.preset)
        .map_err(|_| Status::invalid_argument("invalid forecast preset"))?;
    if matches!(preset, ForecastPreset::Unspecified) {
        return Err(Status::invalid_argument(
            "forecast preset must be specified",
        ));
    }
    Ok(preset)
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

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
