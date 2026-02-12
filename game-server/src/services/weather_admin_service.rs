//! gRPC WeatherAdminService scaffold.

use proto::weather::v1::weather_admin_service_server::WeatherAdminService;
use proto::weather::v1::{
    GetWeatherScheduleRequest, GetWeatherScheduleResponse, ReplaceWeatherScheduleRequest,
    ReplaceWeatherScheduleResponse, SimulateForecastRequest, SimulateForecastResponse,
};
use tonic::{Request, Response, Status};

/// Placeholder WeatherAdmin service implementation.
#[derive(Clone, Default)]
pub struct WeatherAdminServiceImpl;

#[tonic::async_trait]
impl WeatherAdminService for WeatherAdminServiceImpl {
    async fn get_weather_schedule(
        &self,
        _request: Request<GetWeatherScheduleRequest>,
    ) -> Result<Response<GetWeatherScheduleResponse>, Status> {
        Err(Status::unimplemented(
            "weather admin service not implemented yet",
        ))
    }

    async fn replace_weather_schedule(
        &self,
        _request: Request<ReplaceWeatherScheduleRequest>,
    ) -> Result<Response<ReplaceWeatherScheduleResponse>, Status> {
        Err(Status::unimplemented(
            "weather admin service not implemented yet",
        ))
    }

    async fn simulate_forecast(
        &self,
        _request: Request<SimulateForecastRequest>,
    ) -> Result<Response<SimulateForecastResponse>, Status> {
        Err(Status::unimplemented(
            "weather admin service not implemented yet",
        ))
    }
}
