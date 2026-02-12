//! gRPC WeatherQueryService scaffold.

use proto::weather::v1::weather_query_service_server::WeatherQueryService;
use proto::weather::v1::{
    ForecastUpdateEvent, GetForecastNowRequest, GetForecastNowResponse, GetWeatherNowRequest,
    GetWeatherNowResponse, StreamForecastUpdatesRequest, StreamWeatherUpdatesRequest,
    WeatherUpdateEvent,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// Placeholder WeatherQuery service implementation.
#[derive(Clone, Default)]
pub struct WeatherQueryServiceImpl;

#[tonic::async_trait]
impl WeatherQueryService for WeatherQueryServiceImpl {
    type StreamWeatherUpdatesStream = ReceiverStream<Result<WeatherUpdateEvent, Status>>;
    type StreamForecastUpdatesStream = ReceiverStream<Result<ForecastUpdateEvent, Status>>;

    async fn get_weather_now(
        &self,
        _request: Request<GetWeatherNowRequest>,
    ) -> Result<Response<GetWeatherNowResponse>, Status> {
        Err(Status::unimplemented(
            "weather query service not implemented yet",
        ))
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
        _request: Request<GetForecastNowRequest>,
    ) -> Result<Response<GetForecastNowResponse>, Status> {
        Err(Status::unimplemented(
            "weather query service not implemented yet",
        ))
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
