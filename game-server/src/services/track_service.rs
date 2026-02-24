//! gRPC TrackService implementation for static centerline geometry.

use proto::race::v1::track_service_server::TrackService;
use proto::race::v1::{GetTrackDataRequest, GetTrackDataResponse};
use tonic::{Request, Response, Status};

use crate::runtime::engine_worker::EngineClient;
use crate::services::error_map::map_worker_err;
use crate::services::mappers::track_data_to_proto;

/// gRPC TrackService implementation backed by the engine worker.
#[derive(Clone)]
pub struct TrackServiceImpl {
    engine: EngineClient,
}

impl TrackServiceImpl {
    /// Builds the service with access to engine worker commands.
    pub fn new(engine: EngineClient) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl TrackService for TrackServiceImpl {
    async fn get_track_data(
        &self,
        request: Request<GetTrackDataRequest>,
    ) -> Result<Response<GetTrackDataResponse>, Status> {
        let GetTrackDataRequest {
            map_id,
            map_version,
        } = request.into_inner();

        if map_id.trim().is_empty() {
            return Err(Status::invalid_argument("map_id is required"));
        }

        let track = self.engine.track_data().await.map_err(map_worker_err)?;

        if track.map_id != map_id {
            return Err(Status::not_found("track not found"));
        }

        if let Some(version) = map_version {
            if track.version != version {
                return Err(Status::not_found("track version not found"));
            }
        }

        let response = GetTrackDataResponse {
            track: Some(track_data_to_proto(track)),
        };
        Ok(Response::new(response))
    }
}
