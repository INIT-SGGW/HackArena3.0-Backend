//! gRPC TrackService implementation for static centerline geometry.

use proto::race::v1::track_service_server::TrackService;
use proto::race::v1::{GetTrackDataRequest, GetTrackDataResponse};
use tonic::{Request, Response, Status};

use crate::runtime::engine_worker::{
    EngineActivityKind, EngineClient, EngineCommandTarget, EngineRuntimeState,
};
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

    fn resolve_track_target(
        runtime_state: &EngineRuntimeState,
        map_id: &str,
    ) -> Result<EngineCommandTarget, Status> {
        match runtime_state.activity_kind {
            EngineActivityKind::OfficialRace => {
                if runtime_state.map_id != map_id {
                    return Err(Status::not_found("track not found"));
                }
                Ok(EngineCommandTarget::OfficialRace)
            }
            EngineActivityKind::Sandbox => {
                let matching: Vec<_> = runtime_state
                    .active_sandboxes
                    .iter()
                    .filter(|entry| entry.map_id == map_id)
                    .collect();

                if matching.is_empty() {
                    return Err(Status::not_found("track not found"));
                }
                if matching.len() > 1 {
                    return Err(Status::failed_precondition(
                        "multiple active sandbox sessions match requested map_id",
                    ));
                }

                Ok(EngineCommandTarget::Sandbox {
                    sandbox_id: matching[0].sandbox_id.clone(),
                })
            }
            EngineActivityKind::None => Err(Status::failed_precondition(
                "runtime is not active; cannot read track data",
            )),
        }
    }
}

#[tonic::async_trait]
impl TrackService for TrackServiceImpl {
    async fn get_track_data(
        &self,
        request: Request<GetTrackDataRequest>,
    ) -> Result<Response<GetTrackDataResponse>, Status> {
        let GetTrackDataRequest { map_id } = request.into_inner();

        if map_id.trim().is_empty() {
            return Err(Status::invalid_argument("map_id is required"));
        }

        let runtime_state = self.engine.runtime_state().await.map_err(map_worker_err)?;
        let target = Self::resolve_track_target(&runtime_state, &map_id)?;
        let mut track = self
            .engine
            .track_data_in(target)
            .await
            .map_err(map_worker_err)?;
        track.map_id = map_id.clone();

        let response = GetTrackDataResponse {
            track: Some(track_data_to_proto(track)),
        };
        Ok(Response::new(response))
    }
}
