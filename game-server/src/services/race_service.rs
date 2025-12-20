//! gRPC RaceService implementation and transport mapping.

use std::sync::Arc;

use proto::race::v1::{
    GetCarStateRequest, GetCarStateResponse, QuickJoinRequest, QuickJoinResponse,
    SetControlsRequest, SetControlsResponse, race_service_server::RaceService,
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, server::NamedService};

use crate::runtime::engine_worker::EngineClient;

use super::error_map::map_worker_err;
use super::mappers::{car_state_to_proto, proto_to_controls};

/// gRPC RaceService implementation backed by a single engine world.
#[derive(Clone)]
pub struct RaceServiceImpl {
    engine: EngineClient,
    // Optional shared state for future extensions.
    _state: Arc<Mutex<()>>,
}

impl RaceServiceImpl {
    /// Build a Race service that talks to the engine worker.
    pub fn new(engine: EngineClient) -> Self {
        Self {
            engine,
            _state: Arc::new(Mutex::new(())),
        }
    }
}

impl NamedService for RaceServiceImpl {
    const NAME: &'static str = "proto.race.v1.RaceService";
}

#[tonic::async_trait]
impl RaceService for RaceServiceImpl {
    async fn quick_join(
        &self,
        _request: Request<QuickJoinRequest>,
    ) -> Result<Response<QuickJoinResponse>, Status> {
        // One car per client, no teams, no auth.
        let engine = self.engine.clone();
        let car_id = engine.spawn_car().await.map_err(map_worker_err)?;

        let resp = QuickJoinResponse {
            car_id,
            map_id: "test".into(),
        };

        Ok(Response::new(resp))
    }

    async fn set_controls(
        &self,
        request: Request<SetControlsRequest>,
    ) -> Result<Response<SetControlsResponse>, Status> {
        let req = request.into_inner();
        let controls = proto_to_controls(&req);

        self.engine
            .set_controls(req.car_id, controls)
            .await
            .map_err(map_worker_err)?;

        Ok(Response::new(SetControlsResponse {}))
    }

    async fn get_car_state(
        &self,
        request: Request<GetCarStateRequest>,
    ) -> Result<Response<GetCarStateResponse>, Status> {
        let req = request.into_inner();

        let state = self
            .engine
            .read_car_state(req.car_id)
            .await
            .map_err(map_worker_err)?;

        let resp = car_state_to_proto(req.car_id, state);

        Ok(Response::new(resp))
    }
}
