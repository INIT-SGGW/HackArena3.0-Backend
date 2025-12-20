//! gRPC RaceService implementation and transport mapping.

use std::sync::Arc;

use dashmap::DashMap;
use proto::race::v1::{
    GetCarStateRequest, GetCarStateStreamResponse, QuickJoinRequest, QuickJoinResponse,
    SetControlsRequest, SetControlsResponse, StreamSettings,
    get_car_state_stream_response::Payload, race_service_server::RaceService,
};
use tokio::sync::{Mutex, mpsc};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, server::NamedService};

use crate::runtime::engine_worker::EngineClient;

use super::error_map::map_worker_err;
use super::mappers::{car_state_to_proto, proto_to_controls};

const DEFAULT_STREAM_HZ: u32 = 20;
const MIN_STREAM_HZ: u32 = 1;
const MAX_STREAM_HZ: u32 = 120;

/// gRPC RaceService implementation backed by a single engine world.
#[derive(Clone)]
pub struct RaceServiceImpl {
    engine: EngineClient,
    simulation_hz: u32,
    active_streams: DashMap<u64, ()>,
    // Optional shared state for future extensions.
    _state: Arc<Mutex<()>>,
}

impl RaceServiceImpl {
    /// Build a Race service that talks to the engine worker.
    pub fn new(engine: EngineClient, simulation_hz: u32) -> Self {
        Self {
            engine,
            simulation_hz,
            active_streams: DashMap::new(),
            _state: Arc::new(Mutex::new(())),
        }
    }
}

impl NamedService for RaceServiceImpl {
    const NAME: &'static str = "proto.race.v1.RaceService";
}

#[tonic::async_trait]
impl RaceService for RaceServiceImpl {
    type GetCarStateStream = ReceiverStream<Result<GetCarStateStreamResponse, Status>>;

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
    ) -> Result<Response<Self::GetCarStateStream>, Status> {
        let req = request.into_inner();

        if self.active_streams.contains_key(&req.car_id) {
            tracing::warn!(
                car_id = req.car_id,
                "car state stream already active; rejecting new stream"
            );
            return Err(Status::already_exists("car state stream already active"));
        }
        self.active_streams.insert(req.car_id, ());

        let engine = self.engine.clone();
        let simulation_hz = self.simulation_hz;
        let active_streams = self.active_streams.clone();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let requested_hz = if req.hz == 0 {
                DEFAULT_STREAM_HZ
            } else {
                req.hz
            };
            let max_hz = MAX_STREAM_HZ.min(simulation_hz);
            let effective_hz = requested_hz.clamp(MIN_STREAM_HZ, max_hz);
            let clamped = effective_hz != requested_hz;
            let period = Duration::from_secs_f64(1.0 / effective_hz as f64);
            let mut ticker = tokio::time::interval(period);

            tracing::info!(
                car_id = req.car_id,
                requested_hz,
                effective_hz,
                clamped,
                "car state stream started"
            );

            let settings = StreamSettings {
                requested_hz,
                effective_hz,
                clamped,
            };
            let settings_msg = GetCarStateStreamResponse {
                payload: Some(Payload::Settings(settings)),
            };
            if tx.try_send(Ok(settings_msg)).is_err() {
                active_streams.remove(&req.car_id);
                return;
            }

            loop {
                ticker.tick().await;

                let state = match engine.read_car_state(req.car_id).await {
                    Ok(state) => state,
                    Err(err) => {
                        tracing::warn!(
                            car_id = req.car_id,
                            error = %err,
                            "car state stream stopped due to engine error"
                        );
                        let _ = engine.despawn_car(req.car_id).await;
                        let _ = tx.send(Err(map_worker_err(err))).await;
                        break;
                    }
                };

                let resp = car_state_to_proto(req.car_id, state);
                let msg = GetCarStateStreamResponse {
                    payload: Some(Payload::State(resp)),
                };
                match tx.try_send(Ok(msg)) {
                    Ok(_) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        tracing::debug!(
                            car_id = req.car_id,
                            "car state stream stopped (client disconnected)"
                        );
                        if let Err(err) = engine.despawn_car(req.car_id).await {
                            tracing::warn!(
                                car_id = req.car_id,
                                error = %err,
                                "failed to despawn car after client disconnect"
                            );
                        }
                        break;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        tracing::warn!(
                            car_id = req.car_id,
                            "car state stream backpressure; dropping stream"
                        );
                        let _ = engine.despawn_car(req.car_id).await;
                        break;
                    }
                }
            }

            tracing::info!(car_id = req.car_id, "car state stream ended");
            active_streams.remove(&req.car_id);
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
