//! gRPC RaceService implementation and transport mapping.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use boink::model::Controls;
use dashmap::DashMap;
use proto::race::v1::{
    FrontendSpectatorEvent, FrontendSpectatorSnapshot, GetFrontendSpectatorRequest,
    GetParticipantRaceRequest, ParticipantRaceEvent, QuickJoinRequest, QuickJoinResponse,
    SetControlsDevRequest, SetControlsRequest, SetControlsResponse, SpectatorView,
    StreamClampReason, StreamSettings, ViewDowngradeReason,
    frontend_spectator_event::Payload as FrontendSpectatorPayload,
    race_service_server::RaceService,
};
use tokio::sync::{Mutex, mpsc};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, server::NamedService};

use crate::auth::jwt::{TokenValidator, parse_bearer_token};
use crate::runtime::engine_worker::EngineClient;

use super::error_map::map_worker_err;
use super::mappers::{frontend_full_state, proto_to_controls};

const DEFAULT_STREAM_HZ: u32 = 20;
const MIN_STREAM_HZ: u32 = 1;
const MAX_STREAM_HZ: u32 = 120;

/// gRPC RaceService implementation backed by a single engine world.
#[derive(Clone)]
pub struct RaceServiceImpl {
    engine: EngineClient,
    simulation_hz: u32,
    active_streams: Arc<DashMap<u64, ()>>,
    known_cars: Arc<DashMap<u64, ()>>,
    last_client_seq: Arc<DashMap<u64, u64>>,
    instance_cars: Arc<DashMap<String, u64>>,
    token_validator: Arc<TokenValidator>,
    // Optional shared state for future extensions.
    _state: Arc<Mutex<()>>,
}

impl RaceServiceImpl {
    /// Build a Race service that talks to the engine worker.
    pub fn new(
        engine: EngineClient,
        simulation_hz: u32,
        jwks_url: &str,
        jwt_audience: Vec<String>,
        jwt_issuers: Vec<String>,
    ) -> Self {
        Self {
            engine,
            simulation_hz,
            active_streams: Arc::new(DashMap::new()),
            known_cars: Arc::new(DashMap::new()),
            last_client_seq: Arc::new(DashMap::new()),
            instance_cars: Arc::new(DashMap::new()),
            token_validator: Arc::new(TokenValidator::new_with_config(
                jwks_url,
                jwt_audience,
                jwt_issuers,
            )),
            _state: Arc::new(Mutex::new(())),
        }
    }
}

impl NamedService for RaceServiceImpl {
    const NAME: &'static str = "race.v1.RaceService";
}

#[tonic::async_trait]
impl RaceService for RaceServiceImpl {
    type StreamFrontendSpectatorStream = ReceiverStream<Result<FrontendSpectatorEvent, Status>>;
    type StreamParticipantRaceStream = ReceiverStream<Result<ParticipantRaceEvent, Status>>;

    async fn quick_join(
        &self,
        request: Request<QuickJoinRequest>,
    ) -> Result<Response<QuickJoinResponse>, Status> {
        let engine = self.engine.clone();
        let car_id = engine.spawn_car().await.map_err(map_worker_err)?;
        if let Some(token) = parse_bearer_token(request.metadata())? {
            if let Some(instance_uuid) = self
                .token_validator
                .instance_uuid_from_token(&token)
                .await?
            {
                self.instance_cars.insert(instance_uuid, car_id);
            }
        }

        let resp = QuickJoinResponse {
            car_id,
            map_id: "test".into(),
        };
        self.known_cars.insert(car_id, ());
        self.last_client_seq.insert(car_id, 0);

        Ok(Response::new(resp))
    }

    async fn set_controls(
        &self,
        request: Request<SetControlsRequest>,
    ) -> Result<Response<SetControlsResponse>, Status> {
        let auth = parse_bearer_token(request.metadata())?;
        let req = request.into_inner();
        let car_id = match auth {
            Some(token) => {
                let instance_uuid = self
                    .token_validator
                    .instance_uuid_from_token(&token)
                    .await?
                    .ok_or_else(|| Status::unauthenticated("missing instance_uuid claim"))?;
                let car_id = self
                    .instance_cars
                    .get(&instance_uuid)
                    .map(|entry| *entry.value())
                    .ok_or_else(|| Status::not_found("unknown instance_uuid"))?;
                car_id
            }
            None => self.resolve_single_car_id()?,
        };
        let controls = proto_to_controls(&req);

        self.engine
            .set_controls(car_id, controls)
            .await
            .map_err(map_worker_err)?;

        self.last_client_seq.insert(car_id, req.client_seq);
        let resp = SetControlsResponse {
            client_seq: req.client_seq,
            accepted_throttle: req.throttle,
            accepted_brake: req.brake,
            accepted_steering: req.steering,
            applies_from_tick: 0,
        };

        Ok(Response::new(resp))
    }

    async fn set_controls_dev(
        &self,
        request: Request<SetControlsDevRequest>,
    ) -> Result<Response<SetControlsResponse>, Status> {
        let req = request.into_inner();
        let controls = Controls {
            throttle: req.throttle,
            brake: req.brake,
            steer: req.steering,
        };

        self.engine
            .set_controls(req.target_car_id, controls)
            .await
            .map_err(map_worker_err)?;

        self.last_client_seq
            .insert(req.target_car_id, req.client_seq);
        let resp = SetControlsResponse {
            client_seq: req.client_seq,
            accepted_throttle: req.throttle,
            accepted_brake: req.brake,
            accepted_steering: req.steering,
            applies_from_tick: 0,
        };

        Ok(Response::new(resp))
    }

    async fn stream_frontend_spectator(
        &self,
        request: Request<GetFrontendSpectatorRequest>,
    ) -> Result<Response<Self::StreamFrontendSpectatorStream>, Status> {
        let auth = parse_bearer_token(request.metadata())?;
        let req = request.into_inner();

        let requested_view = normalize_requested_view(req.requested_view);
        let scopes = match auth {
            Some(token) => self.token_validator.scopes_from_token(&token).await?,
            None => Vec::new(),
        };
        let (resolved_view, view_downgrade_reason) = resolve_view(requested_view, &scopes);

        let engine = self.engine.clone();
        let simulation_hz = self.simulation_hz;
        let active_streams = self.active_streams.clone();
        let known_cars = self.known_cars.clone();
        let last_client_seq = self.last_client_seq.clone();
        let instance_cars = self.instance_cars.clone();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(run_frontend_spectator_stream(
            engine,
            simulation_hz,
            active_streams,
            known_cars,
            last_client_seq,
            instance_cars,
            req,
            requested_view,
            resolved_view,
            view_downgrade_reason,
            tx,
        ));

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn stream_participant_race(
        &self,
        _request: Request<GetParticipantRaceRequest>,
    ) -> Result<Response<Self::StreamParticipantRaceStream>, Status> {
        Err(Status::unimplemented(
            "participant race stream requires validated car identity",
        ))
    }
}

impl RaceServiceImpl {
    fn resolve_single_car_id(&self) -> Result<u64, Status> {
        let mut iter = self.known_cars.iter();
        let Some(first) = iter.next() else {
            return Err(Status::not_found("no car assigned to this client"));
        };
        if iter.next().is_some() {
            return Err(Status::failed_precondition(
                "multiple cars active; use dev controls",
            ));
        }
        Ok(*first.key())
    }
}

fn resolve_view(
    requested_view: SpectatorView,
    scopes: &[String],
) -> (SpectatorView, ViewDowngradeReason) {
    let allowed_view = if scopes.iter().any(|s| s == "race.read.all") {
        SpectatorView::All
    } else if scopes.iter().any(|s| s == "race.read.team") {
        SpectatorView::Team
    } else {
        SpectatorView::Public
    };
    if (requested_view as i32) <= (allowed_view as i32) {
        (requested_view, ViewDowngradeReason::None)
    } else {
        (allowed_view, ViewDowngradeReason::NotAuthorized)
    }
}

fn normalize_requested_view(raw: i32) -> SpectatorView {
    match SpectatorView::try_from(raw).unwrap_or(SpectatorView::Public) {
        SpectatorView::Unspecified => SpectatorView::Public,
        view => view,
    }
}

fn resolve_stream_rate(
    requested_hz: u32,
    simulation_hz: u32,
) -> (u32, u32, StreamClampReason, Duration) {
    let requested_hz = if requested_hz == 0 {
        DEFAULT_STREAM_HZ
    } else {
        requested_hz
    };
    let max_hz = MAX_STREAM_HZ.min(simulation_hz);
    let effective_hz = requested_hz.clamp(MIN_STREAM_HZ, max_hz);
    let clamp_reason = if effective_hz == requested_hz {
        StreamClampReason::None
    } else {
        StreamClampReason::ServerLimit
    };
    let period = Duration::from_secs_f64(1.0 / effective_hz as f64);
    (requested_hz, effective_hz, clamp_reason, period)
}

fn build_settings_event(
    requested_hz: u32,
    effective_hz: u32,
    clamp_reason: StreamClampReason,
    resolved_view: SpectatorView,
    view_downgrade_reason: ViewDowngradeReason,
) -> FrontendSpectatorEvent {
    let settings = StreamSettings {
        requested_hz,
        effective_hz,
        clamp_reason: clamp_reason as i32,
        resolved_view: resolved_view as i32,
        view_downgrade_reason: view_downgrade_reason as i32,
        map_id: "test".into(),
    };
    FrontendSpectatorEvent {
        payload: Some(FrontendSpectatorPayload::Settings(settings)),
    }
}

async fn run_frontend_spectator_stream(
    engine: EngineClient,
    simulation_hz: u32,
    active_streams: Arc<DashMap<u64, ()>>,
    known_cars: Arc<DashMap<u64, ()>>,
    last_client_seq: Arc<DashMap<u64, u64>>,
    instance_cars: Arc<DashMap<String, u64>>,
    req: GetFrontendSpectatorRequest,
    requested_view: SpectatorView,
    resolved_view: SpectatorView,
    view_downgrade_reason: ViewDowngradeReason,
    tx: mpsc::Sender<Result<FrontendSpectatorEvent, Status>>,
) {
    static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);
    let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    active_streams.insert(stream_id, ());

    let (requested_hz, effective_hz, clamp_reason, period) =
        resolve_stream_rate(req.requested_hz, simulation_hz);
    let mut ticker = tokio::time::interval(period);
    let mut tick: u64 = 0;

    tracing::info!(
        requested_hz,
        effective_hz,
        clamp_reason = ?clamp_reason,
        requested_view = ?requested_view,
        resolved_view = ?resolved_view,
        view_downgrade_reason = ?view_downgrade_reason,
        "frontend spectator stream started"
    );

    let settings_msg = build_settings_event(
        requested_hz,
        effective_hz,
        clamp_reason,
        resolved_view,
        view_downgrade_reason,
    );
    if tx.try_send(Ok(settings_msg)).is_err() {
        active_streams.remove(&stream_id);
        return;
    }

    loop {
        ticker.tick().await;
        tick = tick.wrapping_add(1);

        let car_ids: Vec<u64> = known_cars.iter().map(|entry| *entry.key()).collect();
        let mut cars = Vec::with_capacity(car_ids.len());

        for car_id in car_ids {
            let seq = last_client_seq.get(&car_id).map(|v| *v).unwrap_or(0);
            match engine.read_car_state(car_id).await {
                Ok(state) => cars.push(frontend_full_state(car_id, state, seq)),
                Err(err) => {
                    tracing::warn!(
                        car_id,
                        error = %err,
                        "failed to read car state for spectator snapshot"
                    );
                    known_cars.remove(&car_id);
                    last_client_seq.remove(&car_id);
                }
            }
        }

        let snapshot = FrontendSpectatorSnapshot {
            tick,
            server_time_ms: current_time_ms(),
            cars,
        };
        let msg = FrontendSpectatorEvent {
            payload: Some(FrontendSpectatorPayload::Snapshot(snapshot)),
        };
        match tx.try_send(Ok(msg)) {
            Ok(_) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!("frontend spectator stream stopped (client disconnected)");
                cleanup_frontend_cars(
                    "disconnect",
                    &engine,
                    &known_cars,
                    &last_client_seq,
                    &instance_cars,
                )
                .await;
                break;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("frontend spectator stream backpressure; dropping stream");
                cleanup_frontend_cars(
                    "backpressure",
                    &engine,
                    &known_cars,
                    &last_client_seq,
                    &instance_cars,
                )
                .await;
                break;
            }
        }
    }

    tracing::info!("frontend spectator stream ended");
    active_streams.remove(&stream_id);
}

fn current_time_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn cleanup_frontend_cars(
    reason: &'static str,
    engine: &EngineClient,
    known_cars: &DashMap<u64, ()>,
    last_client_seq: &DashMap<u64, u64>,
    instance_cars: &DashMap<String, u64>,
) {
    let car_ids: Vec<u64> = known_cars.iter().map(|entry| *entry.key()).collect();
    tracing::info!(
        reason,
        car_count = car_ids.len(),
        "frontend cleanup: despawning cars"
    );
    for car_id in car_ids {
        if let Err(err) = engine.despawn_car(car_id).await {
            tracing::warn!(
                car_id,
                error = %err,
                "failed to despawn car during frontend cleanup"
            );
        }
        known_cars.remove(&car_id);
        last_client_seq.remove(&car_id);
        let instance_keys: Vec<String> = instance_cars
            .iter()
            .filter(|entry| *entry.value() == car_id)
            .map(|entry| entry.key().clone())
            .collect();
        for instance_uuid in instance_keys {
            instance_cars.remove(&instance_uuid);
        }
        tracing::info!(car_id, reason, "frontend cleanup: car removed");
    }
}
