//! gRPC RaceService implementation and transport mapping.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use proto::race::v1::{
    FrontendSpectatorDebugInfo, FrontendSpectatorEvent, FrontendSpectatorSnapshot,
    GetFrontendSpectatorRequest, GetParticipantRaceRequest, ParticipantRaceEvent,
    ParticipantRaceSnapshot, QuickJoinDevRequest, QuickJoinDevResponse, SetControlsDevRequest,
    SetControlsRequest, SetControlsResponse, SpectatorView, StreamClampReason, StreamSettings,
    ViewDowngradeReason, frontend_spectator_event::Payload as FrontendSpectatorPayload,
    get_frontend_spectator_request::Target as FrontendSpectatorTarget,
    participant_race_event::Payload as ParticipantRacePayload, race_service_server::RaceService,
};
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, server::NamedService};

use crate::auth::game_token::{GameTokenValidator, parse_game_token};
use crate::config::AppEnv;
use crate::runtime::engine_worker::{
    EngineActiveSandboxState, EngineActivityKind, EngineClient, EngineCommandTarget,
    EngineRuntimeState,
};

pub mod frame_hub;
pub mod runtime_store;
pub use frame_hub::{FrameHub, RuntimeFrame, spawn_frame_hub};
pub use runtime_store::{RaceRuntimeStore, RuntimeCarIdentity};

use super::error_map::map_worker_err;
use super::mappers::{
    engine_gear_shift_to_proto, frontend_full_state, participant_opponent_state,
    participant_self_state, proto_dev_to_controls, proto_to_controls,
};

const DEFAULT_STREAM_HZ: u32 = 20;
const MIN_STREAM_HZ: u32 = 1;
const MAX_STREAM_HZ: u32 = 120;
const FRONTEND_STREAM_CHANNEL_CAPACITY: usize = 4;
const PARTICIPANT_STREAM_CHANNEL_CAPACITY: usize = 1;

/// gRPC RaceService implementation backed by a single engine world.
#[derive(Clone)]
pub struct RaceServiceImpl {
    engine: EngineClient,
    simulation_hz: u32,
    app_env: AppEnv,
    runtime_store: Arc<RaceRuntimeStore>,
    frame_hub: FrameHub,
    active_streams: Arc<DashMap<u64, ()>>,
    known_cars: Arc<DashMap<u64, ()>>,
    last_client_seq: Arc<DashMap<u64, u64>>,
    instance_cars: Arc<DashMap<String, u64>>,
    car_owners: Arc<DashMap<u64, String>>,
    car_engine_ids: Arc<DashMap<u64, u64>>,
    car_targets: Arc<DashMap<u64, EngineCommandTarget>>,
    token_validator: Arc<GameTokenValidator>,
}

impl RaceServiceImpl {
    /// Build a Race service that talks to the engine worker.
    pub fn new(
        engine: EngineClient,
        simulation_hz: u32,
        app_env: AppEnv,
        hps_endpoint: &str,
        jwt_audience: Vec<String>,
        jwt_issuers: Vec<String>,
        runtime_store: Arc<RaceRuntimeStore>,
        frame_hub: FrameHub,
    ) -> Self {
        Self {
            engine,
            simulation_hz,
            app_env,
            runtime_store: runtime_store.clone(),
            frame_hub,
            active_streams: Arc::new(DashMap::new()),
            known_cars: runtime_store.known_cars(),
            last_client_seq: runtime_store.last_client_seq(),
            instance_cars: runtime_store.instance_cars(),
            car_owners: runtime_store.car_owners(),
            car_engine_ids: runtime_store.car_engine_ids(),
            car_targets: runtime_store.car_targets(),
            token_validator: Arc::new(GameTokenValidator::new_with_config(
                hps_endpoint,
                jwt_audience,
                jwt_issuers,
            )),
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

    async fn quick_join_dev(
        &self,
        request: Request<QuickJoinDevRequest>,
    ) -> Result<Response<QuickJoinDevResponse>, Status> {
        if self.app_env.is_production() {
            return Err(Status::failed_precondition(
                "quick join is available only in development/preprod",
            ));
        }

        let auth = parse_game_token(request.metadata())?;
        let req = request.into_inner();
        let engine = self.engine.clone();
        let runtime_state = engine.runtime_state().await.map_err(map_worker_err)?;
        let active_sandbox = select_quick_join_sandbox(&runtime_state, &req.sandbox_id)?;

        let sandbox_id = active_sandbox.sandbox_id.clone();
        let map_id = active_sandbox.map_id.clone();
        let engine_car_id = engine
            .spawn_sandbox_car(sandbox_id.clone())
            .await
            .map_err(map_worker_err)?;
        let public_car_id = self.runtime_store.allocate_public_car_id();
        let mut identity = RuntimeCarIdentity::default();
        if let Some(token) = auth.as_ref() {
            identity.subject = Some(self.token_validator.subject_from_token(token).await?);
            identity.team_id = self.token_validator.team_id_from_token(token).await?;
            identity.instance_uuid = self.token_validator.instance_uuid_from_token(token).await?;
            if let Some(instance_uuid) = identity.instance_uuid.clone() {
                self.instance_cars
                    .insert(instance_uuid.clone(), public_car_id);
                self.car_owners.insert(public_car_id, instance_uuid);
            }
        }
        let local_user_id = identity
            .subject
            .clone()
            .unwrap_or_else(|| format!("car-{public_car_id}"));
        identity.local_bot_index = Some(
            self.runtime_store
                .allocate_local_bot_index(&sandbox_id, &local_user_id),
        );
        self.runtime_store.set_car_identity(public_car_id, identity);

        let resp = QuickJoinDevResponse {
            car_id: public_car_id,
            map_id,
        };
        self.known_cars.insert(public_car_id, ());
        self.last_client_seq.insert(public_car_id, 0);
        self.car_engine_ids.insert(public_car_id, engine_car_id);
        self.car_targets
            .insert(public_car_id, EngineCommandTarget::Sandbox { sandbox_id });

        Ok(Response::new(resp))
    }

    async fn set_controls(
        &self,
        request: Request<SetControlsRequest>,
    ) -> Result<Response<SetControlsResponse>, Status> {
        let auth = parse_game_token(request.metadata())?;
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
        let controls = proto_to_controls(&req)?;
        let target = self.target_for_car(car_id)?;
        let engine_car_id = self.engine_car_id_for(car_id)?;

        let accepted_controls = self
            .engine
            .set_controls_in(target, engine_car_id, controls)
            .await
            .map_err(map_worker_err)?;

        self.last_client_seq.insert(car_id, req.client_seq);
        let resp = SetControlsResponse {
            client_seq: req.client_seq,
            accepted_throttle: req.throttle,
            accepted_brake: req.brake,
            accepted_steering: req.steering,
            applies_from_tick: 0,
            accepted_shift: engine_gear_shift_to_proto(accepted_controls.accepted_shift),
        };

        Ok(Response::new(resp))
    }

    async fn set_controls_dev(
        &self,
        request: Request<SetControlsDevRequest>,
    ) -> Result<Response<SetControlsResponse>, Status> {
        let req = request.into_inner();
        let controls = proto_dev_to_controls(&req)?;
        let target = self.target_for_car(req.target_car_id)?;
        let engine_car_id = self.engine_car_id_for(req.target_car_id)?;

        let accepted_controls = self
            .engine
            .set_controls_in(target, engine_car_id, controls)
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
            accepted_shift: engine_gear_shift_to_proto(accepted_controls.accepted_shift),
        };

        Ok(Response::new(resp))
    }

    async fn stream_frontend_spectator(
        &self,
        request: Request<GetFrontendSpectatorRequest>,
    ) -> Result<Response<Self::StreamFrontendSpectatorStream>, Status> {
        let auth = parse_game_token(request.metadata())?;
        let req = request.into_inner();

        let requested_view = normalize_requested_view(req.requested_view);
        let (scopes, cleanup_instance_uuid) = match auth {
            Some(token) => {
                let scopes = self.token_validator.scopes_from_token(&token).await?;
                let cleanup_instance_uuid = self
                    .token_validator
                    .instance_uuid_from_token(&token)
                    .await?;
                (scopes, cleanup_instance_uuid)
            }
            None => (Vec::new(), None),
        };
        let (resolved_view, view_downgrade_reason) = resolve_view(requested_view, &scopes);

        let engine = self.engine.clone();
        let runtime_state = engine.runtime_state().await.map_err(map_worker_err)?;
        let visible_target = self.resolve_stream_target(&req, &runtime_state)?;
        let simulation_hz = self.simulation_hz;
        let frame_hub = self.frame_hub.clone();
        let active_streams = self.active_streams.clone();
        let known_cars = self.known_cars.clone();
        let last_client_seq = self.last_client_seq.clone();
        let instance_cars = self.instance_cars.clone();
        let car_owners = self.car_owners.clone();
        let car_engine_ids = self.car_engine_ids.clone();
        let car_targets = self.car_targets.clone();
        let (tx, rx) = mpsc::channel(FRONTEND_STREAM_CHANNEL_CAPACITY);

        tokio::spawn(run_frontend_spectator_stream(
            engine,
            frame_hub,
            simulation_hz,
            active_streams,
            known_cars,
            last_client_seq,
            instance_cars,
            car_owners,
            car_engine_ids,
            car_targets,
            req,
            requested_view,
            resolved_view,
            view_downgrade_reason,
            resolve_runtime_map_id(&runtime_state, visible_target.as_ref()),
            visible_target,
            cleanup_instance_uuid,
            tx,
        ));

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn stream_participant_race(
        &self,
        request: Request<GetParticipantRaceRequest>,
    ) -> Result<Response<Self::StreamParticipantRaceStream>, Status> {
        let token = parse_game_token(request.metadata())?
            .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
        let req = request.into_inner();
        let scopes = self.token_validator.scopes_from_token(&token).await?;
        let instance_uuid = self
            .token_validator
            .instance_uuid_from_token(&token)
            .await?
            .ok_or_else(|| Status::unauthenticated("missing instance_uuid claim"))?;
        let self_public_car_id = self
            .instance_cars
            .get(&instance_uuid)
            .map(|entry| *entry.value())
            .ok_or_else(|| Status::not_found("unknown instance_uuid"))?;
        let self_target = self.target_for_car(self_public_car_id)?;
        let self_engine_car_id = self.engine_car_id_for(self_public_car_id)?;

        let requested_view = SpectatorView::Team;
        let (resolved_view, view_downgrade_reason) = resolve_view(requested_view, &scopes);

        let engine = self.engine.clone();
        let runtime_state = engine.runtime_state().await.map_err(map_worker_err)?;
        let runtime_map_id = resolve_runtime_map_id(&runtime_state, Some(&self_target));
        let simulation_hz = self.simulation_hz;
        let frame_hub = self.frame_hub.clone();
        let active_streams = self.active_streams.clone();
        let known_cars = self.known_cars.clone();
        let last_client_seq = self.last_client_seq.clone();
        let instance_cars = self.instance_cars.clone();
        let car_owners = self.car_owners.clone();
        let car_engine_ids = self.car_engine_ids.clone();
        let car_targets = self.car_targets.clone();
        let (tx, rx) = mpsc::channel(PARTICIPANT_STREAM_CHANNEL_CAPACITY);

        tokio::spawn(run_participant_race_stream(
            engine,
            frame_hub,
            simulation_hz,
            active_streams,
            known_cars,
            last_client_seq,
            instance_cars,
            car_owners,
            car_engine_ids,
            car_targets,
            req,
            requested_view,
            resolved_view,
            view_downgrade_reason,
            runtime_map_id,
            self_public_car_id,
            self_engine_car_id,
            self_target,
            tx,
        ));

        Ok(Response::new(ReceiverStream::new(rx)))
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

    fn target_for_car(&self, car_id: u64) -> Result<EngineCommandTarget, Status> {
        self.car_targets
            .get(&car_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| Status::not_found("unknown car target"))
    }

    fn engine_car_id_for(&self, car_id: u64) -> Result<u64, Status> {
        self.car_engine_ids
            .get(&car_id)
            .map(|entry| *entry.value())
            .ok_or_else(|| Status::not_found("unknown car target"))
    }

    fn resolve_stream_target(
        &self,
        req: &GetFrontendSpectatorRequest,
        runtime_state: &EngineRuntimeState,
    ) -> Result<Option<EngineCommandTarget>, Status> {
        if let Some(target) = req.target.as_ref() {
            return match target {
                FrontendSpectatorTarget::Sandbox(value) => {
                    if value.sandbox_id.trim().is_empty() {
                        return Err(Status::invalid_argument(
                            "sandbox_id is required for sandbox spectator target",
                        ));
                    }
                    let active = select_quick_join_sandbox(runtime_state, &value.sandbox_id)?;
                    Ok(Some(EngineCommandTarget::Sandbox {
                        sandbox_id: active.sandbox_id.clone(),
                    }))
                }
                FrontendSpectatorTarget::OfficialRace(_) => {
                    if !matches!(
                        runtime_state.activity_kind,
                        EngineActivityKind::OfficialRace
                    ) {
                        return Err(Status::failed_precondition(
                            "official race runtime is not active",
                        ));
                    }
                    Ok(Some(EngineCommandTarget::OfficialRace))
                }
            };
        }

        Err(Status::invalid_argument("spectator target must be set"))
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

fn resolve_runtime_map_id(
    runtime_state: &EngineRuntimeState,
    visible_target: Option<&EngineCommandTarget>,
) -> String {
    if let Some(EngineCommandTarget::Sandbox { sandbox_id }) = visible_target {
        if let Some(active) = runtime_state
            .active_sandboxes
            .iter()
            .find(|entry| entry.sandbox_id == *sandbox_id)
        {
            return active.map_id.clone();
        }
    }

    if matches!(runtime_state.activity_kind, EngineActivityKind::Sandbox)
        && runtime_state.active_sandboxes.len() == 1
    {
        if let Some(active) = runtime_state.active_sandboxes.first() {
            return active.map_id.clone();
        }
    }
    runtime_state.map_id.clone()
}

fn select_quick_join_sandbox<'a>(
    runtime_state: &'a EngineRuntimeState,
    requested_sandbox_id: &str,
) -> Result<&'a EngineActiveSandboxState, Status> {
    if runtime_state.active_sandboxes.is_empty() {
        return Err(Status::failed_precondition("sandbox runtime is not active"));
    }

    let requested_sandbox_id = requested_sandbox_id.trim();
    if !requested_sandbox_id.is_empty() {
        return runtime_state
            .active_sandboxes
            .iter()
            .find(|entry| entry.sandbox_id == requested_sandbox_id)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "sandbox runtime is not active for sandbox_id={requested_sandbox_id}"
                ))
            });
    }

    if runtime_state.active_sandboxes.len() == 1 {
        return Ok(&runtime_state.active_sandboxes[0]);
    }

    Err(Status::failed_precondition(
        "sandbox_id is required when multiple sandbox sessions are active",
    ))
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

fn build_stream_settings(
    requested_hz: u32,
    effective_hz: u32,
    clamp_reason: StreamClampReason,
    resolved_view: SpectatorView,
    view_downgrade_reason: ViewDowngradeReason,
    map_id: &str,
) -> StreamSettings {
    StreamSettings {
        requested_hz,
        effective_hz,
        clamp_reason: clamp_reason as i32,
        resolved_view: resolved_view as i32,
        view_downgrade_reason: view_downgrade_reason as i32,
        map_id: map_id.to_string(),
    }
}

fn build_frontend_settings_event(
    requested_hz: u32,
    effective_hz: u32,
    clamp_reason: StreamClampReason,
    resolved_view: SpectatorView,
    view_downgrade_reason: ViewDowngradeReason,
    map_id: &str,
) -> FrontendSpectatorEvent {
    let settings = build_stream_settings(
        requested_hz,
        effective_hz,
        clamp_reason,
        resolved_view,
        view_downgrade_reason,
        map_id,
    );
    FrontendSpectatorEvent {
        payload: Some(FrontendSpectatorPayload::Settings(settings)),
    }
}

fn build_participant_settings_event(
    requested_hz: u32,
    effective_hz: u32,
    clamp_reason: StreamClampReason,
    resolved_view: SpectatorView,
    view_downgrade_reason: ViewDowngradeReason,
    map_id: &str,
) -> ParticipantRaceEvent {
    let settings = build_stream_settings(
        requested_hz,
        effective_hz,
        clamp_reason,
        resolved_view,
        view_downgrade_reason,
        map_id,
    );
    ParticipantRaceEvent {
        payload: Some(ParticipantRacePayload::Settings(settings)),
    }
}

async fn run_frontend_spectator_stream(
    engine: EngineClient,
    frame_hub: FrameHub,
    simulation_hz: u32,
    active_streams: Arc<DashMap<u64, ()>>,
    known_cars: Arc<DashMap<u64, ()>>,
    last_client_seq: Arc<DashMap<u64, u64>>,
    instance_cars: Arc<DashMap<String, u64>>,
    car_owners: Arc<DashMap<u64, String>>,
    car_engine_ids: Arc<DashMap<u64, u64>>,
    car_targets: Arc<DashMap<u64, EngineCommandTarget>>,
    req: GetFrontendSpectatorRequest,
    requested_view: SpectatorView,
    resolved_view: SpectatorView,
    view_downgrade_reason: ViewDowngradeReason,
    runtime_map_id: String,
    visible_target: Option<EngineCommandTarget>,
    cleanup_instance_uuid: Option<String>,
    tx: mpsc::Sender<Result<FrontendSpectatorEvent, Status>>,
) {
    static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);
    let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    active_streams.insert(stream_id, ());

    let (requested_hz, effective_hz, clamp_reason, period) =
        resolve_stream_rate(req.requested_hz, simulation_hz);
    let mut ticker = tokio::time::interval(period);
    let frame_rx = frame_hub.subscribe();

    tracing::info!(
        requested_hz,
        effective_hz,
        clamp_reason = ?clamp_reason,
        requested_view = ?requested_view,
        resolved_view = ?resolved_view,
        view_downgrade_reason = ?view_downgrade_reason,
        "frontend spectator stream started"
    );

    let settings_msg = build_frontend_settings_event(
        requested_hz,
        effective_hz,
        clamp_reason,
        resolved_view,
        view_downgrade_reason,
        &runtime_map_id,
    );
    if tx.send(Ok(settings_msg)).await.is_err() {
        active_streams.remove(&stream_id);
        return;
    }

    loop {
        ticker.tick().await;
        let frame = frame_rx.borrow().clone();
        let mut frame_cars: Vec<_> = frame
            .cars
            .values()
            .filter(|entry| match visible_target.as_ref() {
                Some(target) => &entry.target == target,
                None => true,
            })
            .cloned()
            .collect();
        frame_cars.sort_by_key(|entry| entry.public_car_id);

        let mut cars = Vec::with_capacity(frame_cars.len());
        for entry in frame_cars {
            cars.push(frontend_full_state(
                entry.public_car_id,
                entry.state,
                entry.last_client_seq,
            ));
        }

        let debug = if req.include_debug {
            match visible_target.as_ref() {
                Some(EngineCommandTarget::OfficialRace) => {
                    frame
                        .official_race_duration_s
                        .map(|duration_s| FrontendSpectatorDebugInfo {
                            engine_race_elapsed_sec: duration_s,
                        })
                }
                Some(EngineCommandTarget::Sandbox { sandbox_id }) => frame
                    .sandbox_race_duration_s
                    .get(sandbox_id)
                    .copied()
                    .map(|duration_s| FrontendSpectatorDebugInfo {
                        engine_race_elapsed_sec: duration_s,
                    }),
                None => None,
            }
        } else {
            None
        };

        let snapshot = FrontendSpectatorSnapshot {
            tick: frame.tick,
            server_time_ms: frame.server_time_ms,
            cars,
            debug,
        };
        let msg = FrontendSpectatorEvent {
            payload: Some(FrontendSpectatorPayload::Snapshot(snapshot)),
        };
        if tx.send(Ok(msg)).await.is_err() {
            tracing::debug!("frontend spectator stream stopped (client disconnected)");
            cleanup_frontend_cars(
                "disconnect",
                &engine,
                &known_cars,
                &last_client_seq,
                &instance_cars,
                &car_owners,
                &car_engine_ids,
                &car_targets,
                visible_target.as_ref(),
                cleanup_instance_uuid.as_deref(),
            )
            .await;
            break;
        }
    }

    tracing::info!("frontend spectator stream ended");
    active_streams.remove(&stream_id);
}

fn emit_participant_terminal_error(
    tx: &mpsc::Sender<Result<ParticipantRaceEvent, Status>>,
    status: Status,
) {
    if tx.try_send(Err(status)).is_err() {
        tracing::debug!("participant stream terminal status not delivered");
    }
}

async fn run_participant_race_stream(
    engine: EngineClient,
    frame_hub: FrameHub,
    simulation_hz: u32,
    active_streams: Arc<DashMap<u64, ()>>,
    known_cars: Arc<DashMap<u64, ()>>,
    last_client_seq: Arc<DashMap<u64, u64>>,
    instance_cars: Arc<DashMap<String, u64>>,
    car_owners: Arc<DashMap<u64, String>>,
    car_engine_ids: Arc<DashMap<u64, u64>>,
    car_targets: Arc<DashMap<u64, EngineCommandTarget>>,
    req: GetParticipantRaceRequest,
    requested_view: SpectatorView,
    resolved_view: SpectatorView,
    view_downgrade_reason: ViewDowngradeReason,
    runtime_map_id: String,
    self_public_car_id: u64,
    self_engine_car_id: u64,
    self_target: EngineCommandTarget,
    tx: mpsc::Sender<Result<ParticipantRaceEvent, Status>>,
) {
    static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(100_000);
    let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    active_streams.insert(stream_id, ());

    let (requested_hz, effective_hz, clamp_reason, period) =
        resolve_stream_rate(req.requested_hz, simulation_hz);
    let mut ticker = tokio::time::interval(period);
    let frame_rx = frame_hub.subscribe();

    tracing::info!(
        stream_id,
        self_public_car_id,
        self_engine_car_id,
        requested_hz,
        effective_hz,
        clamp_reason = ?clamp_reason,
        requested_view = ?requested_view,
        resolved_view = ?resolved_view,
        view_downgrade_reason = ?view_downgrade_reason,
        target = ?self_target,
        "participant race stream started"
    );

    let settings_msg = build_participant_settings_event(
        requested_hz,
        effective_hz,
        clamp_reason,
        resolved_view,
        view_downgrade_reason,
        &runtime_map_id,
    );
    if tx.send(Ok(settings_msg)).await.is_err() {
        cleanup_participant_car(
            "initial-send-failed",
            &engine,
            self_public_car_id,
            &self_target,
            &known_cars,
            &last_client_seq,
            &instance_cars,
            &car_owners,
            &car_engine_ids,
            &car_targets,
        )
        .await;
        active_streams.remove(&stream_id);
        return;
    }

    loop {
        ticker.tick().await;
        let frame = frame_rx.borrow().clone();

        let Some(self_car) = frame.cars.get(&self_public_car_id).cloned() else {
            emit_participant_terminal_error(
                &tx,
                Status::not_found("participant car is no longer active"),
            );
            cleanup_participant_car(
                "self-missing",
                &engine,
                self_public_car_id,
                &self_target,
                &known_cars,
                &last_client_seq,
                &instance_cars,
                &car_owners,
                &car_engine_ids,
                &car_targets,
            )
            .await;
            break;
        };
        if self_car.target != self_target {
            emit_participant_terminal_error(
                &tx,
                Status::failed_precondition("participant car target changed"),
            );
            cleanup_participant_car(
                "self-target-changed",
                &engine,
                self_public_car_id,
                &self_target,
                &known_cars,
                &last_client_seq,
                &instance_cars,
                &car_owners,
                &car_engine_ids,
                &car_targets,
            )
            .await;
            break;
        }

        if self_car.engine_car_id != self_engine_car_id {
            emit_participant_terminal_error(
                &tx,
                Status::failed_precondition("participant car engine mapping changed"),
            );
            cleanup_participant_car(
                "self-engine-id-changed",
                &engine,
                self_public_car_id,
                &self_target,
                &known_cars,
                &last_client_seq,
                &instance_cars,
                &car_owners,
                &car_engine_ids,
                &car_targets,
            )
            .await;
            break;
        }

        let mut opponents: Vec<_> = frame
            .cars
            .values()
            .filter(|entry| {
                entry.public_car_id != self_public_car_id && entry.target == self_target
            })
            .cloned()
            .collect();
        opponents.sort_by_key(|entry| entry.public_car_id);

        let opponents = opponents
            .into_iter()
            .map(|entry| participant_opponent_state(entry.public_car_id, entry.state))
            .collect();

        let snapshot = ParticipantRaceSnapshot {
            tick: frame.tick,
            server_time_ms: frame.server_time_ms,
            self_: Some(participant_self_state(
                self_public_car_id,
                self_car.state,
                self_car.last_client_seq,
            )),
            opponents,
        };
        let msg = ParticipantRaceEvent {
            payload: Some(ParticipantRacePayload::Snapshot(snapshot)),
        };
        if tx.send(Ok(msg)).await.is_err() {
            tracing::debug!("participant race stream stopped (client disconnected)");
            cleanup_participant_car(
                "disconnect",
                &engine,
                self_public_car_id,
                &self_target,
                &known_cars,
                &last_client_seq,
                &instance_cars,
                &car_owners,
                &car_engine_ids,
                &car_targets,
            )
            .await;
            break;
        }
    }

    tracing::info!(
        stream_id,
        self_public_car_id,
        target = ?self_target,
        "participant race stream ended"
    );
    active_streams.remove(&stream_id);
}

fn remove_instance_mapping_for_car(
    public_car_id: u64,
    instance_cars: &DashMap<String, u64>,
    car_owners: &DashMap<u64, String>,
) {
    let Some((_, owner_instance_uuid)) = car_owners.remove(&public_car_id) else {
        return;
    };

    let should_remove_instance = instance_cars
        .get(&owner_instance_uuid)
        .map(|entry| *entry.value() == public_car_id)
        .unwrap_or(false);
    if should_remove_instance {
        instance_cars.remove(&owner_instance_uuid);
    }
}

fn remove_car_state(
    public_car_id: u64,
    known_cars: &DashMap<u64, ()>,
    last_client_seq: &DashMap<u64, u64>,
    instance_cars: &DashMap<String, u64>,
    car_owners: &DashMap<u64, String>,
    car_engine_ids: &DashMap<u64, u64>,
    car_targets: &DashMap<u64, EngineCommandTarget>,
) {
    known_cars.remove(&public_car_id);
    last_client_seq.remove(&public_car_id);
    car_engine_ids.remove(&public_car_id);
    car_targets.remove(&public_car_id);
    remove_instance_mapping_for_car(public_car_id, instance_cars, car_owners);
}

async fn cleanup_frontend_cars(
    reason: &'static str,
    engine: &EngineClient,
    known_cars: &DashMap<u64, ()>,
    last_client_seq: &DashMap<u64, u64>,
    instance_cars: &DashMap<String, u64>,
    car_owners: &DashMap<u64, String>,
    car_engine_ids: &DashMap<u64, u64>,
    car_targets: &DashMap<u64, EngineCommandTarget>,
    cleanup_target: Option<&EngineCommandTarget>,
    owner_instance_uuid: Option<&str>,
) {
    let public_car_id = resolve_owned_cleanup_car(
        owner_instance_uuid,
        cleanup_target,
        instance_cars,
        car_owners,
        car_targets,
    );
    tracing::info!(
        reason,
        car_count = usize::from(public_car_id.is_some()),
        owner_instance_uuid = ?owner_instance_uuid,
        cleanup_target = ?cleanup_target,
        "frontend cleanup: despawning owned cars"
    );
    let Some(public_car_id) = public_car_id else {
        return;
    };

    let Some(target) = car_targets
        .get(&public_car_id)
        .map(|entry| entry.value().clone())
    else {
        remove_car_state(
            public_car_id,
            known_cars,
            last_client_seq,
            instance_cars,
            car_owners,
            car_engine_ids,
            car_targets,
        );
        return;
    };
    let Some(engine_car_id) = car_engine_ids
        .get(&public_car_id)
        .map(|entry| *entry.value())
    else {
        remove_car_state(
            public_car_id,
            known_cars,
            last_client_seq,
            instance_cars,
            car_owners,
            car_engine_ids,
            car_targets,
        );
        return;
    };

    if let Err(err) = engine.despawn_car_in(target, engine_car_id).await {
        tracing::warn!(
            public_car_id,
            engine_car_id,
            error = %err,
            "failed to despawn car during frontend cleanup"
        );
    }
    remove_car_state(
        public_car_id,
        known_cars,
        last_client_seq,
        instance_cars,
        car_owners,
        car_engine_ids,
        car_targets,
    );
    tracing::info!(
        public_car_id,
        engine_car_id,
        reason,
        "frontend cleanup: car removed"
    );
}

async fn cleanup_participant_car(
    reason: &'static str,
    engine: &EngineClient,
    public_car_id: u64,
    target: &EngineCommandTarget,
    known_cars: &DashMap<u64, ()>,
    last_client_seq: &DashMap<u64, u64>,
    instance_cars: &DashMap<String, u64>,
    car_owners: &DashMap<u64, String>,
    car_engine_ids: &DashMap<u64, u64>,
    car_targets: &DashMap<u64, EngineCommandTarget>,
) {
    match target {
        EngineCommandTarget::Sandbox { .. } => {
            let engine_car_id = car_engine_ids
                .get(&public_car_id)
                .map(|entry| *entry.value());
            if let Some(engine_car_id) = engine_car_id {
                if let Err(err) = engine.despawn_car_in(target.clone(), engine_car_id).await {
                    tracing::warn!(
                        public_car_id,
                        engine_car_id,
                        target = ?target,
                        error = %err,
                        "failed to despawn participant car during cleanup"
                    );
                }
            }

            remove_car_state(
                public_car_id,
                known_cars,
                last_client_seq,
                instance_cars,
                car_owners,
                car_engine_ids,
                car_targets,
            );
            tracing::info!(
                public_car_id,
                engine_car_id = ?engine_car_id,
                target = ?target,
                reason,
                "participant cleanup: sandbox car removed"
            );
        }
        EngineCommandTarget::OfficialRace => {
            tracing::info!(
                public_car_id,
                target = ?target,
                reason,
                "participant cleanup: preserving official race car"
            );
        }
    }
}

fn resolve_owned_cleanup_car(
    owner_instance_uuid: Option<&str>,
    cleanup_target: Option<&EngineCommandTarget>,
    instance_cars: &DashMap<String, u64>,
    car_owners: &DashMap<u64, String>,
    car_targets: &DashMap<u64, EngineCommandTarget>,
) -> Option<u64> {
    let owner_instance_uuid = owner_instance_uuid?;

    if let Some(target) = cleanup_target {
        if let Some(public_car_id) = car_owners.iter().find_map(|entry| {
            if entry.value().as_str() != owner_instance_uuid {
                return None;
            }
            car_targets
                .get(entry.key())
                .and_then(|car_target| (car_target.value() == target).then_some(*entry.key()))
        }) {
            return Some(public_car_id);
        }
    }

    instance_cars
        .get(owner_instance_uuid)
        .map(|entry| *entry.value())
}
