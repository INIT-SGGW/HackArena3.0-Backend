//! gRPC RaceService implementation and transport mapping.

#[cfg(feature = "official")]
use std::collections::VecDeque;
#[cfg(feature = "official")]
use std::io::SeekFrom;
#[cfg(feature = "official")]
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use proto::race::v1::{
    BackToTrackRequest, BackToTrackResponse, EmergencyPitstopRequest, EmergencyPitstopResponse,
    FrontendSpectatorDebugInfo, FrontendSpectatorEvent, FrontendSpectatorSnapshot,
    GetFrontendSpectatorRequest, GetOfficialTeamBotLogsRequest, GetOfficialTeamBotLogsResponse,
    OfficialTeamBotLogLine, OfficialTeamBotLogsSnapshot, QuickJoinDevRequest, QuickJoinDevResponse,
    RequestPitstopRequest, RequestPitstopResponse, SetControlsDevRequest, SetControlsResponse,
    SetNextPitTireTypeRequest, SetNextPitTireTypeResponse, SpectatorView, StreamClampReason,
    StreamOfficialTeamBotLogsRequest, StreamOfficialTeamBotLogsResponse, StreamSettings,
    ViewDowngradeReason, frontend_spectator_event::Payload as FrontendSpectatorPayload,
    get_frontend_spectator_request::Target as FrontendSpectatorTarget,
    race_service_server::RaceService,
    stream_official_team_bot_logs_response::Payload as OfficialTeamBotLogsPayload,
};
#[cfg(feature = "official")]
use proto::race::v1::{
    ParticipantCommandRejectReason, ParticipantCommandStatus, TireType as ProtoTireType,
};
#[cfg(feature = "local")]
use rand::Rng;
use tokio::sync::mpsc;
use tokio::time::Duration;
#[cfg(feature = "official")]
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader},
    time::MissedTickBehavior,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, server::NamedService};

use crate::auth::game_token::{GameTokenValidator, parse_game_token};
use crate::config::AppEnv;
#[cfg(feature = "local")]
use crate::local::sandbox_config_store::{LocalSandboxConfigStore, LocalSandboxSpawnModeRecord};
use crate::runtime::engine_worker::{
    EngineActiveSandboxState, EngineActivityKind, EngineClient, EngineCommandTarget,
    EngineRuntimeState,
};
#[cfg(feature = "official")]
use crate::services::submission::OfficialSandboxJoinRegistry;

pub mod frame_hub;
pub mod runtime_store;
pub use frame_hub::{FrameHub, RuntimeFrame, spawn_frame_hub};
pub use runtime_store::{
    RaceRuntimeStore, RuntimeCarIdentity, RuntimePitEntrySource, RuntimePitHistoryEntry,
    RuntimePitStateSnapshot, RuntimePitTireType,
};

use super::error_map::map_worker_err;
use super::mappers::{engine_gear_shift_to_proto, frontend_full_state, proto_dev_to_controls};

const DEFAULT_STREAM_HZ: u32 = 20;
const MIN_STREAM_HZ: u32 = 1;
const MAX_STREAM_HZ: u32 = 120;
const FRONTEND_STREAM_CHANNEL_CAPACITY: usize = 4;
#[cfg(feature = "official")]
const OFFICIAL_BOT_LOG_MAX_LINES: usize = 5_000;
#[cfg(feature = "official")]
const OFFICIAL_BOT_LOG_MAX_CHARS: usize = 200_000;
#[cfg(feature = "official")]
const OFFICIAL_BOT_LOG_STREAM_CHANNEL_CAPACITY: usize = 64;
#[cfg(feature = "official")]
const OFFICIAL_BOT_LOG_STREAM_POLL_INTERVAL_MS: u64 = 200;

#[cfg(feature = "official")]
struct BotLogsSnapshot {
    lines: Vec<String>,
    truncated: bool,
    file_size_bytes: u64,
}

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
    #[cfg(feature = "official")]
    official_sandbox_joins: OfficialSandboxJoinRegistry,
    #[cfg(feature = "local")]
    local_sandbox_store: LocalSandboxConfigStore,
}

impl RaceServiceImpl {
    /// Build a Race service that talks to the engine worker.
    pub(crate) fn new(
        engine: EngineClient,
        simulation_hz: u32,
        app_env: AppEnv,
        game_token_jwks_endpoint: &str,
        jwt_audience: Vec<String>,
        jwt_issuers: Vec<String>,
        runtime_store: Arc<RaceRuntimeStore>,
        frame_hub: FrameHub,
        #[cfg(feature = "official")] official_sandbox_joins: OfficialSandboxJoinRegistry,
        #[cfg(feature = "local")] local_sandbox_store: LocalSandboxConfigStore,
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
                game_token_jwks_endpoint,
                jwt_audience,
                jwt_issuers,
            )),
            #[cfg(feature = "official")]
            official_sandbox_joins,
            #[cfg(feature = "local")]
            local_sandbox_store,
        }
    }
}

impl NamedService for RaceServiceImpl {
    const NAME: &'static str = "race.v1.RaceService";
}

#[tonic::async_trait]
impl RaceService for RaceServiceImpl {
    type StreamFrontendSpectatorStream = ReceiverStream<Result<FrontendSpectatorEvent, Status>>;
    type StreamOfficialTeamBotLogsStream =
        ReceiverStream<Result<StreamOfficialTeamBotLogsResponse, Status>>;

    async fn quick_join_dev(
        &self,
        request: Request<QuickJoinDevRequest>,
    ) -> Result<Response<QuickJoinDevResponse>, Status> {
        if self.app_env.is_production() {
            return Err(Status::failed_precondition(
                "quick join is available only in development/preprod",
            ));
        }

        let auth = parse_game_token(request.metadata())?
            .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
        let req = request.into_inner();
        let response = self.join_sandbox(req.sandbox_id, auth).await?;
        Ok(Response::new(response))
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
        self.runtime_store.set_controls_input(
            req.target_car_id,
            controls.throttle,
            controls.brake,
            controls.brake_balancer,
            controls.differential_lock,
        );
        let resp = SetControlsResponse {
            client_seq: req.client_seq,
            applies_from_tick: self.frame_hub.latest().tick,
            accepted_shift: engine_gear_shift_to_proto(accepted_controls.accepted_shift),
        };

        Ok(Response::new(resp))
    }

    async fn back_to_track(
        &self,
        request: Request<BackToTrackRequest>,
    ) -> Result<Response<BackToTrackResponse>, Status> {
        #[cfg(not(feature = "official"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "BackToTrack is supported only in official backend mode",
            ));
        }
        #[cfg(feature = "official")]
        {
            let auth = parse_game_token(request.metadata())?
                .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
            let (public_car_id, engine_car_id) = self.resolve_team_official_car(&auth).await?;
            let frame = self.frame_hub.latest();
            let applies_from_tick = frame.tick;
            let response = match self
                .engine
                .set_car_back_to_track_in(EngineCommandTarget::OfficialRace, engine_car_id)
                .await
            {
                Ok(()) => {
                    self.runtime_store
                        .mark_back_to_track_applied(public_car_id, frame.server_time_ms);
                    BackToTrackResponse {
                        status: ParticipantCommandStatus::Accepted as i32,
                        applies_from_tick,
                        rejected_reason: ParticipantCommandRejectReason::Unspecified as i32,
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        engine_car_id,
                        error = %err,
                        "BackToTrack command rejected"
                    );
                    BackToTrackResponse {
                        status: ParticipantCommandStatus::Rejected as i32,
                        applies_from_tick,
                        rejected_reason: ParticipantCommandRejectReason::NotAllowed as i32,
                    }
                }
            };
            Ok(Response::new(response))
        }
    }

    async fn request_pitstop(
        &self,
        request: Request<RequestPitstopRequest>,
    ) -> Result<Response<RequestPitstopResponse>, Status> {
        #[cfg(not(feature = "official"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "RequestPitstop is supported only in official backend mode",
            ));
        }
        #[cfg(feature = "official")]
        {
            let auth = parse_game_token(request.metadata())?
                .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
            let req = request.into_inner();
            let (public_car_id, _engine_car_id) = self.resolve_team_official_car(&auth).await?;
            self.runtime_store
                .set_pit_request_active(public_car_id, req.request_pitstop);
            let response = RequestPitstopResponse {
                status: ParticipantCommandStatus::Accepted as i32,
                applies_from_tick: self.frame_hub.latest().tick,
                rejected_reason: ParticipantCommandRejectReason::Unspecified as i32,
            };
            Ok(Response::new(response))
        }
    }

    async fn emergency_pitstop(
        &self,
        request: Request<EmergencyPitstopRequest>,
    ) -> Result<Response<EmergencyPitstopResponse>, Status> {
        #[cfg(not(feature = "official"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "EmergencyPitstop is supported only in official backend mode",
            ));
        }
        #[cfg(feature = "official")]
        {
            let auth = parse_game_token(request.metadata())?
                .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
            let (public_car_id, engine_car_id) = self.resolve_team_official_car(&auth).await?;
            let frame = self.frame_hub.latest();
            let applies_from_tick = frame.tick;
            let response = match self
                .engine
                .set_car_to_pitstop_in(EngineCommandTarget::OfficialRace, engine_car_id)
                .await
            {
                Ok(()) => {
                    self.runtime_store
                        .mark_emergency_pitstop_requested(public_car_id, frame.server_time_ms);
                    EmergencyPitstopResponse {
                        status: ParticipantCommandStatus::Accepted as i32,
                        applies_from_tick,
                        rejected_reason: ParticipantCommandRejectReason::Unspecified as i32,
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        public_car_id,
                        engine_car_id,
                        error = %err,
                        "EmergencyPitstop command rejected"
                    );
                    EmergencyPitstopResponse {
                        status: ParticipantCommandStatus::Rejected as i32,
                        applies_from_tick,
                        rejected_reason: ParticipantCommandRejectReason::NotAllowed as i32,
                    }
                }
            };
            Ok(Response::new(response))
        }
    }

    async fn set_next_pit_tire_type(
        &self,
        request: Request<SetNextPitTireTypeRequest>,
    ) -> Result<Response<SetNextPitTireTypeResponse>, Status> {
        #[cfg(not(feature = "official"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "SetNextPitTireType is supported only in official backend mode",
            ));
        }
        #[cfg(feature = "official")]
        {
            let auth = parse_game_token(request.metadata())?
                .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
            let req = request.into_inner();
            let (public_car_id, _engine_car_id) = self.resolve_team_official_car(&auth).await?;
            let applies_from_tick = self.frame_hub.latest().tick;
            let response = match runtime_tire_type_from_proto(req.next_tire_type) {
                Ok(next_tire_type) => {
                    self.runtime_store
                        .set_next_pit_tire_type(public_car_id, next_tire_type);
                    SetNextPitTireTypeResponse {
                        status: ParticipantCommandStatus::Accepted as i32,
                        applies_from_tick,
                        rejected_reason: ParticipantCommandRejectReason::Unspecified as i32,
                    }
                }
                Err(()) => SetNextPitTireTypeResponse {
                    status: ParticipantCommandStatus::Rejected as i32,
                    applies_from_tick,
                    rejected_reason: ParticipantCommandRejectReason::NotAllowed as i32,
                },
            };
            Ok(Response::new(response))
        }
    }

    async fn get_official_team_bot_logs(
        &self,
        request: Request<GetOfficialTeamBotLogsRequest>,
    ) -> Result<Response<GetOfficialTeamBotLogsResponse>, Status> {
        #[cfg(not(feature = "official"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "GetOfficialTeamBotLogs is supported only in official backend mode",
            ));
        }
        #[cfg(feature = "official")]
        {
            let auth = parse_game_token(request.metadata())?
                .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
            let team_id = self
                .token_validator
                .team_id_from_token(&auth)
                .await?
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| Status::unauthenticated("missing team_id claim"))?;

            let join_state = self
                .official_sandbox_joins
                .get(team_id.as_str())
                .map(|entry| entry.value().clone())
                .ok_or_else(|| Status::not_found("no active official sandbox bot for team"))?;

            let snapshot = read_tail_bot_logs_snapshot(&join_state.log_file_path)
                .await
                .map_err(|err| {
                    tracing::warn!(
                        team_id = %team_id,
                        log_file = %join_state.log_file_path.display(),
                        error = %err,
                        "failed to read official team bot logs"
                    );
                    Status::internal("failed to read official team bot logs")
                })?;

            Ok(Response::new(GetOfficialTeamBotLogsResponse {
                lines: snapshot.lines,
                truncated: snapshot.truncated,
            }))
        }
    }

    async fn stream_official_team_bot_logs(
        &self,
        request: Request<StreamOfficialTeamBotLogsRequest>,
    ) -> Result<Response<Self::StreamOfficialTeamBotLogsStream>, Status> {
        #[cfg(not(feature = "official"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "StreamOfficialTeamBotLogs is supported only in official backend mode",
            ));
        }
        #[cfg(feature = "official")]
        {
            let auth = parse_game_token(request.metadata())?
                .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
            let team_id = self
                .token_validator
                .team_id_from_token(&auth)
                .await?
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| Status::unauthenticated("missing team_id claim"))?;

            let join_state = self
                .official_sandbox_joins
                .get(team_id.as_str())
                .map(|entry| entry.value().clone())
                .ok_or_else(|| Status::not_found("no active official sandbox bot for team"))?;

            let snapshot = read_tail_bot_logs_snapshot(&join_state.log_file_path)
                .await
                .map_err(|err| {
                    tracing::warn!(
                        team_id = %team_id,
                        log_file = %join_state.log_file_path.display(),
                        error = %err,
                        "failed to read official team bot log snapshot"
                    );
                    Status::internal("failed to read official team bot logs")
                })?;

            let (tx, rx) = mpsc::channel(OFFICIAL_BOT_LOG_STREAM_CHANNEL_CAPACITY);
            let joins = self.official_sandbox_joins.clone();
            let team_id_for_task = team_id.clone();
            let log_path = join_state.log_file_path.clone();
            let mut offset = snapshot.file_size_bytes;
            let mut pending_tail = String::new();

            tokio::spawn(async move {
                let snapshot_event = StreamOfficialTeamBotLogsResponse {
                    payload: Some(OfficialTeamBotLogsPayload::Snapshot(
                        OfficialTeamBotLogsSnapshot {
                            lines: snapshot.lines,
                            truncated: snapshot.truncated,
                        },
                    )),
                };
                if tx.send(Ok(snapshot_event)).await.is_err() {
                    return;
                }

                let mut interval = tokio::time::interval(Duration::from_millis(
                    OFFICIAL_BOT_LOG_STREAM_POLL_INTERVAL_MS,
                ));
                interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                interval.tick().await;

                loop {
                    if tx.is_closed() {
                        break;
                    }
                    interval.tick().await;

                    let current_log_path = joins
                        .get(team_id_for_task.as_str())
                        .map(|entry| entry.value().log_file_path.clone());
                    let Some(current_log_path) = current_log_path else {
                        break;
                    };
                    if current_log_path != log_path {
                        break;
                    }

                    match read_appended_bot_log_lines(&log_path, &mut offset, &mut pending_tail)
                        .await
                    {
                        Ok(lines) => {
                            for line in lines {
                                let event = StreamOfficialTeamBotLogsResponse {
                                    payload: Some(OfficialTeamBotLogsPayload::Line(
                                        OfficialTeamBotLogLine { line },
                                    )),
                                };
                                if tx.send(Ok(event)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Err(err) => {
                            let _ = tx
                                .send(Err(Status::internal(format!(
                                    "failed to read official team bot logs: {err}"
                                ))))
                                .await;
                            break;
                        }
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        }
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
}

#[cfg(feature = "official")]
async fn read_tail_bot_logs_snapshot(path: &Path) -> Result<BotLogsSnapshot, std::io::Error> {
    let file = File::open(path).await?;
    let file_size_bytes = file.metadata().await?.len();
    let mut lines_reader = BufReader::new(file).lines();
    let mut window: VecDeque<(String, usize)> = VecDeque::new();
    let mut total_chars = 0usize;
    let mut truncated = false;

    while let Some(raw_line) = lines_reader.next_line().await? {
        let (line, line_chars, line_was_truncated) =
            truncate_line_tail(raw_line, OFFICIAL_BOT_LOG_MAX_CHARS);
        if line_was_truncated {
            truncated = true;
        }
        window.push_back((line, line_chars));
        total_chars = total_chars.saturating_add(line_chars);

        while window.len() > OFFICIAL_BOT_LOG_MAX_LINES || total_chars > OFFICIAL_BOT_LOG_MAX_CHARS
        {
            if let Some((_, removed_chars)) = window.pop_front() {
                total_chars = total_chars.saturating_sub(removed_chars);
                truncated = true;
            } else {
                break;
            }
        }
    }

    Ok(BotLogsSnapshot {
        lines: window.into_iter().map(|(line, _line_chars)| line).collect(),
        truncated,
        file_size_bytes,
    })
}

#[cfg(feature = "official")]
async fn read_appended_bot_log_lines(
    path: &Path,
    offset: &mut u64,
    pending_tail: &mut String,
) -> Result<Vec<String>, std::io::Error> {
    let mut file = File::open(path).await?;
    let file_size_bytes = file.metadata().await?.len();
    if file_size_bytes < *offset {
        *offset = 0;
        pending_tail.clear();
    }

    file.seek(SeekFrom::Start(*offset)).await?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    *offset = offset.saturating_add(bytes.len() as u64);
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    pending_tail.push_str(&String::from_utf8_lossy(&bytes));
    let mut lines = Vec::new();
    while let Some(line_end_idx) = pending_tail.find('\n') {
        let mut line = pending_tail[..line_end_idx].to_string();
        if line.ends_with('\r') {
            line.pop();
        }
        let (line, _line_chars, _line_was_truncated) =
            truncate_line_tail(line, OFFICIAL_BOT_LOG_MAX_CHARS);
        lines.push(line);

        pending_tail.drain(..=line_end_idx);
    }

    Ok(lines)
}

#[cfg(feature = "official")]
fn truncate_line_tail(line: String, max_chars: usize) -> (String, usize, bool) {
    let char_count = line.chars().count();
    if char_count <= max_chars {
        return (line, char_count, false);
    }

    let start_char_index = char_count - max_chars;
    let start_byte_index = line
        .char_indices()
        .nth(start_char_index)
        .map(|(byte_idx, _)| byte_idx)
        .unwrap_or(line.len());

    let tail = line[start_byte_index..].to_string();
    (tail, max_chars, true)
}

impl RaceServiceImpl {
    async fn join_sandbox(
        &self,
        requested_sandbox_id: String,
        auth: String,
    ) -> Result<QuickJoinDevResponse, Status> {
        let engine = self.engine.clone();
        let runtime_state = engine.runtime_state().await.map_err(map_worker_err)?;
        let active_sandbox = select_quick_join_sandbox(&runtime_state, &requested_sandbox_id)?;

        let sandbox_id = active_sandbox.sandbox_id.clone();
        let map_id = active_sandbox.map_id.clone();
        let target = EngineCommandTarget::Sandbox {
            sandbox_id: sandbox_id.clone(),
        };
        let engine_car_id = engine
            .spawn_sandbox_car(sandbox_id.clone())
            .await
            .map_err(map_worker_err)?;
        let spawn_apply_result = {
            #[cfg(feature = "local")]
            {
                self.apply_local_spawn_mode(&sandbox_id, target.clone(), engine_car_id)
                    .await
            }
            #[cfg(not(feature = "local"))]
            {
                self.engine
                    .set_car_before_finish_line_in(target.clone(), engine_car_id)
                    .await
                    .map_err(map_worker_err)
            }
        };
        if let Err(status) = spawn_apply_result {
            if let Err(err) = engine.despawn_car_in(target.clone(), engine_car_id).await {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    engine_car_id,
                    error = %err,
                    "failed to despawn car after local spawn-mode apply failure"
                );
            }
            return Err(status);
        }

        let public_car_id = self.runtime_store.allocate_public_car_id();
        let mut identity = RuntimeCarIdentity::default();
        identity.subject = Some(self.token_validator.subject_from_token(&auth).await?);
        identity.team_id = self.token_validator.team_id_from_token(&auth).await?;
        identity.instance_uuid = self.token_validator.instance_uuid_from_token(&auth).await?;
        if let Some(instance_uuid) = identity.instance_uuid.clone() {
            self.instance_cars
                .insert(instance_uuid.clone(), public_car_id);
            self.car_owners.insert(public_car_id, instance_uuid);
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

        self.known_cars.insert(public_car_id, ());
        self.last_client_seq.insert(public_car_id, 0);
        self.car_engine_ids.insert(public_car_id, engine_car_id);
        self.car_targets.insert(public_car_id, target);

        Ok(QuickJoinDevResponse {
            car_id: public_car_id,
            map_id,
        })
    }

    #[cfg(feature = "local")]
    async fn local_spawn_mode_for_sandbox(
        &self,
        sandbox_id: &str,
    ) -> Result<LocalSandboxSpawnModeRecord, Status> {
        let snapshot = self.local_sandbox_store.get_snapshot().await;
        snapshot
            .sandboxes
            .iter()
            .find(|entry| entry.sandbox_id == sandbox_id)
            .map(|entry| entry.config.spawn_mode)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "local sandbox config not found for sandbox_id={sandbox_id}"
                ))
            })
    }

    #[cfg(feature = "local")]
    async fn apply_local_spawn_mode(
        &self,
        sandbox_id: &str,
        target: EngineCommandTarget,
        engine_car_id: u64,
    ) -> Result<(), Status> {
        let spawn_mode = self.local_spawn_mode_for_sandbox(sandbox_id).await?;
        match spawn_mode {
            LocalSandboxSpawnModeRecord::StartLine => self
                .engine
                .set_car_before_finish_line_in(target, engine_car_id)
                .await
                .map_err(map_worker_err),
            LocalSandboxSpawnModeRecord::RandomOnTrack => self
                .engine
                .set_car_random_on_track_in(target, engine_car_id)
                .await
                .map_err(map_worker_err),
            LocalSandboxSpawnModeRecord::InPit => self
                .engine
                .set_car_to_pitstop_in(target, engine_car_id)
                .await
                .map_err(map_worker_err),
            LocalSandboxSpawnModeRecord::RandomStartSlot => {
                let slots = self
                    .engine
                    .get_number_of_start_pos_in(target.clone())
                    .await
                    .map_err(map_worker_err)?;
                if slots == 0 {
                    return Err(Status::failed_precondition(
                        "no start slots available for selected map",
                    ));
                }
                let start_slot = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(1..=slots)
                };
                self.engine
                    .set_car_at_start_pos_in(target, engine_car_id, start_slot)
                    .await
                    .map_err(map_worker_err)
            }
        }
    }

    #[cfg(feature = "official")]
    async fn resolve_team_official_car(&self, auth: &str) -> Result<(u64, u64), Status> {
        let team_id = self
            .token_validator
            .team_id_from_token(auth)
            .await?
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| Status::unauthenticated("missing team_id claim"))?;

        let identity_map = self.runtime_store.car_identity_map();
        let mut matching_cars = Vec::new();
        for identity_entry in identity_map.iter() {
            if identity_entry.value().team_id.as_deref() != Some(team_id.as_str()) {
                continue;
            }

            let public_car_id = *identity_entry.key();
            let is_official = self
                .car_targets
                .get(&public_car_id)
                .map(|entry| matches!(entry.value(), EngineCommandTarget::OfficialRace))
                .unwrap_or(false);
            if !is_official {
                continue;
            }

            let Some(engine_car_id) = self
                .car_engine_ids
                .get(&public_car_id)
                .map(|entry| *entry.value())
            else {
                continue;
            };

            matching_cars.push((public_car_id, engine_car_id));
        }

        match matching_cars.len() {
            0 => Err(Status::not_found("no active official-race car for team")),
            1 => Ok(matching_cars[0]),
            _ => Err(Status::failed_precondition(
                "multiple active official-race cars found for team",
            )),
        }
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

#[cfg(feature = "official")]
fn runtime_tire_type_from_proto(raw: i32) -> Result<RuntimePitTireType, ()> {
    let tire_type = ProtoTireType::try_from(raw).map_err(|_| ())?;
    Ok(match tire_type {
        ProtoTireType::Unspecified => RuntimePitTireType::Unspecified,
        ProtoTireType::Hard => RuntimePitTireType::Hard,
        ProtoTireType::Soft => RuntimePitTireType::Soft,
        ProtoTireType::Wet => RuntimePitTireType::Wet,
    })
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
                &entry.pit_state,
                entry.controls_input,
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
