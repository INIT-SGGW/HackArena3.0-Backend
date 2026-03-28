//! gRPC RaceParticipantService implementation (bidi participant stream).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use boink::model::{Controls, GearShift as EngineGearShift};
use proto::race::v1::{
    CarDimensions, LocalSandboxJoinRequest, LocalSandboxJoinResponse, ParticipantBootstrap,
    ParticipantCommandAck, ParticipantCommandRejectReason, ParticipantCommandStatus,
    ParticipantCommandType, ParticipantServerEvent, ParticipantSnapshot,
    PrepareOfficialJoinRequest, PrepareOfficialJoinResponse, SpectatorView, StreamClampReason,
    StreamSettings, TireType as ProtoTireType, ViewDowngradeReason,
    participant_client_message::Payload as ParticipantClientPayload,
    participant_server_event::Payload as ParticipantServerPayload,
    race_participant_service_server::RaceParticipantService,
};
#[cfg(feature = "local")]
use rand::Rng;
#[cfg(feature = "official")]
use tokio::sync::Mutex;
#[cfg(feature = "official")]
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::auth::game_token::{GameTokenValidator, parse_game_token};
#[cfg(feature = "official")]
use crate::db::repos::submission::SubmissionRepo;
#[cfg(feature = "local")]
use crate::local::sandbox_config_store::{LocalSandboxConfigStore, LocalSandboxSpawnModeRecord};
#[cfg(feature = "official")]
use crate::runtime::engine_worker::EngineActivityKind;
#[cfg(feature = "local")]
use crate::runtime::engine_worker::{EngineActiveSandboxState, EngineRuntimeState};
use crate::runtime::engine_worker::{EngineClient, EngineCommandTarget};

use super::error_map::map_worker_err;
use super::mappers::{
    engine_gear_shift_to_proto, participant_opponent_state, participant_self_state,
    proto_participant_controls_to_controls,
};
use super::race::RuntimeCarIdentity;
use super::race::runtime_store::RuntimePitTireType;
use super::race::{FrameHub, RaceRuntimeStore};
#[cfg(feature = "official")]
use crate::services::submission::{
    GameTokenIssuer, OfficialRaceBotRegistry, OfficialSandboxJoinRegistry,
    TeamOfficialRaceBotState, WrapperAuthTokenIssuer, official_bot_container_name_for_team,
    start_bot_container, wait_bot_container_exit_code,
};

const PARTICIPANT_REQUESTED_HZ: u32 = 30;
const MIN_STREAM_HZ: u32 = 1;
const MAX_STREAM_HZ: u32 = 120;
const PARTICIPANT_STREAM_CHANNEL_CAPACITY: usize = 1;

#[derive(Clone)]
pub struct RaceParticipantServiceImpl {
    engine: EngineClient,
    simulation_hz: u32,
    runtime_store: Arc<RaceRuntimeStore>,
    frame_hub: FrameHub,
    token_validator: Arc<GameTokenValidator>,
    next_stream_seq: Arc<AtomicU64>,
    #[cfg(feature = "official")]
    official_sandbox_joins: OfficialSandboxJoinRegistry,
    #[cfg(feature = "official")]
    official_race_bots: OfficialRaceBotRegistry,
    #[cfg(feature = "official")]
    submission_repo: SubmissionRepo,
    #[cfg(feature = "official")]
    game_token_issuer: Arc<GameTokenIssuer>,
    #[cfg(feature = "official")]
    wrapper_auth_token_issuer: Arc<WrapperAuthTokenIssuer>,
    #[cfg(feature = "official")]
    wrapper_backend_endpoint: String,
    #[cfg(feature = "official")]
    slot_updates_tx: broadcast::Sender<String>,
    #[cfg(feature = "official")]
    prepare_command_lock: Arc<Mutex<()>>,
    #[cfg(feature = "local")]
    local_sandbox_store: LocalSandboxConfigStore,
}

impl RaceParticipantServiceImpl {
    pub(crate) fn new(
        engine: EngineClient,
        simulation_hz: u32,
        game_token_jwks_endpoint: &str,
        jwt_audience: Vec<String>,
        jwt_issuers: Vec<String>,
        runtime_store: Arc<RaceRuntimeStore>,
        frame_hub: FrameHub,
        #[cfg(feature = "official")] official_sandbox_joins: OfficialSandboxJoinRegistry,
        #[cfg(feature = "official")] official_race_bots: OfficialRaceBotRegistry,
        #[cfg(feature = "official")] submission_repo: SubmissionRepo,
        #[cfg(feature = "official")] game_token_issuer: Arc<GameTokenIssuer>,
        #[cfg(feature = "official")] wrapper_auth_token_issuer: Arc<WrapperAuthTokenIssuer>,
        #[cfg(feature = "official")] wrapper_backend_endpoint: String,
        #[cfg(feature = "official")] slot_updates_tx: broadcast::Sender<String>,
        #[cfg(feature = "local")] local_sandbox_store: LocalSandboxConfigStore,
    ) -> Self {
        Self {
            engine,
            simulation_hz,
            runtime_store,
            frame_hub,
            token_validator: Arc::new(GameTokenValidator::new_with_config(
                game_token_jwks_endpoint,
                jwt_audience,
                jwt_issuers,
            )),
            next_stream_seq: Arc::new(AtomicU64::new(100_000)),
            #[cfg(feature = "official")]
            official_sandbox_joins,
            #[cfg(feature = "official")]
            official_race_bots,
            #[cfg(feature = "official")]
            submission_repo,
            #[cfg(feature = "official")]
            game_token_issuer,
            #[cfg(feature = "official")]
            wrapper_auth_token_issuer,
            #[cfg(feature = "official")]
            wrapper_backend_endpoint,
            #[cfg(feature = "official")]
            slot_updates_tx,
            #[cfg(feature = "official")]
            prepare_command_lock: Arc::new(Mutex::new(())),
            #[cfg(feature = "local")]
            local_sandbox_store,
        }
    }
}

impl RaceParticipantServiceImpl {
    #[cfg(feature = "local")]
    async fn local_sandbox_join_impl(
        &self,
        requested_sandbox_id: String,
        auth: String,
    ) -> Result<LocalSandboxJoinResponse, Status> {
        let runtime_state = self.engine.runtime_state().await.map_err(map_worker_err)?;
        let active_sandbox = select_local_join_sandbox(&runtime_state, &requested_sandbox_id)?;

        let sandbox_id = active_sandbox.sandbox_id.clone();
        let map_id = active_sandbox.map_id.clone();
        let target = EngineCommandTarget::Sandbox {
            sandbox_id: sandbox_id.clone(),
        };
        let engine_car_id = self
            .engine
            .spawn_sandbox_car(sandbox_id.clone())
            .await
            .map_err(map_worker_err)?;

        if let Err(status) = self
            .apply_local_spawn_mode(&sandbox_id, target.clone(), engine_car_id)
            .await
        {
            if let Err(err) = self
                .engine
                .despawn_car_in(target.clone(), engine_car_id)
                .await
            {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    engine_car_id,
                    error = %err,
                    "failed to despawn car after local_sandbox_join spawn-mode apply failure"
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
            self.runtime_store
                .instance_cars()
                .insert(instance_uuid.clone(), public_car_id);
            self.runtime_store
                .car_owners()
                .insert(public_car_id, instance_uuid);
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

        self.runtime_store.known_cars().insert(public_car_id, ());
        self.runtime_store
            .last_client_seq()
            .insert(public_car_id, 0);
        self.runtime_store
            .car_engine_ids()
            .insert(public_car_id, engine_car_id);
        self.runtime_store
            .car_targets()
            .insert(public_car_id, target);

        Ok(LocalSandboxJoinResponse {
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
    async fn required_team_id_from_token(&self, token: &str) -> Result<String, Status> {
        self.token_validator
            .team_id_from_token(token)
            .await?
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Status::unauthenticated("missing team_id claim"))
    }

    #[cfg(feature = "official")]
    fn resolve_team_official_race_car(&self, team_id: &str) -> Result<Option<(u64, u64)>, Status> {
        let team_id = team_id.trim();
        if team_id.is_empty() {
            return Err(Status::unauthenticated("missing team_id claim"));
        }

        let identities = self.runtime_store.car_identity_map();
        let targets = self.runtime_store.car_targets();
        let engine_ids = self.runtime_store.car_engine_ids();

        let mut matching = Vec::new();
        for identity_entry in identities.iter() {
            if identity_entry.value().team_id.as_deref() != Some(team_id) {
                continue;
            }

            let public_car_id = *identity_entry.key();
            let is_official_race = targets
                .get(&public_car_id)
                .map(|entry| matches!(entry.value(), EngineCommandTarget::OfficialRace))
                .unwrap_or(false);
            if !is_official_race {
                continue;
            }

            let Some(engine_car_id) = engine_ids.get(&public_car_id).map(|entry| *entry.value())
            else {
                continue;
            };

            matching.push((public_car_id, engine_car_id));
        }

        match matching.len() {
            0 => Ok(None),
            1 => Ok(matching.pop()),
            _ => Err(Status::failed_precondition(
                "multiple active official-race cars found for team",
            )),
        }
    }

    #[cfg(feature = "official")]
    fn require_team_official_race_car(&self, team_id: &str) -> Result<(u64, u64), Status> {
        self.resolve_team_official_race_car(team_id)?
            .ok_or_else(|| Status::not_found("no active official-race car for team"))
    }

    #[cfg(feature = "official")]
    async fn resolve_selected_slot_for_team(&self, team_id: &str) -> Result<i16, Status> {
        let loaded_slot = self
            .official_race_bots
            .get(team_id)
            .map(|entry| entry.value().slot_index);
        self.submission_repo
            .resolve_selected_slot_index(team_id, loaded_slot)
            .await
            .map_err(|err| Status::internal(format!("failed to resolve selected slot: {err}")))
    }

    #[cfg(feature = "official")]
    async fn resolve_slot_submission_image(
        &self,
        team_id: &str,
        slot_index: i16,
    ) -> Result<(String, String), Status> {
        let slot_submission = self
            .submission_repo
            .get_succeeded_submission_for_slot(team_id, slot_index)
            .await
            .map_err(|err| Status::internal(format!("failed to resolve slot submission: {err}")))?;
        let Some(slot_submission) = slot_submission else {
            return Err(Status::failed_precondition(
                "selected slot does not contain succeeded submission",
            ));
        };
        let image_ref = slot_submission
            .image_ref
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Status::failed_precondition("selected slot submission is missing image_ref")
            })?
            .to_string();
        Ok((slot_submission.submission_id, image_ref))
    }

    #[cfg(feature = "official")]
    async fn cleanup_spawned_official_race_car(&self, public_car_id: u64, engine_car_id: u64) {
        if let Err(err) = self
            .engine
            .despawn_car_in(EngineCommandTarget::OfficialRace, engine_car_id)
            .await
        {
            tracing::warn!(
                public_car_id,
                engine_car_id,
                error = %err,
                "prepare official join: failed to rollback spawned official-race car"
            );
        }
        self.runtime_store.remove_car(public_car_id);
    }

    #[cfg(feature = "official")]
    fn spawn_official_race_bot_exit_monitor(&self, team_id: String, container_id: String) {
        let service = self.clone();
        tokio::spawn(async move {
            let exit_code = match wait_bot_container_exit_code(&container_id).await {
                Ok(code) => code,
                Err(err) => {
                    tracing::warn!(
                        team_id = %team_id,
                        container_id = %container_id,
                        error = %err,
                        "official race bot exit monitor failed to wait for container"
                    );
                    return;
                }
            };
            tracing::info!(
                team_id = %team_id,
                container_id = %container_id,
                exit_code,
                "official race bot container exited"
            );

            let _guard = service.prepare_command_lock.lock().await;
            let Some(current) = service
                .official_race_bots
                .get(&team_id)
                .map(|entry| entry.value().clone())
            else {
                return;
            };
            if current.container_id != container_id {
                return;
            }
            service.official_race_bots.remove(&team_id);
            service
                .runtime_store
                .clear_active_bot_slot(current.public_car_id);
            let _ = service.slot_updates_tx.send(team_id.clone());

            if let Err(err) = service
                .engine
                .set_car_to_pitstop_in(EngineCommandTarget::OfficialRace, current.engine_car_id)
                .await
            {
                tracing::warn!(
                    team_id = %team_id,
                    public_car_id = current.public_car_id,
                    engine_car_id = current.engine_car_id,
                    error = %err,
                    "official race bot exit monitor: failed to move car to pit"
                );
            }
            let now_ms = service.frame_hub.latest().server_time_ms;
            service
                .runtime_store
                .mark_emergency_pitstop_requested(current.public_car_id, now_ms);
        });
    }

    #[cfg(feature = "official")]
    pub(crate) fn spawn_slot_switch_poller(&self) {
        let service = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(300));
            loop {
                ticker.tick().await;
                let team_ids: Vec<String> = service
                    .official_race_bots
                    .iter()
                    .map(|entry| entry.key().clone())
                    .collect();
                for team_id in team_ids {
                    if let Err(status) = service
                        .try_apply_pending_selected_slot_for_team(&team_id)
                        .await
                    {
                        tracing::warn!(
                            team_id = %team_id,
                            code = ?status.code(),
                            error = %status,
                            "official race selected-slot switch attempt failed"
                        );
                    }
                }
            }
        });
    }

    #[cfg(feature = "official")]
    async fn try_apply_pending_selected_slot_for_team(&self, team_id: &str) -> Result<(), Status> {
        let _guard = self.prepare_command_lock.lock().await;
        let Some(race_bot_state) = self
            .official_race_bots
            .get(team_id)
            .map(|entry| entry.value().clone())
        else {
            return Ok(());
        };
        if !self
            .runtime_store
            .is_in_stationary_fix(race_bot_state.public_car_id)
        {
            return Ok(());
        }

        let selected_slot = self.resolve_selected_slot_for_team(team_id).await?;
        if selected_slot == race_bot_state.slot_index {
            return Ok(());
        }
        let (submission_id, image_ref) = self
            .resolve_slot_submission_image(team_id, selected_slot)
            .await?;
        let team_bot_token = self.game_token_issuer.issue_team_bot_token(team_id).await?;
        let wrapper_auth_token = self
            .wrapper_auth_token_issuer
            .issue_wrapper_auth_token()
            .await?;
        let switching_container_id =
            format!("switching-{}", self.frame_hub.latest().server_time_ms);
        if let Some(mut entry) = self.official_race_bots.get_mut(team_id) {
            entry.container_id = switching_container_id;
        }
        let container_id = start_bot_container(
            &image_ref,
            &race_bot_state.container_name,
            &self.wrapper_backend_endpoint,
            &team_bot_token,
            &wrapper_auth_token,
            team_id,
            &submission_id,
            "official-race",
            selected_slot,
        )
        .await
        .map_err(|err| {
            Status::internal(format!(
                "failed to restart official-race bot container for selected slot: {err}"
            ))
        })?;

        self.official_race_bots.insert(
            team_id.to_string(),
            TeamOfficialRaceBotState {
                public_car_id: race_bot_state.public_car_id,
                engine_car_id: race_bot_state.engine_car_id,
                slot_index: selected_slot,
                container_name: race_bot_state.container_name,
                container_id: container_id.clone(),
            },
        );
        self.runtime_store
            .set_active_bot_slot(race_bot_state.public_car_id, selected_slot);
        let _ = self.slot_updates_tx.send(team_id.to_string());
        self.spawn_official_race_bot_exit_monitor(team_id.to_string(), container_id);
        Ok(())
    }
}

#[cfg(feature = "local")]
fn select_local_join_sandbox<'a>(
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

#[tonic::async_trait]
impl RaceParticipantService for RaceParticipantServiceImpl {
    type StreamStream = ReceiverStream<Result<ParticipantServerEvent, Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<proto::race::v1::ParticipantClientMessage>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let token = parse_game_token(request.metadata())?
            .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
        let self_public_car_id = if let Some(instance_uuid) = self
            .token_validator
            .instance_uuid_from_token(&token)
            .await?
        {
            self.runtime_store
                .instance_cars()
                .get(&instance_uuid)
                .map(|entry| *entry.value())
                .ok_or_else(|| Status::not_found("unknown instance_uuid"))?
        } else {
            #[cfg(feature = "official")]
            {
                let team_id = self.required_team_id_from_token(&token).await?;
                let runtime_state = self.engine.runtime_state().await.map_err(map_worker_err)?;
                match runtime_state.activity_kind {
                    EngineActivityKind::OfficialRace => {
                        self.require_team_official_race_car(&team_id)?.0
                    }
                    EngineActivityKind::Sandbox => self
                        .official_sandbox_joins
                        .get(&team_id)
                        .map(|entry| entry.value().public_car_id)
                        .ok_or_else(|| {
                            Status::not_found("no active official sandbox join for team")
                        })?,
                    EngineActivityKind::None => {
                        return Err(Status::failed_precondition("runtime is not active"));
                    }
                }
            }
            #[cfg(not(feature = "official"))]
            {
                return Err(Status::unauthenticated("missing instance_uuid claim"));
            }
        };
        let self_target = self
            .runtime_store
            .car_target(self_public_car_id)
            .ok_or_else(|| Status::not_found("unknown car target"))?;
        let self_engine_car_id = self
            .runtime_store
            .car_engine_id(self_public_car_id)
            .ok_or_else(|| Status::not_found("unknown car target"))?;
        let scopes = self.token_validator.scopes_from_token(&token).await?;

        let incoming = request.into_inner();
        let stream_id = self.next_stream_seq.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(PARTICIPANT_STREAM_CHANNEL_CAPACITY);

        let engine = self.engine.clone();
        let frame_hub = self.frame_hub.clone();
        let runtime_store = self.runtime_store.clone();
        let simulation_hz = self.simulation_hz;

        tokio::spawn(async move {
            run_participant_stream(
                engine,
                frame_hub,
                runtime_store,
                simulation_hz,
                scopes,
                stream_id,
                self_public_car_id,
                self_engine_car_id,
                self_target,
                incoming,
                tx,
            )
            .await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn prepare_official_join(
        &self,
        request: Request<PrepareOfficialJoinRequest>,
    ) -> Result<Response<PrepareOfficialJoinResponse>, Status> {
        #[cfg(not(feature = "official"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "PrepareOfficialJoin is supported only in official backend mode",
            ));
        }
        #[cfg(feature = "official")]
        {
            let token = parse_game_token(request.metadata())?
                .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
            let _ = request.into_inner();
            let team_id = self.required_team_id_from_token(&token).await?;

            let _prepare_guard = self.prepare_command_lock.lock().await;
            let runtime_state = self.engine.runtime_state().await.map_err(map_worker_err)?;

            match runtime_state.activity_kind {
                EngineActivityKind::OfficialRace => {
                    let map_id = runtime_state.map_id.clone();
                    let mut spawned_new_car = false;
                    let (public_car_id, engine_car_id) =
                        if let Some((public_car_id, engine_car_id)) =
                            self.resolve_team_official_race_car(&team_id)?
                        {
                            tracing::info!(
                                team_id = %team_id,
                                public_car_id,
                                engine_car_id,
                                map_id = %map_id,
                                "prepare official join: reused official-race car"
                            );
                            (public_car_id, engine_car_id)
                        } else {
                            self.official_race_bots.remove(&team_id);
                            let engine_car_id =
                                self.engine.spawn_car().await.map_err(map_worker_err)?;
                            let public_car_id = self.runtime_store.allocate_public_car_id();
                            let mut identity = RuntimeCarIdentity::default();
                            identity.team_id = Some(team_id.clone());
                            self.runtime_store.set_car_identity(public_car_id, identity);
                            self.runtime_store.known_cars().insert(public_car_id, ());
                            self.runtime_store
                                .last_client_seq()
                                .insert(public_car_id, 0);
                            self.runtime_store
                                .car_engine_ids()
                                .insert(public_car_id, engine_car_id);
                            self.runtime_store
                                .car_targets()
                                .insert(public_car_id, EngineCommandTarget::OfficialRace);
                            spawned_new_car = true;
                            tracing::info!(
                                team_id = %team_id,
                                public_car_id,
                                engine_car_id,
                                map_id = %map_id,
                                "prepare official join: spawned official-race car"
                            );
                            (public_car_id, engine_car_id)
                        };

                    if let Some(existing) = self.official_race_bots.get(&team_id) {
                        if !existing.container_id.starts_with("switching-") {
                            self.runtime_store
                                .set_active_bot_slot(public_car_id, existing.slot_index);
                            return Ok(Response::new(PrepareOfficialJoinResponse {
                                car_id: public_car_id,
                                map_id,
                            }));
                        }
                    }

                    let selected_slot = match self.resolve_selected_slot_for_team(&team_id).await {
                        Ok(value) => value,
                        Err(status) => {
                            if spawned_new_car {
                                self.cleanup_spawned_official_race_car(
                                    public_car_id,
                                    engine_car_id,
                                )
                                .await;
                            }
                            return Err(status);
                        }
                    };
                    let (submission_id, image_ref) = match self
                        .resolve_slot_submission_image(&team_id, selected_slot)
                        .await
                    {
                        Ok(value) => value,
                        Err(status) => {
                            if spawned_new_car {
                                self.cleanup_spawned_official_race_car(
                                    public_car_id,
                                    engine_car_id,
                                )
                                .await;
                            }
                            return Err(status);
                        }
                    };

                    let team_bot_token =
                        match self.game_token_issuer.issue_team_bot_token(&team_id).await {
                            Ok(value) => value,
                            Err(status) => {
                                if spawned_new_car {
                                    self.cleanup_spawned_official_race_car(
                                        public_car_id,
                                        engine_car_id,
                                    )
                                    .await;
                                }
                                return Err(status);
                            }
                        };
                    let wrapper_auth_token = match self
                        .wrapper_auth_token_issuer
                        .issue_wrapper_auth_token()
                        .await
                    {
                        Ok(value) => value,
                        Err(status) => {
                            if spawned_new_car {
                                self.cleanup_spawned_official_race_car(
                                    public_car_id,
                                    engine_car_id,
                                )
                                .await;
                            }
                            return Err(status);
                        }
                    };
                    let container_name =
                        official_bot_container_name_for_team(&team_id).map_err(|err| {
                            Status::failed_precondition(format!(
                                "invalid team_id for container name: {err}"
                            ))
                        })?;
                    let container_id = match start_bot_container(
                        &image_ref,
                        &container_name,
                        &self.wrapper_backend_endpoint,
                        &team_bot_token,
                        &wrapper_auth_token,
                        &team_id,
                        &submission_id,
                        "official-race",
                        selected_slot,
                    )
                    .await
                    {
                        Ok(id) => id,
                        Err(err) => {
                            if spawned_new_car {
                                self.cleanup_spawned_official_race_car(
                                    public_car_id,
                                    engine_car_id,
                                )
                                .await;
                            }
                            return Err(Status::internal(format!(
                                "failed to start official-race bot container: {err}"
                            )));
                        }
                    };

                    self.official_race_bots.insert(
                        team_id.clone(),
                        TeamOfficialRaceBotState {
                            public_car_id,
                            engine_car_id,
                            slot_index: selected_slot,
                            container_name: container_name.clone(),
                            container_id: container_id.clone(),
                        },
                    );
                    self.runtime_store
                        .set_active_bot_slot(public_car_id, selected_slot);
                    let _ = self.slot_updates_tx.send(team_id.clone());
                    self.spawn_official_race_bot_exit_monitor(
                        team_id.clone(),
                        container_id.clone(),
                    );
                    tracing::info!(
                        team_id = %team_id,
                        public_car_id,
                        engine_car_id,
                        slot_index = selected_slot,
                        submission_id = %submission_id,
                        container_name = %container_name,
                        container_id = %container_id,
                        "prepare official join: started official-race bot container"
                    );
                    Ok(Response::new(PrepareOfficialJoinResponse {
                        car_id: public_car_id,
                        map_id,
                    }))
                }
                EngineActivityKind::Sandbox => {
                    let join_state = self
                        .official_sandbox_joins
                        .get(&team_id)
                        .map(|entry| entry.value().clone())
                        .ok_or_else(|| {
                            Status::not_found("no active official sandbox join for team")
                        })?;

                    let map_id = runtime_state
                        .active_sandboxes
                        .iter()
                        .find(|entry| entry.sandbox_id == join_state.sandbox_id)
                        .map(|entry| entry.map_id.clone())
                        .ok_or_else(|| {
                            Status::failed_precondition(
                                "sandbox runtime is not active for team join",
                            )
                        })?;

                    tracing::info!(
                        team_id = %team_id,
                        sandbox_id = %join_state.sandbox_id,
                        slot_index = join_state.slot_index,
                        public_car_id = join_state.public_car_id,
                        map_id = %map_id,
                        "prepare official join: resolved sandbox join"
                    );
                    Ok(Response::new(PrepareOfficialJoinResponse {
                        car_id: join_state.public_car_id,
                        map_id,
                    }))
                }
                EngineActivityKind::None => {
                    Err(Status::failed_precondition("runtime is not active"))
                }
            }
        }
    }

    async fn local_sandbox_join(
        &self,
        request: Request<LocalSandboxJoinRequest>,
    ) -> Result<Response<LocalSandboxJoinResponse>, Status> {
        #[cfg(not(feature = "local"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "LocalSandboxJoin is supported only in local backend mode",
            ));
        }
        #[cfg(feature = "local")]
        {
            let auth = parse_game_token(request.metadata())?
                .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
            let req = request.into_inner();
            let joined = self.local_sandbox_join_impl(req.sandbox_id, auth).await?;
            Ok(Response::new(joined))
        }
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

fn resolve_participant_rate(simulation_hz: u32) -> (u32, u32, StreamClampReason, Duration) {
    let requested_hz = PARTICIPANT_REQUESTED_HZ;
    let max_hz = MAX_STREAM_HZ.min(simulation_hz.max(1));
    let effective_hz = requested_hz.clamp(MIN_STREAM_HZ, max_hz);
    let clamp_reason = if effective_hz == requested_hz {
        StreamClampReason::None
    } else {
        StreamClampReason::ServerLimit
    };
    let period = Duration::from_secs_f64(1.0 / effective_hz as f64);
    (requested_hz, effective_hz, clamp_reason, period)
}

fn resolve_runtime_map_id(frame_hub: &FrameHub, visible_target: &EngineCommandTarget) -> String {
    let frame = frame_hub.latest();
    let Some(runtime_state) = frame.runtime_state.as_ref() else {
        return String::new();
    };

    if let EngineCommandTarget::Sandbox { sandbox_id } = visible_target {
        if let Some(active) = runtime_state
            .active_sandboxes
            .iter()
            .find(|entry| entry.sandbox_id == *sandbox_id)
        {
            return active.map_id.clone();
        }
    }

    runtime_state.map_id.clone()
}

async fn send_participant_event(
    tx: &mpsc::Sender<Result<ParticipantServerEvent, Status>>,
    server_seq: &mut u64,
    payload: ParticipantServerPayload,
) -> bool {
    let msg = ParticipantServerEvent {
        server_seq: *server_seq,
        payload: Some(payload),
    };
    *server_seq = server_seq.saturating_add(1);
    tx.send(Ok(msg)).await.is_ok()
}

fn emit_participant_terminal_error(
    tx: &mpsc::Sender<Result<ParticipantServerEvent, Status>>,
    status: Status,
) {
    if tx.try_send(Err(status)).is_err() {
        tracing::debug!("participant stream terminal status not delivered");
    }
}

async fn cleanup_participant_car(
    reason: &'static str,
    engine: &EngineClient,
    runtime_store: &RaceRuntimeStore,
    public_car_id: u64,
    target: &EngineCommandTarget,
    engine_car_id: u64,
) {
    match target {
        EngineCommandTarget::Sandbox { .. } => {
            #[cfg(feature = "official")]
            if runtime_store
                .car_identity(public_car_id)
                .and_then(|identity| identity.team_id)
                .is_some()
            {
                tracing::info!(
                    public_car_id,
                    engine_car_id,
                    target = ?target,
                    reason,
                    "participant cleanup: preserving official sandbox car"
                );
                return;
            }
            if let Err(err) = engine.despawn_car_in(target.clone(), engine_car_id).await {
                tracing::warn!(
                    public_car_id,
                    engine_car_id,
                    target = ?target,
                    error = %err,
                    "failed to despawn participant car during cleanup"
                );
            }
            runtime_store.remove_car(public_car_id);
            tracing::info!(
                public_car_id,
                engine_car_id,
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

fn participant_settings(
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

fn participant_ack(
    client_seq: u64,
    applies_from_tick: u64,
    accepted_shift: i32,
) -> proto::race::v1::ParticipantControlsAck {
    proto::race::v1::ParticipantControlsAck {
        client_seq,
        applies_from_tick,
        accepted_shift,
    }
}

fn participant_command_ack(
    client_seq: u64,
    command_type: ParticipantCommandType,
    status: ParticipantCommandStatus,
    applies_from_tick: u64,
    rejected_reason: ParticipantCommandRejectReason,
    cooldown_remaining_ms: u32,
) -> ParticipantCommandAck {
    ParticipantCommandAck {
        client_seq,
        command_type: command_type as i32,
        status: status as i32,
        applies_from_tick,
        rejected_reason: rejected_reason as i32,
        cooldown_remaining_ms,
    }
}

fn runtime_tire_type_from_proto(raw: i32) -> Result<RuntimePitTireType, ()> {
    let tire_type = ProtoTireType::try_from(raw).map_err(|_| ())?;
    Ok(match tire_type {
        ProtoTireType::Unspecified => RuntimePitTireType::Unspecified,
        ProtoTireType::Hard => RuntimePitTireType::Hard,
        ProtoTireType::Soft => RuntimePitTireType::Soft,
        ProtoTireType::Wet => RuntimePitTireType::Wet,
    })
}

async fn run_participant_stream(
    engine: EngineClient,
    frame_hub: FrameHub,
    runtime_store: Arc<RaceRuntimeStore>,
    simulation_hz: u32,
    scopes: Vec<String>,
    stream_id: u64,
    self_public_car_id: u64,
    self_engine_car_id: u64,
    self_target: EngineCommandTarget,
    mut incoming: Streaming<proto::race::v1::ParticipantClientMessage>,
    tx: mpsc::Sender<Result<ParticipantServerEvent, Status>>,
) {
    let (requested_hz, effective_hz, clamp_reason, period) =
        resolve_participant_rate(simulation_hz);
    let mut ticker = tokio::time::interval(period);
    let requested_view = SpectatorView::Team;
    let (resolved_view, view_downgrade_reason) = resolve_view(requested_view, &scopes);
    let runtime_map_id = resolve_runtime_map_id(&frame_hub, &self_target);
    let mut initialized = false;
    let mut server_seq = 1_u64;

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
        "participant bidi stream started"
    );

    loop {
        tokio::select! {
            msg = incoming.message() => {
                let msg = match msg {
                    Ok(Some(value)) => value,
                    Ok(None) => {
                        cleanup_participant_car(
                            "disconnect",
                            &engine,
                            runtime_store.as_ref(),
                            self_public_car_id,
                            &self_target,
                            self_engine_car_id,
                        ).await;
                        break;
                    }
                    Err(status) => {
                        emit_participant_terminal_error(&tx, status);
                        cleanup_participant_car(
                            "client-stream-error",
                            &engine,
                            runtime_store.as_ref(),
                            self_public_car_id,
                            &self_target,
                            self_engine_car_id,
                        ).await;
                        break;
                    }
                };

                let Some(payload) = msg.payload else {
                    emit_participant_terminal_error(
                        &tx,
                        Status::invalid_argument("participant message payload is required"),
                    );
                    cleanup_participant_car(
                        "invalid-message-empty-payload",
                        &engine,
                        runtime_store.as_ref(),
                        self_public_car_id,
                        &self_target,
                        self_engine_car_id,
                    ).await;
                    break;
                };

                match payload {
                    ParticipantClientPayload::Init(_) => {
                        if initialized {
                            emit_participant_terminal_error(
                                &tx,
                                Status::invalid_argument("participant init may be sent only once"),
                            );
                            cleanup_participant_car(
                                "duplicate-init",
                                &engine,
                                runtime_store.as_ref(),
                                self_public_car_id,
                                &self_target,
                                self_engine_car_id,
                            ).await;
                            break;
                        }

                        initialized = true;
                        let settings = participant_settings(
                            requested_hz,
                            effective_hz,
                            clamp_reason,
                            resolved_view,
                            view_downgrade_reason,
                            &runtime_map_id,
                        );
                        if !send_participant_event(
                            &tx,
                            &mut server_seq,
                            ParticipantServerPayload::Settings(settings),
                        ).await {
                            cleanup_participant_car(
                                "initial-send-failed",
                                &engine,
                                runtime_store.as_ref(),
                                self_public_car_id,
                                &self_target,
                                self_engine_car_id,
                            ).await;
                            break;
                        }

                        let car_dimensions = match engine
                            .read_car_dimensions_in(self_target.clone(), self_engine_car_id)
                            .await
                        {
                            Ok((width_m, depth_m)) => Some(CarDimensions { width_m, depth_m }),
                            Err(err) => {
                                tracing::warn!(
                                    stream_id,
                                    self_public_car_id,
                                    self_engine_car_id,
                                    target = ?self_target,
                                    error = %err,
                                    "participant bootstrap: failed to read car dimensions"
                                );
                                None
                            }
                        };
                        let bootstrap = ParticipantBootstrap { car_dimensions };
                        if !send_participant_event(
                            &tx,
                            &mut server_seq,
                            ParticipantServerPayload::Bootstrap(bootstrap),
                        )
                        .await
                        {
                            cleanup_participant_car(
                                "bootstrap-send-failed",
                                &engine,
                                runtime_store.as_ref(),
                                self_public_car_id,
                                &self_target,
                                self_engine_car_id,
                            )
                            .await;
                            break;
                        }
                    }
                    ParticipantClientPayload::Controls(value) => {
                        if !initialized {
                            emit_participant_terminal_error(
                                &tx,
                                Status::invalid_argument("first participant message must be init"),
                            );
                            cleanup_participant_car(
                                "controls-before-init",
                                &engine,
                                runtime_store.as_ref(),
                                self_public_car_id,
                                &self_target,
                                self_engine_car_id,
                            ).await;
                            break;
                        }

                        let controls = match proto_participant_controls_to_controls(
                            &ParticipantClientPayload::Controls(value)
                        ) {
                            Ok(Some((client_seq, controls))) => (client_seq, controls),
                            Ok(None) => continue,
                            Err(status) => {
                                emit_participant_terminal_error(&tx, status);
                                cleanup_participant_car(
                                    "invalid-controls",
                                    &engine,
                                    runtime_store.as_ref(),
                                    self_public_car_id,
                                    &self_target,
                                    self_engine_car_id,
                                ).await;
                                break;
                            }
                        };

                        let (client_seq, requested_controls) = controls;
                        let frame = frame_hub.latest();
                        let pit_state = runtime_store
                            .pit_state_snapshot(self_public_car_id, frame.server_time_ms);
                        let applied_controls = if pit_state.emergency_lock_remaining_ms > 0 {
                            Controls::new(0.0, 1.0, 0.5, 0.0, 0.0, EngineGearShift::None)
                        } else {
                            requested_controls
                        };
                        let accepted = match engine
                            .set_controls_in(
                                self_target.clone(),
                                self_engine_car_id,
                                applied_controls,
                            )
                            .await
                        {
                            Ok(value) => value,
                            Err(err) => {
                                emit_participant_terminal_error(&tx, map_worker_err(err));
                                cleanup_participant_car(
                                    "set-controls-failed",
                                    &engine,
                                    runtime_store.as_ref(),
                                    self_public_car_id,
                                    &self_target,
                                    self_engine_car_id,
                                ).await;
                                break;
                            }
                        };
                        runtime_store
                            .last_client_seq()
                            .insert(self_public_car_id, client_seq);
                        runtime_store.set_controls_input(
                            self_public_car_id,
                            requested_controls.throttle,
                            requested_controls.brake,
                            requested_controls.brake_balancer,
                            requested_controls.differential_lock,
                        );
                        let applies_from_tick = frame.tick;
                        let ack = participant_ack(
                            client_seq,
                            applies_from_tick,
                            engine_gear_shift_to_proto(accepted.accepted_shift),
                        );
                        if !send_participant_event(
                            &tx,
                            &mut server_seq,
                            ParticipantServerPayload::Ack(ack),
                        ).await {
                            cleanup_participant_car(
                                "ack-send-failed",
                                &engine,
                                runtime_store.as_ref(),
                                self_public_car_id,
                                &self_target,
                                self_engine_car_id,
                            ).await;
                            break;
                        }
                    }
                    ParticipantClientPayload::BackToTrack(command) => {
                        if !initialized {
                            emit_participant_terminal_error(
                                &tx,
                                Status::invalid_argument("first participant message must be init"),
                            );
                            cleanup_participant_car(
                                "command-before-init",
                                &engine,
                                runtime_store.as_ref(),
                                self_public_car_id,
                                &self_target,
                                self_engine_car_id,
                            )
                            .await;
                            break;
                        }

                        let frame = frame_hub.latest();
                        let applies_from_tick = frame.tick;
                        #[cfg(feature = "official")]
                        {
                            let cooldown_remaining_ms = runtime_store
                                .back_to_track_cooldown_remaining_ms(
                                    self_public_car_id,
                                    frame.server_time_ms,
                                );
                            if cooldown_remaining_ms > 0 {
                                let ack = participant_command_ack(
                                    command.client_seq,
                                    ParticipantCommandType::BackToTrack,
                                    ParticipantCommandStatus::Rejected,
                                    applies_from_tick,
                                    ParticipantCommandRejectReason::CooldownActive,
                                    cooldown_remaining_ms,
                                );

                                if !send_participant_event(
                                    &tx,
                                    &mut server_seq,
                                    ParticipantServerPayload::CommandAck(ack),
                                )
                                .await
                                {
                                    cleanup_participant_car(
                                        "command-ack-send-failed",
                                        &engine,
                                        runtime_store.as_ref(),
                                        self_public_car_id,
                                        &self_target,
                                        self_engine_car_id,
                                    )
                                    .await;
                                    break;
                                }
                                continue;
                            }
                            let in_pit = frame
                                .cars
                                .get(&self_public_car_id)
                                .map(|car| car.state.pitstop_state.is_in_any_zone())
                                .unwrap_or(false);
                            if in_pit {
                                let ack = participant_command_ack(
                                    command.client_seq,
                                    ParticipantCommandType::BackToTrack,
                                    ParticipantCommandStatus::Rejected,
                                    applies_from_tick,
                                    ParticipantCommandRejectReason::InPit,
                                    0,
                                );

                                if !send_participant_event(
                                    &tx,
                                    &mut server_seq,
                                    ParticipantServerPayload::CommandAck(ack),
                                )
                                .await
                                {
                                    cleanup_participant_car(
                                        "command-ack-send-failed",
                                        &engine,
                                        runtime_store.as_ref(),
                                        self_public_car_id,
                                        &self_target,
                                        self_engine_car_id,
                                    )
                                    .await;
                                    break;
                                }
                                continue;
                            }
                        }
                        let ack = match engine
                            .set_car_back_to_track_in(self_target.clone(), self_engine_car_id)
                            .await
                        {
                            Ok(()) => {
                                #[cfg(feature = "official")]
                                runtime_store.mark_back_to_track_applied(
                                    self_public_car_id,
                                    frame.server_time_ms,
                                );
                                participant_command_ack(
                                    command.client_seq,
                                    ParticipantCommandType::BackToTrack,
                                    ParticipantCommandStatus::Accepted,
                                    applies_from_tick,
                                    ParticipantCommandRejectReason::Unspecified,
                                    0,
                                )
                            }
                            Err(err) => {
                                tracing::warn!(
                                    stream_id,
                                    car_id = self_public_car_id,
                                    target = ?self_target,
                                    error = %err,
                                    "participant back_to_track command rejected"
                                );
                                participant_command_ack(
                                    command.client_seq,
                                    ParticipantCommandType::BackToTrack,
                                    ParticipantCommandStatus::Rejected,
                                    applies_from_tick,
                                    ParticipantCommandRejectReason::NotAllowed,
                                    0,
                                )
                            }
                        };

                        if !send_participant_event(
                            &tx,
                            &mut server_seq,
                            ParticipantServerPayload::CommandAck(ack),
                        )
                        .await
                        {
                            cleanup_participant_car(
                                "command-ack-send-failed",
                                &engine,
                                runtime_store.as_ref(),
                                self_public_car_id,
                                &self_target,
                                self_engine_car_id,
                            )
                            .await;
                            break;
                        }
                    }
                    ParticipantClientPayload::EmergencyPitstop(command) => {
                        if !initialized {
                            emit_participant_terminal_error(
                                &tx,
                                Status::invalid_argument("first participant message must be init"),
                            );
                            cleanup_participant_car(
                                "command-before-init",
                                &engine,
                                runtime_store.as_ref(),
                                self_public_car_id,
                                &self_target,
                                self_engine_car_id,
                            )
                            .await;
                            break;
                        }

                        let frame = frame_hub.latest();
                        let applies_from_tick = frame.tick;
                        #[cfg(feature = "official")]
                        {
                            let cooldown_remaining_ms = runtime_store
                                .emergency_pitstop_cooldown_remaining_ms(
                                    self_public_car_id,
                                    frame.server_time_ms,
                                );
                            if cooldown_remaining_ms > 0 {
                                let ack = participant_command_ack(
                                    command.client_seq,
                                    ParticipantCommandType::EmergencyPitstop,
                                    ParticipantCommandStatus::Rejected,
                                    applies_from_tick,
                                    ParticipantCommandRejectReason::CooldownActive,
                                    cooldown_remaining_ms,
                                );

                                if !send_participant_event(
                                    &tx,
                                    &mut server_seq,
                                    ParticipantServerPayload::CommandAck(ack),
                                )
                                .await
                                {
                                    cleanup_participant_car(
                                        "command-ack-send-failed",
                                        &engine,
                                        runtime_store.as_ref(),
                                        self_public_car_id,
                                        &self_target,
                                        self_engine_car_id,
                                    )
                                    .await;
                                    break;
                                }
                                continue;
                            }
                        }
                        let ack = match engine
                            .set_car_to_pitstop_in(self_target.clone(), self_engine_car_id)
                            .await
                        {
                            Ok(()) => {
                                #[cfg(feature = "official")]
                                runtime_store.mark_emergency_pitstop_requested(
                                    self_public_car_id,
                                    frame.server_time_ms,
                                );
                                participant_command_ack(
                                    command.client_seq,
                                    ParticipantCommandType::EmergencyPitstop,
                                    ParticipantCommandStatus::Accepted,
                                    applies_from_tick,
                                    ParticipantCommandRejectReason::Unspecified,
                                    0,
                                )
                            }
                            Err(err) => {
                                tracing::warn!(
                                    stream_id,
                                    car_id = self_public_car_id,
                                    target = ?self_target,
                                    error = %err,
                                    "participant emergency_pitstop command rejected"
                                );
                                participant_command_ack(
                                    command.client_seq,
                                    ParticipantCommandType::EmergencyPitstop,
                                    ParticipantCommandStatus::Rejected,
                                    applies_from_tick,
                                    ParticipantCommandRejectReason::NotAllowed,
                                    0,
                                )
                            }
                        };

                        if !send_participant_event(
                            &tx,
                            &mut server_seq,
                            ParticipantServerPayload::CommandAck(ack),
                        )
                        .await
                        {
                            cleanup_participant_car(
                                "command-ack-send-failed",
                                &engine,
                                runtime_store.as_ref(),
                                self_public_car_id,
                                &self_target,
                                self_engine_car_id,
                            )
                            .await;
                            break;
                        }
                    }
                    ParticipantClientPayload::SetNextPitTireType(command) => {
                        if !initialized {
                            emit_participant_terminal_error(
                                &tx,
                                Status::invalid_argument("first participant message must be init"),
                            );
                            cleanup_participant_car(
                                "command-before-init",
                                &engine,
                                runtime_store.as_ref(),
                                self_public_car_id,
                                &self_target,
                                self_engine_car_id,
                            )
                            .await;
                            break;
                        }

                        let applies_from_tick = frame_hub.latest().tick;
                        let ack = match runtime_tire_type_from_proto(command.next_tire_type) {
                            Ok(next_tire_type) => {
                                runtime_store
                                    .set_next_tire_from_bot(self_public_car_id, next_tire_type);
                                participant_command_ack(
                                    command.client_seq,
                                    ParticipantCommandType::SetNextPitTireType,
                                    ParticipantCommandStatus::Accepted,
                                    applies_from_tick,
                                    ParticipantCommandRejectReason::Unspecified,
                                    0,
                                )
                            }
                            Err(()) => participant_command_ack(
                                command.client_seq,
                                ParticipantCommandType::SetNextPitTireType,
                                ParticipantCommandStatus::Rejected,
                                applies_from_tick,
                                ParticipantCommandRejectReason::NotAllowed,
                                0,
                            ),
                        };

                        if !send_participant_event(
                            &tx,
                            &mut server_seq,
                            ParticipantServerPayload::CommandAck(ack),
                        )
                        .await
                        {
                            cleanup_participant_car(
                                "command-ack-send-failed",
                                &engine,
                                runtime_store.as_ref(),
                                self_public_car_id,
                                &self_target,
                                self_engine_car_id,
                            )
                            .await;
                            break;
                        }
                    }
                }
            }
            _ = ticker.tick() => {
                if !initialized {
                    continue;
                }

                let frame = frame_hub.latest();
                let Some(self_car) = frame.cars.get(&self_public_car_id).cloned() else {
                    emit_participant_terminal_error(
                        &tx,
                        Status::not_found("participant car is no longer active"),
                    );
                    cleanup_participant_car(
                        "self-missing",
                        &engine,
                        runtime_store.as_ref(),
                        self_public_car_id,
                        &self_target,
                        self_engine_car_id,
                    ).await;
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
                        runtime_store.as_ref(),
                        self_public_car_id,
                        &self_target,
                        self_engine_car_id,
                    ).await;
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
                        runtime_store.as_ref(),
                        self_public_car_id,
                        &self_target,
                        self_engine_car_id,
                    ).await;
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

                let snapshot = ParticipantSnapshot {
                    tick: frame.tick,
                    server_time_ms: frame.server_time_ms,
                    self_: Some(participant_self_state(
                        self_public_car_id,
                        self_car.state,
                        self_car.last_client_seq,
                        &self_car.pit_state,
                    )),
                    opponents,
                };

                if !send_participant_event(
                    &tx,
                    &mut server_seq,
                    ParticipantServerPayload::Snapshot(snapshot),
                ).await {
                    cleanup_participant_car(
                        "snapshot-send-failed",
                        &engine,
                        runtime_store.as_ref(),
                        self_public_car_id,
                        &self_target,
                        self_engine_car_id,
                    ).await;
                    break;
                }
            }
        }
    }

    tracing::info!(
        stream_id,
        self_public_car_id,
        target = ?self_target,
        "participant bidi stream ended"
    );
}
