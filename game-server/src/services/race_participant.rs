//! gRPC RaceParticipantService implementation (bidi participant stream).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use boink::model::Controls;
use proto::race::v1::{
    ParticipantServerEvent, ParticipantSnapshot, SpectatorView, StreamClampReason, StreamSettings,
    ViewDowngradeReason, participant_client_message::Payload as ParticipantClientPayload,
    participant_server_event::Payload as ParticipantServerPayload,
    race_participant_service_server::RaceParticipantService,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::auth::game_token::{GameTokenValidator, parse_game_token};
use crate::runtime::engine_worker::{EngineClient, EngineCommandTarget};

use super::error_map::map_worker_err;
use super::mappers::{
    engine_gear_shift_to_proto, participant_opponent_state, participant_self_state,
    proto_participant_controls_to_controls,
};
use super::race::{FrameHub, RaceRuntimeStore};

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
}

impl RaceParticipantServiceImpl {
    pub fn new(
        engine: EngineClient,
        simulation_hz: u32,
        game_token_jwks_endpoint: &str,
        jwt_audience: Vec<String>,
        jwt_issuers: Vec<String>,
        runtime_store: Arc<RaceRuntimeStore>,
        frame_hub: FrameHub,
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
        }
    }
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
        let instance_uuid = self
            .token_validator
            .instance_uuid_from_token(&token)
            .await?
            .ok_or_else(|| Status::unauthenticated("missing instance_uuid claim"))?;
        let self_public_car_id = self
            .runtime_store
            .instance_cars()
            .get(&instance_uuid)
            .map(|entry| *entry.value())
            .ok_or_else(|| Status::not_found("unknown instance_uuid"))?;
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
    controls: Controls,
    applies_from_tick: u64,
    accepted_shift: i32,
) -> proto::race::v1::ParticipantControlsAck {
    proto::race::v1::ParticipantControlsAck {
        client_seq,
        applies_from_tick,
        accepted_shift,
        accepted_throttle: controls.throttle,
        accepted_brake: controls.brake,
        accepted_steering: controls.steer,
    }
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

                        let (client_seq, controls) = controls;
                        let accepted = match engine
                            .set_controls_in(self_target.clone(), self_engine_car_id, controls)
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
                        let applies_from_tick = frame_hub.latest().tick;
                        let ack = participant_ack(
                            client_seq,
                            controls,
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
