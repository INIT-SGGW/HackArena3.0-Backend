//! Standalone local race admin service implementation.

use boink::model::ghost::GhostModeSettings as EngineGhostModeSettings;
use proto::race::v1::local_race_admin_service_server::LocalRaceAdminService;
use proto::race::v1::{
    AbortLocalRaceRequest, AbortLocalRaceResponse, CloseLocalRaceRequest, CloseLocalRaceResponse,
    CreateLocalRaceRequest, CreateLocalRaceResponse, GhostModeSettings, LocalRaceConfigInput,
    LocalRacePhase, LocalRaceRuntimeInfo, LocalTimeOfDayMode, LocalTimeOfDaySettings,
    LocalWeatherSettings, RuntimeTimeOfDayPreset, StartLocalRaceCountdownRequest,
    StartLocalRaceCountdownResponse,
};
use proto::weather::v1::WeatherType;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::local::local_race_state::{LocalRaceStateStore, current_unix_ms, timestamp_from_ms};
use crate::runtime::engine_worker::{EngineClient, EngineRuntimeTimeOfDayPreset};
use crate::services::error_map::map_worker_err;

const DEFAULT_LOCAL_RACE_MAX_PARTICIPANTS: u32 = 33;

#[derive(Clone)]
pub struct LocalRaceAdminServiceImpl {
    engine: EngineClient,
    state: LocalRaceStateStore,
}

impl LocalRaceAdminServiceImpl {
    pub fn new(engine: EngineClient, state: LocalRaceStateStore) -> Self {
        Self { engine, state }
    }
}

#[tonic::async_trait]
impl LocalRaceAdminService for LocalRaceAdminServiceImpl {
    async fn create_local_race(
        &self,
        request: Request<CreateLocalRaceRequest>,
    ) -> Result<Response<CreateLocalRaceResponse>, Status> {
        let request = request.into_inner();
        let config = request
            .config
            .ok_or_else(|| Status::invalid_argument("config is required"))?;
        let normalized = normalize_config(config)?;
        let race_id = Uuid::new_v4().to_string();
        let time_of_day_preset = resolve_time_of_day_preset(
            normalized
                .time_of_day
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("time_of_day is required"))?,
        )?;
        let engine_ghost = normalized
            .ghost_mode
            .as_ref()
            .map(engine_ghost_mode_from_proto);

        let runtime_after = self
            .engine
            .activate_local_race(
                request.expected_revision,
                race_id.clone(),
                normalized.map_id.clone(),
                time_of_day_preset,
                engine_ghost,
            )
            .await
            .map_err(map_worker_err)?;

        let race = LocalRaceRuntimeInfo {
            race_id: race_id.clone(),
            race_name: normalized.race_name,
            map_id: normalized.map_id,
            race_duration_sec: normalized.race_duration_sec,
            time_of_day: normalized.time_of_day,
            active_time_of_day_preset: runtime_time_of_day_preset_to_proto(time_of_day_preset)
                as i32,
            ghost_mode: normalized.ghost_mode,
            weather: normalized.weather,
            phase: LocalRacePhase::Staging as i32,
            created_at_utc: Some(timestamp_from_ms(current_unix_ms())),
            countdown_end_at_utc: None,
            running_started_at_utc: None,
            planned_end_at_utc: None,
            joined_participant_count: 0,
            max_participants: normalized.max_participants,
        };
        self.state.set_active_race(race.clone()).await;

        tracing::info!(
            race_id = %race_id,
            revision = runtime_after.revision,
            "standalone local race created"
        );

        Ok(Response::new(CreateLocalRaceResponse {
            revision: runtime_after.revision,
            race: Some(race),
        }))
    }

    async fn start_local_race_countdown(
        &self,
        request: Request<StartLocalRaceCountdownRequest>,
    ) -> Result<Response<StartLocalRaceCountdownResponse>, Status> {
        let request = request.into_inner();
        let runtime_before = self.engine.runtime_state().await.map_err(map_worker_err)?;
        if runtime_before.revision != request.expected_revision {
            return Err(Status::failed_precondition(format!(
                "runtime revision mismatch: expected {}, actual {}",
                request.expected_revision, runtime_before.revision
            )));
        }
        let active = self
            .state
            .active_race()
            .await
            .ok_or_else(|| Status::not_found("no active local race"))?;
        if active.race_id != request.race_id {
            return Err(Status::not_found(
                "active local race id does not match request",
            ));
        }
        if LocalRacePhase::try_from(active.phase).unwrap_or(LocalRacePhase::Unspecified)
            != LocalRacePhase::Staging
        {
            return Err(Status::failed_precondition(
                "local race countdown can be started only in staging phase",
            ));
        }
        let now_ms = current_unix_ms();
        let countdown_end_ms = now_ms.saturating_add(u64::from(request.countdown_seconds) * 1_000);
        let countdown_end = timestamp_from_ms(countdown_end_ms);

        let updated = self
            .state
            .update_active_race(&request.race_id, |race| {
                race.phase = if request.countdown_seconds == 0 {
                    LocalRacePhase::Running as i32
                } else {
                    LocalRacePhase::Countdown as i32
                };
                race.countdown_end_at_utc = Some(countdown_end.clone());
                if request.countdown_seconds == 0 {
                    race.running_started_at_utc = Some(countdown_end.clone());
                    race.planned_end_at_utc = Some(timestamp_from_ms(
                        countdown_end_ms.saturating_add(u64::from(race.race_duration_sec) * 1_000),
                    ));
                }
            })
            .await
            .map_err(map_state_err)?;
        let _ = updated;

        let runtime_after = self
            .engine
            .bump_revision(request.expected_revision)
            .await
            .map_err(map_worker_err)?;

        Ok(Response::new(StartLocalRaceCountdownResponse {
            revision: runtime_after.revision,
            race_id: request.race_id,
            countdown_end_at_utc: Some(countdown_end),
        }))
    }

    async fn abort_local_race(
        &self,
        request: Request<AbortLocalRaceRequest>,
    ) -> Result<Response<AbortLocalRaceResponse>, Status> {
        let request = request.into_inner();
        let runtime_before = self.engine.runtime_state().await.map_err(map_worker_err)?;
        if runtime_before.revision != request.expected_revision {
            return Err(Status::failed_precondition(format!(
                "runtime revision mismatch: expected {}, actual {}",
                request.expected_revision, runtime_before.revision
            )));
        }
        let updated = self
            .state
            .update_active_race(&request.race_id, |race| {
                race.phase = LocalRacePhase::Aborted as i32;
            })
            .await
            .map_err(map_state_err)?;
        let runtime_after = self
            .engine
            .deactivate_local_race(request.expected_revision, request.race_id.clone())
            .await
            .map_err(map_worker_err)?;

        tracing::info!(race_id = %updated.race_id, "standalone local race aborted");
        Ok(Response::new(AbortLocalRaceResponse {
            revision: runtime_after.revision,
            race_id: request.race_id,
        }))
    }

    async fn close_local_race(
        &self,
        request: Request<CloseLocalRaceRequest>,
    ) -> Result<Response<CloseLocalRaceResponse>, Status> {
        let request = request.into_inner();
        let race = self
            .state
            .active_race()
            .await
            .ok_or_else(|| Status::not_found("no active local race"))?;
        if race.race_id != request.race_id {
            return Err(Status::not_found(
                "active local race id does not match request",
            ));
        }
        let phase = LocalRacePhase::try_from(race.phase).unwrap_or(LocalRacePhase::Unspecified);
        if !matches!(phase, LocalRacePhase::Finished | LocalRacePhase::Aborted) {
            return Err(Status::failed_precondition(
                "local race can be closed only when finished or aborted",
            ));
        }

        let runtime_after = if race.phase == LocalRacePhase::Finished as i32 {
            self.engine
                .deactivate_local_race(request.expected_revision, request.race_id.clone())
                .await
                .map_err(map_worker_err)?
        } else {
            self.engine
                .bump_revision(request.expected_revision)
                .await
                .map_err(map_worker_err)?
        };
        self.state
            .clear_active_race(&request.race_id)
            .await
            .map_err(map_state_err)?;

        Ok(Response::new(CloseLocalRaceResponse {
            revision: runtime_after.revision,
            race_id: request.race_id,
        }))
    }
}

fn normalize_config(mut config: LocalRaceConfigInput) -> Result<LocalRaceConfigInput, Status> {
    config.race_name = config.race_name.trim().to_string();
    if config.race_name.is_empty() {
        config.race_name = "Local race".to_string();
    }
    config.map_id = config.map_id.trim().to_string();
    if config.map_id.is_empty() {
        return Err(Status::invalid_argument("map_id must be non-empty"));
    }
    if !config.map_id.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        return Err(Status::invalid_argument(
            "map_id must be an alphanumeric standalone storage key",
        ));
    }
    if config.race_duration_sec == 0 {
        return Err(Status::invalid_argument(
            "race_duration_sec must be greater than zero",
        ));
    }
    if config.time_of_day.is_none() {
        config.time_of_day = Some(LocalTimeOfDaySettings {
            mode: LocalTimeOfDayMode::FixedPreset as i32,
            fixed_preset: RuntimeTimeOfDayPreset::Noon as i32,
        });
    }
    if config.weather.is_none() {
        config.weather = Some(LocalWeatherSettings {
            weather_type: WeatherType::Clear as i32,
            temperature_c: 20,
        });
    }
    if config.max_participants == 0 {
        config.max_participants = DEFAULT_LOCAL_RACE_MAX_PARTICIPANTS;
    }
    Ok(config)
}

fn resolve_time_of_day_preset(
    value: &LocalTimeOfDaySettings,
) -> Result<EngineRuntimeTimeOfDayPreset, Status> {
    let mode = LocalTimeOfDayMode::try_from(value.mode)
        .map_err(|_| Status::invalid_argument("invalid time_of_day.mode"))?;
    if mode == LocalTimeOfDayMode::Unspecified {
        return Err(Status::invalid_argument(
            "time_of_day.mode must be specified",
        ));
    }
    let preset = RuntimeTimeOfDayPreset::try_from(value.fixed_preset)
        .map_err(|_| Status::invalid_argument("invalid time_of_day.fixed_preset"))?;
    match preset {
        RuntimeTimeOfDayPreset::Morning => Ok(EngineRuntimeTimeOfDayPreset::Morning),
        RuntimeTimeOfDayPreset::Noon => Ok(EngineRuntimeTimeOfDayPreset::Noon),
        RuntimeTimeOfDayPreset::Evening => Ok(EngineRuntimeTimeOfDayPreset::Evening),
        RuntimeTimeOfDayPreset::Night => Ok(EngineRuntimeTimeOfDayPreset::Night),
        RuntimeTimeOfDayPreset::Unspecified => Err(Status::invalid_argument(
            "time_of_day.fixed_preset must be specified",
        )),
    }
}

fn runtime_time_of_day_preset_to_proto(
    value: EngineRuntimeTimeOfDayPreset,
) -> RuntimeTimeOfDayPreset {
    match value {
        EngineRuntimeTimeOfDayPreset::Morning => RuntimeTimeOfDayPreset::Morning,
        EngineRuntimeTimeOfDayPreset::Noon => RuntimeTimeOfDayPreset::Noon,
        EngineRuntimeTimeOfDayPreset::Evening => RuntimeTimeOfDayPreset::Evening,
        EngineRuntimeTimeOfDayPreset::Night => RuntimeTimeOfDayPreset::Night,
        EngineRuntimeTimeOfDayPreset::Unspecified => RuntimeTimeOfDayPreset::Unspecified,
    }
}

fn engine_ghost_mode_from_proto(value: &GhostModeSettings) -> EngineGhostModeSettings {
    EngineGhostModeSettings {
        enabled: value.enabled,
        enter_speed_max_mps: value.enter_speed_max_mps,
        exit_speed_min_mps: value.exit_speed_min_mps,
        enter_delay_ms: value.enter_delay_ms,
        exit_delay_ms: value.exit_delay_ms,
        until_completed_laps: value.until_completed_laps,
        vehicle_overlap_exit_delay_ms: value.vehicle_overlap_exit_delay_ms,
    }
}

fn map_state_err(err: crate::local::local_race_state::LocalRaceStateError) -> Status {
    use crate::local::local_race_state::LocalRaceStateError;
    match err {
        LocalRaceStateError::NoActiveRace => Status::not_found("no active local race"),
        LocalRaceStateError::RaceMismatch => {
            Status::not_found("active local race id does not match request")
        }
        LocalRaceStateError::JoinClosed => {
            Status::failed_precondition("local race join is allowed only in staging phase")
        }
        LocalRaceStateError::ParticipantLimitReached => {
            Status::resource_exhausted("local race participant limit reached")
        }
    }
}
