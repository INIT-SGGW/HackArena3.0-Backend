//! gRPC RaceParticipantService implementation (bidi participant stream).

#[cfg(feature = "official")]
use std::collections::HashSet;
#[cfg(feature = "official")]
use std::path::{Path, PathBuf};
#[cfg(feature = "official")]
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
#[cfg(feature = "official")]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "official")]
use boink::model::TyreType;
#[cfg(feature = "official")]
use boink::model::ghost::GhostModeSettings;
use boink::model::{Controls, GearShift as EngineGearShift};
#[cfg(feature = "official")]
use dashmap::DashMap;
use proto::race::v1::{
    CarDimensions, LocalRaceJoinRequest, LocalRaceJoinResponse, LocalSandboxJoinRequest,
    LocalSandboxJoinResponse, ParticipantBootstrap, ParticipantCommandAck,
    ParticipantCommandRejectReason, ParticipantCommandStatus, ParticipantCommandType,
    ParticipantServerEvent, ParticipantSnapshot, PrepareOfficialJoinRequest,
    PrepareOfficialJoinResponse, SpectatorView, StreamClampReason, StreamSettings,
    TireType as ProtoTireType, ViewDowngradeReason,
    participant_client_message::Payload as ParticipantClientPayload,
    participant_server_event::Payload as ParticipantServerPayload,
    race_participant_service_server::RaceParticipantService,
};
#[cfg(feature = "local")]
use rand::Rng;
#[cfg(feature = "official")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "official")]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(feature = "official")]
use tokio::process::Command;
#[cfg(feature = "official")]
use tokio::sync::Mutex;
#[cfg(feature = "official")]
use tokio::sync::broadcast;
use tokio::sync::mpsc;
#[cfg(feature = "official")]
use tokio::sync::oneshot;
#[cfg(feature = "official")]
use tokio::task::JoinHandle;
#[cfg(feature = "official")]
use tokio::time::MissedTickBehavior;
#[cfg(feature = "official")]
use tokio::{fs, select};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::auth::game_token::GameTokenValidator;
#[cfg(not(feature = "standalone"))]
use crate::auth::game_token::parse_game_token;
#[cfg(feature = "official")]
use crate::db::repos::race_config::{RaceConfigRecord, RaceConfigRepo};
#[cfg(feature = "official")]
use crate::db::repos::submission::SubmissionRepo;
#[cfg(feature = "local")]
use crate::local::local_race_state::{LocalRaceStateError, LocalRaceStateStore};
#[cfg(feature = "local")]
use crate::local::sandbox_config_store::{LocalSandboxConfigStore, LocalSandboxSpawnModeRecord};
#[cfg(feature = "local")]
use crate::runtime::engine_worker::{EngineActiveSandboxState, EngineRuntimeState};
#[cfg(feature = "official")]
use crate::runtime::engine_worker::{EngineActivityKind, EngineRuntimeTimeOfDayPreset};
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
    remove_bot_container, start_bot_container, start_bot_container_with_options,
    wait_bot_container_exit_code,
};

const PARTICIPANT_REQUESTED_HZ: u32 = 30;
const MIN_STREAM_HZ: u32 = 1;
const MAX_STREAM_HZ: u32 = 120;
const PARTICIPANT_STREAM_CHANNEL_CAPACITY: usize = 1;
const PARTICIPANT_DESPAWN_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(feature = "official")]
const OFFICIAL_RACE_LAUNCHER_TICK_MS: u64 = 200;
#[cfg(feature = "official")]
const OFFICIAL_RACE_GHOST_MODE_FILE: &str = "official-race-ghost-mode.json";
#[cfg(feature = "official")]
const OFFICIAL_RACE_PREPARE_GRID_PIN_INTERVAL_MS: u64 = 500;
#[cfg(feature = "official")]
const OFFICIAL_RACE_BOT_AUTO_RESTART_MAX_ATTEMPTS: u32 = 12;
#[cfg(feature = "official")]
const OFFICIAL_RACE_BOT_AUTO_RESTART_BASE_DELAY_MS: u64 = 500;
#[cfg(feature = "official")]
const OFFICIAL_RACE_BOT_PRESTART_RESTART_DELAY_MS: u64 = 1_000;
#[cfg(feature = "official")]
const OFFICIAL_RACE_BOT_EXIT_LOG_TAIL_LINES: u32 = 40;
#[cfg(feature = "official")]
const SUBMISSIONS_ROOT: &str = ".submissions";
#[cfg(feature = "official")]
const TEAM_LOGS_SUBDIR: &str = "logs";

#[cfg(feature = "official")]
#[derive(Debug, Default, Clone, Copy)]
struct OfficialRaceStartStats {
    total_listed: usize,
    started: usize,
    skipped_grid_overflow: usize,
    skipped_duplicate_team: usize,
    skipped_missing_submission: usize,
    skipped_runtime_error: usize,
    skipped_token_error: usize,
    skipped_container_error: usize,
}

#[cfg(feature = "official")]
#[derive(Debug, Clone, Serialize)]
struct OfficialRaceTeamResultEntry {
    team_id: String,
    roster_index: u32,
    start_status: String,
    slot_index: Option<i16>,
    submission_id: Option<String>,
    car_id: Option<u64>,
    container_id: Option<String>,
    completed_laps: u32,
    current_lap_distance_m: Option<f32>,
    total_distance_m: Option<f32>,
    last_lap_time_ms: Option<u32>,
    best_lap_time_ms: Option<u32>,
    lap_times_ms: Vec<u32>,
    has_started_moving: bool,
    #[serde(skip_serializing)]
    last_recorded_completed_laps: u32,
    #[serde(skip_serializing)]
    initial_lap_progress_m: Option<f32>,
}

#[cfg(feature = "official")]
#[derive(Debug, Clone, Serialize)]
struct OfficialRaceResultsFile {
    session_id: String,
    race_id: String,
    race_name: String,
    map_id: String,
    started_at_unix_ms: u64,
    finalized_at_unix_ms: Option<u64>,
    status: String,
    teams: Vec<OfficialRaceTeamResultEntry>,
}

#[cfg(feature = "official")]
#[derive(Debug, Clone)]
struct OfficialRaceLaunchResult {
    stats: OfficialRaceStartStats,
    teams: Vec<OfficialRaceTeamResultEntry>,
}

#[cfg(feature = "official")]
struct OfficialRaceResultsRecorder {
    session_file_path: PathBuf,
    latest_file_path: PathBuf,
    next_write_after_ms: u64,
    data: OfficialRaceResultsFile,
}

#[cfg(feature = "official")]
#[derive(Debug, Clone, Deserialize)]
struct OfficialRaceGhostModeSettingsFile {
    enabled: bool,
    enter_speed_max_mps: f32,
    exit_speed_min_mps: f32,
    enter_delay_ms: u32,
    exit_delay_ms: u32,
    until_completed_laps: u32,
    vehicle_overlap_exit_delay_ms: u32,
}

#[cfg(feature = "official")]
impl OfficialRaceResultsRecorder {
    fn new(
        assets_dir: &Path,
        race_config: &RaceConfigRecord,
        launch: OfficialRaceLaunchResult,
        now_ms: u64,
    ) -> Self {
        let results_dir = assets_dir.join("official-race-results");
        let session_id = format!(
            "{}_{}",
            now_ms,
            race_config
                .race_id
                .chars()
                .map(|ch| {
                    if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                        ch
                    } else {
                        '_'
                    }
                })
                .collect::<String>()
        );
        let data = OfficialRaceResultsFile {
            session_id: session_id.clone(),
            race_id: race_config.race_id.clone(),
            race_name: race_config.config.race_name.clone(),
            map_id: race_config.config.map_id.clone(),
            started_at_unix_ms: now_ms,
            finalized_at_unix_ms: None,
            status: "running".to_string(),
            teams: launch.teams,
        };
        Self {
            session_file_path: results_dir.join(format!("{session_id}.json")),
            latest_file_path: results_dir.join("latest.json"),
            next_write_after_ms: now_ms,
            data,
        }
    }

    async fn tick(&mut self, service: &RaceParticipantServiceImpl) {
        self.refresh_from_runtime(service);
        let now_ms = current_unix_ms();
        if now_ms >= self.next_write_after_ms {
            self.next_write_after_ms = now_ms.saturating_add(1_000);
            if let Err(err) = self.write_snapshot().await {
                tracing::warn!(error = %err, "official race results recorder: failed to write snapshot");
            }
        }
    }

    async fn finalize(&mut self) {
        self.data.status = "finalized".to_string();
        self.data.finalized_at_unix_ms = Some(current_unix_ms());
        if let Err(err) = self.write_snapshot().await {
            tracing::warn!(error = %err, "official race results recorder: failed to write final snapshot");
        }
    }

    fn refresh_from_runtime(&mut self, service: &RaceParticipantServiceImpl) {
        let frame = service.frame_hub.latest();
        let lap_length_m = frame.official_lap_length_m.unwrap_or(0.0).max(0.0);

        for team in &mut self.data.teams {
            let Some(car_id) = team.car_id else {
                continue;
            };
            let Some(car_frame) = frame.cars.get(&car_id) else {
                continue;
            };
            if !matches!(car_frame.target, EngineCommandTarget::OfficialRace) {
                continue;
            }
            let Some(metrics) = car_frame.race_metrics else {
                continue;
            };

            team.completed_laps = metrics.completed_laps;
            let lap_progress_m = metrics.lap_progress_m.max(0.0);
            let baseline = match team.initial_lap_progress_m {
                Some(value) => value,
                None => {
                    team.initial_lap_progress_m = Some(lap_progress_m);
                    lap_progress_m
                }
            };
            let moved_from_baseline = (lap_progress_m - baseline).abs() > 1.0;
            if metrics.completed_laps > 0 || moved_from_baseline {
                team.has_started_moving = true;
            }
            if team.has_started_moving {
                team.current_lap_distance_m = Some(lap_progress_m);
                team.total_distance_m =
                    Some(metrics.completed_laps as f32 * lap_length_m + lap_progress_m);
            } else {
                team.current_lap_distance_m = None;
                team.total_distance_m = None;
            }
            team.last_lap_time_ms = metrics.last_lap_time_ms;
            team.best_lap_time_ms = car_frame.best_lap_time_ms;

            if metrics.completed_laps > team.last_recorded_completed_laps {
                if let Some(last_lap_time_ms) = metrics.last_lap_time_ms {
                    team.lap_times_ms.push(last_lap_time_ms);
                }
                team.last_recorded_completed_laps = metrics.completed_laps;
            }
        }
    }

    async fn write_snapshot(&self) -> anyhow::Result<()> {
        let payload = serde_json::to_vec_pretty(&self.data)?;
        write_json_atomic(&self.session_file_path, &payload).await?;
        write_json_atomic(&self.latest_file_path, &payload).await?;
        Ok(())
    }
}

#[cfg(feature = "official")]
fn current_unix_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(feature = "official")]
async fn write_json_atomic(path: &Path, payload: &[u8]) -> anyhow::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "failed to write race results JSON: path has no parent ({})",
            path.display()
        )
    })?;
    fs::create_dir_all(parent).await?;

    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, payload).await?;

    if let Err(initial_err) = fs::rename(&tmp_path, path).await {
        if fs::try_exists(path).await.unwrap_or(false) {
            let _ = fs::remove_file(path).await;
            fs::rename(&tmp_path, path).await.map_err(|replace_err| {
                anyhow::anyhow!(
                    "failed to atomically replace race results JSON {} (initial: {}, replace: {})",
                    path.display(),
                    initial_err,
                    replace_err
                )
            })?;
        } else {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(anyhow::anyhow!(
                "failed to finalize race results JSON {}: {}",
                path.display(),
                initial_err
            ));
        }
    }

    Ok(())
}

#[cfg(feature = "official")]
async fn read_bot_container_logs_tail(container_id: &str, tail_lines: u32) -> Option<String> {
    let mut command = Command::new("docker");
    command
        .arg("logs")
        .arg("--tail")
        .arg(tail_lines.to_string())
        .arg(container_id);
    let output = command.output().await.ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let combined = match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => return None,
        (false, true) => stdout,
        (true, false) => stderr,
        (false, false) => format!("{stdout}\n{stderr}"),
    };
    let max_chars = 4_000usize;
    if combined.chars().count() <= max_chars {
        return Some(combined);
    }
    let truncated: String = combined
        .chars()
        .rev()
        .take(max_chars)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    Some(format!("...{truncated}"))
}

#[cfg(feature = "official")]
fn sanitize_storage_component(value: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    let trimmed = sanitized.trim_matches('_');
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(feature = "official")]
async fn stream_official_race_bot_logs_to_file<R>(
    stream: R,
    writer: Arc<Mutex<fs::File>>,
    channel: &'static str,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                let mut guard = writer.lock().await;
                if let Err(err) = guard
                    .write_all(format!("[{channel}] {line}\n").as_bytes())
                    .await
                {
                    tracing::warn!(
                        channel,
                        error = %err,
                        "official race bot log capture: failed writing line"
                    );
                    break;
                }
                if let Err(err) = guard.flush().await {
                    tracing::warn!(
                        channel,
                        error = %err,
                        "official race bot log capture: failed flushing log file"
                    );
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                tracing::warn!(
                    channel,
                    error = %err,
                    "official race bot log capture: failed reading docker logs stream"
                );
                let mut guard = writer.lock().await;
                let _ = guard
                    .write_all(format!("[{channel}] <stream read error: {err}>\n").as_bytes())
                    .await;
                let _ = guard.flush().await;
                break;
            }
        }
    }
}

#[derive(Clone)]
pub struct RaceParticipantServiceImpl {
    engine: EngineClient,
    simulation_hz: u32,
    runtime_store: Arc<RaceRuntimeStore>,
    frame_hub: FrameHub,
    #[cfg_attr(feature = "standalone", allow(dead_code))]
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
    #[cfg(feature = "official")]
    submissions_root: PathBuf,
    #[cfg(feature = "official")]
    log_capture_tasks: Arc<DashMap<String, JoinHandle<()>>>,
    #[cfg(feature = "local")]
    local_sandbox_store: LocalSandboxConfigStore,
    #[cfg(feature = "local")]
    local_race_state: LocalRaceStateStore,
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
        #[cfg(feature = "local")] local_race_state: LocalRaceStateStore,
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
            #[cfg(feature = "official")]
            submissions_root: PathBuf::from(SUBMISSIONS_ROOT),
            #[cfg(feature = "official")]
            log_capture_tasks: Arc::new(DashMap::new()),
            #[cfg(feature = "local")]
            local_sandbox_store,
            #[cfg(feature = "local")]
            local_race_state,
        }
    }
}

impl RaceParticipantServiceImpl {
    #[cfg(feature = "official")]
    fn team_logs_dir(&self, team_id: &str) -> PathBuf {
        self.submissions_root
            .join(sanitize_storage_component(team_id))
            .join(TEAM_LOGS_SUBDIR)
    }

    #[cfg(feature = "official")]
    fn build_official_race_bot_log_path(
        &self,
        team_id: &str,
        submission_id: &str,
        slot_index: i16,
        container_id: &str,
    ) -> PathBuf {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        let team = sanitize_storage_component(team_id);
        let submission = sanitize_storage_component(submission_id);
        let container_short = sanitize_storage_component(
            container_id.trim().get(..12).unwrap_or(container_id.trim()),
        );
        let file_name = format!(
            "{ts_ms}_team-{team}_submission-{submission}_sandbox-official-race_slot-{slot_index}_container-{container_short}.log"
        );
        self.team_logs_dir(team_id).join(file_name)
    }

    #[cfg(feature = "official")]
    async fn stop_official_race_log_capture_for_team(&self, team_id: &str) {
        let Some((_, handle)) = self.log_capture_tasks.remove(team_id) else {
            return;
        };
        if !handle.is_finished() {
            handle.abort();
        }
        match handle.await {
            Ok(()) => {}
            Err(err) if err.is_cancelled() => {}
            Err(err) => {
                tracing::warn!(
                    team_id = %team_id,
                    error = %err,
                    "official race bot log capture task ended with join error"
                );
            }
        }
    }

    #[cfg(feature = "official")]
    async fn start_official_race_bot_log_capture(
        &self,
        team_id: &str,
        submission_id: &str,
        slot_index: i16,
        container_id: &str,
    ) -> PathBuf {
        let logs_dir = self.team_logs_dir(team_id);
        if let Err(err) = fs::create_dir_all(&logs_dir).await {
            tracing::warn!(
                team_id = %team_id,
                path = %logs_dir.display(),
                error = %err,
                "official race bot log capture: failed to create logs directory"
            );
        }

        let log_file_path =
            self.build_official_race_bot_log_path(team_id, submission_id, slot_index, container_id);
        let mut log_file = match fs::File::create(&log_file_path).await {
            Ok(file) => file,
            Err(err) => {
                tracing::warn!(
                    team_id = %team_id,
                    log_file = %log_file_path.display(),
                    error = %err,
                    "official race bot log capture: failed to create log file"
                );
                return log_file_path;
            }
        };

        let started_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        let started_at_utc = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "unknown".to_string());
        let header = format!(
            "# team_id={team_id}\n# submission_id={submission_id}\n# sandbox_id=official-race\n# slot_index={slot_index}\n# container_id={container_id}\n# started_at_unix_ms={started_at_ms}\n# started_at_utc={started_at_utc}\n"
        );
        if let Err(err) = log_file.write_all(header.as_bytes()).await {
            tracing::warn!(
                team_id = %team_id,
                log_file = %log_file_path.display(),
                error = %err,
                "official race bot log capture: failed to write log header"
            );
            return log_file_path;
        }
        if let Err(err) = log_file.flush().await {
            tracing::warn!(
                team_id = %team_id,
                log_file = %log_file_path.display(),
                error = %err,
                "official race bot log capture: failed to flush log header"
            );
            return log_file_path;
        }

        let writer = Arc::new(Mutex::new(log_file));
        let mut command = Command::new("docker");
        command
            .arg("logs")
            .arg("-f")
            .arg("--timestamps")
            .arg(container_id)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(err) => {
                tracing::warn!(
                    team_id = %team_id,
                    container_id = %container_id,
                    error = %err,
                    "official race bot log capture: failed to start docker logs follow process"
                );
                let mut guard = writer.lock().await;
                let _ = guard
                    .write_all(format!("# docker/logs follow failed error={err}\n").as_bytes())
                    .await;
                let _ = guard.flush().await;
                return log_file_path;
            }
        };

        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill().await;
            tracing::warn!(
                team_id = %team_id,
                container_id = %container_id,
                "official race bot log capture: docker/logs did not provide stdout stream"
            );
            return log_file_path;
        };
        let Some(stderr) = child.stderr.take() else {
            let _ = child.kill().await;
            tracing::warn!(
                team_id = %team_id,
                container_id = %container_id,
                "official race bot log capture: docker/logs did not provide stderr stream"
            );
            return log_file_path;
        };

        let writer_for_task = writer.clone();
        let team_id_for_task = team_id.to_string();
        let container_id_for_task = container_id.to_string();
        let log_file_path_for_task = log_file_path.clone();
        let stdout_task = tokio::spawn(stream_official_race_bot_logs_to_file(
            stdout,
            writer.clone(),
            "stdout",
        ));
        let stderr_task = tokio::spawn(stream_official_race_bot_logs_to_file(
            stderr, writer, "stderr",
        ));

        let capture_task = tokio::spawn(async move {
            let wait_result = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            let mut guard = writer_for_task.lock().await;
            match wait_result {
                Ok(status) => {
                    let _ = guard
                        .write_all(
                            format!("# docker/logs follow ended status={:?}\n", status.code())
                                .as_bytes(),
                        )
                        .await;
                    let _ = guard.flush().await;
                    tracing::info!(
                        team_id = %team_id_for_task,
                        container_id = %container_id_for_task,
                        log_file = %log_file_path_for_task.display(),
                        status = ?status.code(),
                        "official race bot log capture finished"
                    );
                }
                Err(err) => {
                    let _ = guard
                        .write_all(format!("# docker/logs follow failed error={err}\n").as_bytes())
                        .await;
                    let _ = guard.flush().await;
                    tracing::warn!(
                        team_id = %team_id_for_task,
                        container_id = %container_id_for_task,
                        log_file = %log_file_path_for_task.display(),
                        error = %err,
                        "official race bot log capture process failed"
                    );
                }
            }
        });

        if let Some(previous_task) = self
            .log_capture_tasks
            .insert(team_id.to_string(), capture_task)
        {
            previous_task.abort();
        }

        tracing::info!(
            team_id = %team_id,
            submission_id = %submission_id,
            slot_index,
            container_id = %container_id,
            log_file = %log_file_path.display(),
            "official race bot log capture started"
        );
        log_file_path
    }

    #[cfg(feature = "local")]
    async fn local_sandbox_join_impl(
        &self,
        requested_sandbox_id: String,
        auth: Option<String>,
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
        #[cfg(not(feature = "standalone"))]
        {
            let auth = auth.ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
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
        }
        #[cfg(feature = "standalone")]
        let _ = auth;
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
    async fn local_race_join_impl(
        &self,
        race_id: String,
        display_name: String,
    ) -> Result<LocalRaceJoinResponse, Status> {
        let race_id = race_id.trim().to_string();
        if race_id.is_empty() {
            return Err(Status::invalid_argument("race_id must be non-empty"));
        }
        let display_name = {
            let value = display_name.trim();
            if value.is_empty() {
                "Local bot".to_string()
            } else {
                value.chars().take(64).collect()
            }
        };
        let active_race = self
            .local_race_state
            .active_race()
            .await
            .ok_or_else(|| Status::failed_precondition("local race runtime is not active"))?;
        if active_race.race_id != race_id {
            return Err(Status::not_found(
                "local race runtime is not active for requested race_id",
            ));
        }

        let target = EngineCommandTarget::LocalRace {
            race_id: race_id.clone(),
        };
        let engine_car_id = self
            .engine
            .spawn_local_race_car(race_id.clone())
            .await
            .map_err(map_worker_err)?;
        let public_car_id = self.runtime_store.allocate_public_car_id();

        let participant = match self
            .local_race_state
            .register_participant(&race_id, public_car_id, display_name.clone())
            .await
        {
            Ok(participant) => participant,
            Err(err) => {
                if let Err(despawn_err) = self
                    .engine
                    .despawn_car_in(target.clone(), engine_car_id)
                    .await
                {
                    tracing::warn!(
                        race_id = %race_id,
                        engine_car_id,
                        error = %despawn_err,
                        "failed to despawn local race car after join rejection"
                    );
                }
                return Err(map_local_race_state_err(err));
            }
        };

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
        let start_slot = u64::from(participant.participant_index)
            .saturating_sub(1)
            .rem_euclid(slots)
            .saturating_add(1);
        if let Err(status) = self
            .engine
            .set_car_at_start_pos_in(target.clone(), engine_car_id, start_slot)
            .await
            .map_err(map_worker_err)
        {
            if let Err(err) = self
                .engine
                .despawn_car_in(target.clone(), engine_car_id)
                .await
            {
                tracing::warn!(
                    race_id = %race_id,
                    engine_car_id,
                    error = %err,
                    "failed to despawn local race car after start-position failure"
                );
            }
            self.local_race_state
                .remove_participant(public_car_id)
                .await;
            return Err(status);
        }

        let mut identity = RuntimeCarIdentity::default();
        identity.local_race_display_name = Some(display_name);
        identity.local_race_participant_index = Some(participant.participant_index);
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

        Ok(LocalRaceJoinResponse {
            car_id: public_car_id,
            race_id,
            map_id: active_race.map_id,
            participant_index: participant.participant_index,
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
    async fn try_restart_official_race_bot_after_exit_locked(
        &self,
        team_id: &str,
        current: &TeamOfficialRaceBotState,
    ) -> Result<Option<String>, Status> {
        let race_started = self.runtime_store.is_official_race_started();
        if race_started
            && current.auto_restart_attempts >= OFFICIAL_RACE_BOT_AUTO_RESTART_MAX_ATTEMPTS
        {
            return Ok(None);
        }
        let runtime_state = self.engine.runtime_state().await.map_err(map_worker_err)?;
        if !matches!(
            runtime_state.activity_kind,
            EngineActivityKind::OfficialRace
        ) {
            return Ok(None);
        }

        let team_bot_token = self.game_token_issuer.issue_team_bot_token(team_id).await?;
        let wrapper_auth_token = self
            .wrapper_auth_token_issuer
            .issue_wrapper_auth_token()
            .await?;
        let container_id = start_bot_container_with_options(
            &current.image_ref,
            &current.container_name,
            &self.wrapper_backend_endpoint,
            &team_bot_token,
            &wrapper_auth_token,
            team_id,
            &current.submission_id,
            "official-race",
            current.slot_index,
            false,
        )
        .await
        .map_err(|err| {
            Status::internal(format!(
                "failed to restart official-race bot container after exit: {err}"
            ))
        })?;
        let log_file_path = self
            .start_official_race_bot_log_capture(
                team_id,
                &current.submission_id,
                current.slot_index,
                &container_id,
            )
            .await;

        self.official_race_bots.insert(
            team_id.to_string(),
            TeamOfficialRaceBotState {
                public_car_id: current.public_car_id,
                engine_car_id: current.engine_car_id,
                start_position_index: current.start_position_index,
                slot_index: current.slot_index,
                container_name: current.container_name.clone(),
                container_id: container_id.clone(),
                submission_id: current.submission_id.clone(),
                image_ref: current.image_ref.clone(),
                log_file_path,
                auto_restart_attempts: if race_started {
                    current.auto_restart_attempts.saturating_add(1)
                } else {
                    0
                },
            },
        );
        self.runtime_store
            .set_active_bot_slot(current.public_car_id, current.slot_index);
        let _ = self.slot_updates_tx.send(team_id.to_string());
        Ok(Some(container_id))
    }

    #[cfg(feature = "official")]
    async fn refresh_official_race_bots_before_start_locked(&self) -> (usize, usize) {
        let bots: Vec<(String, TeamOfficialRaceBotState)> = self
            .official_race_bots
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        let mut restarted = 0usize;
        let mut failed = 0usize;

        for (team_id, current) in bots {
            let restart_result = async {
                let team_bot_token = self
                    .game_token_issuer
                    .issue_team_bot_token(&team_id)
                    .await?;
                let wrapper_auth_token = self
                    .wrapper_auth_token_issuer
                    .issue_wrapper_auth_token()
                    .await?;
                start_bot_container_with_options(
                    &current.image_ref,
                    &current.container_name,
                    &self.wrapper_backend_endpoint,
                    &team_bot_token,
                    &wrapper_auth_token,
                    &team_id,
                    &current.submission_id,
                    "official-race",
                    current.slot_index,
                    false,
                )
                .await
                .map_err(|err| {
                    Status::internal(format!(
                        "failed to refresh official-race bot container for start: {err}"
                    ))
                })
            }
            .await;

            match restart_result {
                Ok(container_id) => {
                    let log_file_path = self
                        .start_official_race_bot_log_capture(
                            &team_id,
                            &current.submission_id,
                            current.slot_index,
                            &container_id,
                        )
                        .await;
                    self.official_race_bots.insert(
                        team_id.clone(),
                        TeamOfficialRaceBotState {
                            public_car_id: current.public_car_id,
                            engine_car_id: current.engine_car_id,
                            start_position_index: current.start_position_index,
                            slot_index: current.slot_index,
                            container_name: current.container_name.clone(),
                            container_id: container_id.clone(),
                            submission_id: current.submission_id.clone(),
                            image_ref: current.image_ref.clone(),
                            log_file_path,
                            auto_restart_attempts: 0,
                        },
                    );
                    self.runtime_store
                        .set_active_bot_slot(current.public_car_id, current.slot_index);
                    let _ = self.slot_updates_tx.send(team_id.clone());
                    self.spawn_official_race_bot_exit_monitor(
                        team_id.clone(),
                        container_id.clone(),
                    );
                    restarted = restarted.saturating_add(1);
                    tracing::info!(
                        team_id = %team_id,
                        public_car_id = current.public_car_id,
                        engine_car_id = current.engine_car_id,
                        slot_index = current.slot_index,
                        container_id = %container_id,
                        "official race launcher: refreshed bot container for race start"
                    );
                }
                Err(status) => {
                    failed = failed.saturating_add(1);
                    tracing::warn!(
                        team_id = %team_id,
                        public_car_id = current.public_car_id,
                        engine_car_id = current.engine_car_id,
                        slot_index = current.slot_index,
                        code = ?status.code(),
                        error = %status,
                        "official race launcher: failed to refresh bot container for race start"
                    );
                }
            }
        }

        (restarted, failed)
    }

    #[cfg(feature = "official")]
    async fn force_official_race_hard_tyres(&self) -> (usize, usize) {
        let bots: Vec<(String, TeamOfficialRaceBotState)> = self
            .official_race_bots
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        let mut forced = 0usize;
        let mut failed = 0usize;

        for (team_id, bot) in bots {
            match self
                .engine
                .force_set_car_tyre_type_in(
                    EngineCommandTarget::OfficialRace,
                    bot.engine_car_id,
                    TyreType::Hard,
                )
                .await
            {
                Ok(()) => {
                    forced = forced.saturating_add(1);
                    self.runtime_store
                        .set_next_tire_from_frontend(bot.public_car_id, RuntimePitTireType::Hard);
                }
                Err(err) => {
                    failed = failed.saturating_add(1);
                    tracing::warn!(
                        team_id = %team_id,
                        public_car_id = bot.public_car_id,
                        engine_car_id = bot.engine_car_id,
                        error = %err,
                        "official race launcher: failed to force HARD tyre at race start"
                    );
                }
            }
        }

        (forced, failed)
    }

    #[cfg(feature = "official")]
    async fn handle_unrecoverable_official_race_bot_exit(
        &self,
        team_id: &str,
        current: &TeamOfficialRaceBotState,
        exited_container_id: &str,
        exit_code: i32,
    ) -> bool {
        if !self.runtime_store.is_official_race_started() {
            return false;
        }

        let runtime_target = self.runtime_store.car_target(current.public_car_id);
        let runtime_engine_id = self.runtime_store.car_engine_id(current.public_car_id);
        let runtime_matches = matches!(runtime_target, Some(EngineCommandTarget::OfficialRace))
            && runtime_engine_id == Some(current.engine_car_id);
        if !runtime_matches {
            return false;
        }

        if let Some(mut entry) = self.official_race_bots.get_mut(team_id) {
            if entry.container_id != exited_container_id {
                return true;
            }
            entry.container_id = format!(
                "awaiting-slot-switch-{}",
                self.frame_hub.latest().server_time_ms
            );
            entry.auto_restart_attempts = 0;
        } else {
            return true;
        }
        self.stop_official_race_log_capture_for_team(team_id).await;

        if let Err(err) = remove_bot_container(&current.container_name).await {
            tracing::warn!(
                team_id = %team_id,
                container_name = %current.container_name,
                error = %err,
                "official race bot exit monitor: failed to remove exited bot container before fallback switch"
            );
        }

        let now_ms = self.frame_hub.latest().server_time_ms;
        match self
            .engine
            .set_car_to_pitstop_in(EngineCommandTarget::OfficialRace, current.engine_car_id)
            .await
        {
            Ok(()) => {
                self.runtime_store
                    .mark_emergency_pitstop_requested(current.public_car_id, now_ms);
            }
            Err(err) => {
                tracing::warn!(
                    team_id = %team_id,
                    public_car_id = current.public_car_id,
                    engine_car_id = current.engine_car_id,
                    error = %err,
                    "official race bot exit monitor: failed to send car to emergency pit for fallback slot switch"
                );
            }
        }

        self.runtime_store
            .clear_active_bot_slot(current.public_car_id);
        let _ = self.slot_updates_tx.send(team_id.to_string());
        tracing::warn!(
            team_id = %team_id,
            public_car_id = current.public_car_id,
            engine_car_id = current.engine_car_id,
            slot_index = current.slot_index,
            container_name = %current.container_name,
            exited_container_id = %exited_container_id,
            exit_code,
            "official race bot exit monitor: switched to emergency pit awaiting manual bot slot change"
        );
        true
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
            if exit_code != 0
                && let Some(tail) = read_bot_container_logs_tail(
                    &container_id,
                    OFFICIAL_RACE_BOT_EXIT_LOG_TAIL_LINES,
                )
                .await
            {
                tracing::warn!(
                    team_id = %team_id,
                    container_id = %container_id,
                    exit_code,
                    log_tail = %tail,
                    "official race bot container exited with non-zero code; docker logs tail"
                );
            }

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
            let delay_ms = if service.runtime_store.is_official_race_started() {
                let shift = current.auto_restart_attempts.min(4);
                let multiplier = 1_u64 << shift;
                OFFICIAL_RACE_BOT_AUTO_RESTART_BASE_DELAY_MS.saturating_mul(multiplier)
            } else {
                OFFICIAL_RACE_BOT_PRESTART_RESTART_DELAY_MS
            };
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;

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
            match service
                .try_restart_official_race_bot_after_exit_locked(&team_id, &current)
                .await
            {
                Ok(Some(restarted_container_id)) => {
                    tracing::warn!(
                        team_id = %team_id,
                        public_car_id = current.public_car_id,
                        engine_car_id = current.engine_car_id,
                        previous_container_id = %container_id,
                        restarted_container_id = %restarted_container_id,
                        restart_attempt = current.auto_restart_attempts.saturating_add(1),
                        max_restart_attempts = OFFICIAL_RACE_BOT_AUTO_RESTART_MAX_ATTEMPTS,
                        exit_code,
                        "official race bot exit monitor: auto-restarted wrapper container after unexpected exit"
                    );
                    service.spawn_official_race_bot_exit_monitor(team_id, restarted_container_id);
                    return;
                }
                Ok(None) => {}
                Err(status) => {
                    tracing::warn!(
                        team_id = %team_id,
                        public_car_id = current.public_car_id,
                        engine_car_id = current.engine_car_id,
                        container_id = %container_id,
                        restart_attempt = current.auto_restart_attempts.saturating_add(1),
                        max_restart_attempts = OFFICIAL_RACE_BOT_AUTO_RESTART_MAX_ATTEMPTS,
                        code = ?status.code(),
                        error = %status,
                        "official race bot exit monitor: failed to auto-restart wrapper container"
                    );
                }
            }
            if service
                .handle_unrecoverable_official_race_bot_exit(
                    &team_id,
                    &current,
                    &container_id,
                    exit_code,
                )
                .await
            {
                return;
            }
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
            service
                .stop_official_race_log_capture_for_team(&team_id)
                .await;
            service.official_race_bots.remove(&team_id);
            service
                .runtime_store
                .clear_active_bot_slot(current.public_car_id);
            let _ = service.slot_updates_tx.send(team_id.clone());
            match service
                .engine
                .despawn_car_in(EngineCommandTarget::OfficialRace, current.engine_car_id)
                .await
            {
                Ok(()) => {}
                Err(err) => {
                    tracing::warn!(
                        team_id = %team_id,
                        public_car_id = current.public_car_id,
                        engine_car_id = current.engine_car_id,
                        error = %err,
                        "official race bot exit monitor: failed to despawn car after bot exit"
                    );
                }
            }
            service.runtime_store.remove_car(current.public_car_id);
            tracing::warn!(
                team_id = %team_id,
                public_car_id = current.public_car_id,
                engine_car_id = current.engine_car_id,
                container_id = %container_id,
                "official race bot exit monitor: removed runtime car after bot container exit"
            );
        });
    }

    #[cfg(feature = "official")]
    async fn reconcile_official_race_bot_liveness(&self, team_id: &str) {
        let Some(current) = self
            .official_race_bots
            .get(team_id)
            .map(|entry| entry.value().clone())
        else {
            return;
        };

        let runtime_target = self.runtime_store.car_target(current.public_car_id);
        let runtime_engine_id = self.runtime_store.car_engine_id(current.public_car_id);
        let runtime_matches = matches!(runtime_target, Some(EngineCommandTarget::OfficialRace))
            && runtime_engine_id == Some(current.engine_car_id);
        if runtime_matches {
            return;
        }

        self.stop_official_race_log_capture_for_team(team_id).await;
        self.official_race_bots.remove(team_id);
        if let Err(err) = remove_bot_container(&current.container_name).await {
            tracing::warn!(
                team_id = %team_id,
                container_name = %current.container_name,
                error = %err,
                "official race liveness reconcile: failed to remove stale bot container"
            );
        }
        self.runtime_store
            .clear_active_bot_slot(current.public_car_id);
        let _ = self.slot_updates_tx.send(team_id.to_string());
        tracing::warn!(
            team_id = %team_id,
            public_car_id = current.public_car_id,
            engine_car_id = current.engine_car_id,
            container_name = %current.container_name,
            container_id = %current.container_id,
            "official race liveness reconcile: removed stale bot state after missing runtime car"
        );
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
                    service.reconcile_official_race_bot_liveness(&team_id).await;
                    if service.official_race_bots.get(&team_id).is_none() {
                        continue;
                    }
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
    pub(crate) fn spawn_official_race_file_launcher(
        &self,
        race_config_repo: RaceConfigRepo,
        teams_file_path: PathBuf,
        prepare_signal_path: PathBuf,
        start_signal_path: PathBuf,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        let service = self.clone();
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(Duration::from_millis(OFFICIAL_RACE_LAUNCHER_TICK_MS));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut prepare_signal_present_prev = false;
            let mut start_signal_present_prev = false;
            let mut race_prepared = false;
            let mut race_started = false;
            let mut active_results_recorder: Option<OfficialRaceResultsRecorder> = None;
            let assets_dir = teams_file_path
                .parent()
                .map(|path| path.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("assets"));
            service.runtime_store.clear_official_race_session();

            tracing::info!(
                prepare_signal = %prepare_signal_path.display(),
                start_signal = %start_signal_path.display(),
                teams_file = %teams_file_path.display(),
                "official race launcher started"
            );

            loop {
                select! {
                    _ = shutdown_rx.recv() => {
                        if let Some(recorder) = active_results_recorder.as_mut() {
                            recorder.finalize().await;
                        }
                        service.runtime_store.clear_official_race_session();
                        tracing::info!("official race launcher stopped");
                        return;
                    }
                    _ = ticker.tick() => {}
                }

                let prepare_signal_present = match fs::try_exists(&prepare_signal_path).await {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            path = %prepare_signal_path.display(),
                            error = %err,
                            "official race launcher: failed to check .prepare signal file"
                        );
                        false
                    }
                };
                let start_signal_present = match fs::try_exists(&start_signal_path).await {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            path = %start_signal_path.display(),
                            error = %err,
                            "official race launcher: failed to check .start signal file"
                        );
                        false
                    }
                };

                if prepare_signal_present && !prepare_signal_present_prev {
                    if let Some(recorder) = active_results_recorder.as_mut() {
                        recorder.finalize().await;
                    }
                    active_results_recorder = None;
                    match service
                        .handle_official_race_prepare_signal(
                            &race_config_repo,
                            &teams_file_path,
                            &assets_dir,
                        )
                        .await
                    {
                        Ok(recorder) => {
                            active_results_recorder = Some(recorder);
                            race_prepared = true;
                            race_started = false;
                        }
                        Err(status) => {
                            race_prepared = false;
                            race_started = false;
                            tracing::warn!(
                                code = ?status.code(),
                                error = %status,
                                "official race launcher: prepare trigger failed"
                            );
                        }
                    }
                }

                if start_signal_present && !start_signal_present_prev {
                    if !race_prepared {
                        if let Some(recorder) = active_results_recorder.as_mut() {
                            recorder.finalize().await;
                        }
                        active_results_recorder = None;
                        match service
                            .handle_official_race_prepare_signal(
                                &race_config_repo,
                                &teams_file_path,
                                &assets_dir,
                            )
                            .await
                        {
                            Ok(recorder) => {
                                active_results_recorder = Some(recorder);
                                race_prepared = true;
                                race_started = false;
                            }
                            Err(status) => {
                                tracing::warn!(
                                    code = ?status.code(),
                                    error = %status,
                                    "official race launcher: start trigger failed during implicit prepare"
                                );
                            }
                        }
                    }

                    if race_prepared && !race_started {
                        let (refreshed_count, failed_count) = service
                            .refresh_official_race_bots_before_start_locked()
                            .await;
                        let (forced_hard_count, forced_hard_failed_count) =
                            service.force_official_race_hard_tyres().await;
                        let start_unix_ms = service.frame_hub.latest().server_time_ms;
                        service
                            .runtime_store
                            .mark_official_race_started(start_unix_ms);
                        race_started = true;
                        tracing::info!(
                            start_unix_ms,
                            refreshed_count,
                            failed_count,
                            forced_hard_count,
                            forced_hard_failed_count,
                            "official race launcher: start signal accepted; participant controls enabled"
                        );
                    }
                }

                if let Some(recorder) = active_results_recorder.as_mut() {
                    if race_prepared && !race_started {
                        service.keep_official_race_grid_positions_locked().await;
                    }
                    if race_started {
                        let now_ms = service.frame_hub.latest().server_time_ms;
                        let remaining = service.runtime_store.official_race_remaining_sec(now_ms);
                        if matches!(remaining, Some(value) if value <= 0.0) {
                            tracing::info!(
                                now_ms,
                                "official race launcher: race duration elapsed; closing official race session"
                            );
                            recorder.finalize().await;
                            active_results_recorder = None;
                            {
                                let _guard = service.prepare_command_lock.lock().await;
                                service.close_official_race_session_locked().await;
                            }
                            race_prepared = false;
                            race_started = false;
                            continue;
                        }
                    }
                    recorder.tick(&service).await;
                    let runtime_activity = service
                        .frame_hub
                        .latest()
                        .runtime_state
                        .as_ref()
                        .map(|state| state.activity_kind);
                    if !matches!(runtime_activity, Some(EngineActivityKind::OfficialRace)) {
                        recorder.finalize().await;
                        active_results_recorder = None;
                        race_prepared = false;
                        race_started = false;
                        service.runtime_store.clear_official_race_session();
                    }
                }

                prepare_signal_present_prev = prepare_signal_present;
                start_signal_present_prev = start_signal_present;
            }
        });
    }

    #[cfg(feature = "official")]
    async fn handle_official_race_prepare_signal(
        &self,
        race_config_repo: &RaceConfigRepo,
        teams_file_path: &Path,
        assets_dir: &Path,
    ) -> Result<OfficialRaceResultsRecorder, Status> {
        let snapshot = race_config_repo
            .get_snapshot()
            .await
            .map_err(|err| Status::internal(format!("failed to load race configs: {err}")))?;
        let Some(race_config) = snapshot.races.into_iter().next() else {
            return Err(Status::failed_precondition(
                "official race launcher: no race config available",
            ));
        };

        let team_ids = self.read_official_race_team_ids(teams_file_path).await?;
        let ghost_mode_settings = self
            .read_official_race_ghost_mode_settings(assets_dir)
            .await?;
        let _guard = self.prepare_command_lock.lock().await;
        let launch_result = self
            .launch_official_race_session_locked(race_config.clone(), team_ids, ghost_mode_settings)
            .await?;
        let stats = launch_result.stats;
        let prepared_at_ms = current_unix_ms();
        self.runtime_store.set_official_race_prepared(
            &race_config.config.race_name,
            &race_config.config.map_id,
            race_config.config.race_duration_sec,
            prepared_at_ms,
        );

        let recorder = OfficialRaceResultsRecorder::new(
            assets_dir,
            &race_config,
            launch_result,
            prepared_at_ms,
        );
        recorder.write_snapshot().await.map_err(|err| {
            Status::internal(format!(
                "failed to write initial race results snapshot: {err}"
            ))
        })?;

        tracing::info!(
            race_id = %race_config.race_id,
            race_name = %race_config.config.race_name,
            map_id = %race_config.config.map_id,
            starts_at_ms = race_config.config.starts_at_ms,
            total_listed = stats.total_listed,
            started = stats.started,
            skipped_grid_overflow = stats.skipped_grid_overflow,
            skipped_duplicate_team = stats.skipped_duplicate_team,
            skipped_missing_submission = stats.skipped_missing_submission,
            skipped_runtime_error = stats.skipped_runtime_error,
            skipped_token_error = stats.skipped_token_error,
            skipped_container_error = stats.skipped_container_error,
            "official race launcher: race session prepared from signal trigger"
        );

        Ok(recorder)
    }

    #[cfg(feature = "official")]
    async fn read_official_race_team_ids(
        &self,
        teams_file_path: &Path,
    ) -> Result<Vec<String>, Status> {
        let content = fs::read_to_string(teams_file_path).await.map_err(|err| {
            Status::internal(format!(
                "failed to read official race team roster file {}: {err}",
                teams_file_path.display()
            ))
        })?;

        let mut team_ids = Vec::new();
        for raw_line in content.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            team_ids.push(line.to_string());
        }
        Ok(team_ids)
    }

    #[cfg(feature = "official")]
    async fn read_official_race_ghost_mode_settings(
        &self,
        assets_dir: &Path,
    ) -> Result<Option<GhostModeSettings>, Status> {
        let file_path = assets_dir.join(OFFICIAL_RACE_GHOST_MODE_FILE);
        let exists = fs::try_exists(&file_path).await.map_err(|err| {
            Status::internal(format!(
                "failed to check official race ghost-mode settings file {}: {err}",
                file_path.display()
            ))
        })?;
        if !exists {
            tracing::info!(
                path = %file_path.display(),
                "official race launcher: ghost mode settings file not found, using defaults"
            );
            return Ok(None);
        }

        let content = fs::read_to_string(&file_path).await.map_err(|err| {
            Status::internal(format!(
                "failed to read official race ghost-mode settings file {}: {err}",
                file_path.display()
            ))
        })?;
        let parsed: OfficialRaceGhostModeSettingsFile =
            serde_json::from_str(&content).map_err(|err| {
                Status::invalid_argument(format!(
                    "invalid official race ghost-mode settings JSON {}: {err}",
                    file_path.display()
                ))
            })?;

        if !parsed.enter_speed_max_mps.is_finite()
            || !parsed.exit_speed_min_mps.is_finite()
            || parsed.enter_speed_max_mps < 0.0
            || parsed.exit_speed_min_mps < 0.0
            || parsed.enter_speed_max_mps > parsed.exit_speed_min_mps
        {
            return Err(Status::invalid_argument(format!(
                "invalid ghost mode settings in {}: require finite non-negative speeds and enter_speed_max_mps <= exit_speed_min_mps",
                file_path.display()
            )));
        }

        tracing::info!(
            path = %file_path.display(),
            enabled = parsed.enabled,
            enter_speed_max_mps = parsed.enter_speed_max_mps,
            exit_speed_min_mps = parsed.exit_speed_min_mps,
            enter_delay_ms = parsed.enter_delay_ms,
            exit_delay_ms = parsed.exit_delay_ms,
            until_completed_laps = parsed.until_completed_laps,
            vehicle_overlap_exit_delay_ms = parsed.vehicle_overlap_exit_delay_ms,
            "official race launcher: loaded ghost mode settings from file"
        );

        Ok(Some(GhostModeSettings {
            enabled: parsed.enabled,
            enter_speed_max_mps: parsed.enter_speed_max_mps,
            exit_speed_min_mps: parsed.exit_speed_min_mps,
            enter_delay_ms: parsed.enter_delay_ms,
            exit_delay_ms: parsed.exit_delay_ms,
            until_completed_laps: parsed.until_completed_laps,
            vehicle_overlap_exit_delay_ms: parsed.vehicle_overlap_exit_delay_ms,
        }))
    }

    #[cfg(feature = "official")]
    async fn launch_official_race_session_locked(
        &self,
        race_config: RaceConfigRecord,
        team_ids: Vec<String>,
        ghost_mode_settings: Option<GhostModeSettings>,
    ) -> Result<OfficialRaceLaunchResult, Status> {
        self.cleanup_official_race_session_locked().await;

        let runtime_before = self.engine.runtime_state().await.map_err(map_worker_err)?;
        self.engine
            .switch_runtime(
                runtime_before.revision,
                EngineActivityKind::OfficialRace,
                race_config.config.map_id.clone(),
                None,
                Some(race_time_of_day_preset_to_engine(
                    race_config.config.time_of_day_preset,
                )),
                ghost_mode_settings,
            )
            .await
            .map_err(map_worker_err)?;

        let start_slots = self
            .engine
            .get_number_of_start_pos_in(EngineCommandTarget::OfficialRace)
            .await
            .map_err(map_worker_err)?;
        if start_slots == 0 {
            return Err(Status::failed_precondition(
                "official race map has no start slots",
            ));
        }

        // Keep already-spawned cars pinned to grid during long prepare launches.
        let (prepare_grid_stop_tx, mut prepare_grid_stop_rx) = oneshot::channel::<()>();
        let prepare_grid_service = self.clone();
        let prepare_grid_task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(
                OFFICIAL_RACE_PREPARE_GRID_PIN_INTERVAL_MS,
            ));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                select! {
                    _ = ticker.tick() => {
                        prepare_grid_service.keep_official_race_grid_positions_locked().await;
                    }
                    _ = &mut prepare_grid_stop_rx => {
                        break;
                    }
                }
            }
        });

        let mut stats = OfficialRaceStartStats::default();
        let mut result_teams = Vec::new();
        let mut seen_teams = HashSet::new();
        for (index, team_id) in team_ids.into_iter().enumerate() {
            stats.total_listed += 1;
            let roster_index = u32::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| Status::internal("official race roster index overflow"))?;
            let position_index = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| Status::internal("official race roster index overflow"))?;

            if position_index > start_slots {
                stats.skipped_grid_overflow += 1;
                result_teams.push(OfficialRaceTeamResultEntry {
                    team_id: team_id.clone(),
                    roster_index,
                    start_status: "skipped_grid_overflow".to_string(),
                    slot_index: None,
                    submission_id: None,
                    car_id: None,
                    container_id: None,
                    completed_laps: 0,
                    current_lap_distance_m: None,
                    total_distance_m: None,
                    last_lap_time_ms: None,
                    best_lap_time_ms: None,
                    lap_times_ms: Vec::new(),
                    last_recorded_completed_laps: 0,
                    has_started_moving: false,
                    initial_lap_progress_m: None,
                });
                tracing::warn!(
                    team_id = %team_id,
                    position_index,
                    start_slots,
                    "official race launcher: skipping team because grid is full"
                );
                continue;
            }
            if !seen_teams.insert(team_id.clone()) {
                stats.skipped_duplicate_team += 1;
                result_teams.push(OfficialRaceTeamResultEntry {
                    team_id: team_id.clone(),
                    roster_index,
                    start_status: "skipped_duplicate_team".to_string(),
                    slot_index: None,
                    submission_id: None,
                    car_id: None,
                    container_id: None,
                    completed_laps: 0,
                    current_lap_distance_m: None,
                    total_distance_m: None,
                    last_lap_time_ms: None,
                    best_lap_time_ms: None,
                    lap_times_ms: Vec::new(),
                    last_recorded_completed_laps: 0,
                    has_started_moving: false,
                    initial_lap_progress_m: None,
                });
                tracing::warn!(
                    team_id = %team_id,
                    "official race launcher: skipping duplicate team entry in roster"
                );
                continue;
            }

            let Some((slot_index, submission_id, image_ref)) = self
                .resolve_lowest_runnable_slot_submission(&team_id)
                .await?
            else {
                stats.skipped_missing_submission += 1;
                result_teams.push(OfficialRaceTeamResultEntry {
                    team_id: team_id.clone(),
                    roster_index,
                    start_status: "skipped_missing_submission".to_string(),
                    slot_index: None,
                    submission_id: None,
                    car_id: None,
                    container_id: None,
                    completed_laps: 0,
                    current_lap_distance_m: None,
                    total_distance_m: None,
                    last_lap_time_ms: None,
                    best_lap_time_ms: None,
                    lap_times_ms: Vec::new(),
                    last_recorded_completed_laps: 0,
                    has_started_moving: false,
                    initial_lap_progress_m: None,
                });
                tracing::warn!(
                    team_id = %team_id,
                    "official race launcher: no runnable succeeded slot found for team"
                );
                continue;
            };

            let engine_car_id = match self.engine.spawn_car().await {
                Ok(value) => value,
                Err(err) => {
                    stats.skipped_runtime_error += 1;
                    result_teams.push(OfficialRaceTeamResultEntry {
                        team_id: team_id.clone(),
                        roster_index,
                        start_status: "skipped_runtime_error".to_string(),
                        slot_index: Some(slot_index),
                        submission_id: Some(submission_id.clone()),
                        car_id: None,
                        container_id: None,
                        completed_laps: 0,
                        current_lap_distance_m: None,
                        total_distance_m: None,
                        last_lap_time_ms: None,
                        best_lap_time_ms: None,
                        lap_times_ms: Vec::new(),
                        last_recorded_completed_laps: 0,
                        has_started_moving: false,
                        initial_lap_progress_m: None,
                    });
                    tracing::warn!(
                        team_id = %team_id,
                        error = %err,
                        "official race launcher: failed to spawn official-race car"
                    );
                    continue;
                }
            };
            let public_car_id = self.runtime_store.allocate_public_car_id();
            self.register_official_race_runtime_car(
                &team_id,
                public_car_id,
                engine_car_id,
                position_index,
            );

            if let Err(err) = self
                .engine
                .set_car_at_start_pos_in(
                    EngineCommandTarget::OfficialRace,
                    engine_car_id,
                    position_index,
                )
                .await
            {
                stats.skipped_runtime_error += 1;
                result_teams.push(OfficialRaceTeamResultEntry {
                    team_id: team_id.clone(),
                    roster_index,
                    start_status: "skipped_runtime_error".to_string(),
                    slot_index: Some(slot_index),
                    submission_id: Some(submission_id.clone()),
                    car_id: Some(public_car_id),
                    container_id: None,
                    completed_laps: 0,
                    current_lap_distance_m: None,
                    total_distance_m: None,
                    last_lap_time_ms: None,
                    best_lap_time_ms: None,
                    lap_times_ms: Vec::new(),
                    last_recorded_completed_laps: 0,
                    has_started_moving: false,
                    initial_lap_progress_m: None,
                });
                tracing::warn!(
                    team_id = %team_id,
                    public_car_id,
                    engine_car_id,
                    position_index,
                    error = %err,
                    "official race launcher: failed to set start position"
                );
                self.cleanup_spawned_official_race_car(public_car_id, engine_car_id)
                    .await;
                continue;
            }

            let team_bot_token = match self.game_token_issuer.issue_team_bot_token(&team_id).await {
                Ok(value) => value,
                Err(status) => {
                    stats.skipped_token_error += 1;
                    result_teams.push(OfficialRaceTeamResultEntry {
                        team_id: team_id.clone(),
                        roster_index,
                        start_status: "skipped_token_error".to_string(),
                        slot_index: Some(slot_index),
                        submission_id: Some(submission_id.clone()),
                        car_id: Some(public_car_id),
                        container_id: None,
                        completed_laps: 0,
                        current_lap_distance_m: None,
                        total_distance_m: None,
                        last_lap_time_ms: None,
                        best_lap_time_ms: None,
                        lap_times_ms: Vec::new(),
                        last_recorded_completed_laps: 0,
                        has_started_moving: false,
                        initial_lap_progress_m: None,
                    });
                    tracing::warn!(
                        team_id = %team_id,
                        code = ?status.code(),
                        error = %status,
                        "official race launcher: failed to issue TEAM_BOT token"
                    );
                    self.cleanup_spawned_official_race_car(public_car_id, engine_car_id)
                        .await;
                    continue;
                }
            };
            let wrapper_auth_token = match self
                .wrapper_auth_token_issuer
                .issue_wrapper_auth_token()
                .await
            {
                Ok(value) => value,
                Err(status) => {
                    stats.skipped_token_error += 1;
                    result_teams.push(OfficialRaceTeamResultEntry {
                        team_id: team_id.clone(),
                        roster_index,
                        start_status: "skipped_token_error".to_string(),
                        slot_index: Some(slot_index),
                        submission_id: Some(submission_id.clone()),
                        car_id: Some(public_car_id),
                        container_id: None,
                        completed_laps: 0,
                        current_lap_distance_m: None,
                        total_distance_m: None,
                        last_lap_time_ms: None,
                        best_lap_time_ms: None,
                        lap_times_ms: Vec::new(),
                        last_recorded_completed_laps: 0,
                        has_started_moving: false,
                        initial_lap_progress_m: None,
                    });
                    tracing::warn!(
                        team_id = %team_id,
                        code = ?status.code(),
                        error = %status,
                        "official race launcher: failed to issue wrapper auth token"
                    );
                    self.cleanup_spawned_official_race_car(public_car_id, engine_car_id)
                        .await;
                    continue;
                }
            };

            let container_name = match official_bot_container_name_for_team(&team_id) {
                Ok(value) => value,
                Err(err) => {
                    stats.skipped_runtime_error += 1;
                    result_teams.push(OfficialRaceTeamResultEntry {
                        team_id: team_id.clone(),
                        roster_index,
                        start_status: "skipped_runtime_error".to_string(),
                        slot_index: Some(slot_index),
                        submission_id: Some(submission_id.clone()),
                        car_id: Some(public_car_id),
                        container_id: None,
                        completed_laps: 0,
                        current_lap_distance_m: None,
                        total_distance_m: None,
                        last_lap_time_ms: None,
                        best_lap_time_ms: None,
                        lap_times_ms: Vec::new(),
                        last_recorded_completed_laps: 0,
                        has_started_moving: false,
                        initial_lap_progress_m: None,
                    });
                    tracing::warn!(
                        team_id = %team_id,
                        error = %err,
                        "official race launcher: invalid team id for container name"
                    );
                    self.cleanup_spawned_official_race_car(public_car_id, engine_car_id)
                        .await;
                    continue;
                }
            };
            let container_id = match start_bot_container(
                &image_ref,
                &container_name,
                &self.wrapper_backend_endpoint,
                &team_bot_token,
                &wrapper_auth_token,
                &team_id,
                &submission_id,
                "official-race",
                slot_index,
            )
            .await
            {
                Ok(value) => value,
                Err(err) => {
                    stats.skipped_container_error += 1;
                    result_teams.push(OfficialRaceTeamResultEntry {
                        team_id: team_id.clone(),
                        roster_index,
                        start_status: "skipped_container_error".to_string(),
                        slot_index: Some(slot_index),
                        submission_id: Some(submission_id.clone()),
                        car_id: Some(public_car_id),
                        container_id: None,
                        completed_laps: 0,
                        current_lap_distance_m: None,
                        total_distance_m: None,
                        last_lap_time_ms: None,
                        best_lap_time_ms: None,
                        lap_times_ms: Vec::new(),
                        last_recorded_completed_laps: 0,
                        has_started_moving: false,
                        initial_lap_progress_m: None,
                    });
                    tracing::warn!(
                        team_id = %team_id,
                        public_car_id,
                        engine_car_id,
                        error = %err,
                        "official race launcher: failed to start bot container"
                    );
                    let _ = remove_bot_container(&container_name).await;
                    self.cleanup_spawned_official_race_car(public_car_id, engine_car_id)
                        .await;
                    continue;
                }
            };
            let log_file_path = self
                .start_official_race_bot_log_capture(
                    &team_id,
                    &submission_id,
                    slot_index,
                    &container_id,
                )
                .await;

            self.official_race_bots.insert(
                team_id.clone(),
                TeamOfficialRaceBotState {
                    public_car_id,
                    engine_car_id,
                    start_position_index: position_index,
                    slot_index,
                    container_name: container_name.clone(),
                    container_id: container_id.clone(),
                    submission_id: submission_id.clone(),
                    image_ref: image_ref.clone(),
                    log_file_path,
                    auto_restart_attempts: 0,
                },
            );
            self.runtime_store
                .set_active_bot_slot(public_car_id, slot_index);
            if let Err(err) = self
                .submission_repo
                .upsert_selected_slot_index(&team_id, slot_index)
                .await
            {
                tracing::warn!(
                    team_id = %team_id,
                    slot_index,
                    error = %err,
                    "official race launcher: failed to persist selected slot"
                );
            }
            let _ = self.slot_updates_tx.send(team_id.clone());
            self.spawn_official_race_bot_exit_monitor(team_id.clone(), container_id.clone());

            stats.started += 1;
            result_teams.push(OfficialRaceTeamResultEntry {
                team_id: team_id.clone(),
                roster_index,
                start_status: "started".to_string(),
                slot_index: Some(slot_index),
                submission_id: Some(submission_id.clone()),
                car_id: Some(public_car_id),
                container_id: Some(container_id.clone()),
                completed_laps: 0,
                current_lap_distance_m: None,
                total_distance_m: None,
                last_lap_time_ms: None,
                best_lap_time_ms: None,
                lap_times_ms: Vec::new(),
                last_recorded_completed_laps: 0,
                has_started_moving: false,
                initial_lap_progress_m: None,
            });
            tracing::info!(
                team_id = %team_id,
                public_car_id,
                engine_car_id,
                slot_index,
                submission_id = %submission_id,
                container_name = %container_name,
                container_id = %container_id,
                position_index,
                "official race launcher: team started"
            );
        }

        let _ = prepare_grid_stop_tx.send(());
        let _ = prepare_grid_task.await;

        Ok(OfficialRaceLaunchResult {
            stats,
            teams: result_teams,
        })
    }

    #[cfg(feature = "official")]
    async fn cleanup_official_race_session_locked(&self) {
        self.runtime_store.clear_official_race_session();
        let existing_bots: Vec<(String, TeamOfficialRaceBotState)> = self
            .official_race_bots
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        for (team_id, bot_state) in existing_bots {
            self.stop_official_race_log_capture_for_team(&team_id).await;
            self.official_race_bots.remove(&team_id);
            if let Err(err) = remove_bot_container(&bot_state.container_name).await {
                tracing::warn!(
                    team_id = %team_id,
                    container_name = %bot_state.container_name,
                    error = %err,
                    "official race launcher: failed to remove previous bot container"
                );
            }
            if let Err(err) = self
                .engine
                .despawn_car_in(EngineCommandTarget::OfficialRace, bot_state.engine_car_id)
                .await
            {
                tracing::debug!(
                    team_id = %team_id,
                    public_car_id = bot_state.public_car_id,
                    engine_car_id = bot_state.engine_car_id,
                    error = %err,
                    "official race launcher: despawn of previous car failed"
                );
            }
            self.runtime_store
                .clear_active_bot_slot(bot_state.public_car_id);
            self.runtime_store.remove_car(bot_state.public_car_id);
            let _ = self.slot_updates_tx.send(team_id);
        }

        let stale_official_cars: Vec<u64> = self
            .runtime_store
            .car_targets()
            .iter()
            .filter_map(|entry| match entry.value() {
                EngineCommandTarget::OfficialRace => Some(*entry.key()),
                EngineCommandTarget::Sandbox { .. } | EngineCommandTarget::LocalRace { .. } => None,
            })
            .collect();

        for public_car_id in stale_official_cars {
            let team_id = self
                .runtime_store
                .car_identity_map()
                .get(&public_car_id)
                .and_then(|entry| entry.value().team_id.clone());
            let engine_car_id = self
                .runtime_store
                .car_engine_ids()
                .get(&public_car_id)
                .map(|entry| *entry.value());

            if let Some(engine_car_id) = engine_car_id {
                if let Err(err) = self
                    .engine
                    .despawn_car_in(EngineCommandTarget::OfficialRace, engine_car_id)
                    .await
                {
                    tracing::debug!(
                        public_car_id,
                        engine_car_id,
                        error = %err,
                        "official race launcher: failed to despawn stale official-race car"
                    );
                }
            }

            self.runtime_store.clear_active_bot_slot(public_car_id);
            self.runtime_store.remove_car(public_car_id);
            if let Some(team_id) = team_id {
                let _ = self.slot_updates_tx.send(team_id);
            }
        }
    }

    #[cfg(feature = "official")]
    async fn close_official_race_session_locked(&self) {
        let runtime_state = match self.engine.runtime_state().await {
            Ok(state) => state,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "official race launcher: failed to read runtime state before closing session"
                );
                self.cleanup_official_race_session_locked().await;
                return;
            }
        };

        if matches!(
            runtime_state.activity_kind,
            EngineActivityKind::OfficialRace
        ) {
            let map_id = runtime_state.map_id.clone();
            if let Err(err) = self
                .engine
                .switch_runtime(
                    runtime_state.revision,
                    EngineActivityKind::None,
                    map_id,
                    None,
                    Some(runtime_state.time_of_day_preset),
                    None,
                )
                .await
            {
                tracing::warn!(
                    error = %err,
                    "official race launcher: failed to switch runtime to none after race timeout"
                );
            }
        }

        self.cleanup_official_race_session_locked().await;
    }

    #[cfg(feature = "official")]
    async fn resolve_lowest_runnable_slot_submission(
        &self,
        team_id: &str,
    ) -> Result<Option<(i16, String, String)>, Status> {
        for slot_index in 1_i16..=3_i16 {
            let slot_submission = self
                .submission_repo
                .get_succeeded_submission_for_slot(team_id, slot_index)
                .await
                .map_err(|err| {
                    Status::internal(format!(
                        "failed to resolve slot submission for team {team_id}: {err}"
                    ))
                })?;
            let Some(slot_submission) = slot_submission else {
                continue;
            };
            let Some(image_ref) = slot_submission
                .image_ref
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string())
            else {
                continue;
            };
            return Ok(Some((slot_index, slot_submission.submission_id, image_ref)));
        }
        Ok(None)
    }

    #[cfg(feature = "official")]
    fn register_official_race_runtime_car(
        &self,
        team_id: &str,
        public_car_id: u64,
        engine_car_id: u64,
        start_position_index: u64,
    ) {
        let mut identity = RuntimeCarIdentity::default();
        identity.team_id = Some(team_id.to_string());
        identity.official_race_start_position_index = Some(start_position_index);
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
        self.runtime_store
            .set_bot_switch_in_progress(race_bot_state.public_car_id, true);
        let switch_result: Result<String, Status> = async {
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
            start_bot_container(
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
            })
        }
        .await;
        self.runtime_store
            .set_bot_switch_in_progress(race_bot_state.public_car_id, false);
        let container_id = switch_result?;
        let log_file_path = self
            .start_official_race_bot_log_capture(
                team_id,
                &submission_id,
                selected_slot,
                &container_id,
            )
            .await;

        self.official_race_bots.insert(
            team_id.to_string(),
            TeamOfficialRaceBotState {
                public_car_id: race_bot_state.public_car_id,
                engine_car_id: race_bot_state.engine_car_id,
                start_position_index: race_bot_state.start_position_index,
                slot_index: selected_slot,
                container_name: race_bot_state.container_name,
                container_id: container_id.clone(),
                submission_id,
                image_ref,
                log_file_path,
                auto_restart_attempts: 0,
            },
        );
        self.runtime_store
            .set_active_bot_slot(race_bot_state.public_car_id, selected_slot);
        let _ = self.slot_updates_tx.send(team_id.to_string());
        self.spawn_official_race_bot_exit_monitor(team_id.to_string(), container_id);
        Ok(())
    }

    #[cfg(feature = "official")]
    async fn keep_official_race_grid_positions_locked(&self) {
        let car_targets = self.runtime_store.car_targets();
        let car_engine_ids = self.runtime_store.car_engine_ids();
        let car_identities = self.runtime_store.car_identity_map();
        let states: Vec<(u64, u64, u64)> = car_targets
            .iter()
            .filter_map(|entry| {
                if !matches!(entry.value(), EngineCommandTarget::OfficialRace) {
                    return None;
                }
                let public_car_id = *entry.key();
                let engine_car_id = car_engine_ids
                    .get(&public_car_id)
                    .map(|value| *value.value())?;
                let start_position_index = car_identities
                    .get(&public_car_id)
                    .and_then(|identity| identity.value().official_race_start_position_index)?;
                Some((public_car_id, engine_car_id, start_position_index))
            })
            .collect();
        for (public_car_id, engine_car_id, start_position_index) in states {
            if let Err(err) = self
                .engine
                .set_car_at_start_pos_in(
                    EngineCommandTarget::OfficialRace,
                    engine_car_id,
                    start_position_index,
                )
                .await
            {
                tracing::warn!(
                    public_car_id,
                    engine_car_id,
                    start_position_index,
                    error = %err,
                    "official race launcher: failed to keep prepared car at start position"
                );
            }
        }
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
        #[cfg(not(feature = "standalone"))]
        let token = parse_game_token(request.metadata())?
            .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?;
        #[cfg(not(feature = "standalone"))]
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
                    EngineActivityKind::LocalRace => {
                        return Err(Status::failed_precondition(
                            "official token stream is not available for local race",
                        ));
                    }
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
        #[cfg(feature = "standalone")]
        let self_public_car_id = {
            let car_id = parse_standalone_car_id(request.metadata())?;
            if self.runtime_store.car_target(car_id).is_none() {
                return Err(Status::not_found("unknown x-ha3-car-id"));
            }
            car_id
        };
        let self_target = self
            .runtime_store
            .car_target(self_public_car_id)
            .ok_or_else(|| Status::not_found("unknown car target"))?;
        let self_engine_car_id = self
            .runtime_store
            .car_engine_id(self_public_car_id)
            .ok_or_else(|| Status::not_found("unknown car target"))?;
        #[cfg(not(feature = "standalone"))]
        let scopes = self.token_validator.scopes_from_token(&token).await?;
        #[cfg(feature = "standalone")]
        let scopes = Vec::new();

        let incoming = request.into_inner();
        let stream_id = self.next_stream_seq.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(PARTICIPANT_STREAM_CHANNEL_CAPACITY);

        let engine = self.engine.clone();
        let frame_hub = self.frame_hub.clone();
        let runtime_store = self.runtime_store.clone();
        let simulation_hz = self.simulation_hz;
        #[cfg(feature = "local")]
        let local_race_state = self.local_race_state.clone();

        tokio::spawn(async move {
            run_participant_stream(
                engine,
                frame_hub,
                runtime_store,
                #[cfg(feature = "local")]
                local_race_state,
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
            let frame_runtime_state = self.frame_hub.latest().runtime_state.clone();

            if let Some(existing) = self
                .official_race_bots
                .get(&team_id)
                .map(|entry| entry.value().clone())
            {
                let map_id = self
                    .runtime_store
                    .official_race_public_state()
                    .map(|state| state.map_id)
                    .or_else(|| {
                        frame_runtime_state.as_ref().and_then(|state| {
                            if matches!(state.activity_kind, EngineActivityKind::OfficialRace) {
                                Some(state.map_id.clone())
                            } else {
                                None
                            }
                        })
                    })
                    .ok_or_else(|| {
                        Status::failed_precondition("official-race runtime map is not available")
                    })?;
                let (public_car_id, engine_car_id) =
                    self.require_team_official_race_car(&team_id)?;
                if public_car_id != existing.public_car_id
                    || engine_car_id != existing.engine_car_id
                {
                    tracing::warn!(
                        team_id = %team_id,
                        registry_public_car_id = existing.public_car_id,
                        runtime_public_car_id = public_car_id,
                        registry_engine_car_id = existing.engine_car_id,
                        runtime_engine_car_id = engine_car_id,
                        "prepare official join: roster/runtime mismatch detected; using runtime car ids"
                    );
                }
                self.runtime_store
                    .set_active_bot_slot(public_car_id, existing.slot_index);
                tracing::info!(
                    team_id = %team_id,
                    public_car_id,
                    engine_car_id,
                    slot_index = existing.slot_index,
                    container_name = %existing.container_name,
                    container_id = %existing.container_id,
                    "prepare official join: resolved official-race roster entry"
                );
                return Ok(Response::new(PrepareOfficialJoinResponse {
                    car_id: public_car_id,
                    map_id,
                }));
            }

            if let Some(join_state) = self
                .official_sandbox_joins
                .get(&team_id)
                .map(|entry| entry.value().clone())
            {
                let map_id = frame_runtime_state
                    .as_ref()
                    .and_then(|state| {
                        state
                            .active_sandboxes
                            .iter()
                            .find(|entry| entry.sandbox_id == join_state.sandbox_id)
                            .map(|entry| entry.map_id.clone())
                    })
                    .ok_or_else(|| {
                        Status::failed_precondition("sandbox runtime is not active for team join")
                    })?;
                tracing::info!(
                    team_id = %team_id,
                    sandbox_id = %join_state.sandbox_id,
                    slot_index = join_state.slot_index,
                    public_car_id = join_state.public_car_id,
                    map_id = %map_id,
                    "prepare official join: resolved sandbox join"
                );
                return Ok(Response::new(PrepareOfficialJoinResponse {
                    car_id: join_state.public_car_id,
                    map_id,
                }));
            }

            match frame_runtime_state
                .as_ref()
                .map(|state| state.activity_kind)
                .unwrap_or(EngineActivityKind::None)
            {
                EngineActivityKind::OfficialRace => Err(Status::not_found(
                    "team is not part of active official-race roster",
                )),
                EngineActivityKind::Sandbox => Err(Status::not_found(
                    "no active official sandbox join for team",
                )),
                EngineActivityKind::LocalRace => Err(Status::failed_precondition(
                    "official join is not available in local race",
                )),
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
            #[cfg(not(feature = "standalone"))]
            let auth = Some(
                parse_game_token(request.metadata())?
                    .ok_or_else(|| Status::unauthenticated("missing x-ha3-game-token"))?,
            );
            #[cfg(feature = "standalone")]
            let auth: Option<String> = None;
            let req = request.into_inner();
            let joined = self.local_sandbox_join_impl(req.sandbox_id, auth).await?;
            Ok(Response::new(joined))
        }
    }

    async fn local_race_join(
        &self,
        request: Request<LocalRaceJoinRequest>,
    ) -> Result<Response<LocalRaceJoinResponse>, Status> {
        #[cfg(not(feature = "local"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "LocalRaceJoin is supported only in local backend mode",
            ));
        }
        #[cfg(feature = "local")]
        {
            let req = request.into_inner();
            let joined = self
                .local_race_join_impl(req.race_id, req.display_name)
                .await?;
            Ok(Response::new(joined))
        }
    }
}

#[cfg(feature = "local")]
fn map_local_race_state_err(err: LocalRaceStateError) -> Status {
    match err {
        LocalRaceStateError::NoActiveRace => {
            Status::failed_precondition("local race runtime is not active")
        }
        LocalRaceStateError::RaceMismatch => {
            Status::not_found("local race runtime is not active for requested race_id")
        }
        LocalRaceStateError::JoinClosed => {
            Status::failed_precondition("local race join is allowed only in staging phase")
        }
        LocalRaceStateError::ParticipantLimitReached => {
            Status::resource_exhausted("local race participant limit reached")
        }
    }
}

#[cfg(feature = "standalone")]
fn parse_standalone_car_id(metadata: &tonic::metadata::MetadataMap) -> Result<u64, Status> {
    let value = metadata
        .get("x-ha3-car-id")
        .ok_or_else(|| Status::invalid_argument("missing x-ha3-car-id"))?;
    let raw = value
        .to_str()
        .map_err(|_| Status::invalid_argument("invalid x-ha3-car-id header"))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument("x-ha3-car-id must be a valid u64"));
    }
    trimmed
        .parse::<u64>()
        .map_err(|_| Status::invalid_argument("x-ha3-car-id must be a valid u64"))
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
    if let EngineCommandTarget::LocalRace { race_id } = visible_target {
        if let Some(active) = runtime_state.active_local_race.as_ref() {
            if active.race_id == *race_id {
                return active.map_id.clone();
            }
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

#[cfg(feature = "local")]
async fn local_race_gameplay_closed(
    local_race_state: &LocalRaceStateStore,
    target: &EngineCommandTarget,
) -> bool {
    let EngineCommandTarget::LocalRace { race_id } = target else {
        return false;
    };
    local_race_state
        .gameplay_commands_closed(race_id.as_str())
        .await
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
            let detached_before_despawn = {
                if runtime_store.is_bot_switch_in_progress(public_car_id) {
                    tracing::info!(
                        public_car_id,
                        engine_car_id,
                        target = ?target,
                        reason,
                        switch_reason = "participant-disconnect-during-switch",
                        "participant cleanup: preserving official sandbox car during slot switch"
                    );
                    return;
                }
                runtime_store.remove_car(public_car_id);
                tracing::warn!(
                    public_car_id,
                    engine_car_id,
                    target = ?target,
                    reason = "participant-disconnect-non-switch",
                    "participant cleanup: official sandbox bot disconnected outside slot switch; detached runtime car and forcing cleanup"
                );
                true
            };
            #[cfg(not(feature = "official"))]
            let detached_before_despawn = false;
            match tokio::time::timeout(
                PARTICIPANT_DESPAWN_TIMEOUT,
                engine.despawn_car_in(target.clone(), engine_car_id),
            )
            .await
            {
                Err(_) => {
                    tracing::warn!(
                        public_car_id,
                        engine_car_id,
                        target = ?target,
                        timeout_sec = PARTICIPANT_DESPAWN_TIMEOUT.as_secs(),
                        "participant cleanup: despawn timed out"
                    );
                }
                Ok(Err(err)) => {
                    tracing::warn!(
                        public_car_id,
                        engine_car_id,
                        target = ?target,
                        error = %err,
                        "failed to despawn participant car during cleanup"
                    );
                }
                Ok(Ok(())) => {}
            }
            if !detached_before_despawn {
                runtime_store.remove_car(public_car_id);
            }
            tracing::info!(
                public_car_id,
                engine_car_id,
                target = ?target,
                reason,
                detached_before_despawn,
                "participant cleanup: sandbox car removed"
            );
        }
        EngineCommandTarget::OfficialRace => {
            #[cfg(feature = "official")]
            if runtime_store.is_bot_switch_in_progress(public_car_id) {
                tracing::info!(
                    public_car_id,
                    engine_car_id,
                    target = ?target,
                    reason,
                    switch_reason = "participant-disconnect-during-switch",
                    "participant cleanup: preserving official race car during slot switch"
                );
                return;
            }
            tracing::info!(
                public_car_id,
                engine_car_id,
                target = ?target,
                reason,
                "participant cleanup: preserving official race car after participant disconnect"
            );
        }
        EngineCommandTarget::LocalRace { .. } => {
            match tokio::time::timeout(
                PARTICIPANT_DESPAWN_TIMEOUT,
                engine.despawn_car_in(target.clone(), engine_car_id),
            )
            .await
            {
                Err(_) => {
                    tracing::warn!(
                        public_car_id,
                        engine_car_id,
                        target = ?target,
                        timeout_sec = PARTICIPANT_DESPAWN_TIMEOUT.as_secs(),
                        "participant cleanup: local race despawn timed out"
                    );
                }
                Ok(Err(err)) => {
                    tracing::warn!(
                        public_car_id,
                        engine_car_id,
                        target = ?target,
                        error = %err,
                        "failed to despawn local race car during cleanup"
                    );
                }
                Ok(Ok(())) => {}
            }
            runtime_store.remove_car(public_car_id);
            tracing::info!(
                public_car_id,
                engine_car_id,
                target = ?target,
                reason,
                "participant cleanup: local race car removed"
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

#[cfg(feature = "official")]
fn race_time_of_day_preset_to_engine(
    preset: proto::race::v1::RaceTimeOfDayPreset,
) -> EngineRuntimeTimeOfDayPreset {
    match preset {
        proto::race::v1::RaceTimeOfDayPreset::Morning => EngineRuntimeTimeOfDayPreset::Morning,
        proto::race::v1::RaceTimeOfDayPreset::Noon => EngineRuntimeTimeOfDayPreset::Noon,
        proto::race::v1::RaceTimeOfDayPreset::Evening => EngineRuntimeTimeOfDayPreset::Evening,
        proto::race::v1::RaceTimeOfDayPreset::Night => EngineRuntimeTimeOfDayPreset::Night,
        proto::race::v1::RaceTimeOfDayPreset::Unspecified => {
            EngineRuntimeTimeOfDayPreset::Unspecified
        }
    }
}

async fn run_participant_stream(
    engine: EngineClient,
    frame_hub: FrameHub,
    runtime_store: Arc<RaceRuntimeStore>,
    #[cfg(feature = "local")] local_race_state: LocalRaceStateStore,
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
                        #[cfg(feature = "local")]
                        if local_race_gameplay_closed(&local_race_state, &self_target).await {
                            runtime_store.last_client_seq().insert(self_public_car_id, client_seq);
                            runtime_store
                                .set_controls_input(self_public_car_id, 0.0, 1.0, 0.5, 0.0);
                            let ack = participant_ack(
                                client_seq,
                                frame.tick,
                                engine_gear_shift_to_proto(EngineGearShift::None),
                            );
                            if !send_participant_event(
                                &tx,
                                &mut server_seq,
                                ParticipantServerPayload::Ack(ack),
                            )
                            .await
                            {
                                cleanup_participant_car(
                                    "ack-send-failed",
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
                        #[cfg(feature = "local")]
                        if local_race_gameplay_closed(&local_race_state, &self_target).await {
                            let ack = participant_command_ack(
                                command.client_seq,
                                ParticipantCommandType::BackToTrack,
                                ParticipantCommandStatus::Rejected,
                                applies_from_tick,
                                ParticipantCommandRejectReason::NotAllowed,
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
                        let ack = match engine
                            .set_car_back_to_track_in(self_target.clone(), self_engine_car_id)
                            .await
                        {
                            Ok(()) => {
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
                        #[cfg(feature = "local")]
                        if local_race_gameplay_closed(&local_race_state, &self_target).await {
                            let ack = participant_command_ack(
                                command.client_seq,
                                ParticipantCommandType::EmergencyPitstop,
                                ParticipantCommandStatus::Rejected,
                                applies_from_tick,
                                ParticipantCommandRejectReason::NotAllowed,
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
                        #[cfg(feature = "official")]
                        {
                            let in_pit = frame
                                .cars
                                .get(&self_public_car_id)
                                .map(|car| car.state.pitstop_state.is_in_any_zone())
                                .unwrap_or(false);
                            if in_pit {
                                let ack = participant_command_ack(
                                    command.client_seq,
                                    ParticipantCommandType::EmergencyPitstop,
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
                        #[cfg(feature = "local")]
                        if local_race_gameplay_closed(&local_race_state, &self_target).await {
                            let ack = participant_command_ack(
                                command.client_seq,
                                ParticipantCommandType::SetNextPitTireType,
                                ParticipantCommandStatus::Rejected,
                                applies_from_tick,
                                ParticipantCommandRejectReason::NotAllowed,
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
