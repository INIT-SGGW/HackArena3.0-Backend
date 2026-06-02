//! Standalone backend binary for local self-hosted runs.
//!
//! Build with `--features standalone`.

#[cfg(all(not(feature = "ide"), not(feature = "standalone")))]
compile_error!("ha3-standalone requires --features standalone");
#[cfg(all(not(feature = "ide"), feature = "official"))]
compile_error!("ha3-standalone cannot be built with --features official");
#[cfg(all(feature = "ide", not(debug_assertions)))]
compile_error!("feature `ide` is for editor use only; do not enable in release builds");

use std::error::Error;
use std::ffi::OsString;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use game_server::config::Config;
use proto::race::v1::local_race_admin_service_client::LocalRaceAdminServiceClient;
use proto::race::v1::local_sandbox_admin_service_client::LocalSandboxAdminServiceClient;
use proto::race::v1::race_table_query_service_client::RaceTableQueryServiceClient;
use proto::race::v1::{
    CloseLocalRaceRequest, CreateLocalRaceRequest, GetLocalRuntimeStateRequest,
    GetRaceTableRequest, LocalRaceConfigInput, LocalRacePhase, LocalRaceTableSnapshot,
    LocalRaceTableTarget, LocalRuntimeState, RaceTableEntryStatus, RaceTableTarget,
    StartLocalRaceCountdownRequest, race_table_snapshot, race_table_target,
};
use serde::{Deserialize, Serialize};
use tonic::transport::{Channel, Endpoint};

const STANDALONE_CONFIG_FILENAME: &str = "standalone.toml";
const LEGACY_STANDALONE_ENV_FILENAME: &str = ".env.standalone";
const FALLBACK_ENV_FILENAME: &str = ".env";
const STANDALONE_CONFIG_VERSION: u32 = 1;
const USER_LOG_TARGET: &str = "ha3_standalone::user";
const RUN_RACE_READINESS_RETRY_MS: u64 = 250;
const RUN_RACE_POLL_INTERVAL_MS: u64 = 500;

#[derive(Debug, Default, PartialEq, Eq)]
struct StandaloneCliArgs {
    run_race: Option<PathBuf>,
    result_json_out: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunRaceArgs {
    scenario_path: PathBuf,
    result_json_out: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct StandaloneTomlConfig {
    config_version: u32,
    log_level: Option<String>,
    listen_addr: Option<String>,
    frontend_enable: Option<bool>,
    frontend_listen_addr: Option<String>,
    frontend_dir: Option<String>,
    tracks_dir: Option<String>,
    bolids_dir: Option<String>,
    simulation_hz: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct RunRaceScenarioFile {
    map_id: String,
    race_duration_sec: u32,
    expected_participants: u32,
    race_name: Option<String>,
    countdown_seconds: Option<u32>,
    max_participants: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunRaceScenario {
    map_id: String,
    race_duration_sec: u32,
    expected_participants: u32,
    race_name: String,
    countdown_seconds: u32,
    max_participants: u32,
}

#[derive(Debug)]
struct StandaloneTomlResolvedConfig {
    default_log_filter: &'static str,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StandaloneLocalRaceResultsFile {
    mode: String,
    race_id: String,
    race_name: String,
    map_id: String,
    started_at_unix_ms: u64,
    finalized_at_unix_ms: u64,
    status: String,
    expected_participants: u32,
    joined_participants: u32,
    participants: Vec<StandaloneLocalRaceResultParticipant>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StandaloneLocalRaceResultParticipant {
    position: u32,
    car_id: u64,
    display_name: String,
    participant_index: u32,
    gap_to_leader_ms: Option<u32>,
    laps_behind: u32,
    in_pit: bool,
    status: String,
}

#[derive(Debug)]
struct StandaloneEnvLoadSummary {
    standalone_toml: Option<PathBuf>,
    legacy_env: Option<PathBuf>,
    fallback_env: Option<PathBuf>,
    default_log_filter: &'static str,
    warnings: Vec<String>,
}

impl Default for StandaloneEnvLoadSummary {
    fn default() -> Self {
        Self {
            standalone_toml: None,
            legacy_env: None,
            fallback_env: None,
            default_log_filter: "info",
            warnings: Vec::new(),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli_args = parse_cli_args()?;
    let load_summary = load_standalone_process_env()?;
    let _tracing_guard = game_server::init_tracing_with_default_filter(
        "ha3-standalone",
        Some(load_summary.default_log_filter),
    )?;
    log_standalone_startup_summary(&load_summary);

    if cli_args.run_race.is_some() {
        tracing::info!(
            target: USER_LOG_TARGET,
            "Run-race mode enabled; frontend hosting is forced off"
        );
        force_env("FRONTEND_ENABLE", "false");
    }

    let cfg = Arc::new(Config::load_or_exit());

    if let Some(run_race) = cli_args.into_run_race_args()? {
        run_race_mode(cfg, run_race).await
    } else {
        game_server::run(cfg).await
    }
}

fn load_standalone_process_env() -> Result<StandaloneEnvLoadSummary, Box<dyn Error>> {
    let mut summary = StandaloneEnvLoadSummary::default();

    if let Some(path) = find_file_upwards(STANDALONE_CONFIG_FILENAME) {
        let resolved = apply_standalone_toml(&path)?;
        summary.default_log_filter = resolved.default_log_filter;
        summary.warnings.extend(resolved.warnings);
        summary.standalone_toml = Some(path);
    }

    summary.legacy_env = load_env_file_if_present(LEGACY_STANDALONE_ENV_FILENAME)?;
    summary.fallback_env = load_env_file_if_present(FALLBACK_ENV_FILENAME)?;

    Ok(summary)
}

fn apply_standalone_toml(path: &Path) -> Result<StandaloneTomlResolvedConfig, Box<dyn Error>> {
    let raw = std::fs::read_to_string(path)?;
    let config: StandaloneTomlConfig = toml::from_str(&raw).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("failed to parse {}: {err}", path.display()),
        )
    })?;

    if config.config_version != STANDALONE_CONFIG_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "unsupported standalone config version {} in {}; expected {}",
                config.config_version,
                path.display(),
                STANDALONE_CONFIG_VERSION
            ),
        )
        .into());
    }

    set_env_if_missing("LISTEN_ADDR", config.listen_addr);
    set_env_if_missing(
        "FRONTEND_ENABLE",
        config.frontend_enable.map(bool_to_env_string),
    );
    set_env_if_missing("FRONTEND_LISTEN_ADDR", config.frontend_listen_addr);
    set_env_if_missing("FRONTEND_DIR", config.frontend_dir);
    set_env_if_missing("TRACKS_DIR", config.tracks_dir);
    set_env_if_missing("BOLIDS_DIR", config.bolids_dir);
    set_env_if_missing("SIMULATION_HZ", config.simulation_hz.map(|v| v.to_string()));

    Ok(StandaloneTomlResolvedConfig {
        default_log_filter: standalone_log_filter(config.log_level.as_deref())?,
        warnings: Vec::new(),
    })
}

fn bool_to_env_string(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

fn set_env_if_missing(name: &str, value: Option<String>) {
    let Some(value) = value else {
        return;
    };
    if std::env::var_os(name).is_none() {
        // SAFETY: standalone startup mutates process env before spawning worker tasks.
        unsafe { std::env::set_var(name, value) };
    }
}

fn force_env(name: &str, value: &str) {
    // SAFETY: standalone startup mutates process env before spawning worker tasks.
    unsafe { std::env::set_var(name, value) };
}

fn standalone_log_filter(value: Option<&str>) -> Result<&'static str, Box<dyn Error>> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok("info"),
        Some("minimal") => Ok("warn,ha3_standalone::user=info"),
        Some("verbose") => Ok("trace"),
        Some("debug") => Ok("debug"),
        Some("info") => Ok("info"),
        Some("warn") => Ok("warn"),
        Some("error") => Ok("error"),
        Some(other) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "invalid standalone log_level `{other}`; expected one of: minimal, verbose, debug, info, warn, error"
            ),
        )
        .into()),
    }
}

fn log_standalone_startup_summary(load_summary: &StandaloneEnvLoadSummary) {
    for warning in &load_summary.warnings {
        eprintln!("Warning: {warning}");
        tracing::warn!(target: USER_LOG_TARGET, warning, "standalone config warning");
    }

    if let Some(path) = &load_summary.standalone_toml {
        tracing::info!(
            target: USER_LOG_TARGET,
            path = %display_path(path),
            "Using standalone config file"
        );
    } else {
        tracing::debug!("standalone TOML config not found; using legacy env fallbacks if present");
    }
    if let Some(path) = &load_summary.legacy_env {
        tracing::warn!(
            target: USER_LOG_TARGET,
            path = %display_path(path),
            "Using legacy .env.standalone fallback; migrate to standalone.toml"
        );
    }
    if let Some(path) = &load_summary.fallback_env {
        tracing::warn!(
            target: USER_LOG_TARGET,
            path = %display_path(path),
            "Using generic .env fallback; migrate to standalone.toml"
        );
    }
}

fn is_reserved_file_env_key(name: &str) -> bool {
    matches!(name, "APP_ENV" | "RUST_LOG")
}

#[allow(deprecated)]
fn load_env_file_if_present(file_name: &str) -> Result<Option<PathBuf>, Box<dyn Error>> {
    let Some(path) = find_file_upwards(file_name) else {
        return Ok(None);
    };

    let mut applied_any = false;
    for item in dotenv::from_path_iter(&path)? {
        let (key, value) = item?;
        if is_reserved_file_env_key(&key) || std::env::var_os(&key).is_some() {
            continue;
        }
        // SAFETY: standalone startup mutates process env before spawning worker tasks.
        unsafe { std::env::set_var(key, value) };
        applied_any = true;
    }

    Ok(applied_any.then_some(path))
}

fn find_file_upwards(file_name: &str) -> Option<PathBuf> {
    let mut current = std::env::current_dir().ok()?;
    loop {
        let candidate = current.join(file_name);
        if candidate.is_file() {
            return Some(candidate);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn display_path(path: &Path) -> String {
    path.display().to_string().replace("\\\\?\\", "")
}

fn parse_cli_args() -> Result<StandaloneCliArgs, Box<dyn Error>> {
    parse_cli_args_from(std::env::args_os().skip(1))
}

fn parse_cli_args_from<I>(args: I) -> Result<StandaloneCliArgs, Box<dyn Error>>
where
    I: IntoIterator<Item = OsString>,
{
    let mut parsed = StandaloneCliArgs::default();
    let args: Vec<OsString> = args.into_iter().collect();
    let mut idx = 0usize;
    while idx < args.len() {
        let arg = args[idx].to_str().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "argument is not valid UTF-8")
        })?;
        match arg {
            "--run-race" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "--run-race requires a path")
                })?;
                parsed.run_race = Some(PathBuf::from(value));
            }
            "--result-json-out" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "--result-json-out requires a path",
                    )
                })?;
                parsed.result_json_out = Some(PathBuf::from(value));
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {other}"),
                )
                .into());
            }
        }
        idx += 1;
    }
    Ok(parsed)
}

impl StandaloneCliArgs {
    fn into_run_race_args(self) -> Result<Option<RunRaceArgs>, Box<dyn Error>> {
        match (self.run_race, self.result_json_out) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--result-json-out requires --run-race",
            )
            .into()),
            (Some(scenario_path), result_json_out) => Ok(Some(RunRaceArgs {
                scenario_path,
                result_json_out,
            })),
        }
    }
}

fn load_run_race_scenario(path: &Path) -> Result<RunRaceScenario, Box<dyn Error>> {
    let raw = std::fs::read_to_string(path)?;
    let file: RunRaceScenarioFile = toml::from_str(&raw).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse {}: {err}", path.display()),
        )
    })?;
    normalize_run_race_scenario(file)
}

fn normalize_run_race_scenario(
    file: RunRaceScenarioFile,
) -> Result<RunRaceScenario, Box<dyn Error>> {
    let map_id = file.map_id.trim().to_string();
    if map_id.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "map_id must be non-empty").into());
    }
    if file.race_duration_sec == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "race_duration_sec must be greater than zero",
        )
        .into());
    }
    if file.expected_participants == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected_participants must be greater than zero",
        )
        .into());
    }
    let race_name = file
        .race_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Local race")
        .to_string();
    let max_participants = file.max_participants.unwrap_or(file.expected_participants);
    if max_participants < file.expected_participants {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "max_participants must be greater than or equal to expected_participants",
        )
        .into());
    }

    Ok(RunRaceScenario {
        map_id,
        race_duration_sec: file.race_duration_sec,
        expected_participants: file.expected_participants,
        race_name,
        countdown_seconds: file.countdown_seconds.unwrap_or(0),
        max_participants,
    })
}

async fn run_race_mode(cfg: Arc<Config>, args: RunRaceArgs) -> Result<(), Box<dyn Error>> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let controller_cfg = cfg.clone();
    let controller_future = async move {
        let result = run_race_controller(controller_cfg, args).await;
        let _ = shutdown_tx.send(());
        result
    };
    let run_future = game_server::run_until_shutdown(cfg, async move {
        let _ = shutdown_rx.await;
    });
    let (controller_result, run_result) = tokio::join!(controller_future, run_future);

    run_result?;
    controller_result
}

async fn run_race_controller(cfg: Arc<Config>, args: RunRaceArgs) -> Result<(), Box<dyn Error>> {
    let scenario = load_run_race_scenario(&args.scenario_path)?;
    let endpoint_url = standalone_grpc_endpoint_url(cfg.listen_addr);

    tracing::info!(
        target: USER_LOG_TARGET,
        scenario = %display_path(&args.scenario_path),
        endpoint = %endpoint_url,
        expected_participants = scenario.expected_participants,
        "Starting standalone run-race controller"
    );

    let channel = wait_for_standalone_readiness(&endpoint_url).await?;
    let mut sandbox_client = LocalSandboxAdminServiceClient::new(channel.clone());
    let mut race_admin_client = LocalRaceAdminServiceClient::new(channel.clone());
    let mut race_table_client = RaceTableQueryServiceClient::new(channel);

    let runtime = get_local_runtime_state(&mut sandbox_client).await?;
    let create_response = race_admin_client
        .create_local_race(CreateLocalRaceRequest {
            expected_revision: runtime.revision,
            config: Some(LocalRaceConfigInput {
                race_name: scenario.race_name.clone(),
                map_id: scenario.map_id.clone(),
                race_duration_sec: scenario.race_duration_sec,
                time_of_day: None,
                ghost_mode: None,
                weather: None,
                max_participants: scenario.max_participants,
            }),
        })
        .await?
        .into_inner();
    let created_race = create_response
        .race
        .ok_or_else(|| io::Error::other("CreateLocalRace returned no race payload"))?;

    tracing::info!(
        target: USER_LOG_TARGET,
        race_id = %created_race.race_id,
        map_id = %created_race.map_id,
        race_duration_sec = created_race.race_duration_sec,
        expected_participants = scenario.expected_participants,
        "Standalone local race created; waiting for participants"
    );

    let staged_state = wait_for_expected_participants(
        &mut sandbox_client,
        created_race.race_id.as_str(),
        scenario.expected_participants,
    )
    .await?;

    tracing::info!(
        target: USER_LOG_TARGET,
        race_id = %created_race.race_id,
        joined_participants = staged_state
            .active_race
            .as_ref()
            .map(|race| race.joined_participant_count)
            .unwrap_or(0),
        countdown_seconds = scenario.countdown_seconds,
        "Expected participants joined; starting standalone local race"
    );

    race_admin_client
        .start_local_race_countdown(StartLocalRaceCountdownRequest {
            expected_revision: staged_state.revision,
            race_id: created_race.race_id.clone(),
            countdown_seconds: scenario.countdown_seconds,
        })
        .await?;

    let finished_state =
        wait_for_finished_race(&mut sandbox_client, created_race.race_id.as_str()).await?;
    let active_race = finished_state.active_race.as_ref().ok_or_else(|| {
        io::Error::other("finished local race disappeared before results collection")
    })?;

    let snapshot =
        get_local_race_table_snapshot(&mut race_table_client, created_race.race_id.clone()).await?;
    let finalized_at_unix_ms =
        current_unix_ms_from_timestamp(active_race.planned_end_at_utc.as_ref())
            .unwrap_or_else(current_unix_ms);
    let results = build_results_file(
        active_race,
        snapshot,
        scenario.expected_participants,
        finalized_at_unix_ms,
    );

    print_run_race_results(&results);

    if let Some(path) = args.result_json_out.as_ref() {
        write_run_race_results_json(path, &results)?;
        tracing::info!(
            target: USER_LOG_TARGET,
            path = %display_path(path),
            "Standalone run-race results written to JSON"
        );
    }

    if let Err(err) = race_admin_client
        .close_local_race(CloseLocalRaceRequest {
            expected_revision: finished_state.revision,
            race_id: created_race.race_id.clone(),
        })
        .await
    {
        tracing::warn!(
            target: USER_LOG_TARGET,
            race_id = %created_race.race_id,
            error = %err,
            "Standalone run-race failed to close local race cleanly"
        );
    }

    tracing::info!(
        target: USER_LOG_TARGET,
        race_id = %created_race.race_id,
        "Standalone run-race finished successfully"
    );

    Ok(())
}

async fn wait_for_standalone_readiness(endpoint_url: &str) -> Result<Channel, Box<dyn Error>> {
    let endpoint = Endpoint::from_shared(endpoint_url.to_string())?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(5));

    loop {
        match endpoint.clone().connect().await {
            Ok(channel) => {
                let mut client = LocalSandboxAdminServiceClient::new(channel.clone());
                if get_local_runtime_state(&mut client).await.is_ok() {
                    return Ok(channel);
                }
            }
            Err(err) => {
                tracing::debug!(
                    target: USER_LOG_TARGET,
                    endpoint = %endpoint_url,
                    error = %err,
                    "Standalone run-race waiting for gRPC readiness"
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(RUN_RACE_READINESS_RETRY_MS)).await;
    }
}

async fn get_local_runtime_state(
    client: &mut LocalSandboxAdminServiceClient<Channel>,
) -> Result<LocalRuntimeState, Box<dyn Error>> {
    let response = client
        .get_local_runtime_state(GetLocalRuntimeStateRequest {})
        .await?
        .into_inner();
    response
        .state
        .ok_or_else(|| io::Error::other("GetLocalRuntimeState returned no state").into())
}

async fn wait_for_expected_participants(
    client: &mut LocalSandboxAdminServiceClient<Channel>,
    race_id: &str,
    expected_participants: u32,
) -> Result<LocalRuntimeState, Box<dyn Error>> {
    loop {
        let state = get_local_runtime_state(client).await?;
        let race = state.active_race.as_ref().ok_or_else(|| {
            io::Error::other("active local race disappeared while waiting for participants")
        })?;
        if race.race_id != race_id {
            return Err(io::Error::other(
                "active local race id changed while waiting for participants",
            )
            .into());
        }
        let phase = parse_local_race_phase(race.phase)?;
        if phase != LocalRacePhase::Staging {
            return Err(io::Error::other(format!(
                "local race left staging before expected participants joined (phase={phase:?})"
            ))
            .into());
        }
        if race.joined_participant_count >= expected_participants {
            return Ok(state);
        }
        tokio::time::sleep(Duration::from_millis(RUN_RACE_POLL_INTERVAL_MS)).await;
    }
}

async fn wait_for_finished_race(
    client: &mut LocalSandboxAdminServiceClient<Channel>,
    race_id: &str,
) -> Result<LocalRuntimeState, Box<dyn Error>> {
    loop {
        let state = get_local_runtime_state(client).await?;
        let race = state
            .active_race
            .as_ref()
            .ok_or_else(|| io::Error::other("active local race disappeared before finishing"))?;
        if race.race_id != race_id {
            return Err(io::Error::other("active local race id changed before finishing").into());
        }
        match parse_local_race_phase(race.phase)? {
            LocalRacePhase::Finished => return Ok(state),
            LocalRacePhase::Aborted => {
                return Err(io::Error::other("local race was aborted before finishing").into());
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(RUN_RACE_POLL_INTERVAL_MS)).await;
            }
        }
    }
}

async fn get_local_race_table_snapshot(
    client: &mut RaceTableQueryServiceClient<Channel>,
    race_id: String,
) -> Result<LocalRaceTableSnapshot, Box<dyn Error>> {
    let response = client
        .get_race_table(GetRaceTableRequest {
            target: Some(RaceTableTarget {
                target: Some(race_table_target::Target::LocalRace(LocalRaceTableTarget {
                    race_id,
                })),
            }),
        })
        .await?
        .into_inner();
    let snapshot = response
        .snapshot
        .ok_or_else(|| io::Error::other("GetRaceTable returned no snapshot"))?;
    match snapshot.snapshot {
        Some(race_table_snapshot::Snapshot::LocalRace(local_race)) => Ok(local_race),
        _ => Err(io::Error::other("GetRaceTable returned non-local-race snapshot").into()),
    }
}

fn build_results_file(
    race: &proto::race::v1::LocalRaceRuntimeInfo,
    snapshot: LocalRaceTableSnapshot,
    expected_participants: u32,
    finalized_at_unix_ms: u64,
) -> StandaloneLocalRaceResultsFile {
    let participants = snapshot
        .entries
        .into_iter()
        .map(|entry| {
            let participant = entry.participant.unwrap_or_default();
            StandaloneLocalRaceResultParticipant {
                position: entry.position,
                car_id: entry.car_id,
                display_name: if participant.display_name.trim().is_empty() {
                    format!("car-{}", entry.car_id)
                } else {
                    participant.display_name
                },
                participant_index: participant.participant_index,
                gap_to_leader_ms: entry.gap_to_leader_ms,
                laps_behind: entry.laps_behind,
                in_pit: entry.in_pit,
                status: race_table_status_name(entry.status),
            }
        })
        .collect();

    StandaloneLocalRaceResultsFile {
        mode: "standalone_local_race".to_string(),
        race_id: race.race_id.clone(),
        race_name: race.race_name.clone(),
        map_id: race.map_id.clone(),
        started_at_unix_ms: current_unix_ms_from_timestamp(race.running_started_at_utc.as_ref())
            .unwrap_or(0),
        finalized_at_unix_ms,
        status: "finished".to_string(),
        expected_participants,
        joined_participants: race.joined_participant_count,
        participants,
    }
}

fn print_run_race_results(results: &StandaloneLocalRaceResultsFile) {
    println!();
    println!(
        "Standalone local race finished: {} ({}) on {}",
        results.race_name, results.race_id, results.map_id
    );
    println!(
        "{:<8} {:<24} {:<12} {:<8} {:<12} {:<16} {}",
        "position", "display_name", "participant", "car_id", "laps_behind", "gap_ms", "status"
    );
    for participant in &results.participants {
        let gap = participant
            .gap_to_leader_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<8} {:<24} {:<12} {:<8} {:<12} {:<16} {}",
            participant.position,
            truncate_for_table(&participant.display_name, 24),
            participant.participant_index,
            participant.car_id,
            participant.laps_behind,
            gap,
            participant.status
        );
    }
}

fn truncate_for_table(value: &str, max_len: usize) -> String {
    let mut chars = value.chars();
    let truncated: String = chars.by_ref().take(max_len).collect();
    if chars.next().is_some() && max_len > 1 {
        format!(
            "{}~",
            truncated.chars().take(max_len - 1).collect::<String>()
        )
    } else {
        truncated
    }
}

fn write_run_race_results_json(
    path: &Path,
    results: &StandaloneLocalRaceResultsFile,
) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(results)?;
    std::fs::write(path, json)?;
    Ok(())
}

fn standalone_grpc_endpoint_url(listen_addr: SocketAddr) -> String {
    let host = match listen_addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        ip => ip,
    };
    match host {
        IpAddr::V4(ip) => format!("http://{}:{}", ip, listen_addr.port()),
        IpAddr::V6(ip) => format!("http://[{}]:{}", ip, listen_addr.port()),
    }
}

fn parse_local_race_phase(value: i32) -> Result<LocalRacePhase, Box<dyn Error>> {
    LocalRacePhase::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid LocalRacePhase value: {value}"),
        )
        .into()
    })
}

fn race_table_status_name(value: i32) -> String {
    match RaceTableEntryStatus::try_from(value).unwrap_or(RaceTableEntryStatus::Unspecified) {
        RaceTableEntryStatus::Active => "ACTIVE".to_string(),
        RaceTableEntryStatus::Dnf => "DNF".to_string(),
        RaceTableEntryStatus::Unspecified => "UNSPECIFIED".to_string(),
    }
}

fn current_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn current_unix_ms_from_timestamp(value: Option<&prost_types::Timestamp>) -> Option<u64> {
    let timestamp = value?;
    if timestamp.seconds < 0 {
        return None;
    }
    let millis = timestamp
        .seconds
        .checked_mul(1_000)?
        .checked_add(i64::from(timestamp.nanos / 1_000_000))?;
    u64::try_from(millis).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cli_args_accepts_run_race_and_json_output() {
        let parsed = parse_cli_args_from([
            OsString::from("--run-race"),
            OsString::from("race.toml"),
            OsString::from("--result-json-out"),
            OsString::from("results.json"),
        ])
        .expect("cli args should parse");

        assert_eq!(
            parsed,
            StandaloneCliArgs {
                run_race: Some(PathBuf::from("race.toml")),
                result_json_out: Some(PathBuf::from("results.json")),
            }
        );
    }

    #[test]
    fn parse_cli_args_rejects_result_without_run_race() {
        let parsed = StandaloneCliArgs {
            run_race: None,
            result_json_out: Some(PathBuf::from("results.json")),
        };
        let err = parsed
            .into_run_race_args()
            .expect_err("result-json-out without run-race should fail");
        assert!(
            err.to_string()
                .contains("--result-json-out requires --run-race"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn normalize_run_race_scenario_defaults_max_participants_to_expected() {
        let scenario = normalize_run_race_scenario(RunRaceScenarioFile {
            map_id: "ovalis_04".to_string(),
            race_duration_sec: 60,
            expected_participants: 8,
            race_name: None,
            countdown_seconds: None,
            max_participants: None,
        })
        .expect("scenario should normalize");

        assert_eq!(
            scenario,
            RunRaceScenario {
                map_id: "ovalis_04".to_string(),
                race_duration_sec: 60,
                expected_participants: 8,
                race_name: "Local race".to_string(),
                countdown_seconds: 0,
                max_participants: 8,
            }
        );
    }

    #[test]
    fn normalize_run_race_scenario_rejects_invalid_limits() {
        let err = normalize_run_race_scenario(RunRaceScenarioFile {
            map_id: "ovalis_04".to_string(),
            race_duration_sec: 60,
            expected_participants: 10,
            race_name: None,
            countdown_seconds: None,
            max_participants: Some(5),
        })
        .expect_err("scenario should reject max_participants < expected_participants");

        assert!(
            err.to_string().contains(
                "max_participants must be greater than or equal to expected_participants"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn standalone_log_filter_rejects_invalid_values() {
        let err = standalone_log_filter(Some("loud")).expect_err("should reject invalid value");
        assert!(
            err.to_string().contains("invalid standalone log_level"),
            "unexpected error: {err}"
        );
    }
}
