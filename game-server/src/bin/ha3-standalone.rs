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
use std::path::{Path, PathBuf};
use std::sync::Arc;

use game_server::config::Config;
use serde::Deserialize;

const STANDALONE_CONFIG_FILENAME: &str = "standalone.toml";
const LEGACY_STANDALONE_ENV_FILENAME: &str = ".env.standalone";
const FALLBACK_ENV_FILENAME: &str = ".env";
const STANDALONE_CONFIG_VERSION: u32 = 1;
const USER_LOG_TARGET: &str = "ha3_standalone::user";

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

#[derive(Debug, Default)]
struct StandaloneEnvLoadSummary {
    standalone_toml: Option<PathBuf>,
    legacy_env: Option<PathBuf>,
    fallback_env: Option<PathBuf>,
    default_log_filter: &'static str,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let load_summary = load_standalone_process_env()?;
    let _tracing_guard = game_server::init_tracing_with_default_filter(
        "ha3-standalone",
        Some(load_summary.default_log_filter),
    )?;

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

    let cfg = Arc::new(Config::load_or_exit());

    game_server::run(cfg).await
}

fn load_standalone_process_env() -> Result<StandaloneEnvLoadSummary, Box<dyn Error>> {
    let mut summary = StandaloneEnvLoadSummary {
        default_log_filter: "info",
        ..StandaloneEnvLoadSummary::default()
    };

    if let Some(path) = find_file_upwards(STANDALONE_CONFIG_FILENAME) {
        summary.default_log_filter = apply_standalone_toml(&path)?;
        summary.standalone_toml = Some(path);
    }

    summary.legacy_env = load_env_file_if_present(LEGACY_STANDALONE_ENV_FILENAME)?;
    summary.fallback_env = load_env_file_if_present(FALLBACK_ENV_FILENAME)?;

    Ok(summary)
}

fn apply_standalone_toml(path: &Path) -> Result<&'static str, Box<dyn Error>> {
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

    Ok(standalone_log_filter(config.log_level.as_deref())?)
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
