use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use http::{HeaderName, HeaderValue};
use tower_http::cors::{AllowOrigin, ExposeHeaders};

const DEFAULT_EXPOSE_HEADERS: &[&str] = &["grpc-status", "grpc-message"];
#[cfg(feature = "local")]
const LOCAL_MAX_ACTIVE_SANDBOXES: u32 = 10;
#[cfg(feature = "local")]
const LOCAL_SANDBOX_STORE_RELATIVE_PATH: &str = "local/sandbox-configs.json";
#[cfg(feature = "official")]
const DEFAULT_BUILD_SERVICE_GRPC_ENDPOINT: &str = "http://127.0.0.1:56051";
#[cfg(feature = "official")]
const DEFAULT_HPS_ENDPOINT: &str = "https://platform.hackarena.pl";
#[cfg(feature = "official")]
const DEFAULT_BUILD_SERVICE_TIMEOUT_MS: u64 = 5_000;
#[cfg(feature = "official")]
const DEFAULT_HPS_GET_TIMEOUT_MS: u64 = 5_000;
#[cfg(feature = "official")]
const DEFAULT_HPS_CACHE_TTL_MS: u64 = 3_600_000;
#[cfg(feature = "official")]
const DEFAULT_HPS_EDITION: &str = "3";
#[cfg(feature = "official")]
const DEFAULT_BUILD_UPLOADS_RELATIVE_PATH: &str = "uploads/build";
#[cfg(feature = "official")]
const DEFAULT_BUILD_UPLOAD_MAX_SIZE_BYTES: u64 = 32 * 1024 * 1024; // 32 MiB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppEnv {
    Development,
    Preprod,
    Production,
}

impl AppEnv {
    pub fn from_env() -> Self {
        let v = std::env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());
        match v.to_ascii_lowercase().as_str() {
            "preprod" => AppEnv::Preprod,
            "production" | "prod" => AppEnv::Production,
            _ => AppEnv::Development,
        }
    }

    pub fn is_production(self) -> bool {
        matches!(self, AppEnv::Production)
    }

    pub fn is_development(self) -> bool {
        matches!(self, AppEnv::Development)
    }
}

#[derive(Debug)]
pub struct Config {
    pub env: AppEnv,
    pub listen_addr: SocketAddr,
    pub allow_origin: AllowOrigin,
    pub expose_headers: ExposeHeaders,
    pub tracks_dir: PathBuf,
    pub bolids_dir: PathBuf,
    pub simulation_hz: u32,
    pub debug_drawer_enabled: bool,
    pub jwks_url: String,
    pub jwt_audience: Vec<String>,
    pub jwt_issuers: Vec<String>,
    #[cfg(feature = "local")]
    pub local_sandbox_store_path: PathBuf,
    #[cfg(feature = "local")]
    pub local_max_active_sandboxes: u32,
    #[cfg(feature = "official")]
    pub official_database_url: String,
    #[cfg(feature = "official")]
    pub official_db_max_connections: u32,
    #[cfg(feature = "official")]
    pub build_service_grpc_endpoint: String,
    #[cfg(feature = "official")]
    pub build_service_submit_timeout_ms: u64,
    #[cfg(feature = "official")]
    pub build_service_get_timeout_ms: u64,
    #[cfg(feature = "official")]
    pub build_service_list_timeout_ms: u64,
    #[cfg(feature = "official")]
    pub build_service_cancel_timeout_ms: u64,
    #[cfg(feature = "official")]
    pub build_uploads_root: PathBuf,
    #[cfg(feature = "official")]
    pub build_upload_max_size_bytes: u64,
    #[cfg(feature = "official")]
    pub hps_endpoint: String,
    #[cfg(feature = "official")]
    pub hps_get_timeout_ms: u64,
    #[cfg(feature = "official")]
    pub hps_cache_ttl_ms: u64,
    #[cfg(feature = "official")]
    pub hps_edition: String,
}

impl Config {
    pub fn load_or_exit() -> Self {
        match Self::load() {
            Ok(cfg) => cfg,
            Err(err) => {
                tracing::error!("Failed to load config: {:#}", err);
                std::process::exit(1);
            }
        }
    }

    fn load() -> Result<Self, String> {
        let app_env = AppEnv::from_env();
        tracing::debug!(app_env = ?app_env, "resolved APP_ENV");
        let jwks_url = read_env_string("GAME_JWKS_URL").ok_or("GAME_JWKS_URL must be set")?;

        #[cfg(feature = "official")]
        let audience_env = "GAME_JWT_OFFICIAL_AUDIENCE";
        #[cfg(not(feature = "official"))]
        let audience_env = "GAME_JWT_LOCAL_AUDIENCE";

        #[cfg(feature = "official")]
        let issuers_env = "GAME_JWT_OFFICIAL_ISSUERS";
        #[cfg(not(feature = "official"))]
        let issuers_env = "GAME_JWT_LOCAL_ISSUERS";

        let jwt_audience =
            parse_list_env(audience_env)?.ok_or_else(|| format!("{audience_env} must be set"))?;
        let jwt_issuers =
            parse_list_env(issuers_env)?.ok_or_else(|| format!("{issuers_env} must be set"))?;

        let listen_addr = std::env::var("LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
            .parse::<SocketAddr>()
            .map_err(|e| format!("Invalid LISTEN_ADDR: {}", e))?;

        let raw_allow_origins = std::env::var("CORS_ALLOWED_ORIGINS").ok();

        #[cfg(feature = "local")]
        {
            let has_wildcard = match raw_allow_origins.as_deref() {
                None => true,
                Some(raw) => {
                    let trimmed = raw.trim();
                    trimmed.is_empty()
                        || trimmed == "*"
                        || raw.split(',').any(|entry| entry.trim() == "*")
                }
            };
            if has_wildcard {
                return Err(
                    "CORS_ALLOWED_ORIGINS must contain explicit origins in local mode".into(),
                );
            }
        }

        let allow_origin = match (app_env.is_production(), raw_allow_origins.as_deref()) {
            (true, None) => return Err("CORS_ALLOWED_ORIGINS must be set in production".into()),
            (true, Some(v)) if v.trim().is_empty() => {
                return Err("CORS_ALLOWED_ORIGINS cannot be empty in production".into());
            }
            (_, Some(v)) => parse_allow_origin(v)?,
            (false, None) => AllowOrigin::any(),
        };

        let raw_expose_headers = std::env::var("CORS_EXPOSE_HEADERS").ok();
        let expose_headers = match raw_expose_headers {
            Some(v) if !v.trim().is_empty() => parse_expose_headers(&v)?,
            _ => default_expose_headers(),
        };

        let tracks_rel = PathBuf::from("assets").join("tracks");
        let tracks_dir = resolve_dir("TRACKS_DIR", tracks_rel)
            .map_err(|e| format!("Failed to resolve tracks directory: {}", e))?;

        tracing::info!(path = %tracks_dir.display(), "using tracks directory");

        if tracks_dir
            .read_dir()
            .map_err(|e| e.to_string())?
            .next()
            .is_none()
        {
            tracing::warn!(path=%tracks_dir.display(), "tracks directory is empty");
        }

        let bolids_rel = PathBuf::from("assets").join("bolids");
        let bolids_dir = resolve_dir("BOLIDS_DIR", bolids_rel)
            .map_err(|e| format!("Failed to resolve bolids directory: {}", e))?;

        tracing::info!(path = %bolids_dir.display(), "using bolids directory");

        if bolids_dir
            .read_dir()
            .map_err(|e| e.to_string())?
            .next()
            .is_none()
        {
            tracing::warn!(path=%bolids_dir.display(), "bolids directory is empty");
        }

        let simulation_hz = std::env::var("SIMULATION_HZ")
            .unwrap_or_else(|_| "60".to_string())
            .parse::<u32>()
            .map_err(|e| format!("Invalid SIMULATION_HZ: {}", e))?;
        if simulation_hz == 0 {
            return Err("SIMULATION_HZ must be >= 1".into());
        }

        tracing::info!(simulation_hz, "server config");

        let debug_drawer_enabled = if cfg!(debug_assertions) {
            parse_bool_env("BOINK_DEBUG_DRAWER").unwrap_or(false)
        } else {
            false
        };

        if debug_drawer_enabled {
            tracing::info!("debug drawer enabled");
        }

        #[cfg(feature = "local")]
        let local_sandbox_store_path = default_local_sandbox_store_path();
        #[cfg(feature = "local")]
        let local_max_active_sandboxes = LOCAL_MAX_ACTIVE_SANDBOXES;

        #[cfg(feature = "official")]
        let official_database_url = read_env_string("OFFICIAL_DATABASE_URL")
            .ok_or("OFFICIAL_DATABASE_URL must be set for official backend")?;

        #[cfg(feature = "official")]
        let official_db_max_connections =
            parse_u32_env("OFFICIAL_DB_MAX_CONNECTIONS")?.unwrap_or(8);

        #[cfg(feature = "official")]
        let build_service_grpc_endpoint = read_env_string("BUILD_SERVICE_GRPC_ENDPOINT")
            .unwrap_or_else(|| DEFAULT_BUILD_SERVICE_GRPC_ENDPOINT.to_string());
        #[cfg(feature = "official")]
        let build_service_submit_timeout_ms = parse_u64_env("BUILD_SERVICE_SUBMIT_TIMEOUT_MS")?
            .unwrap_or(DEFAULT_BUILD_SERVICE_TIMEOUT_MS);
        #[cfg(feature = "official")]
        let build_service_get_timeout_ms = parse_u64_env("BUILD_SERVICE_GET_TIMEOUT_MS")?
            .unwrap_or(DEFAULT_BUILD_SERVICE_TIMEOUT_MS);
        #[cfg(feature = "official")]
        let build_service_list_timeout_ms = parse_u64_env("BUILD_SERVICE_LIST_TIMEOUT_MS")?
            .unwrap_or(DEFAULT_BUILD_SERVICE_TIMEOUT_MS);
        #[cfg(feature = "official")]
        let build_service_cancel_timeout_ms = parse_u64_env("BUILD_SERVICE_CANCEL_TIMEOUT_MS")?
            .unwrap_or(DEFAULT_BUILD_SERVICE_TIMEOUT_MS);
        #[cfg(feature = "official")]
        let build_uploads_root = read_env_string("BUILD_UPLOADS_ROOT")
            .and_then(|raw| {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(PathBuf::from(trimmed))
                }
            })
            .unwrap_or_else(default_build_uploads_root);
        #[cfg(feature = "official")]
        let build_upload_max_size_bytes = parse_u64_env("BUILD_UPLOAD_MAX_SIZE_BYTES")?
            .unwrap_or(DEFAULT_BUILD_UPLOAD_MAX_SIZE_BYTES);
        #[cfg(feature = "official")]
        let hps_endpoint =
            read_env_string("HPS_ENDPOINT").unwrap_or_else(|| DEFAULT_HPS_ENDPOINT.to_string());
        #[cfg(feature = "official")]
        let hps_get_timeout_ms =
            parse_u64_env("HPS_GET_TIMEOUT_MS")?.unwrap_or(DEFAULT_HPS_GET_TIMEOUT_MS);
        #[cfg(feature = "official")]
        let hps_cache_ttl_ms =
            parse_u64_env("HPS_CACHE_TTL_MS")?.unwrap_or(DEFAULT_HPS_CACHE_TTL_MS);
        #[cfg(feature = "official")]
        let hps_edition =
            read_env_string("HPS_EDITION").unwrap_or_else(|| DEFAULT_HPS_EDITION.to_string());
        #[cfg(feature = "official")]
        for (name, value) in [
            (
                "BUILD_SERVICE_SUBMIT_TIMEOUT_MS",
                build_service_submit_timeout_ms,
            ),
            ("BUILD_SERVICE_GET_TIMEOUT_MS", build_service_get_timeout_ms),
            (
                "BUILD_SERVICE_LIST_TIMEOUT_MS",
                build_service_list_timeout_ms,
            ),
            (
                "BUILD_SERVICE_CANCEL_TIMEOUT_MS",
                build_service_cancel_timeout_ms,
            ),
            ("BUILD_UPLOAD_MAX_SIZE_BYTES", build_upload_max_size_bytes),
            ("HPS_GET_TIMEOUT_MS", hps_get_timeout_ms),
            ("HPS_CACHE_TTL_MS", hps_cache_ttl_ms),
        ] {
            if value == 0 {
                return Err(format!("{name} must be >= 1"));
            }
        }
        #[cfg(feature = "official")]
        if hps_edition.trim().is_empty() {
            return Err("HPS_EDITION must be non-empty".into());
        }

        Ok(Self {
            env: app_env,
            listen_addr,
            allow_origin,
            expose_headers,
            tracks_dir,
            bolids_dir,
            simulation_hz,
            debug_drawer_enabled,
            jwks_url,
            jwt_audience,
            jwt_issuers,
            #[cfg(feature = "local")]
            local_sandbox_store_path,
            #[cfg(feature = "local")]
            local_max_active_sandboxes,
            #[cfg(feature = "official")]
            official_database_url,
            #[cfg(feature = "official")]
            official_db_max_connections,
            #[cfg(feature = "official")]
            build_service_grpc_endpoint,
            #[cfg(feature = "official")]
            build_service_submit_timeout_ms,
            #[cfg(feature = "official")]
            build_service_get_timeout_ms,
            #[cfg(feature = "official")]
            build_service_list_timeout_ms,
            #[cfg(feature = "official")]
            build_service_cancel_timeout_ms,
            #[cfg(feature = "official")]
            build_uploads_root,
            #[cfg(feature = "official")]
            build_upload_max_size_bytes,
            #[cfg(feature = "official")]
            hps_endpoint,
            #[cfg(feature = "official")]
            hps_get_timeout_ms,
            #[cfg(feature = "official")]
            hps_cache_ttl_ms,
            #[cfg(feature = "official")]
            hps_edition,
        })
    }
}

#[cfg(feature = "local")]
fn default_local_sandbox_store_path() -> PathBuf {
    if let Some(dir) = exe_dir() {
        return dir.join(LOCAL_SANDBOX_STORE_RELATIVE_PATH);
    }
    PathBuf::from(LOCAL_SANDBOX_STORE_RELATIVE_PATH)
}

#[cfg(feature = "official")]
fn default_build_uploads_root() -> PathBuf {
    if let Some(dir) = exe_dir() {
        return dir.join(DEFAULT_BUILD_UPLOADS_RELATIVE_PATH);
    }
    PathBuf::from(DEFAULT_BUILD_UPLOADS_RELATIVE_PATH)
}

fn exe_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()?
        .parent()
        .map(|p| p.to_path_buf())
}

fn resolve_dir<P: AsRef<Path>>(env_var: &str, default_rel: P) -> anyhow::Result<PathBuf> {
    let default_rel = default_rel.as_ref();
    let mut tried: Vec<(PathBuf, &'static str)> = Vec::new();

    if let Ok(v) = std::env::var(env_var) {
        let p = PathBuf::from(v);
        tried.push((p.clone(), "env"));

        if p.is_dir() {
            let p = p.canonicalize().unwrap_or(p);
            tracing::debug!(path = %p.display(), source = "env", "dir resolved");
            return Ok(p);
        }

        anyhow::bail!(
            "{} points to a non-existent directory: {}",
            env_var,
            tried[0].0.display()
        );
    }

    if let Some(dir) = exe_dir() {
        let p = dir.join(default_rel);
        tried.push((p.clone(), "exe_dir"));
        if p.is_dir() {
            let p = p.canonicalize().unwrap_or(p);
            tracing::debug!(path = %p.display(), source = "exe_dir", "dir resolved");
            return Ok(p);
        }

        #[cfg(debug_assertions)]
        {
            let dir_lc = dir.to_string_lossy().to_ascii_lowercase();
            if dir_lc.contains("\\target\\debug")
                || dir_lc.contains("/target/debug")
                || dir_lc.contains("\\target\\release")
                || dir_lc.contains("/target/release")
            {
                let m = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(default_rel);
                tried.push((m.clone(), "manifest_dev"));
                if m.is_dir() {
                    let m = m.canonicalize().unwrap_or(m);
                    tracing::debug!(path = %m.display(), source = "manifest_dev", "dir resolved");
                    return Ok(m);
                }
            }
        }
    }

    let list = tried
        .iter()
        .map(|(p, s)| format!("{} ({s})", p.display()))
        .collect::<Vec<_>>()
        .join("; ");

    Err(anyhow::anyhow!(
        "Could not resolve directory for {env_var}. Tried: {list}"
    ))
}

fn parse_allow_origin(raw: &str) -> Result<AllowOrigin, String> {
    if raw.trim().is_empty() || raw.trim() == "*" {
        return Ok(AllowOrigin::any());
    }

    let mut list = Vec::new();
    for part in raw.split(',') {
        let s = part.trim();
        if s.is_empty() {
            continue;
        }
        let hv = s
            .parse::<HeaderValue>()
            .map_err(|e| format!("Invalid CORS origin {:?}: {}", s, e))?;
        list.push(hv);
    }

    if list.is_empty() {
        return Ok(AllowOrigin::any());
    }

    Ok(AllowOrigin::list(list))
}

fn default_expose_headers() -> ExposeHeaders {
    let list = DEFAULT_EXPOSE_HEADERS
        .iter()
        .map(|h| HeaderName::from_static(h))
        .collect::<Vec<_>>();
    ExposeHeaders::list(list)
}

fn parse_expose_headers(raw: &str) -> Result<ExposeHeaders, String> {
    let mut list = DEFAULT_EXPOSE_HEADERS
        .iter()
        .map(|h| HeaderName::from_static(h))
        .collect::<Vec<_>>();

    for part in raw.split(',') {
        let header = part
            .trim()
            .parse::<HeaderName>()
            .map_err(|e| format!("Invalid CORS expose header: {}", e))?;
        if !list.contains(&header) {
            list.push(header);
        }
    }

    Ok(ExposeHeaders::list(list))
}

fn parse_bool_env(name: &str) -> Option<bool> {
    match std::env::var(name) {
        Ok(value) => match value.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON" => Some(true),
            "0" | "false" | "FALSE" | "no" | "NO" | "off" | "OFF" => Some(false),
            _ => None,
        },
        Err(_) => None,
    }
}

fn read_env_string(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Err(_) => None,
    }
}

fn parse_list_env(name: &str) -> Result<Option<Vec<String>>, String> {
    let Some(raw) = read_env_string(name) else {
        return Ok(None);
    };
    let list = raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| entry.to_string())
        .collect::<Vec<_>>();
    if list.is_empty() {
        return Err(format!("{name} cannot be empty"));
    }
    Ok(Some(list))
}

#[cfg(feature = "official")]
fn parse_u32_env(name: &str) -> Result<Option<u32>, String> {
    match read_env_string(name) {
        Some(value) => value
            .parse::<u32>()
            .map(Some)
            .map_err(|e| format!("Invalid {name}: {e}")),
        None => Ok(None),
    }
}

#[cfg(feature = "official")]
fn parse_u64_env(name: &str) -> Result<Option<u64>, String> {
    match read_env_string(name) {
        Some(value) => value
            .parse::<u64>()
            .map(Some)
            .map_err(|e| format!("Invalid {name}: {e}")),
        None => Ok(None),
    }
}
