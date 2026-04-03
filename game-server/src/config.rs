use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use http::{HeaderName, HeaderValue};
use tower_http::cors::{AllowOrigin, ExposeHeaders};

const DEFAULT_EXPOSE_HEADERS: &[&str] = &["grpc-status", "grpc-message"];
const DEFAULT_HPS_ENDPOINT: &str = "http://127.0.0.1:50052";
const DEFAULT_API_URL: &str = "https://ha3-api.hackarena.pl";
#[cfg(feature = "official")]
const DEFAULT_SUBMISSION_ARCHIVE_MAX_MB: u32 = 25;
#[cfg(feature = "official")]
const DEFAULT_SUBMISSION_BUILD_TIMEOUT_SEC: u64 = 1_800;
#[cfg(feature = "local")]
const LOCAL_MAX_ACTIVE_SANDBOXES: u32 = 10;
#[cfg(feature = "local")]
const LOCAL_SANDBOX_STORE_RELATIVE_PATH: &str = "local/sandbox-configs.json";
#[cfg(all(feature = "local", not(feature = "standalone")))]
const LOCAL_TRACKS_CACHE_RELATIVE_PATH: &str = "local/tracks-cache";

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
    pub hps_endpoint: String,
    pub game_token_jwks_endpoint: String,
    pub jwt_audience: Vec<String>,
    pub jwt_issuers: Vec<String>,
    pub api_url: String,
    #[cfg(feature = "local")]
    pub broker_endpoint: String,
    #[cfg(feature = "local")]
    pub backend_endpoint: String,
    #[cfg(feature = "local")]
    pub local_sandbox_store_path: PathBuf,
    #[cfg(feature = "local")]
    pub local_tracks_cache_dir: PathBuf,
    #[cfg(feature = "local")]
    pub local_max_active_sandboxes: u32,
    #[cfg(feature = "official")]
    pub local_tracks_dir: Option<PathBuf>,
    #[cfg(feature = "official")]
    pub official_database_url: String,
    #[cfg(feature = "official")]
    pub official_db_max_connections: u32,
    #[cfg(feature = "official")]
    pub builder_host: String,
    #[cfg(feature = "official")]
    pub builder_ssh_key_path: PathBuf,
    #[cfg(feature = "official")]
    pub builder_ssh_known_hosts_file: PathBuf,
    #[cfg(feature = "official")]
    pub registry: String,
    #[cfg(feature = "official")]
    pub submission_archive_max_mb: u32,
    #[cfg(feature = "official")]
    pub submission_build_timeout_sec: u64,
    #[cfg(feature = "official")]
    pub keycloak_token_url: String,
    #[cfg(feature = "official")]
    pub keycloak_client_id: String,
    #[cfg(feature = "official")]
    pub keycloak_client_secret: String,
    #[cfg(feature = "official")]
    pub keycloak_ha3_wrapper_client_id: String,
    #[cfg(feature = "official")]
    pub keycloak_ha3_wrapper_client_secret: String,
    #[cfg(feature = "official")]
    pub game_token_issuer_endpoint: String,
    #[cfg(feature = "official")]
    pub official_bot_backend_endpoint: String,
    #[cfg(feature = "official")]
    pub wrapper_gh_owner: String,
    #[cfg(feature = "official")]
    pub wrapper_python_gh_repo: String,
    #[cfg(feature = "official")]
    pub wrapper_csharp_gh_repo: String,
    #[cfg(feature = "official")]
    pub wrapper_typescript_gh_repo: String,
    #[cfg(feature = "official")]
    pub gh_token: Option<String>,
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
        let hps_endpoint =
            read_env_string("HPS_ENDPOINT").unwrap_or_else(|| DEFAULT_HPS_ENDPOINT.to_string());

        #[cfg(feature = "official")]
        let audience_env = "GAME_JWT_OFFICIAL_AUDIENCE";
        #[cfg(not(feature = "official"))]
        let audience_env = "GAME_JWT_LOCAL_AUDIENCE";

        #[cfg(feature = "official")]
        let issuers_env = "GAME_JWT_OFFICIAL_ISSUERS";
        #[cfg(not(feature = "official"))]
        let issuers_env = "GAME_JWT_LOCAL_ISSUERS";

        #[cfg(not(feature = "standalone"))]
        let jwt_audience =
            parse_list_env(audience_env)?.ok_or_else(|| format!("{audience_env} must be set"))?;
        #[cfg(not(feature = "standalone"))]
        let jwt_issuers =
            parse_list_env(issuers_env)?.ok_or_else(|| format!("{issuers_env} must be set"))?;
        #[cfg(feature = "standalone")]
        let jwt_audience = parse_list_env(audience_env)?.unwrap_or_default();
        #[cfg(feature = "standalone")]
        let jwt_issuers = parse_list_env(issuers_env)?.unwrap_or_default();

        let listen_addr = std::env::var("LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
            .parse::<SocketAddr>()
            .map_err(|e| format!("Invalid LISTEN_ADDR: {}", e))?;

        let raw_allow_origins = std::env::var("CORS_ALLOWED_ORIGINS").ok();

        #[cfg(all(feature = "local", not(feature = "standalone")))]
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

        #[cfg(feature = "official")]
        let tracks_dir = {
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
            tracks_dir
        };
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let tracks_dir = {
            let dir = resolve_local_tracks_cache_dir()
                .map_err(|e| format!("Failed to resolve local tracks cache directory: {e}"))?;
            tracing::info!(path = %dir.display(), "using local tracks cache directory");
            dir
        };
        #[cfg(all(feature = "local", feature = "standalone"))]
        let tracks_dir = {
            let tracks_rel = PathBuf::from("assets").join("tracks");
            let tracks_dir = resolve_dir("TRACKS_DIR", tracks_rel)
                .map_err(|e| format!("Failed to resolve tracks directory: {}", e))?;

            tracing::info!(
                path = %tracks_dir.display(),
                "using standalone tracks bundle directory"
            );
            tracks_dir
        };

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

        let api_url = read_env_string("API_URL").unwrap_or_else(|| DEFAULT_API_URL.to_string());
        #[cfg(not(feature = "standalone"))]
        let game_token_jwks_endpoint = to_game_token_jwks_endpoint(&api_url)?;
        #[cfg(feature = "standalone")]
        let game_token_jwks_endpoint = "http://127.0.0.1:65535/gametoken".to_string();
        #[cfg(feature = "official")]
        let game_token_issuer_endpoint = to_game_token_issuer_endpoint(&api_url)?;
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let broker_endpoint = to_broker_endpoint(&api_url)?;
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let backend_endpoint = to_backend_endpoint(&api_url)?;
        #[cfg(all(feature = "local", feature = "standalone"))]
        let broker_endpoint = String::new();
        #[cfg(all(feature = "local", feature = "standalone"))]
        let backend_endpoint = String::new();

        #[cfg(feature = "local")]
        let local_sandbox_store_path = default_local_sandbox_store_path();
        #[cfg(feature = "local")]
        let local_tracks_cache_dir = tracks_dir.clone();
        #[cfg(feature = "local")]
        let local_max_active_sandboxes = LOCAL_MAX_ACTIVE_SANDBOXES;
        #[cfg(feature = "official")]
        let local_tracks_dir = resolve_optional_dir("LOCAL_TRACKS_DIR")
            .map_err(|e| format!("Failed to resolve LOCAL_TRACKS_DIR: {e}"))?;
        #[cfg(feature = "official")]
        if let Some(path) = &local_tracks_dir {
            tracing::warn!(
                path = %path.display(),
                "LOCAL_TRACKS_DIR is deprecated/ignored in official backend; AssetService uses TRACKS_DIR bundle layout"
            );
        }

        #[cfg(feature = "official")]
        let official_database_url = read_env_string("OFFICIAL_DATABASE_URL")
            .ok_or("OFFICIAL_DATABASE_URL must be set for official backend")?;

        #[cfg(feature = "official")]
        let official_db_max_connections =
            parse_u32_env("OFFICIAL_DB_MAX_CONNECTIONS")?.unwrap_or(8);
        #[cfg(feature = "official")]
        let builder_host = read_env_string("BUILDER_HOST")
            .ok_or("BUILDER_HOST must be set for official backend")?;
        #[cfg(feature = "official")]
        let builder_ssh_key_path = {
            let raw = read_env_string("BUILDER_SSH_KEY_PATH")
                .ok_or("BUILDER_SSH_KEY_PATH must be set for official backend")?;
            let path = PathBuf::from(raw);
            if !path.is_file() {
                return Err(format!(
                    "BUILDER_SSH_KEY_PATH points to a non-existent file: {}",
                    path.display()
                ));
            }
            path.canonicalize().unwrap_or(path)
        };
        #[cfg(feature = "official")]
        let builder_ssh_known_hosts_file = PathBuf::from(
            read_env_string("BUILDER_SSH_KNOWN_HOSTS_FILE")
                .unwrap_or_else(|| ".submissions/known_hosts".to_string()),
        );
        #[cfg(feature = "official")]
        let registry =
            read_env_string("REGISTRY").ok_or("REGISTRY must be set for official backend")?;
        #[cfg(feature = "official")]
        let keycloak_token_url = validate_http_url(
            "KEYCLOAK_TOKEN_URL",
            &read_env_string("KEYCLOAK_TOKEN_URL")
                .ok_or("KEYCLOAK_TOKEN_URL must be set for official backend")?,
        )?;
        #[cfg(feature = "official")]
        let keycloak_client_id = read_env_string("KEYCLOAK_CLIENT_ID")
            .ok_or("KEYCLOAK_CLIENT_ID must be set for official backend")?;
        #[cfg(feature = "official")]
        let keycloak_client_secret = read_env_string("KEYCLOAK_CLIENT_SECRET")
            .ok_or("KEYCLOAK_CLIENT_SECRET must be set for official backend")?;
        #[cfg(feature = "official")]
        let keycloak_ha3_wrapper_client_id = read_env_string("KEYCLOAK_HA3_WRAPPER_CLIENT_ID")
            .ok_or("KEYCLOAK_HA3_WRAPPER_CLIENT_ID must be set for official backend")?;
        #[cfg(feature = "official")]
        let keycloak_ha3_wrapper_client_secret =
            read_env_string("KEYCLOAK_HA3_WRAPPER_CLIENT_SECRET")
                .ok_or("KEYCLOAK_HA3_WRAPPER_CLIENT_SECRET must be set for official backend")?;
        #[cfg(feature = "official")]
        let official_bot_backend_endpoint = validate_http_url(
            "OFFICIAL_BOT_BACKEND_ENDPOINT",
            &read_env_string("OFFICIAL_BOT_BACKEND_ENDPOINT")
                .ok_or("OFFICIAL_BOT_BACKEND_ENDPOINT must be set for official backend")?,
        )?;
        #[cfg(feature = "official")]
        let wrapper_gh_owner = read_env_string("WRAPPER_GH_OWNER")
            .ok_or("WRAPPER_GH_OWNER must be set for official backend")?;
        #[cfg(feature = "official")]
        let wrapper_python_gh_repo = read_env_string("WRAPPER_PYTHON_GH_REPO")
            .ok_or("WRAPPER_PYTHON_GH_REPO must be set for official backend")?;
        #[cfg(feature = "official")]
        let wrapper_csharp_gh_repo = read_env_string("WRAPPER_CSHARP_GH_REPO")
            .ok_or("WRAPPER_CSHARP_GH_REPO must be set for official backend")?;
        #[cfg(feature = "official")]
        let wrapper_typescript_gh_repo = read_env_string("WRAPPER_TYPESCRIPT_GH_REPO")
            .ok_or("WRAPPER_TYPESCRIPT_GH_REPO must be set for official backend")?;
        #[cfg(feature = "official")]
        let gh_token = read_env_string("GH_TOKEN");
        #[cfg(feature = "official")]
        let submission_archive_max_mb = parse_u32_env("SUBMISSION_ARCHIVE_MAX_MB")?
            .unwrap_or(DEFAULT_SUBMISSION_ARCHIVE_MAX_MB);
        #[cfg(feature = "official")]
        if submission_archive_max_mb == 0 {
            return Err("SUBMISSION_ARCHIVE_MAX_MB must be >= 1".into());
        }
        #[cfg(feature = "official")]
        let submission_build_timeout_sec = parse_u64_env("SUBMISSION_BUILD_TIMEOUT_SEC")?
            .unwrap_or(DEFAULT_SUBMISSION_BUILD_TIMEOUT_SEC);
        #[cfg(feature = "official")]
        if submission_build_timeout_sec == 0 {
            return Err("SUBMISSION_BUILD_TIMEOUT_SEC must be >= 1".into());
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
            hps_endpoint,
            game_token_jwks_endpoint,
            jwt_audience,
            jwt_issuers,
            api_url,
            #[cfg(feature = "local")]
            broker_endpoint,
            #[cfg(feature = "local")]
            backend_endpoint,
            #[cfg(feature = "local")]
            local_sandbox_store_path,
            #[cfg(feature = "local")]
            local_tracks_cache_dir,
            #[cfg(feature = "local")]
            local_max_active_sandboxes,
            #[cfg(feature = "official")]
            local_tracks_dir,
            #[cfg(feature = "official")]
            official_database_url,
            #[cfg(feature = "official")]
            official_db_max_connections,
            #[cfg(feature = "official")]
            builder_host,
            #[cfg(feature = "official")]
            builder_ssh_key_path,
            #[cfg(feature = "official")]
            builder_ssh_known_hosts_file,
            #[cfg(feature = "official")]
            registry,
            #[cfg(feature = "official")]
            submission_archive_max_mb,
            #[cfg(feature = "official")]
            submission_build_timeout_sec,
            #[cfg(feature = "official")]
            keycloak_token_url,
            #[cfg(feature = "official")]
            keycloak_client_id,
            #[cfg(feature = "official")]
            keycloak_client_secret,
            #[cfg(feature = "official")]
            keycloak_ha3_wrapper_client_id,
            #[cfg(feature = "official")]
            keycloak_ha3_wrapper_client_secret,
            #[cfg(feature = "official")]
            game_token_issuer_endpoint,
            #[cfg(feature = "official")]
            official_bot_backend_endpoint,
            #[cfg(feature = "official")]
            wrapper_gh_owner,
            #[cfg(feature = "official")]
            wrapper_python_gh_repo,
            #[cfg(feature = "official")]
            wrapper_csharp_gh_repo,
            #[cfg(feature = "official")]
            wrapper_typescript_gh_repo,
            #[cfg(feature = "official")]
            gh_token,
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

#[cfg(all(feature = "local", not(feature = "standalone")))]
fn to_broker_endpoint(api_url: &str) -> Result<String, String> {
    let trimmed = validate_api_url(api_url)?;
    Ok(format!("{trimmed}/broker"))
}

#[cfg(not(feature = "standalone"))]
fn to_game_token_jwks_endpoint(api_url: &str) -> Result<String, String> {
    let trimmed = validate_api_url(api_url)?;
    Ok(format!("{trimmed}/gametoken"))
}

#[cfg(feature = "official")]
fn to_game_token_issuer_endpoint(api_url: &str) -> Result<String, String> {
    let trimmed = validate_api_url(api_url)?;
    Ok(format!("{trimmed}/gametoken"))
}

#[cfg(all(feature = "local", not(feature = "standalone")))]
fn to_backend_endpoint(api_url: &str) -> Result<String, String> {
    let trimmed = validate_api_url(api_url)?;
    Ok(format!("{trimmed}/backend"))
}

#[cfg(not(feature = "standalone"))]
fn validate_api_url(api_url: &str) -> Result<&str, String> {
    let trimmed = api_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("API_URL cannot be empty".into());
    }

    if !trimmed.starts_with("http://") && !trimmed.starts_with("https://") {
        return Err("API_URL must start with http:// or https://".into());
    }

    Ok(trimmed)
}

#[cfg(feature = "official")]
fn validate_http_url(name: &str, raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(format!("{name} cannot be empty"));
    }

    let uri = trimmed
        .parse::<http::Uri>()
        .map_err(|e| format!("Invalid {name}: {e}"))?;

    match uri.scheme_str() {
        Some("http") | Some("https") => {}
        _ => return Err(format!("{name} must start with http:// or https://")),
    }

    if uri.authority().is_none() {
        return Err(format!("{name} must include host"));
    }

    Ok(trimmed.to_string())
}

fn exe_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()?
        .parent()
        .map(|p| p.to_path_buf())
}

#[cfg(all(feature = "local", not(feature = "standalone")))]
fn resolve_local_tracks_cache_dir() -> anyhow::Result<PathBuf> {
    let path = if let Some(raw) = read_env_string("LOCAL_TRACKS_CACHE_DIR") {
        PathBuf::from(raw)
    } else if let Some(dir) = exe_dir() {
        dir.join(LOCAL_TRACKS_CACHE_RELATIVE_PATH)
    } else {
        PathBuf::from(LOCAL_TRACKS_CACHE_RELATIVE_PATH)
    };
    std::fs::create_dir_all(&path)?;
    Ok(path.canonicalize().unwrap_or(path))
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

#[cfg(feature = "official")]
fn resolve_optional_dir(env_var: &str) -> anyhow::Result<Option<PathBuf>> {
    let Some(raw) = read_env_string(env_var) else {
        return Ok(None);
    };
    let path = PathBuf::from(raw);
    if !path.is_dir() {
        anyhow::bail!(
            "{env_var} points to a non-existent directory: {}",
            path.display()
        );
    }
    Ok(Some(path.canonicalize().unwrap_or(path)))
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
