use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use http::{HeaderName, HeaderValue};
use tower_http::cors::{AllowOrigin, ExposeHeaders};

const DEFAULT_EXPOSE_HEADERS: &[&str] = &["grpc-status", "grpc-message"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppEnv {
    Development,
    Production,
}

impl AppEnv {
    pub fn from_env() -> Self {
        let v = std::env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());
        match v.to_ascii_lowercase().as_str() {
            "production" | "prod" => AppEnv::Production,
            _ => AppEnv::Development,
        }
    }

    pub fn is_production(self) -> bool {
        matches!(self, AppEnv::Production)
    }
}

#[derive(Debug)]
pub struct Config {
    pub env: AppEnv,
    pub listen_addr: SocketAddr,
    pub allow_origin: AllowOrigin,
    pub expose_headers: ExposeHeaders,
    pub tracks_dir: PathBuf,
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

        let listen_addr = std::env::var("LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
            .parse::<SocketAddr>()
            .map_err(|e| format!("Invalid LISTEN_ADDR: {}", e))?;

        let allow_origin = match (
            app_env.is_production(),
            std::env::var("CORS_ALLOWED_ORIGINS"),
        ) {
            (true, Err(_)) => return Err("CORS_ALLOWED_ORIGINS must be set in production".into()),
            (true, Ok(v)) if v.trim().is_empty() => {
                return Err("CORS_ALLOWED_ORIGINS cannot be empty in production".into());
            }
            (_, Ok(v)) => parse_allow_origin(&v)?,
            (false, Err(_)) => AllowOrigin::any(),
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

        Ok(Self {
            env: app_env,
            listen_addr,
            allow_origin,
            expose_headers,
            tracks_dir,
        })
    }
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
            tracing::info!(path = %p.display(), source = "env", "dir resolved");
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
            tracing::info!(path = %p.display(), source = "exe_dir", "dir resolved");
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
                    tracing::info!(path = %m.display(), source = "manifest_dev", "dir resolved");
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
