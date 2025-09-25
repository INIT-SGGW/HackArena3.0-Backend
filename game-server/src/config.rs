use http::{HeaderName, HeaderValue};
use std::env;
use std::net::SocketAddr;

use tower_http::cors::{AllowOrigin, ExposeHeaders};

const DEFAULT_EXPOSE_HEADERS: &[&str] = &["grpc-status", "grpc-message"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppEnv {
    Development,
    Production,
}

impl AppEnv {
    pub fn from_env() -> Self {
        match env::var("APP_ENV")
            .unwrap_or_else(|_| "development".to_string())
            .as_str()
        {
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
    pub listen_addr: SocketAddr,
    pub allow_origin: AllowOrigin,
    pub expose_headers: ExposeHeaders,
    pub env: AppEnv,
}

impl Config {
    pub fn load_or_exit() -> Self {
        match Self::load() {
            Ok(cfg) => cfg,
            Err(err) => {
                eprintln!("Failed to load config: {}", err);
                std::process::exit(1);
            }
        }
    }

    fn load() -> Result<Self, String> {
        let env = AppEnv::from_env();

        let listen_addr = env::var("LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".parse().unwrap())
            .parse::<SocketAddr>()
            .map_err(|e| format!("Invalid LISTEN_ADDR: {}", e))?;

        let raw_origins = env::var("CORS_ALLOWED_ORIGINS").ok();
        let allow_origin = match (env.is_production(), raw_origins) {
            (true, None) => return Err("CORS_ALLOWED_ORIGINS must be set in production".into()),
            (_, Some(v)) => parse_allow_origin(&v)?,
            (false, None) => AllowOrigin::any(),
        };

        let raw_expose_headers = env::var("CORS_EXPOSE_HEADERS").ok();
        let expose_headers = match raw_expose_headers {
            Some(v) if !v.trim().is_empty() => parse_expose_headers(&v)?,
            _ => default_expose_headers(),
        };

        Ok(Self {
            listen_addr,
            allow_origin,
            expose_headers,
            env,
        })
    }
}

fn parse_allow_origin(raw: &str) -> Result<AllowOrigin, String> {
    if raw.trim() == "*" {
        return Ok(AllowOrigin::any());
    }

    let list = raw
        .split(',')
        .map(|s| s.trim().parse::<HeaderValue>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Invalid CORS origin: {}", e))?;

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
