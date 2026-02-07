//! JWT validation helpers backed by a JWKS endpoint.
//!
//! The validator fetches and caches the JSON Web Key Set (JWKS), then uses it
//! to verify ES256 tokens and extract common authorization claims.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::{Mutex, RwLock};
use tonic::Status;
use tonic::metadata::MetadataMap;

const JWKS_CACHE_TTL: Duration = Duration::from_secs(300);
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const JWKS_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_AUDIENCE_OFFICIAL: &str = "ha3-official";
pub const DEFAULT_ISSUERS_OFFICIAL: [&str; 1] = ["ha3-auth"];
pub const DEFAULT_AUDIENCE_LOCAL: &str = "ha3-dev-auth";
pub const DEFAULT_ISSUERS_LOCAL: [&str; 1] = ["ha3-local"];

#[derive(Clone, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Clone, Deserialize)]
struct Jwk {
    kty: String,
    kid: Option<String>,
    crv: Option<String>,
    x: Option<String>,
    y: Option<String>,
}

#[derive(Clone, Deserialize)]
struct TokenClaims {
    scope: Option<String>,
    scp: Option<Vec<String>>,
    instance_uuid: Option<String>,
}

struct JwksCache {
    jwks: Option<Jwks>,
    fetched_at: Option<Instant>,
}

/// Validates JWTs against a JWKS URL and extracts authorization claims.
pub struct TokenValidator {
    jwks_url: String,
    client: Client,
    cache: RwLock<JwksCache>,
    fetch_lock: Mutex<()>,
    audience: Vec<String>,
    issuers: Vec<String>,
}

impl TokenValidator {
    /// Create a new validator with explicit audience and issuer configuration.
    pub fn new_with_config(jwks_url: &str, audience: Vec<String>, issuers: Vec<String>) -> Self {
        Self {
            jwks_url: jwks_url.to_string(),
            client: Client::builder()
                .timeout(JWKS_FETCH_TIMEOUT)
                .connect_timeout(JWKS_CONNECT_TIMEOUT)
                .build()
                .expect("jwt jwks client must build"),
            cache: RwLock::new(JwksCache {
                jwks: None,
                fetched_at: None,
            }),
            fetch_lock: Mutex::new(()),
            audience,
            issuers,
        }
    }

    /// Validate the token and return a de-duplicated list of scopes.
    pub async fn scopes_from_token(&self, token: &str) -> Result<Vec<String>, Status> {
        let claims = self.claims_from_token(token).await?;
        Ok(extract_scopes(&claims))
    }

    /// Validate the token and return the instance UUID claim, if present.
    pub async fn instance_uuid_from_token(&self, token: &str) -> Result<Option<String>, Status> {
        let claims = self.claims_from_token(token).await?;
        Ok(claims.instance_uuid)
    }

    async fn claims_from_token(&self, token: &str) -> Result<TokenClaims, Status> {
        let header =
            decode_header(token).map_err(|_| Status::unauthenticated("invalid bearer token"))?;
        let alg = header.alg;
        if alg != Algorithm::ES256 {
            return Err(Status::unauthenticated("unsupported jwt algorithm"));
        }
        let jwks = self.jwks().await?;
        let mut candidates: Vec<&Jwk> = if let Some(kid) = header.kid.as_deref() {
            jwks.keys
                .iter()
                .filter(|k| k.kid.as_deref() == Some(kid))
                .collect()
        } else {
            jwks.keys.iter().collect()
        };
        if candidates.is_empty() {
            candidates = jwks.keys.iter().collect();
        }
        if candidates.is_empty() {
            return Err(Status::unauthenticated("no jwk keys available"));
        }

        for jwk in candidates {
            let key = match jwk_to_key(jwk) {
                Ok(key) => key,
                Err(_) => continue,
            };
            let mut validation = Validation::new(alg);
            let audience: Vec<&str> = self.audience.iter().map(String::as_str).collect();
            let issuers: Vec<&str> = self.issuers.iter().map(String::as_str).collect();
            validation.set_audience(&audience);
            validation.set_issuer(&issuers);
            validation.validate_exp = true;
            validation.required_spec_claims.insert("exp".to_string());
            match decode::<TokenClaims>(token, &key, &validation) {
                Ok(data) => return Ok(data.claims),
                Err(err) => tracing::debug!("jwt decode error: {err}"),
            }
        }

        Err(Status::unauthenticated("invalid bearer token"))
    }

    async fn jwks(&self) -> Result<Jwks, Status> {
        {
            let cache = self.cache.read().await;
            if let (Some(jwks), Some(fetched_at)) = (&cache.jwks, cache.fetched_at) {
                if fetched_at.elapsed() < JWKS_CACHE_TTL {
                    return Ok(jwks.clone());
                }
            }
        }

        let _guard = self.fetch_lock.lock().await;
        {
            let cache = self.cache.read().await;
            if let (Some(jwks), Some(fetched_at)) = (&cache.jwks, cache.fetched_at) {
                if fetched_at.elapsed() < JWKS_CACHE_TTL {
                    return Ok(jwks.clone());
                }
            }
        }

        let jwks = self.fetch_jwks().await?;
        let mut cache = self.cache.write().await;
        cache.jwks = Some(jwks.clone());
        cache.fetched_at = Some(Instant::now());
        Ok(jwks)
    }

    async fn fetch_jwks(&self) -> Result<Jwks, Status> {
        let resp = self
            .client
            .get(&self.jwks_url)
            .send()
            .await
            .map_err(map_jwks_fetch_error)?;
        let status = resp.status();
        if !status.is_success() {
            if status.is_client_error() {
                return Err(Status::failed_precondition(format!(
                    "jwks endpoint rejected request ({status})"
                )));
            }
            return Err(Status::unavailable(format!(
                "jwks endpoint error ({status})"
            )));
        }
        resp.json::<Jwks>()
            .await
            .map_err(|_| Status::internal("invalid jwks response"))
    }
}

fn map_jwks_fetch_error(err: reqwest::Error) -> Status {
    if err.is_timeout() {
        return Status::deadline_exceeded("jwks fetch timed out");
    }
    if err.is_connect() {
        return Status::unavailable("jwks connect failed");
    }
    Status::unavailable("jwks fetch failed")
}

fn jwk_to_key(jwk: &Jwk) -> Result<DecodingKey, Status> {
    if jwk.kty != "EC" {
        return Err(Status::unauthenticated("unsupported jwk"));
    }
    if jwk.crv.as_deref() != Some("P-256") {
        return Err(Status::unauthenticated("unsupported jwk curve"));
    }
    let x = jwk
        .x
        .as_deref()
        .ok_or_else(|| Status::unauthenticated("invalid jwk"))?;
    let y = jwk
        .y
        .as_deref()
        .ok_or_else(|| Status::unauthenticated("invalid jwk"))?;
    DecodingKey::from_ec_components(x, y).map_err(|_| Status::unauthenticated("invalid jwk"))
}

fn extract_scopes(claims: &TokenClaims) -> Vec<String> {
    let mut scopes = HashSet::new();
    if let Some(scope) = &claims.scope {
        for entry in scope.split_whitespace() {
            if !entry.is_empty() {
                scopes.insert(entry.to_string());
            }
        }
    }
    if let Some(list) = &claims.scp {
        for entry in list {
            if !entry.is_empty() {
                scopes.insert(entry.clone());
            }
        }
    }
    scopes.into_iter().collect()
}

/// Parse the `x-ha3-game-token` metadata entry and return a token if present.
pub fn parse_game_token(metadata: &MetadataMap) -> Result<Option<String>, Status> {
    let Some(value) = metadata.get("x-ha3-game-token") else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| Status::unauthenticated("invalid x-ha3-game-token header"))?;
    if raw.trim().is_empty() {
        return Err(Status::unauthenticated("empty x-ha3-game-token"));
    }
    Ok(Some(raw.to_string()))
}
