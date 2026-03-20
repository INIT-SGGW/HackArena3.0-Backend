//! Shared JWKS-based JWT validation.

use std::time::{Duration, Instant};

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use proto::auth::v1::game_token_jwks_service_client::GameTokenJwksServiceClient;
use proto::auth::v1::{GetGameTokenJwksRequest, JwkEcCurve, JwkKeyType, JwtAlgorithm};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, RwLock};
use tonic::Status;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

const JWKS_CACHE_TTL: Duration = Duration::from_secs(300);
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const JWKS_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

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

struct JwksCache {
    jwks: Option<Jwks>,
    fetched_at: Option<Instant>,
}

/// Shared validator for ES256 JWTs signed by keys from a JWKS endpoint.
pub struct JwksValidator {
    hps_endpoint: String,
    channel: Channel,
    cache: RwLock<JwksCache>,
    fetch_lock: Mutex<()>,
    audience: Vec<String>,
    issuers: Vec<String>,
}

impl JwksValidator {
    /// Create a new validator with explicit HPS endpoint, audience and issuer constraints.
    pub fn new(hps_endpoint: &str, audience: Vec<String>, issuers: Vec<String>) -> Self {
        let endpoint = Endpoint::from_shared(hps_endpoint.to_string())
            .expect("GAME TOKEN JWKS gRPC endpoint must be a valid URI");
        let endpoint = if hps_endpoint.starts_with("https://") {
            endpoint
                .tls_config(ClientTlsConfig::new().with_enabled_roots())
                .expect("GAME TOKEN JWKS endpoint TLS config failed")
        } else {
            endpoint
        };
        let channel = endpoint
            .connect_timeout(JWKS_CONNECT_TIMEOUT)
            .connect_lazy();

        Self {
            hps_endpoint: hps_endpoint.to_string(),
            channel,
            cache: RwLock::new(JwksCache {
                jwks: None,
                fetched_at: None,
            }),
            fetch_lock: Mutex::new(()),
            audience,
            issuers,
        }
    }

    /// Decode and validate a JWT into the requested claims type.
    pub async fn decode_claims<T>(&self, token: &str) -> Result<T, Status>
    where
        T: DeserializeOwned,
    {
        let header = decode_header(token).map_err(|_| Status::unauthenticated("invalid jwt"))?;
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
            match decode::<T>(token, &key, &validation) {
                Ok(data) => return Ok(data.claims),
                Err(err) => tracing::debug!("jwt decode error: {err}"),
            }
        }

        Err(Status::unauthenticated("invalid jwt"))
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

        match self.fetch_jwks().await {
            Ok(jwks) => {
                let mut cache = self.cache.write().await;
                cache.jwks = Some(jwks.clone());
                cache.fetched_at = Some(Instant::now());
                Ok(jwks)
            }
            Err(err) => {
                let cache = self.cache.read().await;
                if let Some(jwks) = &cache.jwks {
                    tracing::warn!(
                        error = %err,
                        endpoint = %self.hps_endpoint,
                        "jwks refresh failed; using stale cached keys"
                    );
                    return Ok(jwks.clone());
                }
                Err(err)
            }
        }
    }

    async fn fetch_jwks(&self) -> Result<Jwks, Status> {
        let mut client = GameTokenJwksServiceClient::new(self.channel.clone());
        let response = tokio::time::timeout(
            JWKS_FETCH_TIMEOUT,
            client.get_game_token_jwks(GetGameTokenJwksRequest {}),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("jwks fetch timed out"))?
        .map_err(map_jwks_fetch_error)?
        .into_inner();

        let mut keys = Vec::new();
        for entry in response.keys {
            let Some(mapped) = map_proto_jwk(entry) else {
                continue;
            };
            keys.push(mapped);
        }

        if keys.is_empty() {
            return Err(Status::unavailable(
                "jwks response did not contain usable keys",
            ));
        }

        Ok(Jwks { keys })
    }
}

fn map_jwks_fetch_error(status: tonic::Status) -> Status {
    if status.code() == tonic::Code::DeadlineExceeded {
        return Status::deadline_exceeded("jwks fetch timed out");
    }
    if status.code() == tonic::Code::Unavailable {
        return Status::unavailable("jwks fetch unavailable");
    }
    Status::unavailable(format!("jwks fetch failed: {status}"))
}

fn map_proto_jwk(entry: proto::auth::v1::GameTokenJwk) -> Option<Jwk> {
    let key_type = JwkKeyType::try_from(entry.kty).ok()?;
    if key_type != JwkKeyType::Ec {
        return None;
    }

    let curve = JwkEcCurve::try_from(entry.crv).ok()?;
    if curve != JwkEcCurve::P256 {
        return None;
    }

    let alg = JwtAlgorithm::try_from(entry.alg).ok()?;
    if alg != JwtAlgorithm::Es256 {
        return None;
    }

    let x = entry.x.trim();
    let y = entry.y.trim();
    if x.is_empty() || y.is_empty() {
        return None;
    }

    let kid = entry.kid.trim();
    let kid = if kid.is_empty() {
        None
    } else {
        Some(kid.to_string())
    };

    Some(Jwk {
        kty: "EC".to_string(),
        kid,
        crv: Some("P-256".to_string()),
        x: Some(x.to_string()),
        y: Some(y.to_string()),
    })
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
