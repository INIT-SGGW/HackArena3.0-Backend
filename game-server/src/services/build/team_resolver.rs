use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use proto::hackarena::platform::common::v1::Uuid;
use proto::hackarena::platform::teams::v1::GetTeamByUserRequest;
use proto::hackarena::platform::teams::v1::teams_service_client::TeamsServiceClient;
use thiserror::Error;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

use crate::config::Config;

const CONNECT_TIMEOUT_MS: u64 = 2_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTeam {
    pub team_id: String,
    pub team_name: String,
}

#[derive(Debug, Clone)]
struct CachedTeamEntry {
    team: ResolvedTeam,
    expires_at_ms: i64,
}

/// Resolver for mapping authenticated subject (`sub`) to team context.
#[derive(Clone)]
pub struct BuildTeamResolver {
    channel: Channel,
    get_timeout: Duration,
    cache_ttl_ms: i64,
    edition: String,
    cache_by_subject: Arc<RwLock<HashMap<String, CachedTeamEntry>>>,
}

impl BuildTeamResolver {
    pub fn from_config(cfg: &Config) -> Result<Self, BuildTeamResolverError> {
        let endpoint_raw = cfg.hps_endpoint.clone();
        let endpoint = Endpoint::from_shared(endpoint_raw.clone()).map_err(|source| {
            BuildTeamResolverError::InvalidEndpoint {
                endpoint: endpoint_raw.clone(),
                source,
            }
        })?;
        let channel = endpoint
            .connect_timeout(Duration::from_millis(CONNECT_TIMEOUT_MS))
            .connect_lazy();

        let cache_ttl_ms = i64::try_from(cfg.hps_cache_ttl_ms)
            .map_err(|_| BuildTeamResolverError::InvalidCacheTtl)?;

        Ok(Self {
            channel,
            get_timeout: Duration::from_millis(cfg.hps_get_timeout_ms),
            cache_ttl_ms,
            edition: cfg.hps_edition.clone(),
            cache_by_subject: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn resolve_team_for_subject(
        &self,
        subject: &str,
    ) -> Result<ResolvedTeam, BuildTeamResolverError> {
        let subject = subject.trim();
        if subject.is_empty() {
            return Err(BuildTeamResolverError::InvalidSubject);
        }

        let now_ms = now_ms();
        if let Some(cached) = self.cache_by_subject.read().await.get(subject) {
            if cached.expires_at_ms > now_ms {
                return Ok(cached.team.clone());
            }
        }

        let mut client = TeamsServiceClient::new(self.channel.clone());
        let request = GetTeamByUserRequest {
            user_id: Some(Uuid {
                value: subject.to_string(),
            }),
            edition: self.edition.clone(),
        };
        let response = tokio::time::timeout(self.get_timeout, client.get_team_by_user(request))
            .await
            .map_err(|_| BuildTeamResolverError::Timeout {
                timeout_ms: duration_to_ms(self.get_timeout),
            })?
            .map_err(|status| BuildTeamResolverError::GrpcStatus { status })?
            .into_inner();

        let team = response
            .team
            .ok_or_else(|| BuildTeamResolverError::TeamNotFound {
                subject: subject.to_string(),
            })?;
        let team_id = team
            .id
            .map(|id| id.value)
            .unwrap_or_default()
            .trim()
            .to_string();
        if team_id.is_empty() {
            return Err(BuildTeamResolverError::MissingTeamId {
                subject: subject.to_string(),
            });
        }

        let resolved = ResolvedTeam {
            team_id,
            team_name: team.name,
        };

        let expires_at_ms = now_ms.saturating_add(self.cache_ttl_ms);
        self.cache_by_subject.write().await.insert(
            subject.to_string(),
            CachedTeamEntry {
                team: resolved.clone(),
                expires_at_ms,
            },
        );

        Ok(resolved)
    }
}

#[derive(Debug, Error)]
pub enum BuildTeamResolverError {
    #[error("invalid HPS_ENDPOINT `{endpoint}`: {source}")]
    InvalidEndpoint {
        endpoint: String,
        source: tonic::transport::Error,
    },
    #[error("HPS cache ttl does not fit into i64")]
    InvalidCacheTtl,
    #[error("subject (`sub`) must be non-empty")]
    InvalidSubject,
    #[error("GetTeamByUser timed out after {timeout_ms} ms")]
    Timeout { timeout_ms: u64 },
    #[error("GetTeamByUser failed: {status}")]
    GrpcStatus { status: tonic::Status },
    #[error("no team found for subject `{subject}`")]
    TeamNotFound { subject: String },
    #[error("team response for subject `{subject}` did not contain team id")]
    MissingTeamId { subject: String },
}

fn duration_to_ms(duration: Duration) -> u64 {
    duration
        .as_millis()
        .min(u128::from(u64::MAX))
        .try_into()
        .unwrap_or(u64::MAX)
}

fn now_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration
            .as_millis()
            .min(i64::MAX as u128)
            .try_into()
            .unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}
