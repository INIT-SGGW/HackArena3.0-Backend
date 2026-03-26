//! gRPC SubmissionService implementation with async remote build worker.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use boink::error::Error as BoinkError;
use dashmap::DashMap;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use proto::achievement::v1::achievement_service_client::AchievementServiceClient;
use proto::achievement::v1::{GrantLogsAchievementRequest, GrantLogsFailReason};
use proto::auth::v1::game_token_issuer_service_client::GameTokenIssuerServiceClient;
use proto::auth::v1::{GameTokenIssueType, IssueGameTokenRequest};
use proto::hackarena::platform::common::v1::Uuid as PlatformUuid;
use proto::hackarena::platform::teams::v1::teams_service_client::TeamsServiceClient;
use proto::hackarena::platform::teams::v1::{
    GetEventTeamsRequest, GetTeamByUserRequest, Team, TeamEvent,
};
use proto::submission::v1::official_sandbox_command_service_server::OfficialSandboxCommandService;
use proto::submission::v1::slot_query_service_server::SlotQueryService;
use proto::submission::v1::submission_service_server::SubmissionService;
use proto::submission::v1::{
    BuildFinished, BuildLog, BuildStarted, GetSlotsRequest, GetSlotsResponse,
    GetSubmissionArchiveRequest, GetSubmissionArchiveResponse, GetSubmissionLogsRequest,
    GetSubmissionLogsResponse, JoinOfficialSandboxRequest, JoinOfficialSandboxResponse,
    LeaveOfficialSandboxRequest, LeaveOfficialSandboxResponse, ListRecentSubmissionsRequest,
    ListRecentSubmissionsResponse, OfficialSandboxCommandStatus, RecentSubmissionDto, SlotDto,
    SlotSummaryDto, StreamSlotsRequest, StreamSlotsResponse, SubmitBuildRequest,
    SubmitBuildStreamResponse, WrapperKind, submit_build_stream_response,
};
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, USER_AGENT};
use serde::Deserialize;
use tar::{Archive, Builder};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::http::Uri;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request, Response, Status};

use crate::auth::auth_claims::TokenValidator;
use crate::config::Config;
use crate::db::repos::submission::{NewSubmissionRecord, SubmissionRepo};
use crate::runtime::engine_worker::{EngineClient, EngineCommandTarget, EngineWorkerError};
use crate::services::error_map::map_worker_err;
use crate::services::log_redaction::redact_log_line;
use crate::services::race::{RaceRuntimeStore, RuntimeCarIdentity};

const TEAM_EDITION: &str = "3";
const TEAM_CACHE_TTL: Duration = Duration::from_secs(300);
const HPS_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const GAME_TOKEN_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const ACHIEVEMENT_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const ACHIEVEMENT_TRANSIENT_RETRIES: u32 = 2;
const ACHIEVEMENT_RETRY_BACKOFF_BASE_MS: u64 = 200;
const KEYCLOAK_TOKEN_TIMEOUT: Duration = Duration::from_secs(10);
const SERVICE_TOKEN_DEFAULT_TTL_SEC: u64 = 300;
const SERVICE_TOKEN_TTL_SAFETY_SEC: u64 = 30;
const SERVICE_TOKEN_MIN_TTL_SEC: u64 = 10;
const BOT_DOCKER_TIMEOUT: Duration = Duration::from_secs(60);
const SUBMISSION_QUEUE_CAPACITY: usize = 64;
const SLOT_UPDATE_CHANNEL_CAPACITY: usize = 128;
const SLOT_STREAM_CHANNEL_CAPACITY: usize = 8;
const BUILD_EVENT_CHANNEL_CAPACITY: usize = 128;
const LIST_RECENT_SUBMISSIONS_LIMIT: i64 = 1000;
const SUBMISSIONS_ROOT: &str = ".submissions";
const LEGACY_BOT_LOGS_SUBDIR: &str = "bot-logs";
const TEAM_SUBMISSIONS_SUBDIR: &str = "submissions";
const TEAM_LOGS_SUBDIR: &str = "logs";
const GITHUB_API_BASE_URL: &str = "https://api.github.com";
const GITHUB_API_TIMEOUT: Duration = Duration::from_secs(30);
const REGISTRY_API_TIMEOUT: Duration = Duration::from_secs(10);
const REGISTRY_MANIFEST_V2_ACCEPT: &str = "application/vnd.docker.distribution.manifest.v2+json";
const REGISTRY_DIGEST_HEADER: &str = "docker-content-digest";
const DEFAULT_CSHARP_DOTNET_RUNTIME_VERSION: &str = "8.0";

#[derive(Debug, Clone)]
pub(crate) struct SubmissionBuildJob {
    submission_id: String,
    team_id: String,
    slot_index: i16,
    wrapper_kind: WrapperKind,
    wrapper_version: String,
    archive_path: PathBuf,
    events_tx: mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
}

#[derive(Debug, Clone)]
pub(crate) struct TeamSandboxJoinState {
    pub(crate) sandbox_id: String,
    pub(crate) slot_index: i16,
    pub(crate) public_car_id: u64,
    pub(crate) engine_car_id: u64,
    pub(crate) container_name: String,
    pub(crate) container_id: String,
    pub(crate) log_file_path: PathBuf,
}

pub(crate) type OfficialSandboxJoinRegistry = Arc<DashMap<String, TeamSandboxJoinState>>;

pub(crate) fn new_official_sandbox_join_registry() -> OfficialSandboxJoinRegistry {
    Arc::new(DashMap::new())
}

#[derive(Debug, Clone)]
struct TeamCacheEntry {
    team_id: String,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct TeamNamesCacheEntry {
    team_names: HashMap<String, String>,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct ServiceTokenCacheEntry {
    token: String,
    expires_at: Instant,
}

#[derive(Debug, Deserialize)]
struct KeycloakTokenResponse {
    access_token: String,
    expires_in: Option<u64>,
    token_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KeycloakErrorResponse {
    error: Option<String>,
    error_description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GitHubReleaseResponse {
    assets: Vec<GitHubReleaseAsset>,
}

#[derive(Debug, Deserialize)]
struct GitHubReleaseAsset {
    id: u64,
    name: String,
}

#[derive(Debug, Clone)]
struct PythonRunConfig {
    entrypoint: Vec<String>,
    source_dir: String,
}

#[derive(Debug, Clone)]
struct CsharpRunConfig {
    entrypoint: Vec<String>,
    source_dir: String,
    runtime_version: String,
    csproj_paths: Vec<String>,
}

#[derive(Debug, Clone)]
enum BuildContextPreparation {
    Python(PythonRunConfig),
    Csharp(CsharpRunConfig),
}

#[derive(Debug, Deserialize)]
struct WrapperManifestToml {
    run: Option<WrapperRunToml>,
    runtime: Option<WrapperRuntimeToml>,
}

#[derive(Debug, Deserialize)]
struct WrapperRunToml {
    entrypoint: Option<Vec<String>>,
    source_dir: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WrapperRuntimeToml {
    version: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegistryDeleteOutcome {
    Deleted,
    NotFound,
}

#[derive(Debug, Clone)]
struct ParsedImageRef {
    registry_host: String,
    repository: String,
    tag: String,
}

/// Teams resolver backed by HPS with in-memory cache and service-token auth.
#[derive(Clone)]
pub struct HpsTeamResolver {
    channel: Channel,
    http_client: reqwest::Client,
    keycloak_token_url: String,
    keycloak_client_id: String,
    keycloak_client_secret: String,
    team_cache: Arc<DashMap<String, TeamCacheEntry>>,
    team_names_cache: Arc<RwLock<Option<TeamNamesCacheEntry>>>,
    auth_token: Arc<RwLock<Option<ServiceTokenCacheEntry>>>,
}

impl HpsTeamResolver {
    /// Creates resolver for configured HPS endpoint.
    pub(crate) fn new(
        hps_endpoint: &str,
        keycloak_token_url: String,
        keycloak_client_id: String,
        keycloak_client_secret: String,
    ) -> anyhow::Result<Self> {
        let endpoint = Endpoint::from_shared(hps_endpoint.to_string())
            .map_err(|err| anyhow!("invalid HPS endpoint URI `{hps_endpoint}`: {err}"))?;
        let endpoint = if hps_endpoint.starts_with("https://") {
            endpoint
                .tls_config(ClientTlsConfig::new().with_enabled_roots())
                .map_err(|err| anyhow!("invalid HPS TLS config for `{hps_endpoint}`: {err}"))?
        } else {
            endpoint
        };
        let http_client = reqwest::Client::builder()
            .timeout(KEYCLOAK_TOKEN_TIMEOUT)
            .build()
            .context("failed to build Keycloak HTTP client")?;

        Ok(Self {
            channel: endpoint.connect_lazy(),
            http_client,
            keycloak_token_url,
            keycloak_client_id,
            keycloak_client_secret,
            team_cache: Arc::new(DashMap::new()),
            team_names_cache: Arc::new(RwLock::new(None)),
            auth_token: Arc::new(RwLock::new(None)),
        })
    }

    /// Resolves team id for given user.
    pub(crate) async fn resolve_team_id(&self, user_id: &str) -> Result<String, Status> {
        let user_id = user_id.trim();
        if user_id.is_empty() {
            return Err(Status::unauthenticated("missing user id"));
        }

        if let Some(entry) = self.team_cache.get(user_id)
            && entry.expires_at > Instant::now()
        {
            return Ok(entry.team_id.clone());
        }

        let resolved = self.fetch_team_id_with_retry(user_id).await?;
        self.team_cache.insert(
            user_id.to_string(),
            TeamCacheEntry {
                team_id: resolved.clone(),
                expires_at: Instant::now() + TEAM_CACHE_TTL,
            },
        );
        Ok(resolved)
    }

    async fn fetch_team_id_with_retry(&self, user_id: &str) -> Result<String, Status> {
        let token = self.current_or_fetch_token().await?;
        match self.fetch_team_id_with_token(user_id, &token).await {
            Ok(team_id) => Ok(team_id),
            Err(status)
                if matches!(
                    status.code(),
                    Code::Unauthenticated | Code::PermissionDenied
                ) =>
            {
                let refreshed = self.refresh_token().await?;
                self.fetch_team_id_with_token(user_id, &refreshed).await
            }
            Err(status) => Err(status),
        }
    }

    async fn fetch_team_id_with_token(&self, user_id: &str, token: &str) -> Result<String, Status> {
        let mut client = TeamsServiceClient::new(self.channel.clone());
        let mut request = Request::new(GetTeamByUserRequest {
            user_id: Some(PlatformUuid {
                value: user_id.to_string(),
            }),
            edition: TEAM_EDITION.to_string(),
        });
        attach_auth_cookie(&mut request, token)?;

        let response = tokio::time::timeout(HPS_RPC_TIMEOUT, client.get_team_by_user(request))
            .await
            .map_err(|_| Status::deadline_exceeded("HPS GetTeamByUser timed out"))?
            .map_err(|err| Status::new(err.code(), format!("HPS GetTeamByUser failed: {err}")))?;
        team_id_from_team(response.into_inner().team)
    }

    pub(crate) async fn resolve_team_names_map(&self) -> Result<HashMap<String, String>, Status> {
        if let Some(cached) = self.team_names_cache.read().await.clone()
            && cached.expires_at > Instant::now()
        {
            return Ok(cached.team_names);
        }

        let team_names = self.fetch_team_names_with_retry().await?;
        *self.team_names_cache.write().await = Some(TeamNamesCacheEntry {
            team_names: team_names.clone(),
            expires_at: Instant::now() + TEAM_CACHE_TTL,
        });
        Ok(team_names)
    }

    async fn fetch_team_names_with_retry(&self) -> Result<HashMap<String, String>, Status> {
        let token = self.current_or_fetch_token().await?;
        match self.fetch_team_names_with_token(&token).await {
            Ok(team_names) => Ok(team_names),
            Err(status)
                if matches!(
                    status.code(),
                    Code::Unauthenticated | Code::PermissionDenied
                ) =>
            {
                let refreshed = self.refresh_token().await?;
                self.fetch_team_names_with_token(&refreshed).await
            }
            Err(status) => Err(status),
        }
    }

    async fn fetch_team_names_with_token(
        &self,
        token: &str,
    ) -> Result<HashMap<String, String>, Status> {
        let mut client = TeamsServiceClient::new(self.channel.clone());
        let mut request = Request::new(GetEventTeamsRequest {
            edition: TEAM_EDITION.to_string(),
        });
        attach_auth_cookie(&mut request, token)?;

        let response = tokio::time::timeout(HPS_RPC_TIMEOUT, client.get_event_teams(request))
            .await
            .map_err(|_| Status::deadline_exceeded("HPS GetEventTeams timed out"))?
            .map_err(|err| Status::new(err.code(), format!("HPS GetEventTeams failed: {err}")))?;
        Ok(team_name_map_from_events(response.into_inner().teams))
    }

    async fn current_or_fetch_token(&self) -> Result<String, Status> {
        if let Some(cached) = self.auth_token.read().await.clone()
            && cached.expires_at > Instant::now()
        {
            return Ok(cached.token);
        }
        self.refresh_token().await
    }

    pub(crate) async fn current_or_fetch_service_token(&self) -> Result<String, Status> {
        self.current_or_fetch_token().await
    }

    async fn refresh_token(&self) -> Result<String, Status> {
        let cached = self.fetch_service_token().await.map_err(|err| {
            tracing::error!(error = %err, "failed to fetch service token from Keycloak");
            Status::unauthenticated("failed to fetch service token from Keycloak")
        })?;
        let token = cached.token.clone();
        *self.auth_token.write().await = Some(cached);
        Ok(token)
    }

    pub(crate) async fn refresh_service_token(&self) -> Result<String, Status> {
        self.refresh_token().await
    }

    async fn fetch_service_token(&self) -> anyhow::Result<ServiceTokenCacheEntry> {
        let response = self
            .http_client
            .post(&self.keycloak_token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", self.keycloak_client_id.as_str()),
                ("client_secret", self.keycloak_client_secret.as_str()),
            ])
            .send()
            .await
            .context("Keycloak token request failed")?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read Keycloak token response body")?;

        if !status.is_success() {
            let details = serde_json::from_str::<KeycloakErrorResponse>(&body)
                .ok()
                .map(|value| match (value.error, value.error_description) {
                    (Some(err), Some(desc)) if !desc.trim().is_empty() => {
                        format!("{err}: {}", desc.trim())
                    }
                    (Some(err), _) => err,
                    _ => body.trim().to_string(),
                })
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "<empty>".to_string());
            bail!(
                "Keycloak token request returned {}: {details}",
                status.as_u16()
            );
        }

        let parsed: KeycloakTokenResponse =
            serde_json::from_str(&body).context("failed to parse Keycloak token response JSON")?;
        let token = parsed.access_token.trim();
        if token.is_empty() {
            bail!("Keycloak token response did not include access_token");
        }

        if let Some(token_type) = parsed.token_type.as_deref()
            && !token_type.eq_ignore_ascii_case("bearer")
        {
            tracing::warn!(token_type = %token_type, "unexpected Keycloak token_type");
        }

        let ttl_source = parsed.expires_in.unwrap_or(SERVICE_TOKEN_DEFAULT_TTL_SEC);
        let ttl_with_margin = ttl_source.saturating_sub(SERVICE_TOKEN_TTL_SAFETY_SEC);
        let ttl_effective = ttl_with_margin.max(SERVICE_TOKEN_MIN_TTL_SEC);

        Ok(ServiceTokenCacheEntry {
            token: token.to_string(),
            expires_at: Instant::now() + Duration::from_secs(ttl_effective),
        })
    }
}

#[derive(Clone)]
pub struct GameTokenIssuer {
    channel: Channel,
    origin: Uri,
    team_resolver: Arc<HpsTeamResolver>,
}

impl GameTokenIssuer {
    pub(crate) fn new(endpoint: &str, team_resolver: Arc<HpsTeamResolver>) -> anyhow::Result<Self> {
        let endpoint_url = endpoint.trim();
        if endpoint_url.is_empty() {
            bail!("game-token issuer endpoint is empty");
        }
        let origin: Uri = endpoint_url
            .parse()
            .map_err(|err| anyhow!("invalid game-token issuer origin `{endpoint_url}`: {err}"))?;
        let endpoint = Endpoint::from_shared(endpoint_url.to_string())
            .map_err(|err| anyhow!("invalid game-token issuer endpoint `{endpoint_url}`: {err}"))?;
        let endpoint = if endpoint_url.starts_with("https://") {
            endpoint
                .tls_config(ClientTlsConfig::new().with_enabled_roots())
                .map_err(|err| {
                    anyhow!(
                        "invalid TLS config for game-token issuer endpoint `{endpoint_url}`: {err}"
                    )
                })?
        } else {
            endpoint
        };
        Ok(Self {
            channel: endpoint.connect_lazy(),
            origin,
            team_resolver,
        })
    }

    pub(crate) async fn issue_team_bot_token(&self, team_id: &str) -> Result<String, Status> {
        let token = self.team_resolver.current_or_fetch_service_token().await?;
        match self.issue_team_bot_token_with_cookie(team_id, &token).await {
            Ok(value) => Ok(value),
            Err(status)
                if matches!(
                    status.code(),
                    Code::Unauthenticated | Code::PermissionDenied
                ) =>
            {
                let refreshed = self.team_resolver.refresh_service_token().await?;
                self.issue_team_bot_token_with_cookie(team_id, &refreshed)
                    .await
            }
            Err(status) => Err(status),
        }
    }

    async fn issue_team_bot_token_with_cookie(
        &self,
        team_id: &str,
        service_token: &str,
    ) -> Result<String, Status> {
        let team_id = team_id.trim();
        if team_id.is_empty() {
            return Err(Status::failed_precondition("team_id is empty"));
        }

        let mut client =
            GameTokenIssuerServiceClient::with_origin(self.channel.clone(), self.origin.clone());
        let mut request = Request::new(IssueGameTokenRequest {
            token_type: GameTokenIssueType::TeamBot as i32,
        });
        attach_auth_cookie(&mut request, service_token)?;
        attach_team_id_header(&mut request, team_id)?;

        let response =
            tokio::time::timeout(GAME_TOKEN_RPC_TIMEOUT, client.issue_game_token(request))
                .await
                .map_err(|_| Status::deadline_exceeded("GameToken IssueGameToken timed out"))?
                .map_err(|err| {
                    Status::new(
                        err.code(),
                        format!("GameToken IssueGameToken failed: {err}"),
                    )
                })?;

        let jwt = response
            .into_inner()
            .token
            .map(|token| token.jwt)
            .unwrap_or_default();
        let jwt = jwt.trim();
        if jwt.is_empty() {
            return Err(Status::failed_precondition(
                "IssueGameToken returned empty token",
            ));
        }

        Ok(jwt.to_string())
    }
}

#[derive(Clone)]
pub struct WrapperAuthTokenIssuer {
    http_client: reqwest::Client,
    keycloak_token_url: String,
    keycloak_client_id: String,
    keycloak_client_secret: String,
}

impl WrapperAuthTokenIssuer {
    pub(crate) fn new(
        keycloak_token_url: String,
        keycloak_client_id: String,
        keycloak_client_secret: String,
    ) -> anyhow::Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(KEYCLOAK_TOKEN_TIMEOUT)
            .build()
            .context("failed to build wrapper-auth Keycloak HTTP client")?;
        Ok(Self {
            http_client,
            keycloak_token_url,
            keycloak_client_id,
            keycloak_client_secret,
        })
    }

    pub(crate) async fn issue_wrapper_auth_token(&self) -> Result<String, Status> {
        self.fetch_wrapper_auth_token().await.map_err(|err| {
            tracing::error!(
                error = %err,
                "wrapper-auth: failed to fetch auth token from Keycloak"
            );
            Status::unauthenticated("wrapper-auth: failed to fetch auth token from Keycloak")
        })
    }

    async fn fetch_wrapper_auth_token(&self) -> anyhow::Result<String> {
        let response = self
            .http_client
            .post(&self.keycloak_token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", self.keycloak_client_id.as_str()),
                ("client_secret", self.keycloak_client_secret.as_str()),
            ])
            .send()
            .await
            .context("wrapper-auth: Keycloak token request failed")?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("wrapper-auth: failed to read Keycloak token response body")?;

        if !status.is_success() {
            let details = serde_json::from_str::<KeycloakErrorResponse>(&body)
                .ok()
                .map(|value| match (value.error, value.error_description) {
                    (Some(err), Some(desc)) if !desc.trim().is_empty() => {
                        format!("{err}: {}", desc.trim())
                    }
                    (Some(err), _) => err,
                    _ => body.trim().to_string(),
                })
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "<empty>".to_string());
            bail!(
                "wrapper-auth: Keycloak token request returned {}: {details}",
                status.as_u16()
            );
        }

        let parsed: KeycloakTokenResponse = serde_json::from_str(&body)
            .context("wrapper-auth: failed to parse Keycloak token response JSON")?;
        let token = parsed.access_token.trim();
        if token.is_empty() {
            bail!("wrapper-auth: Keycloak response did not include access_token");
        }

        if let Some(token_type) = parsed.token_type.as_deref()
            && !token_type.eq_ignore_ascii_case("bearer")
        {
            tracing::warn!(token_type = %token_type, "wrapper-auth: unexpected Keycloak token_type");
        }

        Ok(token.to_string())
    }
}

#[derive(Clone)]
pub struct LogsAchievementGranter {
    channel: Channel,
    origin: Uri,
    team_resolver: Arc<HpsTeamResolver>,
}

impl LogsAchievementGranter {
    pub(crate) fn new(endpoint: &str, team_resolver: Arc<HpsTeamResolver>) -> anyhow::Result<Self> {
        let endpoint_url = endpoint.trim();
        if endpoint_url.is_empty() {
            bail!("achievement service endpoint is empty");
        }
        let origin: Uri = endpoint_url
            .parse()
            .map_err(|err| anyhow!("invalid achievement service origin `{endpoint_url}`: {err}"))?;
        let endpoint = Endpoint::from_shared(endpoint_url.to_string()).map_err(|err| {
            anyhow!("invalid achievement service endpoint `{endpoint_url}`: {err}")
        })?;
        let endpoint = if endpoint_url.starts_with("https://") {
            endpoint
                .tls_config(ClientTlsConfig::new().with_enabled_roots())
                .map_err(|err| {
                    anyhow!(
                        "invalid TLS config for achievement service endpoint `{endpoint_url}`: {err}"
                    )
                })?
        } else {
            endpoint
        };
        Ok(Self {
            channel: endpoint.connect_lazy(),
            origin,
            team_resolver,
        })
    }

    pub(crate) async fn grant_logs_achievement(&self, team_id: &str) -> Result<(), Status> {
        let token = self.team_resolver.current_or_fetch_service_token().await?;
        match self
            .grant_logs_achievement_with_retry(team_id, &token)
            .await
        {
            Ok(()) => Ok(()),
            Err(status)
                if matches!(
                    status.code(),
                    Code::Unauthenticated | Code::PermissionDenied
                ) =>
            {
                let refreshed = self.team_resolver.refresh_service_token().await?;
                self.grant_logs_achievement_with_retry(team_id, &refreshed)
                    .await
            }
            Err(status) => Err(status),
        }
    }

    async fn grant_logs_achievement_with_retry(
        &self,
        team_id: &str,
        service_token: &str,
    ) -> Result<(), Status> {
        let mut retry_attempt: u32 = 0;
        loop {
            match self
                .grant_logs_achievement_with_cookie(team_id, service_token)
                .await
            {
                Ok(()) => return Ok(()),
                Err(status)
                    if is_transient_achievement_error(status.code())
                        && retry_attempt < ACHIEVEMENT_TRANSIENT_RETRIES =>
                {
                    retry_attempt += 1;
                    let backoff = achievement_retry_backoff(retry_attempt);
                    tracing::debug!(
                        team_id,
                        retry_attempt,
                        max_attempts = ACHIEVEMENT_TRANSIENT_RETRIES + 1,
                        code = ?status.code(),
                        delay_ms = backoff.as_millis(),
                        "retrying logs achievement grant after transient error"
                    );
                    tokio::time::sleep(backoff).await;
                }
                Err(status) => return Err(status),
            }
        }
    }

    async fn grant_logs_achievement_with_cookie(
        &self,
        team_id: &str,
        service_token: &str,
    ) -> Result<(), Status> {
        let team_id = team_id.trim();
        if team_id.is_empty() {
            return Err(Status::failed_precondition("team_id is empty"));
        }

        let mut client =
            AchievementServiceClient::with_origin(self.channel.clone(), self.origin.clone());
        let mut request = Request::new(GrantLogsAchievementRequest {
            team_id: team_id.to_string(),
        });
        attach_auth_cookie(&mut request, service_token)?;

        let response = tokio::time::timeout(
            ACHIEVEMENT_RPC_TIMEOUT,
            client.grant_logs_achievement(request),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("Achievement GrantLogsAchievement timed out"))?
        .map_err(|err| {
            Status::new(
                err.code(),
                format!("Achievement GrantLogsAchievement failed: {err}"),
            )
        })?;

        let response = response.into_inner();
        if response.is_granted_successfully {
            tracing::info!(team_id, "logs achievement granted");
            return Ok(());
        }

        let reason = GrantLogsFailReason::try_from(response.reason).ok();
        if matches!(
            reason,
            Some(GrantLogsFailReason::GrantLogsFailAlreadyGranted)
        ) {
            tracing::debug!(team_id, "logs achievement already granted");
            return Ok(());
        }

        tracing::warn!(
            team_id,
            reason = ?reason,
            raw_reason = response.reason,
            "logs achievement grant request was not accepted"
        );
        Ok(())
    }
}

fn is_transient_achievement_error(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable | Code::DeadlineExceeded | Code::Internal | Code::Unknown
    )
}

fn achievement_retry_backoff(retry_attempt: u32) -> Duration {
    let exponent = retry_attempt.saturating_sub(1).min(4);
    let multiplier = 1u64 << exponent;
    Duration::from_millis(ACHIEVEMENT_RETRY_BACKOFF_BASE_MS.saturating_mul(multiplier))
}

/// gRPC SubmissionService implementation.
#[derive(Clone)]
pub struct SubmissionServiceImpl {
    repo: SubmissionRepo,
    token_validator: Arc<TokenValidator>,
    team_resolver: Arc<HpsTeamResolver>,
    queue_tx: mpsc::Sender<SubmissionBuildJob>,
    submissions_root: PathBuf,
    archive_max_bytes: usize,
}

impl SubmissionServiceImpl {
    /// Creates SubmissionService implementation.
    pub(crate) fn new(
        repo: SubmissionRepo,
        token_validator: Arc<TokenValidator>,
        team_resolver: Arc<HpsTeamResolver>,
        queue_tx: mpsc::Sender<SubmissionBuildJob>,
        archive_max_mb: u32,
    ) -> Self {
        Self {
            repo,
            token_validator,
            team_resolver,
            queue_tx,
            submissions_root: PathBuf::from(SUBMISSIONS_ROOT),
            archive_max_bytes: archive_max_mb as usize * 1024 * 1024,
        }
    }
}

/// gRPC SlotQueryService implementation.
#[derive(Clone)]
pub struct SlotQueryServiceImpl {
    repo: SubmissionRepo,
    token_validator: Arc<TokenValidator>,
    team_resolver: Arc<HpsTeamResolver>,
    slot_updates_tx: broadcast::Sender<String>,
    join_registry: OfficialSandboxJoinRegistry,
}

/// gRPC OfficialSandboxCommandService implementation.
#[derive(Clone)]
pub struct OfficialSandboxCommandServiceImpl {
    repo: SubmissionRepo,
    token_validator: Arc<TokenValidator>,
    team_resolver: Arc<HpsTeamResolver>,
    game_token_issuer: Arc<GameTokenIssuer>,
    logs_achievement_granter: Arc<LogsAchievementGranter>,
    wrapper_auth_token_issuer: Arc<WrapperAuthTokenIssuer>,
    wrapper_backend_endpoint: String,
    engine: EngineClient,
    runtime_store: Arc<RaceRuntimeStore>,
    slot_updates_tx: broadcast::Sender<String>,
    join_registry: OfficialSandboxJoinRegistry,
    submissions_root: PathBuf,
    log_capture_tasks: Arc<DashMap<String, JoinHandle<()>>>,
    join_command_lock: Arc<Mutex<()>>,
}

impl SlotQueryServiceImpl {
    pub(crate) fn new(
        repo: SubmissionRepo,
        token_validator: Arc<TokenValidator>,
        team_resolver: Arc<HpsTeamResolver>,
        slot_updates_tx: broadcast::Sender<String>,
        join_registry: OfficialSandboxJoinRegistry,
    ) -> Self {
        Self {
            repo,
            token_validator,
            team_resolver,
            slot_updates_tx,
            join_registry,
        }
    }
}

impl OfficialSandboxCommandServiceImpl {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        repo: SubmissionRepo,
        token_validator: Arc<TokenValidator>,
        team_resolver: Arc<HpsTeamResolver>,
        game_token_issuer: Arc<GameTokenIssuer>,
        logs_achievement_granter: Arc<LogsAchievementGranter>,
        wrapper_auth_token_issuer: Arc<WrapperAuthTokenIssuer>,
        wrapper_backend_endpoint: String,
        engine: EngineClient,
        runtime_store: Arc<RaceRuntimeStore>,
        slot_updates_tx: broadcast::Sender<String>,
        join_registry: OfficialSandboxJoinRegistry,
    ) -> Self {
        Self {
            repo,
            token_validator,
            team_resolver,
            game_token_issuer,
            logs_achievement_granter,
            wrapper_auth_token_issuer,
            wrapper_backend_endpoint,
            engine,
            runtime_store,
            slot_updates_tx,
            join_registry,
            submissions_root: PathBuf::from(SUBMISSIONS_ROOT),
            log_capture_tasks: Arc::new(DashMap::new()),
            join_command_lock: Arc::new(Mutex::new(())),
        }
    }

    fn container_name_for_team(team_id: &str) -> anyhow::Result<String> {
        let team_component = sanitize_tag_component(team_id)?;
        Ok(format!("ha3-official-bot-team-{team_component}"))
    }

    fn team_logs_dir(&self, team_id: &str) -> PathBuf {
        self.submissions_root
            .join(sanitize_storage_component(team_id))
            .join(TEAM_LOGS_SUBDIR)
    }

    fn build_bot_log_path(
        &self,
        team_id: &str,
        submission_id: &str,
        sandbox_id: &str,
        slot_index: i16,
        container_id: &str,
    ) -> PathBuf {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        let team = sanitize_storage_component(team_id);
        let submission = sanitize_storage_component(submission_id);
        let sandbox = sanitize_storage_component(sandbox_id);
        let container_short = sanitize_storage_component(
            container_id.trim().get(..12).unwrap_or(container_id.trim()),
        );
        let file_name = format!(
            "{ts_ms}_team-{team}_submission-{submission}_sandbox-{sandbox}_slot-{slot_index}_container-{container_short}.log"
        );
        self.team_logs_dir(team_id).join(file_name)
    }

    async fn stop_log_capture_for_team(&self, team_id: &str) {
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
                    "bot log capture task ended with join error"
                );
            }
        }
    }

    async fn start_bot_log_capture(
        &self,
        team_id: &str,
        submission_id: &str,
        sandbox_id: &str,
        slot_index: i16,
        container_id: &str,
    ) -> anyhow::Result<PathBuf> {
        let logs_dir = self.team_logs_dir(team_id);
        fs::create_dir_all(&logs_dir).await.with_context(|| {
            format!("failed to create bot logs directory {}", logs_dir.display())
        })?;

        let log_file_path =
            self.build_bot_log_path(team_id, submission_id, sandbox_id, slot_index, container_id);
        let mut log_file = fs::File::create(&log_file_path).await.with_context(|| {
            format!("failed to create bot log file {}", log_file_path.display())
        })?;
        let started_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        log_file
            .write_all(
                format!(
                    "# team_id={team_id}\n# submission_id={submission_id}\n# sandbox_id={sandbox_id}\n# slot_index={slot_index}\n# container_id={container_id}\n# started_at_unix_ms={started_at_ms}\n"
                )
                .as_bytes(),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to write bot log header {}",
                    log_file_path.display()
                )
            })?;
        log_file.flush().await.with_context(|| {
            format!("failed to flush bot log header {}", log_file_path.display())
        })?;

        let mut command = Command::new("docker");
        command
            .arg("logs")
            .arg("-f")
            .arg("--timestamps")
            .arg(container_id)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .context("failed to execute docker/logs follow process")?;
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                let _ = child.kill().await;
                bail!("docker/logs did not provide stdout stream");
            }
        };
        let stderr = match child.stderr.take() {
            Some(stderr) => stderr,
            None => {
                let _ = child.kill().await;
                bail!("docker/logs did not provide stderr stream");
            }
        };

        let writer = Arc::new(Mutex::new(log_file));
        let log_file_path_for_task = log_file_path.clone();
        let team_id_for_task = team_id.to_string();
        let container_id_for_task = container_id.to_string();
        let logs_achievement_granted = Arc::new(AtomicBool::new(false));
        let logs_achievement_granter = self.logs_achievement_granter.clone();
        let stdout_task = tokio::spawn(stream_bot_logs_to_file(
            stdout,
            writer.clone(),
            "stdout",
            team_id.to_string(),
            logs_achievement_granter.clone(),
            logs_achievement_granted.clone(),
        ));
        let stderr_task = tokio::spawn(stream_bot_logs_to_file(
            stderr,
            writer.clone(),
            "stderr",
            team_id.to_string(),
            logs_achievement_granter,
            logs_achievement_granted,
        ));

        let capture_task = tokio::spawn(async move {
            let wait_result = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            let mut guard = writer.lock().await;
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
                        "bot log capture finished"
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
                        "bot log capture process failed"
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
            sandbox_id = %sandbox_id,
            slot_index,
            container_id = %container_id,
            log_file = %log_file_path.display(),
            "started bot log capture"
        );

        Ok(log_file_path)
    }

    async fn rollback_join_runtime(
        &self,
        team_id: &str,
        sandbox_id: &str,
        target: EngineCommandTarget,
        public_car_id: u64,
        engine_car_id: u64,
        container_name: &str,
    ) {
        if let Err(err) = remove_bot_container(container_name).await {
            tracing::warn!(
                team_id = %team_id,
                sandbox_id = %sandbox_id,
                container_name = %container_name,
                error = %err,
                "failed to rollback bot container after join failure"
            );
        }
        if let Err(err) = self.engine.despawn_car_in(target, engine_car_id).await {
            if is_engine_resource_not_found(&err) {
                tracing::debug!(
                    team_id = %team_id,
                    sandbox_id = %sandbox_id,
                    engine_car_id,
                    "rollback join: engine car already absent"
                );
            } else {
                tracing::warn!(
                    team_id = %team_id,
                    sandbox_id = %sandbox_id,
                    engine_car_id,
                    error = %err,
                    "failed to rollback spawned join car after bot container failure"
                );
            }
        }
        self.runtime_store.remove_car(public_car_id);
    }

    async fn cleanup_join_state_locked(
        &self,
        team_id: &str,
        join_state: TeamSandboxJoinState,
        reason: &str,
    ) -> Result<(), Status> {
        if let Err(err) = remove_bot_container(&join_state.container_name).await {
            tracing::warn!(
                team_id = %team_id,
                sandbox_id = %join_state.sandbox_id,
                container_name = %join_state.container_name,
                error = %err,
                "failed to remove bot container during join cleanup"
            );
        }
        self.stop_log_capture_for_team(team_id).await;
        match compress_bot_log_file_if_exists(&join_state.log_file_path).await {
            Ok(Some(archive_path)) => {
                tracing::info!(
                    team_id = %team_id,
                    sandbox_id = %join_state.sandbox_id,
                    source_log = %join_state.log_file_path.display(),
                    archived_log = %archive_path.display(),
                    "compressed bot log file"
                );
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    team_id = %team_id,
                    sandbox_id = %join_state.sandbox_id,
                    log_file = %join_state.log_file_path.display(),
                    error = %err,
                    "failed to compress bot log file"
                );
            }
        }

        let target = EngineCommandTarget::Sandbox {
            sandbox_id: join_state.sandbox_id.clone(),
        };
        if let Err(err) = self
            .engine
            .despawn_car_in(target, join_state.engine_car_id)
            .await
        {
            if is_engine_resource_not_found(&err) {
                tracing::debug!(
                    team_id = %team_id,
                    sandbox_id = %join_state.sandbox_id,
                    engine_car_id = join_state.engine_car_id,
                    "join cleanup: engine car already absent"
                );
            } else {
                return Err(map_worker_err(err));
            }
        }

        self.runtime_store.remove_car(join_state.public_car_id);
        self.join_registry.remove(team_id);
        let _ = self.slot_updates_tx.send(team_id.to_string());

        tracing::info!(
            team_id = %team_id,
            sandbox_id = %join_state.sandbox_id,
            slot_index = join_state.slot_index,
            public_car_id = join_state.public_car_id,
            engine_car_id = join_state.engine_car_id,
            container_name = %join_state.container_name,
            container_id = %join_state.container_id,
            log_file = %join_state.log_file_path.display(),
            reason,
            "official sandbox join resources cleaned"
        );
        Ok(())
    }

    fn spawn_container_exit_monitor(&self, team_id: String, container_id: String) {
        let service = self.clone();
        tokio::spawn(async move {
            let exit_code = match wait_bot_container_exit_code(&container_id).await {
                Ok(exit_code) => exit_code,
                Err(err) => {
                    tracing::warn!(
                        team_id = %team_id,
                        container_id = %container_id,
                        error = %err,
                        "failed waiting for bot container exit"
                    );
                    return;
                }
            };
            tracing::info!(
                team_id = %team_id,
                container_id = %container_id,
                exit_code,
                "bot container exited"
            );

            let _join_guard = service.join_command_lock.lock().await;
            let Some(join_state) = service
                .join_registry
                .get(&team_id)
                .map(|entry| entry.value().clone())
            else {
                return;
            };
            if join_state.container_id != container_id {
                return;
            }

            if let Err(err) = service
                .cleanup_join_state_locked(&team_id, join_state, "container-exit")
                .await
            {
                tracing::warn!(
                    team_id = %team_id,
                    container_id = %container_id,
                    error = %err,
                    "failed to cleanup join after bot container exit"
                );
            }
        });
    }
}

#[tonic::async_trait]
impl SubmissionService for SubmissionServiceImpl {
    type SubmitBuildStreamStream = ReceiverStream<Result<SubmitBuildStreamResponse, Status>>;

    async fn submit_build_stream(
        &self,
        request: Request<SubmitBuildRequest>,
    ) -> Result<Response<Self::SubmitBuildStreamStream>, Status> {
        let user_id = self.token_validator.subject(request.metadata()).await?;
        let req = request.into_inner();
        let wrapper_kind = WrapperKind::try_from(req.wrapper_kind)
            .map_err(|_| Status::invalid_argument("invalid wrapper_kind"))?;
        if !matches!(wrapper_kind, WrapperKind::Python | WrapperKind::Csharp) {
            return Err(Status::invalid_argument(
                "only WRAPPER_KIND_PYTHON and WRAPPER_KIND_CSHARP are supported in MVP",
            ));
        }

        let wrapper_version = req.wrapper_version.trim();
        if wrapper_version.is_empty() {
            return Err(Status::invalid_argument(
                "wrapper_version must be non-empty",
            ));
        }
        let slot_index_i32 = req.slot.ok_or_else(|| {
            Status::failed_precondition("slot must be set and must be between 1 and 3")
        })?;
        if !(1..=3).contains(&slot_index_i32) {
            return Err(Status::failed_precondition(
                "slot must be set and must be between 1 and 3",
            ));
        }
        let slot_index = i16::try_from(slot_index_i32)
            .map_err(|_| Status::failed_precondition("slot must be between 1 and 3"))?;

        if req.user_archive_tar_gz.is_empty() {
            return Err(Status::invalid_argument(
                "user_archive_tar_gz must be non-empty",
            ));
        }
        if req.user_archive_tar_gz.len() > self.archive_max_bytes {
            return Err(Status::resource_exhausted(format!(
                "archive exceeds SUBMISSION_ARCHIVE_MAX_MB ({} MB)",
                self.archive_max_bytes / (1024 * 1024)
            )));
        }

        let (events_tx, events_rx) = mpsc::channel(BUILD_EVENT_CHANNEL_CAPACITY);
        let team_id = self.team_resolver.resolve_team_id(&user_id).await?;
        let submission_id = uuid::Uuid::new_v4().to_string();
        let archive_dir = self
            .submissions_root
            .join(sanitize_storage_component(&team_id))
            .join(TEAM_SUBMISSIONS_SUBDIR)
            .join(sanitize_storage_component(&submission_id));
        fs::create_dir_all(&archive_dir).await.map_err(|err| {
            Status::internal(format!("failed to create submission directory: {err}"))
        })?;
        let archive_path = archive_dir.join("user.tar.gz");
        fs::write(&archive_path, req.user_archive_tar_gz)
            .await
            .map_err(|err| {
                Status::internal(format!("failed to persist submission archive: {err}"))
            })?;

        self.repo
            .create_submission(&NewSubmissionRecord {
                submission_id: submission_id.clone(),
                team_id: team_id.clone(),
                user_id,
                description: req.description,
                wrapper_kind: wrapper_kind_to_db(wrapper_kind).to_string(),
                wrapper_version: wrapper_version.to_string(),
                archive_path: archive_path.display().to_string(),
            })
            .await
            .map_err(|err| Status::internal(format!("failed to persist submission: {err}")))?;
        self.repo
            .ensure_team_slots(&team_id)
            .await
            .map_err(|err| Status::internal(format!("failed to ensure team slots: {err}")))?;

        self.queue_tx
            .send(SubmissionBuildJob {
                submission_id: submission_id.clone(),
                team_id,
                slot_index,
                wrapper_kind,
                wrapper_version: wrapper_version.to_string(),
                archive_path,
                events_tx: events_tx.clone(),
            })
            .await
            .map_err(|_| Status::unavailable("submission worker is unavailable"))?;

        emit_build_started(&events_tx, &submission_id).await;

        Ok(Response::new(ReceiverStream::new(events_rx)))
    }

    async fn list_recent_submissions(
        &self,
        request: Request<ListRecentSubmissionsRequest>,
    ) -> Result<Response<ListRecentSubmissionsResponse>, Status> {
        let user_id = self.token_validator.subject(request.metadata()).await?;
        let team_id = self.team_resolver.resolve_team_id(&user_id).await?;

        let records = self
            .repo
            .list_recent_submissions(&team_id, LIST_RECENT_SUBMISSIONS_LIMIT)
            .await
            .map_err(|err| Status::internal(format!("failed to list recent submissions: {err}")))?;

        let mut submissions = Vec::with_capacity(records.len());
        for record in records {
            let archive_size_bytes = match fs::metadata(Path::new(&record.archive_path)).await {
                Ok(metadata) => metadata.len(),
                Err(err) => {
                    tracing::warn!(
                        team_id = %team_id,
                        submission_id = %record.submission_id,
                        archive_path = %record.archive_path,
                        error = %err,
                        "recent submission archive metadata unavailable"
                    );
                    0
                }
            };

            submissions.push(RecentSubmissionDto {
                submission_id: record.submission_id,
                submitted_at: Some(prost_types::Timestamp {
                    seconds: record.created_at_unix_seconds,
                    nanos: 0,
                }),
                archive_size_bytes,
                description: record.description,
            });
        }

        Ok(Response::new(ListRecentSubmissionsResponse { submissions }))
    }

    async fn get_submission_archive(
        &self,
        request: Request<GetSubmissionArchiveRequest>,
    ) -> Result<Response<GetSubmissionArchiveResponse>, Status> {
        let user_id = self.token_validator.subject(request.metadata()).await?;
        let team_id = self.team_resolver.resolve_team_id(&user_id).await?;
        let req = request.into_inner();
        let submission_id = req.submission_id.trim();
        if submission_id.is_empty() {
            return Err(Status::invalid_argument("submission_id must be non-empty"));
        }

        let archive_path = self
            .repo
            .get_submission_archive_path(&team_id, submission_id)
            .await
            .map_err(|err| {
                Status::internal(format!("failed to load submission archive path: {err}"))
            })?
            .ok_or_else(|| Status::not_found("submission not found for team"))?;

        let user_archive_tar_gz =
            fs::read(&archive_path)
                .await
                .map_err(|err| match err.kind() {
                    ErrorKind::NotFound => Status::not_found("submission archive not found"),
                    _ => Status::internal(format!("failed to read submission archive: {err}")),
                })?;

        Ok(Response::new(GetSubmissionArchiveResponse {
            user_archive_tar_gz: user_archive_tar_gz.into(),
        }))
    }

    async fn get_submission_logs(
        &self,
        request: Request<GetSubmissionLogsRequest>,
    ) -> Result<Response<GetSubmissionLogsResponse>, Status> {
        let user_id = self.token_validator.subject(request.metadata()).await?;
        let team_id = self.team_resolver.resolve_team_id(&user_id).await?;
        let req = request.into_inner();
        let submission_id = req.submission_id.trim();
        if submission_id.is_empty() {
            return Err(Status::invalid_argument("submission_id must be non-empty"));
        }

        let _archive_path = self
            .repo
            .get_submission_archive_path(&team_id, submission_id)
            .await
            .map_err(|err| {
                Status::internal(format!("failed to load submission archive path: {err}"))
            })?
            .ok_or_else(|| Status::not_found("submission not found for team"))?;

        let team_logs_dir = self
            .submissions_root
            .join(sanitize_storage_component(&team_id))
            .join(TEAM_LOGS_SUBDIR);
        let legacy_logs_dir = self.submissions_root.join(LEGACY_BOT_LOGS_SUBDIR);
        let submission_marker = format!("submission-{}", sanitize_storage_component(submission_id));

        let mut log_files = collect_submission_log_files(
            &team_logs_dir,
            SubmissionLogSource::Current,
            &submission_marker,
        )
        .await
        .map_err(|err| Status::internal(format!("failed to list submission log files: {err}")))?;
        log_files.extend(
            collect_submission_log_files(
                &legacy_logs_dir,
                SubmissionLogSource::Legacy,
                &submission_marker,
            )
            .await
            .map_err(|err| {
                Status::internal(format!("failed to list legacy submission log files: {err}"))
            })?,
        );

        log_files.sort_by(|left, right| {
            left.file_name
                .cmp(&right.file_name)
                .then_with(|| left.source.cmp(&right.source))
        });

        if log_files.is_empty() {
            return Err(Status::not_found("submission logs not found"));
        }

        let build_logs_tar_gz =
            tokio::task::spawn_blocking(move || package_submission_logs_tar_gz(&log_files))
                .await
                .map_err(|err| {
                    Status::internal(format!("failed to package submission logs: {err}"))
                })?
                .map_err(|err| {
                    Status::internal(format!("failed to package submission logs: {err}"))
                })?;

        Ok(Response::new(GetSubmissionLogsResponse {
            build_logs_tar_gz: build_logs_tar_gz.into(),
        }))
    }
}

#[tonic::async_trait]
impl SlotQueryService for SlotQueryServiceImpl {
    type StreamSlotsStream = ReceiverStream<Result<StreamSlotsResponse, Status>>;

    async fn get_slots(
        &self,
        request: Request<GetSlotsRequest>,
    ) -> Result<Response<GetSlotsResponse>, Status> {
        let user_id = self.token_validator.subject(request.metadata()).await?;
        let team_id = self.team_resolver.resolve_team_id(&user_id).await?;
        let filled_slots = self
            .repo
            .list_filled_succeeded_slots(&team_id)
            .await
            .map_err(|err| Status::internal(format!("failed to query team slots: {err}")))?;

        Ok(Response::new(GetSlotsResponse {
            slots: filled_slots
                .into_iter()
                .map(|slot| SlotSummaryDto {
                    slot: Some(i32::from(slot.slot_index)),
                    submission_id: slot.submission_id,
                    description: slot.description.unwrap_or_default(),
                })
                .collect(),
        }))
    }

    async fn stream_slots(
        &self,
        request: Request<StreamSlotsRequest>,
    ) -> Result<Response<Self::StreamSlotsStream>, Status> {
        let user_id = self.token_validator.subject(request.metadata()).await?;
        let team_id = self.team_resolver.resolve_team_id(&user_id).await?;
        let mut updates_rx = self.slot_updates_tx.subscribe();
        let repo = self.repo.clone();
        let joins = self.join_registry.clone();
        let (tx, rx) = mpsc::channel(SLOT_STREAM_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            if let Err(status) =
                emit_slots_snapshot(&repo, &team_id, loaded_slot_for_team(&joins, &team_id), &tx)
                    .await
            {
                let _ = tx.send(Err(status)).await;
                return;
            }

            loop {
                match updates_rx.recv().await {
                    Ok(updated_team_id) => {
                        if updated_team_id != team_id {
                            continue;
                        }
                        if let Err(status) = emit_slots_snapshot(
                            &repo,
                            &team_id,
                            loaded_slot_for_team(&joins, &team_id),
                            &tx,
                        )
                        .await
                        {
                            let _ = tx.send(Err(status)).await;
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            team_id = %team_id,
                            skipped_events = skipped,
                            "slot stream lagged; emitting latest snapshot"
                        );
                        if let Err(status) = emit_slots_snapshot(
                            &repo,
                            &team_id,
                            loaded_slot_for_team(&joins, &team_id),
                            &tx,
                        )
                        .await
                        {
                            let _ = tx.send(Err(status)).await;
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }

                if tx.is_closed() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl OfficialSandboxCommandService for OfficialSandboxCommandServiceImpl {
    async fn join_official_sandbox(
        &self,
        request: Request<JoinOfficialSandboxRequest>,
    ) -> Result<Response<JoinOfficialSandboxResponse>, Status> {
        let user_id = self.token_validator.subject(request.metadata()).await?;
        let team_id = self.team_resolver.resolve_team_id(&user_id).await?;
        let req = request.into_inner();
        let sandbox_id = req.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Ok(Response::new(join_failed("sandbox_id must be non-empty")));
        }
        if !(1..=3).contains(&req.slot) {
            return Ok(Response::new(join_failed("slot must be between 1 and 3")));
        }
        let slot_index = i16::try_from(req.slot)
            .map_err(|_| Status::failed_precondition("slot must be between 1 and 3"))?;

        let _join_guard = self.join_command_lock.lock().await;
        if self.join_registry.contains_key(&team_id) {
            return Ok(Response::new(join_failed(
                "team bot is already joined; leave first",
            )));
        }

        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;
        let sandbox_active = runtime
            .active_sandboxes
            .iter()
            .any(|sandbox| sandbox.sandbox_id == sandbox_id);
        if !sandbox_active {
            return Ok(Response::new(join_failed(
                "sandbox runtime is not active for requested sandbox_id",
            )));
        }

        let slot_submission = self
            .repo
            .get_succeeded_submission_for_slot(&team_id, slot_index)
            .await
            .map_err(|err| Status::internal(format!("failed to resolve slot submission: {err}")))?;
        let Some(slot_submission) = slot_submission else {
            return Ok(Response::new(join_failed(
                "requested slot does not contain succeeded submission",
            )));
        };
        let Some(image_ref) = slot_submission
            .image_ref
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(Response::new(join_failed(
                "requested slot submission is missing image_ref",
            )));
        };
        let team_bot_token = self
            .game_token_issuer
            .issue_team_bot_token(&team_id)
            .await?;
        let wrapper_auth_token = self
            .wrapper_auth_token_issuer
            .issue_wrapper_auth_token()
            .await?;
        let container_name = Self::container_name_for_team(&team_id).map_err(|err| {
            Status::failed_precondition(format!("invalid team_id for container name: {err}"))
        })?;

        let target = EngineCommandTarget::Sandbox {
            sandbox_id: sandbox_id.clone(),
        };
        let engine_car_id = self
            .engine
            .spawn_sandbox_car(sandbox_id.clone())
            .await
            .map_err(map_worker_err)?;
        if let Err(err) = self
            .engine
            .set_car_before_finish_line_in(target.clone(), engine_car_id)
            .await
        {
            if let Err(cleanup_err) = self
                .engine
                .despawn_car_in(target.clone(), engine_car_id)
                .await
            {
                tracing::warn!(
                    team_id = %team_id,
                    sandbox_id = %sandbox_id,
                    engine_car_id,
                    error = %cleanup_err,
                    "failed to rollback spawned join car after placement error"
                );
            }
            return Err(map_worker_err(err));
        }

        let public_car_id = self.runtime_store.allocate_public_car_id();
        let mut identity = RuntimeCarIdentity::default();
        identity.subject = Some(user_id);
        identity.team_id = Some(team_id.clone());
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
            .insert(public_car_id, target.clone());

        let container_id = match start_bot_container(
            image_ref,
            &container_name,
            &self.wrapper_backend_endpoint,
            &team_bot_token,
            &wrapper_auth_token,
            &team_id,
            &slot_submission.submission_id,
            &sandbox_id,
            slot_index,
        )
        .await
        {
            Ok(id) => id,
            Err(err) => {
                self.rollback_join_runtime(
                    &team_id,
                    &sandbox_id,
                    target,
                    public_car_id,
                    engine_car_id,
                    &container_name,
                )
                .await;
                return Ok(Response::new(join_failed(format!(
                    "failed to start bot container: {}",
                    clip_for_error(&format!("{err:#}"))
                ))));
            }
        };
        let log_file_path = match self
            .start_bot_log_capture(
                &team_id,
                &slot_submission.submission_id,
                &sandbox_id,
                slot_index,
                &container_id,
            )
            .await
        {
            Ok(path) => path,
            Err(err) => {
                self.stop_log_capture_for_team(&team_id).await;
                self.rollback_join_runtime(
                    &team_id,
                    &sandbox_id,
                    target,
                    public_car_id,
                    engine_car_id,
                    &container_name,
                )
                .await;
                return Ok(Response::new(join_failed(format!(
                    "failed to initialize bot log capture: {}",
                    clip_for_error(&format!("{err:#}"))
                ))));
            }
        };

        self.join_registry.insert(
            team_id.clone(),
            TeamSandboxJoinState {
                sandbox_id: sandbox_id.clone(),
                slot_index,
                public_car_id,
                engine_car_id,
                container_name: container_name.clone(),
                container_id: container_id.clone(),
                log_file_path: log_file_path.clone(),
            },
        );
        let _ = self.slot_updates_tx.send(team_id.clone());
        self.spawn_container_exit_monitor(team_id.clone(), container_id.clone());

        tracing::info!(
            team_id = %team_id,
            sandbox_id = %sandbox_id,
            slot_index,
            submission_id = %slot_submission.submission_id,
            public_car_id,
            engine_car_id,
            container_name = %container_name,
            container_id = %container_id,
            log_file = %log_file_path.display(),
            "official sandbox join succeeded"
        );
        Ok(Response::new(JoinOfficialSandboxResponse {
            status: OfficialSandboxCommandStatus::Ok as i32,
            message: "joined official sandbox".to_string(),
        }))
    }

    async fn leave_official_sandbox(
        &self,
        request: Request<LeaveOfficialSandboxRequest>,
    ) -> Result<Response<LeaveOfficialSandboxResponse>, Status> {
        let user_id = self.token_validator.subject(request.metadata()).await?;
        let team_id = self.team_resolver.resolve_team_id(&user_id).await?;
        let _ = request.into_inner();

        let _join_guard = self.join_command_lock.lock().await;
        let join_state = self
            .join_registry
            .get(&team_id)
            .map(|entry| entry.value().clone());
        let Some(join_state) = join_state else {
            return Ok(Response::new(LeaveOfficialSandboxResponse {
                status: OfficialSandboxCommandStatus::Ok as i32,
                message: "already left".to_string(),
            }));
        };

        self.cleanup_join_state_locked(&team_id, join_state, "leave-request")
            .await?;
        Ok(Response::new(LeaveOfficialSandboxResponse {
            status: OfficialSandboxCommandStatus::Ok as i32,
            message: "left official sandbox".to_string(),
        }))
    }
}

/// Spawns submission worker and returns queue sender, slot updates notifier, and task handle.
pub(crate) fn spawn_submission_worker(
    cfg: Arc<Config>,
    repo: SubmissionRepo,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> anyhow::Result<(
    mpsc::Sender<SubmissionBuildJob>,
    broadcast::Sender<String>,
    JoinHandle<()>,
)> {
    preflight_ssh_transport(&cfg)?;
    let (tx, mut rx) = mpsc::channel::<SubmissionBuildJob>(SUBMISSION_QUEUE_CAPACITY);
    let (slot_updates_tx, _) = broadcast::channel::<String>(SLOT_UPDATE_CHANNEL_CAPACITY);
    let slot_updates_tx_worker = slot_updates_tx.clone();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    tracing::info!("submission worker shutdown requested");
                    break;
                }
                maybe_job = rx.recv() => {
                    let Some(job) = maybe_job else {
                        break;
                    };
                    tracing::info!(
                        submission_id = %job.submission_id,
                        team_id = %job.team_id,
                        slot_index = job.slot_index,
                        "submission worker: picked queued job"
                    );
                    if let Err(err) = process_submission_job(
                        cfg.clone(),
                        repo.clone(),
                        &slot_updates_tx_worker,
                        &job
                    ).await {
                        let error_message = format!("{err:#}");
                        tracing::error!(
                            submission_id = %job.submission_id,
                            team_id = %job.team_id,
                            slot_index = job.slot_index,
                            error = %err,
                            error_detail = %error_message,
                            "submission job failed"
                        );
                        emit_build_log_line(
                            &job.events_tx,
                            format!("build failed: {error_message}"),
                        )
                        .await;
                        if let Err(mark_err) = repo.mark_failed(&job.submission_id, &error_message).await {
                            tracing::error!(submission_id = %job.submission_id, error = %mark_err, "failed to mark submission as failed");
                        }
                        emit_build_finished(&job.events_tx, false).await;
                    }
                }
            }
        }
        tracing::info!("submission worker stopped");
    });
    Ok((tx, slot_updates_tx, handle))
}

async fn process_submission_job(
    cfg: Arc<Config>,
    repo: SubmissionRepo,
    slot_updates_tx: &broadcast::Sender<String>,
    job: &SubmissionBuildJob,
) -> anyhow::Result<()> {
    repo.mark_building(&job.submission_id)
        .await
        .context("failed to mark submission as building")?;
    let timeout = Duration::from_secs(cfg.submission_build_timeout_sec);
    tracing::info!(
        submission_id = %job.submission_id,
        team_id = %job.team_id,
        slot_index = job.slot_index,
        timeout_sec = timeout.as_secs(),
        "submission worker: starting remote build pipeline"
    );
    let image_ref = tokio::time::timeout(
        timeout,
        run_remote_build_pipeline(&cfg, job, &job.events_tx),
    )
    .await
    .map_err(|_| {
        anyhow!(
            "submission build timed out after {} seconds",
            timeout.as_secs()
        )
    })??;
    repo.mark_succeeded_and_assign_slot(
        &job.submission_id,
        &job.team_id,
        job.slot_index,
        &image_ref,
    )
    .await
    .with_context(|| {
        format!(
            "failed to mark submission succeeded and assign slot team_id={} slot_index={}",
            job.team_id, job.slot_index
        )
    })?;
    let _ = slot_updates_tx.send(job.team_id.clone());
    if let Err(err) =
        cleanup_orphaned_team_submissions(&repo, &job.team_id, &job.submission_id).await
    {
        tracing::warn!(
            submission_id = %job.submission_id,
            team_id = %job.team_id,
            error = %err,
            "submission cleanup failed (best-effort)"
        );
    }
    tracing::info!(
        submission_id = %job.submission_id,
        team_id = %job.team_id,
        slot_index = job.slot_index,
        image_ref = %image_ref,
        "submission worker: job completed"
    );
    emit_build_finished(&job.events_tx, true).await;
    Ok(())
}

async fn run_remote_build_pipeline(
    cfg: &Config,
    job: &SubmissionBuildJob,
    events_tx: &mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
) -> anyhow::Result<String> {
    let image_ref = format_image_ref(&cfg.registry, &job.team_id, &job.submission_id)?;
    let team_component = sanitize_tag_component(&job.team_id)?;
    let submission_component = sanitize_tag_component(&job.submission_id)?;

    let temp_dir = tempfile::tempdir().context("failed to create temp directory")?;
    let context_dir = temp_dir.path().join("build-context");
    let archive_path = job.archive_path.clone();
    let prepare_context_dir = context_dir.clone();
    let wrapper_kind = job.wrapper_kind;
    let build_context = tokio::task::spawn_blocking(move || {
        prepare_build_context(&archive_path, &prepare_context_dir, wrapper_kind)
    })
    .await
    .context("build-context task join error")??;

    match &build_context {
        BuildContextPreparation::Python(run_config) => {
            emit_build_log_line(
                events_tx,
                format!(
                    "[context/python] resolved run config: source_dir={}, entrypoint={:?}",
                    run_config.source_dir, run_config.entrypoint
                ),
            )
            .await;
            prepare_python_wrapper_wheel(
                cfg,
                &job.wrapper_version,
                &context_dir,
                run_config,
                events_tx,
            )
            .await?;
        }
        BuildContextPreparation::Csharp(run_config) => {
            emit_build_log_line(
                events_tx,
                format!(
                    "[context/csharp] resolved run config: source_dir={}, runtime_version={}, entrypoint={:?}, projects={}",
                    run_config.source_dir,
                    run_config.runtime_version,
                    run_config.entrypoint,
                    run_config.csproj_paths.len()
                ),
            )
            .await;
            prepare_csharp_wrapper_package(
                cfg,
                &job.wrapper_version,
                &context_dir,
                run_config,
                events_tx,
            )
            .await?;
        }
    }

    let local_context_tar = temp_dir.path().join("context.tar.gz");
    let package_context_dir = context_dir.clone();
    let package_tar = local_context_tar.clone();
    tokio::task::spawn_blocking(move || {
        package_context_as_tar_gz(&package_context_dir, &package_tar)
    })
    .await
    .context("context packaging task join error")??;

    let remote_archive = format!("/tmp/ha3-submission-{submission_component}.tar.gz");
    let remote_context = format!("/tmp/ha3-submission-{submission_component}");
    let mut scp_cmd = Command::new("scp");
    add_common_ssh_options(&mut scp_cmd, cfg);
    scp_cmd
        .arg("-q")
        .arg(local_context_tar.as_os_str())
        .arg(format!("root@{}:{remote_archive}", cfg.builder_host));
    run_command_streaming(scp_cmd, "scp", events_tx).await?;

    let remote_prepare_cmd = format!(
        "set -euo pipefail; \
         rm -rf {remote_context}; \
         mkdir -p {remote_context}; \
         tar -xzf {remote_archive} -C {remote_context}",
        remote_context = shell_escape_single(&remote_context),
        remote_archive = shell_escape_single(&remote_archive),
    );
    run_ssh_script(cfg, &remote_prepare_cmd, "ssh/prepare", events_tx).await?;

    let remote_build_cmd = format!(
        "set -euo pipefail; \
         cd {remote_context}; \
         docker build -t {image_ref} .",
        remote_context = shell_escape_single(&remote_context),
        image_ref = shell_escape_single(&image_ref),
    );
    run_ssh_script(cfg, &remote_build_cmd, "ssh/docker-build", events_tx).await?;

    // Hard guard against false-positive success from remote shell state.
    let remote_verify_cmd = format!(
        "set -euo pipefail; docker image inspect {image_ref} >/dev/null",
        image_ref = shell_escape_single(&image_ref),
    );
    run_ssh_script(cfg, &remote_verify_cmd, "ssh/verify-image", events_tx).await?;

    let remote_push_cmd = format!(
        "set -euo pipefail; docker push {image_ref}",
        image_ref = shell_escape_single(&image_ref),
    );
    run_ssh_script(cfg, &remote_push_cmd, "ssh/docker-push", events_tx).await?;

    let remote_cleanup_cmd = format!(
        "rm -rf {remote_context} {remote_archive}",
        remote_context = shell_escape_single(&remote_context),
        remote_archive = shell_escape_single(&remote_archive),
    );
    if let Err(err) = run_ssh_script(cfg, &remote_cleanup_cmd, "ssh/cleanup", events_tx).await {
        tracing::warn!(
            submission_id = %job.submission_id,
            team_id = %job.team_id,
            error = %err,
            "remote cleanup failed after build pipeline"
        );
    }

    tracing::info!(
        submission_id = %job.submission_id,
        team_id = %team_component,
        image_ref = %image_ref,
        "submission build succeeded"
    );

    Ok(image_ref)
}

async fn cleanup_orphaned_team_submissions(
    repo: &SubmissionRepo,
    team_id: &str,
    keep_submission_id: &str,
) -> anyhow::Result<()> {
    let candidates = repo
        .list_orphaned_succeeded_submissions(team_id, keep_submission_id)
        .await
        .with_context(|| format!("failed to list orphaned submissions for team_id={team_id}"))?;
    tracing::info!(
        team_id = %team_id,
        keep_submission_id = %keep_submission_id,
        candidates = candidates.len(),
        "submission cleanup started"
    );
    if candidates.is_empty() {
        return Ok(());
    }

    let mut registry_deleted = 0_u32;
    let mut registry_not_found = 0_u32;
    let mut registry_failed = 0_u32;
    let mut local_cleaned = 0_u32;
    let mut local_failed = 0_u32;

    for candidate in candidates {
        let mut registry_ok = true;

        if let Some(image_ref) = candidate.image_ref.as_deref() {
            match delete_registry_image_by_ref(image_ref).await {
                Ok(RegistryDeleteOutcome::Deleted) => {
                    registry_deleted += 1;
                    tracing::info!(
                        submission_id = %candidate.submission_id,
                        team_id = %team_id,
                        image_ref = %image_ref,
                        "submission cleanup: registry image deleted"
                    );
                }
                Ok(RegistryDeleteOutcome::NotFound) => {
                    registry_not_found += 1;
                    tracing::info!(
                        submission_id = %candidate.submission_id,
                        team_id = %team_id,
                        image_ref = %image_ref,
                        "submission cleanup: registry image already missing"
                    );
                }
                Err(err) => {
                    registry_failed += 1;
                    registry_ok = false;
                    tracing::warn!(
                        submission_id = %candidate.submission_id,
                        team_id = %team_id,
                        image_ref = %image_ref,
                        error = %err,
                        "submission cleanup: failed to delete registry image"
                    );
                }
            }
        } else {
            tracing::info!(
                submission_id = %candidate.submission_id,
                team_id = %team_id,
                "submission cleanup: image_ref already cleared; skipping registry delete"
            );
        }

        if !registry_ok {
            continue;
        }

        if candidate.image_ref.is_some()
            && let Err(err) = repo.clear_image_ref(&candidate.submission_id).await
        {
            tracing::warn!(
                submission_id = %candidate.submission_id,
                team_id = %team_id,
                error = %err,
                "submission cleanup: failed to clear image_ref in DB"
            );
        }

        match delete_local_submission_artifacts(&candidate.archive_path).await {
            Ok(()) => {
                local_cleaned += 1;
                tracing::info!(
                    submission_id = %candidate.submission_id,
                    team_id = %team_id,
                    archive_path = %candidate.archive_path,
                    "submission cleanup: local archive artifacts removed"
                );
            }
            Err(err) => {
                local_failed += 1;
                tracing::warn!(
                    submission_id = %candidate.submission_id,
                    team_id = %team_id,
                    archive_path = %candidate.archive_path,
                    error = %err,
                    "submission cleanup: failed to remove local archive artifacts"
                );
            }
        }
    }

    tracing::info!(
        team_id = %team_id,
        keep_submission_id = %keep_submission_id,
        registry_deleted,
        registry_not_found,
        registry_failed,
        local_cleaned,
        local_failed,
        "submission cleanup finished"
    );
    Ok(())
}

async fn delete_registry_image_by_ref(image_ref: &str) -> anyhow::Result<RegistryDeleteOutcome> {
    let parsed = parse_image_ref(image_ref)?;
    let registry_base_url = registry_api_base_url(&parsed.registry_host)?;
    let client = reqwest::Client::builder()
        .timeout(REGISTRY_API_TIMEOUT)
        .build()
        .context("failed to build registry HTTP client")?;

    let manifest_url = format!(
        "{}/v2/{}/manifests/{}",
        registry_base_url, parsed.repository, parsed.tag
    );
    let head_response = client
        .head(&manifest_url)
        .header(USER_AGENT, "ha3-game-server")
        .header(ACCEPT, REGISTRY_MANIFEST_V2_ACCEPT)
        .send()
        .await
        .with_context(|| format!("registry HEAD request failed for {manifest_url}"))?;
    match head_response.status() {
        StatusCode::NOT_FOUND => return Ok(RegistryDeleteOutcome::NotFound),
        status if !status.is_success() => {
            bail!(
                "registry HEAD failed for {} (HTTP {})",
                clip_for_error(image_ref),
                status.as_u16()
            );
        }
        _ => {}
    }

    let digest = head_response
        .headers()
        .get(REGISTRY_DIGEST_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("registry HEAD response missing Docker-Content-Digest header"))?;
    let delete_url = format!(
        "{}/v2/{}/manifests/{}",
        registry_base_url, parsed.repository, digest
    );
    let delete_response = client
        .delete(&delete_url)
        .header(USER_AGENT, "ha3-game-server")
        .send()
        .await
        .with_context(|| format!("registry DELETE request failed for {delete_url}"))?;
    match delete_response.status() {
        StatusCode::NOT_FOUND => Ok(RegistryDeleteOutcome::NotFound),
        status if status.is_success() => Ok(RegistryDeleteOutcome::Deleted),
        status => bail!(
            "registry DELETE failed for {} (HTTP {})",
            clip_for_error(image_ref),
            status.as_u16()
        ),
    }
}

fn parse_image_ref(image_ref: &str) -> anyhow::Result<ParsedImageRef> {
    let trimmed = image_ref.trim();
    if trimmed.is_empty() {
        bail!("image_ref is empty");
    }

    let normalized = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))
        .unwrap_or(trimmed);
    let (registry_host, repository_with_tag) = normalized
        .split_once('/')
        .ok_or_else(|| anyhow!("image_ref must contain registry host and repository"))?;
    if registry_host.trim().is_empty() {
        bail!("image_ref registry host is empty");
    }

    let (repository, tag) = repository_with_tag
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("image_ref must contain tag"))?;
    if repository.trim().is_empty() {
        bail!("image_ref repository is empty");
    }
    if tag.trim().is_empty() {
        bail!("image_ref tag is empty");
    }

    Ok(ParsedImageRef {
        registry_host: registry_host.trim().to_string(),
        repository: repository.trim().to_string(),
        tag: tag.trim().to_string(),
    })
}

fn registry_api_base_url(registry_host: &str) -> anyhow::Result<String> {
    let trimmed = registry_host.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        bail!("registry host is empty");
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Ok(trimmed.to_string());
    }
    Ok(format!("http://{trimmed}"))
}

async fn delete_local_submission_artifacts(archive_path: &str) -> anyhow::Result<()> {
    let archive_path = PathBuf::from(archive_path);
    match fs::remove_file(&archive_path).await {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to remove archive file {}", archive_path.display())
            });
        }
    }

    if let Some(parent) = archive_path.parent() {
        match fs::remove_dir(parent).await {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) if err.kind() == ErrorKind::DirectoryNotEmpty => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to remove submission archive directory {}",
                        parent.display()
                    )
                });
            }
        }
    }

    Ok(())
}

async fn run_ssh_script(
    cfg: &Config,
    script: &str,
    label: &str,
    events_tx: &mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
) -> anyhow::Result<()> {
    let remote_command = format!("bash -lc {}", shell_escape_single(script));
    let mut ssh_cmd = Command::new("ssh");
    add_common_ssh_options(&mut ssh_cmd, cfg);
    ssh_cmd
        .arg(format!("root@{}", cfg.builder_host))
        .arg(remote_command);
    run_command_streaming(ssh_cmd, label, events_tx).await
}

async fn prepare_python_wrapper_wheel(
    cfg: &Config,
    wrapper_version: &str,
    context_dir: &Path,
    run_config: &PythonRunConfig,
    events_tx: &mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
) -> anyhow::Result<()> {
    emit_build_log_line(events_tx, "[wrapper-fetch/python] resolving release asset").await;
    let (release_tag, asset_version) = normalize_wrapper_version(wrapper_version)?;
    let asset_name = format!("hackarena3-{asset_version}-py3-none-any.whl");
    let release =
        fetch_github_release_by_tag(cfg, &cfg.wrapper_python_gh_repo, &release_tag).await?;
    let asset = release
        .assets
        .into_iter()
        .find(|entry| entry.name.eq_ignore_ascii_case(&asset_name))
        .ok_or_else(|| {
            anyhow!("wrapper-fetch: release `{release_tag}` does not contain asset `{asset_name}`")
        })?;

    emit_build_log_line(
        events_tx,
        format!(
            "[wrapper-fetch/python] downloading `{}` from {}/{}",
            asset.name, cfg.wrapper_gh_owner, cfg.wrapper_python_gh_repo
        ),
    )
    .await;

    let wheel_bytes =
        download_github_release_asset(cfg, &cfg.wrapper_python_gh_repo, &asset).await?;

    let wheels_dir = context_dir.join("wheels");
    fs::create_dir_all(&wheels_dir)
        .await
        .with_context(|| format!("failed to create wheels directory {}", wheels_dir.display()))?;
    if asset.name.contains('/') || asset.name.contains('\\') {
        bail!("wrapper-fetch: asset name contains invalid path separators");
    }
    let wheel_path = wheels_dir.join(&asset.name);
    fs::write(&wheel_path, wheel_bytes)
        .await
        .with_context(|| format!("failed to write wrapper wheel {}", wheel_path.display()))?;

    let dockerfile_path = context_dir.join("Dockerfile");
    let dockerfile = build_python_dockerfile(&asset.name, run_config)?;
    fs::write(&dockerfile_path, dockerfile)
        .await
        .with_context(|| {
            format!(
                "failed to write Dockerfile at {}",
                dockerfile_path.display()
            )
        })?;

    emit_build_log_line(events_tx, "[wrapper-fetch/python] wrapper wheel prepared").await;
    Ok(())
}

async fn prepare_csharp_wrapper_package(
    cfg: &Config,
    wrapper_version: &str,
    context_dir: &Path,
    run_config: &CsharpRunConfig,
    events_tx: &mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
) -> anyhow::Result<()> {
    emit_build_log_line(events_tx, "[wrapper-fetch/csharp] resolving release asset").await;
    let (release_tag, asset_version) = normalize_wrapper_version(wrapper_version)?;
    let release =
        fetch_github_release_by_tag(cfg, &cfg.wrapper_csharp_gh_repo, &release_tag).await?;
    let asset = select_csharp_release_asset(release.assets, &asset_version, &release_tag)?;

    emit_build_log_line(
        events_tx,
        format!(
            "[wrapper-fetch/csharp] downloading `{}` from {}/{}",
            asset.name, cfg.wrapper_gh_owner, cfg.wrapper_csharp_gh_repo
        ),
    )
    .await;

    let package_bytes =
        download_github_release_asset(cfg, &cfg.wrapper_csharp_gh_repo, &asset).await?;

    let wrappers_dir = context_dir.join("wrappers");
    fs::create_dir_all(&wrappers_dir).await.with_context(|| {
        format!(
            "failed to create wrappers directory {}",
            wrappers_dir.display()
        )
    })?;
    if asset.name.contains('/') || asset.name.contains('\\') {
        bail!("wrapper-fetch: asset name contains invalid path separators");
    }
    let package_path = wrappers_dir.join(&asset.name);
    fs::write(&package_path, package_bytes)
        .await
        .with_context(|| format!("failed to write wrapper package {}", package_path.display()))?;

    let dockerfile_path = context_dir.join("Dockerfile");
    let dockerfile = build_csharp_dockerfile(&asset.name, run_config)?;
    fs::write(&dockerfile_path, dockerfile)
        .await
        .with_context(|| {
            format!(
                "failed to write Dockerfile at {}",
                dockerfile_path.display()
            )
        })?;

    emit_build_log_line(events_tx, "[wrapper-fetch/csharp] wrapper package prepared").await;
    Ok(())
}

async fn fetch_github_release_by_tag(
    cfg: &Config,
    repo: &str,
    release_tag: &str,
) -> anyhow::Result<GitHubReleaseResponse> {
    let release_url = format!(
        "{}/repos/{}/{}/releases/tags/{}",
        GITHUB_API_BASE_URL, cfg.wrapper_gh_owner, repo, release_tag
    );
    let client = reqwest::Client::builder()
        .timeout(GITHUB_API_TIMEOUT)
        .build()
        .context("failed to build GitHub HTTP client")?;

    let mut release_request = client
        .get(&release_url)
        .header(USER_AGENT, "ha3-game-server")
        .header(ACCEPT, "application/vnd.github+json");
    if let Some(token) = cfg.gh_token.as_deref()
        && !token.trim().is_empty()
    {
        release_request = release_request.header(AUTHORIZATION, format!("Bearer {token}"));
    }

    let release_response = release_request
        .send()
        .await
        .context("wrapper-fetch: failed to query GitHub release by tag")?;
    if !release_response.status().is_success() {
        bail!(
            "{}",
            github_release_error_message(release_response.status().as_u16(), release_tag)
        );
    }

    release_response
        .json()
        .await
        .context("wrapper-fetch: failed to decode GitHub release payload")
}

async fn download_github_release_asset(
    cfg: &Config,
    repo: &str,
    asset: &GitHubReleaseAsset,
) -> anyhow::Result<Vec<u8>> {
    let asset_download_url = format!(
        "{}/repos/{}/{}/releases/assets/{}",
        GITHUB_API_BASE_URL, cfg.wrapper_gh_owner, repo, asset.id
    );
    let client = reqwest::Client::builder()
        .timeout(GITHUB_API_TIMEOUT)
        .build()
        .context("failed to build GitHub HTTP client")?;
    let mut asset_request = client
        .get(&asset_download_url)
        .header(USER_AGENT, "ha3-game-server")
        .header(ACCEPT, "application/octet-stream");
    if let Some(token) = cfg.gh_token.as_deref()
        && !token.trim().is_empty()
    {
        asset_request = asset_request.header(AUTHORIZATION, format!("Bearer {token}"));
    }

    let asset_response = asset_request
        .send()
        .await
        .context("wrapper-fetch: failed to download release asset")?;
    if !asset_response.status().is_success() {
        bail!(
            "{}",
            github_asset_error_message(asset_response.status().as_u16(), &asset.name)
        );
    }

    let bytes = asset_response
        .bytes()
        .await
        .context("wrapper-fetch: failed to read release asset bytes")?;
    Ok(bytes.to_vec())
}

fn select_csharp_release_asset(
    assets: Vec<GitHubReleaseAsset>,
    asset_version: &str,
    release_tag: &str,
) -> anyhow::Result<GitHubReleaseAsset> {
    let asset_version = asset_version.to_ascii_lowercase();
    let expected_legacy_prefix = format!("hackarena3-{asset_version}");
    let expected_dotted_name = format!("hackarena3.wrapper.csharp.{asset_version}.nupkg");
    let mut matches: Vec<GitHubReleaseAsset> = assets
        .into_iter()
        .filter(|entry| {
            let name = entry.name.to_ascii_lowercase();
            name.ends_with(".nupkg")
                && (name.starts_with(&expected_legacy_prefix) || name == expected_dotted_name)
        })
        .collect();

    match matches.len() {
        0 => bail!(
            "wrapper-fetch: release `{}` does not contain a C# package matching `hackarena3-{}*.nupkg` or `HackArena3.Wrapper.CSharp.{}.nupkg`",
            release_tag,
            asset_version,
            asset_version
        ),
        1 => Ok(matches.swap_remove(0)),
        _ => {
            let candidates = matches
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "wrapper-fetch: release `{}` has multiple matching C# packages: {}",
                release_tag,
                candidates
            )
        }
    }
}

fn normalize_wrapper_version(raw: &str) -> anyhow::Result<(String, String)> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("wrapper_version must be non-empty");
    }

    let asset_version = trimmed.trim_start_matches('v');
    if asset_version.is_empty() {
        bail!("wrapper_version must contain version after optional `v` prefix");
    }

    Ok((format!("v{asset_version}"), asset_version.to_string()))
}

fn build_python_dockerfile(
    wheel_file_name: &str,
    run_config: &PythonRunConfig,
) -> anyhow::Result<String> {
    if run_config.entrypoint.is_empty() {
        bail!("manifest run.entrypoint must be non-empty");
    }
    let cmd_json = serde_json::to_string(&run_config.entrypoint)
        .context("failed to serialize manifest run.entrypoint to Docker CMD")?;

    Ok(format!(
        r#"FROM python:3.10-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
COPY wheels/{wheel_file_name} /app/wheels/{wheel_file_name}
COPY user/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir /app/wheels/{wheel_file_name}
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY user/{source_dir} /app/src
ENTRYPOINT {cmd_json}
"#,
        source_dir = run_config.source_dir
    ))
}

fn build_csharp_dockerfile(
    package_file_name: &str,
    run_config: &CsharpRunConfig,
) -> anyhow::Result<String> {
    if run_config.entrypoint.is_empty() {
        bail!("manifest run.entrypoint must be non-empty");
    }
    let cmd_json = serde_json::to_string(&run_config.entrypoint)
        .context("failed to serialize manifest run.entrypoint to Docker ENTRYPOINT")?;
    let restore_chain = run_config
        .csproj_paths
        .iter()
        .map(|project| {
            let absolute_project_path = format!("/app/user/{}/{}", run_config.source_dir, project);
            format!(
                "dotnet restore {} --source /app/wrappers --source https://api.nuget.org/v3/index.json",
                shell_escape_single(&absolute_project_path)
            )
        })
        .collect::<Vec<_>>()
        .join(" && \\\n    ");
    let build_chain = run_config
        .csproj_paths
        .iter()
        .map(|project| {
            let absolute_project_path = format!("/app/user/{}/{}", run_config.source_dir, project);
            format!(
                "dotnet build {} -c Release --no-restore",
                shell_escape_single(&absolute_project_path)
            )
        })
        .collect::<Vec<_>>()
        .join(" && \\\n    ");

    Ok(format!(
        r#"FROM mcr.microsoft.com/dotnet/sdk:{runtime_version}
WORKDIR /app
COPY wrappers/{package_file_name} /app/wrappers/{package_file_name}
COPY user /app/user
RUN {restore_chain}
RUN {build_chain}
ENTRYPOINT {cmd_json}
"#,
        runtime_version = run_config.runtime_version
    ))
}

fn github_release_error_message(status: u16, release_tag: &str) -> String {
    match status {
        401 | 403 => "wrapper-fetch: GitHub token missing or insufficient permissions".to_string(),
        404 => format!("wrapper-fetch: release tag `{release_tag}` not found"),
        _ => format!("wrapper-fetch: GitHub release request failed with HTTP {status}"),
    }
}

fn github_asset_error_message(status: u16, asset_name: &str) -> String {
    match status {
        401 | 403 => "wrapper-fetch: GitHub token missing or insufficient permissions".to_string(),
        404 => format!("wrapper-fetch: asset `{asset_name}` not found"),
        _ => format!("wrapper-fetch: GitHub asset download failed with HTTP {status}"),
    }
}

fn prepare_build_context(
    archive_path: &Path,
    context_dir: &Path,
    wrapper_kind: WrapperKind,
) -> anyhow::Result<BuildContextPreparation> {
    std::fs::create_dir_all(context_dir).context("failed to create build context directory")?;
    let user_dir = context_dir.join("user");
    std::fs::create_dir_all(&user_dir).context("failed to create user directory")?;
    let extracted_entries = extract_archive_secure(archive_path, &user_dir)?;

    match wrapper_kind {
        WrapperKind::Python => {
            let requirements = user_dir.join("requirements.txt");
            if !requirements.is_file() {
                let sample = format_path_sample(&extracted_entries, 20);
                let nested_requirements = user_dir.join("user").join("requirements.txt").is_file();
                let extra_hint = if nested_requirements {
                    " Found nested `user/requirements.txt`; archive likely has extra top-level `user/` folder."
                } else {
                    ""
                };
                bail!(
                    "archive validation failed: expected `requirements.txt` at archive root (mapped to build-context/user/requirements.txt). Extracted entries sample: {sample}.{extra_hint}"
                );
            }
            let run_config = resolve_python_run_config(&user_dir)?;
            Ok(BuildContextPreparation::Python(run_config))
        }
        WrapperKind::Csharp => {
            let run_config = resolve_csharp_run_config(&user_dir, &extracted_entries)?;
            Ok(BuildContextPreparation::Csharp(run_config))
        }
        _ => bail!("unsupported wrapper_kind in build worker: {wrapper_kind:?}"),
    }
}

fn resolve_python_run_config(user_dir: &Path) -> anyhow::Result<PythonRunConfig> {
    let mut source_dir = "src".to_string();
    let mut manifest_entrypoint: Option<Vec<String>> = None;
    if let Some(manifest) = read_wrapper_manifest(user_dir)? {
        if let Some(run) = manifest.run {
            if let Some(source_dir_raw) = run.source_dir {
                source_dir = normalize_manifest_source_dir(&source_dir_raw)?;
            }
            if let Some(entrypoint) = run.entrypoint {
                manifest_entrypoint = Some(normalize_manifest_entrypoint(entrypoint)?);
            }
        }
    }

    let source_path = user_dir.join(&source_dir);
    if !source_path.is_dir() {
        bail!(
            "run.source_dir `{}` does not exist in archive (expected `{}`)",
            source_dir,
            source_path.display()
        );
    }

    if let Some(entrypoint) = manifest_entrypoint {
        return Ok(PythonRunConfig {
            entrypoint,
            source_dir,
        });
    }

    let bot_module_entry = source_path.join("bot").join("__main__.py");
    if bot_module_entry.is_file() {
        return Ok(PythonRunConfig {
            entrypoint: vec!["python".to_string(), "-m".to_string(), "bot".to_string()],
            source_dir,
        });
    }
    let main_py_entry = source_path.join("main.py");
    if main_py_entry.is_file() {
        return Ok(PythonRunConfig {
            entrypoint: vec![
                "python".to_string(),
                "-u".to_string(),
                "/app/src/main.py".to_string(),
            ],
            source_dir,
        });
    }

    bail!(
        "cannot resolve bot entrypoint: set optional manifest [run].entrypoint or include {}/bot/__main__.py or {}/main.py",
        source_dir,
        source_dir
    )
}

fn resolve_csharp_run_config(
    user_dir: &Path,
    extracted_entries: &[String],
) -> anyhow::Result<CsharpRunConfig> {
    let mut source_dir = "src".to_string();
    let mut source_dir_from_manifest = false;
    let mut manifest_entrypoint: Option<Vec<String>> = None;
    let mut runtime_version: Option<String> = None;
    if let Some(manifest) = read_wrapper_manifest(user_dir)? {
        if let Some(run) = manifest.run {
            if let Some(source_dir_raw) = run.source_dir {
                source_dir = normalize_manifest_source_dir(&source_dir_raw)?;
                source_dir_from_manifest = true;
            }
            if let Some(entrypoint) = run.entrypoint {
                manifest_entrypoint = Some(normalize_manifest_entrypoint(entrypoint)?);
            }
        }
        if let Some(runtime_version_raw) = manifest.runtime.and_then(|runtime| runtime.version) {
            runtime_version = Some(normalize_dotnet_runtime_version(&runtime_version_raw)?);
        }
    }
    let runtime_version =
        runtime_version.unwrap_or_else(|| DEFAULT_CSHARP_DOTNET_RUNTIME_VERSION.to_string());

    let source_path = user_dir.join(&source_dir);
    if !source_path.is_dir() {
        bail!(
            "run.source_dir `{}` does not exist in archive (expected `{}`)",
            source_dir,
            source_path.display()
        );
    }

    let mut csproj_paths = discover_csproj_relative_paths(&source_path)?;
    if csproj_paths.is_empty() && !source_dir_from_manifest {
        let root_csproj_paths = discover_csproj_relative_paths(user_dir)?;
        if !root_csproj_paths.is_empty() {
            source_dir = ".".to_string();
            csproj_paths = root_csproj_paths;
        }
    }
    if csproj_paths.is_empty() {
        let sample = format_path_sample(extracted_entries, 20);
        if source_dir_from_manifest {
            bail!(
                "archive validation failed: no .csproj files found under run.source_dir `{}`. Extracted entries sample: {}",
                source_dir,
                sample
            );
        }
        bail!(
            "archive validation failed: no .csproj files found under default run.source_dir `src` or archive root. Extracted entries sample: {}",
            sample
        );
    }

    let entrypoint = if let Some(entrypoint) = manifest_entrypoint {
        entrypoint
    } else {
        let selected_csproj = csproj_paths.first().ok_or_else(|| {
            anyhow!("internal error: missing .csproj despite non-empty project list")
        })?;
        vec![
            "dotnet".to_string(),
            "run".to_string(),
            "--project".to_string(),
            format!("/app/user/{source_dir}/{selected_csproj}"),
        ]
    };

    Ok(CsharpRunConfig {
        entrypoint,
        source_dir,
        runtime_version,
        csproj_paths,
    })
}

fn read_wrapper_manifest(user_dir: &Path) -> anyhow::Result<Option<WrapperManifestToml>> {
    let manifest_path = user_dir.join("manifest.toml");
    if !manifest_path.is_file() {
        return Ok(None);
    }

    let manifest_raw = std::fs::read_to_string(&manifest_path).with_context(|| {
        format!(
            "failed to read wrapper manifest {}",
            manifest_path.display()
        )
    })?;
    let manifest: WrapperManifestToml = toml::from_str(&manifest_raw).with_context(|| {
        format!(
            "failed to parse wrapper manifest {}",
            manifest_path.display()
        )
    })?;
    Ok(Some(manifest))
}

fn normalize_manifest_entrypoint(entrypoint: Vec<String>) -> anyhow::Result<Vec<String>> {
    let trimmed_entrypoint: Vec<String> = entrypoint
        .into_iter()
        .map(|arg| arg.trim().to_string())
        .filter(|arg| !arg.is_empty())
        .collect();
    if trimmed_entrypoint.is_empty() {
        bail!("manifest run.entrypoint must not be empty");
    }
    Ok(trimmed_entrypoint)
}

fn normalize_manifest_source_dir(raw: &str) -> anyhow::Result<String> {
    let normalized_slashes = raw.trim().replace('\\', "/");
    let trimmed = normalized_slashes
        .trim_start_matches("./")
        .trim_start_matches('/');
    if trimmed.is_empty() {
        bail!("manifest run.source_dir must not be empty");
    }

    let without_user_prefix = if let Some(stripped) = trimmed.strip_prefix("user/") {
        if stripped.trim().is_empty() {
            bail!("manifest run.source_dir `user/` is invalid");
        }
        stripped
    } else {
        trimmed
    };

    let path = Path::new(without_user_prefix);
    let mut parts: Vec<String> = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(segment) => {
                let segment = segment.to_string_lossy();
                if segment.is_empty() {
                    bail!("manifest run.source_dir contains empty path segment");
                }
                if !segment
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
                {
                    bail!("manifest run.source_dir contains unsupported characters");
                }
                parts.push(segment.to_string());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                bail!("manifest run.source_dir must not contain `..`");
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("manifest run.source_dir must be a relative path");
            }
        }
    }
    if parts.is_empty() {
        bail!("manifest run.source_dir must not be empty");
    }
    Ok(parts.join("/"))
}

fn normalize_dotnet_runtime_version(raw: &str) -> anyhow::Result<String> {
    let version = raw.trim();
    if version.is_empty() {
        bail!("manifest runtime.version must not be empty when provided");
    }
    if !version
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '-')
    {
        bail!("manifest runtime.version contains unsupported characters");
    }
    Ok(version.to_string())
}

fn discover_csproj_relative_paths(source_dir: &Path) -> anyhow::Result<Vec<String>> {
    let mut paths = Vec::new();
    discover_csproj_relative_paths_inner(source_dir, source_dir, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn discover_csproj_relative_paths_inner(
    root_dir: &Path,
    current_dir: &Path,
    output: &mut Vec<String>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(current_dir)
        .with_context(|| format!("failed to read directory {}", current_dir.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read entry in {}", current_dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to inspect entry {}", path.display()))?;
        if file_type.is_dir() {
            discover_csproj_relative_paths_inner(root_dir, &path, output)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let is_csproj = path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("csproj"));
        if !is_csproj {
            continue;
        }
        let relative = path.strip_prefix(root_dir).with_context(|| {
            format!(
                "failed to compute project path relative to {}",
                root_dir.display()
            )
        })?;
        output.push(relative.to_string_lossy().replace('\\', "/"));
    }
    Ok(())
}

fn extract_archive_secure(archive_path: &Path, user_dir: &Path) -> anyhow::Result<Vec<String>> {
    let archive_file = std::fs::File::open(archive_path)
        .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
    let decoder = GzDecoder::new(archive_file);
    let mut archive = Archive::new(decoder);
    let mut extracted = Vec::new();

    for entry_result in archive
        .entries()
        .context("failed to read archive entries")?
    {
        let mut entry = entry_result.context("failed to read archive entry")?;
        let entry_type = entry.header().entry_type();
        let relative_path = entry
            .path()
            .context("failed to resolve archive entry path")?;
        let relative_path_str = relative_path.display().to_string();
        if !(entry_type.is_dir() || entry_type.is_file()) {
            bail!(
                "archive contains unsupported entry type for path `{}`",
                relative_path_str
            );
        }

        validate_relative_archive_path(&relative_path)?;
        let destination = user_dir.join(relative_path.as_ref());
        if !destination.starts_with(user_dir) {
            bail!(
                "archive entry `{}` resolved outside user directory",
                relative_path_str
            );
        }

        if entry_type.is_dir() {
            std::fs::create_dir_all(&destination)
                .with_context(|| format!("failed to create directory {}", destination.display()))?;
            extracted.push(format!("{}/", relative_path_str));
            continue;
        }

        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }
        entry
            .unpack(&destination)
            .with_context(|| format!("failed to unpack file {}", destination.display()))?;
        extracted.push(relative_path_str);
    }

    Ok(extracted)
}

fn validate_relative_archive_path(path: &Path) -> anyhow::Result<()> {
    if path.as_os_str().is_empty() {
        bail!("archive contains empty path entry");
    }

    for component in path.components() {
        match component {
            Component::Normal(value) if !value.is_empty() => {}
            Component::CurDir => {}
            Component::RootDir | Component::ParentDir | Component::Prefix(_) => {
                bail!("archive contains non-relative path");
            }
            Component::Normal(_) => bail!("archive contains empty path segment"),
        }
    }

    Ok(())
}

fn format_path_sample(paths: &[String], limit: usize) -> String {
    if paths.is_empty() {
        return "<empty archive>".to_string();
    }

    let mut sample = paths
        .iter()
        .take(limit)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    if paths.len() > limit {
        sample.push_str(", ...");
    }
    sample
}

fn package_context_as_tar_gz(context_dir: &Path, out_tar_gz: &Path) -> anyhow::Result<()> {
    let out = std::fs::File::create(out_tar_gz)
        .with_context(|| format!("failed to create {}", out_tar_gz.display()))?;
    let encoder = GzEncoder::new(out, Compression::default());
    let mut builder = Builder::new(encoder);
    builder
        .append_dir_all(".", context_dir)
        .with_context(|| format!("failed to tar build context {}", context_dir.display()))?;
    let encoder = builder
        .into_inner()
        .context("failed to finalize tar archive")?;
    encoder
        .finish()
        .context("failed to finalize gzip archive")?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SubmissionLogSource {
    Current,
    Legacy,
}

impl SubmissionLogSource {
    fn archive_prefix(self) -> &'static str {
        match self {
            SubmissionLogSource::Current => "logs",
            SubmissionLogSource::Legacy => "legacy-logs",
        }
    }
}

#[derive(Debug, Clone)]
struct SubmissionLogFile {
    source: SubmissionLogSource,
    file_name: String,
    path: PathBuf,
}

async fn collect_submission_log_files(
    directory: &Path,
    source: SubmissionLogSource,
    submission_marker: &str,
) -> anyhow::Result<Vec<SubmissionLogFile>> {
    let mut read_dir = match fs::read_dir(directory).await {
        Ok(read_dir) => read_dir,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "failed to open submission logs directory {}",
                    directory.display()
                )
            });
        }
    };

    let mut files = Vec::new();
    loop {
        let Some(entry) = read_dir
            .next_entry()
            .await
            .with_context(|| format!("failed to read directory entry {}", directory.display()))?
        else {
            break;
        };

        let file_type = entry
            .file_type()
            .await
            .with_context(|| format!("failed to read file type {}", entry.path().display()))?;
        if !file_type.is_file() {
            continue;
        }

        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !is_submission_log_artifact(file_name) || !file_name.contains(submission_marker) {
            continue;
        }

        files.push(SubmissionLogFile {
            source,
            file_name: file_name.to_string(),
            path,
        });
    }

    Ok(files)
}

fn is_submission_log_artifact(file_name: &str) -> bool {
    let lower = file_name.to_ascii_lowercase();
    lower.ends_with(".log") || lower.ends_with(".log.tar.gz")
}

fn package_submission_logs_tar_gz(log_files: &[SubmissionLogFile]) -> anyhow::Result<Vec<u8>> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut builder = Builder::new(encoder);
    for log_file in log_files {
        let archive_path = format!(
            "{}/{}",
            log_file.source.archive_prefix(),
            log_file.file_name
        );
        builder
            .append_path_with_name(&log_file.path, Path::new(&archive_path))
            .with_context(|| format!("failed to append log file {}", log_file.path.display()))?;
    }
    let encoder = builder
        .into_inner()
        .context("failed to finalize submission logs tar archive")?;
    let bytes = encoder
        .finish()
        .context("failed to finalize submission logs gzip archive")?;
    Ok(bytes)
}

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

fn format_image_ref(registry: &str, team_id: &str, submission_id: &str) -> anyhow::Result<String> {
    let registry = registry.trim().trim_end_matches('/');
    if registry.is_empty() {
        bail!("REGISTRY must be non-empty");
    }
    let team = sanitize_tag_component(team_id)?;
    let submission = sanitize_tag_component(submission_id)?;
    Ok(format!(
        "{registry}/ha3/team-{team}:submission-{submission}"
    ))
}

fn sanitize_tag_component(value: &str) -> anyhow::Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("tag component cannot be empty");
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        bail!("tag component contains unsupported characters");
    }
    Ok(value.to_string())
}

fn shell_escape_single(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn wrapper_kind_to_db(kind: WrapperKind) -> &'static str {
    match kind {
        WrapperKind::Python => "python",
        WrapperKind::Csharp => "csharp",
        WrapperKind::Cpp => "cpp",
        WrapperKind::JsTs => "js_ts",
        WrapperKind::Unspecified => "unspecified",
    }
}

async fn start_bot_container(
    image_ref: &str,
    container_name: &str,
    wrapper_backend_endpoint: &str,
    team_token: &str,
    wrapper_auth_token: &str,
    team_id: &str,
    submission_id: &str,
    sandbox_id: &str,
    slot_index: i16,
) -> anyhow::Result<String> {
    remove_bot_container(container_name).await?;
    pull_bot_image(image_ref).await?;

    let mut command = Command::new("docker");
    command
        .arg("run")
        .arg("-d")
        .arg("--name")
        .arg(container_name)
        .arg("--label")
        .arg(format!("ha3.team_id={team_id}"))
        .arg("--label")
        .arg(format!("ha3.submission_id={submission_id}"))
        .arg("--label")
        .arg(format!("ha3.sandbox_id={sandbox_id}"))
        .arg("--label")
        .arg(format!("ha3.slot_index={slot_index}"))
        .arg("--env")
        .arg("HA3_WRAPPER_BACKEND_ENDPOINT")
        .arg("--env")
        .arg("HA3_WRAPPER_TEAM_TOKEN")
        .arg("--env")
        .arg("HA3_WRAPPER_AUTH_TOKEN")
        .arg(image_ref);
    #[cfg(feature = "official")]
    {
        command.arg("--official");
    }
    command.env("HA3_WRAPPER_BACKEND_ENDPOINT", wrapper_backend_endpoint);
    command.env("HA3_WRAPPER_TEAM_TOKEN", team_token);
    command.env("HA3_WRAPPER_AUTH_TOKEN", wrapper_auth_token);
    let (stdout, _stderr) =
        run_command_capture(command, "docker/run", Some(BOT_DOCKER_TIMEOUT)).await?;

    let container_id = stdout
        .lines()
        .rev()
        .find_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .unwrap_or_default();
    if container_id.is_empty() {
        bail!("docker/run returned empty container id");
    }
    Ok(container_id.to_string())
}

async fn pull_bot_image(image_ref: &str) -> anyhow::Result<()> {
    let mut command = Command::new("docker");
    command.arg("pull").arg(image_ref);
    let _ = run_command_capture(command, "docker/pull", Some(BOT_DOCKER_TIMEOUT)).await?;
    Ok(())
}

async fn wait_bot_container_exit_code(container_id: &str) -> anyhow::Result<i32> {
    let mut command = Command::new("docker");
    command.arg("wait").arg(container_id);
    let (stdout, _stderr) = run_command_capture(command, "docker/wait", None).await?;
    let code_raw = stdout
        .lines()
        .rev()
        .find_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .ok_or_else(|| anyhow!("docker/wait did not return exit code"))?;
    code_raw
        .parse::<i32>()
        .with_context(|| format!("invalid exit code from docker/wait: `{code_raw}`"))
}

async fn remove_bot_container(container_name: &str) -> anyhow::Result<()> {
    let mut command = Command::new("docker");
    command.arg("rm").arg("-f").arg(container_name);
    match run_command_capture(command, "docker/rm", Some(BOT_DOCKER_TIMEOUT)).await {
        Ok(_) => Ok(()),
        Err(err) => {
            let lower = err.to_string().to_ascii_lowercase();
            if lower.contains("no such container") {
                Ok(())
            } else {
                Err(err)
            }
        }
    }
}

async fn compress_bot_log_file_if_exists(log_file_path: &Path) -> anyhow::Result<Option<PathBuf>> {
    if !log_file_path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.to_ascii_lowercase().ends_with(".log"))
    {
        return Ok(None);
    }

    match fs::metadata(log_file_path).await {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Ok(None);
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to stat bot log file {}", log_file_path.display())
            });
        }
    }

    let archive_path = compressed_bot_log_archive_path(log_file_path)?;
    let source_path = log_file_path.to_path_buf();
    let archive_path_for_task = archive_path.clone();

    tokio::task::spawn_blocking(move || {
        compress_bot_log_file_blocking(&source_path, &archive_path_for_task)
    })
    .await
    .context("bot log compression task panicked")??;

    Ok(Some(archive_path))
}

fn compressed_bot_log_archive_path(log_file_path: &Path) -> anyhow::Result<PathBuf> {
    let file_name = log_file_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("bot log file path must end with valid UTF-8 file name"))?;
    Ok(log_file_path.with_file_name(format!("{file_name}.tar.gz")))
}

fn compress_bot_log_file_blocking(source_path: &Path, archive_path: &Path) -> anyhow::Result<()> {
    let source_file_name = source_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("bot log file path must end with valid UTF-8 file name"))?;

    let result = (|| {
        let output_file = std::fs::File::create(archive_path).with_context(|| {
            format!(
                "failed to create compressed bot log {}",
                archive_path.display()
            )
        })?;
        let encoder = GzEncoder::new(output_file, Compression::default());
        let mut builder = Builder::new(encoder);
        builder
            .append_path_with_name(source_path, Path::new(source_file_name))
            .with_context(|| format!("failed to append bot log {}", source_path.display()))?;
        let encoder = builder
            .into_inner()
            .context("failed to finalize bot log tar archive")?;
        encoder
            .finish()
            .context("failed to finalize bot log gzip archive")?;
        std::fs::remove_file(source_path).with_context(|| {
            format!("failed to remove source bot log {}", source_path.display())
        })?;
        Ok(())
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(archive_path);
    }
    result
}

async fn stream_bot_logs_to_file<R>(
    stream: R,
    writer: Arc<Mutex<fs::File>>,
    channel: &'static str,
    team_id: String,
    logs_achievement_granter: Arc<LogsAchievementGranter>,
    logs_achievement_granted: Arc<AtomicBool>,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if !logs_achievement_granted.load(Ordering::Relaxed)
                    && contains_dupa_with_letter_boundaries_only(&line)
                    && !logs_achievement_granted.swap(true, Ordering::Relaxed)
                {
                    let team_id_for_grant = team_id.clone();
                    let granter = logs_achievement_granter.clone();
                    tokio::spawn(async move {
                        if let Err(status) =
                            granter.grant_logs_achievement(&team_id_for_grant).await
                        {
                            tracing::warn!(
                                team_id = %team_id_for_grant,
                                code = ?status.code(),
                                error = %status,
                                "failed to grant logs achievement"
                            );
                        }
                    });
                }

                let redacted = redact_log_line(&line);
                let mut guard = writer.lock().await;
                if let Err(err) = guard
                    .write_all(format!("[{channel}] {redacted}\n").as_bytes())
                    .await
                {
                    tracing::warn!(channel, error = %err, "failed writing bot log line");
                    break;
                }
                if let Err(err) = guard.flush().await {
                    tracing::warn!(channel, error = %err, "failed flushing bot log file");
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                tracing::warn!(channel, error = %err, "failed reading docker logs stream");
                let mut guard = writer.lock().await;
                let _ = guard
                    .write_all(
                        format!(
                            "[{channel}] <stream read error: {}>\n",
                            clip_for_error(&err.to_string())
                        )
                        .as_bytes(),
                    )
                    .await;
                let _ = guard.flush().await;
                break;
            }
        }
    }
}

fn contains_dupa_with_letter_boundaries_only(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut search_start = 0usize;

    while search_start < lower.len() {
        let Some(found_rel) = lower[search_start..].find("dupa") else {
            return false;
        };
        let start = search_start + found_rel;
        let end = start + 4;

        let left_is_letter = start > 0 && bytes[start - 1].is_ascii_alphabetic();
        let right_is_letter = end < bytes.len() && bytes[end].is_ascii_alphabetic();
        if !left_is_letter && !right_is_letter {
            return true;
        }

        search_start = start + 1;
    }

    false
}

async fn run_command_capture(
    mut command: Command,
    label: &str,
    timeout: Option<Duration>,
) -> anyhow::Result<(String, String)> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, command.output())
            .await
            .map_err(|_| anyhow!("{label} timed out after {}s", timeout.as_secs()))?
            .with_context(|| format!("failed to execute {label}"))?,
        None => command
            .output()
            .await
            .with_context(|| format!("failed to execute {label}"))?,
    };
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

    if output.status.success() {
        return Ok((stdout, stderr));
    }

    if let Some(hint) = docker_error_hint(&stderr) {
        bail!(
            "{label} failed: {hint} (status: {:?}, stderr: {})",
            output.status.code(),
            clip_for_error(&stderr)
        );
    }

    bail!(
        "{label} failed (status: {:?}, stdout: {}, stderr: {})",
        output.status.code(),
        clip_for_error(&stdout),
        clip_for_error(&stderr)
    );
}

fn docker_error_hint(stderr: &str) -> Option<&'static str> {
    let lower = stderr.to_ascii_lowercase();
    if lower.contains("no such container") {
        return Some("Docker container does not exist");
    }
    if lower.contains("cannot connect to the docker daemon") {
        return Some("Docker daemon is unavailable on backend host");
    }
    if lower.contains("pull access denied")
        || lower.contains("insufficient_scope")
        || lower.contains("requested access to the resource is denied")
    {
        return Some("Docker registry auth failed on backend host");
    }
    if lower.contains("manifest unknown")
        || lower.contains("not found")
        || lower.contains("no such image")
    {
        return Some("Docker image for selected slot was not found in registry");
    }
    None
}

fn add_common_ssh_options(command: &mut Command, cfg: &Config) {
    command
        .arg("-i")
        .arg(cfg.builder_ssh_key_path.as_os_str())
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("StrictHostKeyChecking=accept-new")
        .arg("-o")
        .arg("NumberOfPasswordPrompts=0")
        .arg("-o")
        .arg("PreferredAuthentications=publickey")
        .arg("-o")
        .arg("LogLevel=ERROR")
        .arg("-o")
        .arg("IdentitiesOnly=yes")
        .arg("-o")
        .arg(format!(
            "UserKnownHostsFile={}",
            cfg.builder_ssh_known_hosts_file.display()
        ));
}

async fn run_command_streaming(
    mut command: Command,
    label: &str,
    events_tx: &mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
) -> anyhow::Result<()> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn {label}"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to capture {label} stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("failed to capture {label} stderr"))?;

    let stdout_task = tokio::spawn(pump_command_output(
        stdout,
        format!("{label}/stdout"),
        events_tx.clone(),
    ));
    let stderr_task = tokio::spawn(pump_command_output(
        stderr,
        format!("{label}/stderr"),
        events_tx.clone(),
    ));

    let status = child
        .wait()
        .await
        .with_context(|| format!("{label} process wait failed"))?;
    let stdout_text = stdout_task
        .await
        .with_context(|| format!("{label} stdout task join failed"))??;
    let stderr_text = stderr_task
        .await
        .with_context(|| format!("{label} stderr task join failed"))??;

    if status.success() {
        return Ok(());
    }

    if let Some(hint) = ssh_error_hint(&stderr_text) {
        tracing::warn!(label, hint, "remote command failed");
        emit_build_log_line(events_tx, format!("[{label}] {hint}")).await;
        bail!(
            "{label} failed: {hint} (status: {:?}, stderr: {})",
            status.code(),
            clip_for_error(&stderr_text)
        );
    }

    bail!(
        "{label} failed (status: {:?}, stdout: {}, stderr: {})",
        status.code(),
        clip_for_error(&stdout_text),
        clip_for_error(&stderr_text),
    );
}

async fn pump_command_output<R>(
    reader: R,
    prefix: String,
    events_tx: mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
) -> anyhow::Result<String>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut reader = BufReader::new(reader);
    let mut buffer = Vec::with_capacity(1024);
    let mut collected = String::new();

    loop {
        buffer.clear();
        let bytes = reader.read_until(b'\n', &mut buffer).await?;
        if bytes == 0 {
            break;
        }

        let line = String::from_utf8_lossy(&buffer)
            .trim_end_matches(['\r', '\n'])
            .to_string();
        if line.is_empty() {
            continue;
        }

        if !collected.is_empty() {
            collected.push('\n');
        }
        collected.push_str(&line);

        emit_build_log_line(&events_tx, format!("[{prefix}] {line}")).await;
    }

    Ok(collected)
}

async fn emit_build_started(
    events_tx: &mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
    submission_id: &str,
) {
    let _ = events_tx
        .send(Ok(SubmitBuildStreamResponse {
            event: Some(submit_build_stream_response::Event::Started(BuildStarted {
                submission_id: submission_id.to_string(),
            })),
        }))
        .await;
}

async fn emit_build_log_line(
    events_tx: &mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
    line: impl Into<String>,
) {
    let _ = events_tx
        .send(Ok(SubmitBuildStreamResponse {
            event: Some(submit_build_stream_response::Event::Log(BuildLog {
                line: line.into(),
            })),
        }))
        .await;
}

async fn emit_build_finished(
    events_tx: &mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
    success: bool,
) {
    let _ = events_tx
        .send(Ok(SubmitBuildStreamResponse {
            event: Some(submit_build_stream_response::Event::Finished(
                BuildFinished { success },
            )),
        }))
        .await;
}

fn clip_for_error(value: &str) -> String {
    const LIMIT: usize = 800;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "<empty>".to_string();
    }

    let mut clipped = trimmed.chars().take(LIMIT).collect::<String>();
    if trimmed.chars().count() > LIMIT {
        clipped.push_str("...");
    }
    clipped
}

fn attach_auth_cookie<T>(request: &mut Request<T>, token: &str) -> Result<(), Status> {
    let cookie = format!("auth_token={token}");
    let value = MetadataValue::try_from(cookie.as_str())
        .map_err(|_| Status::unauthenticated("invalid auth token cookie"))?;
    request.metadata_mut().insert("cookie", value);
    Ok(())
}

fn attach_team_id_header<T>(request: &mut Request<T>, team_id: &str) -> Result<(), Status> {
    let team_id = team_id.trim();
    if team_id.is_empty() {
        return Err(Status::failed_precondition("x-team-id cannot be empty"));
    }
    let value = MetadataValue::try_from(team_id)
        .map_err(|_| Status::failed_precondition("invalid x-team-id metadata"))?;
    request.metadata_mut().insert("x-team-id", value);
    Ok(())
}

fn team_id_from_team(team: Option<Team>) -> Result<String, Status> {
    let team = team.ok_or_else(|| Status::not_found("team not found for user"))?;
    let team_id = team
        .id
        .as_ref()
        .map(|value| value.value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Status::failed_precondition("GetTeamByUser returned empty team id"))?;
    Ok(team_id.to_string())
}

fn team_name_map_from_events(teams: Vec<TeamEvent>) -> HashMap<String, String> {
    let mut team_names = HashMap::new();
    for team in teams {
        let Some(team_id) = team
            .id
            .as_ref()
            .map(|value| value.value.trim())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let team_name = team.name.trim();
        if team_name.is_empty() {
            continue;
        }
        team_names.insert(team_id.to_string(), team_name.to_string());
    }
    team_names
}

fn ssh_error_hint(stderr: &str) -> Option<&'static str> {
    let lower = stderr.to_ascii_lowercase();

    if lower.contains("permission denied (publickey)") {
        return Some(
            "SSH auth failed: upload matching public key to builder root/.ssh/authorized_keys",
        );
    }
    if lower.contains("identity file")
        && (lower.contains("not accessible") || lower.contains("no such file"))
    {
        return Some("SSH key file is missing or unreadable (BUILDER_SSH_KEY_PATH)");
    }
    if lower.contains("host key verification failed")
        || lower.contains("remote host identification has changed")
    {
        return Some("SSH host key verification failed (known_hosts mismatch)");
    }
    if lower.contains("no basic auth credentials")
        || lower.contains("push access denied")
        || lower.contains("authorization failed")
    {
        return Some("Registry auth failed on builder (docker login required on builder host)");
    }
    if lower.contains("could not resolve hostname") {
        return Some("SSH host resolution failed for BUILDER_HOST");
    }

    None
}

fn is_engine_resource_not_found(err: &EngineWorkerError) -> bool {
    matches!(err, EngineWorkerError::Engine(BoinkError::NotFound))
}

fn preflight_ssh_transport(cfg: &Config) -> anyhow::Result<()> {
    if !cfg.builder_ssh_key_path.is_file() {
        bail!(
            "BUILDER_SSH_KEY_PATH does not exist or is not a file: {}",
            cfg.builder_ssh_key_path.display()
        );
    }

    if let Some(parent) = cfg.builder_ssh_known_hosts_file.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create known_hosts parent directory: {}",
                parent.display()
            )
        })?;
    }

    if !cfg.builder_ssh_known_hosts_file.exists() {
        std::fs::File::create(&cfg.builder_ssh_known_hosts_file).with_context(|| {
            format!(
                "failed to create known_hosts file: {}",
                cfg.builder_ssh_known_hosts_file.display()
            )
        })?;
    }

    Ok(())
}

async fn emit_slots_snapshot(
    repo: &SubmissionRepo,
    team_id: &str,
    loaded_slot: Option<i16>,
    tx: &mpsc::Sender<Result<StreamSlotsResponse, Status>>,
) -> Result<(), Status> {
    let filled_slots = repo
        .list_filled_succeeded_slots(team_id)
        .await
        .map_err(|err| Status::internal(format!("failed to query team slots: {err}")))?;
    let response = StreamSlotsResponse {
        slots: filled_slots
            .into_iter()
            .map(|slot| SlotDto {
                slot: Some(i32::from(slot.slot_index)),
                submission_id: slot.submission_id,
                description: slot.description.unwrap_or_default(),
                selected: false,
                currently_loaded: Some(slot.slot_index) == loaded_slot,
            })
            .collect(),
    };

    tx.send(Ok(response))
        .await
        .map_err(|_| Status::cancelled("slot stream closed"))?;
    Ok(())
}

fn loaded_slot_for_team(join_registry: &OfficialSandboxJoinRegistry, team_id: &str) -> Option<i16> {
    join_registry
        .get(team_id)
        .map(|entry| entry.value().slot_index)
}

fn join_failed(message: impl Into<String>) -> JoinOfficialSandboxResponse {
    JoinOfficialSandboxResponse {
        status: OfficialSandboxCommandStatus::Failed as i32,
        message: message.into(),
    }
}
