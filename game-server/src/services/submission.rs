//! gRPC SubmissionService implementation with async remote build worker.

use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};
use dashmap::DashMap;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use proto::hackarena::platform::common::v1::Uuid as PlatformUuid;
use proto::hackarena::platform::teams::v1::teams_service_client::TeamsServiceClient;
use proto::hackarena::platform::teams::v1::{GetTeamByUserRequest, Team};
use proto::submission::v1::official_sandbox_command_service_server::OfficialSandboxCommandService;
use proto::submission::v1::slot_query_service_server::SlotQueryService;
use proto::submission::v1::submission_service_server::SubmissionService;
use proto::submission::v1::{
    BuildFinished, BuildLog, BuildStarted, GetSlotsRequest, GetSlotsResponse,
    JoinOfficialSandboxRequest, JoinOfficialSandboxResponse, LeaveOfficialSandboxRequest,
    LeaveOfficialSandboxResponse, OfficialSandboxCommandStatus, SlotDto, SlotSummaryDto,
    StreamSlotsRequest, StreamSlotsResponse, SubmitBuildRequest, SubmitBuildStreamResponse,
    WrapperKind, submit_build_stream_response,
};
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, USER_AGENT};
use serde::Deserialize;
use tar::{Archive, Builder};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request, Response, Status};

use crate::auth::auth_claims::TokenValidator;
use crate::config::Config;
use crate::db::repos::submission::{NewSubmissionRecord, SubmissionRepo};
use crate::runtime::engine_worker::{EngineClient, EngineCommandTarget};
use crate::services::error_map::map_worker_err;
use crate::services::race::{RaceRuntimeStore, RuntimeCarIdentity};

const TEAM_EDITION: &str = "3";
const TEAM_CACHE_TTL: Duration = Duration::from_secs(300);
const HPS_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const KEYCLOAK_TOKEN_TIMEOUT: Duration = Duration::from_secs(10);
const SERVICE_TOKEN_DEFAULT_TTL_SEC: u64 = 300;
const SERVICE_TOKEN_TTL_SAFETY_SEC: u64 = 30;
const SERVICE_TOKEN_MIN_TTL_SEC: u64 = 10;
const SUBMISSION_QUEUE_CAPACITY: usize = 64;
const SLOT_UPDATE_CHANNEL_CAPACITY: usize = 128;
const SLOT_STREAM_CHANNEL_CAPACITY: usize = 8;
const BUILD_EVENT_CHANNEL_CAPACITY: usize = 128;
const SUBMISSIONS_ROOT: &str = ".submissions";
const GITHUB_API_BASE_URL: &str = "https://api.github.com";
const GITHUB_API_TIMEOUT: Duration = Duration::from_secs(30);
const REGISTRY_API_TIMEOUT: Duration = Duration::from_secs(10);
const REGISTRY_MANIFEST_V2_ACCEPT: &str = "application/vnd.docker.distribution.manifest.v2+json";
const REGISTRY_DIGEST_HEADER: &str = "docker-content-digest";

#[derive(Debug, Clone)]
pub(crate) struct SubmissionBuildJob {
    submission_id: String,
    team_id: String,
    slot_index: i16,
    wrapper_version: String,
    archive_path: PathBuf,
    events_tx: mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
}

#[derive(Debug, Clone)]
pub(crate) struct TeamSandboxJoinState {
    sandbox_id: String,
    slot_index: i16,
    public_car_id: u64,
    engine_car_id: u64,
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

    async fn current_or_fetch_token(&self) -> Result<String, Status> {
        if let Some(cached) = self.auth_token.read().await.clone()
            && cached.expires_at > Instant::now()
        {
            return Ok(cached.token);
        }
        self.refresh_token().await
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
    engine: EngineClient,
    runtime_store: Arc<RaceRuntimeStore>,
    slot_updates_tx: broadcast::Sender<String>,
    join_registry: OfficialSandboxJoinRegistry,
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
        engine: EngineClient,
        runtime_store: Arc<RaceRuntimeStore>,
        slot_updates_tx: broadcast::Sender<String>,
        join_registry: OfficialSandboxJoinRegistry,
    ) -> Self {
        Self {
            repo,
            token_validator,
            team_resolver,
            engine,
            runtime_store,
            slot_updates_tx,
            join_registry,
            join_command_lock: Arc::new(Mutex::new(())),
        }
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
        if wrapper_kind != WrapperKind::Python {
            return Err(Status::invalid_argument(
                "only WRAPPER_KIND_PYTHON is supported in MVP",
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
        let archive_dir = self.submissions_root.join(&submission_id);
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
                wrapper_version: wrapper_version.to_string(),
                archive_path,
                events_tx: events_tx.clone(),
            })
            .await
            .map_err(|_| Status::unavailable("submission worker is unavailable"))?;

        emit_build_started(&events_tx, &submission_id).await;

        Ok(Response::new(ReceiverStream::new(events_rx)))
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

        self.join_registry.insert(
            team_id.clone(),
            TeamSandboxJoinState {
                sandbox_id: sandbox_id.clone(),
                slot_index,
                public_car_id,
                engine_car_id,
            },
        );
        let _ = self.slot_updates_tx.send(team_id.clone());

        tracing::info!(
            team_id = %team_id,
            sandbox_id = %sandbox_id,
            slot_index,
            submission_id = %slot_submission.submission_id,
            public_car_id,
            engine_car_id,
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

        let target = EngineCommandTarget::Sandbox {
            sandbox_id: join_state.sandbox_id.clone(),
        };
        self.engine
            .despawn_car_in(target, join_state.engine_car_id)
            .await
            .map_err(map_worker_err)?;

        self.runtime_store.remove_car(join_state.public_car_id);
        self.join_registry.remove(&team_id);
        let _ = self.slot_updates_tx.send(team_id.clone());

        tracing::info!(
            team_id = %team_id,
            sandbox_id = %join_state.sandbox_id,
            slot_index = join_state.slot_index,
            public_car_id = join_state.public_car_id,
            engine_car_id = join_state.engine_car_id,
            "official sandbox leave succeeded"
        );
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
    tokio::task::spawn_blocking(move || prepare_build_context(&archive_path, &prepare_context_dir))
        .await
        .context("build-context task join error")??;
    prepare_wrapper_wheel(cfg, &job.wrapper_version, &context_dir, events_tx).await?;

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

async fn prepare_wrapper_wheel(
    cfg: &Config,
    wrapper_version: &str,
    context_dir: &Path,
    events_tx: &mpsc::Sender<Result<SubmitBuildStreamResponse, Status>>,
) -> anyhow::Result<()> {
    emit_build_log_line(events_tx, "[wrapper-fetch] resolving release asset").await;
    let (release_tag, asset_version) = normalize_wrapper_version(wrapper_version)?;
    let asset_name = format!("hackarena3-{asset_version}-py3-none-any.whl");
    let release_url = format!(
        "{}/repos/{}/{}/releases/tags/{}",
        GITHUB_API_BASE_URL, cfg.wrapper_gh_owner, cfg.wrapper_gh_repo, release_tag
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
            github_release_error_message(release_response.status().as_u16(), &release_tag)
        );
    }

    let release: GitHubReleaseResponse = release_response
        .json()
        .await
        .context("wrapper-fetch: failed to decode GitHub release payload")?;
    let asset = release
        .assets
        .into_iter()
        .find(|entry| entry.name == asset_name)
        .ok_or_else(|| {
            anyhow!("wrapper-fetch: release `{release_tag}` does not contain asset `{asset_name}`")
        })?;

    emit_build_log_line(
        events_tx,
        format!(
            "[wrapper-fetch] downloading `{}` from {}/{}",
            asset.name, cfg.wrapper_gh_owner, cfg.wrapper_gh_repo
        ),
    )
    .await;

    let asset_download_url = format!(
        "{}/repos/{}/{}/releases/assets/{}",
        GITHUB_API_BASE_URL, cfg.wrapper_gh_owner, cfg.wrapper_gh_repo, asset.id
    );
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
    let wheel_bytes = asset_response
        .bytes()
        .await
        .context("wrapper-fetch: failed to read wheel bytes")?;

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
    fs::write(&dockerfile_path, build_python_dockerfile(&asset.name))
        .await
        .with_context(|| {
            format!(
                "failed to write Dockerfile at {}",
                dockerfile_path.display()
            )
        })?;

    emit_build_log_line(events_tx, "[wrapper-fetch] wrapper wheel prepared").await;
    Ok(())
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

fn build_python_dockerfile(wheel_file_name: &str) -> String {
    format!(
        r#"FROM python:3.10-slim
WORKDIR /app
COPY wheels/{wheel_file_name} /app/wheels/{wheel_file_name}
COPY user/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir /app/wheels/{wheel_file_name}
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY user/src /app/src
"#
    )
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

fn prepare_build_context(archive_path: &Path, context_dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(context_dir).context("failed to create build context directory")?;
    let user_dir = context_dir.join("user");
    std::fs::create_dir_all(&user_dir).context("failed to create user directory")?;
    let extracted_entries = extract_archive_secure(archive_path, &user_dir)?;

    let user_src = user_dir.join("src");
    if !user_src.is_dir() {
        let sample = format_path_sample(&extracted_entries, 20);
        let nested_user_src = user_dir.join("user").join("src").is_dir();
        let extra_hint = if nested_user_src {
            " Found nested `user/src`; archive likely has extra top-level `user/` folder."
        } else {
            ""
        };
        bail!(
            "archive validation failed: expected `src/` at archive root (mapped to build-context/user/src). Extracted entries sample: {sample}.{extra_hint}"
        );
    }
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
