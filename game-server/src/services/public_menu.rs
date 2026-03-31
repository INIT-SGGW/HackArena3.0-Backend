//! gRPC PublicMenuService implementation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use proto::race::v1::public_menu_service_server::PublicMenuService;
use proto::race::v1::{
    GetPublicMenuStateRequest, GetPublicMenuStateResponse, PublicMenuState, PublicRuntimeState,
    PublicSandboxRuntimeMode, PublicUpcomingRaceSummary, StreamPublicMenuStateRequest,
    public_runtime_state,
};
#[cfg(feature = "official")]
use proto::race::v1::{
    PublicOfficialRaceRunningState, PublicOfficialRaceRuntimeInfo, PublicOfficialRaceStagingState,
    public_official_race_runtime_info,
};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::auth::auth_claims::TokenValidator;
use crate::db::repos::race_config::{RaceConfigRecord, RaceConfigRepo};
use crate::db::repos::sandbox_config::{SandboxConfigRecord, SandboxConfigRepo};
use crate::runtime::engine_worker::{EngineActivityKind, EngineClient};
#[cfg(feature = "official")]
use crate::runtime::engine_worker::EngineCommandTarget;
use crate::services::error_map::map_worker_err;
use crate::services::race::RaceRuntimeStore;
#[cfg(feature = "official")]
use crate::services::sandbox_admin::mappers::{
    find_sandbox_by_id, public_sandbox_runtime_info_from_record,
    runtime_time_of_day_preset_to_proto, unix_ms_to_timestamp, utc_now_timestamp,
};
use crate::services::submission::HpsTeamResolver;

const STREAM_CHANNEL_CAPACITY: usize = 16;
const STREAM_POLL_INTERVAL_MS: u64 = 1000;
const UPCOMING_RACE_WINDOW_MS: i64 = 60 * 60 * 1000;
const UPCOMING_CACHE_TTL_MS: i64 = 60 * 1000;
const SANDBOX_CONFIG_CACHE_TTL_MS: i64 = 60 * 1000;

fn comparable_menu_state(state: &PublicMenuState) -> PublicMenuState {
    let mut comparable = state.clone();
    if let Some(runtime) = comparable.runtime.as_mut() {
        runtime.server_time_utc = None;
    }
    comparable
}

/// Shared invalidation signal for upcoming-races cache.
#[derive(Clone, Default)]
pub(crate) struct UpcomingRacesCacheInvalidation {
    generation: Arc<AtomicU64>,
}

impl UpcomingRacesCacheInvalidation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    pub fn invalidate_for_change(&self, old_start_ms: Option<i64>, new_start_ms: Option<i64>) {
        let now_ms = current_unix_ms();
        if old_start_ms
            .map(|start_ms| is_within_upcoming_window(start_ms, now_ms))
            .unwrap_or(false)
            || new_start_ms
                .map(|start_ms| is_within_upcoming_window(start_ms, now_ms))
                .unwrap_or(false)
        {
            let _ = self.generation.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Shared invalidation signal for sandbox-config cache.
#[derive(Clone, Default)]
pub(crate) struct SandboxConfigCacheInvalidation {
    generation: Arc<AtomicU64>,
}

impl SandboxConfigCacheInvalidation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    pub fn invalidate(&self) {
        let _ = self.generation.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
struct UpcomingRacesCacheState {
    generation: u64,
    cached_at_ms: i64,
    races: Vec<PublicUpcomingRaceSummary>,
}

impl Default for UpcomingRacesCacheState {
    fn default() -> Self {
        Self {
            generation: 0,
            cached_at_ms: 0,
            races: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct SandboxConfigCacheState {
    generation: u64,
    cached_at_ms: i64,
    sandboxes: Vec<SandboxConfigRecord>,
}

impl Default for SandboxConfigCacheState {
    fn default() -> Self {
        Self {
            generation: 0,
            cached_at_ms: 0,
            sandboxes: Vec::new(),
        }
    }
}

/// PublicMenu service backed by sandbox config repository and runtime worker state.
#[derive(Clone)]
pub struct PublicMenuServiceImpl {
    sandbox_repo: SandboxConfigRepo,
    race_repo: RaceConfigRepo,
    engine: EngineClient,
    runtime_store: Arc<RaceRuntimeStore>,
    token_validator: Arc<TokenValidator>,
    team_resolver: Arc<HpsTeamResolver>,
    upcoming_invalidation: UpcomingRacesCacheInvalidation,
    sandbox_invalidation: SandboxConfigCacheInvalidation,
    upcoming_cache: Arc<RwLock<UpcomingRacesCacheState>>,
    sandbox_cache: Arc<RwLock<SandboxConfigCacheState>>,
}

impl PublicMenuServiceImpl {
    pub(crate) fn with_repo(
        sandbox_repo: SandboxConfigRepo,
        race_repo: RaceConfigRepo,
        engine: EngineClient,
        runtime_store: Arc<RaceRuntimeStore>,
        token_validator: Arc<TokenValidator>,
        team_resolver: Arc<HpsTeamResolver>,
        upcoming_invalidation: UpcomingRacesCacheInvalidation,
        sandbox_invalidation: SandboxConfigCacheInvalidation,
    ) -> Self {
        Self {
            sandbox_repo,
            race_repo,
            engine,
            runtime_store,
            token_validator,
            team_resolver,
            upcoming_invalidation,
            sandbox_invalidation,
            upcoming_cache: Arc::new(RwLock::new(UpcomingRacesCacheState::default())),
            sandbox_cache: Arc::new(RwLock::new(SandboxConfigCacheState::default())),
        }
    }

    async fn build_menu_state(&self, team_id: &str) -> Result<PublicMenuState, Status> {
        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;
        let sandbox_configs = self.get_sandbox_configs().await?;
        let active_car_counts = self.runtime_store.active_car_counts_by_sandbox();
        let joined_sandboxes = self.runtime_store.joined_sandbox_ids_for_team(team_id);
        let runtime_state = PublicRuntimeState {
            server_time_utc: Some(utc_now_timestamp()),
            active_mode: match runtime.activity_kind {
                EngineActivityKind::OfficialRace => {
                    #[cfg(feature = "official")]
                    {
                        let official_state = self.runtime_store.official_race_public_state();
                        let race_name = official_state
                            .as_ref()
                            .map(|state| state.race_name.trim())
                            .filter(|value| !value.is_empty())
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "Official Race".to_string());
                        let map_id = if runtime.map_id.trim().is_empty() {
                            official_state
                                .as_ref()
                                .map(|state| state.map_id.clone())
                                .unwrap_or_default()
                        } else {
                            runtime.map_id.clone()
                        };
                        let race_duration_sec =
                            self.runtime_store.official_race_duration_sec().unwrap_or(0);
                        let start_time_utc = official_state
                            .as_ref()
                            .and_then(|state| i64::try_from(state.prepared_at_ms).ok())
                            .map(unix_ms_to_timestamp);
                        let phase_state = if self.runtime_store.is_official_race_started() {
                            let planned_end_at_utc = self
                                .runtime_store
                                .official_race_started_at_ms()
                                .zip(self.runtime_store.official_race_duration_sec())
                                .and_then(|(started_at_ms, duration_sec)| {
                                    started_at_ms
                                        .checked_add(
                                            u64::from(duration_sec).saturating_mul(1_000),
                                        )
                                        .and_then(|end_ms| i64::try_from(end_ms).ok())
                                        .map(unix_ms_to_timestamp)
                                });
                            Some(public_official_race_runtime_info::PhaseState::Running(
                                PublicOfficialRaceRunningState { planned_end_at_utc },
                            ))
                        } else {
                            let expected_player_count = self
                                .runtime_store
                                .car_targets()
                                .iter()
                                .filter(|entry| {
                                    matches!(entry.value(), EngineCommandTarget::OfficialRace)
                                })
                                .count()
                                .try_into()
                                .unwrap_or(u32::MAX);
                            Some(public_official_race_runtime_info::PhaseState::Staging(
                                PublicOfficialRaceStagingState {
                                    ready_player_count: expected_player_count,
                                    expected_player_count,
                                },
                            ))
                        };
                        Some(public_runtime_state::ActiveMode::OfficialRace(
                            PublicOfficialRaceRuntimeInfo {
                                race_name,
                                map_id,
                                start_time_utc,
                                race_duration_sec,
                                phase_state,
                            },
                        ))
                    }
                    #[cfg(not(feature = "official"))]
                    {
                        None
                    }
                }
                EngineActivityKind::Sandbox => Some(public_runtime_state::ActiveMode::SandboxMode(
                    PublicSandboxRuntimeMode {
                        sandboxes: runtime
                            .active_sandboxes
                            .iter()
                            .filter_map(|active| {
                                find_sandbox_by_id(&sandbox_configs, &active.sandbox_id).map(
                                    |record| {
                                        public_sandbox_runtime_info_from_record(
                                            record,
                                            runtime_time_of_day_preset_to_proto(
                                                active.time_of_day_preset,
                                            ),
                                            active_car_counts
                                                .get(&active.sandbox_id)
                                                .copied()
                                                .unwrap_or(0),
                                            joined_sandboxes.contains(&active.sandbox_id),
                                        )
                                    },
                                )
                            })
                            .collect(),
                    },
                )),
                EngineActivityKind::None => None,
            },
        };

        Ok(PublicMenuState {
            runtime: Some(runtime_state),
            upcoming_races: self.get_upcoming_races().await?,
        })
    }

    async fn get_sandbox_configs(&self) -> Result<Vec<SandboxConfigRecord>, Status> {
        let now_ms = current_unix_ms();
        let generation = self.sandbox_invalidation.generation();

        {
            let cache = self.sandbox_cache.read().await;
            if cache.generation == generation
                && now_ms.saturating_sub(cache.cached_at_ms) <= SANDBOX_CONFIG_CACHE_TTL_MS
            {
                return Ok(cache.sandboxes.clone());
            }
        }

        let sandbox_snapshot =
            self.sandbox_repo.get_snapshot().await.map_err(|err| {
                Status::internal(format!("failed to load sandbox configs: {err}"))
            })?;

        let mut cache = self.sandbox_cache.write().await;
        cache.generation = generation;
        cache.cached_at_ms = now_ms;
        cache.sandboxes = sandbox_snapshot.sandboxes.clone();

        Ok(sandbox_snapshot.sandboxes)
    }

    async fn get_upcoming_races(&self) -> Result<Vec<PublicUpcomingRaceSummary>, Status> {
        let now_ms = current_unix_ms();
        let generation = self.upcoming_invalidation.generation();

        {
            let cache = self.upcoming_cache.read().await;
            if cache.generation == generation
                && now_ms.saturating_sub(cache.cached_at_ms) <= UPCOMING_CACHE_TTL_MS
            {
                return Ok(cache.races.clone());
            }
        }

        let race_snapshot = self
            .race_repo
            .get_snapshot()
            .await
            .map_err(|err| Status::internal(format!("failed to load race configs: {err}")))?;
        let races = build_upcoming_races(race_snapshot.races, now_ms);

        let mut cache = self.upcoming_cache.write().await;
        cache.generation = generation;
        cache.cached_at_ms = now_ms;
        cache.races = races.clone();

        Ok(races)
    }
}

fn build_upcoming_races(
    races: Vec<RaceConfigRecord>,
    now_ms: i64,
) -> Vec<PublicUpcomingRaceSummary> {
    races
        .into_iter()
        .filter(|race| is_within_upcoming_window(race.config.starts_at_ms, now_ms))
        .map(|race| PublicUpcomingRaceSummary {
            race_name: race.config.race_name,
            start_time_utc: Some(unix_ms_to_timestamp(race.config.starts_at_ms)),
            race_duration_sec: race.config.race_duration_sec,
        })
        .collect()
}

fn is_within_upcoming_window(start_ms: i64, now_ms: i64) -> bool {
    let window_end_ms = now_ms.saturating_add(UPCOMING_RACE_WINDOW_MS);
    start_ms >= now_ms && start_ms <= window_end_ms
}

fn current_unix_ms() -> i64 {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
    let nanos_ms = i64::from(duration.subsec_millis());
    seconds.saturating_mul(1000).saturating_add(nanos_ms)
}

#[tonic::async_trait]
impl PublicMenuService for PublicMenuServiceImpl {
    type StreamPublicMenuStateStream = ReceiverStream<Result<PublicMenuState, Status>>;

    async fn get_public_menu_state(
        &self,
        request: Request<GetPublicMenuStateRequest>,
    ) -> Result<Response<GetPublicMenuStateResponse>, Status> {
        let user_id = self.token_validator.subject(request.metadata()).await?;
        let team_id = self.team_resolver.resolve_team_id(&user_id).await?;
        let state = self.build_menu_state(&team_id).await?;
        Ok(Response::new(GetPublicMenuStateResponse {
            state: Some(state),
        }))
    }

    async fn stream_public_menu_state(
        &self,
        request: Request<StreamPublicMenuStateRequest>,
    ) -> Result<Response<Self::StreamPublicMenuStateStream>, Status> {
        let user_id = self.token_validator.subject(request.metadata()).await?;
        let team_id = self.team_resolver.resolve_team_id(&user_id).await?;
        let service = self.clone();
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            let mut last_comparable_state: Option<PublicMenuState> = None;
            let mut ticker =
                tokio::time::interval(tokio::time::Duration::from_millis(STREAM_POLL_INTERVAL_MS));

            loop {
                match service.build_menu_state(&team_id).await {
                    Ok(state) => {
                        let comparable = comparable_menu_state(&state);
                        if last_comparable_state.as_ref() != Some(&comparable) {
                            last_comparable_state = Some(comparable);
                            if tx.send(Ok(state)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                }

                ticker.tick().await;
                if tx.is_closed() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
