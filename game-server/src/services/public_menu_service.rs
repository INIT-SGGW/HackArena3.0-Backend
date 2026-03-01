//! gRPC PublicMenuService implementation.

use proto::race::v1::public_menu_service_server::PublicMenuService;
use proto::race::v1::{
    GetPublicMenuStateRequest, GetPublicMenuStateResponse, PublicMenuState, PublicRuntimeState,
    PublicSandboxRuntimeMode, PublicUpcomingRaceSummary, StreamPublicMenuStateRequest,
    public_runtime_state,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::db::repos::race_config::{RaceConfigRecord, RaceConfigRepo};
use crate::db::repos::sandbox_config::SandboxConfigRepo;
use crate::runtime::engine_worker::{EngineActivityKind, EngineClient};
use crate::services::error_map::map_worker_err;
use crate::services::sandbox_mappers::{
    find_sandbox_by_id, public_sandbox_runtime_info_from_record,
    runtime_time_of_day_preset_to_proto, unix_ms_to_timestamp, utc_now_timestamp,
};

const STREAM_CHANNEL_CAPACITY: usize = 16;
const STREAM_POLL_INTERVAL_MS: u64 = 1000;
const UPCOMING_RACE_WINDOW_MS: i64 = 60 * 60 * 1000;

fn comparable_menu_state(state: &PublicMenuState) -> PublicMenuState {
    let mut comparable = state.clone();
    if let Some(runtime) = comparable.runtime.as_mut() {
        runtime.server_time_utc = None;
    }
    comparable
}

/// PublicMenu service backed by sandbox config repository and runtime worker state.
#[derive(Clone)]
pub struct PublicMenuServiceImpl {
    sandbox_repo: SandboxConfigRepo,
    race_repo: RaceConfigRepo,
    engine: EngineClient,
}

impl PublicMenuServiceImpl {
    pub fn with_repo(
        sandbox_repo: SandboxConfigRepo,
        race_repo: RaceConfigRepo,
        engine: EngineClient,
    ) -> Self {
        Self {
            sandbox_repo,
            race_repo,
            engine,
        }
    }

    async fn build_menu_state(&self) -> Result<PublicMenuState, Status> {
        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;
        let sandbox_snapshot =
            self.sandbox_repo.get_snapshot().await.map_err(|err| {
                Status::internal(format!("failed to load sandbox configs: {err}"))
            })?;
        let race_snapshot = self
            .race_repo
            .get_snapshot()
            .await
            .map_err(|err| Status::internal(format!("failed to load race configs: {err}")))?;

        let runtime_state = PublicRuntimeState {
            server_time_utc: Some(utc_now_timestamp()),
            active_mode: match runtime.activity_kind {
                EngineActivityKind::Sandbox => Some(public_runtime_state::ActiveMode::SandboxMode(
                    PublicSandboxRuntimeMode {
                        sandboxes: runtime
                            .active_sandboxes
                            .iter()
                            .filter_map(|active| {
                                find_sandbox_by_id(&sandbox_snapshot.sandboxes, &active.sandbox_id)
                                    .map(|record| {
                                        public_sandbox_runtime_info_from_record(
                                            record,
                                            runtime_time_of_day_preset_to_proto(
                                                active.time_of_day_preset,
                                            ),
                                            0,
                                        )
                                    })
                            })
                            .collect(),
                    },
                )),
                EngineActivityKind::None | EngineActivityKind::OfficialRace => None,
            },
        };

        Ok(PublicMenuState {
            runtime: Some(runtime_state),
            upcoming_races: build_upcoming_races(race_snapshot.races),
        })
    }
}

fn build_upcoming_races(races: Vec<RaceConfigRecord>) -> Vec<PublicUpcomingRaceSummary> {
    let now_ms = current_unix_ms();
    let window_end_ms = now_ms.saturating_add(UPCOMING_RACE_WINDOW_MS);

    races
        .into_iter()
        .filter(|race| {
            race.config.starts_at_ms >= now_ms && race.config.starts_at_ms <= window_end_ms
        })
        .map(|race| PublicUpcomingRaceSummary {
            race_name: race.config.race_name,
            start_time_utc: Some(unix_ms_to_timestamp(race.config.starts_at_ms)),
            race_duration_sec: race.config.race_duration_sec,
        })
        .collect()
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
        _request: Request<GetPublicMenuStateRequest>,
    ) -> Result<Response<GetPublicMenuStateResponse>, Status> {
        let state = self.build_menu_state().await?;
        Ok(Response::new(GetPublicMenuStateResponse {
            state: Some(state),
        }))
    }

    async fn stream_public_menu_state(
        &self,
        _request: Request<StreamPublicMenuStateRequest>,
    ) -> Result<Response<Self::StreamPublicMenuStateStream>, Status> {
        let service = self.clone();
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            let mut last_comparable_state: Option<PublicMenuState> = None;
            let mut ticker =
                tokio::time::interval(tokio::time::Duration::from_millis(STREAM_POLL_INTERVAL_MS));

            loop {
                match service.build_menu_state().await {
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
