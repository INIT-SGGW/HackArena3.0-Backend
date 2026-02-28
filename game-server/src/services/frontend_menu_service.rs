//! gRPC PublicMenuService implementation.

use proto::race::v1::public_menu_service_server::PublicMenuService;
use proto::race::v1::{
    GetPublicMenuStateRequest, GetPublicMenuStateResponse, PublicMenuState, PublicRuntimeState,
    StreamPublicMenuStateRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::db::repos::sandbox_config::SandboxConfigRepo;
use crate::runtime::engine_worker::EngineClient;
use crate::services::error_map::map_worker_err;
use crate::services::sandbox_mappers::{
    find_sandbox_by_id, public_sandbox_runtime_info_from_record, runtime_activity_kind_to_proto,
    runtime_time_of_day_preset_to_proto, utc_now_timestamp,
};

const STREAM_CHANNEL_CAPACITY: usize = 16;
const STREAM_POLL_INTERVAL_MS: u64 = 1000;

/// PublicMenu service backed by sandbox config repository and runtime worker state.
#[derive(Clone)]
pub struct FrontendMenuServiceImpl {
    repo: SandboxConfigRepo,
    engine: EngineClient,
}

impl FrontendMenuServiceImpl {
    pub fn with_repo(repo: SandboxConfigRepo, engine: EngineClient) -> Self {
        Self { repo, engine }
    }

    async fn build_menu_state(&self) -> Result<PublicMenuState, Status> {
        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;
        let snapshot =
            self.repo.get_snapshot().await.map_err(|err| {
                Status::internal(format!("failed to load sandbox configs: {err}"))
            })?;

        let runtime_state = PublicRuntimeState {
            activity_kind: runtime_activity_kind_to_proto(runtime.activity_kind) as i32,
            active_sandboxes: runtime
                .active_sandboxes
                .iter()
                .filter_map(|active| {
                    find_sandbox_by_id(&snapshot.sandboxes, &active.sandbox_id).map(|record| {
                        public_sandbox_runtime_info_from_record(
                            record,
                            runtime_time_of_day_preset_to_proto(active.time_of_day_preset),
                            0,
                        )
                    })
                })
                .collect(),
            server_time_utc: Some(utc_now_timestamp()),
        };

        Ok(PublicMenuState {
            runtime: Some(runtime_state),
        })
    }
}

#[tonic::async_trait]
impl PublicMenuService for FrontendMenuServiceImpl {
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
            let mut last_state: Option<PublicMenuState> = None;
            let mut ticker =
                tokio::time::interval(tokio::time::Duration::from_millis(STREAM_POLL_INTERVAL_MS));

            loop {
                match service.build_menu_state().await {
                    Ok(state) => {
                        if last_state.as_ref() != Some(&state) {
                            last_state = Some(state.clone());
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
