//! gRPC FrontendMenuService implementation.

use proto::race::v1::frontend_menu_service_server::FrontendMenuService;
use proto::race::v1::{
    FrontendMenuState, GetFrontendMenuStateRequest, GetFrontendMenuStateResponse, RuntimeState,
    StreamFrontendMenuStateRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::db::repos::sandbox_config::SandboxConfigRepo;
use crate::runtime::engine_worker::{EngineActivityKind, EngineClient};
use crate::services::error_map::map_worker_err;
use crate::services::sandbox_mappers::{
    find_sandbox_by_id, runtime_activity_kind_to_proto, runtime_time_of_day_preset_to_proto,
    sandbox_runtime_info_from_record, sandbox_to_proto, utc_now_timestamp,
};

const STREAM_CHANNEL_CAPACITY: usize = 16;
const STREAM_POLL_INTERVAL_MS: u64 = 1000;

/// FrontendMenu service backed by sandbox config repository and runtime worker state.
#[derive(Clone)]
pub struct FrontendMenuServiceImpl {
    repo: SandboxConfigRepo,
    engine: EngineClient,
}

impl FrontendMenuServiceImpl {
    pub fn with_repo(repo: SandboxConfigRepo, engine: EngineClient) -> Self {
        Self { repo, engine }
    }

    async fn build_menu_state(&self) -> Result<FrontendMenuState, Status> {
        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;
        let snapshot =
            self.repo.get_snapshot().await.map_err(|err| {
                Status::internal(format!("failed to load sandbox configs: {err}"))
            })?;

        let active_sandbox = if matches!(runtime.activity_kind, EngineActivityKind::Sandbox) {
            runtime
                .active_sandbox_id
                .as_deref()
                .and_then(|sandbox_id| find_sandbox_by_id(&snapshot.sandboxes, sandbox_id))
        } else {
            None
        };

        let runtime_state = RuntimeState {
            revision: runtime.revision,
            activity_kind: runtime_activity_kind_to_proto(runtime.activity_kind) as i32,
            sandboxes: active_sandbox
                .map(|record| {
                    sandbox_runtime_info_from_record(
                        record,
                        runtime_time_of_day_preset_to_proto(runtime.time_of_day_preset),
                    )
                })
                .into_iter()
                .collect(),
            pending_sandbox_activation: None,
            server_time_utc: Some(utc_now_timestamp()),
        };

        Ok(FrontendMenuState {
            runtime: Some(runtime_state),
            configured_sandboxes: snapshot
                .sandboxes
                .into_iter()
                .map(sandbox_to_proto)
                .collect(),
        })
    }
}

#[tonic::async_trait]
impl FrontendMenuService for FrontendMenuServiceImpl {
    type StreamFrontendMenuStateStream = ReceiverStream<Result<FrontendMenuState, Status>>;

    async fn get_frontend_menu_state(
        &self,
        _request: Request<GetFrontendMenuStateRequest>,
    ) -> Result<Response<GetFrontendMenuStateResponse>, Status> {
        let state = self.build_menu_state().await?;
        Ok(Response::new(GetFrontendMenuStateResponse {
            state: Some(state),
        }))
    }

    async fn stream_frontend_menu_state(
        &self,
        _request: Request<StreamFrontendMenuStateRequest>,
    ) -> Result<Response<Self::StreamFrontendMenuStateStream>, Status> {
        let service = self.clone();
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            let mut last_state: Option<FrontendMenuState> = None;
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
