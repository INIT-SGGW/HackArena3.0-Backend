//! gRPC FrontendMenuService implementation.

use proto::race::v1::frontend_menu_service_server::FrontendMenuService;
use proto::race::v1::{
    FrontendMenuState, GetFrontendMenuStateRequest, GetFrontendMenuStateResponse,
    GhostModeSettings as ProtoGhostModeSettings, RuntimeActivityKind, RuntimeState,
    SandboxRuntimeInfo, StreamFrontendMenuStateRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::db::repos::sandbox_config::{
    GhostModeSettingsRecord, SandboxConfigRecord, SandboxConfigRepo,
};
use crate::runtime::engine_worker::{EngineActivityKind, EngineClient};
use crate::services::error_map::map_worker_err;
use crate::services::sandbox_mappers::{sandbox_to_proto, utc_now_timestamp};

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
            match find_unique_sandbox_by_map_id(&snapshot.sandboxes, &runtime.map_id) {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(error = %err, map_id = %runtime.map_id, "unable to resolve unique active sandbox by map_id");
                    None
                }
            }
        } else {
            None
        };

        let runtime_state = RuntimeState {
            revision: runtime.revision,
            activity_kind: runtime_activity_kind_to_proto(runtime.activity_kind) as i32,
            sandboxes: active_sandbox
                .map(sandbox_runtime_info_from_record)
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

fn runtime_activity_kind_to_proto(kind: EngineActivityKind) -> RuntimeActivityKind {
    match kind {
        EngineActivityKind::None => RuntimeActivityKind::None,
        EngineActivityKind::OfficialRace => RuntimeActivityKind::OfficialRace,
        EngineActivityKind::Sandbox => RuntimeActivityKind::Sandbox,
    }
}

fn sandbox_runtime_info_from_record(record: SandboxConfigRecord) -> SandboxRuntimeInfo {
    SandboxRuntimeInfo {
        sandbox_id: record.sandbox_id,
        sandbox_name: record.config.sandbox_name,
        map_id: record.config.map_id,
        active_time_of_day_preset: record.config.time_of_day_preset as i32,
        ghost_mode: record.config.ghost_mode.map(proto_ghost_mode_from_record),
        started_at_utc: None,
        closes_at_utc: None,
    }
}

fn proto_ghost_mode_from_record(record: GhostModeSettingsRecord) -> ProtoGhostModeSettings {
    ProtoGhostModeSettings {
        enabled: record.enabled,
        min_speed_enter_mps: record.min_speed_enter_mps,
        min_speed_exit_mps: record.min_speed_exit_mps,
        enter_delay_ms: record.enter_delay_ms,
        exit_delay_ms: record.exit_delay_ms,
        min_completed_laps: record.min_completed_laps,
        condition_logic: record.condition_logic as i32,
        overlap_exit_delay_ms: record.overlap_exit_delay_ms,
    }
}

fn find_unique_sandbox_by_map_id(
    sandboxes: &[SandboxConfigRecord],
    map_id: &str,
) -> Result<Option<SandboxConfigRecord>, &'static str> {
    let mut matching = sandboxes
        .iter()
        .filter(|entry| entry.config.map_id == map_id);
    let first = matching.next().cloned();
    let second_exists = matching.next().is_some();

    if second_exists {
        return Err("multiple sandbox configs share the same map_id");
    }

    Ok(first)
}
