//! Local-only sandbox admin service implementation.

mod helpers;
mod mappers;

use std::collections::HashMap;
use std::sync::Arc;

use crate::local::map_assets::LocalMapAssetsSync;
use crate::local::sandbox_config_store::{
    LocalSandboxConfigRecord, LocalSandboxConfigStore, LocalTimeOfDaySettingsRecord,
    LocalWeatherSettingsRecord, local_sandbox_input_from_proto, local_sandbox_to_proto,
};
use crate::runtime::engine_worker::{EngineActivityKind, EngineClient};
use crate::services::error_map::map_worker_err;
use crate::services::race::RaceRuntimeStore;
use crate::services::weather::{LocalWeatherEvent, LocalWeatherEventHub, LocalWeatherEventKind};
use proto::race::v1::local_sandbox_admin_service_server::LocalSandboxAdminService;
use proto::race::v1::{
    CreateLocalSandboxConfigRequest, CreateLocalSandboxConfigResponse,
    DeleteLocalSandboxConfigRequest, DeleteLocalSandboxConfigResponse, GetLocalRuntimeStateRequest,
    GetLocalRuntimeStateResponse, GetLocalSandboxConfigsRequest, GetLocalSandboxConfigsResponse,
    LocalActiveSandboxRuntimeInfo, LocalRuntimeState, SetLocalSandboxActivationRequest,
    SetLocalSandboxActivationResponse, StreamLocalRuntimeStateRequest,
    StreamLocalRuntimeStateResponse, UpdateLocalSandboxConfigRequest,
    UpdateLocalSandboxConfigResponse, UpdateLocalSandboxSpawnModeRequest,
    UpdateLocalSandboxSpawnModeResponse, UpdateLocalSandboxTimeOfDayRequest,
    UpdateLocalSandboxTimeOfDayResponse, UpdateLocalSandboxWeatherRequest,
    UpdateLocalSandboxWeatherResponse,
};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use self::helpers::{find_local_sandbox_by_id, local_sandbox_id_v5, map_store_err};
use self::mappers::{
    engine_ghost_mode_settings_from_local_record, local_ghost_mode_to_proto,
    local_spawn_mode_from_proto_value, local_spawn_mode_to_proto, local_time_of_day_from_proto,
    local_time_of_day_to_proto, local_weather_from_proto, local_weather_to_proto,
    resolve_runtime_time_of_day_preset, runtime_time_of_day_preset_to_proto,
    runtime_weather_now_from_local, utc_now_timestamp, weather_params_from_local,
};

#[derive(Clone)]
pub struct LocalSandboxAdminServiceImpl {
    store: LocalSandboxConfigStore,
    engine: EngineClient,
    runtime_store: Arc<RaceRuntimeStore>,
    max_active_sandboxes: u32,
    map_assets_sync: Arc<LocalMapAssetsSync>,
    started_at_utc: Arc<RwLock<HashMap<String, prost_types::Timestamp>>>,
    weather_events: LocalWeatherEventHub,
}

impl LocalSandboxAdminServiceImpl {
    const RUNTIME_STREAM_CHANNEL_CAPACITY: usize = 16;
    const RUNTIME_STREAM_POLL_INTERVAL_MS: u64 = 500;

    pub fn new(
        store: LocalSandboxConfigStore,
        engine: EngineClient,
        runtime_store: Arc<RaceRuntimeStore>,
        max_active_sandboxes: u32,
        map_assets_sync: Arc<LocalMapAssetsSync>,
        weather_events: LocalWeatherEventHub,
    ) -> Self {
        Self {
            store,
            engine,
            runtime_store,
            max_active_sandboxes,
            map_assets_sync,
            started_at_utc: Arc::new(RwLock::new(HashMap::new())),
            weather_events,
        }
    }

    async fn require_sandbox_not_active(&self, sandbox_id: &str) -> Result<(), Status> {
        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;
        if runtime
            .active_sandboxes
            .iter()
            .any(|entry| entry.sandbox_id == sandbox_id)
        {
            return Err(Status::failed_precondition(
                "active sandbox cannot be modified",
            ));
        }
        Ok(())
    }

    async fn apply_time_of_day_if_active(
        &self,
        sandbox_id: &str,
        time_of_day: LocalTimeOfDaySettingsRecord,
    ) {
        let runtime_before = match self.engine.runtime_state().await {
            Ok(state) => state,
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "failed to read runtime state for local time-of-day apply"
                );
                return;
            }
        };
        if !runtime_before
            .active_sandboxes
            .iter()
            .any(|entry| entry.sandbox_id == sandbox_id)
        {
            return;
        }

        let preset = resolve_runtime_time_of_day_preset(time_of_day);
        if let Err(err) = self
            .engine
            .set_runtime_time_of_day(
                runtime_before.revision,
                Some(sandbox_id.to_string()),
                preset,
            )
            .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %err,
                "failed to apply local sandbox time-of-day to active runtime"
            );
        }
    }

    async fn apply_weather_if_active(
        &self,
        sandbox_id: &str,
        weather: LocalWeatherSettingsRecord,
    ) -> bool {
        let runtime_before = match self.engine.runtime_state().await {
            Ok(state) => state,
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "failed to read runtime state for local weather apply"
                );
                return false;
            }
        };
        if !runtime_before
            .active_sandboxes
            .iter()
            .any(|entry| entry.sandbox_id == sandbox_id)
        {
            return false;
        }

        let weather_now = runtime_weather_now_from_local(weather);
        let weather = weather_params_from_local(weather);
        if let Err(err) = self
            .engine
            .set_sandbox_weather(sandbox_id.to_string(), weather, weather_now)
            .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %err,
                "failed to apply local sandbox weather to active runtime"
            );
            return false;
        }
        true
    }

    fn publish_weather_updated(&self, sandbox_id: &str) {
        self.weather_events.publish(LocalWeatherEvent {
            sandbox_id: sandbox_id.to_string(),
            kind: LocalWeatherEventKind::Updated,
        });
    }

    fn publish_weather_deactivated(&self, sandbox_id: &str) {
        self.weather_events.publish(LocalWeatherEvent {
            sandbox_id: sandbox_id.to_string(),
            kind: LocalWeatherEventKind::Deactivated,
        });
    }

    async fn build_runtime_state(&self) -> Result<LocalRuntimeState, Status> {
        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;
        let snapshot = self.store.get_snapshot().await;
        let started_at = self.started_at_utc.read().await.clone();
        let active_car_counts = self.runtime_store.active_car_counts_by_sandbox();

        let mut active_sandboxes = Vec::with_capacity(runtime.active_sandboxes.len());
        for active in &runtime.active_sandboxes {
            let Some(record) = snapshot
                .sandboxes
                .iter()
                .find(|entry| entry.sandbox_id == active.sandbox_id)
            else {
                tracing::warn!(
                    sandbox_id = %active.sandbox_id,
                    "active local sandbox is missing persisted config; omitting from runtime state response"
                );
                continue;
            };

            active_sandboxes.push(LocalActiveSandboxRuntimeInfo {
                sandbox_id: record.sandbox_id.clone(),
                sandbox_name: record.config.sandbox_name.clone(),
                map_id: record.config.map_id.clone(),
                active_time_of_day_preset: runtime_time_of_day_preset_to_proto(
                    active.time_of_day_preset,
                ) as i32,
                time_of_day: Some(local_time_of_day_to_proto(record.config.time_of_day)),
                ghost_mode: record.config.ghost_mode.map(local_ghost_mode_to_proto),
                weather: Some(local_weather_to_proto(record.config.weather)),
                spawn_mode: local_spawn_mode_to_proto(record.config.spawn_mode) as i32,
                started_at_utc: started_at.get(&record.sandbox_id).cloned(),
                active_player_count: active_car_counts
                    .get(&record.sandbox_id)
                    .copied()
                    .unwrap_or(0),
            });
        }

        Ok(LocalRuntimeState {
            revision: runtime.revision,
            server_time_utc: Some(utc_now_timestamp()),
            active_sandboxes,
        })
    }
}

#[tonic::async_trait]
impl LocalSandboxAdminService for LocalSandboxAdminServiceImpl {
    type StreamLocalRuntimeStateStream =
        ReceiverStream<Result<StreamLocalRuntimeStateResponse, Status>>;

    async fn get_local_sandbox_configs(
        &self,
        request: Request<GetLocalSandboxConfigsRequest>,
    ) -> Result<Response<GetLocalSandboxConfigsResponse>, Status> {
        let _ = request.into_inner();
        let snapshot = self.store.get_snapshot().await;
        Ok(Response::new(GetLocalSandboxConfigsResponse {
            revision: snapshot.revision,
            sandboxes: snapshot
                .sandboxes
                .into_iter()
                .map(local_sandbox_to_proto)
                .collect(),
        }))
    }

    async fn create_local_sandbox_config(
        &self,
        request: Request<CreateLocalSandboxConfigRequest>,
    ) -> Result<Response<CreateLocalSandboxConfigResponse>, Status> {
        let request = request.into_inner();
        let config = request
            .config
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("config is required"))
            .and_then(|cfg| local_sandbox_input_from_proto(cfg).map_err(map_store_err))?;
        let sandbox = LocalSandboxConfigRecord {
            sandbox_id: local_sandbox_id_v5(&config, request.expected_revision),
            config,
        };
        self.map_assets_sync
            .ensure_map_cached(&sandbox.config.map_id)
            .await?;
        let revision = self
            .store
            .create_config(request.expected_revision, sandbox.clone())
            .await
            .map_err(map_store_err)?;

        Ok(Response::new(CreateLocalSandboxConfigResponse {
            revision,
            sandbox: Some(local_sandbox_to_proto(sandbox)),
        }))
    }

    async fn update_local_sandbox_config(
        &self,
        request: Request<UpdateLocalSandboxConfigRequest>,
    ) -> Result<Response<UpdateLocalSandboxConfigResponse>, Status> {
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id must be non-empty"));
        }
        self.require_sandbox_not_active(&request.sandbox_id).await?;

        let config = request
            .config
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("config is required"))
            .and_then(|cfg| local_sandbox_input_from_proto(cfg).map_err(map_store_err))?;
        let sandbox = LocalSandboxConfigRecord {
            sandbox_id: request.sandbox_id,
            config,
        };
        self.map_assets_sync
            .ensure_map_cached(&sandbox.config.map_id)
            .await?;

        let revision = self
            .store
            .update_config(request.expected_revision, sandbox.clone())
            .await
            .map_err(map_store_err)?;

        Ok(Response::new(UpdateLocalSandboxConfigResponse {
            revision,
            sandbox: Some(local_sandbox_to_proto(sandbox)),
        }))
    }

    async fn delete_local_sandbox_config(
        &self,
        request: Request<DeleteLocalSandboxConfigRequest>,
    ) -> Result<Response<DeleteLocalSandboxConfigResponse>, Status> {
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id must be non-empty"));
        }
        self.require_sandbox_not_active(&request.sandbox_id).await?;

        let revision = self
            .store
            .delete_config(request.expected_revision, &request.sandbox_id)
            .await
            .map_err(map_store_err)?;
        self.started_at_utc
            .write()
            .await
            .remove(&request.sandbox_id);

        Ok(Response::new(DeleteLocalSandboxConfigResponse {
            revision,
            sandbox_id: request.sandbox_id,
        }))
    }

    async fn update_local_sandbox_time_of_day(
        &self,
        request: Request<UpdateLocalSandboxTimeOfDayRequest>,
    ) -> Result<Response<UpdateLocalSandboxTimeOfDayResponse>, Status> {
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id must be non-empty"));
        }
        let time_of_day = request
            .time_of_day
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("time_of_day is required"))
            .and_then(local_time_of_day_from_proto)?;

        let revision = self
            .store
            .update_time_of_day(request.expected_revision, &request.sandbox_id, time_of_day)
            .await
            .map_err(map_store_err)?;

        self.apply_time_of_day_if_active(&request.sandbox_id, time_of_day)
            .await;

        Ok(Response::new(UpdateLocalSandboxTimeOfDayResponse {
            revision,
            sandbox_id: request.sandbox_id,
            time_of_day: Some(local_time_of_day_to_proto(time_of_day)),
        }))
    }

    async fn update_local_sandbox_weather(
        &self,
        request: Request<UpdateLocalSandboxWeatherRequest>,
    ) -> Result<Response<UpdateLocalSandboxWeatherResponse>, Status> {
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id must be non-empty"));
        }
        let weather = request
            .weather
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("weather is required"))
            .and_then(local_weather_from_proto)?;

        let revision = self
            .store
            .update_weather(request.expected_revision, &request.sandbox_id, weather)
            .await
            .map_err(map_store_err)?;

        if self
            .apply_weather_if_active(&request.sandbox_id, weather)
            .await
        {
            self.publish_weather_updated(&request.sandbox_id);
        }

        Ok(Response::new(UpdateLocalSandboxWeatherResponse {
            revision,
            sandbox_id: request.sandbox_id,
            weather: Some(local_weather_to_proto(weather)),
        }))
    }

    async fn update_local_sandbox_spawn_mode(
        &self,
        request: Request<UpdateLocalSandboxSpawnModeRequest>,
    ) -> Result<Response<UpdateLocalSandboxSpawnModeResponse>, Status> {
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id must be non-empty"));
        }

        let spawn_mode = local_spawn_mode_from_proto_value(request.spawn_mode)?;

        let revision = self
            .store
            .update_spawn_mode(request.expected_revision, &request.sandbox_id, spawn_mode)
            .await
            .map_err(map_store_err)?;

        Ok(Response::new(UpdateLocalSandboxSpawnModeResponse {
            revision,
            sandbox_id: request.sandbox_id,
            spawn_mode: local_spawn_mode_to_proto(spawn_mode) as i32,
        }))
    }

    async fn set_local_sandbox_activation(
        &self,
        request: Request<SetLocalSandboxActivationRequest>,
    ) -> Result<Response<SetLocalSandboxActivationResponse>, Status> {
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id must be non-empty"));
        }

        if request.activate {
            let snapshot = self.store.get_snapshot().await;
            let sandbox = find_local_sandbox_by_id(&snapshot.sandboxes, &request.sandbox_id)
                .ok_or_else(|| {
                    Status::not_found(format!(
                        "local sandbox config not found: {}",
                        request.sandbox_id
                    ))
                })?;
            self.map_assets_sync
                .ensure_map_cached(&sandbox.config.map_id)
                .await?;

            let runtime_before = self.engine.runtime_state().await.map_err(map_worker_err)?;
            if runtime_before.revision != request.expected_revision {
                return Err(Status::failed_precondition(format!(
                    "runtime revision mismatch: expected {}, actual {}",
                    request.expected_revision, runtime_before.revision
                )));
            }

            let already_active = runtime_before
                .active_sandboxes
                .iter()
                .any(|entry| entry.sandbox_id == sandbox.sandbox_id);
            if !already_active
                && runtime_before.active_sandboxes.len() as u32 >= self.max_active_sandboxes
            {
                return Err(Status::failed_precondition(format!(
                    "active sandbox limit reached ({})",
                    self.max_active_sandboxes
                )));
            }

            let runtime_after = self
                .engine
                .switch_runtime(
                    request.expected_revision,
                    EngineActivityKind::Sandbox,
                    sandbox.config.map_id.clone(),
                    Some(sandbox.sandbox_id.clone()),
                    Some(resolve_runtime_time_of_day_preset(
                        sandbox.config.time_of_day,
                    )),
                    Some(engine_ghost_mode_settings_from_local_record(
                        sandbox.config.ghost_mode,
                    )),
                )
                .await
                .map_err(map_worker_err)?;

            let weather_now = runtime_weather_now_from_local(sandbox.config.weather);
            let weather_params = weather_params_from_local(sandbox.config.weather);
            if let Err(err) = self
                .engine
                .set_sandbox_weather(sandbox.sandbox_id.clone(), weather_params, weather_now)
                .await
            {
                tracing::warn!(
                    sandbox_id = %sandbox.sandbox_id,
                    error = %err,
                    "failed to apply local sandbox weather during activation; rolling back activation"
                );
                if let Err(rollback_err) = self
                    .engine
                    .deactivate_sandbox(runtime_after.revision, sandbox.sandbox_id.clone())
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox.sandbox_id,
                        error = %rollback_err,
                        "failed to rollback sandbox activation after weather apply failure"
                    );
                }
                return Err(Status::internal(
                    "failed to apply local sandbox weather during activation",
                ));
            }

            self.started_at_utc
                .write()
                .await
                .insert(sandbox.sandbox_id.clone(), utc_now_timestamp());
            self.publish_weather_updated(&sandbox.sandbox_id);

            return Ok(Response::new(SetLocalSandboxActivationResponse {
                revision: runtime_after.revision,
                activate: true,
                sandbox_id: sandbox.sandbox_id,
            }));
        }

        let runtime_after = self
            .engine
            .deactivate_sandbox(request.expected_revision, request.sandbox_id.clone())
            .await
            .map_err(map_worker_err)?;
        self.started_at_utc
            .write()
            .await
            .remove(&request.sandbox_id);
        self.publish_weather_deactivated(&request.sandbox_id);

        Ok(Response::new(SetLocalSandboxActivationResponse {
            revision: runtime_after.revision,
            activate: false,
            sandbox_id: request.sandbox_id,
        }))
    }

    async fn get_local_runtime_state(
        &self,
        request: Request<GetLocalRuntimeStateRequest>,
    ) -> Result<Response<GetLocalRuntimeStateResponse>, Status> {
        let _ = request.into_inner();
        let state = self.build_runtime_state().await?;

        Ok(Response::new(GetLocalRuntimeStateResponse {
            state: Some(state),
        }))
    }

    async fn stream_local_runtime_state(
        &self,
        request: Request<StreamLocalRuntimeStateRequest>,
    ) -> Result<Response<Self::StreamLocalRuntimeStateStream>, Status> {
        let _ = request.into_inner();
        let service = self.clone();
        let (tx, rx) = mpsc::channel(Self::RUNTIME_STREAM_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            let mut last_state_without_time: Option<LocalRuntimeState> = None;
            let mut ticker =
                tokio::time::interval(Duration::from_millis(Self::RUNTIME_STREAM_POLL_INTERVAL_MS));

            loop {
                if tx.is_closed() {
                    break;
                }
                ticker.tick().await;

                let state = match service.build_runtime_state().await {
                    Ok(state) => state,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                };

                let mut comparable = state.clone();
                comparable.server_time_utc = None;
                let changed = last_state_without_time
                    .as_ref()
                    .map(|last| last != &comparable)
                    .unwrap_or(true);
                if !changed {
                    continue;
                }

                last_state_without_time = Some(comparable);
                if tx
                    .send(Ok(StreamLocalRuntimeStateResponse { state: Some(state) }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
