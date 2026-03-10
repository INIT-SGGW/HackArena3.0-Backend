//! Local-only sandbox admin service implementation.

mod helpers;
mod mappers;
mod validation;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::local::sandbox_config_store::{
    LocalSandboxConfigRecord, LocalSandboxConfigStore, LocalTimeOfDaySettingsRecord,
    LocalWeatherSettingsRecord, local_sandbox_input_from_proto, local_sandbox_to_proto,
};
use crate::runtime::engine_worker::{EngineActivityKind, EngineClient};
use crate::services::error_map::map_worker_err;
use proto::race::v1::local_sandbox_admin_service_server::LocalSandboxAdminService;
use proto::race::v1::{
    CreateLocalSandboxConfigRequest, CreateLocalSandboxConfigResponse,
    DeleteLocalSandboxConfigRequest, DeleteLocalSandboxConfigResponse, GetLocalRuntimeStateRequest,
    GetLocalRuntimeStateResponse, GetLocalSandboxConfigsRequest, GetLocalSandboxConfigsResponse,
    LocalActiveSandboxRuntimeInfo, LocalRuntimeState, SetLocalSandboxActivationRequest,
    SetLocalSandboxActivationResponse, UpdateLocalSandboxConfigRequest,
    UpdateLocalSandboxConfigResponse, UpdateLocalSandboxSpawnModeRequest,
    UpdateLocalSandboxSpawnModeResponse, UpdateLocalSandboxTimeOfDayRequest,
    UpdateLocalSandboxTimeOfDayResponse, UpdateLocalSandboxWeatherRequest,
    UpdateLocalSandboxWeatherResponse,
};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use self::helpers::{find_local_sandbox_by_id, local_sandbox_id_v5, map_store_err};
use self::mappers::{
    engine_ghost_mode_settings_from_local_record, local_ghost_mode_to_proto,
    local_spawn_mode_from_proto_value, local_spawn_mode_to_proto, local_time_of_day_from_proto,
    local_time_of_day_to_proto, local_weather_from_proto, local_weather_to_proto,
    resolve_runtime_time_of_day_preset, runtime_time_of_day_preset_to_proto, utc_now_timestamp,
    weather_params_from_local,
};
use self::validation::{ensure_supported_spawn_mode, validate_map_id_track_exists};

#[derive(Clone)]
pub struct LocalSandboxAdminServiceImpl {
    store: LocalSandboxConfigStore,
    engine: EngineClient,
    max_active_sandboxes: u32,
    tracks_dir: PathBuf,
    started_at_utc: Arc<RwLock<HashMap<String, prost_types::Timestamp>>>,
}

impl LocalSandboxAdminServiceImpl {
    pub fn new(
        store: LocalSandboxConfigStore,
        engine: EngineClient,
        max_active_sandboxes: u32,
        tracks_dir: PathBuf,
    ) -> Self {
        Self {
            store,
            engine,
            max_active_sandboxes,
            tracks_dir,
            started_at_utc: Arc::new(RwLock::new(HashMap::new())),
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

    async fn apply_weather_if_active(&self, sandbox_id: &str, weather: LocalWeatherSettingsRecord) {
        let runtime_before = match self.engine.runtime_state().await {
            Ok(state) => state,
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "failed to read runtime state for local weather apply"
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

        let weather = weather_params_from_local(weather);
        if let Err(err) = self
            .engine
            .set_sandbox_weather(sandbox_id.to_string(), weather)
            .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %err,
                "failed to apply local sandbox weather to active runtime"
            );
        }
    }
}

#[tonic::async_trait]
impl LocalSandboxAdminService for LocalSandboxAdminServiceImpl {
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
        validate_map_id_track_exists(&self.tracks_dir, &sandbox.config.map_id).await?;
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
        validate_map_id_track_exists(&self.tracks_dir, &sandbox.config.map_id).await?;

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

        self.apply_weather_if_active(&request.sandbox_id, weather)
            .await;

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
        ensure_supported_spawn_mode(spawn_mode)?;

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
            validate_map_id_track_exists(&self.tracks_dir, &sandbox.config.map_id).await?;
            ensure_supported_spawn_mode(sandbox.config.spawn_mode)?;

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

            self.started_at_utc
                .write()
                .await
                .insert(sandbox.sandbox_id.clone(), utc_now_timestamp());

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
        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;
        let snapshot = self.store.get_snapshot().await;
        let started_at = self.started_at_utc.read().await.clone();

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
                active_player_count: 0,
            });
        }

        Ok(Response::new(GetLocalRuntimeStateResponse {
            state: Some(LocalRuntimeState {
                revision: runtime.revision,
                server_time_utc: Some(utc_now_timestamp()),
                active_sandboxes,
            }),
        }))
    }
}
