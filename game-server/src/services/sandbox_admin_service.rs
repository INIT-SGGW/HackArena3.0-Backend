//! gRPC sandbox admin services implementation.

use std::sync::Arc;

use proto::race::v1::runtime_admin_service_server::RuntimeAdminService;
use proto::race::v1::sandbox_config_admin_service_server::SandboxConfigAdminService;
use proto::race::v1::{
    AbortOfficialRaceRequest, AbortOfficialRaceResponse, AdminRuntimeState,
    AdminSandboxRuntimeMode, CancelSandboxActivationScheduleRequest,
    CancelSandboxActivationScheduleResponse, CloseOfficialRaceSessionRequest,
    CloseOfficialRaceSessionResponse, CreateSandboxConfigRequest, CreateSandboxConfigResponse,
    DeleteSandboxConfigRequest, DeleteSandboxConfigResponse, GetAdminRuntimeStateRequest,
    GetAdminRuntimeStateResponse, GetSandboxConfigsRequest, GetSandboxConfigsResponse,
    RuntimeTimeOfDayPreset, ScheduleSandboxActivationRequest, ScheduleSandboxActivationResponse,
    StartOfficialRaceCountdownRequest, StartOfficialRaceCountdownResponse,
    UpdateSandboxConfigRequest, UpdateSandboxConfigResponse, UpdateSandboxTimeOfDayRequest,
    UpdateSandboxTimeOfDayResponse, admin_runtime_state,
};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::auth::auth_claims::TokenValidator;
use crate::db::repos::sandbox_config::{
    SandboxConfigInputRecord, SandboxConfigRecord, SandboxConfigRepo, SandboxConfigRepoError,
};
use crate::runtime::engine_worker::{
    EngineActivityKind, EngineClient, EnginePendingSandboxActivation,
};
use crate::services::error_map::map_worker_err;
use crate::services::public_menu_service::SandboxConfigCacheInvalidation;
use crate::services::sandbox_mappers::{
    admin_sandbox_runtime_info_from_record, engine_ghost_mode_settings_from_record,
    find_sandbox_by_id, pending_sandbox_operation_to_proto, runtime_time_of_day_preset_from_proto,
    runtime_time_of_day_preset_to_proto, sandbox_input_from_proto, sandbox_to_proto,
    timestamp_to_unix_ms, unix_ms_to_timestamp, utc_now_timestamp,
};

/// Sandbox admin services backed by persisted sandbox config snapshot.
#[derive(Clone)]
pub struct SandboxAdminServiceImpl {
    repo: SandboxConfigRepo,
    token_validator: Arc<TokenValidator>,
    engine: EngineClient,
    sandbox_config_invalidation: SandboxConfigCacheInvalidation,
}

impl SandboxAdminServiceImpl {
    pub(crate) fn with_repo(
        repo: SandboxConfigRepo,
        token_validator: Arc<TokenValidator>,
        engine: EngineClient,
        sandbox_config_invalidation: SandboxConfigCacheInvalidation,
    ) -> Self {
        Self {
            repo,
            token_validator,
            engine,
            sandbox_config_invalidation,
        }
    }
}

#[tonic::async_trait]
impl SandboxConfigAdminService for SandboxAdminServiceImpl {
    async fn get_sandbox_configs(
        &self,
        request: Request<GetSandboxConfigsRequest>,
    ) -> Result<Response<GetSandboxConfigsResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let snapshot = self
            .repo
            .get_snapshot()
            .await
            .map_err(map_repo_error_to_status)?;

        Ok(Response::new(GetSandboxConfigsResponse {
            revision: snapshot.revision,
            sandboxes: snapshot
                .sandboxes
                .into_iter()
                .map(sandbox_to_proto)
                .collect(),
        }))
    }

    async fn create_sandbox_config(
        &self,
        request: Request<CreateSandboxConfigRequest>,
    ) -> Result<Response<CreateSandboxConfigResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        let config = request
            .config
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("config is required"))
            .and_then(sandbox_input_from_proto)?;

        let sandbox_id = sandbox_id_v5(&config, request.expected_revision);
        let sandbox = SandboxConfigRecord { sandbox_id, config };

        let revision = self
            .repo
            .create_config(request.expected_revision, &sandbox)
            .await
            .map_err(map_repo_error_to_status)?;
        self.sandbox_config_invalidation.invalidate();

        Ok(Response::new(CreateSandboxConfigResponse {
            revision,
            sandbox: Some(sandbox_to_proto(sandbox)),
        }))
    }

    async fn update_sandbox_config(
        &self,
        request: Request<UpdateSandboxConfigRequest>,
    ) -> Result<Response<UpdateSandboxConfigResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id must be non-empty"));
        }
        self.require_sandbox_not_active(&request.sandbox_id).await?;
        let config = request
            .config
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("config is required"))
            .and_then(sandbox_input_from_proto)?;

        let sandbox = SandboxConfigRecord {
            sandbox_id: request.sandbox_id,
            config,
        };
        let revision = self
            .repo
            .update_config(request.expected_revision, &sandbox)
            .await
            .map_err(map_repo_error_to_status)?;
        self.sandbox_config_invalidation.invalidate();

        Ok(Response::new(UpdateSandboxConfigResponse {
            revision,
            sandbox: Some(sandbox_to_proto(sandbox)),
        }))
    }

    async fn delete_sandbox_config(
        &self,
        request: Request<DeleteSandboxConfigRequest>,
    ) -> Result<Response<DeleteSandboxConfigResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id must be non-empty"));
        }
        self.require_sandbox_not_active(&request.sandbox_id).await?;

        let revision = self
            .repo
            .delete_config(request.expected_revision, &request.sandbox_id)
            .await
            .map_err(map_repo_error_to_status)?;
        self.sandbox_config_invalidation.invalidate();

        Ok(Response::new(DeleteSandboxConfigResponse {
            revision,
            sandbox_id: request.sandbox_id,
        }))
    }

    async fn update_sandbox_time_of_day(
        &self,
        request: Request<UpdateSandboxTimeOfDayRequest>,
    ) -> Result<Response<UpdateSandboxTimeOfDayResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id must be non-empty"));
        }
        let preset = RuntimeTimeOfDayPreset::try_from(request.preset)
            .map_err(|_| Status::invalid_argument("invalid preset"))?;
        if matches!(preset, RuntimeTimeOfDayPreset::Unspecified) {
            return Err(Status::invalid_argument("preset must be specified"));
        }

        let snapshot = self
            .repo
            .get_snapshot()
            .await
            .map_err(map_repo_error_to_status)?;
        let sandbox =
            find_sandbox_by_id(&snapshot.sandboxes, &request.sandbox_id).ok_or_else(|| {
                Status::not_found(format!("sandbox config not found: {}", request.sandbox_id))
            })?;
        let updated = SandboxConfigRecord {
            sandbox_id: sandbox.sandbox_id.clone(),
            config: SandboxConfigInputRecord {
                sandbox_name: sandbox.config.sandbox_name,
                map_id: sandbox.config.map_id,
                time_of_day_preset: preset,
                ghost_mode: sandbox.config.ghost_mode,
            },
        };
        let revision = self
            .repo
            .update_config(request.expected_revision, &updated)
            .await
            .map_err(map_repo_error_to_status)?;
        self.sandbox_config_invalidation.invalidate();

        let mut applied_to_active_runtime = false;
        match self.engine.runtime_state().await {
            Ok(runtime_before) => {
                if matches!(runtime_before.activity_kind, EngineActivityKind::Sandbox)
                    && runtime_before
                        .active_sandboxes
                        .iter()
                        .any(|entry| entry.sandbox_id == request.sandbox_id)
                {
                    match self
                        .engine
                        .set_runtime_time_of_day(
                            runtime_before.revision,
                            Some(request.sandbox_id.clone()),
                            runtime_time_of_day_preset_from_proto(preset),
                        )
                        .await
                    {
                        Ok(_) => {
                            applied_to_active_runtime = true;
                        }
                        Err(err) => {
                            tracing::warn!(
                                sandbox_id = %request.sandbox_id,
                                error = %err,
                                "failed to apply sandbox time-of-day to active runtime"
                            );
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %request.sandbox_id,
                    error = %err,
                    "failed to read runtime state for sandbox time-of-day apply"
                );
            }
        }

        Ok(Response::new(UpdateSandboxTimeOfDayResponse {
            revision,
            sandbox_id: updated.sandbox_id,
            preset: preset as i32,
            applied_at_utc: Some(utc_now_timestamp()),
            applied_to_active_runtime,
        }))
    }
}

#[tonic::async_trait]
impl RuntimeAdminService for SandboxAdminServiceImpl {
    async fn schedule_sandbox_activation(
        &self,
        request: Request<ScheduleSandboxActivationRequest>,
    ) -> Result<Response<ScheduleSandboxActivationResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id must be non-empty"));
        }

        if let Some(effective_at_utc) = request.effective_at_utc.as_ref() {
            let execute_at_unix_ms = timestamp_to_unix_ms(effective_at_utc)?;
            let now = utc_now_timestamp();
            let now_unix_ms = timestamp_to_unix_ms(&now)?;
            if execute_at_unix_ms <= now_unix_ms {
                return Err(Status::invalid_argument(
                    "effective_at_utc must be in the future",
                ));
            }

            if request.activate {
                let sandbox = self
                    .repo
                    .get_snapshot()
                    .await
                    .map_err(map_repo_error_to_status)?
                    .sandboxes
                    .into_iter()
                    .find(|entry| entry.sandbox_id == request.sandbox_id)
                    .ok_or_else(|| {
                        Status::not_found(format!(
                            "sandbox config not found: {}",
                            request.sandbox_id
                        ))
                    })?;
                let pending = EnginePendingSandboxActivation {
                    activate: true,
                    sandbox_id: request.sandbox_id.clone(),
                    execute_at_unix_ms,
                    map_id: Some(sandbox.config.map_id),
                    time_of_day_preset: Some(runtime_time_of_day_preset_from_proto(
                        sandbox.config.time_of_day_preset,
                    )),
                    ghost_mode_settings: Some(engine_ghost_mode_settings_from_record(
                        sandbox.config.ghost_mode.as_ref(),
                    )),
                };
                let runtime_after = self
                    .engine
                    .set_pending_sandbox_activation(request.expected_revision, pending)
                    .await
                    .map_err(map_worker_err)?;

                return Ok(Response::new(ScheduleSandboxActivationResponse {
                    revision: runtime_after.revision,
                    activate: true,
                    sandbox_id: request.sandbox_id,
                    effective_at_utc: Some(unix_ms_to_timestamp(execute_at_unix_ms)),
                }));
            }

            let pending = EnginePendingSandboxActivation {
                activate: false,
                sandbox_id: request.sandbox_id.clone(),
                execute_at_unix_ms,
                map_id: None,
                time_of_day_preset: None,
                ghost_mode_settings: None,
            };
            let runtime_after = self
                .engine
                .set_pending_sandbox_activation(request.expected_revision, pending)
                .await
                .map_err(map_worker_err)?;

            return Ok(Response::new(ScheduleSandboxActivationResponse {
                revision: runtime_after.revision,
                activate: false,
                sandbox_id: request.sandbox_id,
                effective_at_utc: Some(unix_ms_to_timestamp(execute_at_unix_ms)),
            }));
        }

        if request.activate {
            let sandbox = self
                .repo
                .get_snapshot()
                .await
                .map_err(map_repo_error_to_status)?
                .sandboxes
                .into_iter()
                .find(|entry| entry.sandbox_id == request.sandbox_id)
                .ok_or_else(|| {
                    Status::not_found(format!("sandbox config not found: {}", request.sandbox_id))
                })?;

            let runtime_after = self
                .engine
                .switch_runtime(
                    request.expected_revision,
                    EngineActivityKind::Sandbox,
                    sandbox.config.map_id.clone(),
                    Some(sandbox.sandbox_id.clone()),
                    Some(runtime_time_of_day_preset_from_proto(
                        sandbox.config.time_of_day_preset,
                    )),
                    Some(engine_ghost_mode_settings_from_record(
                        sandbox.config.ghost_mode.as_ref(),
                    )),
                )
                .await
                .map_err(map_worker_err)?;

            return Ok(Response::new(ScheduleSandboxActivationResponse {
                revision: runtime_after.revision,
                activate: true,
                sandbox_id: sandbox.sandbox_id,
                effective_at_utc: Some(utc_now_timestamp()),
            }));
        }

        let runtime_after = self
            .engine
            .deactivate_sandbox(request.expected_revision, request.sandbox_id.clone())
            .await
            .map_err(map_worker_err)?;

        Ok(Response::new(ScheduleSandboxActivationResponse {
            revision: runtime_after.revision,
            activate: false,
            sandbox_id: request.sandbox_id,
            effective_at_utc: Some(utc_now_timestamp()),
        }))
    }

    async fn cancel_sandbox_activation_schedule(
        &self,
        request: Request<CancelSandboxActivationScheduleRequest>,
    ) -> Result<Response<CancelSandboxActivationScheduleResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id must be non-empty"));
        }

        let (runtime_after, canceled) = self
            .engine
            .cancel_pending_sandbox_activation(
                request.expected_revision,
                request.sandbox_id.clone(),
            )
            .await
            .map_err(map_worker_err)?;

        Ok(Response::new(CancelSandboxActivationScheduleResponse {
            revision: runtime_after.revision,
            sandbox_id: request.sandbox_id,
            canceled,
        }))
    }

    async fn get_admin_runtime_state(
        &self,
        request: Request<GetAdminRuntimeStateRequest>,
    ) -> Result<Response<GetAdminRuntimeStateResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;
        let snapshot = self
            .repo
            .get_snapshot()
            .await
            .map_err(map_repo_error_to_status)?;

        let state = AdminRuntimeState {
            revision: runtime.revision,
            server_time_utc: Some(utc_now_timestamp()),
            active_mode: match runtime.activity_kind {
                EngineActivityKind::Sandbox => Some(admin_runtime_state::ActiveMode::SandboxMode(
                    AdminSandboxRuntimeMode {
                        sandboxes: runtime
                            .active_sandboxes
                            .iter()
                            .filter_map(|active| {
                                find_sandbox_by_id(&snapshot.sandboxes, &active.sandbox_id).map(
                                    |record| {
                                        admin_sandbox_runtime_info_from_record(
                                            record,
                                            runtime_time_of_day_preset_to_proto(
                                                active.time_of_day_preset,
                                            ),
                                        )
                                    },
                                )
                            })
                            .collect(),
                        pending_sandbox_operations: runtime
                            .pending_sandbox_activations
                            .iter()
                            .cloned()
                            .map(pending_sandbox_operation_to_proto)
                            .collect(),
                    },
                )),
                EngineActivityKind::None | EngineActivityKind::OfficialRace => None,
            },
        };
        Ok(Response::new(GetAdminRuntimeStateResponse {
            state: Some(state),
        }))
    }

    async fn start_official_race_countdown(
        &self,
        request: Request<StartOfficialRaceCountdownRequest>,
    ) -> Result<Response<StartOfficialRaceCountdownResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        Err(Status::unimplemented(
            "official race countdown controls are not implemented yet",
        ))
    }

    async fn abort_official_race(
        &self,
        request: Request<AbortOfficialRaceRequest>,
    ) -> Result<Response<AbortOfficialRaceResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        Err(Status::unimplemented(
            "official race abort controls are not implemented yet",
        ))
    }

    async fn close_official_race_session(
        &self,
        request: Request<CloseOfficialRaceSessionRequest>,
    ) -> Result<Response<CloseOfficialRaceSessionResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        Err(Status::unimplemented(
            "official race close controls are not implemented yet",
        ))
    }
}

impl SandboxAdminServiceImpl {
    async fn require_admin(&self, metadata: &MetadataMap) -> Result<(), Status> {
        let is_admin = self.token_validator.is_admin(metadata).await?;
        if !is_admin {
            return Err(Status::permission_denied("admin role required"));
        }
        Ok(())
    }

    async fn require_sandbox_not_active(&self, sandbox_id: &str) -> Result<(), Status> {
        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;
        if !matches!(runtime.activity_kind, EngineActivityKind::Sandbox)
            || !runtime
                .active_sandboxes
                .iter()
                .any(|entry| entry.sandbox_id == sandbox_id)
        {
            return Ok(());
        }

        Err(Status::failed_precondition(
            "active sandbox cannot be modified",
        ))
    }
}

fn sandbox_id_v5(config: &SandboxConfigInputRecord, expected_revision: u64) -> String {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    let payload = format!(
        "expected_revision={};sandbox_name={};map_id={};time_of_day_preset={};ts_ns={}",
        expected_revision,
        config.sandbox_name,
        config.map_id,
        config.time_of_day_preset as i32,
        duration.as_nanos(),
    );
    Uuid::new_v5(&Uuid::NAMESPACE_OID, payload.as_bytes()).to_string()
}

fn map_repo_error_to_status(err: SandboxConfigRepoError) -> Status {
    match err {
        SandboxConfigRepoError::RevisionMismatch { .. } => {
            Status::failed_precondition(err.to_string())
        }
        SandboxConfigRepoError::AlreadyExists { .. } => Status::already_exists(err.to_string()),
        SandboxConfigRepoError::NotFound { .. } => Status::not_found(err.to_string()),
        SandboxConfigRepoError::InvalidTimeOfDayPreset
        | SandboxConfigRepoError::InvalidGhostConditionLogic => {
            Status::invalid_argument(err.to_string())
        }
        SandboxConfigRepoError::Sqlx(_)
        | SandboxConfigRepoError::StateMissing
        | SandboxConfigRepoError::PartialGhostData { .. }
        | SandboxConfigRepoError::NumericOutOfRange { .. }
        | SandboxConfigRepoError::RevisionOverflow => Status::internal(err.to_string()),
    }
}
