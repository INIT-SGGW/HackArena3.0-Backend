//! gRPC SandboxAdminService implementation.

use std::sync::Arc;

use proto::race::v1::sandbox_admin_service_server::SandboxAdminService;
use proto::race::v1::{
    CreateSandboxConfigRequest, CreateSandboxConfigResponse, DeleteSandboxConfigRequest,
    DeleteSandboxConfigResponse, GetRuntimeStateRequest, GetRuntimeStateResponse,
    GetSandboxConfigsRequest, GetSandboxConfigsResponse, RuntimeState, RuntimeTimeOfDayPreset,
    SetSandboxActivationRequest, SetSandboxActivationResponse, SetSandboxTimeOfDayRequest,
    SetSandboxTimeOfDayResponse, UpdateSandboxConfigRequest, UpdateSandboxConfigResponse,
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
use crate::services::sandbox_mappers::{
    default_engine_ghost_mode_settings, engine_ghost_mode_settings_from_record, find_sandbox_by_id,
    pending_sandbox_activation_to_proto, runtime_activity_kind_to_proto,
    runtime_time_of_day_preset_from_proto, runtime_time_of_day_preset_to_proto,
    sandbox_input_from_proto, sandbox_runtime_info_from_record, sandbox_to_proto,
    timestamp_to_unix_ms, utc_now_timestamp,
};

/// SandboxAdmin service backed by persisted sandbox config snapshot.
#[derive(Clone)]
pub struct SandboxAdminServiceImpl {
    repo: SandboxConfigRepo,
    token_validator: Arc<TokenValidator>,
    engine: EngineClient,
}

impl SandboxAdminServiceImpl {
    pub fn with_repo(
        repo: SandboxConfigRepo,
        token_validator: Arc<TokenValidator>,
        engine: EngineClient,
    ) -> Self {
        Self {
            repo,
            token_validator,
            engine,
        }
    }
}

#[tonic::async_trait]
impl SandboxAdminService for SandboxAdminServiceImpl {
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

        Ok(Response::new(DeleteSandboxConfigResponse {
            revision,
            sandbox_id: request.sandbox_id,
        }))
    }

    async fn set_sandbox_activation(
        &self,
        request: Request<SetSandboxActivationRequest>,
    ) -> Result<Response<SetSandboxActivationResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();

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
                if request.sandbox_id.trim().is_empty() {
                    return Err(Status::invalid_argument(
                        "sandbox_id must be non-empty when activate=true",
                    ));
                }
                let exists = self
                    .repo
                    .get_snapshot()
                    .await
                    .map_err(map_repo_error_to_status)?
                    .sandboxes
                    .into_iter()
                    .any(|entry| entry.sandbox_id == request.sandbox_id);
                if !exists {
                    return Err(Status::not_found(format!(
                        "sandbox config not found: {}",
                        request.sandbox_id
                    )));
                }
            } else if !request.sandbox_id.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "sandbox_id must be empty when activate=false",
                ));
            }

            let pending = EnginePendingSandboxActivation {
                activate: request.activate,
                sandbox_id: if request.activate {
                    request.sandbox_id.clone()
                } else {
                    String::new()
                },
                execute_at_unix_ms,
            };
            let runtime_after = self
                .engine
                .set_pending_sandbox_activation(request.expected_revision, Some(pending))
                .await
                .map_err(map_worker_err)?;

            return Ok(Response::new(SetSandboxActivationResponse {
                revision: runtime_after.revision,
                activate: request.activate,
                sandbox_id: if request.activate {
                    request.sandbox_id
                } else {
                    String::new()
                },
                effective_at_utc: runtime_after.pending_sandbox_activation.map(|entry| {
                    crate::services::sandbox_mappers::unix_ms_to_timestamp(entry.execute_at_unix_ms)
                }),
            }));
        }

        if request.activate {
            if request.sandbox_id.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "sandbox_id must be non-empty when activate=true",
                ));
            }

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

            return Ok(Response::new(SetSandboxActivationResponse {
                revision: runtime_after.revision,
                activate: true,
                sandbox_id: sandbox.sandbox_id,
                effective_at_utc: Some(utc_now_timestamp()),
            }));
        }

        let runtime_after = self
            .engine
            .switch_runtime(
                request.expected_revision,
                EngineActivityKind::None,
                current_runtime_map_id_for_deactivation(&self.engine).await?,
                None,
                None,
                Some(default_engine_ghost_mode_settings()),
            )
            .await
            .map_err(map_worker_err)?;

        Ok(Response::new(SetSandboxActivationResponse {
            revision: runtime_after.revision,
            activate: false,
            sandbox_id: String::new(),
            effective_at_utc: Some(utc_now_timestamp()),
        }))
    }

    async fn set_sandbox_time_of_day(
        &self,
        request: Request<SetSandboxTimeOfDayRequest>,
    ) -> Result<Response<SetSandboxTimeOfDayResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let request = request.into_inner();
        let preset = RuntimeTimeOfDayPreset::try_from(request.preset)
            .map_err(|_| Status::invalid_argument("invalid preset"))?;
        if matches!(preset, RuntimeTimeOfDayPreset::Unspecified) {
            return Err(Status::invalid_argument("preset must be specified"));
        }

        let runtime_before = self.engine.runtime_state().await.map_err(map_worker_err)?;
        if !matches!(runtime_before.activity_kind, EngineActivityKind::Sandbox) {
            return Err(Status::failed_precondition(
                "sandbox time-of-day override requires active sandbox runtime",
            ));
        }
        let active_sandbox_id = runtime_before.active_sandbox_id.clone().ok_or_else(|| {
            Status::failed_precondition("active sandbox runtime does not include active_sandbox_id")
        })?;

        let snapshot = self
            .repo
            .get_snapshot()
            .await
            .map_err(map_repo_error_to_status)?;
        let active_sandbox = find_sandbox_by_id(&snapshot.sandboxes, &active_sandbox_id)
            .ok_or_else(|| {
                Status::failed_precondition(
                    "active sandbox runtime does not match configured sandbox entry",
                )
            })?;

        if !request.sandbox_id.trim().is_empty() && request.sandbox_id != active_sandbox.sandbox_id
        {
            return Err(Status::failed_precondition(
                "requested sandbox_id is not the currently active sandbox",
            ));
        }

        let runtime_after = self
            .engine
            .set_runtime_time_of_day(
                request.expected_revision,
                runtime_time_of_day_preset_from_proto(preset),
            )
            .await
            .map_err(map_worker_err)?;

        Ok(Response::new(SetSandboxTimeOfDayResponse {
            revision: runtime_after.revision,
            sandbox_id: active_sandbox.sandbox_id,
            preset: runtime_time_of_day_preset_to_proto(runtime_after.time_of_day_preset) as i32,
            applied_at_utc: Some(utc_now_timestamp()),
        }))
    }

    async fn get_runtime_state(
        &self,
        request: Request<GetRuntimeStateRequest>,
    ) -> Result<Response<GetRuntimeStateResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;
        let snapshot = self
            .repo
            .get_snapshot()
            .await
            .map_err(map_repo_error_to_status)?;

        let active_sandbox = if matches!(runtime.activity_kind, EngineActivityKind::Sandbox) {
            runtime
                .active_sandbox_id
                .as_deref()
                .and_then(|sandbox_id| find_sandbox_by_id(&snapshot.sandboxes, sandbox_id))
        } else {
            None
        };

        let state = RuntimeState {
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
            pending_sandbox_activation: runtime
                .pending_sandbox_activation
                .map(pending_sandbox_activation_to_proto),
            server_time_utc: Some(utc_now_timestamp()),
        };
        Ok(Response::new(GetRuntimeStateResponse {
            state: Some(state),
        }))
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
            || runtime.active_sandbox_id.as_deref() != Some(sandbox_id)
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

async fn current_runtime_map_id_for_deactivation(engine: &EngineClient) -> Result<String, Status> {
    let runtime_before = engine.runtime_state().await.map_err(map_worker_err)?;
    Ok(runtime_before.map_id)
}
