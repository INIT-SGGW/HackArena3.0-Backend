//! gRPC SandboxAdminService implementation.

use std::sync::Arc;

use boink::model::{
    GhostModeConditionLogic as EngineGhostModeConditionLogic,
    GhostModeSettings as EngineGhostModeSettings,
};
use proto::race::v1::sandbox_admin_service_server::SandboxAdminService;
use proto::race::v1::{
    CreateSandboxConfigRequest, CreateSandboxConfigResponse, DeleteSandboxConfigRequest,
    DeleteSandboxConfigResponse, GetRuntimeStateRequest, GetRuntimeStateResponse,
    GetSandboxConfigsRequest, GetSandboxConfigsResponse,
    GhostModeConditionLogic as ProtoGhostModeConditionLogic, RuntimeActivityKind, RuntimeState,
    SetSandboxActivationRequest, SetSandboxActivationResponse, SetSandboxTimeOfDayRequest,
    SetSandboxTimeOfDayResponse, UpdateSandboxConfigRequest, UpdateSandboxConfigResponse,
};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::auth::auth_claims::TokenValidator;
use crate::db::repos::sandbox_config::{
    GhostModeSettingsRecord, SandboxConfigInputRecord, SandboxConfigRecord, SandboxConfigRepo,
    SandboxConfigRepoError,
};
use crate::runtime::engine_worker::{EngineActivityKind, EngineClient};
use crate::services::error_map::map_worker_err;
use crate::services::sandbox_mappers::{
    sandbox_input_from_proto, sandbox_to_proto, utc_now_timestamp,
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

        if request.effective_at_utc.is_some() {
            return Err(Status::unimplemented(
                "scheduled sandbox activation is not implemented yet",
            ));
        }

        let runtime_before = self.engine.runtime_state().await.map_err(map_worker_err)?;
        if runtime_before.revision != request.expected_revision {
            return Err(Status::failed_precondition(format!(
                "runtime revision mismatch: expected {}, actual {}",
                request.expected_revision, runtime_before.revision
            )));
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
                .switch_runtime(EngineActivityKind::Sandbox, sandbox.config.map_id.clone())
                .await
                .map_err(map_worker_err)?;
            self.engine
                .set_ghost_mode_settings(engine_ghost_mode_settings_from_record(
                    sandbox.config.ghost_mode.as_ref(),
                ))
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
            .switch_runtime(EngineActivityKind::None, runtime_before.map_id)
            .await
            .map_err(map_worker_err)?;
        self.engine
            .set_ghost_mode_settings(default_engine_ghost_mode_settings())
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
        let _ = request;
        Err(Status::unimplemented(
            "sandbox time-of-day override flow not implemented yet",
        ))
    }

    async fn get_runtime_state(
        &self,
        request: Request<GetRuntimeStateRequest>,
    ) -> Result<Response<GetRuntimeStateResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        let runtime = self.engine.runtime_state().await.map_err(map_worker_err)?;

        let state = RuntimeState {
            revision: runtime.revision,
            activity_kind: runtime_activity_kind_to_proto(runtime.activity_kind) as i32,
            sandboxes: Vec::new(),
            pending_sandbox_activation: None,
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

fn runtime_activity_kind_to_proto(kind: EngineActivityKind) -> RuntimeActivityKind {
    match kind {
        EngineActivityKind::None => RuntimeActivityKind::None,
        EngineActivityKind::OfficialRace => RuntimeActivityKind::OfficialRace,
        EngineActivityKind::Sandbox => RuntimeActivityKind::Sandbox,
    }
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

fn default_engine_ghost_mode_settings() -> EngineGhostModeSettings {
    EngineGhostModeSettings {
        enabled: false,
        min_speed_enter_mps: 0.0,
        min_speed_exit_mps: 0.0,
        enter_delay_ms: 0,
        exit_delay_ms: 0,
        min_completed_laps: 0,
        condition_logic: EngineGhostModeConditionLogic::Unspecified,
        overlap_exit_delay_ms: 0,
    }
}

fn engine_ghost_mode_settings_from_record(
    record: Option<&GhostModeSettingsRecord>,
) -> EngineGhostModeSettings {
    let Some(record) = record else {
        return default_engine_ghost_mode_settings();
    };

    EngineGhostModeSettings {
        enabled: record.enabled,
        min_speed_enter_mps: record.min_speed_enter_mps,
        min_speed_exit_mps: record.min_speed_exit_mps,
        enter_delay_ms: record.enter_delay_ms,
        exit_delay_ms: record.exit_delay_ms,
        min_completed_laps: record.min_completed_laps,
        condition_logic: proto_condition_logic_to_engine(record.condition_logic),
        overlap_exit_delay_ms: record.overlap_exit_delay_ms,
    }
}

fn proto_condition_logic_to_engine(
    value: ProtoGhostModeConditionLogic,
) -> EngineGhostModeConditionLogic {
    match value {
        ProtoGhostModeConditionLogic::And => EngineGhostModeConditionLogic::And,
        ProtoGhostModeConditionLogic::Or => EngineGhostModeConditionLogic::Or,
        ProtoGhostModeConditionLogic::Unspecified => EngineGhostModeConditionLogic::Unspecified,
    }
}
