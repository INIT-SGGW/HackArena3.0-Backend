//! gRPC SandboxAdminService implementation.

use std::sync::Arc;

use proto::race::v1::sandbox_admin_service_server::SandboxAdminService;
use proto::race::v1::{
    CreateSandboxConfigRequest, CreateSandboxConfigResponse, DeleteSandboxConfigRequest,
    DeleteSandboxConfigResponse, GetRuntimeStateRequest, GetRuntimeStateResponse,
    GetSandboxConfigsRequest, GetSandboxConfigsResponse, RuntimeActivityKind, RuntimeState,
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
        let _ = request;
        Err(Status::unimplemented(
            "sandbox activation flow not implemented yet",
        ))
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
