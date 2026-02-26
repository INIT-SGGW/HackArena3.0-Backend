//! gRPC SandboxAdminService scaffold.

use proto::race::v1::sandbox_admin_service_server::SandboxAdminService;
use proto::race::v1::{
    CreateSandboxConfigRequest, CreateSandboxConfigResponse, DeleteSandboxConfigRequest,
    DeleteSandboxConfigResponse, GetRuntimeStateRequest, GetRuntimeStateResponse,
    GetSandboxConfigsRequest, GetSandboxConfigsResponse, SetSandboxActivationRequest,
    SetSandboxActivationResponse, SetSandboxTimeOfDayRequest, SetSandboxTimeOfDayResponse,
    UpdateSandboxConfigRequest, UpdateSandboxConfigResponse,
};
use tonic::{Request, Response, Status};

/// Placeholder SandboxAdmin service implementation.
#[derive(Clone, Default)]
pub struct SandboxAdminServiceImpl;

#[tonic::async_trait]
impl SandboxAdminService for SandboxAdminServiceImpl {
    async fn get_sandbox_configs(
        &self,
        _request: Request<GetSandboxConfigsRequest>,
    ) -> Result<Response<GetSandboxConfigsResponse>, Status> {
        Err(Status::unimplemented(
            "sandbox admin service not implemented yet",
        ))
    }

    async fn create_sandbox_config(
        &self,
        _request: Request<CreateSandboxConfigRequest>,
    ) -> Result<Response<CreateSandboxConfigResponse>, Status> {
        Err(Status::unimplemented(
            "sandbox admin service not implemented yet",
        ))
    }

    async fn update_sandbox_config(
        &self,
        _request: Request<UpdateSandboxConfigRequest>,
    ) -> Result<Response<UpdateSandboxConfigResponse>, Status> {
        Err(Status::unimplemented(
            "sandbox admin service not implemented yet",
        ))
    }

    async fn delete_sandbox_config(
        &self,
        _request: Request<DeleteSandboxConfigRequest>,
    ) -> Result<Response<DeleteSandboxConfigResponse>, Status> {
        Err(Status::unimplemented(
            "sandbox admin service not implemented yet",
        ))
    }

    async fn set_sandbox_activation(
        &self,
        _request: Request<SetSandboxActivationRequest>,
    ) -> Result<Response<SetSandboxActivationResponse>, Status> {
        Err(Status::unimplemented(
            "sandbox admin service not implemented yet",
        ))
    }

    async fn set_sandbox_time_of_day(
        &self,
        _request: Request<SetSandboxTimeOfDayRequest>,
    ) -> Result<Response<SetSandboxTimeOfDayResponse>, Status> {
        Err(Status::unimplemented(
            "sandbox admin service not implemented yet",
        ))
    }

    async fn get_runtime_state(
        &self,
        _request: Request<GetRuntimeStateRequest>,
    ) -> Result<Response<GetRuntimeStateResponse>, Status> {
        Err(Status::unimplemented(
            "sandbox admin service not implemented yet",
        ))
    }
}
