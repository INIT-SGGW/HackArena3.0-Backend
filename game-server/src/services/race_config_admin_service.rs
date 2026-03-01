//! gRPC RaceConfigAdminService implementation.

#[cfg(feature = "official")]
use std::sync::Arc;

use proto::race::v1::race_config_admin_service_server::RaceConfigAdminService;
use proto::race::v1::{
    CreateRaceConfigRequest, CreateRaceConfigResponse, DeleteRaceConfigRequest,
    DeleteRaceConfigResponse, GetRaceConfigsRequest, GetRaceConfigsResponse,
    UpdateRaceConfigRequest, UpdateRaceConfigResponse,
};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

#[cfg(feature = "official")]
use crate::auth::auth_claims::TokenValidator;
#[cfg(feature = "official")]
use crate::db::repos::race_config::RaceConfigRepo;

/// RaceConfigAdmin service.
#[derive(Clone)]
pub struct RaceConfigAdminServiceImpl {
    #[cfg(feature = "official")]
    repo: Option<RaceConfigRepo>,
    #[cfg(feature = "official")]
    token_validator: Option<Arc<TokenValidator>>,
}

impl RaceConfigAdminServiceImpl {
    #[cfg(feature = "official")]
    pub fn with_repo(repo: RaceConfigRepo, token_validator: Arc<TokenValidator>) -> Self {
        Self {
            repo: Some(repo),
            token_validator: Some(token_validator),
        }
    }
}

impl Default for RaceConfigAdminServiceImpl {
    fn default() -> Self {
        Self {
            #[cfg(feature = "official")]
            repo: None,
            #[cfg(feature = "official")]
            token_validator: None,
        }
    }
}

#[tonic::async_trait]
impl RaceConfigAdminService for RaceConfigAdminServiceImpl {
    async fn get_race_configs(
        &self,
        request: Request<GetRaceConfigsRequest>,
    ) -> Result<Response<GetRaceConfigsResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        Err(Status::unimplemented(
            "race config CRUD migration is not implemented yet",
        ))
    }

    async fn create_race_config(
        &self,
        request: Request<CreateRaceConfigRequest>,
    ) -> Result<Response<CreateRaceConfigResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        Err(Status::unimplemented(
            "race config CRUD migration is not implemented yet",
        ))
    }

    async fn update_race_config(
        &self,
        request: Request<UpdateRaceConfigRequest>,
    ) -> Result<Response<UpdateRaceConfigResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        Err(Status::unimplemented(
            "race config CRUD migration is not implemented yet",
        ))
    }

    async fn delete_race_config(
        &self,
        request: Request<DeleteRaceConfigRequest>,
    ) -> Result<Response<DeleteRaceConfigResponse>, Status> {
        self.require_admin(request.metadata()).await?;
        Err(Status::unimplemented(
            "race config CRUD migration is not implemented yet",
        ))
    }
}

impl RaceConfigAdminServiceImpl {
    async fn require_admin(&self, metadata: &MetadataMap) -> Result<(), Status> {
        #[cfg(feature = "official")]
        {
            let _ = self.repo.as_ref().ok_or_else(|| {
                Status::failed_precondition("race config admin service is not configured")
            })?;
            let validator = self.token_validator.as_ref().ok_or_else(|| {
                Status::failed_precondition("race config admin service auth is not configured")
            })?;
            let is_admin = validator.is_admin(metadata).await?;
            if !is_admin {
                return Err(Status::permission_denied("admin role required"));
            }
            return Ok(());
        }

        #[cfg(not(feature = "official"))]
        {
            let _ = metadata;
            Err(Status::unimplemented(
                "race config admin service is available only in official backend",
            ))
        }
    }
}
