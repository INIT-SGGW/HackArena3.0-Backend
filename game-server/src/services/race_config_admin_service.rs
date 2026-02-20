//! gRPC RaceConfigAdminService scaffold.

use proto::race::v1::race_config_admin_service_server::RaceConfigAdminService;
use proto::race::v1::{
    GetRaceConfigScheduleRequest, GetRaceConfigScheduleResponse, ReplaceRaceConfigScheduleRequest,
    ReplaceRaceConfigScheduleResponse,
};
use tonic::{Request, Response, Status};

/// Placeholder RaceConfigAdmin service implementation.
#[derive(Clone, Default)]
pub struct RaceConfigAdminServiceImpl;

#[tonic::async_trait]
impl RaceConfigAdminService for RaceConfigAdminServiceImpl {
    async fn get_race_config_schedule(
        &self,
        _request: Request<GetRaceConfigScheduleRequest>,
    ) -> Result<Response<GetRaceConfigScheduleResponse>, Status> {
        Err(Status::unimplemented(
            "race config admin service not implemented yet",
        ))
    }

    async fn replace_race_config_schedule(
        &self,
        _request: Request<ReplaceRaceConfigScheduleRequest>,
    ) -> Result<Response<ReplaceRaceConfigScheduleResponse>, Status> {
        Err(Status::unimplemented(
            "race config admin service not implemented yet",
        ))
    }
}
