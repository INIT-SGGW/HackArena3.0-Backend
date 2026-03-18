//! Internal build-service integration modules (official backend only).

mod client;
mod staging;
mod submission;
mod team_resolver;

pub use client::{BuildGrpcClient, BuildGrpcClientError};
pub use staging::{FsUploadStager, StagedUpload, UploadStageInput, UploadStagingError};
pub use submission::{
    BuildSubmissionService, BuildSubmissionServiceError, CancelSubmissionInput,
    SubmitBuildUploadInput, SubmitBuildUploadResult,
};
pub use team_resolver::{BuildTeamResolver, BuildTeamResolverError, ResolvedTeam};
