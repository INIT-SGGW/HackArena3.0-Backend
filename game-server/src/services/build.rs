//! Internal build-service integration modules (official backend only).

mod client;
mod staging;
mod submission;

pub use client::{BuildGrpcClient, BuildGrpcClientError};
pub use staging::{FsUploadStager, StagedUpload, UploadStageInput, UploadStagingError};
pub use submission::{
    BuildSubmissionService, BuildSubmissionServiceError, CancelSubmissionInput,
    SubmitBuildUploadInput, SubmitBuildUploadResult,
};
