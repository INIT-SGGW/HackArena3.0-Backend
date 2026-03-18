//! Internal build-service integration modules (official backend only).

mod client;
mod staging;

pub use client::{BuildGrpcClient, BuildGrpcClientError};
pub use staging::{FsUploadStager, StagedUpload, UploadStageInput, UploadStagingError};
