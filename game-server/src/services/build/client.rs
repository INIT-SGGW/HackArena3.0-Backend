use std::time::Duration;

use proto::hackarena::build::v1::build_service_client::BuildServiceClient;
use proto::hackarena::build::v1::{
    CancelBuildRequest, CancelBuildResponse, GetBuildRequest, GetBuildResponse, ListBuildsRequest,
    ListBuildsResponse, SubmitBuildRequest, SubmitBuildResponse,
};
use thiserror::Error;
use tonic::transport::{Channel, Endpoint};

use crate::config::Config;

const CONNECT_TIMEOUT_MS: u64 = 2_000;

/// Thin gRPC client wrapper for `hackarena.build.v1.BuildService`.
#[derive(Clone)]
pub struct BuildGrpcClient {
    channel: Channel,
    submit_timeout: Duration,
    get_timeout: Duration,
    list_timeout: Duration,
    cancel_timeout: Duration,
}

impl BuildGrpcClient {
    /// Creates a client from server config.
    pub fn from_config(cfg: &Config) -> Result<Self, BuildGrpcClientError> {
        let endpoint_raw = cfg.build_service_grpc_endpoint.clone();
        let endpoint = Endpoint::from_shared(endpoint_raw.clone()).map_err(|source| {
            BuildGrpcClientError::InvalidEndpoint {
                endpoint: endpoint_raw.clone(),
                source,
            }
        })?;
        let channel = endpoint
            .connect_timeout(Duration::from_millis(CONNECT_TIMEOUT_MS))
            .connect_lazy();

        Ok(Self {
            channel,
            submit_timeout: Duration::from_millis(cfg.build_service_submit_timeout_ms),
            get_timeout: Duration::from_millis(cfg.build_service_get_timeout_ms),
            list_timeout: Duration::from_millis(cfg.build_service_list_timeout_ms),
            cancel_timeout: Duration::from_millis(cfg.build_service_cancel_timeout_ms),
        })
    }

    /// Calls `SubmitBuild`.
    pub async fn submit_build(
        &self,
        request: SubmitBuildRequest,
    ) -> Result<SubmitBuildResponse, BuildGrpcClientError> {
        let mut client = BuildServiceClient::new(self.channel.clone());
        let operation = "SubmitBuild";
        let timeout = self.submit_timeout;
        tokio::time::timeout(timeout, client.submit_build(request))
            .await
            .map_err(|_| BuildGrpcClientError::Timeout {
                operation,
                timeout_ms: duration_to_ms(timeout),
            })?
            .map(|response| response.into_inner())
            .map_err(|status| BuildGrpcClientError::GrpcStatus { operation, status })
    }

    /// Calls `GetBuild`.
    pub async fn get_build(
        &self,
        request: GetBuildRequest,
    ) -> Result<GetBuildResponse, BuildGrpcClientError> {
        let mut client = BuildServiceClient::new(self.channel.clone());
        let operation = "GetBuild";
        let timeout = self.get_timeout;
        tokio::time::timeout(timeout, client.get_build(request))
            .await
            .map_err(|_| BuildGrpcClientError::Timeout {
                operation,
                timeout_ms: duration_to_ms(timeout),
            })?
            .map(|response| response.into_inner())
            .map_err(|status| BuildGrpcClientError::GrpcStatus { operation, status })
    }

    /// Calls `ListBuilds`.
    pub async fn list_builds(
        &self,
        request: ListBuildsRequest,
    ) -> Result<ListBuildsResponse, BuildGrpcClientError> {
        let mut client = BuildServiceClient::new(self.channel.clone());
        let operation = "ListBuilds";
        let timeout = self.list_timeout;
        tokio::time::timeout(timeout, client.list_builds(request))
            .await
            .map_err(|_| BuildGrpcClientError::Timeout {
                operation,
                timeout_ms: duration_to_ms(timeout),
            })?
            .map(|response| response.into_inner())
            .map_err(|status| BuildGrpcClientError::GrpcStatus { operation, status })
    }

    /// Calls `CancelBuild`.
    pub async fn cancel_build(
        &self,
        request: CancelBuildRequest,
    ) -> Result<CancelBuildResponse, BuildGrpcClientError> {
        let mut client = BuildServiceClient::new(self.channel.clone());
        let operation = "CancelBuild";
        let timeout = self.cancel_timeout;
        tokio::time::timeout(timeout, client.cancel_build(request))
            .await
            .map_err(|_| BuildGrpcClientError::Timeout {
                operation,
                timeout_ms: duration_to_ms(timeout),
            })?
            .map(|response| response.into_inner())
            .map_err(|status| BuildGrpcClientError::GrpcStatus { operation, status })
    }
}

#[derive(Debug, Error)]
pub enum BuildGrpcClientError {
    #[error("invalid BUILD_SERVICE_GRPC_ENDPOINT `{endpoint}`: {source}")]
    InvalidEndpoint {
        endpoint: String,
        source: tonic::transport::Error,
    },
    #[error("{operation} timed out after {timeout_ms} ms")]
    Timeout {
        operation: &'static str,
        timeout_ms: u64,
    },
    #[error("{operation} failed: {status}")]
    GrpcStatus {
        operation: &'static str,
        status: tonic::Status,
    },
}

fn duration_to_ms(duration: Duration) -> u64 {
    duration
        .as_millis()
        .min(u128::from(u64::MAX))
        .try_into()
        .unwrap_or(u64::MAX)
}
