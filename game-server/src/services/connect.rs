//! Local-only gRPC ConnectService used for backend selection validation.

use bytes::Bytes;
use proto::hackarena::connect::v1::connect_service_server::ConnectService;
use proto::hackarena::connect::v1::{
    ConnectStatus, ValidateConnectionRequest, ValidateConnectionResponse,
};
use tonic::{Request, Response, Status, server::NamedService};

use crate::local::broker::BrokerRegistrationState;

const SUPPORTED_PROTOCOL_VERSION: &str = "1";

#[derive(Clone)]
pub struct ConnectServiceImpl {
    broker_registration_state: BrokerRegistrationState,
}

impl ConnectServiceImpl {
    pub fn new(broker_registration_state: BrokerRegistrationState) -> Self {
        Self {
            broker_registration_state,
        }
    }

    fn response(
        status: ConnectStatus,
        backend_id: String,
        message: impl Into<String>,
        nonce_echo: Bytes,
    ) -> Response<ValidateConnectionResponse> {
        Response::new(ValidateConnectionResponse {
            status: status as i32,
            backend_id,
            message: message.into(),
            nonce_echo,
        })
    }
}

impl NamedService for ConnectServiceImpl {
    const NAME: &'static str = "hackarena.connect.v1.ConnectService";
}

#[tonic::async_trait]
impl ConnectService for ConnectServiceImpl {
    async fn validate_connection(
        &self,
        request: Request<ValidateConnectionRequest>,
    ) -> Result<Response<ValidateConnectionResponse>, Status> {
        let req = request.into_inner();
        let registration = self.broker_registration_state.current().await;
        let current_backend_id = registration
            .as_ref()
            .map(|state| state.backend_id.clone())
            .unwrap_or_default();

        let requested_backend_id = req.backend_id.trim();
        if requested_backend_id.is_empty() {
            return Ok(Self::response(
                ConnectStatus::InvalidArgument,
                current_backend_id,
                "backend_id is required",
                req.nonce,
            ));
        }

        let protocol_version = req.protocol_version.trim();
        if protocol_version.is_empty() {
            return Ok(Self::response(
                ConnectStatus::InvalidArgument,
                current_backend_id,
                "protocol_version is required",
                req.nonce,
            ));
        }

        if protocol_version != SUPPORTED_PROTOCOL_VERSION {
            return Ok(Self::response(
                ConnectStatus::UnsupportedProtocol,
                current_backend_id,
                format!(
                    "unsupported protocol_version `{protocol_version}`; expected `{SUPPORTED_PROTOCOL_VERSION}`"
                ),
                req.nonce,
            ));
        }

        let Some(registered) = registration else {
            return Ok(Self::response(
                ConnectStatus::InternalError,
                String::new(),
                "broker registration state is not ready",
                req.nonce,
            ));
        };

        if requested_backend_id != registered.backend_id {
            return Ok(Self::response(
                ConnectStatus::BackendMismatch,
                registered.backend_id,
                "requested backend_id does not match current backend",
                req.nonce,
            ));
        }

        Ok(Self::response(
            ConnectStatus::Ok,
            registered.backend_id,
            "connection validated",
            req.nonce,
        ))
    }
}
