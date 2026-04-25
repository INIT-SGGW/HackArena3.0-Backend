//! Local-only gRPC ConnectService used for backend selection validation.

use bytes::Bytes;
use proto::hackarena::connect::v1::connect_service_server::ConnectService;
use proto::hackarena::connect::v1::{
    ConnectStatus, ValidateConnectionRequest, ValidateConnectionResponse,
};
use tonic::{Request, Response, Status, server::NamedService};

#[cfg(all(feature = "local", not(feature = "standalone")))]
use crate::local::broker::BrokerRegistrationState;

const SUPPORTED_PROTOCOL_VERSION: &str = "1";
#[cfg(all(feature = "local", feature = "standalone"))]
const STANDALONE_BACKEND_ID: &str = "standalone";

#[derive(Clone)]
pub struct ConnectServiceImpl {
    #[cfg(all(feature = "local", not(feature = "standalone")))]
    broker_registration_state: BrokerRegistrationState,
    #[cfg(all(feature = "local", feature = "standalone"))]
    backend_id: String,
}

impl ConnectServiceImpl {
    #[cfg(all(feature = "local", not(feature = "standalone")))]
    pub fn new(broker_registration_state: BrokerRegistrationState) -> Self {
        Self {
            broker_registration_state,
        }
    }

    #[cfg(all(feature = "local", feature = "standalone"))]
    pub fn new_standalone() -> Self {
        Self {
            backend_id: STANDALONE_BACKEND_ID.to_string(),
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
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let registration = self.broker_registration_state.current().await;
        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let current_backend_id = registration
            .as_ref()
            .map(|state| state.backend_id.clone())
            .unwrap_or_default();

        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let requested_backend_id = req.backend_id.trim();
        #[cfg(all(feature = "local", not(feature = "standalone")))]
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
                #[cfg(all(feature = "local", not(feature = "standalone")))]
                current_backend_id,
                #[cfg(all(feature = "local", feature = "standalone"))]
                self.backend_id.clone(),
                "protocol_version is required",
                req.nonce,
            ));
        }

        if protocol_version != SUPPORTED_PROTOCOL_VERSION {
            return Ok(Self::response(
                ConnectStatus::UnsupportedProtocol,
                #[cfg(all(feature = "local", not(feature = "standalone")))]
                current_backend_id,
                #[cfg(all(feature = "local", feature = "standalone"))]
                self.backend_id.clone(),
                format!(
                    "unsupported protocol_version `{protocol_version}`; expected `{SUPPORTED_PROTOCOL_VERSION}`"
                ),
                req.nonce,
            ));
        }

        #[cfg(all(feature = "local", feature = "standalone"))]
        {
            let backend_id = req.backend_id.trim().to_string();
            let backend_id = if backend_id.is_empty() {
                self.backend_id.clone()
            } else {
                backend_id
            };

            return Ok(Self::response(
                ConnectStatus::Ok,
                backend_id,
                "connection validated (standalone mode)",
                req.nonce,
            ));
        }

        #[cfg(all(feature = "local", not(feature = "standalone")))]
        let Some(registered) = registration else {
            return Ok(Self::response(
                ConnectStatus::InternalError,
                String::new(),
                "broker registration state is not ready",
                req.nonce,
            ));
        };

        #[cfg(all(feature = "local", not(feature = "standalone")))]
        if requested_backend_id != registered.backend_id {
            return Ok(Self::response(
                ConnectStatus::BackendMismatch,
                registered.backend_id,
                "requested backend_id does not match current backend",
                req.nonce,
            ));
        }

        #[cfg(all(feature = "local", not(feature = "standalone")))]
        Ok(Self::response(
            ConnectStatus::Ok,
            registered.backend_id,
            "connection validated",
            req.nonce,
        ))
    }
}
