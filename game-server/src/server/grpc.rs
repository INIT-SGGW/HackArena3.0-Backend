//! gRPC server wiring and middleware layers.

use proto::race::v1::asset_service_server::AssetServiceServer;
use proto::race::v1::race_service_server::RaceServiceServer;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tonic_health::server::health_reporter;
use tonic_web::GrpcWebLayer;
use tower_http::trace::TraceLayer;

use crate::config::Config;
use crate::runtime::engine_worker::EngineClient;
use crate::services::asset_service::AssetServiceImpl;
use crate::services::race_service::RaceServiceImpl;

use super::cors::cors_layer;
use super::shutdown::shutdown_signal;

/// Runs the gRPC server (with gRPC-web and CORS) until shutdown is requested.
pub async fn serve_grpc(
    cfg: Arc<Config>,
    engine: EngineClient,
    mut shutdown_rx: broadcast::Receiver<()>,
    active_connections: Arc<AtomicUsize>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (health_reporter, health_service) = health_reporter();

    health_reporter
        .set_serving::<AssetServiceServer<AssetServiceImpl>>()
        .await;
    health_reporter
        .set_serving::<RaceServiceServer<RaceServiceImpl>>()
        .await;

    let asset_impl = AssetServiceImpl::new(cfg.tracks_dir.clone());
    let race_impl = RaceServiceImpl::new(
        engine,
        cfg.simulation_hz,
        &cfg.jwks_url,
        cfg.jwt_audience.clone(),
        cfg.jwt_issuers.clone(),
    );

    let cors = cors_layer(&cfg);

    tracing::info!(
        "Starting gRPC server (gRPC-web enabled) on {}",
        cfg.listen_addr
    );

    let listener = TcpListener::bind(cfg.listen_addr).await?;
    let incoming = TcpListenerStream::new(listener).filter_map(move |conn| {
        let active_connections = active_connections.clone();
        match conn {
            Ok(stream) => {
                let peer = stream.peer_addr().ok();
                active_connections.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    peer_addr = ?peer,
                    "gRPC connection accepted"
                );
                Some(Ok::<_, std::io::Error>(TrackedStream::new(
                    stream,
                    active_connections,
                )))
            }
            Err(err) => {
                tracing::warn!(error = %err, "failed to accept gRPC connection");
                None
            }
        }
    });

    let trace_layer = TraceLayer::new_for_grpc().on_request(log_grpc_request);

    Server::builder()
        .accept_http1(true)
        .layer(trace_layer)
        .layer(cors)
        .layer(GrpcWebLayer::new())
        .add_service(health_service)
        .add_service(AssetServiceServer::new(asset_impl))
        .add_service(RaceServiceServer::new(race_impl))
        .serve_with_incoming_shutdown(incoming, shutdown_signal(&mut shutdown_rx))
        .await
        .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
}

fn client_ip_from_headers(headers: &http::HeaderMap) -> String {
    let forwarded_for = headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if let Some(value) = forwarded_for {
        return value.to_string();
    }

    let real_ip = headers
        .get("x-real-ip")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if let Some(value) = real_ip {
        return value.to_string();
    }

    "unknown".to_string()
}

fn log_grpc_request(request: &http::Request<tonic::body::Body>, _span: &tracing::Span) {
    let client_ip = client_ip_from_headers(request.headers());
    let path = request.uri().path();
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");

    if path == "/grpc.health.v1.Health/Check" {
        tracing::debug!("grpc health check received");
        return;
    }

    tracing::trace!(
        client_ip = %client_ip,
        method = %path,
        content_type = %content_type,
        "grpc request received"
    );
}

struct TrackedStream {
    inner: tokio::net::TcpStream,
    active_connections: Arc<AtomicUsize>,
}

impl TrackedStream {
    fn new(inner: tokio::net::TcpStream, active_connections: Arc<AtomicUsize>) -> Self {
        Self {
            inner,
            active_connections,
        }
    }
}

impl Drop for TrackedStream {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

impl AsyncRead for TrackedStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TrackedStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, data)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Connected for TrackedStream {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        TcpConnectInfo {
            local_addr: self.inner.local_addr().ok(),
            remote_addr: self.inner.peer_addr().ok(),
        }
    }
}
