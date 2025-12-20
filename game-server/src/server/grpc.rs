//! gRPC server wiring and middleware layers.

use std::sync::Arc;

use proto::race::v1::asset_service_server::AssetServiceServer;
use proto::race::v1::race_service_server::RaceServiceServer;
use tokio::sync::broadcast;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_web::GrpcWebLayer;

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
) -> Result<(), tonic::transport::Error> {
    let (health_reporter, health_service) = health_reporter();

    health_reporter
        .set_serving::<AssetServiceServer<AssetServiceImpl>>()
        .await;
    health_reporter
        .set_serving::<RaceServiceServer<RaceServiceImpl>>()
        .await;

    let asset_impl = AssetServiceImpl::new(cfg.tracks_dir.clone());
    let race_impl = RaceServiceImpl::new(engine, cfg.simulation_hz);

    let cors = cors_layer(&cfg);

    tracing::info!(
        "Starting gRPC server (gRPC-web enabled) on {}",
        cfg.listen_addr
    );

    Server::builder()
        .accept_http1(true)
        .layer(cors)
        .layer(GrpcWebLayer::new())
        .add_service(health_service)
        .add_service(AssetServiceServer::new(asset_impl))
        .add_service(RaceServiceServer::new(race_impl))
        .serve_with_shutdown(cfg.listen_addr, shutdown_signal(&mut shutdown_rx))
        .await
}
