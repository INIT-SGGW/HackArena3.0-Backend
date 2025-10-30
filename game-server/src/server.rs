use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_web::GrpcWebLayer;
use tower_http::cors::{AllowHeaders, Any, CorsLayer};

use proto::race::v1::asset_service_server::AssetServiceServer;

use crate::{config::Config, services::asset_service::AssetServiceImpl};

pub async fn run(cfg: Config) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!(%cfg.listen_addr, ?cfg.env, "starting");

    let (_reporter, health_service) = health_reporter();
    let grpc_web = GrpcWebLayer::new();
    let cors = build_cors(&cfg);

    let asset_service = AssetServiceServer::new(AssetServiceImpl::new(cfg.tracks_dir.clone()));

    Server::builder()
        .accept_http1(true)
        .layer(cors)
        .layer(grpc_web)
        .add_service(health_service)
        .add_service(asset_service)
        .serve_with_shutdown(cfg.listen_addr, shutdown_signal())
        .await?;

    tracing::info!("shutdown gracefully");
    Ok(())
}

fn build_cors(cfg: &Config) -> CorsLayer {
    CorsLayer::new()
        .allow_methods(Any)
        .allow_headers(AllowHeaders::mirror_request())
        .allow_origin(cfg.allow_origin.clone())
        .expose_headers(cfg.expose_headers.clone())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
