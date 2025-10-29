use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_web::GrpcWebLayer;
use tower_http::cors::{AllowHeaders, Any, CorsLayer};

use crate::config::Config;

pub async fn run(cfg: Config) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!(%cfg.listen_addr, ?cfg.env, "starting");

    let (_reporter, health_service) = health_reporter();
    let grpc_web = GrpcWebLayer::new();
    let cors = build_cors(&cfg);

    Server::builder()
        .accept_http1(true)
        .layer(cors)
        .layer(grpc_web)
        .add_service(health_service)
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
