//! Game server entrypoint and process orchestration.

mod config;
mod runtime;
mod server;
mod services;

use std::error::Error;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::task::{JoinHandle, LocalSet};
use tracing::{error, info};

use runtime::engine_worker::EngineClient;

use crate::config::Config;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    init_tracing();

    let cfg = Arc::new(Config::load_or_exit());

    tracing::info!("game-server starting");
    tracing::info!("gRPC bind address: {}", cfg.listen_addr);

    let local = LocalSet::new();
    local.run_until(async move { run_app(cfg).await }).await
}

async fn run_app(cfg: Arc<Config>) -> Result<(), Box<dyn Error>> {
    // Broadcast-based shutdown channel shared by all background tasks.
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(16);

    // Start the engine worker (owns the boink wrapper).
    let (engine, engine_task) = start_engine_worker(cfg.clone(), shutdown_tx.subscribe()).await?;

    // Run gRPC server until shutdown.
    let shutdown_tx_grpc = shutdown_tx.clone();
    let grpc_task = tokio::task::spawn_local({
        let cfg = cfg.clone();
        async move {
            info!("Starting gRPC server on {}", cfg.listen_addr);
            if let Err(e) = crate::server::serve_grpc(cfg, engine, shutdown_rx).await {
                error!("gRPC server terminated with error: {e}");
                let _ = shutdown_tx_grpc.send(());
            }
        }
    });

    // Wait for either Ctrl+C (handled in server shutdown) or a task failure.
    tokio::select! {
        _ = grpc_task => {
            info!("gRPC server task finished");
        }
        _ = engine_task => {
            info!("engine worker task finished");
        }
    }

    // Best-effort: notify remaining tasks to stop.
    let _ = shutdown_tx.send(());

    Ok(())
}

fn init_tracing() {
    // Keep this minimal and production-safe. Configure via RUST_LOG.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

/// Starts the engine worker and returns the client handle plus its task join handle.
async fn start_engine_worker(
    cfg: Arc<Config>,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<(EngineClient, JoinHandle<()>), Box<dyn Error>> {
    let (client, handle) = runtime::engine_worker::spawn(cfg, shutdown_rx).await?;
    Ok((client, handle))
}
