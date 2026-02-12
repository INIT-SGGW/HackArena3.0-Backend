//! Game server library entrypoints and shared runtime helpers.

pub mod auth;
pub mod config;
#[cfg(feature = "official")]
pub mod db;
pub mod domain;
pub mod runtime;
pub mod server;
pub mod services;

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use tokio::sync::broadcast;
use tokio::task::{JoinHandle, LocalSet};
use tracing::{error, info};

use runtime::engine_worker::EngineClient;

use crate::config::Config;

/// Run the game server using the provided configuration.
pub async fn run(cfg: Arc<Config>) -> Result<(), Box<dyn Error>> {
    tracing::info!("gRPC bind address: {}", cfg.listen_addr);
    let local = LocalSet::new();
    local.run_until(async move { run_app(cfg).await }).await
}

/// Initialize tracing with environment-configured filters.
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

async fn run_app(cfg: Arc<Config>) -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "official")]
    let db_pool = {
        let pool = crate::db::connect_and_migrate(
            &cfg.official_database_url,
            cfg.official_db_max_connections,
        )
        .await?;
        tracing::info!("official database ready");
        pool
    };

    // Separate shutdown channels so we can grace gRPC before stopping the engine.
    let (grpc_shutdown_tx, grpc_shutdown_rx) = broadcast::channel::<()>(16);
    let (engine_shutdown_tx, engine_shutdown_rx) = broadcast::channel::<()>(16);

    // Start the engine worker (owns the boink wrapper).
    let (engine, engine_task) = start_engine_worker(cfg.clone(), engine_shutdown_rx).await?;

    // Run gRPC server until shutdown.
    let active_connections = Arc::new(AtomicUsize::new(0));
    let shutdown_tx_grpc = grpc_shutdown_tx.clone();
    let grpc_task = tokio::task::spawn_local({
        let cfg = cfg.clone();
        let active_connections = active_connections.clone();
        async move {
            info!("Starting gRPC server on {}", cfg.listen_addr);
            if let Err(e) = crate::server::serve_grpc(
                cfg,
                engine,
                #[cfg(feature = "official")]
                db_pool.clone(),
                grpc_shutdown_rx,
                active_connections,
            )
            .await
            {
                error!("gRPC server terminated with error: {e}");
                let _ = shutdown_tx_grpc.send(());
            }
        }
    });
    crate::server::shutdown::orchestrate_shutdown(
        grpc_task,
        engine_task,
        grpc_shutdown_tx,
        engine_shutdown_tx,
        active_connections,
    )
    .await;

    Ok(())
}

/// Starts the engine worker and returns the client handle plus its task join handle.
async fn start_engine_worker(
    cfg: Arc<Config>,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<(EngineClient, JoinHandle<()>), Box<dyn Error>> {
    let (client, handle) = runtime::engine_worker::spawn(cfg, shutdown_rx).await?;
    Ok((client, handle))
}
