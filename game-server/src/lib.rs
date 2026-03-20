//! Game server library entrypoints and shared runtime helpers.

#[cfg(all(feature = "official", feature = "local"))]
compile_error!("features `official` and `local` are mutually exclusive");

pub mod auth;
pub mod config;
#[cfg(feature = "official")]
pub mod db;
pub mod domain;
#[cfg(feature = "local")]
pub mod local;
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
    let (engine, engine_task) = start_engine_worker(
        cfg.clone(),
        #[cfg(feature = "official")]
        db_pool.clone(),
        engine_shutdown_rx,
    )
    .await?;

    #[cfg(feature = "local")]
    let (broker_registration_state, broker_task): (
        crate::local::broker::BrokerRegistrationState,
        Option<JoinHandle<()>>,
    ) = {
        let (state, handle) = crate::local::broker::start_registration_manager(
            cfg.clone(),
            grpc_shutdown_tx.subscribe(),
        )
        .await
        .map_err(|err| -> Box<dyn Error> {
            let _ = engine_shutdown_tx.send(());
            Box::new(std::io::Error::other(err.to_string()))
        })?;
        (state, Some(handle))
    };

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
                #[cfg(feature = "local")]
                broker_registration_state.clone(),
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

    #[cfg(feature = "local")]
    if let Some(task) = broker_task {
        let _ = task.await;
    }

    Ok(())
}

/// Starts the engine worker and returns the client handle plus its task join handle.
async fn start_engine_worker(
    cfg: Arc<Config>,
    #[cfg(feature = "official")] official_db_pool: sqlx::PgPool,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<(EngineClient, JoinHandle<()>), Box<dyn Error>> {
    #[cfg(feature = "official")]
    let weather_sync = runtime::weather_sync::WeatherSyncState::with_repo(
        crate::db::repos::weather::WeatherRepo::new(official_db_pool.clone()),
    );
    #[cfg(not(feature = "official"))]
    let weather_sync = runtime::weather_sync::WeatherSyncState::disabled();

    let (client, handle) =
        runtime::engine_worker::spawn(cfg.clone(), weather_sync, shutdown_rx).await?;

    #[cfg(feature = "official")]
    if cfg!(debug_assertions) && cfg.env.is_development() {
        let sandbox_repo =
            crate::db::repos::sandbox_config::SandboxConfigRepo::new(official_db_pool);
        runtime::bootstrap::bootstrap_first_configured_sandbox_for_official_dev(
            &cfg,
            &client,
            &sandbox_repo,
        )
        .await;
    }

    Ok((client, handle))
}
