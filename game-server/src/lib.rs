//! Game server library entrypoints and shared runtime helpers.

#[cfg(all(feature = "official", feature = "local", not(feature = "standalone")))]
compile_error!("features `official` and `local` are mutually exclusive");
#[cfg(all(feature = "official", feature = "standalone"))]
compile_error!("features `official` and `standalone` are mutually exclusive");

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

use time::{OffsetDateTime, format_description};
use tokio::sync::broadcast;
use tokio::task::{JoinHandle, LocalSet};
use tracing::error;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use runtime::engine_worker::EngineClient;

use crate::config::Config;

#[cfg(feature = "standalone")]
const USER_LOG_TARGET: &str = "ha3_standalone::user";

/// Run the game server using the provided configuration.
pub async fn run(cfg: Arc<Config>) -> Result<(), Box<dyn Error>> {
    #[cfg(not(feature = "standalone"))]
    tracing::info!("gRPC bind address: {}", cfg.listen_addr);
    let local = LocalSet::new();
    local.run_until(async move { run_app(cfg).await }).await
}

/// Initialize tracing with environment-configured filters and file persistence.
pub fn init_tracing(binary_name: &str) -> Result<WorkerGuard, Box<dyn Error>> {
    init_tracing_with_default_filter(binary_name, None)
}

/// Initialize tracing with optional default filter when `RUST_LOG` is not set.
pub fn init_tracing_with_default_filter(
    binary_name: &str,
    default_filter: Option<&str>,
) -> Result<WorkerGuard, Box<dyn Error>> {
    let logs_dir = std::path::PathBuf::from(".logs");
    std::fs::create_dir_all(&logs_dir)?;
    let now = OffsetDateTime::now_utc();
    let timestamp_format =
        format_description::parse("[year]-[month]-[day]_[hour]-[minute]-[second]")?;
    let timestamp = now.format(&timestamp_format)?;
    let filename = format!("{binary_name}_{timestamp}_{:03}.log", now.millisecond());
    let file_appender = tracing_appender::rolling::never(logs_dir, filename);
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);
    let env_filter = if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::EnvFilter::from_default_env()
    } else if let Some(default_filter) = default_filter {
        tracing_subscriber::EnvFilter::builder()
            .with_default_directive(LevelFilter::ERROR.into())
            .parse_lossy(default_filter)
    } else {
        tracing_subscriber::EnvFilter::from_default_env()
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(file_writer),
        )
        .try_init()?;

    Ok(guard)
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

    #[cfg(all(feature = "local", not(feature = "standalone")))]
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

    #[cfg(feature = "standalone")]
    let frontend_shutdown_tx = grpc_shutdown_tx.clone();
    #[cfg(feature = "standalone")]
    let mut frontend_task: Option<JoinHandle<()>> = if cfg.frontend_enable {
        Some(tokio::task::spawn_local({
            let cfg = cfg.clone();
            let shutdown_tx_frontend = frontend_shutdown_tx.clone();
            let frontend_shutdown_rx = grpc_shutdown_tx.subscribe();
            async move {
                tracing::debug!(
                    listen_addr = %cfg.frontend_listen_addr,
                    "starting standalone frontend HTTP server task"
                );
                if let Err(e) = crate::server::serve_frontend(cfg, frontend_shutdown_rx).await {
                    error!("standalone frontend HTTP server terminated with error: {e}");
                    let _ = shutdown_tx_frontend.send(());
                }
            }
        }))
    } else {
        tracing::info!(
            target: USER_LOG_TARGET,
            "Frontend hosting is disabled; standalone is running without browser UI"
        );
        None
    };

    // Run gRPC server until shutdown.
    let active_connections = Arc::new(AtomicUsize::new(0));
    let shutdown_tx_grpc = grpc_shutdown_tx.clone();
    let grpc_task = tokio::task::spawn_local({
        let cfg = cfg.clone();
        let active_connections = active_connections.clone();
        async move {
            tracing::debug!(listen_addr = %cfg.listen_addr, "starting gRPC server task");
            if let Err(e) = crate::server::serve_grpc(
                cfg,
                engine,
                #[cfg(feature = "official")]
                db_pool.clone(),
                grpc_shutdown_rx,
                active_connections,
                #[cfg(all(feature = "local", not(feature = "standalone")))]
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

    #[cfg(feature = "standalone")]
    if let Some(mut task) = frontend_task.take() {
        let _ = frontend_shutdown_tx.send(());
        if tokio::time::timeout(std::time::Duration::from_secs(3), &mut task)
            .await
            .is_err()
        {
            tracing::warn!("standalone frontend HTTP shutdown timeout after 3s; aborting");
            task.abort();
            let _ = task.await;
        }
    }

    #[cfg(all(feature = "local", not(feature = "standalone")))]
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
