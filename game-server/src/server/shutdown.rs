//! Graceful shutdown handling for server tasks.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

/// Returns a future used by `serve_with_shutdown`.
pub async fn shutdown_signal(shutdown_rx: &mut broadcast::Receiver<()>) {
    let _ = shutdown_rx.recv().await;
    tracing::info!("gRPC shutdown requested");
}

/// Coordinates shutdown sequencing and timeouts.
pub async fn orchestrate_shutdown(
    mut grpc_task: JoinHandle<()>,
    mut engine_task: JoinHandle<()>,
    grpc_shutdown_tx: broadcast::Sender<()>,
    engine_shutdown_tx: broadcast::Sender<()>,
    active_connections: Arc<AtomicUsize>,
) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutdown signal (Ctrl+C) received");
            graceful_grpc_shutdown(&mut grpc_task, &grpc_shutdown_tx, &active_connections).await;
            shutdown_engine(&mut engine_task, &engine_shutdown_tx).await;
        }
        _ = &mut grpc_task => {
            tracing::info!("gRPC server task finished");
            let _ = engine_shutdown_tx.send(());
        }
        _ = &mut engine_task => {
            tracing::info!("engine worker task finished");
            let _ = grpc_shutdown_tx.send(());
        }
    }
}

async fn graceful_grpc_shutdown(
    grpc_task: &mut JoinHandle<()>,
    grpc_shutdown_tx: &broadcast::Sender<()>,
    active_connections: &Arc<AtomicUsize>,
) {
    let _ = grpc_shutdown_tx.send(());
    if wait_for_grpc(grpc_task, active_connections).await {
        return;
    }

    let open_conns = active_connections.load(Ordering::Relaxed);
    tracing::warn!("gRPC shutdown timeout after 3s; aborting");
    tracing::warn!(open_conns, "gRPC connections still open");
    grpc_task.abort();
    let _ = grpc_task.await;
}

async fn wait_for_grpc(
    grpc_task: &mut JoinHandle<()>,
    active_connections: &Arc<AtomicUsize>,
) -> bool {
    let deadline = Duration::from_secs(3);
    let start = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = &mut *grpc_task => {
                tracing::info!("gRPC server task finished");
                return true;
            }
            _ = tick.tick() => {
                let elapsed = start.elapsed();
                if elapsed >= deadline {
                    return false;
                }
                let open_conns = active_connections.load(Ordering::Relaxed);
                tracing::info!(
                    elapsed_s = elapsed.as_secs(),
                    max_s = deadline.as_secs(),
                    open_conns,
                    "waiting for gRPC connections to close"
                );
            }
        }
    }
}

async fn shutdown_engine(
    engine_task: &mut JoinHandle<()>,
    engine_shutdown_tx: &broadcast::Sender<()>,
) {
    let _ = engine_shutdown_tx.send(());
    let _ = engine_task.await;
}
