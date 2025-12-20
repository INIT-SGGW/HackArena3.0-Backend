//! Graceful shutdown handling for server tasks.

use tokio::sync::broadcast;

/// Returns a future used by `serve_with_shutdown`.
pub async fn shutdown_signal(shutdown_rx: &mut broadcast::Receiver<()>) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutdown signal (Ctrl+C) received");
        }
        _ = shutdown_rx.recv() => {
            tracing::info!("shutdown broadcast received");
        }
    }

    tracing::info!("gRPC server task finished; requesting shutdown");
}
