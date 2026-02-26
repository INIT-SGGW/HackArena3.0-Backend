//! gRPC error mappings shared across services.

use boink::error::Error as BoinkError;
use tonic::Status;

use crate::runtime::engine_worker::EngineWorkerError;

/// Maps engine-layer errors into gRPC status responses.
pub(crate) fn map_worker_err(err: EngineWorkerError) -> Status {
    match err {
        EngineWorkerError::Engine(e) => map_engine_err(e),
        EngineWorkerError::WorkerStopped => Status::unavailable("engine worker stopped"),
        EngineWorkerError::InvalidArgument(msg) => Status::invalid_argument(msg),
    }
}

/// Map low-level engine errors into a stable gRPC error shape.
fn map_engine_err(e: BoinkError) -> Status {
    // Keep this centralized so all services behave consistently.
    Status::internal(format!("engine error: {e}"))
}
