//! Command types sent to the engine worker.

use boink::model::{CarState, Controls};
use tokio::sync::oneshot;

use super::engine_worker::EngineWorkerError;

/// Commands processed by the engine worker.
///
/// Each command carries a oneshot channel for the response.
#[derive(Debug)]
pub enum EngineCommand {
    SpawnCar {
        reply_tx: oneshot::Sender<Result<u64, EngineWorkerError>>,
    },
    SetControls {
        car_id: u64,
        controls: Controls,
        reply_tx: oneshot::Sender<Result<(), EngineWorkerError>>,
    },
    ReadCarState {
        car_id: u64,
        reply_tx: oneshot::Sender<Result<CarState, EngineWorkerError>>,
    },
    DespawnCar {
        car_id: u64,
        reply_tx: oneshot::Sender<Result<(), EngineWorkerError>>,
    },
}
