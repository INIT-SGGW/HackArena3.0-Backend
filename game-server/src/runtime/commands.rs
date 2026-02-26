//! Command types sent to the engine worker.

use boink::model::{Controls, GhostModeSettings, TrackData, VehicleState};
use tokio::sync::oneshot;

use super::engine_worker::{EngineActivityKind, EngineRuntimeState, EngineWorkerError};

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
        reply_tx: oneshot::Sender<Result<VehicleState, EngineWorkerError>>,
    },
    GetTrackData {
        reply_tx: oneshot::Sender<Result<TrackData, EngineWorkerError>>,
    },
    GetRuntimeState {
        reply_tx: oneshot::Sender<Result<EngineRuntimeState, EngineWorkerError>>,
    },
    SwitchRuntime {
        activity_kind: EngineActivityKind,
        map_id: String,
        reply_tx: oneshot::Sender<Result<EngineRuntimeState, EngineWorkerError>>,
    },
    SetGhostModeSettings {
        settings: GhostModeSettings,
        reply_tx: oneshot::Sender<Result<(), EngineWorkerError>>,
    },
    DespawnCar {
        car_id: u64,
        reply_tx: oneshot::Sender<Result<(), EngineWorkerError>>,
    },
}
