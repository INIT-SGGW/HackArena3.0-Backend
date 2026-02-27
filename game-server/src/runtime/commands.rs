//! Command types sent to the engine worker.

use boink::model::{Controls, GhostModeSettings, TrackData, VehicleState};
use tokio::sync::oneshot;

use super::engine_worker::{
    EngineActivityKind, EngineRuntimeState, EngineRuntimeTimeOfDayPreset, EngineWorkerError,
};

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
        expected_revision: u64,
        activity_kind: EngineActivityKind,
        map_id: String,
        active_sandbox_id: Option<String>,
        time_of_day_preset: Option<EngineRuntimeTimeOfDayPreset>,
        ghost_mode_settings: Option<GhostModeSettings>,
        reply_tx: oneshot::Sender<Result<EngineRuntimeState, EngineWorkerError>>,
    },
    SetRuntimeTimeOfDay {
        expected_revision: u64,
        preset: EngineRuntimeTimeOfDayPreset,
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
