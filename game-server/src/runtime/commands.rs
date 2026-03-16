//! Command types sent to the engine worker.

#[cfg(feature = "local")]
use boink::model::WeatherParams;
use boink::model::{
    AcceptedControls, Controls, GhostModeSettings, TrackData, VehicleRaceMetrics, VehicleState,
};
use tokio::sync::oneshot;

#[cfg(feature = "local")]
use super::engine_worker::EngineRuntimeWeatherNow;
use super::engine_worker::{
    EngineActivityKind, EngineCommandTarget, EnginePendingSandboxActivation, EngineRuntimeState,
    EngineRuntimeTimeOfDayPreset, EngineWorkerError,
};

/// Commands processed by the engine worker.
///
/// Each command carries a oneshot channel for the response.
#[derive(Debug)]
pub enum EngineCommand {
    SpawnCar {
        target: EngineCommandTarget,
        reply_tx: oneshot::Sender<Result<u64, EngineWorkerError>>,
    },
    SetControls {
        target: EngineCommandTarget,
        car_id: u64,
        controls: Controls,
        reply_tx: oneshot::Sender<Result<AcceptedControls, EngineWorkerError>>,
    },
    ReadCarState {
        target: EngineCommandTarget,
        car_id: u64,
        reply_tx: oneshot::Sender<Result<VehicleState, EngineWorkerError>>,
    },
    ReadCarRaceMetrics {
        target: EngineCommandTarget,
        car_id: u64,
        reply_tx: oneshot::Sender<Result<VehicleRaceMetrics, EngineWorkerError>>,
    },
    GetTrackData {
        target: EngineCommandTarget,
        reply_tx: oneshot::Sender<Result<TrackData, EngineWorkerError>>,
    },
    GetRaceDuration {
        target: EngineCommandTarget,
        reply_tx: oneshot::Sender<Result<f32, EngineWorkerError>>,
    },
    GetRuntimeState {
        reply_tx: oneshot::Sender<Result<EngineRuntimeState, EngineWorkerError>>,
    },
    SwitchRuntime {
        expected_revision: u64,
        activity_kind: EngineActivityKind,
        map_id: String,
        sandbox_id: Option<String>,
        time_of_day_preset: Option<EngineRuntimeTimeOfDayPreset>,
        ghost_mode_settings: Option<GhostModeSettings>,
        reply_tx: oneshot::Sender<Result<EngineRuntimeState, EngineWorkerError>>,
    },
    SetRuntimeTimeOfDay {
        expected_revision: u64,
        sandbox_id: Option<String>,
        preset: EngineRuntimeTimeOfDayPreset,
        reply_tx: oneshot::Sender<Result<EngineRuntimeState, EngineWorkerError>>,
    },
    SetPendingSandboxActivation {
        expected_revision: u64,
        pending: EnginePendingSandboxActivation,
        reply_tx: oneshot::Sender<Result<EngineRuntimeState, EngineWorkerError>>,
    },
    CancelPendingSandboxActivation {
        expected_revision: u64,
        sandbox_id: String,
        reply_tx: oneshot::Sender<Result<(EngineRuntimeState, bool), EngineWorkerError>>,
    },
    DeactivateSandbox {
        expected_revision: u64,
        sandbox_id: String,
        reply_tx: oneshot::Sender<Result<EngineRuntimeState, EngineWorkerError>>,
    },
    SetGhostModeSettings {
        target: EngineCommandTarget,
        settings: GhostModeSettings,
        reply_tx: oneshot::Sender<Result<(), EngineWorkerError>>,
    },
    #[cfg(feature = "local")]
    SetWeather {
        sandbox_id: String,
        weather: WeatherParams,
        weather_now: EngineRuntimeWeatherNow,
        reply_tx: oneshot::Sender<Result<(), EngineWorkerError>>,
    },
    DespawnCar {
        target: EngineCommandTarget,
        car_id: u64,
        reply_tx: oneshot::Sender<Result<(), EngineWorkerError>>,
    },
}
