//! Engine worker runtime.
//!
//! This module provides a single-owner task that owns the `boink::Engine`
//! instance and processes commands via an async channel.
//!
//! Rationale:
//! - `boink::Engine` wraps an FFI handle and should have a single clear owner.
//! - gRPC services remain thin and communicate with the worker via commands,
//!   avoiding shared mutable state and complex locking.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use boink::engine::{Engine, EngineBuilder, VehicleMesh, VehicleModelConfig};
use boink::error::Error as BoinkError;
use boink::model::control::{AcceptedControls, Controls};
use boink::model::ghost::GhostModeSettings;
use boink::model::math::Vec3;
use boink::model::state::VehicleState;
use boink::model::track::TrackData;
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use super::weather_sync::{
    WEATHER_TICK_MS, WeatherSyncState, apply_weather_from_schedule, next_boundary_instant,
};
use crate::config::Config;

use super::commands::EngineCommand;

/// Maximum number of queued commands. Keep small enough to apply backpressure.
const COMMAND_QUEUE_CAPACITY: usize = 256;
const DEFAULT_MAP_ID: &str = "test";
const DEFAULT_GHOST_MODE_SETTINGS: GhostModeSettings = GhostModeSettings {
    enabled: false,
    enter_speed_max_mps: 0.0,
    exit_speed_min_mps: 0.0,
    enter_delay_ms: 0,
    exit_delay_ms: 0,
    until_completed_laps: 0,
    vehicle_overlap_exit_delay_ms: 0,
};

/// Backend runtime activity kind reflected by engine worker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineActivityKind {
    None,
    OfficialRace,
    Sandbox,
}

/// Target runtime world for simulation commands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineCommandTarget {
    OfficialRace,
    Sandbox { sandbox_id: String },
}

/// Backend runtime time-of-day preset reflected by engine worker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineRuntimeTimeOfDayPreset {
    Unspecified,
    Morning,
    Noon,
    Evening,
    Night,
}

/// Scheduled sandbox activation/deactivation stored in runtime metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct EnginePendingSandboxActivation {
    pub activate: bool,
    pub sandbox_id: String,
    pub execute_at_unix_ms: i64,
    pub map_id: Option<String>,
    pub time_of_day_preset: Option<EngineRuntimeTimeOfDayPreset>,
    pub ghost_mode_settings: Option<GhostModeSettings>,
}

/// Active sandbox runtime details tracked by the engine worker.
#[derive(Debug, Clone, PartialEq)]
pub struct EngineActiveSandboxState {
    pub sandbox_id: String,
    pub map_id: String,
    pub time_of_day_preset: EngineRuntimeTimeOfDayPreset,
}

struct EngineWorldSlot {
    engine: Engine,
    ghost_mode_settings: GhostModeSettings,
}

struct SandboxEngineHandle {
    slot: Arc<Mutex<EngineWorldSlot>>,
    step_task: JoinHandle<()>,
}

/// Minimal runtime state owned by the engine worker.
#[derive(Debug, Clone, PartialEq)]
pub struct EngineRuntimeState {
    pub revision: u64,
    pub activity_kind: EngineActivityKind,
    pub map_id: String,
    pub active_sandboxes: Vec<EngineActiveSandboxState>,
    pub time_of_day_preset: EngineRuntimeTimeOfDayPreset,
    pub pending_sandbox_activations: Vec<EnginePendingSandboxActivation>,
}

/// Errors returned by the engine worker boundary.
///
/// This is the stable surface for API layers; map it to `tonic::Status` in services.
#[derive(Debug)]
pub enum EngineWorkerError {
    /// The worker task/channel is no longer available.
    WorkerStopped,
    /// An error occurred inside the Boink engine wrapper.
    Engine(BoinkError),
    /// Invalid runtime control request.
    InvalidArgument(String),
    /// Runtime revision did not match expected value for compare-and-swap operation.
    RevisionMismatch { expected: u64, actual: u64 },
}

/// A lightweight handle used by API services to interact with the engine worker.
///
/// Cloning this handle is cheap; all commands are funneled through the worker task.
#[derive(Clone)]
pub struct EngineClient {
    tx: mpsc::Sender<EngineCommand>,
}

impl fmt::Display for EngineWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineWorkerError::WorkerStopped => write!(f, "engine worker is stopped"),
            EngineWorkerError::Engine(e) => write!(f, "engine error: {e}"),
            EngineWorkerError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            EngineWorkerError::RevisionMismatch { expected, actual } => write!(
                f,
                "runtime revision mismatch: expected {expected}, actual {actual}"
            ),
        }
    }
}

impl std::error::Error for EngineWorkerError {}

impl EngineClient {
    /// Spawns a vehicle in official race runtime world.
    pub async fn spawn_car(&self) -> Result<u64, EngineWorkerError> {
        self.spawn_car_in(EngineCommandTarget::OfficialRace).await
    }

    /// Spawns a vehicle in target sandbox runtime world.
    pub async fn spawn_sandbox_car(&self, sandbox_id: String) -> Result<u64, EngineWorkerError> {
        self.spawn_car_in(EngineCommandTarget::Sandbox { sandbox_id })
            .await
    }

    /// Spawns a vehicle in target runtime world and returns its engine-assigned ID.
    pub async fn spawn_car_in(
        &self,
        target: EngineCommandTarget,
    ) -> Result<u64, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::SpawnCar { target, reply_tx })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Sets controls for a given official-race car.
    pub async fn set_controls(
        &self,
        car_id: u64,
        controls: Controls,
    ) -> Result<AcceptedControls, EngineWorkerError> {
        self.set_controls_in(EngineCommandTarget::OfficialRace, car_id, controls)
            .await
    }

    /// Sets controls for a given sandbox car.
    pub async fn set_sandbox_controls(
        &self,
        sandbox_id: String,
        car_id: u64,
        controls: Controls,
    ) -> Result<AcceptedControls, EngineWorkerError> {
        self.set_controls_in(
            EngineCommandTarget::Sandbox { sandbox_id },
            car_id,
            controls,
        )
        .await
    }

    /// Sets controls for a given car in target runtime world.
    pub async fn set_controls_in(
        &self,
        target: EngineCommandTarget,
        car_id: u64,
        controls: Controls,
    ) -> Result<AcceptedControls, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::SetControls {
                target,
                car_id,
                controls,
                reply_tx,
            })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Reads the latest state for a given official-race vehicle.
    pub async fn read_car_state(&self, car_id: u64) -> Result<VehicleState, EngineWorkerError> {
        self.read_car_state_in(EngineCommandTarget::OfficialRace, car_id)
            .await
    }

    /// Reads the latest state for a given sandbox vehicle.
    pub async fn read_sandbox_car_state(
        &self,
        sandbox_id: String,
        car_id: u64,
    ) -> Result<VehicleState, EngineWorkerError> {
        self.read_car_state_in(EngineCommandTarget::Sandbox { sandbox_id }, car_id)
            .await
    }

    /// Reads the latest state for a given vehicle in target runtime world.
    pub async fn read_car_state_in(
        &self,
        target: EngineCommandTarget,
        car_id: u64,
    ) -> Result<VehicleState, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::ReadCarState {
                target,
                car_id,
                reply_tx,
            })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Reads static track geometry from the official-race engine world.
    pub async fn track_data(&self) -> Result<TrackData, EngineWorkerError> {
        self.track_data_in(EngineCommandTarget::OfficialRace).await
    }

    /// Reads static track geometry from the target sandbox engine world.
    pub async fn sandbox_track_data(
        &self,
        sandbox_id: String,
    ) -> Result<TrackData, EngineWorkerError> {
        self.track_data_in(EngineCommandTarget::Sandbox { sandbox_id })
            .await
    }

    /// Reads static track geometry from the target runtime world.
    pub async fn track_data_in(
        &self,
        target: EngineCommandTarget,
    ) -> Result<TrackData, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::GetTrackData { target, reply_tx })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Reads elapsed race duration (seconds) from the official-race engine world.
    pub async fn race_duration(&self) -> Result<f32, EngineWorkerError> {
        self.race_duration_in(EngineCommandTarget::OfficialRace)
            .await
    }

    /// Reads elapsed race duration (seconds) from the target sandbox engine world.
    pub async fn sandbox_race_duration(
        &self,
        sandbox_id: String,
    ) -> Result<f32, EngineWorkerError> {
        self.race_duration_in(EngineCommandTarget::Sandbox { sandbox_id })
            .await
    }

    /// Reads elapsed race duration (seconds) from the target runtime world.
    pub async fn race_duration_in(
        &self,
        target: EngineCommandTarget,
    ) -> Result<f32, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::GetRaceDuration { target, reply_tx })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Reads current runtime activity and map metadata.
    pub async fn runtime_state(&self) -> Result<EngineRuntimeState, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::GetRuntimeState { reply_tx })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Switches activity kind and active map by rebuilding engine world.
    pub async fn switch_runtime(
        &self,
        expected_revision: u64,
        activity_kind: EngineActivityKind,
        map_id: String,
        sandbox_id: Option<String>,
        time_of_day_preset: Option<EngineRuntimeTimeOfDayPreset>,
        ghost_mode_settings: Option<GhostModeSettings>,
    ) -> Result<EngineRuntimeState, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::SwitchRuntime {
                expected_revision,
                activity_kind,
                map_id,
                sandbox_id,
                time_of_day_preset,
                ghost_mode_settings,
                reply_tx,
            })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Updates runtime time-of-day metadata without rebuilding the engine world.
    pub async fn set_runtime_time_of_day(
        &self,
        expected_revision: u64,
        sandbox_id: Option<String>,
        preset: EngineRuntimeTimeOfDayPreset,
    ) -> Result<EngineRuntimeState, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::SetRuntimeTimeOfDay {
                expected_revision,
                sandbox_id,
                preset,
                reply_tx,
            })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Stores or clears scheduled sandbox activation metadata.
    pub async fn set_pending_sandbox_activation(
        &self,
        expected_revision: u64,
        pending: EnginePendingSandboxActivation,
    ) -> Result<EngineRuntimeState, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::SetPendingSandboxActivation {
                expected_revision,
                pending,
                reply_tx,
            })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Cancels scheduled sandbox activation/deactivation metadata.
    pub async fn cancel_pending_sandbox_activation(
        &self,
        expected_revision: u64,
        sandbox_id: String,
    ) -> Result<(EngineRuntimeState, bool), EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::CancelPendingSandboxActivation {
                expected_revision,
                sandbox_id,
                reply_tx,
            })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Deactivates target sandbox runtime session.
    pub async fn deactivate_sandbox(
        &self,
        expected_revision: u64,
        sandbox_id: String,
    ) -> Result<EngineRuntimeState, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::DeactivateSandbox {
                expected_revision,
                sandbox_id,
                reply_tx,
            })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Updates ghost mode settings in official-race world.
    pub async fn set_ghost_mode_settings(
        &self,
        settings: GhostModeSettings,
    ) -> Result<(), EngineWorkerError> {
        self.set_ghost_mode_settings_in(EngineCommandTarget::OfficialRace, settings)
            .await
    }

    /// Updates ghost mode settings in target sandbox world.
    pub async fn set_sandbox_ghost_mode_settings(
        &self,
        sandbox_id: String,
        settings: GhostModeSettings,
    ) -> Result<(), EngineWorkerError> {
        self.set_ghost_mode_settings_in(EngineCommandTarget::Sandbox { sandbox_id }, settings)
            .await
    }

    /// Updates ghost mode settings in target runtime world.
    pub async fn set_ghost_mode_settings_in(
        &self,
        target: EngineCommandTarget,
        settings: GhostModeSettings,
    ) -> Result<(), EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::SetGhostModeSettings {
                target,
                settings,
                reply_tx,
            })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Despawns an official-race car, releasing it from the engine world.
    pub async fn despawn_car(&self, car_id: u64) -> Result<(), EngineWorkerError> {
        self.despawn_car_in(EngineCommandTarget::OfficialRace, car_id)
            .await
    }

    /// Despawns a sandbox car, releasing it from the engine world.
    pub async fn despawn_sandbox_car(
        &self,
        sandbox_id: String,
        car_id: u64,
    ) -> Result<(), EngineWorkerError> {
        self.despawn_car_in(EngineCommandTarget::Sandbox { sandbox_id }, car_id)
            .await
    }

    /// Despawns a car in target runtime world.
    pub async fn despawn_car_in(
        &self,
        target: EngineCommandTarget,
        car_id: u64,
    ) -> Result<(), EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::DespawnCar {
                target,
                car_id,
                reply_tx,
            })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }
}

/// Spawns the engine worker task.
///
/// Returns a client handle for issuing commands and a join handle for shutdown/monitoring.
pub async fn spawn(
    cfg: Arc<Config>,
    weather_sync: WeatherSyncState,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<(EngineClient, JoinHandle<()>), EngineWorkerError> {
    let (tx, mut rx) = mpsc::channel::<EngineCommand>(COMMAND_QUEUE_CAPACITY);
    let client = EngineClient { tx };

    let runtime_state = EngineRuntimeState {
        revision: 0,
        activity_kind: EngineActivityKind::None,
        map_id: DEFAULT_MAP_ID.to_string(),
        active_sandboxes: Vec::new(),
        time_of_day_preset: EngineRuntimeTimeOfDayPreset::Unspecified,
        pending_sandbox_activations: Vec::new(),
    };
    let sandbox_engines: HashMap<String, SandboxEngineHandle> = HashMap::new();
    let official_engine: Option<EngineWorldSlot> = None;

    tracing::info!("engine worker: startup runtime is idle");

    let simulation_dt_seconds = 1.0 / cfg.simulation_hz as f32;
    let run_cfg = Arc::clone(&cfg);
    let handle = tokio::task::spawn_local(async move {
        run_worker(
            official_engine,
            &mut rx,
            &mut shutdown_rx,
            simulation_dt_seconds,
            weather_sync,
            run_cfg,
            runtime_state,
            sandbox_engines,
        )
        .await;
    });

    Ok((client, handle))
}

async fn run_worker(
    mut official_engine: Option<EngineWorldSlot>,
    rx: &mut mpsc::Receiver<EngineCommand>,
    shutdown_rx: &mut broadcast::Receiver<()>,
    simulation_dt_seconds: f32,
    weather_sync: WeatherSyncState,
    cfg: Arc<Config>,
    mut runtime_state: EngineRuntimeState,
    mut sandbox_engines: HashMap<String, SandboxEngineHandle>,
) {
    let mut ticker =
        tokio::time::interval(tokio::time::Duration::from_secs_f32(simulation_dt_seconds));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut weather_tick = {
        let mut weather_tick = tokio::time::interval_at(
            next_boundary_instant(WEATHER_TICK_MS),
            tokio::time::Duration::from_millis(WEATHER_TICK_MS as u64),
        );
        weather_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        tracing::info!("weather sync enabled (interval=60s)");
        if matches!(
            runtime_state.activity_kind,
            EngineActivityKind::OfficialRace
        ) {
            if let Some(official_slot) = official_engine.as_mut() {
                if let Err(err) =
                    apply_weather_from_schedule(&mut official_slot.engine, &weather_sync).await
                {
                    tracing::warn!(error = %err, "initial weather apply failed");
                }
            }
        }
        weather_tick
    };

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                tracing::info!("engine worker: shutdown broadcast received");
                break;
            }

            _ = ticker.tick() => {
                let tick_start = Instant::now();
                let mut should_stop = false;

                match runtime_state.activity_kind {
                    EngineActivityKind::OfficialRace => {
                        if let Some(official_slot) = official_engine.as_mut() {
                            if let Err(err) = official_slot.engine.step(simulation_dt_seconds) {
                                tracing::warn!(error = ?err, "engine worker: official tick failed");
                            }
                            if official_slot.engine.should_close_debug() {
                                tracing::info!("engine worker: official debug drawer requested close");
                                should_stop = true;
                            }
                        } else {
                            tracing::warn!("engine worker: official runtime active without initialized engine");
                        }
                    }
                    EngineActivityKind::Sandbox => {}
                    EngineActivityKind::None => {}
                }

                let elapsed = tick_start.elapsed();
                let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
                tracing::debug!(
                    elapsed_ms = format!("{:.3}", elapsed_ms),
                    "engine worker: tick duration"
                );
                if elapsed.as_secs_f32() > simulation_dt_seconds {
                    tracing::warn!(
                        elapsed_ms = format!("{:.3}", elapsed_ms),
                        budget_ms = format!("{:.3}", simulation_dt_seconds as f64 * 1000.0),
                        "engine worker: tick exceeded budget"
                    );
                }
                if should_stop {
                    break;
                }
                if let Err(err) = maybe_execute_due_pending_sandbox_activation(
                    &mut official_engine,
                    &cfg,
                    &mut runtime_state,
                    &mut sandbox_engines,
                    simulation_dt_seconds,
                    &weather_sync,
                ) {
                    tracing::warn!("engine worker: pending sandbox activation failed: {err}");
                }
            }

            _ = weather_tick.tick() => {
                match runtime_state.activity_kind {
                    EngineActivityKind::OfficialRace => {
                        if let Some(official_slot) = official_engine.as_mut() {
                            if let Err(err) = apply_weather_from_schedule(&mut official_slot.engine, &weather_sync).await {
                                tracing::warn!(error = %err, "engine worker: official scheduled weather apply failed");
                            }
                        } else {
                            tracing::warn!("engine worker: official runtime active without initialized engine");
                        }
                    }
                    EngineActivityKind::Sandbox => {}
                    EngineActivityKind::None => {}
                }
            }

            cmd = rx.recv() => {
                let Some(cmd) = cmd else {
                    tracing::info!("engine worker: command channel closed");
                    break;
                };

                if let Err(err) = handle_command(
                    &mut official_engine,
                    &cfg,
                    &mut runtime_state,
                    &mut sandbox_engines,
                    simulation_dt_seconds,
                    &weather_sync,
                    cmd,
                )
                .await
                {
                    tracing::warn!("engine worker: command failed: {err}");
                }
                if let Err(err) = maybe_execute_due_pending_sandbox_activation(
                    &mut official_engine,
                    &cfg,
                    &mut runtime_state,
                    &mut sandbox_engines,
                    simulation_dt_seconds,
                    &weather_sync,
                ) {
                    tracing::warn!("engine worker: pending sandbox activation failed: {err}");
                }
            }
        }
    }

    for (_, handle) in sandbox_engines.drain() {
        handle.step_task.abort();
    }
    tracing::info!("engine worker: stopped");
}

async fn run_sandbox_loop(
    sandbox_id: String,
    slot: Arc<Mutex<EngineWorldSlot>>,
    simulation_dt_seconds: f32,
    weather_sync: WeatherSyncState,
) {
    let mut ticker =
        tokio::time::interval(tokio::time::Duration::from_secs_f32(simulation_dt_seconds));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut weather_tick = tokio::time::interval_at(
        next_boundary_instant(WEATHER_TICK_MS),
        tokio::time::Duration::from_millis(WEATHER_TICK_MS as u64),
    );
    weather_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    {
        let mut slot_guard = slot.lock().await;
        if let Err(err) = apply_weather_from_schedule(&mut slot_guard.engine, &weather_sync).await {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %err,
                "engine worker: initial sandbox weather apply failed"
            );
        }
    }

    tracing::info!(sandbox_id = %sandbox_id, "engine worker: sandbox loop started");

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let tick_start = Instant::now();
                {
                    let mut slot_guard = slot.lock().await;
                    if let Err(err) = slot_guard.engine.step(simulation_dt_seconds) {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            error = ?err,
                            "engine worker: sandbox tick failed"
                        );
                    }
                }
                let elapsed = tick_start.elapsed();
                let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    elapsed_ms = format!("{:.3}", elapsed_ms),
                    "engine worker: sandbox tick duration"
                );
                if elapsed.as_secs_f32() > simulation_dt_seconds {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        elapsed_ms = format!("{:.3}", elapsed_ms),
                        budget_ms = format!("{:.3}", simulation_dt_seconds as f64 * 1000.0),
                        "engine worker: sandbox tick exceeded budget"
                    );
                }
            }
            _ = weather_tick.tick() => {
                let mut slot_guard = slot.lock().await;
                if let Err(err) = apply_weather_from_schedule(&mut slot_guard.engine, &weather_sync).await {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        error = %err,
                        "engine worker: sandbox scheduled weather apply failed"
                    );
                }
            }
        }
    }
}

async fn with_target_slot_mut<R>(
    target: &EngineCommandTarget,
    runtime_state: &EngineRuntimeState,
    official_engine: &mut Option<EngineWorldSlot>,
    sandbox_engines: &HashMap<String, SandboxEngineHandle>,
    op: impl FnOnce(&mut EngineWorldSlot) -> Result<R, EngineWorkerError>,
) -> Result<R, EngineWorkerError> {
    match target {
        EngineCommandTarget::OfficialRace => {
            if !matches!(
                runtime_state.activity_kind,
                EngineActivityKind::OfficialRace
            ) {
                return Err(EngineWorkerError::InvalidArgument(
                    "official race runtime is not active".to_string(),
                ));
            }
            let official_slot = official_engine.as_mut().ok_or_else(|| {
                EngineWorkerError::InvalidArgument(
                    "official race runtime is not active".to_string(),
                )
            })?;
            op(official_slot)
        }
        EngineCommandTarget::Sandbox { sandbox_id } => {
            if sandbox_id.trim().is_empty() {
                return Err(EngineWorkerError::InvalidArgument(
                    "sandbox_id must be non-empty".to_string(),
                ));
            }
            if !matches!(runtime_state.activity_kind, EngineActivityKind::Sandbox) {
                return Err(EngineWorkerError::InvalidArgument(
                    "sandbox runtime is not active".to_string(),
                ));
            }
            let slot = sandbox_engines
                .get(sandbox_id)
                .map(|handle| Arc::clone(&handle.slot))
                .ok_or_else(|| {
                    EngineWorkerError::InvalidArgument(
                        "sandbox_id does not match active sandbox session".to_string(),
                    )
                })?;
            let mut slot = slot.lock().await;
            op(&mut slot)
        }
    }
}

async fn handle_command(
    official_engine: &mut Option<EngineWorldSlot>,
    cfg: &Config,
    runtime_state: &mut EngineRuntimeState,
    sandbox_engines: &mut HashMap<String, SandboxEngineHandle>,
    simulation_dt_seconds: f32,
    weather_sync: &WeatherSyncState,
    cmd: EngineCommand,
) -> Result<(), EngineWorkerError> {
    match cmd {
        EngineCommand::SpawnCar { target, reply_tx } => {
            let result = with_target_slot_mut(
                &target,
                runtime_state,
                official_engine,
                sandbox_engines,
                |slot| {
                    slot.engine
                        .spawn_vehicle()
                        .map_err(EngineWorkerError::Engine)
                },
            )
            .await;
            let _ = reply_tx.send(result);
            Ok(())
        }

        EngineCommand::SetControls {
            target,
            car_id,
            controls,
            reply_tx,
        } => {
            let result = with_target_slot_mut(
                &target,
                runtime_state,
                official_engine,
                sandbox_engines,
                |slot| {
                    slot.engine
                        .set_controls(car_id, controls)
                        .map_err(EngineWorkerError::Engine)
                },
            )
            .await;
            let _ = reply_tx.send(result);
            Ok(())
        }

        EngineCommand::ReadCarState {
            target,
            car_id,
            reply_tx,
        } => {
            let result = with_target_slot_mut(
                &target,
                runtime_state,
                official_engine,
                sandbox_engines,
                |slot| {
                    slot.engine
                        .read_vehicle_state(car_id)
                        .map_err(EngineWorkerError::Engine)
                },
            )
            .await;
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::GetTrackData { target, reply_tx } => {
            let result = with_target_slot_mut(
                &target,
                runtime_state,
                official_engine,
                sandbox_engines,
                |slot| slot.engine.track_data().map_err(EngineWorkerError::Engine),
            )
            .await;
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::GetRaceDuration { target, reply_tx } => {
            let result = with_target_slot_mut(
                &target,
                runtime_state,
                official_engine,
                sandbox_engines,
                |slot| {
                    slot.engine
                        .race_duration()
                        .map_err(EngineWorkerError::Engine)
                },
            )
            .await;
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::GetRuntimeState { reply_tx } => {
            let _ = reply_tx.send(Ok(runtime_state.clone()));
            Ok(())
        }
        EngineCommand::SwitchRuntime {
            expected_revision,
            activity_kind,
            map_id,
            sandbox_id,
            time_of_day_preset: next_time_of_day_preset,
            ghost_mode_settings: next_ghost_mode_settings,
            reply_tx,
        } => {
            let target_time_of_day_preset =
                next_time_of_day_preset.unwrap_or(runtime_state.time_of_day_preset);
            let result = switch_runtime(
                official_engine,
                cfg,
                runtime_state,
                sandbox_engines,
                expected_revision,
                activity_kind,
                map_id,
                sandbox_id,
                target_time_of_day_preset,
                next_ghost_mode_settings,
                simulation_dt_seconds,
                weather_sync,
            );
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::SetRuntimeTimeOfDay {
            expected_revision,
            sandbox_id,
            preset,
            reply_tx,
        } => {
            let result =
                set_runtime_time_of_day(runtime_state, expected_revision, sandbox_id, preset);
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::SetPendingSandboxActivation {
            expected_revision,
            pending,
            reply_tx,
        } => {
            let result = set_pending_sandbox_activation(runtime_state, expected_revision, pending);
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::CancelPendingSandboxActivation {
            expected_revision,
            sandbox_id,
            reply_tx,
        } => {
            let result =
                cancel_pending_sandbox_activation(runtime_state, expected_revision, &sandbox_id);
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::DeactivateSandbox {
            expected_revision,
            sandbox_id,
            reply_tx,
        } => {
            let result = deactivate_sandbox(
                official_engine,
                runtime_state,
                sandbox_engines,
                expected_revision,
                &sandbox_id,
            );
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::SetGhostModeSettings {
            target,
            settings,
            reply_tx,
        } => {
            let result = with_target_slot_mut(
                &target,
                runtime_state,
                official_engine,
                sandbox_engines,
                |slot| {
                    slot.engine
                        .set_ghost_mode_settings(settings)
                        .map_err(EngineWorkerError::Engine)?;
                    slot.ghost_mode_settings = settings;
                    Ok(())
                },
            )
            .await;
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::DespawnCar {
            target,
            car_id,
            reply_tx,
        } => {
            let result = with_target_slot_mut(
                &target,
                runtime_state,
                official_engine,
                sandbox_engines,
                |slot| {
                    slot.engine
                        .despawn_vehicle(car_id)
                        .map_err(EngineWorkerError::Engine)
                },
            )
            .await;
            let _ = reply_tx.send(result);
            Ok(())
        }
    }
}

fn switch_runtime(
    official_engine: &mut Option<EngineWorldSlot>,
    cfg: &Config,
    runtime_state: &mut EngineRuntimeState,
    sandbox_engines: &mut HashMap<String, SandboxEngineHandle>,
    expected_revision: u64,
    activity_kind: EngineActivityKind,
    map_id: String,
    sandbox_id: Option<String>,
    time_of_day_preset: EngineRuntimeTimeOfDayPreset,
    ghost_mode_settings: Option<GhostModeSettings>,
    simulation_dt_seconds: f32,
    weather_sync: &WeatherSyncState,
) -> Result<EngineRuntimeState, EngineWorkerError> {
    if runtime_state.revision != expected_revision {
        return Err(EngineWorkerError::RevisionMismatch {
            expected: expected_revision,
            actual: runtime_state.revision,
        });
    }

    let mut upserted_sandbox_id: Option<String> = None;

    match activity_kind {
        EngineActivityKind::None => {
            validate_map_id(&map_id)?;
            *official_engine = None;
            for (_, handle) in sandbox_engines.drain() {
                handle.step_task.abort();
            }

            runtime_state.activity_kind = EngineActivityKind::None;
            runtime_state.map_id = map_id;
            runtime_state.active_sandboxes.clear();
            runtime_state.time_of_day_preset = time_of_day_preset;
        }
        EngineActivityKind::OfficialRace => {
            validate_map_id(&map_id)?;
            let current_ghost_mode_settings = official_engine
                .as_ref()
                .map(|slot| slot.ghost_mode_settings)
                .unwrap_or(DEFAULT_GHOST_MODE_SETTINGS);
            let target_ghost_mode_settings =
                ghost_mode_settings.unwrap_or(current_ghost_mode_settings);
            let mut new_engine = build_engine(cfg, &map_id)?;
            new_engine
                .set_ghost_mode_settings(target_ghost_mode_settings)
                .map_err(EngineWorkerError::Engine)?;
            *official_engine = Some(EngineWorldSlot {
                engine: new_engine,
                ghost_mode_settings: target_ghost_mode_settings,
            });
            for (_, handle) in sandbox_engines.drain() {
                handle.step_task.abort();
            }

            runtime_state.activity_kind = EngineActivityKind::OfficialRace;
            runtime_state.map_id = map_id;
            runtime_state.active_sandboxes.clear();
            runtime_state.time_of_day_preset = time_of_day_preset;
        }
        EngineActivityKind::Sandbox => {
            let sandbox_id = sandbox_id.ok_or_else(|| {
                EngineWorkerError::InvalidArgument(
                    "sandbox_id is required for sandbox runtime".to_string(),
                )
            })?;
            if sandbox_id.trim().is_empty() {
                return Err(EngineWorkerError::InvalidArgument(
                    "sandbox_id must be non-empty for sandbox runtime".to_string(),
                ));
            }
            if matches!(
                time_of_day_preset,
                EngineRuntimeTimeOfDayPreset::Unspecified
            ) {
                return Err(EngineWorkerError::InvalidArgument(
                    "time_of_day_preset must be specified for sandbox runtime".to_string(),
                ));
            }
            validate_map_id(&map_id)?;
            if is_sandbox_active(runtime_state, &sandbox_id) {
                return Err(EngineWorkerError::InvalidArgument(
                    "sandbox_id is already active".to_string(),
                ));
            }

            let target_ghost_mode_settings =
                ghost_mode_settings.unwrap_or(DEFAULT_GHOST_MODE_SETTINGS);

            let mut sandbox_engine = build_engine(cfg, &map_id)?;
            sandbox_engine
                .set_ghost_mode_settings(target_ghost_mode_settings)
                .map_err(EngineWorkerError::Engine)?;
            let slot = Arc::new(Mutex::new(EngineWorldSlot {
                engine: sandbox_engine,
                ghost_mode_settings: target_ghost_mode_settings,
            }));
            let step_task = tokio::task::spawn_local(run_sandbox_loop(
                sandbox_id.clone(),
                Arc::clone(&slot),
                simulation_dt_seconds,
                weather_sync.clone(),
            ));
            if let Some(previous) =
                sandbox_engines.insert(sandbox_id.clone(), SandboxEngineHandle { slot, step_task })
            {
                previous.step_task.abort();
            }

            upsert_active_sandbox(
                &mut runtime_state.active_sandboxes,
                EngineActiveSandboxState {
                    sandbox_id: sandbox_id.clone(),
                    map_id: map_id.clone(),
                    time_of_day_preset,
                },
            );
            upserted_sandbox_id = Some(sandbox_id.clone());
            runtime_state.activity_kind = EngineActivityKind::Sandbox;
            runtime_state.time_of_day_preset = time_of_day_preset;
            runtime_state
                .pending_sandbox_activations
                .retain(|pending| pending.sandbox_id != sandbox_id);
        }
    }

    runtime_state.revision = runtime_state.revision.saturating_add(1);

    if matches!(runtime_state.activity_kind, EngineActivityKind::Sandbox) {
        tracing::info!(
            revision = runtime_state.revision,
            sandbox_id = ?upserted_sandbox_id,
            activity_kind = ?runtime_state.activity_kind,
            map_id = %runtime_state.map_id,
            active_sandboxes = runtime_state.active_sandboxes.len(),
            sandbox_engines = sandbox_engines.len(),
            time_of_day_preset = ?runtime_state.time_of_day_preset,
            pending_sandbox_activations = runtime_state.pending_sandbox_activations.len(),
            "engine worker: sandbox upserted"
        );
    } else {
        tracing::info!(
            revision = runtime_state.revision,
            activity_kind = ?runtime_state.activity_kind,
            map_id = %runtime_state.map_id,
            active_sandboxes = runtime_state.active_sandboxes.len(),
            sandbox_engines = sandbox_engines.len(),
            time_of_day_preset = ?runtime_state.time_of_day_preset,
            pending_sandbox_activations = runtime_state.pending_sandbox_activations.len(),
            "engine worker: runtime switched"
        );
    }

    Ok(runtime_state.clone())
}

fn deactivate_sandbox(
    official_engine: &mut Option<EngineWorldSlot>,
    runtime_state: &mut EngineRuntimeState,
    sandbox_engines: &mut HashMap<String, SandboxEngineHandle>,
    expected_revision: u64,
    sandbox_id: &str,
) -> Result<EngineRuntimeState, EngineWorkerError> {
    if runtime_state.revision != expected_revision {
        return Err(EngineWorkerError::RevisionMismatch {
            expected: expected_revision,
            actual: runtime_state.revision,
        });
    }
    if sandbox_id.trim().is_empty() {
        return Err(EngineWorkerError::InvalidArgument(
            "sandbox_id must be non-empty for deactivation".to_string(),
        ));
    }

    let mut removed = false;
    if let Some(position) = runtime_state
        .active_sandboxes
        .iter()
        .position(|entry| entry.sandbox_id == sandbox_id)
    {
        runtime_state.active_sandboxes.remove(position);
        removed = true;
    }
    if let Some(handle) = sandbox_engines.remove(sandbox_id) {
        handle.step_task.abort();
        removed = true;
    }
    runtime_state
        .pending_sandbox_activations
        .retain(|pending| pending.sandbox_id != sandbox_id);

    if runtime_state.active_sandboxes.is_empty()
        && matches!(runtime_state.activity_kind, EngineActivityKind::Sandbox)
    {
        runtime_state.activity_kind = EngineActivityKind::None;
        runtime_state.time_of_day_preset = EngineRuntimeTimeOfDayPreset::Unspecified;
        *official_engine = None;
    }

    runtime_state.revision = runtime_state.revision.saturating_add(1);

    tracing::info!(
        revision = runtime_state.revision,
        sandbox_id = %sandbox_id,
        removed,
        activity_kind = ?runtime_state.activity_kind,
        active_sandboxes = runtime_state.active_sandboxes.len(),
        sandbox_engines = sandbox_engines.len(),
        "engine worker: sandbox deactivated"
    );

    Ok(runtime_state.clone())
}

fn set_runtime_time_of_day(
    runtime_state: &mut EngineRuntimeState,
    expected_revision: u64,
    sandbox_id: Option<String>,
    preset: EngineRuntimeTimeOfDayPreset,
) -> Result<EngineRuntimeState, EngineWorkerError> {
    if runtime_state.revision != expected_revision {
        return Err(EngineWorkerError::RevisionMismatch {
            expected: expected_revision,
            actual: runtime_state.revision,
        });
    }
    if !matches!(runtime_state.activity_kind, EngineActivityKind::Sandbox) {
        return Err(EngineWorkerError::InvalidArgument(
            "sandbox time-of-day override requires active sandbox runtime".to_string(),
        ));
    }
    if matches!(preset, EngineRuntimeTimeOfDayPreset::Unspecified) {
        return Err(EngineWorkerError::InvalidArgument(
            "time-of-day preset must be specified".to_string(),
        ));
    }

    let target_sandbox_id = match sandbox_id {
        Some(sandbox_id) => {
            if sandbox_id.trim().is_empty() {
                return Err(EngineWorkerError::InvalidArgument(
                    "sandbox_id must be non-empty when provided".to_string(),
                ));
            }
            sandbox_id
        }
        None => {
            if runtime_state.active_sandboxes.len() == 1 {
                runtime_state.active_sandboxes[0].sandbox_id.clone()
            } else {
                return Err(EngineWorkerError::InvalidArgument(
                    "sandbox_id is required when multiple sandbox sessions are active".to_string(),
                ));
            }
        }
    };

    let target = runtime_state
        .active_sandboxes
        .iter_mut()
        .find(|entry| entry.sandbox_id == target_sandbox_id)
        .ok_or_else(|| {
            EngineWorkerError::InvalidArgument(
                "sandbox_id does not match active sandbox session".to_string(),
            )
        })?;
    target.time_of_day_preset = preset;
    runtime_state.time_of_day_preset = preset;

    runtime_state.revision = runtime_state.revision.saturating_add(1);

    tracing::info!(
        revision = runtime_state.revision,
        activity_kind = ?runtime_state.activity_kind,
        map_id = %runtime_state.map_id,
        active_sandboxes = runtime_state.active_sandboxes.len(),
        time_of_day_preset = ?runtime_state.time_of_day_preset,
        pending_sandbox_activations = runtime_state.pending_sandbox_activations.len(),
        "engine worker: sandbox time-of-day updated"
    );

    Ok(runtime_state.clone())
}

fn upsert_active_sandbox(
    active_sandboxes: &mut Vec<EngineActiveSandboxState>,
    next: EngineActiveSandboxState,
) {
    if let Some(current) = active_sandboxes
        .iter_mut()
        .find(|entry| entry.sandbox_id == next.sandbox_id)
    {
        *current = next;
        return;
    }
    active_sandboxes.push(next);
}

fn is_sandbox_active(runtime_state: &EngineRuntimeState, sandbox_id: &str) -> bool {
    runtime_state
        .active_sandboxes
        .iter()
        .any(|entry| entry.sandbox_id == sandbox_id)
}

fn set_pending_sandbox_activation(
    runtime_state: &mut EngineRuntimeState,
    expected_revision: u64,
    pending: EnginePendingSandboxActivation,
) -> Result<EngineRuntimeState, EngineWorkerError> {
    if runtime_state.revision != expected_revision {
        return Err(EngineWorkerError::RevisionMismatch {
            expected: expected_revision,
            actual: runtime_state.revision,
        });
    }

    if pending.sandbox_id.trim().is_empty() {
        return Err(EngineWorkerError::InvalidArgument(
            "sandbox_id must be non-empty for pending activation/deactivation".to_string(),
        ));
    }
    if pending.activate {
        if is_sandbox_active(runtime_state, &pending.sandbox_id) {
            return Err(EngineWorkerError::InvalidArgument(
                "sandbox_id is already active".to_string(),
            ));
        }
        let map_id = pending.map_id.as_ref().ok_or_else(|| {
            EngineWorkerError::InvalidArgument(
                "map_id must be set for scheduled activation".to_string(),
            )
        })?;
        if map_id.trim().is_empty() {
            return Err(EngineWorkerError::InvalidArgument(
                "map_id must be non-empty for scheduled activation".to_string(),
            ));
        }
        let time_of_day_preset = pending.time_of_day_preset.ok_or_else(|| {
            EngineWorkerError::InvalidArgument(
                "time_of_day_preset must be set for scheduled activation".to_string(),
            )
        })?;
        if matches!(
            time_of_day_preset,
            EngineRuntimeTimeOfDayPreset::Unspecified
        ) {
            return Err(EngineWorkerError::InvalidArgument(
                "time_of_day_preset must be specified for scheduled activation".to_string(),
            ));
        }
        if pending.ghost_mode_settings.is_none() {
            return Err(EngineWorkerError::InvalidArgument(
                "ghost_mode_settings must be set for scheduled activation".to_string(),
            ));
        }
    } else if pending.map_id.is_some()
        || pending.time_of_day_preset.is_some()
        || pending.ghost_mode_settings.is_some()
    {
        return Err(EngineWorkerError::InvalidArgument(
            "scheduled deactivation must not include activation payload".to_string(),
        ));
    }

    runtime_state.revision = runtime_state.revision.saturating_add(1);
    if let Some(existing) = runtime_state
        .pending_sandbox_activations
        .iter_mut()
        .find(|entry| entry.sandbox_id == pending.sandbox_id)
    {
        *existing = pending;
    } else {
        runtime_state.pending_sandbox_activations.push(pending);
    }

    tracing::info!(
        revision = runtime_state.revision,
        pending_sandbox_activations = runtime_state.pending_sandbox_activations.len(),
        "engine worker: pending sandbox activation updated"
    );

    Ok(runtime_state.clone())
}

fn cancel_pending_sandbox_activation(
    runtime_state: &mut EngineRuntimeState,
    expected_revision: u64,
    sandbox_id: &str,
) -> Result<(EngineRuntimeState, bool), EngineWorkerError> {
    if runtime_state.revision != expected_revision {
        return Err(EngineWorkerError::RevisionMismatch {
            expected: expected_revision,
            actual: runtime_state.revision,
        });
    }
    if sandbox_id.trim().is_empty() {
        return Err(EngineWorkerError::InvalidArgument(
            "sandbox_id must be non-empty for pending cancellation".to_string(),
        ));
    }

    let before_len = runtime_state.pending_sandbox_activations.len();
    runtime_state
        .pending_sandbox_activations
        .retain(|entry| entry.sandbox_id != sandbox_id);
    let canceled = runtime_state.pending_sandbox_activations.len() != before_len;

    if canceled {
        runtime_state.revision = runtime_state.revision.saturating_add(1);
    }

    tracing::info!(
        revision = runtime_state.revision,
        sandbox_id = %sandbox_id,
        canceled,
        pending_sandbox_activations = runtime_state.pending_sandbox_activations.len(),
        "engine worker: pending sandbox activation cancellation processed"
    );

    Ok((runtime_state.clone(), canceled))
}

fn maybe_execute_due_pending_sandbox_activation(
    official_engine: &mut Option<EngineWorldSlot>,
    cfg: &Config,
    runtime_state: &mut EngineRuntimeState,
    sandbox_engines: &mut HashMap<String, SandboxEngineHandle>,
    simulation_dt_seconds: f32,
    weather_sync: &WeatherSyncState,
) -> Result<(), EngineWorkerError> {
    if runtime_state.pending_sandbox_activations.is_empty() {
        return Ok(());
    }
    let now_unix_ms = current_unix_ms();
    let mut due: Vec<_> = runtime_state
        .pending_sandbox_activations
        .iter()
        .filter(|entry| entry.execute_at_unix_ms <= now_unix_ms)
        .cloned()
        .collect();
    if due.is_empty() {
        return Ok(());
    }
    due.sort_by_key(|entry| entry.execute_at_unix_ms);

    for pending in due {
        if pending.activate {
            if is_sandbox_active(runtime_state, &pending.sandbox_id) {
                tracing::info!(
                    sandbox_id = %pending.sandbox_id,
                    "engine worker: scheduled sandbox activation skipped (already active)"
                );
                runtime_state
                    .pending_sandbox_activations
                    .retain(|entry| entry.sandbox_id != pending.sandbox_id);
                continue;
            }
            let map_id = pending.map_id.clone().ok_or_else(|| {
                EngineWorkerError::InvalidArgument(
                    "scheduled activation is missing map_id".to_string(),
                )
            })?;
            let time_of_day_preset = pending.time_of_day_preset.ok_or_else(|| {
                EngineWorkerError::InvalidArgument(
                    "scheduled activation is missing time_of_day_preset".to_string(),
                )
            })?;
            let scheduled_ghost_mode_settings = pending.ghost_mode_settings.ok_or_else(|| {
                EngineWorkerError::InvalidArgument(
                    "scheduled activation is missing ghost_mode_settings".to_string(),
                )
            })?;

            let _ = switch_runtime(
                official_engine,
                cfg,
                runtime_state,
                sandbox_engines,
                runtime_state.revision,
                EngineActivityKind::Sandbox,
                map_id,
                Some(pending.sandbox_id.clone()),
                time_of_day_preset,
                Some(scheduled_ghost_mode_settings),
                simulation_dt_seconds,
                weather_sync,
            )?;
            tracing::info!(sandbox_id = %pending.sandbox_id, "engine worker: scheduled sandbox activation executed");
        } else {
            let _ = deactivate_sandbox(
                official_engine,
                runtime_state,
                sandbox_engines,
                runtime_state.revision,
                &pending.sandbox_id,
            )?;
            tracing::info!(sandbox_id = %pending.sandbox_id, "engine worker: scheduled sandbox deactivation executed");
        }
        runtime_state
            .pending_sandbox_activations
            .retain(|entry| entry.sandbox_id != pending.sandbox_id);
    }

    Ok(())
}
fn current_unix_ms() -> i64 {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
    let nanos_ms = i64::from(duration.subsec_millis());
    seconds.saturating_mul(1000).saturating_add(nanos_ms)
}

fn validate_map_id(map_id: &str) -> Result<(), EngineWorkerError> {
    if map_id.trim().is_empty() {
        return Err(EngineWorkerError::InvalidArgument(
            "map_id must be non-empty".to_string(),
        ));
    }
    if !map_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(EngineWorkerError::InvalidArgument(
            "map_id contains invalid characters".to_string(),
        ));
    }
    Ok(())
}

/// Builds the engine instance using server configuration defaults.
fn build_engine(cfg: &Config, map_id: &str) -> Result<Engine, EngineWorkerError> {
    validate_map_id(map_id)?;
    let track_glb = cfg.tracks_dir.join(format!("{map_id}.glb"));
    tracing::info!(
        env = ?cfg.env,
        map_id = %map_id,
        track_path = %track_glb.display(),
        "initializing engine world"
    );

    let mesh_glb = cfg.bolids_dir.join("test.glb");
    let mesh = VehicleMesh::load(&mesh_glb).map_err(EngineWorkerError::Engine)?;

    let vehicle_model = VehicleModelConfig {
        mesh: Arc::new(mesh),
        center_of_mass: Vec3 {
            x: 0.0,
            y: -1.0,
            z: 0.0,
        },
        wheel_radius: 0.36,
        suspension_rest_length: 0.52,
        mass: 800.0,
        max_steer_angle_deg: 30.0,
    };

    let builder =
        EngineBuilder::new(track_glb, vehicle_model).with_debug_drawer(cfg.debug_drawer_enabled);
    builder.build().map_err(EngineWorkerError::Engine)
}
