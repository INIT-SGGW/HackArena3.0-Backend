//! Engine worker runtime.
//!
//! This module provides a single-owner task that owns the `boink::Engine`
//! instance and processes commands via an async channel.
//!
//! Rationale:
//! - `boink::Engine` wraps an FFI handle and should have a single clear owner.
//! - gRPC services remain thin and communicate with the worker via commands,
//!   avoiding shared mutable state and complex locking.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use boink::engine::{Engine, EngineBuilder, VehicleMesh, VehicleModelConfig};
use boink::error::Error as BoinkError;
use boink::model::control::Controls;
use boink::model::ghost::{GhostModeConditionLogic, GhostModeSettings};
use boink::model::math::Vec3;
use boink::model::state::VehicleState;
use boink::model::track::TrackData;
use tokio::sync::{broadcast, mpsc, oneshot};
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
    min_speed_enter_mps: 0.0,
    min_speed_exit_mps: 0.0,
    enter_delay_ms: 0,
    exit_delay_ms: 0,
    min_completed_laps: 0,
    condition_logic: GhostModeConditionLogic::Unspecified,
    overlap_exit_delay_ms: 0,
};

/// Backend runtime activity kind reflected by engine worker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineActivityKind {
    None,
    OfficialRace,
    Sandbox,
}

/// Minimal runtime state owned by the engine worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineRuntimeState {
    pub revision: u64,
    pub activity_kind: EngineActivityKind,
    pub map_id: String,
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
        }
    }
}

impl std::error::Error for EngineWorkerError {}

impl EngineClient {
    /// Spawns a vehicle and returns its engine-assigned ID.
    pub async fn spawn_car(&self) -> Result<u64, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::SpawnCar { reply_tx })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Sets controls for a given car.
    pub async fn set_controls(
        &self,
        car_id: u64,
        controls: Controls,
    ) -> Result<(), EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::SetControls {
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

    /// Reads the latest state for a given vehicle.
    pub async fn read_car_state(&self, car_id: u64) -> Result<VehicleState, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::ReadCarState { car_id, reply_tx })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Reads static track geometry from the engine world.
    pub async fn track_data(&self) -> Result<TrackData, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::GetTrackData { reply_tx })
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
        activity_kind: EngineActivityKind,
        map_id: String,
    ) -> Result<EngineRuntimeState, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::SwitchRuntime {
                activity_kind,
                map_id,
                reply_tx,
            })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Updates global ghost mode settings in the active engine world.
    pub async fn set_ghost_mode_settings(
        &self,
        settings: GhostModeSettings,
    ) -> Result<(), EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::SetGhostModeSettings { settings, reply_tx })
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?;

        reply_rx
            .await
            .map_err(|_| EngineWorkerError::WorkerStopped)?
    }

    /// Despawns a car, releasing it from the engine world.
    pub async fn despawn_car(&self, car_id: u64) -> Result<(), EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::DespawnCar { car_id, reply_tx })
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
        activity_kind: EngineActivityKind::OfficialRace,
        map_id: DEFAULT_MAP_ID.to_string(),
    };
    let mut engine = build_engine(&cfg, &runtime_state.map_id)?;
    let ghost_mode_settings = DEFAULT_GHOST_MODE_SETTINGS;
    engine
        .set_ghost_mode_settings(ghost_mode_settings)
        .map_err(EngineWorkerError::Engine)?;
    let simulation_dt_seconds = 1.0 / cfg.simulation_hz as f32;
    let run_cfg = Arc::clone(&cfg);
    let handle = tokio::task::spawn_local(async move {
        run_worker(
            engine,
            &mut rx,
            &mut shutdown_rx,
            simulation_dt_seconds,
            weather_sync,
            run_cfg,
            runtime_state,
            ghost_mode_settings,
        )
        .await;
    });

    Ok((client, handle))
}

async fn run_worker(
    mut engine: Engine,
    rx: &mut mpsc::Receiver<EngineCommand>,
    shutdown_rx: &mut broadcast::Receiver<()>,
    simulation_dt_seconds: f32,
    weather_sync: WeatherSyncState,
    cfg: Arc<Config>,
    mut runtime_state: EngineRuntimeState,
    mut ghost_mode_settings: GhostModeSettings,
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
        if let Err(err) = apply_weather_from_schedule(&mut engine, &weather_sync).await {
            tracing::warn!(error = %err, "initial weather apply failed");
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
                if let Err(err) = engine.step(simulation_dt_seconds) {
                    tracing::warn!(error = ?err, "engine worker: tick failed");
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
                if engine.should_close_debug() {
                    tracing::info!("engine worker: debug drawer requested close");
                    break;
                }
            }

            _ = weather_tick.tick() => {
                if let Err(err) = apply_weather_from_schedule(&mut engine, &weather_sync).await {
                    tracing::warn!(error = %err, "engine worker: scheduled weather apply failed");
                }
            }

            cmd = rx.recv() => {
                let Some(cmd) = cmd else {
                    tracing::info!("engine worker: command channel closed");
                    break;
                };

                if let Err(err) = handle_command(
                    &mut engine,
                    &cfg,
                    &mut runtime_state,
                    &mut ghost_mode_settings,
                    cmd,
                ) {
                    tracing::warn!("engine worker: command failed: {err}");
                }
            }
        }
    }

    tracing::info!("engine worker: stopped");
}

fn handle_command(
    engine: &mut Engine,
    cfg: &Config,
    runtime_state: &mut EngineRuntimeState,
    ghost_mode_settings: &mut GhostModeSettings,
    cmd: EngineCommand,
) -> Result<(), EngineWorkerError> {
    match cmd {
        EngineCommand::SpawnCar { reply_tx } => {
            let result = engine.spawn_vehicle().map_err(EngineWorkerError::Engine);
            let _ = reply_tx.send(result);
            Ok(())
        }

        EngineCommand::SetControls {
            car_id,
            controls,
            reply_tx,
        } => {
            let result = engine
                .set_controls(car_id, controls)
                .map_err(EngineWorkerError::Engine);
            let _ = reply_tx.send(result);
            Ok(())
        }

        EngineCommand::ReadCarState { car_id, reply_tx } => {
            let result = engine
                .read_vehicle_state(car_id)
                .map_err(EngineWorkerError::Engine);
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::GetTrackData { reply_tx } => {
            let result = engine.track_data().map_err(EngineWorkerError::Engine);
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::GetRuntimeState { reply_tx } => {
            let _ = reply_tx.send(Ok(runtime_state.clone()));
            Ok(())
        }
        EngineCommand::SwitchRuntime {
            activity_kind,
            map_id,
            reply_tx,
        } => {
            let result = switch_runtime(
                engine,
                cfg,
                runtime_state,
                *ghost_mode_settings,
                activity_kind,
                map_id,
            );
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::SetGhostModeSettings { settings, reply_tx } => {
            let result = engine
                .set_ghost_mode_settings(settings)
                .map_err(EngineWorkerError::Engine);
            if result.is_ok() {
                *ghost_mode_settings = settings;
            }
            let _ = reply_tx.send(result);
            Ok(())
        }
        EngineCommand::DespawnCar { car_id, reply_tx } => {
            let result = engine
                .despawn_vehicle(car_id)
                .map_err(EngineWorkerError::Engine);
            let _ = reply_tx.send(result);
            Ok(())
        }
    }
}

fn switch_runtime(
    engine: &mut Engine,
    cfg: &Config,
    runtime_state: &mut EngineRuntimeState,
    ghost_mode_settings: GhostModeSettings,
    activity_kind: EngineActivityKind,
    map_id: String,
) -> Result<EngineRuntimeState, EngineWorkerError> {
    validate_map_id(&map_id)?;
    let mut new_engine = build_engine(cfg, &map_id)?;
    new_engine
        .set_ghost_mode_settings(ghost_mode_settings)
        .map_err(EngineWorkerError::Engine)?;
    *engine = new_engine;

    runtime_state.revision = runtime_state.revision.saturating_add(1);
    runtime_state.activity_kind = activity_kind;
    runtime_state.map_id = map_id;

    tracing::info!(
        revision = runtime_state.revision,
        activity_kind = ?runtime_state.activity_kind,
        map_id = %runtime_state.map_id,
        "engine worker: runtime switched"
    );

    Ok(runtime_state.clone())
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
