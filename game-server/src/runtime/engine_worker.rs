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

use boink::engine::Engine;
use boink::engine::EngineBuilder;
use boink::error::Error as BoinkError;
use boink::model::control::Controls;
use boink::model::math::Vec3;
use boink::model::state::CarState;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::config::Config;

use super::commands::EngineCommand;

/// Maximum number of queued commands. Keep small enough to apply backpressure.
const COMMAND_QUEUE_CAPACITY: usize = 256;
const SIMULATION_HZ: f64 = 60.0;
const SIMULATION_DT_SECONDS: f64 = 1.0 / SIMULATION_HZ;

/// Errors returned by the engine worker boundary.
///
/// This is the stable surface for API layers; map it to `tonic::Status` in services.
#[derive(Debug)]
pub enum EngineWorkerError {
    /// The worker task/channel is no longer available.
    WorkerStopped,
    /// An error occurred inside the Boink engine wrapper.
    Engine(BoinkError),
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
        }
    }
}

impl std::error::Error for EngineWorkerError {}

impl EngineClient {
    /// Spawns a car and returns its engine-assigned ID.
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

    /// Reads the latest state for a given car.
    pub async fn read_car_state(&self, car_id: u64) -> Result<CarState, EngineWorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EngineCommand::ReadCarState { car_id, reply_tx })
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
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<(EngineClient, JoinHandle<()>), EngineWorkerError> {
    let (tx, mut rx) = mpsc::channel::<EngineCommand>(COMMAND_QUEUE_CAPACITY);
    let client = EngineClient { tx };

    let engine = build_engine(&cfg)?;
    let handle = tokio::task::spawn_local(async move {
        run_worker(engine, &mut rx, &mut shutdown_rx).await;
    });

    Ok((client, handle))
}

async fn run_worker(
    mut engine: Engine,
    rx: &mut mpsc::Receiver<EngineCommand>,
    shutdown_rx: &mut broadcast::Receiver<()>,
) {
    let mut ticker =
        tokio::time::interval(tokio::time::Duration::from_secs_f64(SIMULATION_DT_SECONDS));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                tracing::info!("engine worker: shutdown broadcast received");
                break;
            }

            _ = ticker.tick() => {
                if let Err(err) = engine.step(SIMULATION_DT_SECONDS) {
                    tracing::warn!(error = ?err, "engine worker: tick failed");
                }
            }

            cmd = rx.recv() => {
                let Some(cmd) = cmd else {
                    tracing::info!("engine worker: command channel closed");
                    break;
                };

                if let Err(err) = handle_command(&mut engine, cmd) {
                    tracing::warn!("engine worker: command failed: {err}");
                }
            }
        }
    }

    tracing::info!("engine worker: stopped");
}

fn handle_command(engine: &mut Engine, cmd: EngineCommand) -> Result<(), EngineWorkerError> {
    match cmd {
        EngineCommand::SpawnCar { reply_tx } => {
            let result = engine.spawn_car().map_err(EngineWorkerError::Engine);
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
                .read_car_state(car_id)
                .map_err(EngineWorkerError::Engine);
            let _ = reply_tx.send(result);
            Ok(())
        }
    }
}

/// Builds the engine instance using server configuration defaults.
fn build_engine(cfg: &Config) -> Result<Engine, EngineWorkerError> {
    tracing::info!(env = ?cfg.env, "initializing engine world");

    let (front_left, front_right, rear_left, rear_right) = default_car_layout();
    let builder = EngineBuilder::new(front_left, front_right, rear_left, rear_right, 30.0);
    let builder = builder.with_start_time_seconds(0.0);

    builder.build().map_err(EngineWorkerError::Engine)
}

/// Default wheel layout used for new engine instances.
fn default_car_layout() -> (Vec3, Vec3, Vec3, Vec3) {
    let front_left = Vec3 {
        x: 1.25,
        y: 0.0,
        z: 0.75,
    };
    let front_right = Vec3 {
        x: 1.25,
        y: 0.0,
        z: -0.75,
    };
    let rear_left = Vec3 {
        x: -1.35,
        y: 0.0,
        z: 0.75,
    };
    let rear_right = Vec3 {
        x: -1.35,
        y: 0.0,
        z: -0.75,
    };

    (front_left, front_right, rear_left, rear_right)
}
