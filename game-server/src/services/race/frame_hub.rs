use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use boink::error::Error as BoinkError;
use boink::model::{VehicleRaceMetrics, VehicleState};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, MissedTickBehavior};

use crate::runtime::engine_worker::{
    EngineActivityKind, EngineClient, EngineCommandTarget, EngineRuntimeState, EngineWorkerError,
};

use super::runtime_store::{RaceRuntimeStore, RuntimeCarIdentity};

#[derive(Clone, Debug)]
pub struct RuntimeCarFrame {
    pub public_car_id: u64,
    pub engine_car_id: u64,
    pub target: EngineCommandTarget,
    pub state: VehicleState,
    pub race_metrics: Option<VehicleRaceMetrics>,
    pub last_client_seq: u64,
    pub identity: Option<RuntimeCarIdentity>,
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeFrame {
    pub tick: u64,
    pub server_time_ms: u64,
    pub runtime_state: Option<EngineRuntimeState>,
    pub cars: HashMap<u64, RuntimeCarFrame>,
    pub official_race_duration_s: Option<f32>,
    pub sandbox_race_duration_s: HashMap<String, f32>,
    pub official_lap_length_m: Option<f32>,
    pub sandbox_lap_length_m: HashMap<String, f32>,
}

#[derive(Clone)]
pub struct FrameHub {
    rx: watch::Receiver<Arc<RuntimeFrame>>,
}

impl FrameHub {
    pub fn subscribe(&self) -> watch::Receiver<Arc<RuntimeFrame>> {
        self.rx.clone()
    }

    pub fn latest(&self) -> Arc<RuntimeFrame> {
        self.rx.borrow().clone()
    }
}

#[derive(Default)]
struct FrameCollectorCache {
    official_map_id: Option<String>,
    official_lap_length_m: Option<f32>,
    // sandbox_id -> (map_id, lap_length_m)
    sandbox_lap_length_m: HashMap<String, (String, f32)>,
}

pub fn spawn_frame_hub(
    engine: EngineClient,
    runtime_store: Arc<RaceRuntimeStore>,
    simulation_hz: u32,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> (FrameHub, JoinHandle<()>) {
    let (tx, rx) = watch::channel(Arc::new(RuntimeFrame::default()));
    let hub = FrameHub { rx };

    let handle = tokio::spawn(async move {
        let mut cache = FrameCollectorCache::default();
        let mut tick: u64 = 0;

        let initial = collect_frame(&engine, runtime_store.as_ref(), tick, &mut cache).await;
        tx.send_replace(Arc::new(initial));

        let mut interval =
            tokio::time::interval(Duration::from_secs_f64(1.0 / simulation_hz.max(1) as f64));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    break;
                }
                _ = interval.tick() => {
                    tick = tick.wrapping_add(1);
                    let frame = collect_frame(&engine, runtime_store.as_ref(), tick, &mut cache).await;
                    tx.send_replace(Arc::new(frame));
                }
            }
        }

        tracing::info!("race frame hub stopped");
    });

    (hub, handle)
}

async fn collect_frame(
    engine: &EngineClient,
    runtime_store: &RaceRuntimeStore,
    tick: u64,
    cache: &mut FrameCollectorCache,
) -> RuntimeFrame {
    let mut frame = RuntimeFrame {
        tick,
        server_time_ms: current_time_ms(),
        ..RuntimeFrame::default()
    };

    let runtime_state = match engine.runtime_state().await {
        Ok(state) => {
            frame.runtime_state = Some(state.clone());
            Some(state)
        }
        Err(err) => {
            tracing::warn!(error = %err, "frame hub: failed to read runtime state");
            None
        }
    };

    if let Some(runtime_state) = runtime_state.as_ref() {
        match runtime_state.activity_kind {
            EngineActivityKind::OfficialRace => {
                frame.official_race_duration_s = engine
                    .race_duration_in(EngineCommandTarget::OfficialRace)
                    .await
                    .map_err(|err| {
                        tracing::warn!(
                            error = %err,
                            "frame hub: failed to read official race duration"
                        );
                    })
                    .ok();

                if cache.official_map_id.as_deref() != Some(runtime_state.map_id.as_str()) {
                    cache.official_map_id = Some(runtime_state.map_id.clone());
                    match engine
                        .track_data_in(EngineCommandTarget::OfficialRace)
                        .await
                    {
                        Ok(track_data) => {
                            cache.official_lap_length_m =
                                Some(track_data.lap_length_m.max(1.0_f64) as f32);
                        }
                        Err(err) => {
                            tracing::warn!(
                                map_id = %runtime_state.map_id,
                                error = %err,
                                "frame hub: failed to read official lap length"
                            );
                            cache.official_lap_length_m = None;
                        }
                    }
                }
                frame.official_lap_length_m = cache.official_lap_length_m;
            }
            EngineActivityKind::Sandbox | EngineActivityKind::None => {}
        }

        let active_sandbox_map: HashMap<String, String> = runtime_state
            .active_sandboxes
            .iter()
            .map(|entry| (entry.sandbox_id.clone(), entry.map_id.clone()))
            .collect();

        cache
            .sandbox_lap_length_m
            .retain(|sandbox_id, _| active_sandbox_map.contains_key(sandbox_id));

        for (sandbox_id, map_id) in &active_sandbox_map {
            match engine
                .race_duration_in(EngineCommandTarget::Sandbox {
                    sandbox_id: sandbox_id.clone(),
                })
                .await
            {
                Ok(duration_s) => {
                    frame
                        .sandbox_race_duration_s
                        .insert(sandbox_id.clone(), duration_s);
                }
                Err(err) => {
                    tracing::warn!(
                        sandbox_id,
                        error = %err,
                        "frame hub: failed to read sandbox race duration"
                    );
                }
            }

            let should_refresh_lap_length = cache
                .sandbox_lap_length_m
                .get(sandbox_id)
                .map(|(cached_map_id, _)| cached_map_id != map_id)
                .unwrap_or(true);
            if should_refresh_lap_length {
                match engine
                    .track_data_in(EngineCommandTarget::Sandbox {
                        sandbox_id: sandbox_id.clone(),
                    })
                    .await
                {
                    Ok(track_data) => {
                        cache.sandbox_lap_length_m.insert(
                            sandbox_id.clone(),
                            (map_id.clone(), track_data.lap_length_m.max(1.0_f64) as f32),
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            sandbox_id,
                            map_id,
                            error = %err,
                            "frame hub: failed to read sandbox lap length"
                        );
                    }
                }
            }
            if let Some((_, lap_length_m)) = cache.sandbox_lap_length_m.get(sandbox_id) {
                frame
                    .sandbox_lap_length_m
                    .insert(sandbox_id.clone(), *lap_length_m);
            }
        }
    }

    for public_car_id in runtime_store.known_car_ids() {
        let Some(target) = runtime_store.car_target(public_car_id) else {
            runtime_store.remove_car(public_car_id);
            continue;
        };
        let Some(engine_car_id) = runtime_store.car_engine_id(public_car_id) else {
            runtime_store.remove_car(public_car_id);
            continue;
        };

        let state = match engine
            .read_car_state_in(target.clone(), engine_car_id)
            .await
        {
            Ok(state) => state,
            Err(EngineWorkerError::Engine(BoinkError::NotFound)) => {
                runtime_store.remove_car(public_car_id);
                continue;
            }
            Err(err) => {
                tracing::warn!(
                    public_car_id,
                    engine_car_id,
                    target = ?target,
                    error = %err,
                    "frame hub: failed to read car state"
                );
                continue;
            }
        };

        let race_metrics = if matches!(target, EngineCommandTarget::OfficialRace) {
            match engine
                .read_car_race_metrics_in(EngineCommandTarget::OfficialRace, engine_car_id)
                .await
            {
                Ok(metrics) => Some(metrics),
                Err(EngineWorkerError::Engine(BoinkError::NoData)) => {
                    Some(VehicleRaceMetrics::default())
                }
                Err(EngineWorkerError::Engine(BoinkError::NotFound)) => {
                    runtime_store.remove_car(public_car_id);
                    continue;
                }
                Err(err) => {
                    tracing::warn!(
                        public_car_id,
                        engine_car_id,
                        error = %err,
                        "frame hub: failed to read race metrics"
                    );
                    None
                }
            }
        } else {
            None
        };

        frame.cars.insert(
            public_car_id,
            RuntimeCarFrame {
                public_car_id,
                engine_car_id,
                target,
                state,
                race_metrics,
                last_client_seq: runtime_store.car_last_client_seq(public_car_id),
                identity: runtime_store.car_identity(public_car_id),
            },
        );
    }

    frame
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
