use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use boink::model::{PitstopZone, VehicleRaceMetrics, VehicleState};
use dashmap::DashMap;

use crate::runtime::engine_worker::EngineCommandTarget;

const PIT_HISTORY_MAX_ENTRIES: usize = 32;
const EMERGENCY_PIT_LOCK_MS: u64 = 30_000;
const TELEPORT_IDLE_WINDOW_MS: u64 = 500;

#[derive(Debug, Clone, Default)]
pub struct RuntimeCarIdentity {
    pub subject: Option<String>,
    pub team_id: Option<String>,
    pub instance_uuid: Option<String>,
    pub local_bot_index: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimePitTireType {
    #[default]
    Unspecified,
    Hard,
    Soft,
    Wet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimePitEntrySource {
    #[default]
    BotDecision,
    Requested,
    Emergency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimePitHistoryEntry {
    pub pit_time_ms: u64,
    pub lap: u32,
    pub source: RuntimePitEntrySource,
    pub new_tire_type: RuntimePitTireType,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimePitStateSnapshot {
    pub pit_request_active: bool,
    pub emergency_lock_remaining_ms: u32,
    pub force_idle: bool,
    pub last_pit_time_ms: u64,
    pub last_pit_source: RuntimePitEntrySource,
    pub last_pit_lap: u32,
    pub next_pit_tire_type: RuntimePitTireType,
    pub history: Vec<RuntimePitHistoryEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RuntimeControlInputSnapshot {
    pub input_throttle: f32,
    pub input_brake: f32,
    pub current_brake_balancer: f32,
    pub current_differential_lock: f32,
}

impl Default for RuntimeControlInputSnapshot {
    fn default() -> Self {
        Self {
            input_throttle: 0.0,
            input_brake: 0.0,
            current_brake_balancer: 0.5,
            current_differential_lock: 0.0,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RuntimePitState {
    pit_request_active: bool,
    emergency_lock_until_ms: u64,
    force_idle_until_ms: u64,
    last_pit_time_ms: u64,
    last_pit_source: RuntimePitEntrySource,
    last_pit_lap: u32,
    next_pit_tire_type: RuntimePitTireType,
    history: VecDeque<RuntimePitHistoryEntry>,
    was_fix_stationary_full_pit: bool,
    emergency_intent_pending: bool,
}

impl RuntimePitState {
    fn snapshot(&self, now_ms: u64) -> RuntimePitStateSnapshot {
        let emergency_lock_remaining_ms = if self.emergency_lock_until_ms > now_ms {
            let remaining = self.emergency_lock_until_ms - now_ms;
            remaining.min(u64::from(u32::MAX)) as u32
        } else {
            0
        };
        RuntimePitStateSnapshot {
            pit_request_active: self.pit_request_active,
            emergency_lock_remaining_ms,
            force_idle: self.force_idle_until_ms > now_ms || emergency_lock_remaining_ms > 0,
            last_pit_time_ms: self.last_pit_time_ms,
            last_pit_source: self.last_pit_source,
            last_pit_lap: self.last_pit_lap,
            next_pit_tire_type: self.next_pit_tire_type,
            history: self.history.iter().copied().collect(),
        }
    }
}

#[derive(Clone)]
pub struct RaceRuntimeStore {
    next_public_car_id: Arc<AtomicU64>,
    known_cars: Arc<DashMap<u64, ()>>,
    last_client_seq: Arc<DashMap<u64, u64>>,
    instance_cars: Arc<DashMap<String, u64>>,
    car_owners: Arc<DashMap<u64, String>>,
    car_engine_ids: Arc<DashMap<u64, u64>>,
    car_targets: Arc<DashMap<u64, EngineCommandTarget>>,
    car_identity: Arc<DashMap<u64, RuntimeCarIdentity>>,
    car_pit_state: Arc<DashMap<u64, RuntimePitState>>,
    car_controls_input: Arc<DashMap<u64, RuntimeControlInputSnapshot>>,
    local_bot_next_index: Arc<DashMap<(String, String), u32>>,
}

impl RaceRuntimeStore {
    pub fn new() -> Self {
        Self {
            next_public_car_id: Arc::new(AtomicU64::new(1)),
            known_cars: Arc::new(DashMap::new()),
            last_client_seq: Arc::new(DashMap::new()),
            instance_cars: Arc::new(DashMap::new()),
            car_owners: Arc::new(DashMap::new()),
            car_engine_ids: Arc::new(DashMap::new()),
            car_targets: Arc::new(DashMap::new()),
            car_identity: Arc::new(DashMap::new()),
            car_pit_state: Arc::new(DashMap::new()),
            car_controls_input: Arc::new(DashMap::new()),
            local_bot_next_index: Arc::new(DashMap::new()),
        }
    }

    pub fn allocate_public_car_id(&self) -> u64 {
        self.next_public_car_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn allocate_local_bot_index(&self, sandbox_id: &str, user_id: &str) -> u32 {
        let key = (sandbox_id.to_owned(), user_id.to_owned());
        let mut entry = self.local_bot_next_index.entry(key).or_insert(1);
        let index = *entry;
        *entry = entry.saturating_add(1);
        index
    }

    pub fn set_car_identity(&self, car_id: u64, identity: RuntimeCarIdentity) {
        self.car_identity.insert(car_id, identity);
    }

    pub fn car_identity(&self, car_id: u64) -> Option<RuntimeCarIdentity> {
        self.car_identity
            .get(&car_id)
            .map(|entry| entry.value().clone())
    }

    pub fn car_target(&self, car_id: u64) -> Option<EngineCommandTarget> {
        self.car_targets
            .get(&car_id)
            .map(|entry| entry.value().clone())
    }

    pub fn car_engine_id(&self, car_id: u64) -> Option<u64> {
        self.car_engine_ids.get(&car_id).map(|entry| *entry.value())
    }

    pub fn car_last_client_seq(&self, car_id: u64) -> u64 {
        self.last_client_seq
            .get(&car_id)
            .map(|entry| *entry.value())
            .unwrap_or(0)
    }

    pub fn known_car_ids(&self) -> Vec<u64> {
        let mut car_ids: Vec<u64> = self.known_cars.iter().map(|entry| *entry.key()).collect();
        car_ids.sort_unstable();
        car_ids
    }

    pub fn active_car_counts_by_sandbox(&self) -> HashMap<String, u32> {
        let mut counts = HashMap::new();
        for entry in self.car_targets.iter() {
            if let EngineCommandTarget::Sandbox { sandbox_id } = entry.value() {
                let counter = counts.entry(sandbox_id.clone()).or_insert(0u32);
                *counter = (*counter).saturating_add(1);
            }
        }
        counts
    }

    pub fn remove_car(&self, car_id: u64) {
        self.known_cars.remove(&car_id);
        self.last_client_seq.remove(&car_id);
        self.car_engine_ids.remove(&car_id);
        self.car_targets.remove(&car_id);
        self.car_identity.remove(&car_id);
        self.car_pit_state.remove(&car_id);
        self.car_controls_input.remove(&car_id);

        let Some((_, owner_instance_uuid)) = self.car_owners.remove(&car_id) else {
            return;
        };
        let should_remove_instance = self
            .instance_cars
            .get(&owner_instance_uuid)
            .map(|entry| *entry.value() == car_id)
            .unwrap_or(false);
        if should_remove_instance {
            self.instance_cars.remove(&owner_instance_uuid);
        }
    }

    pub fn set_pit_request_active(&self, car_id: u64, active: bool) {
        let mut entry = self.car_pit_state.entry(car_id).or_default();
        entry.pit_request_active = active;
    }

    pub fn set_controls_input(
        &self,
        car_id: u64,
        throttle: f32,
        brake: f32,
        brake_balancer: f32,
        differential_lock: f32,
    ) {
        self.car_controls_input.insert(
            car_id,
            RuntimeControlInputSnapshot {
                input_throttle: throttle,
                input_brake: brake,
                current_brake_balancer: brake_balancer,
                current_differential_lock: differential_lock,
            },
        );
    }

    pub fn controls_input_snapshot(&self, car_id: u64) -> RuntimeControlInputSnapshot {
        self.car_controls_input
            .get(&car_id)
            .map(|entry| *entry.value())
            .unwrap_or_default()
    }

    pub fn set_next_pit_tire_type(&self, car_id: u64, tire_type: RuntimePitTireType) {
        let mut entry = self.car_pit_state.entry(car_id).or_default();
        entry.next_pit_tire_type = tire_type;
    }

    pub fn mark_back_to_track_applied(&self, car_id: u64, now_ms: u64) {
        let mut entry = self.car_pit_state.entry(car_id).or_default();
        entry.force_idle_until_ms = entry
            .force_idle_until_ms
            .max(now_ms + TELEPORT_IDLE_WINDOW_MS);
    }

    pub fn mark_emergency_pitstop_requested(&self, car_id: u64, now_ms: u64) {
        let mut entry = self.car_pit_state.entry(car_id).or_default();
        entry.emergency_intent_pending = true;
        entry.emergency_lock_until_ms = entry
            .emergency_lock_until_ms
            .max(now_ms.saturating_add(EMERGENCY_PIT_LOCK_MS));
        entry.force_idle_until_ms = entry
            .force_idle_until_ms
            .max(now_ms + TELEPORT_IDLE_WINDOW_MS);
    }

    pub fn pit_state_snapshot(&self, car_id: u64, now_ms: u64) -> RuntimePitStateSnapshot {
        self.car_pit_state
            .get(&car_id)
            .map(|entry| entry.snapshot(now_ms))
            .unwrap_or_default()
    }

    pub fn update_pit_state_from_runtime(
        &self,
        car_id: u64,
        vehicle_state: &VehicleState,
        race_metrics: Option<&VehicleRaceMetrics>,
        now_ms: u64,
    ) -> RuntimePitStateSnapshot {
        let mut entry = self.car_pit_state.entry(car_id).or_default();
        let completed_pit = vehicle_state.pitstop_state.has_zone(PitstopZone::Fix)
            && vehicle_state.pitstop_state.wheels_in_pitstop == 4
            && vehicle_state.speed == 0.0;

        if completed_pit && !entry.was_fix_stationary_full_pit {
            let source = if entry.emergency_intent_pending {
                RuntimePitEntrySource::Emergency
            } else if entry.pit_request_active {
                RuntimePitEntrySource::Requested
            } else {
                RuntimePitEntrySource::BotDecision
            };
            let lap = race_metrics
                .map(|metrics| metrics.completed_laps)
                .unwrap_or(0);
            let history_entry = RuntimePitHistoryEntry {
                pit_time_ms: now_ms,
                lap,
                source,
                new_tire_type: entry.next_pit_tire_type,
            };
            entry.history.push_front(history_entry);
            while entry.history.len() > PIT_HISTORY_MAX_ENTRIES {
                entry.history.pop_back();
            }
            entry.last_pit_time_ms = now_ms;
            entry.last_pit_source = source;
            entry.last_pit_lap = lap;
            entry.emergency_intent_pending = false;
            entry.pit_request_active = false;
        }

        if !completed_pit {
            entry.was_fix_stationary_full_pit = false;
        } else {
            entry.was_fix_stationary_full_pit = true;
        }

        entry.snapshot(now_ms)
    }

    pub fn known_cars(&self) -> Arc<DashMap<u64, ()>> {
        Arc::clone(&self.known_cars)
    }

    pub fn last_client_seq(&self) -> Arc<DashMap<u64, u64>> {
        Arc::clone(&self.last_client_seq)
    }

    pub fn instance_cars(&self) -> Arc<DashMap<String, u64>> {
        Arc::clone(&self.instance_cars)
    }

    pub fn car_owners(&self) -> Arc<DashMap<u64, String>> {
        Arc::clone(&self.car_owners)
    }

    pub fn car_engine_ids(&self) -> Arc<DashMap<u64, u64>> {
        Arc::clone(&self.car_engine_ids)
    }

    pub fn car_targets(&self) -> Arc<DashMap<u64, EngineCommandTarget>> {
        Arc::clone(&self.car_targets)
    }

    pub fn car_identity_map(&self) -> Arc<DashMap<u64, RuntimeCarIdentity>> {
        Arc::clone(&self.car_identity)
    }
}

impl Default for RaceRuntimeStore {
    fn default() -> Self {
        Self::new()
    }
}
