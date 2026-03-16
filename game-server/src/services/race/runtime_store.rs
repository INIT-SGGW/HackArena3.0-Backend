use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::runtime::engine_worker::EngineCommandTarget;

#[derive(Debug, Clone, Default)]
pub struct RuntimeCarIdentity {
    pub subject: Option<String>,
    pub team_id: Option<String>,
    pub instance_uuid: Option<String>,
    pub local_bot_index: Option<u32>,
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
