//! In-memory state for the active standalone local race.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use proto::race::v1::{LocalRaceParticipantIdentity, LocalRacePhase, LocalRaceRuntimeInfo};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct LocalRaceParticipantRecord {
    pub car_id: u64,
    pub display_name: String,
    pub participant_index: u32,
}

#[derive(Debug, Default)]
struct LocalRaceState {
    active: Option<LocalRaceRuntimeInfo>,
    participants: HashMap<u64, LocalRaceParticipantRecord>,
    next_participant_index: u32,
    finish_soft_freeze_applied: bool,
}

#[derive(Debug, Clone, Default)]
pub struct LocalRaceStateStore {
    state: Arc<RwLock<LocalRaceState>>,
}

impl LocalRaceStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn active_race(&self) -> Option<LocalRaceRuntimeInfo> {
        let state = self.state.read().await;
        state.active.as_ref().map(|race| {
            let mut race = normalize_race_phase(race.clone());
            race.joined_participant_count = joined_count_for_race(&state, &race.race_id);
            race
        })
    }

    pub async fn set_active_race(&self, race: LocalRaceRuntimeInfo) {
        let mut state = self.state.write().await;
        state.active = Some(normalize_race_phase(race));
        state.participants.clear();
        state.next_participant_index = 1;
        state.finish_soft_freeze_applied = false;
    }

    pub async fn update_active_race(
        &self,
        race_id: &str,
        update: impl FnOnce(&mut LocalRaceRuntimeInfo),
    ) -> Result<LocalRaceRuntimeInfo, LocalRaceStateError> {
        let mut state = self.state.write().await;
        let race = state
            .active
            .as_mut()
            .ok_or(LocalRaceStateError::NoActiveRace)?;
        if race.race_id != race_id {
            return Err(LocalRaceStateError::RaceMismatch);
        }
        *race = normalize_race_phase(race.clone());
        update(race);
        *race = normalize_race_phase(race.clone());
        let mut race = race.clone();
        race.joined_participant_count = joined_count_for_race(&state, &race.race_id);
        Ok(race)
    }

    pub async fn clear_active_race(&self, race_id: &str) -> Result<(), LocalRaceStateError> {
        let mut state = self.state.write().await;
        let race = state
            .active
            .as_ref()
            .ok_or(LocalRaceStateError::NoActiveRace)?;
        if race.race_id != race_id {
            return Err(LocalRaceStateError::RaceMismatch);
        }
        state.active = None;
        state.participants.clear();
        state.next_participant_index = 1;
        state.finish_soft_freeze_applied = false;
        Ok(())
    }

    pub async fn register_participant(
        &self,
        race_id: &str,
        car_id: u64,
        display_name: String,
    ) -> Result<LocalRaceParticipantRecord, LocalRaceStateError> {
        let mut state = self.state.write().await;
        let race = state
            .active
            .as_mut()
            .ok_or(LocalRaceStateError::NoActiveRace)?;
        if race.race_id != race_id {
            return Err(LocalRaceStateError::RaceMismatch);
        }
        *race = normalize_race_phase(race.clone());
        if LocalRacePhase::try_from(race.phase).unwrap_or(LocalRacePhase::Unspecified)
            != LocalRacePhase::Staging
        {
            return Err(LocalRaceStateError::JoinClosed);
        }
        let max_participants = race.max_participants;
        let joined = state.participants.len().min(u32::MAX as usize) as u32;
        if max_participants > 0 && joined >= max_participants {
            return Err(LocalRaceStateError::ParticipantLimitReached);
        }

        let participant_index = state.next_participant_index.max(1);
        state.next_participant_index = participant_index.saturating_add(1);
        let record = LocalRaceParticipantRecord {
            car_id,
            display_name,
            participant_index,
        };
        state.participants.insert(car_id, record.clone());
        let joined_after = state.participants.len().min(u32::MAX as usize) as u32;
        if let Some(race) = state.active.as_mut() {
            race.joined_participant_count = joined_after;
        }
        Ok(record)
    }

    pub async fn participant_identity(&self, car_id: u64) -> Option<LocalRaceParticipantIdentity> {
        let state = self.state.read().await;
        state
            .participants
            .get(&car_id)
            .map(|participant| LocalRaceParticipantIdentity {
                display_name: participant.display_name.clone(),
                participant_index: participant.participant_index,
            })
    }

    pub async fn remove_participant(&self, car_id: u64) {
        let mut state = self.state.write().await;
        state.participants.remove(&car_id);
        let joined_after = state.participants.len().min(u32::MAX as usize) as u32;
        if let Some(race) = state.active.as_mut() {
            race.joined_participant_count = joined_after;
        }
    }

    pub async fn reconcile_active_race(&self, now_ms: u64) -> Option<LocalRaceReconcileOutcome> {
        let mut state = self.state.write().await;
        let race = state.active.as_mut()?;
        let previous_phase = race_phase(race.phase);
        let normalized = normalize_race_phase_at(race.clone(), now_ms);
        if *race == normalized {
            return None;
        }

        let current_phase = race_phase(normalized.phase);
        *race = normalized.clone();
        if current_phase == LocalRacePhase::Finished {
            state.finish_soft_freeze_applied = false;
        }

        let mut race = normalized;
        race.joined_participant_count = joined_count_for_race(&state, &race.race_id);
        Some(LocalRaceReconcileOutcome {
            race,
            previous_phase,
            current_phase,
        })
    }

    pub async fn finished_race_pending_soft_freeze(&self) -> Option<LocalRaceRuntimeInfo> {
        let state = self.state.read().await;
        let race = state.active.as_ref()?;
        if state.finish_soft_freeze_applied {
            return None;
        }
        let race = normalize_race_phase(race.clone());
        if race_phase(race.phase) != LocalRacePhase::Finished {
            return None;
        }
        let mut race = race;
        race.joined_participant_count = joined_count_for_race(&state, &race.race_id);
        Some(race)
    }

    pub async fn mark_finish_soft_freeze_applied(
        &self,
        race_id: &str,
    ) -> Result<(), LocalRaceStateError> {
        let mut state = self.state.write().await;
        let race = state
            .active
            .as_ref()
            .ok_or(LocalRaceStateError::NoActiveRace)?;
        if race.race_id != race_id {
            return Err(LocalRaceStateError::RaceMismatch);
        }
        state.finish_soft_freeze_applied = true;
        Ok(())
    }

    pub async fn gameplay_commands_closed(&self, race_id: &str) -> bool {
        let state = self.state.read().await;
        let Some(race) = state.active.as_ref() else {
            return false;
        };
        if race.race_id != race_id {
            return false;
        }
        race_phase(normalize_race_phase(race.clone()).phase) != LocalRacePhase::Running
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalRaceStateError {
    NoActiveRace,
    RaceMismatch,
    JoinClosed,
    ParticipantLimitReached,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocalRaceReconcileOutcome {
    pub race: LocalRaceRuntimeInfo,
    pub previous_phase: LocalRacePhase,
    pub current_phase: LocalRacePhase,
}

fn joined_count_for_race(state: &LocalRaceState, race_id: &str) -> u32 {
    if state.active.as_ref().map(|race| race.race_id.as_str()) != Some(race_id) {
        return 0;
    }
    state.participants.len().min(u32::MAX as usize) as u32
}

fn normalize_race_phase(race: LocalRaceRuntimeInfo) -> LocalRaceRuntimeInfo {
    normalize_race_phase_at(race, current_unix_ms())
}

fn normalize_race_phase_at(mut race: LocalRaceRuntimeInfo, now_ms: u64) -> LocalRaceRuntimeInfo {
    match LocalRacePhase::try_from(race.phase).unwrap_or(LocalRacePhase::Unspecified) {
        LocalRacePhase::Countdown => {
            if timestamp_ms(race.countdown_end_at_utc.as_ref()).is_some_and(|end| now_ms >= end) {
                race.phase = LocalRacePhase::Running as i32;
                if race.running_started_at_utc.is_none() {
                    race.running_started_at_utc = race.countdown_end_at_utc.clone();
                }
                if race.planned_end_at_utc.is_none() {
                    let started =
                        timestamp_ms(race.running_started_at_utc.as_ref()).unwrap_or(now_ms);
                    race.planned_end_at_utc = Some(timestamp_from_ms(
                        started.saturating_add(u64::from(race.race_duration_sec) * 1_000),
                    ));
                }
            }
        }
        LocalRacePhase::Running => {
            if timestamp_ms(race.planned_end_at_utc.as_ref()).is_some_and(|end| now_ms >= end) {
                race.phase = LocalRacePhase::Finished as i32;
            }
        }
        _ => {}
    }
    race
}

fn race_phase(raw: i32) -> LocalRacePhase {
    LocalRacePhase::try_from(raw).unwrap_or(LocalRacePhase::Unspecified)
}

pub fn timestamp_from_ms(ms: u64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: (ms / 1_000).min(i64::MAX as u64) as i64,
        nanos: ((ms % 1_000) * 1_000_000) as i32,
    }
}

pub fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn timestamp_ms(value: Option<&prost_types::Timestamp>) -> Option<u64> {
    let value = value?;
    let seconds = u64::try_from(value.seconds).ok()?;
    let nanos = u32::try_from(value.nanos).ok()?;
    Some(
        seconds
            .saturating_mul(1_000)
            .saturating_add(u64::from(nanos / 1_000_000)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_race(phase: LocalRacePhase, now_ms: u64) -> LocalRaceRuntimeInfo {
        LocalRaceRuntimeInfo {
            race_id: "race-1".to_string(),
            race_name: "Race".to_string(),
            map_id: "map".to_string(),
            race_duration_sec: 10,
            time_of_day: None,
            active_time_of_day_preset: 0,
            ghost_mode: None,
            weather: None,
            phase: phase as i32,
            created_at_utc: Some(timestamp_from_ms(now_ms.saturating_sub(5_000))),
            countdown_end_at_utc: None,
            running_started_at_utc: None,
            planned_end_at_utc: None,
            joined_participant_count: 0,
            max_participants: 4,
        }
    }

    #[tokio::test]
    async fn reconcile_transitions_countdown_to_running() {
        let store = LocalRaceStateStore::new();
        let now_ms = current_unix_ms().saturating_add(10_000);
        let mut race = sample_race(LocalRacePhase::Countdown, now_ms);
        race.countdown_end_at_utc = Some(timestamp_from_ms(now_ms.saturating_sub(1)));
        store.set_active_race(race).await;

        let outcome = store
            .reconcile_active_race(now_ms)
            .await
            .expect("countdown should transition");

        assert_eq!(outcome.previous_phase, LocalRacePhase::Countdown);
        assert_eq!(outcome.current_phase, LocalRacePhase::Running);
        assert_eq!(race_phase(outcome.race.phase), LocalRacePhase::Running);
        assert_eq!(
            timestamp_ms(outcome.race.running_started_at_utc.as_ref()),
            Some(now_ms.saturating_sub(1))
        );
        assert_eq!(
            timestamp_ms(outcome.race.planned_end_at_utc.as_ref()),
            Some(now_ms.saturating_sub(1).saturating_add(10_000))
        );
    }

    #[tokio::test]
    async fn reconcile_transitions_running_to_finished() {
        let store = LocalRaceStateStore::new();
        let now_ms = current_unix_ms().saturating_add(10_000);
        let mut race = sample_race(LocalRacePhase::Running, now_ms);
        race.running_started_at_utc = Some(timestamp_from_ms(now_ms.saturating_sub(11_000)));
        race.planned_end_at_utc = Some(timestamp_from_ms(now_ms.saturating_sub(1)));
        store.set_active_race(race).await;

        let outcome = store
            .reconcile_active_race(now_ms)
            .await
            .expect("running should transition");

        assert_eq!(outcome.previous_phase, LocalRacePhase::Running);
        assert_eq!(outcome.current_phase, LocalRacePhase::Finished);
        assert!(store.finished_race_pending_soft_freeze().await.is_some());
        assert!(store.gameplay_commands_closed("race-1").await);
    }

    #[tokio::test]
    async fn finished_race_stays_active_until_cleared() {
        let store = LocalRaceStateStore::new();
        let now_ms = current_unix_ms().saturating_add(10_000);
        let mut race = sample_race(LocalRacePhase::Running, now_ms);
        race.running_started_at_utc = Some(timestamp_from_ms(now_ms.saturating_sub(11_000)));
        race.planned_end_at_utc = Some(timestamp_from_ms(now_ms.saturating_sub(1)));
        store.set_active_race(race).await;

        let _ = store.reconcile_active_race(now_ms).await;
        let active = store.active_race().await.expect("race should stay active");
        assert_eq!(race_phase(active.phase), LocalRacePhase::Finished);

        store
            .clear_active_race("race-1")
            .await
            .expect("finished race should clear");
        assert!(store.active_race().await.is_none());
    }
}
