//! gRPC RaceTableQueryService implementation.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use boink::model::{PitstopZone, VehicleRaceMetrics};
use proto::race::v1::race_table_query_service_server::RaceTableQueryService;
use proto::race::v1::{
    GetRaceTableRequest, GetRaceTableResponse, LocalBotIdentity, LocalRaceParticipantIdentity,
    LocalRaceTableEntry, LocalRaceTableSnapshot, LocalRaceTableTarget, OfficialRaceTableEntry,
    OfficialRaceTableSnapshot, OfficialRaceTableTarget, RaceTableEntryStatus, RaceTableEvent,
    RaceTableSnapshot, RaceTableTarget, SandboxRaceTableEntry, SandboxRaceTableSnapshot,
    SandboxRaceTableTarget, StreamRaceTableRequest, StreamRaceTableResponse, TeamIdentity,
    race_table_snapshot, race_table_target, sandbox_race_table_entry,
};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[cfg(feature = "local")]
use crate::local::local_race_state::LocalRaceStateStore;
use crate::runtime::engine_worker::{EngineActivityKind, EngineCommandTarget, EngineRuntimeState};

use super::race::FrameHub;
use super::race::frame_hub::RuntimeFrame;
use super::race::runtime_store::{RaceRuntimeStore, RuntimeCarIdentity};
#[cfg(feature = "official")]
use super::submission::HpsTeamResolver;

const STREAM_CHANNEL_CAPACITY: usize = 1;
const SNAPSHOT_TICK_MS: u64 = 2000;
const TRAJECTORY_MAX_AGE_MS_OFFICIAL: u64 = 24 * 60 * 60 * 1000;
const GAP_DISABLED_LAPS_BEHIND_THRESHOLD: u32 = 1;
const TRAJECTORY_MIN_PROGRESS_DELTA_M: f64 = 0.1;
const TRAJECTORY_PROGRESS_EPSILON_M: f64 = 1e-3;
const TRAJECTORY_RESET_BACKTRACK_M: f64 = 5.0;
const START_PROGRESS_MOVED_EPSILON_M: f64 = 1.0;
#[cfg(feature = "official")]
const TEAM_NAME_FETCH_FAILED: &str = "failed to fetch";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ParsedRaceTableTarget {
    Sandbox { sandbox_id: String },
    OfficialRace,
    LocalRace { race_id: String },
}

#[derive(Default)]
struct OfficialRaceCache {
    session_marker: Option<String>,
    last_race_elapsed_ms: Option<u64>,
    last_active_rows: HashMap<u64, OfficialRaceTableEntry>,
    archived_dnf_rows: HashMap<u64, OfficialRaceTableEntry>,
    car_trajectories: HashMap<u64, VecDeque<TrajectorySample>>,
    last_computed_gap_ms: HashMap<u64, u32>,
    car_start_progress_m: HashMap<u64, f64>,
}

impl OfficialRaceCache {
    fn reset_runtime_progress(&mut self) {
        self.last_race_elapsed_ms = None;
        self.last_active_rows.clear();
        self.archived_dnf_rows.clear();
        self.car_trajectories.clear();
        self.last_computed_gap_ms.clear();
        self.car_start_progress_m.clear();
    }

    fn reset_if_session_changed(&mut self, marker: &str) {
        if self.session_marker.as_deref() == Some(marker) {
            return;
        }
        self.session_marker = Some(marker.to_owned());
        self.reset_runtime_progress();
    }
}

#[derive(Default)]
struct RaceTableCache {
    official: OfficialRaceCache,
}

#[derive(Clone)]
struct ActiveEntrySample {
    row: OfficialRaceTableEntry,
    completed_laps: u32,
    progress_total_m: f64,
    hide_gap_to_leader: bool,
}

#[derive(Debug, Clone, Copy)]
struct TrajectorySample {
    progress_total_m: f64,
    race_elapsed_ms: u64,
}

#[derive(Clone)]
pub struct RaceTableQueryServiceImpl {
    runtime_store: Arc<RaceRuntimeStore>,
    frame_hub: FrameHub,
    #[cfg(feature = "local")]
    local_race_state: LocalRaceStateStore,
    #[cfg(feature = "official")]
    team_resolver: Arc<HpsTeamResolver>,
    cache: Arc<Mutex<RaceTableCache>>,
}

impl RaceTableQueryServiceImpl {
    pub fn new(
        runtime_store: Arc<RaceRuntimeStore>,
        frame_hub: FrameHub,
        #[cfg(feature = "local")] local_race_state: LocalRaceStateStore,
        #[cfg(feature = "official")] team_resolver: Arc<HpsTeamResolver>,
    ) -> Self {
        Self {
            runtime_store,
            frame_hub,
            #[cfg(feature = "local")]
            local_race_state,
            #[cfg(feature = "official")]
            team_resolver,
            cache: Arc::new(Mutex::new(RaceTableCache::default())),
        }
    }

    async fn build_snapshot(
        &self,
        target: ParsedRaceTableTarget,
    ) -> Result<RaceTableSnapshot, Status> {
        ensure_target_supported(&target)?;

        let frame = self.frame_hub.latest();
        let runtime_state = frame
            .runtime_state
            .as_ref()
            .ok_or_else(|| Status::not_found("runtime state is unavailable"))?;
        ensure_target_active(runtime_state, &target)?;

        #[cfg(feature = "official")]
        let mut snapshot = match target {
            ParsedRaceTableTarget::Sandbox { sandbox_id } => {
                self.build_sandbox_snapshot(sandbox_id).await
            }
            ParsedRaceTableTarget::OfficialRace => {
                self.build_official_race_snapshot(runtime_state, &frame)
                    .await
            }
            ParsedRaceTableTarget::LocalRace { race_id } => {
                self.build_local_race_snapshot(race_id, &frame).await
            }
        }?;

        #[cfg(feature = "official")]
        self.apply_team_names(&mut snapshot).await;

        #[cfg(not(feature = "official"))]
        let snapshot = match target {
            ParsedRaceTableTarget::Sandbox { sandbox_id } => {
                self.build_sandbox_snapshot(sandbox_id).await
            }
            ParsedRaceTableTarget::OfficialRace => {
                self.build_official_race_snapshot(runtime_state, &frame)
                    .await
            }
            ParsedRaceTableTarget::LocalRace { race_id } => {
                self.build_local_race_snapshot(race_id, &frame).await
            }
        }?;

        Ok(snapshot)
    }

    async fn build_sandbox_snapshot(
        &self,
        sandbox_id: String,
    ) -> Result<RaceTableSnapshot, Status> {
        let known_cars = self.runtime_store.known_cars();
        let car_targets = self.runtime_store.car_targets();
        let mut car_ids: Vec<u64> = known_cars.iter().map(|entry| *entry.key()).collect();
        // public_car_id is monotonic and acts as join order.
        car_ids.sort_unstable();

        let mut entries = Vec::with_capacity(car_ids.len());
        for public_car_id in car_ids {
            let Some(car_target) = car_targets
                .get(&public_car_id)
                .map(|entry| entry.value().clone())
            else {
                continue;
            };
            if !matches_sandbox_target(&sandbox_id, &car_target) {
                continue;
            }

            entries.push(SandboxRaceTableEntry {
                car_id: public_car_id,
                identity: Some(identity_for_sandbox_entry(
                    public_car_id,
                    self.runtime_store.car_identity(public_car_id),
                )),
            });
        }

        Ok(RaceTableSnapshot {
            snapshot: Some(race_table_snapshot::Snapshot::Sandbox(
                SandboxRaceTableSnapshot {
                    target: Some(SandboxRaceTableTarget { sandbox_id }),
                    entries,
                },
            )),
        })
    }

    async fn build_official_race_snapshot(
        &self,
        runtime_state: &EngineRuntimeState,
        frame: &RuntimeFrame,
    ) -> Result<RaceTableSnapshot, Status> {
        let lap_length_m = f64::from(frame.official_lap_length_m.unwrap_or(1.0).max(1.0));
        let race_elapsed_ms = frame
            .official_race_duration_s
            .map(|duration_s| (duration_s.max(0.0) * 1000.0) as u64)
            .unwrap_or(0);

        let active_entries = self.collect_official_active_entries(frame, lap_length_m);

        let session_marker = format!("official-race:{}", runtime_state.map_id);
        let entries = self
            .compute_official_rows_with_cache(&session_marker, race_elapsed_ms, active_entries)
            .await;

        Ok(RaceTableSnapshot {
            snapshot: Some(race_table_snapshot::Snapshot::OfficialRace(
                OfficialRaceTableSnapshot {
                    target: Some(OfficialRaceTableTarget {}),
                    entries,
                },
            )),
        })
    }

    async fn build_local_race_snapshot(
        &self,
        race_id: String,
        frame: &RuntimeFrame,
    ) -> Result<RaceTableSnapshot, Status> {
        let lap_length_m = f64::from(
            frame
                .local_race_lap_length_m
                .get(&race_id)
                .copied()
                .unwrap_or(1.0)
                .max(1.0),
        );
        let mut entries = Vec::new();
        for car in frame.cars.values() {
            if !matches!(&car.target, EngineCommandTarget::LocalRace { race_id: car_race_id } if car_race_id == &race_id)
            {
                continue;
            }
            let Some(metrics) = car.race_metrics else {
                continue;
            };
            let identity = car
                .identity
                .as_ref()
                .map(|identity| LocalRaceParticipantIdentity {
                    display_name: identity
                        .local_race_display_name
                        .clone()
                        .unwrap_or_else(|| format!("car-{}", car.public_car_id)),
                    participant_index: identity.local_race_participant_index.unwrap_or(0),
                });
            #[cfg(feature = "local")]
            let identity = match identity {
                Some(identity) => Some(identity),
                None => {
                    self.local_race_state
                        .participant_identity(car.public_car_id)
                        .await
                }
            };
            entries.push((
                metrics.completed_laps,
                metrics.completed_laps as f64 * lap_length_m
                    + f64::from(metrics.lap_progress_m.max(0.0)),
                LocalRaceTableEntry {
                    car_id: car.public_car_id,
                    participant: identity,
                    position: 0,
                    gap_to_leader_ms: None,
                    laps_behind: 0,
                    in_pit: car.state.pitstop_state.has_zone(PitstopZone::Fix),
                    status: RaceTableEntryStatus::Active as i32,
                },
            ));
        }
        entries.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.2.car_id.cmp(&right.2.car_id))
        });
        let leader_laps = entries.first().map(|entry| entry.0).unwrap_or(0);
        let rows = entries
            .into_iter()
            .enumerate()
            .map(|(idx, (completed_laps, _progress, mut row))| {
                row.position = (idx + 1) as u32;
                row.laps_behind = leader_laps.saturating_sub(completed_laps);
                row.gap_to_leader_ms = if idx == 0 { Some(0) } else { None };
                row
            })
            .collect();

        Ok(RaceTableSnapshot {
            snapshot: Some(race_table_snapshot::Snapshot::LocalRace(
                LocalRaceTableSnapshot {
                    target: Some(LocalRaceTableTarget { race_id }),
                    entries: rows,
                },
            )),
        })
    }

    fn collect_official_active_entries(
        &self,
        frame: &RuntimeFrame,
        lap_length_m: f64,
    ) -> Vec<ActiveEntrySample> {
        let mut entries = Vec::new();
        for car in frame.cars.values() {
            if !matches!(car.target, EngineCommandTarget::OfficialRace) {
                continue;
            }
            let Some(metrics) = car.race_metrics else {
                continue;
            };

            entries.push(sample_to_official_entry(
                car.public_car_id,
                metrics,
                lap_length_m,
                car.identity.clone(),
                car.state.pitstop_state.has_zone(PitstopZone::Fix),
            ));
        }
        entries
    }

    async fn compute_official_rows_with_cache(
        &self,
        session_marker: &str,
        race_elapsed_ms: u64,
        mut active_entries: Vec<ActiveEntrySample>,
    ) -> Vec<OfficialRaceTableEntry> {
        let mut cache = self.cache.lock().await;
        let official_cache = &mut cache.official;
        official_cache.reset_if_session_changed(session_marker);

        if let Some(prev_elapsed_ms) = official_cache.last_race_elapsed_ms {
            if race_elapsed_ms + 100 < prev_elapsed_ms {
                official_cache.reset_runtime_progress();
            }
        }
        official_cache.last_race_elapsed_ms = Some(race_elapsed_ms);

        let current_active_ids: HashSet<u64> = active_entries
            .iter()
            .map(|sample| sample.row.car_id)
            .collect();
        normalize_active_progress_from_start(
            official_cache,
            &mut active_entries,
            &current_active_ids,
        );
        sort_active_entries(&mut active_entries);
        update_car_trajectories(
            official_cache,
            race_elapsed_ms,
            &active_entries,
            &current_active_ids,
        );

        let mut active_rows: Vec<OfficialRaceTableEntry> = Vec::with_capacity(active_entries.len());
        if let Some(leader) = active_entries.first().cloned() {
            let leader_trajectory = official_cache.car_trajectories.get(&leader.row.car_id);
            let leader_has_motion = leader_trajectory
                .is_some_and(|trajectory| trajectory.len() >= 2)
                && leader.progress_total_m > START_PROGRESS_MOVED_EPSILON_M;
            for sample in active_entries {
                let mut row = sample.row;
                if row.car_id == leader.row.car_id {
                    row.laps_behind = 0;
                    row.gap_to_leader_ms = Some(0);
                    active_rows.push(row);
                    continue;
                }

                let hide_gap_to_leader = sample.hide_gap_to_leader;
                row.laps_behind = leader.completed_laps.saturating_sub(sample.completed_laps);
                let gap_ms = if hide_gap_to_leader {
                    None
                } else if row.laps_behind > GAP_DISABLED_LAPS_BEHIND_THRESHOLD {
                    None
                } else if !leader_has_motion {
                    Some(0)
                } else {
                    leader_trajectory
                        .and_then(|trajectory| {
                            interpolate_leader_time_ms(trajectory, sample.progress_total_m)
                        })
                        .map(|leader_time_ms| {
                            (race_elapsed_ms as f64 - leader_time_ms)
                                .max(0.0)
                                .min(u32::MAX as f64) as u32
                        })
                        .or_else(|| {
                            official_cache
                                .last_computed_gap_ms
                                .get(&row.car_id)
                                .copied()
                        })
                };

                if hide_gap_to_leader {
                    official_cache.last_computed_gap_ms.remove(&row.car_id);
                }
                if let Some(gap_ms) = gap_ms {
                    official_cache
                        .last_computed_gap_ms
                        .insert(row.car_id, gap_ms);
                }
                row.gap_to_leader_ms = gap_ms;
                active_rows.push(row);
            }
        }

        official_cache
            .last_computed_gap_ms
            .retain(|car_id, _| current_active_ids.contains(car_id));
        official_cache
            .car_start_progress_m
            .retain(|car_id, _| current_active_ids.contains(car_id));
        for car_id in &current_active_ids {
            official_cache.archived_dnf_rows.remove(car_id);
        }

        for (car_id, row) in &official_cache.last_active_rows {
            if current_active_ids.contains(car_id) {
                continue;
            }
            official_cache
                .archived_dnf_rows
                .entry(*car_id)
                .or_insert_with(|| {
                    let mut dnf = row.clone();
                    dnf.status = RaceTableEntryStatus::Dnf as i32;
                    dnf
                });
        }

        official_cache.last_active_rows = active_rows
            .iter()
            .map(|row| (row.car_id, row.clone()))
            .collect();

        let active_count = active_rows.len();
        let mut dnf_rows: Vec<OfficialRaceTableEntry> =
            official_cache.archived_dnf_rows.values().cloned().collect();
        dnf_rows.sort_by(|left, right| left.car_id.cmp(&right.car_id));

        let mut rows = active_rows;
        rows.extend(dnf_rows);

        for (idx, row) in rows.iter_mut().enumerate() {
            row.position = (idx + 1) as u32;
            row.status = if idx < active_count {
                RaceTableEntryStatus::Active as i32
            } else {
                RaceTableEntryStatus::Dnf as i32
            };
            if idx >= active_count {
                row.in_pit = false;
            }
        }

        rows
    }

    #[cfg(feature = "official")]
    async fn apply_team_names(&self, snapshot: &mut RaceTableSnapshot) {
        let team_names = match self.team_resolver.resolve_team_names_map().await {
            Ok(value) => Some(value),
            Err(status) => {
                tracing::warn!(
                    code = ?status.code(),
                    error = %status,
                    "race table: failed to fetch team names from HPS"
                );
                None
            }
        };

        match snapshot.snapshot.as_mut() {
            Some(race_table_snapshot::Snapshot::OfficialRace(official)) => {
                for entry in &mut official.entries {
                    let Some(team) = entry.team.as_mut() else {
                        continue;
                    };
                    team.team_name = resolve_team_name(&team.team_id, team_names.as_ref());
                }
            }
            Some(race_table_snapshot::Snapshot::Sandbox(sandbox)) => {
                for entry in &mut sandbox.entries {
                    let Some(sandbox_race_table_entry::Identity::Team(team)) =
                        entry.identity.as_mut()
                    else {
                        continue;
                    };
                    team.team_name = resolve_team_name(&team.team_id, team_names.as_ref());
                }
            }
            Some(race_table_snapshot::Snapshot::LocalRace(_)) => {}
            None => {}
        }
    }
}

#[cfg(feature = "official")]
fn resolve_team_name(team_id: &str, team_names: Option<&HashMap<String, String>>) -> String {
    team_names
        .and_then(|value| value.get(team_id))
        .cloned()
        .unwrap_or_else(|| TEAM_NAME_FETCH_FAILED.to_string())
}

#[tonic::async_trait]
impl RaceTableQueryService for RaceTableQueryServiceImpl {
    type StreamRaceTableStream = ReceiverStream<Result<StreamRaceTableResponse, Status>>;

    async fn get_race_table(
        &self,
        request: Request<GetRaceTableRequest>,
    ) -> Result<Response<GetRaceTableResponse>, Status> {
        let target = parse_required_target(request.into_inner().target)?;
        let snapshot = self.build_snapshot(target).await?;
        Ok(Response::new(GetRaceTableResponse {
            snapshot: Some(snapshot),
        }))
    }

    async fn stream_race_table(
        &self,
        request: Request<StreamRaceTableRequest>,
    ) -> Result<Response<Self::StreamRaceTableStream>, Status> {
        let target = parse_required_target(request.into_inner().target)?;
        let service = self.clone();
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            let initial = match service.build_snapshot(target.clone()).await {
                Ok(snapshot) => snapshot,
                Err(status) => {
                    let _ = tx.send(Err(status)).await;
                    return;
                }
            };
            let mut last_snapshot = Some(initial.clone());
            let initial_event = StreamRaceTableResponse {
                event: Some(RaceTableEvent {
                    snapshot: Some(initial),
                }),
            };
            if tx.send(Ok(initial_event)).await.is_err() {
                return;
            }

            let mut ticker = tokio::time::interval(Duration::from_millis(SNAPSHOT_TICK_MS));
            loop {
                ticker.tick().await;
                let snapshot = match service.build_snapshot(target.clone()).await {
                    Ok(snapshot) => snapshot,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                };
                if last_snapshot.as_ref() != Some(&snapshot) {
                    last_snapshot = Some(snapshot.clone());
                    let event = StreamRaceTableResponse {
                        event: Some(RaceTableEvent {
                            snapshot: Some(snapshot),
                        }),
                    };
                    if tx.send(Ok(event)).await.is_err() {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn parse_required_target(target: Option<RaceTableTarget>) -> Result<ParsedRaceTableTarget, Status> {
    let target = target.ok_or_else(|| Status::invalid_argument("race table target is required"))?;
    match target.target {
        Some(race_table_target::Target::Sandbox(value)) => {
            let sandbox_id = value.sandbox_id.trim();
            if sandbox_id.is_empty() {
                return Err(Status::invalid_argument(
                    "race table sandbox target requires non-empty sandbox_id",
                ));
            }
            Ok(ParsedRaceTableTarget::Sandbox {
                sandbox_id: sandbox_id.to_string(),
            })
        }
        Some(race_table_target::Target::OfficialRace(_)) => Ok(ParsedRaceTableTarget::OfficialRace),
        Some(race_table_target::Target::LocalRace(value)) => {
            let race_id = value.race_id.trim();
            if race_id.is_empty() {
                return Err(Status::invalid_argument(
                    "race table local race target requires non-empty race_id",
                ));
            }
            Ok(ParsedRaceTableTarget::LocalRace {
                race_id: race_id.to_string(),
            })
        }
        None => Err(Status::invalid_argument("race table target is required")),
    }
}

fn ensure_target_supported(target: &ParsedRaceTableTarget) -> Result<(), Status> {
    if matches!(target, ParsedRaceTableTarget::OfficialRace) {
        #[cfg(all(feature = "local", not(feature = "official")))]
        {
            return Err(Status::unimplemented(
                "official race-table target is not supported by local backend mode",
            ));
        }
    }
    if matches!(target, ParsedRaceTableTarget::LocalRace { .. }) {
        #[cfg(not(feature = "local"))]
        {
            return Err(Status::unimplemented(
                "local race-table target is supported only by local backend mode",
            ));
        }
    }
    Ok(())
}

fn ensure_target_active(
    runtime_state: &EngineRuntimeState,
    target: &ParsedRaceTableTarget,
) -> Result<(), Status> {
    match target {
        ParsedRaceTableTarget::Sandbox { sandbox_id } => runtime_state
            .active_sandboxes
            .iter()
            .find(|entry| entry.sandbox_id == *sandbox_id)
            .map(|_| ())
            .ok_or_else(|| {
                Status::not_found("active sandbox session was not found for race-table target")
            }),
        ParsedRaceTableTarget::OfficialRace => {
            if !matches!(
                runtime_state.activity_kind,
                EngineActivityKind::OfficialRace
            ) {
                return Err(Status::not_found(
                    "official race session is not active for race-table target",
                ));
            }
            Ok(())
        }
        ParsedRaceTableTarget::LocalRace { race_id } => {
            if runtime_state
                .active_local_race
                .as_ref()
                .map(|race| race.race_id.as_str())
                != Some(race_id.as_str())
            {
                return Err(Status::not_found(
                    "active local race session was not found for race-table target",
                ));
            }
            Ok(())
        }
    }
}

fn matches_sandbox_target(sandbox_id: &str, car_target: &EngineCommandTarget) -> bool {
    match car_target {
        EngineCommandTarget::Sandbox {
            sandbox_id: car_sandbox_id,
        } => sandbox_id == car_sandbox_id,
        EngineCommandTarget::OfficialRace | EngineCommandTarget::LocalRace { .. } => false,
    }
}

fn identity_for_sandbox_entry(
    public_car_id: u64,
    identity: Option<RuntimeCarIdentity>,
) -> sandbox_race_table_entry::Identity {
    #[cfg(all(feature = "local", not(feature = "official")))]
    {
        return local_bot_identity(public_car_id, &identity);
    }

    #[cfg(not(all(feature = "local", not(feature = "official"))))]
    {
        if let Some(team_id) = identity.as_ref().and_then(|entry| entry.team_id.clone()) {
            return sandbox_race_table_entry::Identity::Team(TeamIdentity {
                team_id: team_id.clone(),
                team_name: team_id,
            });
        }

        return local_bot_identity(public_car_id, &identity);
    }
}

fn local_bot_identity(
    public_car_id: u64,
    identity: &Option<RuntimeCarIdentity>,
) -> sandbox_race_table_entry::Identity {
    let user_id = identity
        .as_ref()
        .and_then(|entry| entry.subject.clone())
        .unwrap_or_else(|| format!("car-{public_car_id}"));
    let bot_index = identity
        .as_ref()
        .and_then(|entry| entry.local_bot_index)
        .unwrap_or_else(|| public_car_id.min(u32::MAX as u64) as u32);
    let username = user_id.clone();
    sandbox_race_table_entry::Identity::LocalBot(LocalBotIdentity {
        user_id: user_id.clone(),
        username,
        bot_index,
    })
}

fn team_identity_for_official(identity: Option<RuntimeCarIdentity>) -> TeamIdentity {
    let team_id = identity
        .and_then(|entry| entry.team_id)
        .unwrap_or_else(|| "unknown".to_string());
    TeamIdentity {
        team_id: team_id.clone(),
        team_name: team_id,
    }
}

fn sample_to_official_entry(
    public_car_id: u64,
    metrics: VehicleRaceMetrics,
    lap_length_m: f64,
    identity: Option<RuntimeCarIdentity>,
    in_pit: bool,
) -> ActiveEntrySample {
    let lap_progress_raw_m = metrics.lap_progress_m;
    let lap_progress_m = lap_progress_raw_m.max(0.0);
    let progress_total_m = metrics.completed_laps as f64 * lap_length_m + f64::from(lap_progress_m);
    let row = OfficialRaceTableEntry {
        car_id: public_car_id,
        team: Some(team_identity_for_official(identity)),
        position: 0,
        gap_to_leader_ms: None,
        laps_behind: 0,
        in_pit,
        status: RaceTableEntryStatus::Active as i32,
    };

    ActiveEntrySample {
        row,
        completed_laps: metrics.completed_laps,
        progress_total_m,
        // Keep gap hidden for pre-start samples reported before crossing start/finish line.
        hide_gap_to_leader: lap_progress_raw_m < 0.0,
    }
}

fn sort_active_entries(entries: &mut [ActiveEntrySample]) {
    entries.sort_by(|left, right| {
        right
            .progress_total_m
            .partial_cmp(&left.progress_total_m)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.row.car_id.cmp(&right.row.car_id))
    });
}

fn normalize_active_progress_from_start(
    cache: &mut OfficialRaceCache,
    active_entries: &mut [ActiveEntrySample],
    current_active_ids: &HashSet<u64>,
) {
    cache
        .car_start_progress_m
        .retain(|car_id, _| current_active_ids.contains(car_id));

    for sample in active_entries {
        let baseline = *cache
            .car_start_progress_m
            .entry(sample.row.car_id)
            .or_insert(sample.progress_total_m);
        sample.progress_total_m = (sample.progress_total_m - baseline).max(0.0);
    }
}

fn update_car_trajectories(
    cache: &mut OfficialRaceCache,
    race_elapsed_ms: u64,
    active_entries: &[ActiveEntrySample],
    current_active_ids: &HashSet<u64>,
) {
    cache
        .car_trajectories
        .retain(|car_id, _| current_active_ids.contains(car_id));

    let min_keep_ms = race_elapsed_ms.saturating_sub(TRAJECTORY_MAX_AGE_MS_OFFICIAL);
    for sample in active_entries {
        let trajectory = cache.car_trajectories.entry(sample.row.car_id).or_default();

        if let Some(last) = trajectory.back() {
            if sample.progress_total_m + TRAJECTORY_PROGRESS_EPSILON_M < last.progress_total_m {
                let backtrack_m = last.progress_total_m - sample.progress_total_m;
                if backtrack_m >= TRAJECTORY_RESET_BACKTRACK_M {
                    trajectory.clear();
                }
            }
        }

        let should_append = match trajectory.back() {
            Some(last)
                if race_elapsed_ms <= last.race_elapsed_ms
                    || sample.progress_total_m <= last.progress_total_m =>
            {
                false
            }
            Some(last) => {
                (sample.progress_total_m - last.progress_total_m) >= TRAJECTORY_MIN_PROGRESS_DELTA_M
            }
            None => true,
        };

        if should_append {
            trajectory.push_back(TrajectorySample {
                progress_total_m: sample.progress_total_m,
                race_elapsed_ms,
            });
        }

        while trajectory
            .front()
            .is_some_and(|point| point.race_elapsed_ms < min_keep_ms && trajectory.len() > 1)
        {
            trajectory.pop_front();
        }
    }
}

fn interpolate_leader_time_ms(
    trajectory: &VecDeque<TrajectorySample>,
    progress_total_m: f64,
) -> Option<f64> {
    if trajectory.is_empty() {
        return None;
    }
    let first = trajectory.front()?;
    let last = trajectory.back()?;
    if progress_total_m < first.progress_total_m || progress_total_m > last.progress_total_m {
        return None;
    }

    let mut lo = 0usize;
    let mut hi = trajectory.len() - 1;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_progress = trajectory.get(mid)?.progress_total_m;
        if mid_progress < progress_total_m {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    let upper_idx = lo;
    let upper = trajectory.get(upper_idx)?;
    if (upper.progress_total_m - progress_total_m).abs() <= TRAJECTORY_PROGRESS_EPSILON_M {
        return Some(upper.race_elapsed_ms as f64);
    }
    if upper_idx == 0 {
        return Some(upper.race_elapsed_ms as f64);
    }

    let lower = trajectory.get(upper_idx - 1)?;
    let delta_progress = upper.progress_total_m - lower.progress_total_m;
    if delta_progress <= TRAJECTORY_PROGRESS_EPSILON_M {
        return Some(upper.race_elapsed_ms as f64);
    }
    let ratio = ((progress_total_m - lower.progress_total_m) / delta_progress).clamp(0.0, 1.0);
    let delta_time_ms = upper.race_elapsed_ms.saturating_sub(lower.race_elapsed_ms);
    Some(lower.race_elapsed_ms as f64 + ratio * delta_time_ms as f64)
}
