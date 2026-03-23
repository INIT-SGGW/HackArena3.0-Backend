//! gRPC AchievementStreamService implementation.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use boink::model::{
    Gear, Quaternion as EngineQuaternion, TyreType as EngineTyreType, Vec3 as EngineVec3,
};
use proto::achievement::v1::achievement_runtime_source;
use proto::achievement::v1::achievement_stream_service_server::AchievementStreamService;
use proto::achievement::v1::stream_achievement_runtime_response;
use proto::achievement::v1::{
    AchievementOfficialRaceSource, AchievementOfficialSandboxSource, AchievementRuntimeBootstrap,
    AchievementRuntimeSnapshot, AchievementRuntimeSource, AchievementRuntimeSourceDescriptor,
    AchievementRuntimeTopologyUpdate, AchievementSourceSnapshot, AchievementStreamClampReason,
    AchievementStreamSettings, AchievementTireType, AchievementTireWearPerWheel,
    AchievementVehicleState, AchievementWheelPositions, StreamAchievementRuntimeRequest,
    StreamAchievementRuntimeResponse,
};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::runtime::engine_worker::{EngineActivityKind, EngineClient, EngineCommandTarget};
use crate::services::mappers::{track_data_to_proto, vec3_to_proto};

use super::race::frame_hub::RuntimeCarFrame;
use super::race::{FrameHub, RuntimeFrame};

const STREAM_CHANNEL_CAPACITY: usize = 4;
const MIN_STREAM_HZ: u32 = 1;
const MAX_STREAM_HZ: u32 = 120;
const VECTOR_EPSILON: f32 = 1e-6;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum AchievementSourceKey {
    OfficialRace,
    OfficialSandbox(String),
}

impl AchievementSourceKey {
    fn to_proto(&self) -> AchievementRuntimeSource {
        match self {
            AchievementSourceKey::OfficialRace => AchievementRuntimeSource {
                source: Some(achievement_runtime_source::Source::OfficialRace(
                    AchievementOfficialRaceSource {},
                )),
            },
            AchievementSourceKey::OfficialSandbox(sandbox_id) => AchievementRuntimeSource {
                source: Some(achievement_runtime_source::Source::OfficialSandbox(
                    AchievementOfficialSandboxSource {
                        sandbox_id: sandbox_id.clone(),
                    },
                )),
            },
        }
    }

    fn to_engine_target(&self) -> EngineCommandTarget {
        match self {
            AchievementSourceKey::OfficialRace => EngineCommandTarget::OfficialRace,
            AchievementSourceKey::OfficialSandbox(sandbox_id) => EngineCommandTarget::Sandbox {
                sandbox_id: sandbox_id.clone(),
            },
        }
    }
}

#[derive(Clone)]
struct ActiveSource {
    key: AchievementSourceKey,
    map_id: String,
}

#[derive(Clone, Copy)]
struct CenterlineGeometrySample {
    position: EngineVec3,
    tangent: EngineVec3,
}

#[derive(Clone)]
struct SourceDescriptorCacheEntry {
    map_id: String,
    track_data: proto::race::v1::TrackData,
    centerline_geometry: Arc<Vec<CenterlineGeometrySample>>,
}

#[derive(Clone)]
struct ResolvedSourceDescriptor {
    key: AchievementSourceKey,
    map_id: String,
    descriptor: AchievementRuntimeSourceDescriptor,
    centerline_geometry: Arc<Vec<CenterlineGeometrySample>>,
}

#[derive(Clone)]
pub struct AchievementStreamServiceImpl {
    engine: EngineClient,
    frame_hub: FrameHub,
    simulation_hz: u32,
    descriptor_cache: Arc<Mutex<HashMap<AchievementSourceKey, SourceDescriptorCacheEntry>>>,
}

impl AchievementStreamServiceImpl {
    pub fn new(engine: EngineClient, frame_hub: FrameHub, simulation_hz: u32) -> Self {
        Self {
            engine,
            frame_hub,
            simulation_hz,
            descriptor_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn collect_active_sources(frame: &RuntimeFrame) -> Vec<ActiveSource> {
        let Some(runtime_state) = frame.runtime_state.as_ref() else {
            return Vec::new();
        };

        let mut sources = Vec::new();
        if matches!(
            runtime_state.activity_kind,
            EngineActivityKind::OfficialRace
        ) {
            sources.push(ActiveSource {
                key: AchievementSourceKey::OfficialRace,
                map_id: runtime_state.map_id.clone(),
            });
        }

        for sandbox in &runtime_state.active_sandboxes {
            sources.push(ActiveSource {
                key: AchievementSourceKey::OfficialSandbox(sandbox.sandbox_id.clone()),
                map_id: sandbox.map_id.clone(),
            });
        }

        sources.sort_by(|left, right| left.key.cmp(&right.key));
        sources
    }

    async fn resolve_descriptor(&self, source: &ActiveSource) -> Option<ResolvedSourceDescriptor> {
        if let Some(cached) = self.cached_descriptor(source).await {
            return Some(cached);
        }

        let mut track_data = match self
            .engine
            .track_data_in(source.key.to_engine_target())
            .await
        {
            Ok(track_data) => track_data,
            Err(err) => {
                tracing::warn!(
                    source = ?source.key,
                    map_id = %source.map_id,
                    error = %err,
                    "achievement stream: failed to read track data for source"
                );
                return None;
            }
        };

        track_data.map_id = source.map_id.clone();
        let centerline_geometry = Arc::new(
            track_data
                .centerline_samples
                .iter()
                .map(|sample| CenterlineGeometrySample {
                    position: sample.position,
                    tangent: sample.tangent,
                })
                .collect(),
        );
        let entry = SourceDescriptorCacheEntry {
            map_id: source.map_id.clone(),
            track_data: track_data_to_proto(track_data),
            centerline_geometry: Arc::clone(&centerline_geometry),
        };

        {
            let mut cache = self.descriptor_cache.lock().await;
            cache.insert(source.key.clone(), entry.clone());
        }

        Some(ResolvedSourceDescriptor {
            key: source.key.clone(),
            map_id: source.map_id.clone(),
            descriptor: AchievementRuntimeSourceDescriptor {
                source: Some(source.key.to_proto()),
                map_id: source.map_id.clone(),
                track_data: Some(entry.track_data),
            },
            centerline_geometry,
        })
    }

    async fn cached_descriptor(&self, source: &ActiveSource) -> Option<ResolvedSourceDescriptor> {
        let cache = self.descriptor_cache.lock().await;
        let cached = cache.get(&source.key)?;
        if cached.map_id != source.map_id {
            return None;
        }

        Some(ResolvedSourceDescriptor {
            key: source.key.clone(),
            map_id: source.map_id.clone(),
            descriptor: AchievementRuntimeSourceDescriptor {
                source: Some(source.key.to_proto()),
                map_id: source.map_id.clone(),
                track_data: Some(cached.track_data.clone()),
            },
            centerline_geometry: Arc::clone(&cached.centerline_geometry),
        })
    }

    async fn resolve_active_descriptors(
        &self,
        frame: &RuntimeFrame,
    ) -> Vec<ResolvedSourceDescriptor> {
        let active_sources = Self::collect_active_sources(frame);
        let mut resolved = Vec::with_capacity(active_sources.len());
        for source in &active_sources {
            if let Some(descriptor) = self.resolve_descriptor(source).await {
                resolved.push(descriptor);
            }
        }
        resolved.sort_by(|left, right| left.key.cmp(&right.key));
        resolved
    }
}

#[tonic::async_trait]
impl AchievementStreamService for AchievementStreamServiceImpl {
    type StreamAchievementRuntimeStream =
        ReceiverStream<Result<StreamAchievementRuntimeResponse, Status>>;

    async fn stream_achievement_runtime(
        &self,
        request: Request<StreamAchievementRuntimeRequest>,
    ) -> Result<Response<Self::StreamAchievementRuntimeStream>, Status> {
        let req = request.into_inner();
        if req.requested_hz == 0 {
            return Err(Status::invalid_argument(
                "requested_hz must be >= 1 for achievement stream",
            ));
        }
        let service = self.clone();
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            run_stream(service, req.requested_hz, tx).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn run_stream(
    service: AchievementStreamServiceImpl,
    requested_hz_raw: u32,
    tx: mpsc::Sender<Result<StreamAchievementRuntimeResponse, Status>>,
) {
    let (requested_hz, effective_hz, clamp_reason, period) =
        resolve_stream_rate(requested_hz_raw, service.simulation_hz);

    if tx
        .send(Ok(StreamAchievementRuntimeResponse {
            payload: Some(stream_achievement_runtime_response::Payload::Settings(
                AchievementStreamSettings {
                    requested_hz,
                    effective_hz,
                    clamp_reason: clamp_reason as i32,
                },
            )),
        }))
        .await
        .is_err()
    {
        return;
    }

    let initial_frame = service.frame_hub.latest();
    let initial_descriptors = service.resolve_active_descriptors(&initial_frame).await;
    let mut announced_sources: HashMap<AchievementSourceKey, String> = initial_descriptors
        .iter()
        .map(|entry| (entry.key.clone(), entry.map_id.clone()))
        .collect();

    if tx
        .send(Ok(StreamAchievementRuntimeResponse {
            payload: Some(stream_achievement_runtime_response::Payload::Bootstrap(
                AchievementRuntimeBootstrap {
                    sources: initial_descriptors
                        .iter()
                        .map(|entry| entry.descriptor.clone())
                        .collect(),
                },
            )),
        }))
        .await
        .is_err()
    {
        return;
    }

    let initial_snapshot = build_snapshot(&initial_frame, &initial_descriptors);
    if tx
        .send(Ok(StreamAchievementRuntimeResponse {
            payload: Some(stream_achievement_runtime_response::Payload::Snapshot(
                initial_snapshot,
            )),
        }))
        .await
        .is_err()
    {
        return;
    }

    let mut ticker = tokio::time::interval(period);
    loop {
        ticker.tick().await;

        let frame = service.frame_hub.latest();
        let descriptors = service.resolve_active_descriptors(&frame).await;
        let current_sources: HashMap<AchievementSourceKey, String> = descriptors
            .iter()
            .map(|entry| (entry.key.clone(), entry.map_id.clone()))
            .collect();

        let upserted_sources: Vec<AchievementRuntimeSourceDescriptor> = descriptors
            .iter()
            .filter_map(|entry| match announced_sources.get(&entry.key) {
                Some(previous_map_id) if previous_map_id == &entry.map_id => None,
                _ => Some(entry.descriptor.clone()),
            })
            .collect();

        if !upserted_sources.is_empty()
            && tx
                .send(Ok(StreamAchievementRuntimeResponse {
                    payload: Some(
                        stream_achievement_runtime_response::Payload::TopologyUpdate(
                            AchievementRuntimeTopologyUpdate { upserted_sources },
                        ),
                    ),
                }))
                .await
                .is_err()
        {
            break;
        }
        announced_sources = current_sources;

        let snapshot = build_snapshot(&frame, &descriptors);
        if tx
            .send(Ok(StreamAchievementRuntimeResponse {
                payload: Some(stream_achievement_runtime_response::Payload::Snapshot(
                    snapshot,
                )),
            }))
            .await
            .is_err()
        {
            break;
        }
    }
}

fn resolve_stream_rate(
    requested_hz_raw: u32,
    simulation_hz: u32,
) -> (u32, u32, AchievementStreamClampReason, Duration) {
    let requested_hz = requested_hz_raw;
    let max_hz = MAX_STREAM_HZ.min(simulation_hz.max(1));
    let effective_hz = requested_hz.clamp(MIN_STREAM_HZ, max_hz);
    let clamp_reason = if effective_hz == requested_hz {
        AchievementStreamClampReason::None
    } else {
        AchievementStreamClampReason::ServerLimit
    };
    let period = Duration::from_secs_f64(1.0 / effective_hz as f64);
    (requested_hz, effective_hz, clamp_reason, period)
}

fn build_snapshot(
    frame: &RuntimeFrame,
    descriptors: &[ResolvedSourceDescriptor],
) -> AchievementRuntimeSnapshot {
    let sources = descriptors
        .iter()
        .map(|descriptor| AchievementSourceSnapshot {
            source: Some(descriptor.key.to_proto()),
            vehicles: build_source_vehicles(
                frame,
                &descriptor.key,
                descriptor.centerline_geometry.as_slice(),
            ),
        })
        .collect();

    AchievementRuntimeSnapshot {
        tick: frame.tick,
        server_time_ms: frame.server_time_ms,
        sources,
    }
}

fn build_source_vehicles(
    frame: &RuntimeFrame,
    source: &AchievementSourceKey,
    centerline_geometry: &[CenterlineGeometrySample],
) -> Vec<AchievementVehicleState> {
    let mut cars: Vec<RuntimeCarFrame> = frame
        .cars
        .values()
        .filter(|entry| source_matches_target(source, &entry.target))
        .cloned()
        .collect();

    cars.sort_by(|left, right| {
        let left_completed_laps = left
            .race_metrics
            .map_or(0, |metrics| metrics.completed_laps);
        let right_completed_laps = right
            .race_metrics
            .map_or(0, |metrics| metrics.completed_laps);
        let left_lap_progress = left
            .race_metrics
            .map_or(0.0_f32, |metrics| metrics.lap_progress_m);
        let right_lap_progress = right
            .race_metrics
            .map_or(0.0_f32, |metrics| metrics.lap_progress_m);

        right_completed_laps
            .cmp(&left_completed_laps)
            .then_with(|| {
                right_lap_progress
                    .partial_cmp(&left_lap_progress)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.public_car_id.cmp(&right.public_car_id))
    });

    cars.into_iter()
        .enumerate()
        .map(|(idx, car)| vehicle_state_for_achievement(car, (idx + 1) as u32, centerline_geometry))
        .collect()
}

fn vehicle_state_for_achievement(
    car: RuntimeCarFrame,
    race_position: u32,
    centerline_geometry: &[CenterlineGeometrySample],
) -> AchievementVehicleState {
    let completed_laps = car.race_metrics.map_or(0, |metrics| metrics.completed_laps);
    let team_id = car
        .identity
        .as_ref()
        .and_then(|identity| identity.team_id.clone())
        .unwrap_or_else(|| "unknown".to_string());
    let (forward_alignment_to_centerline, signed_speed_along_centerline_mps) =
        compute_alignment_and_signed_speed(&car.state, centerline_geometry);
    let tire_wear = AchievementTireWearPerWheel {
        front_left: tire_wear_from_health(car.state.tyre_health[0]),
        front_right: tire_wear_from_health(car.state.tyre_health[1]),
        rear_left: tire_wear_from_health(car.state.tyre_health[2]),
        rear_right: tire_wear_from_health(car.state.tyre_health[3]),
    };

    AchievementVehicleState {
        car_id: car.public_car_id,
        team_id,
        position: Some(vec3_to_proto(car.state.chassis_position)),
        orientation: Some(proto::race::v1::Quaternion {
            x: car.state.vehicle_orientation.x,
            y: car.state.vehicle_orientation.y,
            z: car.state.vehicle_orientation.z,
            w: car.state.vehicle_orientation.w,
        }),
        completed_laps,
        speed_mps: car.state.speed,
        race_position,
        gear: match car.state.gear {
            Gear::Reverse => -1,
            Gear::Neutral => 0,
            Gear::Forward(gear) => i32::from(gear),
        },
        tire_type: achievement_tire_type_from_engine(car.state.tyre_type) as i32,
        tire_wear: Some(tire_wear),
        wheel_positions: Some(AchievementWheelPositions {
            front_left: Some(vec3_to_proto(car.state.wheel_position[0])),
            front_right: Some(vec3_to_proto(car.state.wheel_position[1])),
            rear_left: Some(vec3_to_proto(car.state.wheel_position[2])),
            rear_right: Some(vec3_to_proto(car.state.wheel_position[3])),
        }),
        forward_alignment_to_centerline,
        signed_speed_along_centerline_mps,
    }
}

fn achievement_tire_type_from_engine(value: EngineTyreType) -> AchievementTireType {
    match value {
        EngineTyreType::Hard => AchievementTireType::Hard,
        EngineTyreType::Soft => AchievementTireType::Soft,
        EngineTyreType::Wet => AchievementTireType::Wet,
    }
}

fn tire_wear_from_health(health: f32) -> f32 {
    if health.is_finite() {
        (1.0 - health).clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn source_matches_target(source: &AchievementSourceKey, target: &EngineCommandTarget) -> bool {
    match (source, target) {
        (AchievementSourceKey::OfficialRace, EngineCommandTarget::OfficialRace) => true,
        (
            AchievementSourceKey::OfficialSandbox(expected_sandbox_id),
            EngineCommandTarget::Sandbox { sandbox_id },
        ) => expected_sandbox_id == sandbox_id,
        _ => false,
    }
}

fn compute_alignment_and_signed_speed(
    state: &boink::model::VehicleState,
    centerline_geometry: &[CenterlineGeometrySample],
) -> (f32, f32) {
    let Some(nearest_tangent) =
        nearest_centerline_tangent(state.chassis_position, centerline_geometry)
    else {
        return (0.0, 0.0);
    };

    let Some(forward) = normalized(rotate_local_forward(state.vehicle_orientation)) else {
        return (0.0, 0.0);
    };
    let Some(tangent) = normalized([nearest_tangent.x, nearest_tangent.y, nearest_tangent.z])
    else {
        return (0.0, 0.0);
    };

    let alignment = dot(forward, tangent).clamp(-1.0, 1.0);
    let signed_speed = if alignment > 0.0 {
        state.speed
    } else if alignment < 0.0 {
        -state.speed
    } else {
        0.0
    };
    (alignment, signed_speed)
}

fn nearest_centerline_tangent(
    position: EngineVec3,
    centerline_geometry: &[CenterlineGeometrySample],
) -> Option<EngineVec3> {
    centerline_geometry
        .iter()
        .min_by(|left, right| {
            let left_dist_sq = squared_distance(position, left.position);
            let right_dist_sq = squared_distance(position, right.position);
            left_dist_sq
                .partial_cmp(&right_dist_sq)
                .unwrap_or(Ordering::Equal)
        })
        .map(|sample| sample.tangent)
}

fn squared_distance(a: EngineVec3, b: EngineVec3) -> f32 {
    let dx = a.x - b.x;
    let dy = a.y - b.y;
    let dz = a.z - b.z;
    dx * dx + dy * dy + dz * dz
}

fn rotate_local_forward(q: EngineQuaternion) -> [f32; 3] {
    [
        2.0 * (q.x * q.z + q.w * q.y),
        2.0 * (q.y * q.z - q.w * q.x),
        1.0 - 2.0 * (q.x * q.x + q.y * q.y),
    ]
}

fn normalized(v: [f32; 3]) -> Option<[f32; 3]> {
    let len_sq = dot(v, v);
    if len_sq <= VECTOR_EPSILON {
        return None;
    }
    let inv_len = len_sq.sqrt().recip();
    Some([v[0] * inv_len, v[1] * inv_len, v[2] * inv_len])
}

fn dot(a: [f32; 3], b: [f32; 3]) -> f32 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}
