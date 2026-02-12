//! gRPC WeatherQueryService implementation.

#[cfg(feature = "official")]
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use proto::weather::v1::weather_query_service_server::WeatherQueryService;
use proto::weather::v1::{
    ForecastPreset, ForecastUpdateEvent, GetForecastNowRequest, GetForecastNowResponse,
    GetWeatherNowRequest, GetWeatherNowResponse, StreamForecastUpdatesRequest,
    StreamWeatherUpdatesRequest, WeatherNow, WeatherType, WeatherUpdateEvent,
};
use tokio::sync::mpsc;
#[cfg(feature = "official")]
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
#[cfg(feature = "official")]
use tracing::error;
use tracing::{debug, warn};

use crate::domain::weather::{
    ForecastPoint as DomainForecastPoint, ScheduleEntry, align_start_to_preset_slot,
    project_forecast, weather_type_at,
};
use crate::services::weather_mappers::forecast_point_to_proto;

#[cfg(feature = "official")]
use crate::db::repos::weather::WeatherRepo;

const SECOND_MS: i64 = 1_000;
const MINUTE_MS: i64 = 60 * SECOND_MS;
const FIFTEEN_MIN_MS: i64 = 15 * MINUTE_MS;
static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

/// WeatherQuery service backed by global weather schedule.
#[derive(Clone, Default)]
pub struct WeatherQueryServiceImpl {
    #[cfg(feature = "official")]
    inner: Option<Arc<WeatherQueryInner>>,
}

#[cfg(feature = "official")]
struct WeatherQueryInner {
    repo: WeatherRepo,
    schedule_cache: RwLock<CachedSchedule>,
    refresh_guard: Mutex<()>,
}

#[cfg(feature = "official")]
#[derive(Default, Clone)]
struct CachedSchedule {
    entries: Vec<ScheduleEntry>,
    refreshed_at_ms: Option<i64>,
}

impl WeatherQueryServiceImpl {
    #[cfg(feature = "official")]
    pub fn with_repo(repo: WeatherRepo) -> Self {
        let inner = Arc::new(WeatherQueryInner {
            repo,
            schedule_cache: RwLock::new(CachedSchedule::default()),
            refresh_guard: Mutex::new(()),
        });

        tokio::spawn(run_cache_refresh_loop(inner.clone()));
        Self { inner: Some(inner) }
    }
}

#[tonic::async_trait]
impl WeatherQueryService for WeatherQueryServiceImpl {
    type StreamWeatherUpdatesStream = ReceiverStream<Result<WeatherUpdateEvent, Status>>;
    type StreamForecastUpdatesStream = ReceiverStream<Result<ForecastUpdateEvent, Status>>;

    async fn get_weather_now(
        &self,
        _request: Request<GetWeatherNowRequest>,
    ) -> Result<Response<GetWeatherNowResponse>, Status> {
        let now_ms = current_time_ms();
        let schedule = self.current_schedule().await?;
        let weather_type = weather_type_at(&schedule, now_ms).unwrap_or(WeatherType::Unspecified);

        Ok(Response::new(GetWeatherNowResponse {
            now: Some(WeatherNow {
                r#type: weather_type as i32,
            }),
        }))
    }

    async fn stream_weather_updates(
        &self,
        request: Request<StreamWeatherUpdatesRequest>,
    ) -> Result<Response<Self::StreamWeatherUpdatesStream>, Status> {
        let _ = self.current_schedule().await?;
        let peer_addr = peer_addr_from_request(&request);
        let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed);

        let service = self.clone();
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            debug!(stream_id, peer = ?peer_addr, "weather stream started");
            let mut last_weather_type: Option<WeatherType> = None;

            let mut ticker = tokio::time::interval_at(
                next_boundary_instant(MINUTE_MS),
                Duration::from_millis(MINUTE_MS as u64),
            );

            if let Err(status) = emit_weather_update(&service, &tx, &mut last_weather_type).await {
                handle_weather_stream_error(&tx, status, stream_id, peer_addr, "initial emission")
                    .await;
                return;
            }

            loop {
                ticker.tick().await;
                let result = emit_weather_update(&service, &tx, &mut last_weather_type).await;
                if let Err(status) = result {
                    handle_weather_stream_error(&tx, status, stream_id, peer_addr, "tick").await;
                    break;
                }
                if tx.is_closed() {
                    debug!(stream_id, peer = ?peer_addr, "weather stream closed by client");
                    break;
                }
            }
            debug!(stream_id, peer = ?peer_addr, "weather stream stopped");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_forecast_now(
        &self,
        request: Request<GetForecastNowRequest>,
    ) -> Result<Response<GetForecastNowResponse>, Status> {
        let preset = extract_preset(request.into_inner())?;
        let now_ms = current_time_ms();
        let start_ms =
            align_start_to_preset_slot(now_ms, preset).map_err(map_domain_error_to_status)?;

        let schedule = self.current_schedule().await?;
        let points =
            project_forecast(&schedule, start_ms, preset).map_err(map_domain_error_to_status)?;
        let response_points = points.into_iter().map(forecast_point_to_proto).collect();

        Ok(Response::new(GetForecastNowResponse {
            points: response_points,
        }))
    }

    async fn stream_forecast_updates(
        &self,
        request: Request<StreamForecastUpdatesRequest>,
    ) -> Result<Response<Self::StreamForecastUpdatesStream>, Status> {
        let peer_addr = peer_addr_from_request(&request);
        let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed);
        let preset = extract_preset_from_spec(
            request.into_inner().spec.map(|spec| spec.preset),
            "spec is required",
        )?;
        let _ = self.current_schedule().await?;

        let service = self.clone();
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            debug!(stream_id, peer = ?peer_addr, preset = ?preset, "forecast stream started");
            let mut last_points: Option<Vec<DomainForecastPoint>> = None;

            let mut ticker = tokio::time::interval_at(
                next_boundary_instant(FIFTEEN_MIN_MS),
                Duration::from_millis(FIFTEEN_MIN_MS as u64),
            );

            if let Err(status) = emit_forecast_update(&service, preset, &tx, &mut last_points).await
            {
                handle_forecast_stream_error(
                    &tx,
                    status,
                    stream_id,
                    peer_addr,
                    preset,
                    "initial emission",
                )
                .await;
                return;
            }

            loop {
                ticker.tick().await;
                let result = emit_forecast_update(&service, preset, &tx, &mut last_points).await;
                if let Err(status) = result {
                    handle_forecast_stream_error(&tx, status, stream_id, peer_addr, preset, "tick")
                        .await;
                    break;
                }
                if tx.is_closed() {
                    debug!(stream_id, peer = ?peer_addr, preset = ?preset, "forecast stream closed by client");
                    break;
                }
            }
            debug!(stream_id, peer = ?peer_addr, preset = ?preset, "forecast stream stopped");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

impl WeatherQueryServiceImpl {
    async fn current_schedule(&self) -> Result<Vec<ScheduleEntry>, Status> {
        #[cfg(feature = "official")]
        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                Status::failed_precondition("weather query service is not configured")
            })?;

            {
                let read = inner.schedule_cache.read().await;
                if !read.entries.is_empty() && !is_cache_stale(read.refreshed_at_ms) {
                    return Ok(read.entries.clone());
                }
            }

            refresh_cache_singleflight(inner).await.map_err(|err| {
                Status::internal(format!("failed to load weather schedule: {err}"))
            })?;
            let read = inner.schedule_cache.read().await;
            return Ok(read.entries.clone());
        }

        #[cfg(not(feature = "official"))]
        {
            Err(Status::unimplemented(
                "weather query service is available only in official backend",
            ))
        }
    }
}

#[cfg(feature = "official")]
async fn refresh_cache_once(inner: &WeatherQueryInner) -> anyhow::Result<()> {
    let schedule = inner.repo.get_schedule().await?;
    let mut write = inner.schedule_cache.write().await;
    write.entries = schedule;
    write.refreshed_at_ms = Some(current_time_ms());
    Ok(())
}

#[cfg(feature = "official")]
async fn refresh_cache_singleflight(inner: &WeatherQueryInner) -> anyhow::Result<()> {
    let _guard = inner.refresh_guard.lock().await;

    {
        let read = inner.schedule_cache.read().await;
        if !read.entries.is_empty() && !is_cache_stale(read.refreshed_at_ms) {
            return Ok(());
        }
    }

    refresh_cache_once(inner).await?;
    Ok(())
}

#[cfg(feature = "official")]
async fn run_cache_refresh_loop(inner: Arc<WeatherQueryInner>) {
    let mut ticker = tokio::time::interval_at(
        next_boundary_instant(MINUTE_MS),
        Duration::from_millis(MINUTE_MS as u64),
    );
    loop {
        ticker.tick().await;
        if let Err(err) = refresh_cache_singleflight(&inner).await {
            error!(error = %err, "weather cache refresh failed");
        } else {
            debug!("weather cache refreshed");
        }
    }
}

async fn emit_weather_update(
    service: &WeatherQueryServiceImpl,
    tx: &mpsc::Sender<Result<WeatherUpdateEvent, Status>>,
    last_weather_type: &mut Option<WeatherType>,
) -> Result<(), Status> {
    let now_ms = current_time_ms();
    let schedule = service.current_schedule().await?;
    let weather_type = weather_type_at(&schedule, now_ms).unwrap_or(WeatherType::Unspecified);
    if *last_weather_type == Some(weather_type) {
        return Ok(());
    }
    *last_weather_type = Some(weather_type);

    let event = WeatherUpdateEvent {
        now: Some(WeatherNow {
            r#type: weather_type as i32,
        }),
    };

    tx.send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("weather stream closed"))
}

async fn emit_forecast_update(
    service: &WeatherQueryServiceImpl,
    preset: ForecastPreset,
    tx: &mpsc::Sender<Result<ForecastUpdateEvent, Status>>,
    last_points: &mut Option<Vec<DomainForecastPoint>>,
) -> Result<(), Status> {
    let now_ms = current_time_ms();
    let start_ms =
        align_start_to_preset_slot(now_ms, preset).map_err(map_domain_error_to_status)?;
    let schedule = service.current_schedule().await?;
    let points =
        project_forecast(&schedule, start_ms, preset).map_err(map_domain_error_to_status)?;

    if last_points.as_ref() == Some(&points) {
        return Ok(());
    }
    *last_points = Some(points.clone());

    let event = ForecastUpdateEvent {
        points: points.into_iter().map(forecast_point_to_proto).collect(),
    };

    tx.send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("forecast stream closed"))
}

async fn handle_weather_stream_error(
    tx: &mpsc::Sender<Result<WeatherUpdateEvent, Status>>,
    status: Status,
    stream_id: u64,
    peer_addr: Option<std::net::SocketAddr>,
    phase: &'static str,
) {
    if status.code() == tonic::Code::Cancelled {
        debug!(stream_id, peer = ?peer_addr, phase, "weather stream closed by client");
        return;
    }

    warn!(stream_id, peer = ?peer_addr, phase, error = %status, "weather stream failed");
    let _ = tx.send(Err(status)).await;
}

async fn handle_forecast_stream_error(
    tx: &mpsc::Sender<Result<ForecastUpdateEvent, Status>>,
    status: Status,
    stream_id: u64,
    peer_addr: Option<std::net::SocketAddr>,
    preset: ForecastPreset,
    phase: &'static str,
) {
    if status.code() == tonic::Code::Cancelled {
        debug!(stream_id, peer = ?peer_addr, preset = ?preset, phase, "forecast stream closed by client");
        return;
    }

    warn!(stream_id, peer = ?peer_addr, preset = ?preset, phase, error = %status, "forecast stream failed");
    let _ = tx.send(Err(status)).await;
}

fn next_boundary_instant(step_ms: i64) -> Instant {
    let now_ms = current_time_ms();
    let next_ms = (now_ms.div_euclid(step_ms) + 1) * step_ms;
    let delay_ms = (next_ms - now_ms).max(1) as u64;
    Instant::now() + Duration::from_millis(delay_ms)
}

fn extract_preset(request: GetForecastNowRequest) -> Result<ForecastPreset, Status> {
    extract_preset_from_spec(request.spec.map(|spec| spec.preset), "spec is required")
}

fn extract_preset_from_spec(
    preset: Option<i32>,
    missing_spec_error: &str,
) -> Result<ForecastPreset, Status> {
    let preset = preset.ok_or_else(|| Status::invalid_argument(missing_spec_error))?;
    let preset = ForecastPreset::try_from(preset)
        .map_err(|_| Status::invalid_argument("invalid forecast preset"))?;
    if matches!(preset, ForecastPreset::Unspecified) {
        return Err(Status::invalid_argument(
            "forecast preset must be specified",
        ));
    }
    Ok(preset)
}

fn map_domain_error_to_status(err: crate::domain::weather::WeatherDomainError) -> Status {
    match err {
        crate::domain::weather::WeatherDomainError::UnspecifiedPreset
        | crate::domain::weather::WeatherDomainError::UnspecifiedNotTail { .. }
        | crate::domain::weather::WeatherDomainError::NonIncreasingTimestamp { .. } => {
            Status::invalid_argument(err.to_string())
        }
        crate::domain::weather::WeatherDomainError::TimestampOverflow => {
            Status::out_of_range(err.to_string())
        }
    }
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(feature = "official")]
fn is_cache_stale(refreshed_at_ms: Option<i64>) -> bool {
    let Some(refreshed_at_ms) = refreshed_at_ms else {
        return true;
    };
    current_time_ms().saturating_sub(refreshed_at_ms) >= CACHE_MAX_STALENESS_MS
}

fn peer_addr_from_request<T>(request: &Request<T>) -> Option<std::net::SocketAddr> {
    request
        .extensions()
        .get::<tonic::transport::server::TcpConnectInfo>()
        .and_then(|info| info.remote_addr())
}

#[cfg(feature = "official")]
const CACHE_MAX_STALENESS_MS: i64 = 5_000;
