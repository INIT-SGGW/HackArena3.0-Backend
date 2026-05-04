//! gRPC WeatherQueryService implementation.

#[cfg(feature = "official")]
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use proto::weather::v1::weather_query_service_server::WeatherQueryService;
use proto::weather::v1::{
    ForecastPoint as ProtoForecastPoint, ForecastPreset, ForecastUpdateEvent,
    GetForecastNowRequest, GetForecastNowResponse, GetWeatherNowRequest, GetWeatherNowResponse,
    StreamForecastUpdatesRequest, StreamWeatherUpdatesRequest, WeatherNow, WeatherTarget,
    WeatherType, WeatherUpdateEvent, weather_target,
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

#[cfg(feature = "official")]
use super::mappers::forecast_points_to_proto;
#[cfg(feature = "official")]
use super::stochastic::stochasticize_forecast_points;
#[cfg(feature = "local")]
use super::{LocalWeatherEventHub, LocalWeatherEventKind};
use crate::domain::weather::ScheduleEntry;
#[cfg(feature = "official")]
use crate::domain::weather::project_forecast;
#[cfg(feature = "official")]
use crate::domain::weather::{
    clamp_schedule_temperature_c, temperature_c_for_weather_type, weather_at,
};
#[cfg(feature = "local")]
use crate::local::local_race_state::LocalRaceStateStore;
#[cfg(feature = "local")]
use crate::runtime::engine_worker::{EngineClient, EngineRuntimeWeatherType};
#[cfg(feature = "local")]
use crate::services::error_map::map_worker_err;

#[cfg(feature = "official")]
use crate::db::repos::weather::WeatherRepo;

const MINUTE_MS: i64 = 60 * 1_000;
const STREAM_CHANNEL_CAPACITY: usize = 16;
static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

/// WeatherQuery service backed by global weather schedule.
#[derive(Clone, Default)]
pub struct WeatherQueryServiceImpl {
    #[cfg(feature = "official")]
    inner: Option<Arc<WeatherQueryInner>>,
    #[cfg(feature = "local")]
    local_inner: Option<LocalWeatherQueryInner>,
}

#[cfg(feature = "official")]
struct WeatherQueryInner {
    repo: WeatherRepo,
    schedule_cache: RwLock<CachedSchedule>,
    refresh_guard: Mutex<()>,
    forecast_cache: RwLock<CachedForecasts>,
    forecast_refresh_guard: Mutex<()>,
}

#[cfg(feature = "official")]
#[derive(Default, Clone)]
struct CachedSchedule {
    entries: Vec<ScheduleEntry>,
    refreshed_at_ms: Option<i64>,
    generation: u64,
}

#[cfg(feature = "official")]
#[derive(Default, Clone)]
struct CachedForecasts {
    one_hour: CachedForecast,
    twelve_hours: CachedForecast,
}

#[cfg(feature = "official")]
#[derive(Default, Clone)]
struct CachedForecast {
    points: Vec<ProtoForecastPoint>,
    minute_slot: Option<i64>,
    schedule_generation: Option<u64>,
}

#[cfg(feature = "local")]
#[derive(Clone)]
struct LocalWeatherQueryInner {
    engine: EngineClient,
    weather_events: LocalWeatherEventHub,
    local_race_state: LocalRaceStateStore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParsedWeatherTarget {
    OfficialRace,
    Sandbox { sandbox_id: String },
    LocalRace { race_id: String },
}

impl WeatherQueryServiceImpl {
    #[cfg(feature = "official")]
    pub fn with_repo(repo: WeatherRepo) -> Self {
        let inner = Arc::new(WeatherQueryInner {
            repo,
            schedule_cache: RwLock::new(CachedSchedule::default()),
            refresh_guard: Mutex::new(()),
            forecast_cache: RwLock::new(CachedForecasts::default()),
            forecast_refresh_guard: Mutex::new(()),
        });

        tokio::spawn(run_cache_refresh_loop(inner.clone()));
        Self {
            inner: Some(inner),
            ..Self::default()
        }
    }

    #[cfg(feature = "local")]
    pub fn for_local(
        engine: EngineClient,
        weather_events: LocalWeatherEventHub,
        local_race_state: LocalRaceStateStore,
    ) -> Self {
        Self {
            local_inner: Some(LocalWeatherQueryInner {
                engine,
                weather_events,
                local_race_state,
            }),
            ..Self::default()
        }
    }
}

#[tonic::async_trait]
impl WeatherQueryService for WeatherQueryServiceImpl {
    type StreamWeatherUpdatesStream = ReceiverStream<Result<WeatherUpdateEvent, Status>>;
    type StreamForecastUpdatesStream = ReceiverStream<Result<ForecastUpdateEvent, Status>>;

    async fn get_weather_now(
        &self,
        request: Request<GetWeatherNowRequest>,
    ) -> Result<Response<GetWeatherNowResponse>, Status> {
        let target = parse_required_weather_target(request.into_inner().target)?;
        let now = self.weather_now_for_target(target).await?;
        Ok(Response::new(GetWeatherNowResponse { now: Some(now) }))
    }

    async fn stream_weather_updates(
        &self,
        request: Request<StreamWeatherUpdatesRequest>,
    ) -> Result<Response<Self::StreamWeatherUpdatesStream>, Status> {
        let target = parse_required_weather_target(request.get_ref().target.clone())?;
        let peer_addr = peer_addr_from_request(&request);
        let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed);

        match target {
            ParsedWeatherTarget::OfficialRace => {
                #[cfg(not(feature = "official"))]
                {
                    return Err(Status::unimplemented(
                        "official-race weather target is available only in official backend",
                    ));
                }

                #[cfg(feature = "official")]
                {
                    let _ = self.current_schedule().await?;
                    let service = self.clone();
                    let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
                    tokio::spawn(async move {
                        debug!(stream_id, peer = ?peer_addr, "weather stream started");
                        let mut last_weather: Option<WeatherNow> = None;

                        let mut ticker = tokio::time::interval_at(
                            next_boundary_instant(MINUTE_MS),
                            Duration::from_millis(MINUTE_MS as u64),
                        );

                        if let Err(status) =
                            emit_weather_update(&service, &tx, &mut last_weather).await
                        {
                            handle_weather_stream_error(
                                &tx,
                                status,
                                stream_id,
                                peer_addr,
                                "initial emission",
                            )
                            .await;
                            return;
                        }

                        loop {
                            ticker.tick().await;
                            let result =
                                emit_weather_update(&service, &tx, &mut last_weather).await;
                            if let Err(status) = result {
                                handle_weather_stream_error(
                                    &tx, status, stream_id, peer_addr, "tick",
                                )
                                .await;
                                break;
                            }
                            if tx.is_closed() {
                                debug!(stream_id, peer = ?peer_addr, "weather stream closed by client");
                                break;
                            }
                        }
                        debug!(stream_id, peer = ?peer_addr, "weather stream stopped");
                    });

                    return Ok(Response::new(ReceiverStream::new(rx)));
                }
            }
            ParsedWeatherTarget::Sandbox { sandbox_id } => {
                #[cfg(not(feature = "local"))]
                {
                    let _ = sandbox_id;
                    return Err(Status::unimplemented(
                        "sandbox weather target is available only in local backend",
                    ));
                }

                #[cfg(feature = "local")]
                {
                    return self
                        .stream_weather_updates_local(stream_id, peer_addr, sandbox_id)
                        .await;
                }
            }
            ParsedWeatherTarget::LocalRace { race_id } => {
                #[cfg(not(feature = "local"))]
                {
                    let _ = race_id;
                    return Err(Status::unimplemented(
                        "local-race weather target is available only in local backend",
                    ));
                }

                #[cfg(feature = "local")]
                {
                    let service = self.clone();
                    let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
                    let initial = service.local_race_weather_now(&race_id).await?;
                    tokio::spawn(async move {
                        debug!(stream_id, peer = ?peer_addr, race_id = %race_id, "local race weather stream started");
                        if tx
                            .send(Ok(WeatherUpdateEvent {
                                now: Some(initial.clone()),
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        let mut last = initial;
                        let mut ticker = tokio::time::interval(Duration::from_millis(1_000));
                        loop {
                            ticker.tick().await;
                            match service.local_race_weather_now(&race_id).await {
                                Ok(now) if now != last => {
                                    last = now.clone();
                                    if tx
                                        .send(Ok(WeatherUpdateEvent { now: Some(now) }))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                Ok(_) => {}
                                Err(status) => {
                                    let _ = tx.send(Err(status)).await;
                                    break;
                                }
                            }
                        }
                    });
                    return Ok(Response::new(ReceiverStream::new(rx)));
                }
            }
        }
    }

    async fn get_forecast_now(
        &self,
        request: Request<GetForecastNowRequest>,
    ) -> Result<Response<GetForecastNowResponse>, Status> {
        let preset = extract_preset(request.into_inner())?;
        let points = self.current_forecast(preset).await?;
        Ok(Response::new(GetForecastNowResponse { points }))
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
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            debug!(stream_id, peer = ?peer_addr, preset = ?preset, "forecast stream started");
            let mut last_points: Option<Vec<ProtoForecastPoint>> = None;

            let mut ticker = tokio::time::interval_at(
                next_boundary_instant(MINUTE_MS),
                Duration::from_millis(MINUTE_MS as u64),
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

    async fn current_forecast(
        &self,
        preset: ForecastPreset,
    ) -> Result<Vec<ProtoForecastPoint>, Status> {
        #[cfg(feature = "official")]
        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                Status::failed_precondition("weather query service is not configured")
            })?;
            let now_ms = current_minute_start_ms();
            let schedule_generation = ensure_schedule_cache_fresh(inner).await.map_err(|err| {
                Status::internal(format!("failed to load weather schedule: {err}"))
            })?;

            {
                let read = inner.forecast_cache.read().await;
                if let Some(cached) = cached_forecast_for_preset(&read, preset) {
                    if is_forecast_cache_fresh(cached, schedule_generation, now_ms) {
                        return Ok(cached.points.clone());
                    }
                }
            }

            refresh_forecast_singleflight(inner, preset, now_ms)
                .await
                .map_err(|err| {
                    Status::internal(format!("failed to compute weather forecast: {err}"))
                })?;

            let read = inner.forecast_cache.read().await;
            let cached = cached_forecast_for_preset(&read, preset)
                .ok_or_else(|| Status::invalid_argument("forecast preset must be specified"))?;
            return Ok(cached.points.clone());
        }

        #[cfg(not(feature = "official"))]
        {
            let _ = preset;
            Err(Status::unimplemented(
                "weather query service is available only in official backend",
            ))
        }
    }

    async fn weather_now_for_target(
        &self,
        target: ParsedWeatherTarget,
    ) -> Result<WeatherNow, Status> {
        match target {
            ParsedWeatherTarget::OfficialRace => {
                #[cfg(feature = "official")]
                {
                    let now_ms = current_time_ms();
                    let schedule = self.current_schedule().await?;
                    return Ok(official_weather_now_from_schedule(&schedule, now_ms));
                }

                #[cfg(not(feature = "official"))]
                {
                    Err(Status::unimplemented(
                        "official-race weather target is available only in official backend",
                    ))
                }
            }
            ParsedWeatherTarget::Sandbox { sandbox_id } => {
                #[cfg(feature = "local")]
                {
                    return self.local_weather_now(&sandbox_id).await;
                }

                #[cfg(not(feature = "local"))]
                {
                    let _ = sandbox_id;
                    Err(Status::unimplemented(
                        "sandbox weather target is available only in local backend",
                    ))
                }
            }
            ParsedWeatherTarget::LocalRace { race_id } => {
                #[cfg(feature = "local")]
                {
                    return self.local_race_weather_now(&race_id).await;
                }

                #[cfg(not(feature = "local"))]
                {
                    let _ = race_id;
                    Err(Status::unimplemented(
                        "local-race weather target is available only in local backend",
                    ))
                }
            }
        }
    }

    #[cfg(feature = "local")]
    async fn local_weather_now(&self, sandbox_id: &str) -> Result<WeatherNow, Status> {
        let local = self.local_inner.as_ref().ok_or_else(|| {
            Status::failed_precondition("weather query service is not configured")
        })?;
        let runtime = local.engine.runtime_state().await.map_err(map_worker_err)?;
        let active = runtime
            .active_sandboxes
            .iter()
            .find(|entry| entry.sandbox_id == sandbox_id)
            .ok_or_else(|| {
                Status::not_found("active sandbox session not found for weather target")
            })?;
        let snapshot = active.weather_now.ok_or_else(|| {
            Status::failed_precondition(
                "weather runtime snapshot is unavailable for active sandbox",
            )
        })?;

        Ok(WeatherNow {
            r#type: runtime_weather_type_to_proto(snapshot.weather_type) as i32,
            temperature_c: snapshot.temperature_c,
        })
    }

    #[cfg(feature = "local")]
    async fn local_race_weather_now(&self, race_id: &str) -> Result<WeatherNow, Status> {
        let local = self.local_inner.as_ref().ok_or_else(|| {
            Status::failed_precondition("weather query service is not configured")
        })?;
        let race =
            local.local_race_state.active_race().await.ok_or_else(|| {
                Status::not_found("active local race not found for weather target")
            })?;
        if race.race_id != race_id {
            return Err(Status::not_found(
                "active local race not found for weather target",
            ));
        }
        let weather = race
            .weather
            .ok_or_else(|| Status::failed_precondition("local race weather is unavailable"))?;
        Ok(WeatherNow {
            r#type: weather.weather_type,
            temperature_c: weather.temperature_c,
        })
    }

    #[cfg(feature = "local")]
    async fn stream_weather_updates_local(
        &self,
        stream_id: u64,
        peer_addr: Option<std::net::SocketAddr>,
        sandbox_id: String,
    ) -> Result<Response<ReceiverStream<Result<WeatherUpdateEvent, Status>>>, Status> {
        let local = self.local_inner.as_ref().ok_or_else(|| {
            Status::failed_precondition("weather query service is not configured")
        })?;
        let mut events_rx = local.weather_events.subscribe();
        let service = self.clone();
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);

        let initial = service.local_weather_now(&sandbox_id).await?;
        tokio::spawn(async move {
            debug!(stream_id, peer = ?peer_addr, sandbox_id = %sandbox_id, "local weather stream started");
            let mut last_now = initial;
            let initial_event = WeatherUpdateEvent { now: Some(initial) };
            if tx.send(Ok(initial_event)).await.is_err() {
                debug!(stream_id, peer = ?peer_addr, sandbox_id = %sandbox_id, "local weather stream closed before initial emission");
                return;
            }

            loop {
                match events_rx.recv().await {
                    Ok(event) => {
                        if event.sandbox_id != sandbox_id {
                            continue;
                        }
                        if matches!(event.kind, LocalWeatherEventKind::Deactivated) {
                            let _ = tx
                                .send(Err(Status::not_found(
                                    "active sandbox session not found for weather target",
                                )))
                                .await;
                            break;
                        }

                        match service.local_weather_now(&sandbox_id).await {
                            Ok(now) => {
                                if now == last_now {
                                    continue;
                                }
                                last_now = now;
                                if tx
                                    .send(Ok(WeatherUpdateEvent { now: Some(now) }))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(status) => {
                                let _ = tx.send(Err(status)).await;
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(
                            stream_id,
                            peer = ?peer_addr,
                            sandbox_id = %sandbox_id,
                            skipped,
                            "local weather stream lagged; resyncing from latest runtime snapshot"
                        );
                        match service.local_weather_now(&sandbox_id).await {
                            Ok(now) => {
                                if now == last_now {
                                    continue;
                                }
                                last_now = now;
                                if tx
                                    .send(Ok(WeatherUpdateEvent { now: Some(now) }))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(status) => {
                                let _ = tx.send(Err(status)).await;
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }

            debug!(stream_id, peer = ?peer_addr, sandbox_id = %sandbox_id, "local weather stream stopped");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(feature = "official")]
async fn refresh_cache_once(inner: &WeatherQueryInner) -> anyhow::Result<()> {
    let schedule = inner.repo.get_schedule().await?;
    let mut write = inner.schedule_cache.write().await;
    let changed = write.entries != schedule;
    write.entries = schedule;
    write.refreshed_at_ms = Some(current_time_ms());
    if changed {
        write.generation = write.generation.wrapping_add(1);
    }
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
async fn ensure_schedule_cache_fresh(inner: &WeatherQueryInner) -> anyhow::Result<u64> {
    {
        let read = inner.schedule_cache.read().await;
        if !read.entries.is_empty() && !is_cache_stale(read.refreshed_at_ms) {
            return Ok(read.generation);
        }
    }

    refresh_cache_singleflight(inner).await?;
    let read = inner.schedule_cache.read().await;
    Ok(read.generation)
}

#[cfg(feature = "official")]
async fn refresh_forecast_singleflight(
    inner: &WeatherQueryInner,
    preset: ForecastPreset,
    now_ms: i64,
) -> anyhow::Result<()> {
    let _guard = inner.forecast_refresh_guard.lock().await;
    let schedule_generation = ensure_schedule_cache_fresh(inner).await?;

    {
        let read = inner.forecast_cache.read().await;
        if let Some(cached) = cached_forecast_for_preset(&read, preset) {
            if is_forecast_cache_fresh(cached, schedule_generation, now_ms) {
                return Ok(());
            }
        } else {
            anyhow::bail!("unsupported forecast preset: {preset:?}");
        }
    }

    refresh_forecast_once(inner, preset, now_ms, schedule_generation).await
}

#[cfg(feature = "official")]
async fn refresh_forecast_once(
    inner: &WeatherQueryInner,
    preset: ForecastPreset,
    now_ms: i64,
    schedule_generation: u64,
) -> anyhow::Result<()> {
    let schedule = {
        let read = inner.schedule_cache.read().await;
        read.entries.clone()
    };

    let points = project_forecast(&schedule, now_ms, preset)
        .map_err(|err| anyhow::anyhow!("forecast projection failed: {err}"))?;
    let baseline_points = forecast_points_to_proto(&points, &schedule, preset)
        .map_err(|err| anyhow::anyhow!("forecast mapping failed: {err}"))?;

    let mut write = inner.forecast_cache.write().await;
    let cached = cached_forecast_for_preset_mut(&mut write, preset)
        .ok_or_else(|| anyhow::anyhow!("unsupported forecast preset: {preset:?}"))?;

    let same_schedule_snapshot = cached.schedule_generation == Some(schedule_generation);
    let prev_points = std::mem::take(&mut cached.points);
    let mut rng = rand::thread_rng();
    cached.points = stochasticize_forecast_points(
        &baseline_points,
        &prev_points,
        preset,
        now_ms,
        &mut rng,
        same_schedule_snapshot,
    );
    cached.minute_slot = Some(now_ms.div_euclid(MINUTE_MS));
    cached.schedule_generation = Some(schedule_generation);
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
            continue;
        }

        let now_ms = current_minute_start_ms();
        for preset in [
            ForecastPreset::ForecastPreset1HourStep15Min,
            ForecastPreset::ForecastPreset12HoursStep1Hour,
        ] {
            if let Err(err) = refresh_forecast_singleflight(&inner, preset, now_ms).await {
                error!(error = %err, preset = ?preset, "weather forecast cache refresh failed");
            } else {
                debug!(preset = ?preset, "weather forecast cache refreshed");
            }
        }
    }
}

#[cfg(feature = "official")]
async fn emit_weather_update(
    service: &WeatherQueryServiceImpl,
    tx: &mpsc::Sender<Result<WeatherUpdateEvent, Status>>,
    last_weather: &mut Option<WeatherNow>,
) -> Result<(), Status> {
    let now_ms = current_time_ms();
    let schedule = service.current_schedule().await?;
    let now = official_weather_now_from_schedule(&schedule, now_ms);
    if last_weather.as_ref() == Some(&now) {
        return Ok(());
    }
    *last_weather = Some(now.clone());

    let event = WeatherUpdateEvent { now: Some(now) };

    tx.send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("weather stream closed"))
}

#[cfg(feature = "official")]
fn official_weather_now_from_schedule(schedule: &[ScheduleEntry], now_ms: i64) -> WeatherNow {
    if let Some(entry) = weather_at(schedule, now_ms) {
        return WeatherNow {
            r#type: entry.weather_type as i32,
            temperature_c: clamp_schedule_temperature_c(entry.temperature_c),
        };
    }

    let weather_type = WeatherType::Unspecified;
    WeatherNow {
        r#type: weather_type as i32,
        temperature_c: temperature_c_for_weather_type(weather_type),
    }
}

async fn emit_forecast_update(
    service: &WeatherQueryServiceImpl,
    preset: ForecastPreset,
    tx: &mpsc::Sender<Result<ForecastUpdateEvent, Status>>,
    last_points: &mut Option<Vec<ProtoForecastPoint>>,
) -> Result<(), Status> {
    let points = service.current_forecast(preset).await?;
    if last_points.as_ref() == Some(&points) {
        return Ok(());
    }
    *last_points = Some(points.clone());

    let event = ForecastUpdateEvent { points };

    tx.send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("forecast stream closed"))
}

#[cfg(feature = "official")]
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

fn parse_required_weather_target(
    target: Option<WeatherTarget>,
) -> Result<ParsedWeatherTarget, Status> {
    let target = target.ok_or_else(|| Status::invalid_argument("weather target is required"))?;
    match target.target {
        Some(weather_target::Target::OfficialRace(_)) => Ok(ParsedWeatherTarget::OfficialRace),
        Some(weather_target::Target::Sandbox(sandbox)) => {
            if sandbox.sandbox_id.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "weather target sandbox_id must be non-empty",
                ));
            }
            Ok(ParsedWeatherTarget::Sandbox {
                sandbox_id: sandbox.sandbox_id,
            })
        }
        Some(weather_target::Target::LocalRace(local_race)) => {
            if local_race.race_id.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "weather target race_id must be non-empty",
                ));
            }
            Ok(ParsedWeatherTarget::LocalRace {
                race_id: local_race.race_id,
            })
        }
        None => Err(Status::invalid_argument(
            "weather target must include exactly one target",
        )),
    }
}

#[cfg(feature = "local")]
fn runtime_weather_type_to_proto(weather_type: EngineRuntimeWeatherType) -> WeatherType {
    match weather_type {
        EngineRuntimeWeatherType::Clear => WeatherType::Clear,
        EngineRuntimeWeatherType::PartlyCloudy => WeatherType::PartlyCloudy,
        EngineRuntimeWeatherType::Overcast => WeatherType::Overcast,
        EngineRuntimeWeatherType::LightRain => WeatherType::LightRain,
        EngineRuntimeWeatherType::MediumRain => WeatherType::MediumRain,
        EngineRuntimeWeatherType::HeavyRain => WeatherType::HeavyRain,
    }
}

#[cfg(feature = "official")]
fn cached_forecast_for_preset(
    cache: &CachedForecasts,
    preset: ForecastPreset,
) -> Option<&CachedForecast> {
    match preset {
        ForecastPreset::ForecastPreset1HourStep15Min => Some(&cache.one_hour),
        ForecastPreset::ForecastPreset12HoursStep1Hour => Some(&cache.twelve_hours),
        ForecastPreset::Unspecified => None,
    }
}

#[cfg(feature = "official")]
fn cached_forecast_for_preset_mut(
    cache: &mut CachedForecasts,
    preset: ForecastPreset,
) -> Option<&mut CachedForecast> {
    match preset {
        ForecastPreset::ForecastPreset1HourStep15Min => Some(&mut cache.one_hour),
        ForecastPreset::ForecastPreset12HoursStep1Hour => Some(&mut cache.twelve_hours),
        ForecastPreset::Unspecified => None,
    }
}

#[cfg(feature = "official")]
fn is_forecast_cache_fresh(cached: &CachedForecast, schedule_generation: u64, now_ms: i64) -> bool {
    if cached.points.is_empty() {
        return false;
    }
    if cached.minute_slot != Some(now_ms.div_euclid(MINUTE_MS)) {
        return false;
    }
    cached.schedule_generation == Some(schedule_generation)
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

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(feature = "official")]
fn current_minute_start_ms() -> i64 {
    let now_ms = current_time_ms();
    now_ms - now_ms.rem_euclid(MINUTE_MS)
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
const CACHE_MAX_STALENESS_MS: i64 = MINUTE_MS;
