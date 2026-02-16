//! Weather synchronization helpers for the engine worker.

#[cfg(feature = "official")]
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use boink::engine::Engine;
#[cfg(feature = "official")]
use boink::model::WeatherParams;
#[cfg(feature = "official")]
use proto::weather::v1::WeatherType;

#[cfg(feature = "official")]
use crate::db::repos::weather::WeatherRepo;
#[cfg(feature = "official")]
use crate::domain::weather::{engine_params_for_weather_type, weather_type_at};

pub const WEATHER_TICK_MS: i64 = 60 * 1000;

#[derive(Clone)]
pub struct WeatherSyncState {
    #[cfg(feature = "official")]
    repo: WeatherRepo,
    #[cfg(feature = "official")]
    last_applied: Arc<tokio::sync::RwLock<Option<WeatherType>>>,
}

impl WeatherSyncState {
    #[cfg(feature = "official")]
    pub fn with_repo(repo: WeatherRepo) -> Self {
        Self {
            repo,
            last_applied: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    #[cfg(not(feature = "official"))]
    pub fn disabled() -> Self {
        Self {}
    }
}

#[cfg(feature = "official")]
pub async fn apply_weather_from_schedule(
    engine: &mut Engine,
    weather_sync: &WeatherSyncState,
) -> anyhow::Result<()> {
    let schedule = weather_sync.repo.get_schedule().await?;
    let now_ms = current_time_ms();
    let resolved = weather_type_at(&schedule, now_ms);
    let weather_type = resolved.unwrap_or(WeatherType::Clear);

    {
        let last_applied = weather_sync.last_applied.read().await;
        if last_applied.is_some_and(|last| last == weather_type) {
            tracing::debug!(weather_type = ?weather_type, "engine worker: weather unchanged");
            return Ok(());
        }
    }

    if resolved.is_none() {
        tracing::warn!(
            "weather schedule has no active entry for now_ms={}; using clear fallback",
            now_ms
        );
    }

    let params = engine_params_for_weather_type(weather_type);
    let weather = WeatherParams {
        cloudiness: params.cloudiness,
        temperature_c: params.temperature_c,
        rain_intensity: params.rain_intensity,
    };
    engine.set_weather(weather).map_err(anyhow::Error::new)?;
    tracing::debug!(weather_type = ?weather_type, "engine worker: weather updated");

    let mut last_applied = weather_sync.last_applied.write().await;
    *last_applied = Some(weather_type);
    Ok(())
}

#[cfg(not(feature = "official"))]
pub async fn apply_weather_from_schedule(
    engine: &mut Engine,
    weather_sync: &WeatherSyncState,
) -> anyhow::Result<()> {
    let _ = (engine, weather_sync);
    Ok(())
}

fn current_time_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

pub fn next_boundary_instant(step_ms: i64) -> tokio::time::Instant {
    let now_ms = current_time_ms();
    let next_ms = (now_ms.div_euclid(step_ms) + 1) * step_ms;
    let delay_ms = (next_ms - now_ms).max(1) as u64;
    tokio::time::Instant::now() + Duration::from_millis(delay_ms)
}
