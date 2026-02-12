//! Weather domain model and projection helpers.

use proto::weather::v1::{ForecastPreset, WeatherType};
use thiserror::Error;

use crate::config::AppEnv;

const HOUR_MS: i64 = 60 * 60 * 1000;
const MINUTE_MS: i64 = 60 * 1000;

/// Global weather schedule entry interpreted by domain logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduleEntry {
    pub starts_at_ms: i64,
    pub weather_type: WeatherType,
}

/// Forecast point produced by domain projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForecastPoint {
    pub time_ms: i64,
    pub weather_type: WeatherType,
}

#[derive(Debug, Error)]
pub enum WeatherDomainError {
    #[error("forecast preset must be specified")]
    UnspecifiedPreset,
    #[error(
        "weather type unspecified is allowed only at the end in this environment (index {index})"
    )]
    UnspecifiedNotTail { index: usize },
    #[error(
        "schedule timestamps must be strictly increasing at index {index} (prev={prev}, current={current})"
    )]
    NonIncreasingTimestamp {
        index: usize,
        prev: i64,
        current: i64,
    },
    #[error("timestamp overflow while projecting forecast")]
    TimestampOverflow,
}

/// Policy controlling where `WEATHER_TYPE_UNSPECIFIED` is accepted in the schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnspecifiedPolicy {
    AllowAnywhere,
    AllowOnlyTail,
}

/// Resolves `WEATHER_TYPE_UNSPECIFIED` validation policy from application environment.
pub fn unspecified_policy_for_env(env: AppEnv) -> UnspecifiedPolicy {
    match env {
        AppEnv::Production => UnspecifiedPolicy::AllowOnlyTail,
        AppEnv::Development | AppEnv::Preprod => UnspecifiedPolicy::AllowAnywhere,
    }
}

/// Validates schedule invariants required by projection logic.
pub fn validate_schedule(
    entries: &[ScheduleEntry],
    unspecified_policy: UnspecifiedPolicy,
) -> Result<(), WeatherDomainError> {
    let mut prev = None;
    let len = entries.len();
    for (idx, entry) in entries.iter().enumerate() {
        if entry.weather_type == WeatherType::Unspecified
            && matches!(unspecified_policy, UnspecifiedPolicy::AllowOnlyTail)
            && idx + 1 != len
        {
            return Err(WeatherDomainError::UnspecifiedNotTail { index: idx });
        }
        if let Some(prev_ts) = prev {
            if entry.starts_at_ms <= prev_ts {
                return Err(WeatherDomainError::NonIncreasingTimestamp {
                    index: idx,
                    prev: prev_ts,
                    current: entry.starts_at_ms,
                });
            }
        }
        prev = Some(entry.starts_at_ms);
    }
    Ok(())
}

/// Returns effective weather type at a given timestamp.
pub fn weather_type_at(entries: &[ScheduleEntry], time_ms: i64) -> Option<WeatherType> {
    if entries.is_empty() {
        return None;
    }

    let idx = entries.partition_point(|entry| entry.starts_at_ms <= time_ms);
    if idx == 0 {
        None
    } else {
        Some(entries[idx - 1].weather_type)
    }
}

/// Aligns timestamp down to preset slot boundary.
pub fn align_start_to_preset_slot(
    time_ms: i64,
    preset: ForecastPreset,
) -> Result<i64, WeatherDomainError> {
    let (_, step_ms) = preset_window(preset)?;
    Ok(time_ms - (time_ms.rem_euclid(step_ms)))
}

/// Projects forecast points for requested preset from explicit start time.
///
/// The projection returns points in `[start_ms, start_ms + horizon_ms)`,
/// sampled every `step_ms`.
pub fn project_forecast(
    entries: &[ScheduleEntry],
    start_ms: i64,
    preset: ForecastPreset,
) -> Result<Vec<ForecastPoint>, WeatherDomainError> {
    validate_schedule(entries, UnspecifiedPolicy::AllowAnywhere)?;
    let (horizon_ms, step_ms) = preset_window(preset)?;

    let end_ms = start_ms
        .checked_add(horizon_ms)
        .ok_or(WeatherDomainError::TimestampOverflow)?;

    let mut points = Vec::new();
    let mut time_ms = start_ms;
    while time_ms < end_ms {
        if let Some(weather_type) = weather_type_at(entries, time_ms) {
            points.push(ForecastPoint {
                time_ms,
                weather_type,
            });
        }
        time_ms = time_ms
            .checked_add(step_ms)
            .ok_or(WeatherDomainError::TimestampOverflow)?;
    }

    Ok(points)
}

fn preset_window(preset: ForecastPreset) -> Result<(i64, i64), WeatherDomainError> {
    match preset {
        ForecastPreset::ForecastPreset1HourStep15Min => Ok((HOUR_MS, 15 * MINUTE_MS)),
        ForecastPreset::ForecastPreset12HoursStep1Hour => Ok((12 * HOUR_MS, HOUR_MS)),
        ForecastPreset::Unspecified => Err(WeatherDomainError::UnspecifiedPreset),
    }
}
