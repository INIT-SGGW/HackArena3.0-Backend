//! Weather domain model and projection helpers.
use std::cmp::Reverse;
use std::collections::HashMap;

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

/// Weather parameters consumed by the simulation engine.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EngineWeatherParams {
    /// Cloudiness in range `0.0..=1.0`.
    pub cloudiness: f32,
    /// Ambient temperature in Celsius.
    pub temperature_c: f32,
    /// Rain intensity in range `0.0..=1.0`.
    pub rain_intensity: f32,
}

/// Maps API weather type to simulation engine parameters.
///
/// `WeatherType::Unspecified` is treated as a safe clear-weather fallback.
pub fn engine_params_for_weather_type(weather_type: WeatherType) -> EngineWeatherParams {
    match weather_type {
        WeatherType::Unspecified | WeatherType::Clear => EngineWeatherParams {
            cloudiness: 0.0,
            temperature_c: 20.0,
            rain_intensity: 0.0,
        },
        WeatherType::PartlyCloudy => EngineWeatherParams {
            cloudiness: 0.5,
            temperature_c: 18.0,
            rain_intensity: 0.0,
        },
        WeatherType::Overcast => EngineWeatherParams {
            cloudiness: 1.0,
            temperature_c: 16.0,
            rain_intensity: 0.0,
        },
        WeatherType::LightRain => EngineWeatherParams {
            cloudiness: 0.8,
            temperature_c: 15.0,
            rain_intensity: 0.3,
        },
        WeatherType::MediumRain => EngineWeatherParams {
            cloudiness: 0.95,
            temperature_c: 14.0,
            rain_intensity: 0.6,
        },
        WeatherType::HeavyRain => EngineWeatherParams {
            cloudiness: 1.0,
            temperature_c: 13.0,
            rain_intensity: 0.85,
        },
    }
}

/// Returns air temperature (C) for a weather type, rounded to whole degrees.
pub fn temperature_c_for_weather_type(weather_type: WeatherType) -> i32 {
    engine_params_for_weather_type(weather_type)
        .temperature_c
        .round() as i32
}

/// Computes rain probability for a `[start_ms, end_ms)` bucket.
///
/// Probability is the fraction of time where effective weather is rainy.
#[must_use]
pub fn rain_probability_for_window(entries: &[ScheduleEntry], start_ms: i64, end_ms: i64) -> f32 {
    if end_ms <= start_ms {
        return 0.0;
    }
    if entries.is_empty() {
        return 0.0;
    }

    let mut rain_ms: i64 = 0;
    let mut cursor = start_ms;
    let mut next_idx = entries.partition_point(|entry| entry.starts_at_ms <= start_ms);
    let mut current_type = if next_idx == 0 {
        WeatherType::Unspecified
    } else {
        entries[next_idx - 1].weather_type
    };

    while cursor < end_ms {
        let next_change = if next_idx < entries.len() {
            entries[next_idx].starts_at_ms.min(end_ms)
        } else {
            end_ms
        };

        if next_change > cursor && is_rain_type(current_type) {
            rain_ms += next_change - cursor;
        }

        if next_change >= end_ms {
            break;
        }

        cursor = next_change;
        current_type = entries[next_idx].weather_type;
        next_idx += 1;
    }

    let total_ms = (end_ms - start_ms) as f32;
    (rain_ms as f32 / total_ms).clamp(0.0, 1.0)
}

/// Computes dominant weather type for a `[start_ms, end_ms)` bucket.
///
/// Dominance is selected by the longest effective duration in the window.
/// In case of ties, the type that appears earlier in the bucket is preferred.
#[must_use]
pub fn dominant_weather_type_for_window(
    entries: &[ScheduleEntry],
    start_ms: i64,
    end_ms: i64,
) -> WeatherType {
    if end_ms <= start_ms || entries.is_empty() {
        return WeatherType::Unspecified;
    }

    let mut stats: HashMap<WeatherType, (i64, i64)> = HashMap::new();

    let mut cursor = start_ms;
    let mut next_idx = entries.partition_point(|entry| entry.starts_at_ms <= start_ms);
    let mut current_type = if next_idx == 0 {
        WeatherType::Unspecified
    } else {
        entries[next_idx - 1].weather_type
    };

    while cursor < end_ms {
        let next_change = if next_idx < entries.len() {
            entries[next_idx].starts_at_ms.min(end_ms)
        } else {
            end_ms
        };

        if next_change > cursor {
            let slot = stats.entry(current_type).or_insert((0, cursor));
            slot.0 += next_change - cursor;
        }

        if next_change >= end_ms {
            break;
        }

        cursor = next_change;
        current_type = entries[next_idx].weather_type;
        next_idx += 1;
    }

    stats
        .into_iter()
        .max_by_key(|(weather_type, (duration_ms, first_seen))| {
            // Prefer longer duration, then earlier appearance, then stable enum order.
            (*duration_ms, Reverse(*first_seen), Reverse(*weather_type))
        })
        .map(|(weather_type, _)| weather_type)
        .unwrap_or(WeatherType::Unspecified)
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
/// Bucket semantics:
/// - first point starts exactly at `start_ms`,
/// - subsequent points are aligned to slot boundaries and then advanced by `step_ms`,
/// - points are returned while `time <= start_ms + horizon_ms`.
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
    let mut first = true;
    while time_ms <= end_ms {
        if let Some(weather_type) = weather_type_at(entries, time_ms) {
            points.push(ForecastPoint {
                time_ms,
                weather_type,
            });
        }

        let next = if first {
            first = false;
            next_aligned_bucket_start(time_ms, step_ms)?
        } else {
            time_ms
                .checked_add(step_ms)
                .ok_or(WeatherDomainError::TimestampOverflow)?
        };
        if next > end_ms {
            break;
        }
        time_ms = next;
    }

    Ok(points)
}

fn next_aligned_bucket_start(time_ms: i64, step_ms: i64) -> Result<i64, WeatherDomainError> {
    let rem = time_ms.rem_euclid(step_ms);
    let delta = if rem == 0 { step_ms } else { step_ms - rem };
    time_ms
        .checked_add(delta)
        .ok_or(WeatherDomainError::TimestampOverflow)
}

fn is_rain_type(weather_type: WeatherType) -> bool {
    matches!(
        weather_type,
        WeatherType::LightRain | WeatherType::MediumRain | WeatherType::HeavyRain
    )
}

fn preset_window(preset: ForecastPreset) -> Result<(i64, i64), WeatherDomainError> {
    match preset {
        ForecastPreset::ForecastPreset1HourStep15Min => Ok((HOUR_MS, 15 * MINUTE_MS)),
        ForecastPreset::ForecastPreset12HoursStep1Hour => Ok((12 * HOUR_MS, HOUR_MS)),
        ForecastPreset::Unspecified => Err(WeatherDomainError::UnspecifiedPreset),
    }
}
