//! Shared stochastic forecast helpers for query and admin paths.

use std::collections::HashMap;

use proto::weather::v1::{ForecastPoint as ProtoForecastPoint, ForecastPreset, WeatherType};
use rand::Rng;

use crate::domain::weather::temperature_c_for_weather_type;

const MINUTE_MS: i64 = 60 * 1_000;

pub(crate) fn stochasticize_forecast_points<R: Rng + ?Sized>(
    baseline_points: &[ProtoForecastPoint],
    previous_points: &[ProtoForecastPoint],
    preset: ForecastPreset,
    now_ms: i64,
    rng: &mut R,
    allow_non_first_bucket_reuse: bool,
) -> Vec<ProtoForecastPoint> {
    let step_ms = preset_step_ms(preset).unwrap_or(MINUTE_MS);
    let is_step_boundary = now_ms.rem_euclid(step_ms) == 0;
    let prev_by_time_ms: HashMap<i64, &ProtoForecastPoint> = previous_points
        .iter()
        .filter_map(|point| forecast_point_time_ms(point).map(|time_ms| (time_ms, point)))
        .collect();

    let mut out = Vec::with_capacity(baseline_points.len());
    let mut prev_output_type: Option<WeatherType> = None;

    for (idx, point) in baseline_points.iter().enumerate() {
        if idx > 0 && allow_non_first_bucket_reuse && !is_step_boundary {
            if let Some(prev) = forecast_point_time_ms(point)
                .and_then(|time_ms| prev_by_time_ms.get(&time_ms).copied())
            {
                let prev_type =
                    WeatherType::try_from(prev.r#type).unwrap_or(WeatherType::Unspecified);
                prev_output_type = Some(prev_type);
                out.push(prev.clone());
                continue;
            }
        }

        let stochastic = stochasticize_point(point, preset, idx, prev_output_type, rng);
        let current_type =
            WeatherType::try_from(stochastic.r#type).unwrap_or(WeatherType::Unspecified);
        prev_output_type = Some(current_type);
        out.push(stochastic);
    }

    out
}

fn stochasticize_point<R: Rng + ?Sized>(
    base: &ProtoForecastPoint,
    preset: ForecastPreset,
    bucket_idx: usize,
    prev_output_type: Option<WeatherType>,
    rng: &mut R,
) -> ProtoForecastPoint {
    let mut out = base.clone();
    let profile = stochastic_profile(preset, bucket_idx);

    let rain_delta = rng.gen_range(-profile.max_rain_delta..=profile.max_rain_delta);
    let rain_probability = (base.rain_probability + rain_delta).clamp(0.0, 1.0);
    let base_type = WeatherType::try_from(base.r#type).unwrap_or(WeatherType::Unspecified);
    let stochastic_type = choose_weather_type_for_probability(
        base_type,
        rain_probability,
        profile,
        prev_output_type,
        rng,
    );
    out.r#type = stochastic_type as i32;
    out.temperature_c = temperature_c_for_weather_type(stochastic_type);
    out.rain_probability = rain_probability;

    cohere_rain_probability_and_type(&mut out);
    out
}

fn choose_weather_type_for_probability<R: Rng + ?Sized>(
    base_type: WeatherType,
    rain_probability: f32,
    profile: StochasticProfile,
    prev_output_type: Option<WeatherType>,
    rng: &mut R,
) -> WeatherType {
    let transition_candidates = transition_candidates_for_type(base_type);
    let compatible_candidates: Vec<(WeatherType, u32)> = transition_candidates
        .iter()
        .copied()
        .filter(|(weather_type, _)| {
            is_type_compatible_with_probability(*weather_type, rain_probability)
        })
        .collect();

    let base_compatible = is_type_compatible_with_probability(base_type, rain_probability);
    if base_compatible {
        if profile.type_mismatch_chance <= 0.0 || !rng.gen_bool(profile.type_mismatch_chance as f64)
        {
            return base_type;
        }
        if compatible_candidates.is_empty() {
            return base_type;
        }
        let smoothed = apply_smoothing_penalty(&compatible_candidates, prev_output_type);
        return pick_weighted_weather_type(&smoothed, rng);
    }

    if !compatible_candidates.is_empty() {
        let smoothed = apply_smoothing_penalty(&compatible_candidates, prev_output_type);
        return pick_weighted_weather_type(&smoothed, rng);
    }

    fallback_type_for_probability(rain_probability)
}

fn apply_smoothing_penalty(
    candidates: &[(WeatherType, u32)],
    prev_output_type: Option<WeatherType>,
) -> Vec<(WeatherType, u32)> {
    let Some(prev_type) = prev_output_type else {
        return candidates.to_vec();
    };

    candidates
        .iter()
        .map(|(weather_type, weight)| {
            let distance = weather_type_distance(prev_type, *weather_type);
            let penalty_pct: u32 = match distance {
                0 => 100,
                1 => 80,
                2 => 35,
                _ => 10,
            };
            let adjusted = ((*weight).saturating_mul(penalty_pct)).max(1) / 100;
            (*weather_type, adjusted.max(1))
        })
        .collect()
}

fn weather_type_distance(a: WeatherType, b: WeatherType) -> u32 {
    weather_type_rank(a).abs_diff(weather_type_rank(b))
}

fn weather_type_rank(weather_type: WeatherType) -> u32 {
    match weather_type {
        WeatherType::Unspecified | WeatherType::Clear => 0,
        WeatherType::PartlyCloudy => 1,
        WeatherType::Overcast => 2,
        WeatherType::LightRain => 3,
        WeatherType::MediumRain => 4,
        WeatherType::HeavyRain => 5,
    }
}

fn cohere_rain_probability_and_type(point: &mut ProtoForecastPoint) {
    let weather_type = WeatherType::try_from(point.r#type).unwrap_or(WeatherType::Unspecified);
    let rain_probability = point.rain_probability.clamp(0.0, 1.0);
    let (min_p, max_p) = rain_probability_bounds_for_type(weather_type);
    point.rain_probability = rain_probability.clamp(min_p, max_p);
}

fn is_type_compatible_with_probability(weather_type: WeatherType, rain_probability: f32) -> bool {
    let (min_p, max_p) = rain_probability_bounds_for_type(weather_type);
    (min_p..=max_p).contains(&rain_probability)
}

fn rain_probability_bounds_for_type(weather_type: WeatherType) -> (f32, f32) {
    match weather_type {
        WeatherType::Unspecified | WeatherType::Clear => (0.0, 0.25),
        WeatherType::PartlyCloudy => (0.0, 0.4),
        WeatherType::Overcast => (0.0, 0.5),
        WeatherType::LightRain | WeatherType::MediumRain | WeatherType::HeavyRain => (0.5, 1.0),
    }
}

fn transition_candidates_for_type(base: WeatherType) -> &'static [(WeatherType, u32)] {
    match base {
        WeatherType::Unspecified => &[(WeatherType::Clear, 100)],
        WeatherType::Clear => &[(WeatherType::PartlyCloudy, 100)],
        WeatherType::PartlyCloudy => &[
            (WeatherType::Clear, 45),
            (WeatherType::Overcast, 45),
            (WeatherType::LightRain, 10),
        ],
        WeatherType::Overcast => &[
            (WeatherType::PartlyCloudy, 40),
            (WeatherType::LightRain, 40),
            (WeatherType::MediumRain, 20),
        ],
        WeatherType::LightRain => &[
            (WeatherType::Overcast, 40),
            (WeatherType::MediumRain, 40),
            (WeatherType::PartlyCloudy, 15),
            (WeatherType::HeavyRain, 5),
        ],
        WeatherType::MediumRain => &[
            (WeatherType::LightRain, 45),
            (WeatherType::HeavyRain, 35),
            (WeatherType::Overcast, 20),
        ],
        WeatherType::HeavyRain => &[(WeatherType::MediumRain, 100)],
    }
}

fn fallback_type_for_probability(rain_probability: f32) -> WeatherType {
    if rain_probability >= 0.5 {
        WeatherType::LightRain
    } else if rain_probability >= 0.05 {
        WeatherType::Overcast
    } else if rain_probability >= 0.01 {
        WeatherType::PartlyCloudy
    } else {
        WeatherType::Clear
    }
}

fn pick_weighted_weather_type<R: Rng + ?Sized>(
    candidates: &[(WeatherType, u32)],
    rng: &mut R,
) -> WeatherType {
    let total_weight: u32 = candidates.iter().map(|(_, weight)| *weight).sum();
    if total_weight == 0 {
        return WeatherType::Unspecified;
    }

    let mut roll = rng.gen_range(0..total_weight);
    for (weather_type, weight) in candidates {
        if roll < *weight {
            return *weather_type;
        }
        roll -= *weight;
    }

    candidates
        .last()
        .map(|(weather_type, _)| *weather_type)
        .unwrap_or(WeatherType::Unspecified)
}

#[derive(Clone, Copy)]
struct StochasticProfile {
    max_rain_delta: f32,
    type_mismatch_chance: f32,
}

fn stochastic_profile(preset: ForecastPreset, bucket_idx: usize) -> StochasticProfile {
    match preset {
        ForecastPreset::ForecastPreset1HourStep15Min => one_hour_stochastic_profile(bucket_idx),
        ForecastPreset::ForecastPreset12HoursStep1Hour => {
            twelve_hours_stochastic_profile(bucket_idx)
        }
        ForecastPreset::Unspecified => StochasticProfile {
            max_rain_delta: 0.0,
            type_mismatch_chance: 0.0,
        },
    }
}

fn one_hour_stochastic_profile(bucket_idx: usize) -> StochasticProfile {
    match bucket_idx {
        0 => StochasticProfile {
            max_rain_delta: 0.02,
            type_mismatch_chance: 0.0,
        },
        1 => StochasticProfile {
            max_rain_delta: 0.03,
            type_mismatch_chance: 0.0,
        },
        2 => StochasticProfile {
            max_rain_delta: 0.04,
            type_mismatch_chance: 0.03,
        },
        3 => StochasticProfile {
            max_rain_delta: 0.05,
            type_mismatch_chance: 0.06,
        },
        _ => StochasticProfile {
            max_rain_delta: 0.06,
            type_mismatch_chance: 0.10,
        },
    }
}

fn twelve_hours_stochastic_profile(bucket_idx: usize) -> StochasticProfile {
    const MAX_BUCKET_IDX_12H: usize = 12;
    const START_PROFILE_BUCKET_1H: usize = 3;
    const REFERENCE_PROFILE_BUCKET_1H: usize = 4;
    const EASE_IN_EXPONENT: f32 = 2.3;
    const LINEAR_BLEND_WEIGHT: f32 = 0.35;
    const TARGET_MAX_RAIN_DELTA_12H: f32 = 0.66;
    const TARGET_MAX_TYPE_MISMATCH_12H: f32 = 0.66;

    if bucket_idx == 0 {
        return one_hour_stochastic_profile(START_PROFILE_BUCKET_1H);
    }

    let clamped_bucket_idx = bucket_idx.min(MAX_BUCKET_IDX_12H);
    let progress = (clamped_bucket_idx.saturating_sub(1) as f32
        / (MAX_BUCKET_IDX_12H.saturating_sub(1) as f32))
        .clamp(0.0, 1.0);
    let eased_progress = progress.powf(EASE_IN_EXPONENT);
    let blended_progress = (LINEAR_BLEND_WEIGHT * progress
        + (1.0 - LINEAR_BLEND_WEIGHT) * eased_progress)
        .clamp(0.0, 1.0);
    let one_hour_reference_profile = one_hour_stochastic_profile(REFERENCE_PROFILE_BUCKET_1H);

    StochasticProfile {
        max_rain_delta: lerp(
            one_hour_reference_profile.max_rain_delta,
            TARGET_MAX_RAIN_DELTA_12H,
            blended_progress,
        ),
        type_mismatch_chance: lerp(
            one_hour_reference_profile.type_mismatch_chance,
            TARGET_MAX_TYPE_MISMATCH_12H,
            blended_progress,
        ),
    }
}

fn lerp(start: f32, end: f32, t: f32) -> f32 {
    start + (end - start) * t
}

fn forecast_point_time_ms(point: &ProtoForecastPoint) -> Option<i64> {
    let timestamp = point.time.as_ref()?;
    let seconds_ms = timestamp.seconds.checked_mul(1_000)?;
    seconds_ms.checked_add(i64::from(timestamp.nanos) / 1_000_000)
}

fn preset_step_ms(preset: ForecastPreset) -> Option<i64> {
    const HOUR_MS: i64 = 60 * MINUTE_MS;
    match preset {
        ForecastPreset::ForecastPreset1HourStep15Min => Some(15 * MINUTE_MS),
        ForecastPreset::ForecastPreset12HoursStep1Hour => Some(HOUR_MS),
        ForecastPreset::Unspecified => None,
    }
}
