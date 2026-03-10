//! Weather service mapping helpers.

use prost_types::Timestamp;
use proto::weather::v1::{ForecastPoint, ForecastPreset, WeatherScheduleEntry, WeatherType};
use tonic::Status;

use crate::domain::weather::{
    ForecastPoint as DomainForecastPoint, ScheduleEntry, dominant_weather_type_for_window,
    rain_probability_for_window, temperature_c_for_weather_type,
};

pub fn schedule_entry_to_proto(entry: ScheduleEntry) -> WeatherScheduleEntry {
    WeatherScheduleEntry {
        from: Some(ms_to_timestamp(entry.starts_at_ms)),
        r#type: entry.weather_type as i32,
    }
}

pub fn schedule_entry_from_proto(entry: &WeatherScheduleEntry) -> Result<ScheduleEntry, Status> {
    let from = entry
        .from
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("entry.from is required"))?;
    let weather_type = WeatherType::try_from(entry.r#type)
        .map_err(|_| Status::invalid_argument("invalid weather type"))?;
    Ok(ScheduleEntry {
        starts_at_ms: timestamp_to_ms(from)?,
        weather_type,
    })
}

pub fn forecast_points_to_proto(
    points: &[DomainForecastPoint],
    schedule: &[ScheduleEntry],
    preset: ForecastPreset,
) -> Result<Vec<ForecastPoint>, Status> {
    let step_ms = preset_step_ms(preset)?;
    let mut out = Vec::with_capacity(points.len());

    for (idx, point) in points.iter().enumerate() {
        let bucket_end_ms = if let Some(next) = points.get(idx + 1) {
            next.time_ms
        } else {
            point
                .time_ms
                .checked_add(step_ms)
                .ok_or_else(|| Status::out_of_range("forecast bucket end overflow"))?
        };

        let rain_probability = rain_probability_for_window(schedule, point.time_ms, bucket_end_ms);
        let dominant_type =
            dominant_weather_type_for_window(schedule, point.time_ms, bucket_end_ms);
        out.push(ForecastPoint {
            time: Some(ms_to_timestamp(point.time_ms)),
            r#type: dominant_type as i32,
            rain_probability,
            temperature_c: temperature_c_for_weather_type(dominant_type),
        });
    }

    Ok(out)
}

fn preset_step_ms(preset: ForecastPreset) -> Result<i64, Status> {
    const MINUTE_MS: i64 = 60 * 1000;
    const HOUR_MS: i64 = 60 * MINUTE_MS;
    match preset {
        ForecastPreset::ForecastPreset1HourStep15Min => Ok(15 * MINUTE_MS),
        ForecastPreset::ForecastPreset12HoursStep1Hour => Ok(HOUR_MS),
        ForecastPreset::Unspecified => Err(Status::invalid_argument(
            "forecast preset must be specified",
        )),
    }
}

pub fn timestamp_to_ms(timestamp: &Timestamp) -> Result<i64, Status> {
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(Status::invalid_argument(
            "timestamp nanos must be in range 0..1_000_000_000",
        ));
    }

    let seconds_ms = timestamp
        .seconds
        .checked_mul(1000)
        .ok_or_else(|| Status::out_of_range("timestamp seconds overflow"))?;
    let nanos_ms = i64::from(timestamp.nanos / 1_000_000);
    seconds_ms
        .checked_add(nanos_ms)
        .ok_or_else(|| Status::out_of_range("timestamp overflow"))
}

pub fn ms_to_timestamp(ms: i64) -> Timestamp {
    let seconds = ms.div_euclid(1000);
    let nanos = (ms.rem_euclid(1000) as i32) * 1_000_000;
    Timestamp { seconds, nanos }
}
