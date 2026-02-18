//! Weather service mapping helpers.

use prost_types::Timestamp;
use proto::weather::v1::{ForecastPoint, WeatherScheduleEntry, WeatherType};
use tonic::Status;

use crate::domain::weather::{
    ForecastPoint as DomainForecastPoint, ScheduleEntry, temperature_c_for_weather_type,
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

pub fn forecast_point_to_proto(point: DomainForecastPoint) -> ForecastPoint {
    ForecastPoint {
        time: Some(ms_to_timestamp(point.time_ms)),
        r#type: point.weather_type as i32,
        // TODO(weather): replace with proper probability model.
        rain_probability: 0.0,
        temperature_c: temperature_c_for_weather_type(point.weather_type),
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
