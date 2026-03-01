//! Race config service mapping helpers.
#![allow(dead_code)]

use prost_types::Timestamp;
use proto::race::v1::{
    RaceConfig as ProtoRaceConfig, RaceConfigInput as ProtoRaceConfigInput, RaceTimeOfDayPreset,
    StartPlacementMode,
};
use tonic::Status;

#[cfg(feature = "official")]
use crate::db::repos::race_config::ScheduleEntry as RepoScheduleEntry;
use crate::domain::race_config::{
    RaceConfigInput as DomainRaceConfigInput, ScheduleEntry as DomainScheduleEntry,
};

/// Maps persisted domain schedule entry to protobuf response shape.
pub fn schedule_entry_to_proto(entry: DomainScheduleEntry) -> ProtoRaceConfig {
    ProtoRaceConfig {
        race_id: entry.race_id,
        config: Some(draft_input_to_proto(entry.config)),
    }
}

/// Maps domain draft input to protobuf draft payload.
pub fn draft_input_to_proto(input: DomainRaceConfigInput) -> ProtoRaceConfigInput {
    let race_duration_sec = if input.ends_at_ms > input.starts_at_ms {
        let duration_ms = input.ends_at_ms - input.starts_at_ms;
        (duration_ms / 1000).min(u32::MAX as i64) as u32
    } else {
        0
    };

    ProtoRaceConfigInput {
        race_name: input.race_name,
        start_time_utc: Some(ms_to_timestamp(input.starts_at_ms)),
        race_duration_sec,
        map_id: input.map_id,
        start_placement_mode: input.start_placement_mode as i32,
        points_multiplier_fixed: input.points_multiplier_fixed,
        time_of_day_preset: input.time_of_day_preset as i32,
    }
}

/// Parses protobuf draft payload into domain draft input.
pub fn draft_input_from_proto(
    input: &ProtoRaceConfigInput,
) -> Result<DomainRaceConfigInput, Status> {
    let start_time = input
        .start_time_utc
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("input.start_time_utc is required"))?;
    let start_placement_mode = StartPlacementMode::try_from(input.start_placement_mode)
        .map_err(|_| Status::invalid_argument("invalid start_placement_mode"))?;
    let time_of_day_preset = RaceTimeOfDayPreset::try_from(input.time_of_day_preset)
        .map_err(|_| Status::invalid_argument("invalid time_of_day_preset"))?;
    if input.race_duration_sec == 0 {
        return Err(Status::invalid_argument(
            "input.race_duration_sec must be greater than 0",
        ));
    }
    let starts_at_ms = timestamp_to_ms(start_time)?;
    let duration_ms = i64::from(input.race_duration_sec) * 1000;
    let ends_at_ms = starts_at_ms
        .checked_add(duration_ms)
        .ok_or_else(|| Status::out_of_range("race duration overflow"))?;

    Ok(DomainRaceConfigInput {
        race_name: input.race_name.clone(),
        starts_at_ms,
        ends_at_ms,
        map_id: input.map_id.clone(),
        start_placement_mode,
        points_multiplier_fixed: input.points_multiplier_fixed,
        time_of_day_preset,
    })
}

/// Maps repository schedule entry to domain schedule entry.
#[cfg(feature = "official")]
pub fn repo_schedule_entry_to_domain(entry: RepoScheduleEntry) -> DomainScheduleEntry {
    DomainScheduleEntry {
        race_id: entry.race_id,
        config: DomainRaceConfigInput {
            race_name: entry.race_name,
            starts_at_ms: entry.starts_at_ms,
            ends_at_ms: entry.ends_at_ms,
            map_id: entry.map_id,
            start_placement_mode: entry.start_placement_mode,
            points_multiplier_fixed: entry.points_multiplier_fixed,
            time_of_day_preset: entry.time_of_day_preset,
        },
    }
}

/// Maps domain schedule entry to repository schedule entry.
#[cfg(feature = "official")]
pub fn domain_schedule_entry_to_repo(entry: &DomainScheduleEntry) -> RepoScheduleEntry {
    RepoScheduleEntry {
        race_id: entry.race_id.clone(),
        race_name: entry.config.race_name.clone(),
        starts_at_ms: entry.config.starts_at_ms,
        ends_at_ms: entry.config.ends_at_ms,
        map_id: entry.config.map_id.clone(),
        start_placement_mode: entry.config.start_placement_mode,
        points_multiplier_fixed: entry.config.points_multiplier_fixed,
        time_of_day_preset: entry.config.time_of_day_preset,
    }
}

/// Maps whole repository schedule to domain schedule.
#[cfg(feature = "official")]
pub fn repo_schedule_to_domain(entries: Vec<RepoScheduleEntry>) -> Vec<DomainScheduleEntry> {
    entries
        .into_iter()
        .map(repo_schedule_entry_to_domain)
        .collect()
}

/// Maps whole domain schedule to repository schedule.
#[cfg(feature = "official")]
pub fn domain_schedule_to_repo(entries: &[DomainScheduleEntry]) -> Vec<RepoScheduleEntry> {
    entries.iter().map(domain_schedule_entry_to_repo).collect()
}

/// Converts protobuf timestamp to unix milliseconds.
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

/// Converts unix milliseconds to protobuf timestamp.
pub fn ms_to_timestamp(ms: i64) -> Timestamp {
    let seconds = ms.div_euclid(1000);
    let nanos = (ms.rem_euclid(1000) as i32) * 1_000_000;
    Timestamp { seconds, nanos }
}
