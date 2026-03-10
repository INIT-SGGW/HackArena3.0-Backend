//! Race config service mapping helpers.

use prost_types::Timestamp;
use proto::race::v1::{
    RaceConfig as ProtoRaceConfig, RaceConfigInput as ProtoRaceConfigInput, RaceTimeOfDayPreset,
    StartPlacementMode,
};
use tonic::Status;

use crate::db::repos::race_config::{
    RaceConfigInputRecord as RepoRaceConfigInputRecord, RaceConfigRecord as RepoRaceConfigRecord,
};
use crate::domain::race_config::{
    RaceConfigInput as DomainRaceConfigInput, ScheduleEntry as DomainScheduleEntry,
};

/// Maps repository race entry to protobuf response shape.
pub fn race_to_proto(entry: RepoRaceConfigRecord) -> ProtoRaceConfig {
    ProtoRaceConfig {
        race_id: entry.race_id,
        config: Some(race_input_to_proto(entry.config)),
    }
}

/// Maps repository race payload to protobuf payload.
pub fn race_input_to_proto(input: RepoRaceConfigInputRecord) -> ProtoRaceConfigInput {
    ProtoRaceConfigInput {
        race_name: input.race_name,
        start_time_utc: Some(ms_to_timestamp(input.starts_at_ms)),
        race_duration_sec: input.race_duration_sec,
        map_id: input.map_id,
        start_placement_mode: input.start_placement_mode as i32,
        points_multiplier_fixed: input.points_multiplier_fixed,
        time_of_day_preset: input.time_of_day_preset as i32,
    }
}

/// Parses protobuf payload into repository race payload.
pub fn race_input_from_proto(
    input: &ProtoRaceConfigInput,
) -> Result<RepoRaceConfigInputRecord, Status> {
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

    Ok(RepoRaceConfigInputRecord {
        race_name: input.race_name.clone(),
        starts_at_ms: timestamp_to_ms(start_time)?,
        race_duration_sec: input.race_duration_sec,
        map_id: input.map_id.clone(),
        start_placement_mode,
        points_multiplier_fixed: input.points_multiplier_fixed,
        time_of_day_preset,
    })
}

/// Maps repository schedule to domain schedule for domain-level validation.
pub fn repo_schedule_to_domain(
    entries: &[RepoRaceConfigRecord],
) -> Result<Vec<DomainScheduleEntry>, Status> {
    entries
        .iter()
        .map(repo_schedule_entry_to_domain)
        .collect::<Result<Vec<_>, _>>()
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

fn repo_schedule_entry_to_domain(
    entry: &RepoRaceConfigRecord,
) -> Result<DomainScheduleEntry, Status> {
    let ends_at_ms = ends_at_ms(entry.config.starts_at_ms, entry.config.race_duration_sec)?;
    Ok(DomainScheduleEntry {
        race_id: entry.race_id.clone(),
        config: DomainRaceConfigInput {
            race_name: entry.config.race_name.clone(),
            starts_at_ms: entry.config.starts_at_ms,
            ends_at_ms,
            map_id: entry.config.map_id.clone(),
            start_placement_mode: entry.config.start_placement_mode,
            points_multiplier_fixed: entry.config.points_multiplier_fixed,
            time_of_day_preset: entry.config.time_of_day_preset,
        },
    })
}

fn ends_at_ms(starts_at_ms: i64, race_duration_sec: u32) -> Result<i64, Status> {
    let duration_ms = i64::from(race_duration_sec)
        .checked_mul(1000)
        .ok_or_else(|| Status::out_of_range("race duration overflow"))?;
    starts_at_ms
        .checked_add(duration_ms)
        .ok_or_else(|| Status::out_of_range("race end time overflow"))
}
