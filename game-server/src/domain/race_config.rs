//! Race config domain model and validation.

use std::collections::HashSet;

use proto::race::v1::{StartPlacementMode, TimeOfDayPreset};
use thiserror::Error;

/// Race configuration payload interpreted by domain logic.
#[derive(Debug, Clone, PartialEq)]
pub struct RaceConfigInput {
    pub race_name: String,
    pub starts_at_ms: i64,
    pub ends_at_ms: i64,
    pub map_id: String,
    pub map_version: Option<u32>,
    pub start_placement_mode: StartPlacementMode,
    pub points_multiplier_fixed: f32,
    pub time_of_day_preset: TimeOfDayPreset,
}

/// Persisted race configuration entry.
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleEntry {
    pub race_id: String,
    pub config: RaceConfigInput,
}

#[derive(Debug, Error)]
pub enum RaceConfigDomainError {
    #[error("race_id must be non-empty at index {index}")]
    EmptyRaceId { index: usize },
    #[error("race_name must be non-empty at index {index}")]
    EmptyRaceName { index: usize },
    #[error("map_id must be non-empty at index {index}")]
    EmptyMapId { index: usize },
    #[error("map_version must be greater than zero at index {index}")]
    InvalidMapVersion { index: usize },
    #[error("start_placement_mode must be specified at index {index}")]
    UnspecifiedStartPlacementMode { index: usize },
    #[error("time_of_day_preset must be specified at index {index}")]
    UnspecifiedTimeOfDayPreset { index: usize },
    #[error("points_multiplier_fixed must be positive and finite at index {index}")]
    InvalidPointsMultiplier { index: usize },
    #[error(
        "race time window must satisfy starts_at_ms < ends_at_ms at index {index} (start={start}, end={end})"
    )]
    InvalidTimeWindow { index: usize, start: i64, end: i64 },
    #[error(
        "schedule starts_at_ms must be strictly increasing at index {index} (prev={prev}, current={current})"
    )]
    NonIncreasingStartTime {
        index: usize,
        prev: i64,
        current: i64,
    },
    #[error(
        "schedule entries must not overlap at index {index} (prev_end={prev_end}, current_start={current_start})"
    )]
    OverlappingTimeWindow {
        index: usize,
        prev_end: i64,
        current_start: i64,
    },
    #[error("race_id must be unique; duplicate \"{race_id}\" at index {index}")]
    DuplicateRaceId { index: usize, race_id: String },
}

/// Validates draft schedule entries before persistence.
pub fn validate_draft_schedule(entries: &[RaceConfigInput]) -> Result<(), RaceConfigDomainError> {
    validate_inputs(entries)
}

/// Validates persisted schedule invariants.
pub fn validate_schedule(entries: &[ScheduleEntry]) -> Result<(), RaceConfigDomainError> {
    let mut seen_race_ids: HashSet<&str> = HashSet::with_capacity(entries.len());
    let mut prev_start: Option<i64> = None;
    let mut prev_end: Option<i64> = None;

    for (idx, entry) in entries.iter().enumerate() {
        if entry.race_id.trim().is_empty() {
            return Err(RaceConfigDomainError::EmptyRaceId { index: idx });
        }
        if !seen_race_ids.insert(entry.race_id.as_str()) {
            return Err(RaceConfigDomainError::DuplicateRaceId {
                index: idx,
                race_id: entry.race_id.clone(),
            });
        }

        validate_input(idx, &entry.config)?;
        validate_ordering_and_overlap(
            idx,
            entry.config.starts_at_ms,
            entry.config.ends_at_ms,
            prev_start,
            prev_end,
        )?;

        prev_start = Some(entry.config.starts_at_ms);
        prev_end = Some(entry.config.ends_at_ms);
    }

    Ok(())
}

fn validate_inputs(entries: &[RaceConfigInput]) -> Result<(), RaceConfigDomainError> {
    let mut prev_start: Option<i64> = None;
    let mut prev_end: Option<i64> = None;

    for (idx, entry) in entries.iter().enumerate() {
        validate_input(idx, entry)?;
        validate_ordering_and_overlap(
            idx,
            entry.starts_at_ms,
            entry.ends_at_ms,
            prev_start,
            prev_end,
        )?;

        prev_start = Some(entry.starts_at_ms);
        prev_end = Some(entry.ends_at_ms);
    }

    Ok(())
}

fn validate_input(index: usize, entry: &RaceConfigInput) -> Result<(), RaceConfigDomainError> {
    if entry.race_name.trim().is_empty() {
        return Err(RaceConfigDomainError::EmptyRaceName { index });
    }
    if entry.map_id.trim().is_empty() {
        return Err(RaceConfigDomainError::EmptyMapId { index });
    }
    if entry.map_version == Some(0) {
        return Err(RaceConfigDomainError::InvalidMapVersion { index });
    }
    if matches!(entry.start_placement_mode, StartPlacementMode::Unspecified) {
        return Err(RaceConfigDomainError::UnspecifiedStartPlacementMode { index });
    }
    if matches!(entry.time_of_day_preset, TimeOfDayPreset::Unspecified) {
        return Err(RaceConfigDomainError::UnspecifiedTimeOfDayPreset { index });
    }
    if !(entry.points_multiplier_fixed.is_finite() && entry.points_multiplier_fixed > 0.0) {
        return Err(RaceConfigDomainError::InvalidPointsMultiplier { index });
    }
    if entry.starts_at_ms >= entry.ends_at_ms {
        return Err(RaceConfigDomainError::InvalidTimeWindow {
            index,
            start: entry.starts_at_ms,
            end: entry.ends_at_ms,
        });
    }
    Ok(())
}

fn validate_ordering_and_overlap(
    index: usize,
    starts_at_ms: i64,
    _ends_at_ms: i64,
    prev_start: Option<i64>,
    prev_end: Option<i64>,
) -> Result<(), RaceConfigDomainError> {
    if let Some(prev) = prev_start {
        if starts_at_ms <= prev {
            return Err(RaceConfigDomainError::NonIncreasingStartTime {
                index,
                prev,
                current: starts_at_ms,
            });
        }
    }
    if let Some(prev_end) = prev_end {
        if starts_at_ms < prev_end {
            return Err(RaceConfigDomainError::OverlappingTimeWindow {
                index,
                prev_end,
                current_start: starts_at_ms,
            });
        }
    }
    Ok(())
}
