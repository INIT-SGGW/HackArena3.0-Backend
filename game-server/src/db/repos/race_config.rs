//! Race config repository for persisted schedule entries.

use proto::race::v1::{RaceTimeOfDayPreset, StartPlacementMode};
use sqlx::PgPool;

/// Persisted race configuration schedule entry.
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleEntry {
    pub race_id: String,
    pub race_name: String,
    pub starts_at_ms: i64,
    pub ends_at_ms: i64,
    pub map_id: String,
    pub start_placement_mode: StartPlacementMode,
    pub points_multiplier_fixed: f32,
    pub time_of_day_preset: RaceTimeOfDayPreset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "start_placement_mode", rename_all = "snake_case")]
enum DbStartPlacementMode {
    Random,
    Scoreboard,
    ReversedScoreboard,
}

impl From<DbStartPlacementMode> for StartPlacementMode {
    fn from(value: DbStartPlacementMode) -> Self {
        match value {
            DbStartPlacementMode::Random => StartPlacementMode::Random,
            DbStartPlacementMode::Scoreboard => StartPlacementMode::Scoreboard,
            DbStartPlacementMode::ReversedScoreboard => StartPlacementMode::ReversedScoreboard,
        }
    }
}

impl TryFrom<StartPlacementMode> for DbStartPlacementMode {
    type Error = &'static str;

    fn try_from(value: StartPlacementMode) -> Result<Self, Self::Error> {
        match value {
            StartPlacementMode::Random => Ok(DbStartPlacementMode::Random),
            StartPlacementMode::Scoreboard => Ok(DbStartPlacementMode::Scoreboard),
            StartPlacementMode::ReversedScoreboard => Ok(DbStartPlacementMode::ReversedScoreboard),
            StartPlacementMode::Unspecified => Err("start placement mode must be specified"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "time_of_day_preset", rename_all = "snake_case")]
enum DbTimeOfDayPreset {
    Morning,
    Noon,
    Evening,
    Night,
}

impl From<DbTimeOfDayPreset> for RaceTimeOfDayPreset {
    fn from(value: DbTimeOfDayPreset) -> Self {
        match value {
            DbTimeOfDayPreset::Morning => RaceTimeOfDayPreset::Morning,
            DbTimeOfDayPreset::Noon => RaceTimeOfDayPreset::Noon,
            DbTimeOfDayPreset::Evening => RaceTimeOfDayPreset::Evening,
            DbTimeOfDayPreset::Night => RaceTimeOfDayPreset::Night,
        }
    }
}

impl TryFrom<RaceTimeOfDayPreset> for DbTimeOfDayPreset {
    type Error = &'static str;

    fn try_from(value: RaceTimeOfDayPreset) -> Result<Self, Self::Error> {
        match value {
            RaceTimeOfDayPreset::Morning => Ok(DbTimeOfDayPreset::Morning),
            RaceTimeOfDayPreset::Noon => Ok(DbTimeOfDayPreset::Noon),
            RaceTimeOfDayPreset::Evening => Ok(DbTimeOfDayPreset::Evening),
            RaceTimeOfDayPreset::Night => Ok(DbTimeOfDayPreset::Night),
            RaceTimeOfDayPreset::Unspecified => Err("time of day preset must be specified"),
        }
    }
}

/// Repository for reading and replacing race configuration schedule.
#[derive(Clone)]
pub struct RaceConfigRepo {
    pool: PgPool,
}

impl RaceConfigRepo {
    /// Creates a repository backed by the provided Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Fetches full schedule ordered by start timestamp.
    pub async fn get_schedule(&self) -> anyhow::Result<Vec<ScheduleEntry>> {
        let rows = sqlx::query!(
            r#"SELECT race_id, race_name, starts_at_ms, ends_at_ms, map_id, start_placement_mode AS "start_placement_mode: DbStartPlacementMode", points_multiplier_fixed, time_of_day_preset AS "time_of_day_preset: DbTimeOfDayPreset" FROM race_config_schedule ORDER BY starts_at_ms ASC"#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            entries.push(ScheduleEntry {
                race_id: row.race_id,
                race_name: row.race_name,
                starts_at_ms: row.starts_at_ms,
                ends_at_ms: row.ends_at_ms,
                map_id: row.map_id,
                start_placement_mode: row.start_placement_mode.into(),
                points_multiplier_fixed: row.points_multiplier_fixed,
                time_of_day_preset: row.time_of_day_preset.into(),
            });
        }

        Ok(entries)
    }

    /// Replaces whole schedule in a single transaction.
    pub async fn replace_schedule(&self, entries: &[ScheduleEntry]) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query!("DELETE FROM race_config_schedule")
            .execute(&mut *tx)
            .await?;

        for entry in entries {
            let start_placement_mode = DbStartPlacementMode::try_from(entry.start_placement_mode)
                .map_err(|msg| anyhow::anyhow!(msg))?;
            let time_of_day_preset = DbTimeOfDayPreset::try_from(entry.time_of_day_preset)
                .map_err(|msg| anyhow::anyhow!(msg))?;

            sqlx::query!(
                "INSERT INTO race_config_schedule (race_id, race_name, starts_at_ms, ends_at_ms, map_id, start_placement_mode, points_multiplier_fixed, time_of_day_preset) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                entry.race_id,
                entry.race_name,
                entry.starts_at_ms,
                entry.ends_at_ms,
                entry.map_id,
                start_placement_mode as _,
                entry.points_multiplier_fixed,
                time_of_day_preset as _,
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
