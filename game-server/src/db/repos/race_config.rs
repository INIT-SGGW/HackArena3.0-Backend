//! Race config repository for persisted race configuration.

use proto::race::v1::{RaceTimeOfDayPreset, StartPlacementMode};
use sqlx::{PgPool, Postgres, Transaction};
use thiserror::Error;

/// Legacy schedule entry shape kept temporarily for mapper compatibility.
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

/// Persisted race config input fields.
#[derive(Debug, Clone, PartialEq)]
pub struct RaceConfigInputRecord {
    pub race_name: String,
    pub starts_at_ms: i64,
    pub race_duration_sec: u32,
    pub map_id: String,
    pub start_placement_mode: StartPlacementMode,
    pub points_multiplier_fixed: f32,
    pub time_of_day_preset: RaceTimeOfDayPreset,
}

/// Persisted race config entry with stable id.
#[derive(Debug, Clone, PartialEq)]
pub struct RaceConfigRecord {
    pub race_id: String,
    pub config: RaceConfigInputRecord,
}

/// Full persisted snapshot used by admin API responses.
#[derive(Debug, Clone, PartialEq)]
pub struct RaceConfigSnapshot {
    pub revision: u64,
    pub races: Vec<RaceConfigRecord>,
}

/// Repository error surface for race config persistence.
#[derive(Debug, Error)]
pub enum RaceConfigRepoError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error("race config state row is missing")]
    StateMissing,
    #[error("revision mismatch: expected {expected}, actual {actual}")]
    RevisionMismatch { expected: u64, actual: u64 },
    #[error("race config already exists: {race_id}")]
    AlreadyExists { race_id: String },
    #[error("race config not found: {race_id}")]
    NotFound { race_id: String },
    #[error("start_placement_mode must be specified")]
    InvalidStartPlacementMode,
    #[error("time_of_day_preset must be specified")]
    InvalidTimeOfDayPreset,
    #[error("persisted numeric value is out of range for race: {race_id} ({field})")]
    NumericOutOfRange {
        race_id: String,
        field: &'static str,
    },
    #[error("revision overflow")]
    RevisionOverflow,
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
    type Error = RaceConfigRepoError;

    fn try_from(value: StartPlacementMode) -> Result<Self, Self::Error> {
        match value {
            StartPlacementMode::Random => Ok(DbStartPlacementMode::Random),
            StartPlacementMode::Scoreboard => Ok(DbStartPlacementMode::Scoreboard),
            StartPlacementMode::ReversedScoreboard => Ok(DbStartPlacementMode::ReversedScoreboard),
            StartPlacementMode::Unspecified => Err(RaceConfigRepoError::InvalidStartPlacementMode),
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
    type Error = RaceConfigRepoError;

    fn try_from(value: RaceTimeOfDayPreset) -> Result<Self, Self::Error> {
        match value {
            RaceTimeOfDayPreset::Morning => Ok(DbTimeOfDayPreset::Morning),
            RaceTimeOfDayPreset::Noon => Ok(DbTimeOfDayPreset::Noon),
            RaceTimeOfDayPreset::Evening => Ok(DbTimeOfDayPreset::Evening),
            RaceTimeOfDayPreset::Night => Ok(DbTimeOfDayPreset::Night),
            RaceTimeOfDayPreset::Unspecified => Err(RaceConfigRepoError::InvalidTimeOfDayPreset),
        }
    }
}

/// Repository for race config snapshot and CRUD updates.
#[derive(Clone)]
pub struct RaceConfigRepo {
    pool: PgPool,
}

impl RaceConfigRepo {
    /// Creates a repository backed by the provided Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Reads current revision and full race config list.
    pub async fn get_snapshot(&self) -> Result<RaceConfigSnapshot, RaceConfigRepoError> {
        let mut tx = self.pool.begin().await?;
        let revision = read_revision_for_share(&mut tx).await?;
        let races = read_configs(&mut tx).await?;
        tx.commit().await?;

        Ok(RaceConfigSnapshot { revision, races })
    }

    /// Inserts new race config and bumps revision.
    pub async fn create_config(
        &self,
        expected_revision: u64,
        race: &RaceConfigRecord,
    ) -> Result<u64, RaceConfigRepoError> {
        let mut tx = self.pool.begin().await?;
        let current_revision = read_revision_for_update(&mut tx).await?;
        ensure_expected_revision(expected_revision, current_revision)?;

        if exists_by_id(&mut tx, &race.race_id).await? {
            return Err(RaceConfigRepoError::AlreadyExists {
                race_id: race.race_id.clone(),
            });
        }

        insert_config(&mut tx, race).await?;
        let next_revision = bump_revision(&mut tx, current_revision).await?;
        tx.commit().await?;
        Ok(next_revision)
    }

    /// Updates existing race config and bumps revision.
    pub async fn update_config(
        &self,
        expected_revision: u64,
        race: &RaceConfigRecord,
    ) -> Result<u64, RaceConfigRepoError> {
        let mut tx = self.pool.begin().await?;
        let current_revision = read_revision_for_update(&mut tx).await?;
        ensure_expected_revision(expected_revision, current_revision)?;

        if !exists_by_id(&mut tx, &race.race_id).await? {
            return Err(RaceConfigRepoError::NotFound {
                race_id: race.race_id.clone(),
            });
        }

        replace_config(&mut tx, race).await?;
        let next_revision = bump_revision(&mut tx, current_revision).await?;
        tx.commit().await?;
        Ok(next_revision)
    }

    /// Deletes existing race config and bumps revision.
    pub async fn delete_config(
        &self,
        expected_revision: u64,
        race_id: &str,
    ) -> Result<u64, RaceConfigRepoError> {
        let mut tx = self.pool.begin().await?;
        let current_revision = read_revision_for_update(&mut tx).await?;
        ensure_expected_revision(expected_revision, current_revision)?;

        let delete_count = sqlx::query!(
            "DELETE FROM race_config_schedule WHERE race_id = $1",
            race_id
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();

        if delete_count == 0 {
            return Err(RaceConfigRepoError::NotFound {
                race_id: race_id.to_string(),
            });
        }

        let next_revision = bump_revision(&mut tx, current_revision).await?;
        tx.commit().await?;
        Ok(next_revision)
    }
}

fn ensure_expected_revision(
    expected_revision: u64,
    current_revision: u64,
) -> Result<(), RaceConfigRepoError> {
    if expected_revision == current_revision {
        return Ok(());
    }

    Err(RaceConfigRepoError::RevisionMismatch {
        expected: expected_revision,
        actual: current_revision,
    })
}

async fn read_revision_for_share(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<u64, RaceConfigRepoError> {
    let row =
        sqlx::query!("SELECT revision FROM race_config_state WHERE singleton_key = TRUE FOR SHARE")
            .fetch_optional(&mut **tx)
            .await?;
    let revision_i64 = row.map(|r| r.revision);
    decode_revision_row(revision_i64)
}

async fn read_revision_for_update(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<u64, RaceConfigRepoError> {
    let row = sqlx::query!(
        "SELECT revision FROM race_config_state WHERE singleton_key = TRUE FOR UPDATE"
    )
    .fetch_optional(&mut **tx)
    .await?;
    let revision_i64 = row.map(|r| r.revision);
    decode_revision_row(revision_i64)
}

fn decode_revision_row(revision_i64: Option<i64>) -> Result<u64, RaceConfigRepoError> {
    let revision_i64 = revision_i64.ok_or(RaceConfigRepoError::StateMissing)?;
    u64::try_from(revision_i64).map_err(|_| RaceConfigRepoError::RevisionOverflow)
}

async fn bump_revision(
    tx: &mut Transaction<'_, Postgres>,
    current_revision: u64,
) -> Result<u64, RaceConfigRepoError> {
    let next_revision = current_revision
        .checked_add(1)
        .ok_or(RaceConfigRepoError::RevisionOverflow)?;
    let next_revision_i64 =
        i64::try_from(next_revision).map_err(|_| RaceConfigRepoError::RevisionOverflow)?;

    sqlx::query!(
        "UPDATE race_config_state SET revision = $1 WHERE singleton_key = TRUE",
        next_revision_i64
    )
    .execute(&mut **tx)
    .await?;

    Ok(next_revision)
}

async fn exists_by_id(
    tx: &mut Transaction<'_, Postgres>,
    race_id: &str,
) -> Result<bool, RaceConfigRepoError> {
    let row = sqlx::query!(
        "SELECT race_id FROM race_config_schedule WHERE race_id = $1",
        race_id
    )
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.is_some())
}

async fn insert_config(
    tx: &mut Transaction<'_, Postgres>,
    race: &RaceConfigRecord,
) -> Result<(), RaceConfigRepoError> {
    let start_placement_mode = DbStartPlacementMode::try_from(race.config.start_placement_mode)?;
    let time_of_day_preset = DbTimeOfDayPreset::try_from(race.config.time_of_day_preset)?;
    let race_duration_sec_i32 = i32::try_from(race.config.race_duration_sec).map_err(|_| {
        RaceConfigRepoError::NumericOutOfRange {
            race_id: race.race_id.clone(),
            field: "race_duration_sec",
        }
    })?;
    let ends_at_ms = compute_ends_at_ms(
        race.config.starts_at_ms,
        race.config.race_duration_sec,
        &race.race_id,
    )?;

    sqlx::query!(
        r#"
        INSERT INTO race_config_schedule (
            race_id,
            race_name,
            starts_at_ms,
            ends_at_ms,
            race_duration_sec,
            map_id,
            start_placement_mode,
            points_multiplier_fixed,
            time_of_day_preset
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9
        )
        "#,
        &race.race_id,
        &race.config.race_name,
        race.config.starts_at_ms,
        ends_at_ms,
        race_duration_sec_i32,
        &race.config.map_id,
        start_placement_mode as _,
        race.config.points_multiplier_fixed,
        time_of_day_preset as _,
    )
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn replace_config(
    tx: &mut Transaction<'_, Postgres>,
    race: &RaceConfigRecord,
) -> Result<(), RaceConfigRepoError> {
    let start_placement_mode = DbStartPlacementMode::try_from(race.config.start_placement_mode)?;
    let time_of_day_preset = DbTimeOfDayPreset::try_from(race.config.time_of_day_preset)?;
    let race_duration_sec_i32 = i32::try_from(race.config.race_duration_sec).map_err(|_| {
        RaceConfigRepoError::NumericOutOfRange {
            race_id: race.race_id.clone(),
            field: "race_duration_sec",
        }
    })?;
    let ends_at_ms = compute_ends_at_ms(
        race.config.starts_at_ms,
        race.config.race_duration_sec,
        &race.race_id,
    )?;

    sqlx::query!(
        r#"
        UPDATE race_config_schedule
        SET race_name = $2,
            starts_at_ms = $3,
            ends_at_ms = $4,
            race_duration_sec = $5,
            map_id = $6,
            start_placement_mode = $7,
            points_multiplier_fixed = $8,
            time_of_day_preset = $9
        WHERE race_id = $1
        "#,
        &race.race_id,
        &race.config.race_name,
        race.config.starts_at_ms,
        ends_at_ms,
        race_duration_sec_i32,
        &race.config.map_id,
        start_placement_mode as _,
        race.config.points_multiplier_fixed,
        time_of_day_preset as _,
    )
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn read_configs(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<RaceConfigRecord>, RaceConfigRepoError> {
    let rows = sqlx::query!(
        r#"
        SELECT
            race_id,
            race_name,
            starts_at_ms,
            race_duration_sec,
            map_id,
            start_placement_mode AS "start_placement_mode: DbStartPlacementMode",
            points_multiplier_fixed,
            time_of_day_preset AS "time_of_day_preset: DbTimeOfDayPreset"
        FROM race_config_schedule
        ORDER BY starts_at_ms ASC, race_id ASC
        "#,
    )
    .fetch_all(&mut **tx)
    .await?;

    let mut races = Vec::with_capacity(rows.len());
    for row in rows {
        let race_id = row.race_id;
        let race_duration_sec = u32::try_from(row.race_duration_sec).map_err(|_| {
            RaceConfigRepoError::NumericOutOfRange {
                race_id: race_id.clone(),
                field: "race_duration_sec",
            }
        })?;

        races.push(RaceConfigRecord {
            race_id,
            config: RaceConfigInputRecord {
                race_name: row.race_name,
                starts_at_ms: row.starts_at_ms,
                race_duration_sec,
                map_id: row.map_id,
                start_placement_mode: row.start_placement_mode.into(),
                points_multiplier_fixed: row.points_multiplier_fixed,
                time_of_day_preset: row.time_of_day_preset.into(),
            },
        });
    }

    Ok(races)
}

fn compute_ends_at_ms(
    starts_at_ms: i64,
    race_duration_sec: u32,
    race_id: &str,
) -> Result<i64, RaceConfigRepoError> {
    let duration_ms = i64::from(race_duration_sec)
        .checked_mul(1000)
        .ok_or_else(|| RaceConfigRepoError::NumericOutOfRange {
            race_id: race_id.to_string(),
            field: "race_duration_sec",
        })?;
    starts_at_ms
        .checked_add(duration_ms)
        .ok_or_else(|| RaceConfigRepoError::NumericOutOfRange {
            race_id: race_id.to_string(),
            field: "ends_at_ms",
        })
}
