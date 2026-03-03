//! Sandbox config repository for persisted admin configuration.

use proto::race::v1::RuntimeTimeOfDayPreset;
use sqlx::{PgPool, Postgres, Transaction};
use thiserror::Error;

/// Persisted ghost mode settings.
#[derive(Debug, Clone, PartialEq)]
pub struct GhostModeSettingsRecord {
    pub enabled: bool,
    pub enter_speed_max_mps: f32,
    pub exit_speed_min_mps: f32,
    pub enter_delay_ms: u32,
    pub exit_delay_ms: u32,
    pub until_completed_laps: u32,
    pub vehicle_overlap_exit_delay_ms: u32,
}

/// Persisted sandbox config input fields.
#[derive(Debug, Clone, PartialEq)]
pub struct SandboxConfigInputRecord {
    pub sandbox_name: String,
    pub map_id: String,
    pub time_of_day_preset: RuntimeTimeOfDayPreset,
    pub ghost_mode: Option<GhostModeSettingsRecord>,
}

/// Persisted sandbox config entry with stable id.
#[derive(Debug, Clone, PartialEq)]
pub struct SandboxConfigRecord {
    pub sandbox_id: String,
    pub config: SandboxConfigInputRecord,
}

/// Full persisted snapshot used by admin API responses.
#[derive(Debug, Clone, PartialEq)]
pub struct SandboxConfigSnapshot {
    pub revision: u64,
    pub sandboxes: Vec<SandboxConfigRecord>,
}

/// Repository error surface for sandbox config persistence.
#[derive(Debug, Error)]
pub enum SandboxConfigRepoError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error("sandbox config state row is missing")]
    StateMissing,
    #[error("revision mismatch: expected {expected}, actual {actual}")]
    RevisionMismatch { expected: u64, actual: u64 },
    #[error("sandbox config already exists: {sandbox_id}")]
    AlreadyExists { sandbox_id: String },
    #[error("sandbox config not found: {sandbox_id}")]
    NotFound { sandbox_id: String },
    #[error("time_of_day_preset must be specified")]
    InvalidTimeOfDayPreset,
    #[error("persisted ghost mode data is partial for sandbox: {sandbox_id}")]
    PartialGhostData { sandbox_id: String },
    #[error("persisted numeric value is out of range for sandbox: {sandbox_id}")]
    NumericOutOfRange { sandbox_id: String },
    #[error("revision overflow")]
    RevisionOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "time_of_day_preset", rename_all = "snake_case")]
enum DbTimeOfDayPreset {
    Morning,
    Noon,
    Evening,
    Night,
}

impl From<DbTimeOfDayPreset> for RuntimeTimeOfDayPreset {
    fn from(value: DbTimeOfDayPreset) -> Self {
        match value {
            DbTimeOfDayPreset::Morning => RuntimeTimeOfDayPreset::Morning,
            DbTimeOfDayPreset::Noon => RuntimeTimeOfDayPreset::Noon,
            DbTimeOfDayPreset::Evening => RuntimeTimeOfDayPreset::Evening,
            DbTimeOfDayPreset::Night => RuntimeTimeOfDayPreset::Night,
        }
    }
}

impl TryFrom<RuntimeTimeOfDayPreset> for DbTimeOfDayPreset {
    type Error = SandboxConfigRepoError;

    fn try_from(value: RuntimeTimeOfDayPreset) -> Result<Self, Self::Error> {
        match value {
            RuntimeTimeOfDayPreset::Morning => Ok(DbTimeOfDayPreset::Morning),
            RuntimeTimeOfDayPreset::Noon => Ok(DbTimeOfDayPreset::Noon),
            RuntimeTimeOfDayPreset::Evening => Ok(DbTimeOfDayPreset::Evening),
            RuntimeTimeOfDayPreset::Night => Ok(DbTimeOfDayPreset::Night),
            RuntimeTimeOfDayPreset::Unspecified => {
                Err(SandboxConfigRepoError::InvalidTimeOfDayPreset)
            }
        }
    }
}

/// Repository for sandbox config snapshot and CRUD updates.
#[derive(Clone)]
pub struct SandboxConfigRepo {
    pool: PgPool,
}

impl SandboxConfigRepo {
    /// Creates a repository backed by the provided Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Reads current revision and full sandbox config list.
    pub async fn get_snapshot(&self) -> Result<SandboxConfigSnapshot, SandboxConfigRepoError> {
        let mut tx = self.pool.begin().await?;
        let revision = read_revision_for_share(&mut tx).await?;
        let sandboxes = read_configs(&mut tx).await?;
        tx.commit().await?;

        Ok(SandboxConfigSnapshot {
            revision,
            sandboxes,
        })
    }

    /// Inserts new sandbox config and bumps revision.
    pub async fn create_config(
        &self,
        expected_revision: u64,
        sandbox: &SandboxConfigRecord,
    ) -> Result<u64, SandboxConfigRepoError> {
        let mut tx = self.pool.begin().await?;
        let current_revision = read_revision_for_update(&mut tx).await?;
        ensure_expected_revision(expected_revision, current_revision)?;

        if exists_by_id(&mut tx, &sandbox.sandbox_id).await? {
            return Err(SandboxConfigRepoError::AlreadyExists {
                sandbox_id: sandbox.sandbox_id.clone(),
            });
        }

        insert_config(&mut tx, sandbox).await?;
        let next_revision = bump_revision(&mut tx, current_revision).await?;
        tx.commit().await?;
        Ok(next_revision)
    }

    /// Updates existing sandbox config and bumps revision.
    pub async fn update_config(
        &self,
        expected_revision: u64,
        sandbox: &SandboxConfigRecord,
    ) -> Result<u64, SandboxConfigRepoError> {
        let mut tx = self.pool.begin().await?;
        let current_revision = read_revision_for_update(&mut tx).await?;
        ensure_expected_revision(expected_revision, current_revision)?;

        if !exists_by_id(&mut tx, &sandbox.sandbox_id).await? {
            return Err(SandboxConfigRepoError::NotFound {
                sandbox_id: sandbox.sandbox_id.clone(),
            });
        }

        replace_config(&mut tx, sandbox).await?;
        let next_revision = bump_revision(&mut tx, current_revision).await?;
        tx.commit().await?;
        Ok(next_revision)
    }

    /// Deletes existing sandbox config and bumps revision.
    pub async fn delete_config(
        &self,
        expected_revision: u64,
        sandbox_id: &str,
    ) -> Result<u64, SandboxConfigRepoError> {
        let mut tx = self.pool.begin().await?;
        let current_revision = read_revision_for_update(&mut tx).await?;
        ensure_expected_revision(expected_revision, current_revision)?;

        let delete_count = sqlx::query!(
            "DELETE FROM sandbox_configs WHERE sandbox_id = $1",
            sandbox_id
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();

        if delete_count == 0 {
            return Err(SandboxConfigRepoError::NotFound {
                sandbox_id: sandbox_id.to_string(),
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
) -> Result<(), SandboxConfigRepoError> {
    if expected_revision == current_revision {
        return Ok(());
    }

    Err(SandboxConfigRepoError::RevisionMismatch {
        expected: expected_revision,
        actual: current_revision,
    })
}

async fn read_revision_for_share(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<u64, SandboxConfigRepoError> {
    let row = sqlx::query!(
        "SELECT revision FROM sandbox_config_state WHERE singleton_key = TRUE FOR SHARE"
    )
    .fetch_optional(&mut **tx)
    .await?;
    decode_revision_row(row.map(|r| r.revision))
}

async fn read_revision_for_update(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<u64, SandboxConfigRepoError> {
    let row = sqlx::query!(
        "SELECT revision FROM sandbox_config_state WHERE singleton_key = TRUE FOR UPDATE"
    )
    .fetch_optional(&mut **tx)
    .await?;
    decode_revision_row(row.map(|r| r.revision))
}

fn decode_revision_row(revision_i64: Option<i64>) -> Result<u64, SandboxConfigRepoError> {
    let revision_i64 = revision_i64.ok_or(SandboxConfigRepoError::StateMissing)?;
    u64::try_from(revision_i64).map_err(|_| SandboxConfigRepoError::RevisionOverflow)
}

async fn bump_revision(
    tx: &mut Transaction<'_, Postgres>,
    current_revision: u64,
) -> Result<u64, SandboxConfigRepoError> {
    let next_revision = current_revision
        .checked_add(1)
        .ok_or(SandboxConfigRepoError::RevisionOverflow)?;
    let next_revision_i64 =
        i64::try_from(next_revision).map_err(|_| SandboxConfigRepoError::RevisionOverflow)?;

    sqlx::query!(
        "UPDATE sandbox_config_state SET revision = $1 WHERE singleton_key = TRUE",
        next_revision_i64
    )
    .execute(&mut **tx)
    .await?;

    Ok(next_revision)
}

async fn exists_by_id(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: &str,
) -> Result<bool, SandboxConfigRepoError> {
    let row = sqlx::query!(
        "SELECT sandbox_id FROM sandbox_configs WHERE sandbox_id = $1",
        sandbox_id
    )
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.is_some())
}

async fn insert_config(
    tx: &mut Transaction<'_, Postgres>,
    sandbox: &SandboxConfigRecord,
) -> Result<(), SandboxConfigRepoError> {
    let time_of_day_preset = DbTimeOfDayPreset::try_from(sandbox.config.time_of_day_preset)?;
    let ghost = DbGhostModeFields::from_record_opt(sandbox.config.ghost_mode.as_ref())?;

    sqlx::query!(
        r#"
        INSERT INTO sandbox_configs (
            sandbox_id,
            sandbox_name,
            map_id,
            time_of_day_preset,
            ghost_mode_enabled,
            ghost_enter_speed_max_mps,
            ghost_exit_speed_min_mps,
            ghost_enter_delay_ms,
            ghost_exit_delay_ms,
            ghost_until_completed_laps,
            ghost_vehicle_overlap_exit_delay_ms
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        )
        "#,
        sandbox.sandbox_id,
        sandbox.config.sandbox_name,
        sandbox.config.map_id,
        time_of_day_preset as _,
        ghost.enabled,
        ghost.enter_speed_max_mps,
        ghost.exit_speed_min_mps,
        ghost.enter_delay_ms,
        ghost.exit_delay_ms,
        ghost.until_completed_laps,
        ghost.vehicle_overlap_exit_delay_ms,
    )
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn replace_config(
    tx: &mut Transaction<'_, Postgres>,
    sandbox: &SandboxConfigRecord,
) -> Result<(), SandboxConfigRepoError> {
    let time_of_day_preset = DbTimeOfDayPreset::try_from(sandbox.config.time_of_day_preset)?;
    let ghost = DbGhostModeFields::from_record_opt(sandbox.config.ghost_mode.as_ref())?;

    sqlx::query!(
        r#"
        UPDATE sandbox_configs
        SET sandbox_name = $2,
            map_id = $3,
            time_of_day_preset = $4,
            ghost_mode_enabled = $5,
            ghost_enter_speed_max_mps = $6,
            ghost_exit_speed_min_mps = $7,
            ghost_enter_delay_ms = $8,
            ghost_exit_delay_ms = $9,
            ghost_until_completed_laps = $10,
            ghost_vehicle_overlap_exit_delay_ms = $11
        WHERE sandbox_id = $1
        "#,
        sandbox.sandbox_id,
        sandbox.config.sandbox_name,
        sandbox.config.map_id,
        time_of_day_preset as _,
        ghost.enabled,
        ghost.enter_speed_max_mps,
        ghost.exit_speed_min_mps,
        ghost.enter_delay_ms,
        ghost.exit_delay_ms,
        ghost.until_completed_laps,
        ghost.vehicle_overlap_exit_delay_ms,
    )
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn read_configs(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<SandboxConfigRecord>, SandboxConfigRepoError> {
    let rows = sqlx::query!(
        r#"
        SELECT
            sandbox_id,
            sandbox_name,
            map_id,
            time_of_day_preset AS "time_of_day_preset: DbTimeOfDayPreset",
            ghost_mode_enabled,
            ghost_enter_speed_max_mps,
            ghost_exit_speed_min_mps,
            ghost_enter_delay_ms,
            ghost_exit_delay_ms,
            ghost_until_completed_laps,
            ghost_vehicle_overlap_exit_delay_ms
        FROM sandbox_configs
        ORDER BY sandbox_id ASC
        "#,
    )
    .fetch_all(&mut **tx)
    .await?;

    let mut sandboxes = Vec::with_capacity(rows.len());
    for row in rows {
        let sandbox_id = row.sandbox_id;
        let ghost_mode = decode_ghost_mode(
            &sandbox_id,
            row.ghost_mode_enabled,
            row.ghost_enter_speed_max_mps,
            row.ghost_exit_speed_min_mps,
            row.ghost_enter_delay_ms,
            row.ghost_exit_delay_ms,
            row.ghost_until_completed_laps,
            row.ghost_vehicle_overlap_exit_delay_ms,
        )?;

        sandboxes.push(SandboxConfigRecord {
            sandbox_id,
            config: SandboxConfigInputRecord {
                sandbox_name: row.sandbox_name,
                map_id: row.map_id,
                time_of_day_preset: row.time_of_day_preset.into(),
                ghost_mode,
            },
        });
    }

    Ok(sandboxes)
}

#[allow(clippy::too_many_arguments)]
fn decode_ghost_mode(
    sandbox_id: &str,
    enabled: Option<bool>,
    enter_speed_max_mps: Option<f32>,
    exit_speed_min_mps: Option<f32>,
    enter_delay_ms_raw: Option<i64>,
    exit_delay_ms_raw: Option<i64>,
    until_completed_laps_raw: Option<i64>,
    vehicle_overlap_exit_delay_ms_raw: Option<i64>,
) -> Result<Option<GhostModeSettingsRecord>, SandboxConfigRepoError> {
    let all_none = enabled.is_none()
        && enter_speed_max_mps.is_none()
        && exit_speed_min_mps.is_none()
        && enter_delay_ms_raw.is_none()
        && exit_delay_ms_raw.is_none()
        && until_completed_laps_raw.is_none()
        && vehicle_overlap_exit_delay_ms_raw.is_none();
    if all_none {
        return Ok(None);
    }

    let Some(enabled) = enabled else {
        return Err(SandboxConfigRepoError::PartialGhostData {
            sandbox_id: sandbox_id.to_string(),
        });
    };
    let Some(enter_speed_max_mps) = enter_speed_max_mps else {
        return Err(SandboxConfigRepoError::PartialGhostData {
            sandbox_id: sandbox_id.to_string(),
        });
    };
    let Some(exit_speed_min_mps) = exit_speed_min_mps else {
        return Err(SandboxConfigRepoError::PartialGhostData {
            sandbox_id: sandbox_id.to_string(),
        });
    };
    let Some(enter_delay_ms_raw) = enter_delay_ms_raw else {
        return Err(SandboxConfigRepoError::PartialGhostData {
            sandbox_id: sandbox_id.to_string(),
        });
    };
    let Some(exit_delay_ms_raw) = exit_delay_ms_raw else {
        return Err(SandboxConfigRepoError::PartialGhostData {
            sandbox_id: sandbox_id.to_string(),
        });
    };
    let Some(until_completed_laps_raw) = until_completed_laps_raw else {
        return Err(SandboxConfigRepoError::PartialGhostData {
            sandbox_id: sandbox_id.to_string(),
        });
    };
    let Some(vehicle_overlap_exit_delay_ms_raw) = vehicle_overlap_exit_delay_ms_raw else {
        return Err(SandboxConfigRepoError::PartialGhostData {
            sandbox_id: sandbox_id.to_string(),
        });
    };

    let enter_delay_ms = u32::try_from(enter_delay_ms_raw).map_err(|_| {
        SandboxConfigRepoError::NumericOutOfRange {
            sandbox_id: sandbox_id.to_string(),
        }
    })?;
    let exit_delay_ms = u32::try_from(exit_delay_ms_raw).map_err(|_| {
        SandboxConfigRepoError::NumericOutOfRange {
            sandbox_id: sandbox_id.to_string(),
        }
    })?;
    let until_completed_laps = u32::try_from(until_completed_laps_raw).map_err(|_| {
        SandboxConfigRepoError::NumericOutOfRange {
            sandbox_id: sandbox_id.to_string(),
        }
    })?;
    let vehicle_overlap_exit_delay_ms = u32::try_from(vehicle_overlap_exit_delay_ms_raw).map_err(
        |_| SandboxConfigRepoError::NumericOutOfRange {
            sandbox_id: sandbox_id.to_string(),
        },
    )?;

    Ok(Some(GhostModeSettingsRecord {
        enabled,
        enter_speed_max_mps,
        exit_speed_min_mps,
        enter_delay_ms,
        exit_delay_ms,
        until_completed_laps,
        vehicle_overlap_exit_delay_ms,
    }))
}

#[derive(Debug, Clone)]
struct DbGhostModeFields {
    enabled: Option<bool>,
    enter_speed_max_mps: Option<f32>,
    exit_speed_min_mps: Option<f32>,
    enter_delay_ms: Option<i64>,
    exit_delay_ms: Option<i64>,
    until_completed_laps: Option<i64>,
    vehicle_overlap_exit_delay_ms: Option<i64>,
}

impl DbGhostModeFields {
    fn from_record_opt(
        record: Option<&GhostModeSettingsRecord>,
    ) -> Result<Self, SandboxConfigRepoError> {
        let Some(record) = record else {
            return Ok(Self {
                enabled: None,
                enter_speed_max_mps: None,
                exit_speed_min_mps: None,
                enter_delay_ms: None,
                exit_delay_ms: None,
                until_completed_laps: None,
                vehicle_overlap_exit_delay_ms: None,
            });
        };

        Ok(Self {
            enabled: Some(record.enabled),
            enter_speed_max_mps: Some(record.enter_speed_max_mps),
            exit_speed_min_mps: Some(record.exit_speed_min_mps),
            enter_delay_ms: Some(i64::from(record.enter_delay_ms)),
            exit_delay_ms: Some(i64::from(record.exit_delay_ms)),
            until_completed_laps: Some(i64::from(record.until_completed_laps)),
            vehicle_overlap_exit_delay_ms: Some(i64::from(record.vehicle_overlap_exit_delay_ms)),
        })
    }
}
