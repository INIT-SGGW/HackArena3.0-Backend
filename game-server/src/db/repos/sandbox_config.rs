//! Sandbox config repository for persisted admin configuration.

use proto::race::v1::{GhostModeConditionLogic, RuntimeTimeOfDayPreset};
use sqlx::{PgPool, Postgres, Transaction};
use thiserror::Error;

/// Persisted ghost mode settings.
#[derive(Debug, Clone, PartialEq)]
pub struct GhostModeSettingsRecord {
    pub enabled: bool,
    pub min_speed_enter_mps: f32,
    pub min_speed_exit_mps: f32,
    pub enter_delay_ms: u32,
    pub exit_delay_ms: u32,
    pub min_completed_laps: u32,
    pub condition_logic: GhostModeConditionLogic,
    pub overlap_exit_delay_ms: u32,
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
    #[error("ghost_mode.condition_logic must be specified")]
    InvalidGhostConditionLogic,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "ghost_mode_condition_logic", rename_all = "snake_case")]
enum DbGhostModeConditionLogic {
    And,
    Or,
}

impl From<DbGhostModeConditionLogic> for GhostModeConditionLogic {
    fn from(value: DbGhostModeConditionLogic) -> Self {
        match value {
            DbGhostModeConditionLogic::And => GhostModeConditionLogic::And,
            DbGhostModeConditionLogic::Or => GhostModeConditionLogic::Or,
        }
    }
}

impl TryFrom<GhostModeConditionLogic> for DbGhostModeConditionLogic {
    type Error = SandboxConfigRepoError;

    fn try_from(value: GhostModeConditionLogic) -> Result<Self, Self::Error> {
        match value {
            GhostModeConditionLogic::And => Ok(DbGhostModeConditionLogic::And),
            GhostModeConditionLogic::Or => Ok(DbGhostModeConditionLogic::Or),
            GhostModeConditionLogic::Unspecified => {
                Err(SandboxConfigRepoError::InvalidGhostConditionLogic)
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
            ghost_min_speed_enter_mps,
            ghost_min_speed_exit_mps,
            ghost_enter_delay_ms,
            ghost_exit_delay_ms,
            ghost_min_completed_laps,
            ghost_condition_logic,
            ghost_overlap_exit_delay_ms
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        )
        "#,
        sandbox.sandbox_id,
        sandbox.config.sandbox_name,
        sandbox.config.map_id,
        time_of_day_preset as _,
        ghost.enabled,
        ghost.min_speed_enter_mps,
        ghost.min_speed_exit_mps,
        ghost.enter_delay_ms,
        ghost.exit_delay_ms,
        ghost.min_completed_laps,
        ghost.condition_logic as _,
        ghost.overlap_exit_delay_ms,
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
            ghost_min_speed_enter_mps = $6,
            ghost_min_speed_exit_mps = $7,
            ghost_enter_delay_ms = $8,
            ghost_exit_delay_ms = $9,
            ghost_min_completed_laps = $10,
            ghost_condition_logic = $11,
            ghost_overlap_exit_delay_ms = $12
        WHERE sandbox_id = $1
        "#,
        sandbox.sandbox_id,
        sandbox.config.sandbox_name,
        sandbox.config.map_id,
        time_of_day_preset as _,
        ghost.enabled,
        ghost.min_speed_enter_mps,
        ghost.min_speed_exit_mps,
        ghost.enter_delay_ms,
        ghost.exit_delay_ms,
        ghost.min_completed_laps,
        ghost.condition_logic as _,
        ghost.overlap_exit_delay_ms,
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
            ghost_min_speed_enter_mps,
            ghost_min_speed_exit_mps,
            ghost_enter_delay_ms,
            ghost_exit_delay_ms,
            ghost_min_completed_laps,
            ghost_condition_logic AS "ghost_condition_logic: DbGhostModeConditionLogic",
            ghost_overlap_exit_delay_ms
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
            row.ghost_min_speed_enter_mps,
            row.ghost_min_speed_exit_mps,
            row.ghost_enter_delay_ms,
            row.ghost_exit_delay_ms,
            row.ghost_min_completed_laps,
            row.ghost_condition_logic,
            row.ghost_overlap_exit_delay_ms,
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
    min_speed_enter_mps: Option<f32>,
    min_speed_exit_mps: Option<f32>,
    enter_delay_ms_raw: Option<i64>,
    exit_delay_ms_raw: Option<i64>,
    min_completed_laps_raw: Option<i64>,
    condition_logic: Option<DbGhostModeConditionLogic>,
    overlap_exit_delay_ms_raw: Option<i64>,
) -> Result<Option<GhostModeSettingsRecord>, SandboxConfigRepoError> {
    let all_none = enabled.is_none()
        && min_speed_enter_mps.is_none()
        && min_speed_exit_mps.is_none()
        && enter_delay_ms_raw.is_none()
        && exit_delay_ms_raw.is_none()
        && min_completed_laps_raw.is_none()
        && condition_logic.is_none()
        && overlap_exit_delay_ms_raw.is_none();
    if all_none {
        return Ok(None);
    }

    let Some(enabled) = enabled else {
        return Err(SandboxConfigRepoError::PartialGhostData {
            sandbox_id: sandbox_id.to_string(),
        });
    };
    let Some(min_speed_enter_mps) = min_speed_enter_mps else {
        return Err(SandboxConfigRepoError::PartialGhostData {
            sandbox_id: sandbox_id.to_string(),
        });
    };
    let Some(min_speed_exit_mps) = min_speed_exit_mps else {
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
    let Some(min_completed_laps_raw) = min_completed_laps_raw else {
        return Err(SandboxConfigRepoError::PartialGhostData {
            sandbox_id: sandbox_id.to_string(),
        });
    };
    let Some(condition_logic) = condition_logic else {
        return Err(SandboxConfigRepoError::PartialGhostData {
            sandbox_id: sandbox_id.to_string(),
        });
    };
    let Some(overlap_exit_delay_ms_raw) = overlap_exit_delay_ms_raw else {
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
    let min_completed_laps = u32::try_from(min_completed_laps_raw).map_err(|_| {
        SandboxConfigRepoError::NumericOutOfRange {
            sandbox_id: sandbox_id.to_string(),
        }
    })?;
    let overlap_exit_delay_ms = u32::try_from(overlap_exit_delay_ms_raw).map_err(|_| {
        SandboxConfigRepoError::NumericOutOfRange {
            sandbox_id: sandbox_id.to_string(),
        }
    })?;

    Ok(Some(GhostModeSettingsRecord {
        enabled,
        min_speed_enter_mps,
        min_speed_exit_mps,
        enter_delay_ms,
        exit_delay_ms,
        min_completed_laps,
        condition_logic: condition_logic.into(),
        overlap_exit_delay_ms,
    }))
}

#[derive(Debug, Clone)]
struct DbGhostModeFields {
    enabled: Option<bool>,
    min_speed_enter_mps: Option<f32>,
    min_speed_exit_mps: Option<f32>,
    enter_delay_ms: Option<i64>,
    exit_delay_ms: Option<i64>,
    min_completed_laps: Option<i64>,
    condition_logic: Option<DbGhostModeConditionLogic>,
    overlap_exit_delay_ms: Option<i64>,
}

impl DbGhostModeFields {
    fn from_record_opt(
        record: Option<&GhostModeSettingsRecord>,
    ) -> Result<Self, SandboxConfigRepoError> {
        let Some(record) = record else {
            return Ok(Self {
                enabled: None,
                min_speed_enter_mps: None,
                min_speed_exit_mps: None,
                enter_delay_ms: None,
                exit_delay_ms: None,
                min_completed_laps: None,
                condition_logic: None,
                overlap_exit_delay_ms: None,
            });
        };

        let condition_logic = DbGhostModeConditionLogic::try_from(record.condition_logic)?;

        Ok(Self {
            enabled: Some(record.enabled),
            min_speed_enter_mps: Some(record.min_speed_enter_mps),
            min_speed_exit_mps: Some(record.min_speed_exit_mps),
            enter_delay_ms: Some(i64::from(record.enter_delay_ms)),
            exit_delay_ms: Some(i64::from(record.exit_delay_ms)),
            min_completed_laps: Some(i64::from(record.min_completed_laps)),
            condition_logic: Some(condition_logic),
            overlap_exit_delay_ms: Some(i64::from(record.overlap_exit_delay_ms)),
        })
    }
}
