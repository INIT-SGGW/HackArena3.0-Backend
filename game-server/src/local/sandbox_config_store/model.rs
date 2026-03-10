use serde::{Deserialize, Serialize};

use super::error::LocalSandboxConfigStoreError;

/// Snapshot of persisted local sandbox configuration data.
#[derive(Debug, Clone, PartialEq)]
pub struct LocalSandboxConfigSnapshot {
    pub revision: u64,
    pub sandboxes: Vec<LocalSandboxConfigRecord>,
}

/// Persisted local sandbox config entry with stable identifier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalSandboxConfigRecord {
    pub sandbox_id: String,
    pub config: LocalSandboxConfigInputRecord,
}

/// Persisted local sandbox configuration fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalSandboxConfigInputRecord {
    pub sandbox_name: String,
    pub map_id: String,
    pub time_of_day: LocalTimeOfDaySettingsRecord,
    pub ghost_mode: Option<LocalGhostModeSettingsRecord>,
    pub weather: LocalWeatherSettingsRecord,
    pub spawn_mode: LocalSandboxSpawnModeRecord,
}

/// Persisted local time-of-day settings.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalTimeOfDaySettingsRecord {
    pub mode: LocalTimeOfDayModeRecord,
    pub fixed_preset: RuntimeTimeOfDayPresetRecord,
}

/// Persisted local weather settings.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalWeatherSettingsRecord {
    pub weather_type: WeatherTypeRecord,
    pub temperature_c: i32,
}

/// Persisted local ghost-mode settings.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct LocalGhostModeSettingsRecord {
    pub enabled: bool,
    pub enter_speed_max_mps: f32,
    pub exit_speed_min_mps: f32,
    pub enter_delay_ms: u32,
    pub exit_delay_ms: u32,
    pub until_completed_laps: u32,
    pub vehicle_overlap_exit_delay_ms: u32,
}

/// Local time-of-day mode persisted in local config file.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LocalTimeOfDayModeRecord {
    FixedPreset,
    AutoByLocalTime,
}

/// Runtime time-of-day preset persisted in local config file.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTimeOfDayPresetRecord {
    Morning,
    Noon,
    Evening,
    Night,
}

/// Weather type persisted in local config file.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WeatherTypeRecord {
    Clear,
    PartlyCloudy,
    Overcast,
    LightRain,
    MediumRain,
    HeavyRain,
}

/// Spawn mode persisted in local config file.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LocalSandboxSpawnModeRecord {
    StartLine,
    RandomOnTrack,
    InPit,
    RandomStartSlot,
}

/// Validates local sandbox configuration input record.
pub fn validate_local_sandbox_config_input(
    input: &LocalSandboxConfigInputRecord,
) -> Result<(), LocalSandboxConfigStoreError> {
    if input.sandbox_name.trim().is_empty() {
        return Err(LocalSandboxConfigStoreError::InvalidConfig {
            message: "sandbox_name must be non-empty".to_string(),
        });
    }
    if input.map_id.trim().is_empty() {
        return Err(LocalSandboxConfigStoreError::InvalidConfig {
            message: "map_id must be non-empty".to_string(),
        });
    }
    validate_time_of_day_settings(input.time_of_day)?;

    if let Some(ghost) = input.ghost_mode {
        if !ghost.enter_speed_max_mps.is_finite() || ghost.enter_speed_max_mps < 0.0 {
            return Err(LocalSandboxConfigStoreError::InvalidConfig {
                message: "ghost_mode.enter_speed_max_mps must be finite and >= 0".to_string(),
            });
        }
        if !ghost.exit_speed_min_mps.is_finite() || ghost.exit_speed_min_mps < 0.0 {
            return Err(LocalSandboxConfigStoreError::InvalidConfig {
                message: "ghost_mode.exit_speed_min_mps must be finite and >= 0".to_string(),
            });
        }
        if ghost.enter_speed_max_mps > ghost.exit_speed_min_mps {
            return Err(LocalSandboxConfigStoreError::InvalidConfig {
                message: "ghost_mode.enter_speed_max_mps must be <= ghost_mode.exit_speed_min_mps"
                    .to_string(),
            });
        }
    }

    Ok(())
}

fn validate_time_of_day_settings(
    settings: LocalTimeOfDaySettingsRecord,
) -> Result<(), LocalSandboxConfigStoreError> {
    if matches!(settings.mode, LocalTimeOfDayModeRecord::FixedPreset)
        && !matches!(
            settings.fixed_preset,
            RuntimeTimeOfDayPresetRecord::Morning
                | RuntimeTimeOfDayPresetRecord::Noon
                | RuntimeTimeOfDayPresetRecord::Evening
                | RuntimeTimeOfDayPresetRecord::Night
        )
    {
        return Err(LocalSandboxConfigStoreError::InvalidConfig {
            message: "time_of_day.fixed_preset must be specified for fixed mode".to_string(),
        });
    }
    Ok(())
}
