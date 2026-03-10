use serde::{Deserialize, Serialize};

use super::error::LocalSandboxConfigStoreError;

const MAX_SANDBOX_NAME_LEN_CHARS: usize = 64;
const MAX_GHOST_DELAY_MS: u32 = 600_000;
const MAX_GHOST_UNTIL_COMPLETED_LAPS: u32 = 100_000;

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
    let sandbox_name = input.sandbox_name.trim();
    if sandbox_name.is_empty() {
        return Err(LocalSandboxConfigStoreError::InvalidConfig {
            message: "sandbox_name must be non-empty".to_string(),
        });
    }
    if sandbox_name.chars().count() > MAX_SANDBOX_NAME_LEN_CHARS {
        return Err(LocalSandboxConfigStoreError::InvalidConfig {
            message: format!(
                "sandbox_name must be at most {MAX_SANDBOX_NAME_LEN_CHARS} characters"
            ),
        });
    }

    let map_id = input.map_id.trim();
    if map_id.is_empty() {
        return Err(LocalSandboxConfigStoreError::InvalidConfig {
            message: "map_id must be non-empty".to_string(),
        });
    }
    if !map_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(LocalSandboxConfigStoreError::InvalidConfig {
            message: "map_id contains invalid characters".to_string(),
        });
    }
    validate_time_of_day_settings(input.time_of_day)?;
    validate_local_weather_settings(input.weather)?;

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
        if ghost.enter_delay_ms > MAX_GHOST_DELAY_MS {
            return Err(LocalSandboxConfigStoreError::InvalidConfig {
                message: format!("ghost_mode.enter_delay_ms must be <= {MAX_GHOST_DELAY_MS}"),
            });
        }
        if ghost.exit_delay_ms > MAX_GHOST_DELAY_MS {
            return Err(LocalSandboxConfigStoreError::InvalidConfig {
                message: format!("ghost_mode.exit_delay_ms must be <= {MAX_GHOST_DELAY_MS}"),
            });
        }
        if ghost.vehicle_overlap_exit_delay_ms > MAX_GHOST_DELAY_MS {
            return Err(LocalSandboxConfigStoreError::InvalidConfig {
                message: format!(
                    "ghost_mode.vehicle_overlap_exit_delay_ms must be <= {MAX_GHOST_DELAY_MS}"
                ),
            });
        }
        if ghost.until_completed_laps > MAX_GHOST_UNTIL_COMPLETED_LAPS {
            return Err(LocalSandboxConfigStoreError::InvalidConfig {
                message: format!(
                    "ghost_mode.until_completed_laps must be <= {MAX_GHOST_UNTIL_COMPLETED_LAPS}"
                ),
            });
        }
    }

    Ok(())
}

/// Validates local weather settings.
pub fn validate_local_weather_settings(
    settings: LocalWeatherSettingsRecord,
) -> Result<(), LocalSandboxConfigStoreError> {
    if !(1..=30).contains(&settings.temperature_c) {
        return Err(LocalSandboxConfigStoreError::InvalidConfig {
            message: "weather.temperature_c must be in range 1..=30".to_string(),
        });
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
