use proto::race::v1::{
    GhostModeSettings, LocalSandboxConfig, LocalSandboxConfigInput, LocalSandboxSpawnMode,
    LocalTimeOfDayMode, LocalTimeOfDaySettings, LocalWeatherSettings, RuntimeTimeOfDayPreset,
};
use proto::weather::v1::WeatherType;

use super::error::LocalSandboxConfigStoreError;
use super::model::{
    LocalGhostModeSettingsRecord, LocalSandboxConfigInputRecord, LocalSandboxConfigRecord,
    LocalSandboxSpawnModeRecord, LocalTimeOfDayModeRecord, LocalTimeOfDaySettingsRecord,
    LocalWeatherSettingsRecord, RuntimeTimeOfDayPresetRecord, WeatherTypeRecord,
    validate_local_sandbox_config_input,
};

/// Converts protobuf local sandbox input payload into local persisted record.
pub fn local_sandbox_input_from_proto(
    input: &LocalSandboxConfigInput,
) -> Result<LocalSandboxConfigInputRecord, LocalSandboxConfigStoreError> {
    let time_of_day =
        local_time_of_day_from_proto(input.time_of_day.as_ref().ok_or_else(|| {
            LocalSandboxConfigStoreError::InvalidConfig {
                message: "time_of_day is required".to_string(),
            }
        })?)?;
    let weather = local_weather_from_proto(input.weather.as_ref().ok_or_else(|| {
        LocalSandboxConfigStoreError::InvalidConfig {
            message: "weather is required".to_string(),
        }
    })?)?;
    let spawn_mode = LocalSandboxSpawnModeRecord::try_from(
        enum_from_i32::<LocalSandboxSpawnMode>(input.spawn_mode, "spawn_mode")?,
    )?;
    let ghost_mode = match input.ghost_mode.as_ref() {
        Some(ghost) => Some(local_ghost_mode_from_proto(ghost)?),
        None => None,
    };

    let record = LocalSandboxConfigInputRecord {
        sandbox_name: input.sandbox_name.clone(),
        map_id: input.map_id.clone(),
        time_of_day,
        ghost_mode,
        weather,
        spawn_mode,
    };
    validate_local_sandbox_config_input(&record)?;
    Ok(record)
}

/// Converts local persisted record into protobuf local sandbox input payload.
pub fn local_sandbox_input_to_proto(
    input: LocalSandboxConfigInputRecord,
) -> LocalSandboxConfigInput {
    LocalSandboxConfigInput {
        sandbox_name: input.sandbox_name,
        map_id: input.map_id,
        time_of_day: Some(local_time_of_day_to_proto(input.time_of_day)),
        ghost_mode: input.ghost_mode.map(local_ghost_mode_to_proto),
        weather: Some(local_weather_to_proto(input.weather)),
        spawn_mode: local_spawn_mode_to_proto(input.spawn_mode) as i32,
    }
}

/// Converts local persisted sandbox record into protobuf local sandbox payload.
pub fn local_sandbox_to_proto(record: LocalSandboxConfigRecord) -> LocalSandboxConfig {
    LocalSandboxConfig {
        sandbox_id: record.sandbox_id,
        config: Some(local_sandbox_input_to_proto(record.config)),
    }
}

fn local_time_of_day_from_proto(
    proto: &LocalTimeOfDaySettings,
) -> Result<LocalTimeOfDaySettingsRecord, LocalSandboxConfigStoreError> {
    let mode = LocalTimeOfDayModeRecord::try_from(enum_from_i32::<LocalTimeOfDayMode>(
        proto.mode,
        "time_of_day.mode",
    )?)?;
    let fixed_preset_proto =
        enum_from_i32::<RuntimeTimeOfDayPreset>(proto.fixed_preset, "time_of_day.fixed_preset")?;
    let fixed_preset = match (mode, fixed_preset_proto) {
        (LocalTimeOfDayModeRecord::FixedPreset, RuntimeTimeOfDayPreset::Unspecified) => {
            return Err(LocalSandboxConfigStoreError::InvalidConfig {
                message: "time_of_day.fixed_preset must be specified for fixed mode".to_string(),
            });
        }
        (_, RuntimeTimeOfDayPreset::Unspecified) => RuntimeTimeOfDayPresetRecord::Noon,
        (_, value) => RuntimeTimeOfDayPresetRecord::try_from(value)?,
    };

    Ok(LocalTimeOfDaySettingsRecord { mode, fixed_preset })
}

fn local_time_of_day_to_proto(settings: LocalTimeOfDaySettingsRecord) -> LocalTimeOfDaySettings {
    LocalTimeOfDaySettings {
        mode: local_time_of_day_mode_to_proto(settings.mode) as i32,
        fixed_preset: runtime_time_of_day_preset_to_proto(settings.fixed_preset) as i32,
    }
}

fn local_weather_from_proto(
    proto: &LocalWeatherSettings,
) -> Result<LocalWeatherSettingsRecord, LocalSandboxConfigStoreError> {
    let weather_type = WeatherTypeRecord::try_from(enum_from_i32::<WeatherType>(
        proto.weather_type,
        "weather.weather_type",
    )?)?;
    Ok(LocalWeatherSettingsRecord {
        weather_type,
        temperature_c: proto.temperature_c,
    })
}

fn local_weather_to_proto(settings: LocalWeatherSettingsRecord) -> LocalWeatherSettings {
    LocalWeatherSettings {
        weather_type: weather_type_to_proto(settings.weather_type) as i32,
        temperature_c: settings.temperature_c,
    }
}

fn local_ghost_mode_from_proto(
    proto: &GhostModeSettings,
) -> Result<LocalGhostModeSettingsRecord, LocalSandboxConfigStoreError> {
    let ghost = LocalGhostModeSettingsRecord {
        enabled: proto.enabled,
        enter_speed_max_mps: proto.enter_speed_max_mps,
        exit_speed_min_mps: proto.exit_speed_min_mps,
        enter_delay_ms: proto.enter_delay_ms,
        exit_delay_ms: proto.exit_delay_ms,
        until_completed_laps: proto.until_completed_laps,
        vehicle_overlap_exit_delay_ms: proto.vehicle_overlap_exit_delay_ms,
    };
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
    Ok(ghost)
}

fn local_ghost_mode_to_proto(settings: LocalGhostModeSettingsRecord) -> GhostModeSettings {
    GhostModeSettings {
        enabled: settings.enabled,
        enter_speed_max_mps: settings.enter_speed_max_mps,
        exit_speed_min_mps: settings.exit_speed_min_mps,
        enter_delay_ms: settings.enter_delay_ms,
        exit_delay_ms: settings.exit_delay_ms,
        until_completed_laps: settings.until_completed_laps,
        vehicle_overlap_exit_delay_ms: settings.vehicle_overlap_exit_delay_ms,
    }
}

fn local_time_of_day_mode_to_proto(mode: LocalTimeOfDayModeRecord) -> LocalTimeOfDayMode {
    match mode {
        LocalTimeOfDayModeRecord::FixedPreset => LocalTimeOfDayMode::FixedPreset,
        LocalTimeOfDayModeRecord::AutoByLocalTime => LocalTimeOfDayMode::AutoByLocalTime,
    }
}

fn runtime_time_of_day_preset_to_proto(
    preset: RuntimeTimeOfDayPresetRecord,
) -> RuntimeTimeOfDayPreset {
    match preset {
        RuntimeTimeOfDayPresetRecord::Morning => RuntimeTimeOfDayPreset::Morning,
        RuntimeTimeOfDayPresetRecord::Noon => RuntimeTimeOfDayPreset::Noon,
        RuntimeTimeOfDayPresetRecord::Evening => RuntimeTimeOfDayPreset::Evening,
        RuntimeTimeOfDayPresetRecord::Night => RuntimeTimeOfDayPreset::Night,
    }
}

fn weather_type_to_proto(weather_type: WeatherTypeRecord) -> WeatherType {
    match weather_type {
        WeatherTypeRecord::Clear => WeatherType::Clear,
        WeatherTypeRecord::PartlyCloudy => WeatherType::PartlyCloudy,
        WeatherTypeRecord::Overcast => WeatherType::Overcast,
        WeatherTypeRecord::LightRain => WeatherType::LightRain,
        WeatherTypeRecord::MediumRain => WeatherType::MediumRain,
        WeatherTypeRecord::HeavyRain => WeatherType::HeavyRain,
    }
}

fn local_spawn_mode_to_proto(spawn_mode: LocalSandboxSpawnModeRecord) -> LocalSandboxSpawnMode {
    match spawn_mode {
        LocalSandboxSpawnModeRecord::StartLine => LocalSandboxSpawnMode::StartLine,
        LocalSandboxSpawnModeRecord::RandomOnTrack => LocalSandboxSpawnMode::RandomOnTrack,
        LocalSandboxSpawnModeRecord::InPit => LocalSandboxSpawnMode::InPit,
        LocalSandboxSpawnModeRecord::RandomStartSlot => LocalSandboxSpawnMode::RandomStartSlot,
    }
}

fn enum_from_i32<E>(value: i32, field: &str) -> Result<E, LocalSandboxConfigStoreError>
where
    E: TryFrom<i32>,
{
    E::try_from(value).map_err(|_| LocalSandboxConfigStoreError::InvalidConfig {
        message: format!("invalid {field}"),
    })
}

impl TryFrom<LocalTimeOfDayMode> for LocalTimeOfDayModeRecord {
    type Error = LocalSandboxConfigStoreError;

    fn try_from(value: LocalTimeOfDayMode) -> Result<Self, Self::Error> {
        match value {
            LocalTimeOfDayMode::FixedPreset => Ok(Self::FixedPreset),
            LocalTimeOfDayMode::AutoByLocalTime => Ok(Self::AutoByLocalTime),
            LocalTimeOfDayMode::Unspecified => Err(LocalSandboxConfigStoreError::InvalidConfig {
                message: "time_of_day.mode must be specified".to_string(),
            }),
        }
    }
}

impl TryFrom<RuntimeTimeOfDayPreset> for RuntimeTimeOfDayPresetRecord {
    type Error = LocalSandboxConfigStoreError;

    fn try_from(value: RuntimeTimeOfDayPreset) -> Result<Self, Self::Error> {
        match value {
            RuntimeTimeOfDayPreset::Morning => Ok(Self::Morning),
            RuntimeTimeOfDayPreset::Noon => Ok(Self::Noon),
            RuntimeTimeOfDayPreset::Evening => Ok(Self::Evening),
            RuntimeTimeOfDayPreset::Night => Ok(Self::Night),
            RuntimeTimeOfDayPreset::Unspecified => {
                Err(LocalSandboxConfigStoreError::InvalidConfig {
                    message: "time_of_day.fixed_preset must be specified".to_string(),
                })
            }
        }
    }
}

impl TryFrom<WeatherType> for WeatherTypeRecord {
    type Error = LocalSandboxConfigStoreError;

    fn try_from(value: WeatherType) -> Result<Self, Self::Error> {
        match value {
            WeatherType::Clear => Ok(Self::Clear),
            WeatherType::PartlyCloudy => Ok(Self::PartlyCloudy),
            WeatherType::Overcast => Ok(Self::Overcast),
            WeatherType::LightRain => Ok(Self::LightRain),
            WeatherType::MediumRain => Ok(Self::MediumRain),
            WeatherType::HeavyRain => Ok(Self::HeavyRain),
            WeatherType::Unspecified => Err(LocalSandboxConfigStoreError::InvalidConfig {
                message: "weather.weather_type must be specified".to_string(),
            }),
        }
    }
}

impl TryFrom<LocalSandboxSpawnMode> for LocalSandboxSpawnModeRecord {
    type Error = LocalSandboxConfigStoreError;

    fn try_from(value: LocalSandboxSpawnMode) -> Result<Self, Self::Error> {
        match value {
            LocalSandboxSpawnMode::StartLine => Ok(Self::StartLine),
            LocalSandboxSpawnMode::RandomOnTrack => Ok(Self::RandomOnTrack),
            LocalSandboxSpawnMode::InPit => Ok(Self::InPit),
            LocalSandboxSpawnMode::RandomStartSlot => Ok(Self::RandomStartSlot),
            LocalSandboxSpawnMode::Unspecified => {
                Err(LocalSandboxConfigStoreError::InvalidConfig {
                    message: "spawn_mode must be specified".to_string(),
                })
            }
        }
    }
}
