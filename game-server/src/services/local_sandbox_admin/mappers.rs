use boink::model::ghost::GhostModeSettings as EngineGhostModeSettings;
use boink::model::weather::WeatherParams;
use proto::race::v1::{
    GhostModeSettings, LocalSandboxSpawnMode, LocalTimeOfDayMode, LocalTimeOfDaySettings,
    LocalWeatherSettings, RuntimeTimeOfDayPreset,
};
use proto::weather::v1::WeatherType;
use tonic::Status;

use crate::domain::weather::engine_params_for_weather_type;
use crate::local::sandbox_config_store::{
    LocalGhostModeSettingsRecord, LocalSandboxSpawnModeRecord, LocalTimeOfDayModeRecord,
    LocalTimeOfDaySettingsRecord, LocalWeatherSettingsRecord, RuntimeTimeOfDayPresetRecord,
    WeatherTypeRecord,
};
use crate::runtime::engine_worker::{
    EngineRuntimeTimeOfDayPreset, EngineRuntimeWeatherNow, EngineRuntimeWeatherType,
};

pub(crate) fn local_time_of_day_from_proto(
    proto: &LocalTimeOfDaySettings,
) -> Result<LocalTimeOfDaySettingsRecord, Status> {
    let mode = LocalTimeOfDayMode::try_from(proto.mode)
        .map_err(|_| Status::invalid_argument("invalid time_of_day.mode"))?;
    let mode = local_time_of_day_mode_from_proto(mode)?;
    let fixed_preset_proto = RuntimeTimeOfDayPreset::try_from(proto.fixed_preset)
        .map_err(|_| Status::invalid_argument("invalid time_of_day.fixed_preset"))?;
    let fixed_preset = match (mode, fixed_preset_proto) {
        (LocalTimeOfDayModeRecord::FixedPreset, RuntimeTimeOfDayPreset::Unspecified) => {
            return Err(Status::invalid_argument(
                "time_of_day.fixed_preset must be specified for fixed mode",
            ));
        }
        (_, RuntimeTimeOfDayPreset::Unspecified) => RuntimeTimeOfDayPresetRecord::Noon,
        (_, preset) => runtime_time_of_day_preset_record_from_proto(preset)?,
    };

    Ok(LocalTimeOfDaySettingsRecord { mode, fixed_preset })
}

pub(crate) fn local_time_of_day_to_proto(
    record: LocalTimeOfDaySettingsRecord,
) -> LocalTimeOfDaySettings {
    LocalTimeOfDaySettings {
        mode: local_time_of_day_mode_to_proto(record.mode) as i32,
        fixed_preset: runtime_time_of_day_preset_record_to_proto(record.fixed_preset) as i32,
    }
}

pub(crate) fn local_weather_from_proto(
    proto: &LocalWeatherSettings,
) -> Result<LocalWeatherSettingsRecord, Status> {
    let weather_type = WeatherType::try_from(proto.weather_type)
        .map_err(|_| Status::invalid_argument("invalid weather.weather_type"))?;
    let weather_type = weather_type_record_from_proto(weather_type)?;
    Ok(LocalWeatherSettingsRecord {
        weather_type,
        temperature_c: proto.temperature_c,
    })
}

pub(crate) fn local_weather_to_proto(record: LocalWeatherSettingsRecord) -> LocalWeatherSettings {
    LocalWeatherSettings {
        weather_type: weather_type_record_to_proto(record.weather_type) as i32,
        temperature_c: record.temperature_c,
    }
}

pub(crate) fn local_spawn_mode_from_proto_value(
    value: i32,
) -> Result<LocalSandboxSpawnModeRecord, Status> {
    let mode = LocalSandboxSpawnMode::try_from(value)
        .map_err(|_| Status::invalid_argument("invalid spawn_mode"))?;
    local_spawn_mode_from_proto(mode)
}

pub(crate) fn local_spawn_mode_to_proto(
    mode: LocalSandboxSpawnModeRecord,
) -> LocalSandboxSpawnMode {
    match mode {
        LocalSandboxSpawnModeRecord::StartLine => LocalSandboxSpawnMode::StartLine,
        LocalSandboxSpawnModeRecord::RandomOnTrack => LocalSandboxSpawnMode::RandomOnTrack,
        LocalSandboxSpawnModeRecord::InPit => LocalSandboxSpawnMode::InPit,
        LocalSandboxSpawnModeRecord::RandomStartSlot => LocalSandboxSpawnMode::RandomStartSlot,
    }
}

pub(crate) fn local_time_of_day_mode_to_proto(
    mode: LocalTimeOfDayModeRecord,
) -> LocalTimeOfDayMode {
    match mode {
        LocalTimeOfDayModeRecord::FixedPreset => LocalTimeOfDayMode::FixedPreset,
        LocalTimeOfDayModeRecord::AutoByLocalTime => LocalTimeOfDayMode::AutoByLocalTime,
    }
}

pub(crate) fn resolve_runtime_time_of_day_preset(
    time_of_day: LocalTimeOfDaySettingsRecord,
) -> EngineRuntimeTimeOfDayPreset {
    // Auto-by-local-time sync loop is implemented separately; for now use configured preset.
    runtime_time_of_day_preset_record_to_engine(time_of_day.fixed_preset)
}

pub(crate) fn runtime_time_of_day_preset_to_proto(
    preset: EngineRuntimeTimeOfDayPreset,
) -> RuntimeTimeOfDayPreset {
    match preset {
        EngineRuntimeTimeOfDayPreset::Unspecified => RuntimeTimeOfDayPreset::Unspecified,
        EngineRuntimeTimeOfDayPreset::Morning => RuntimeTimeOfDayPreset::Morning,
        EngineRuntimeTimeOfDayPreset::Noon => RuntimeTimeOfDayPreset::Noon,
        EngineRuntimeTimeOfDayPreset::Evening => RuntimeTimeOfDayPreset::Evening,
        EngineRuntimeTimeOfDayPreset::Night => RuntimeTimeOfDayPreset::Night,
    }
}

pub(crate) fn weather_params_from_local(weather: LocalWeatherSettingsRecord) -> WeatherParams {
    let weather_type = weather_type_record_to_proto(weather.weather_type);
    let params = engine_params_for_weather_type(weather_type);
    WeatherParams {
        cloudiness: params.cloudiness,
        temperature_c: weather.temperature_c as f32,
        rain_intensity: params.rain_intensity,
    }
}

pub(crate) fn runtime_weather_now_from_local(
    weather: LocalWeatherSettingsRecord,
) -> EngineRuntimeWeatherNow {
    EngineRuntimeWeatherNow {
        weather_type: weather_type_record_to_runtime(weather.weather_type),
        temperature_c: weather.temperature_c,
    }
}

pub(crate) fn engine_ghost_mode_settings_from_local_record(
    record: Option<LocalGhostModeSettingsRecord>,
) -> EngineGhostModeSettings {
    let Some(record) = record else {
        return EngineGhostModeSettings {
            enabled: false,
            enter_speed_max_mps: 0.0,
            exit_speed_min_mps: 0.0,
            enter_delay_ms: 0,
            exit_delay_ms: 0,
            until_completed_laps: 0,
            vehicle_overlap_exit_delay_ms: 0,
        };
    };

    EngineGhostModeSettings {
        enabled: record.enabled,
        enter_speed_max_mps: record.enter_speed_max_mps,
        exit_speed_min_mps: record.exit_speed_min_mps,
        enter_delay_ms: record.enter_delay_ms,
        exit_delay_ms: record.exit_delay_ms,
        until_completed_laps: record.until_completed_laps,
        vehicle_overlap_exit_delay_ms: record.vehicle_overlap_exit_delay_ms,
    }
}

pub(crate) fn local_ghost_mode_to_proto(record: LocalGhostModeSettingsRecord) -> GhostModeSettings {
    GhostModeSettings {
        enabled: record.enabled,
        enter_speed_max_mps: record.enter_speed_max_mps,
        exit_speed_min_mps: record.exit_speed_min_mps,
        enter_delay_ms: record.enter_delay_ms,
        exit_delay_ms: record.exit_delay_ms,
        until_completed_laps: record.until_completed_laps,
        vehicle_overlap_exit_delay_ms: record.vehicle_overlap_exit_delay_ms,
    }
}

pub(crate) fn utc_now_timestamp() -> prost_types::Timestamp {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    prost_types::Timestamp {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

fn local_spawn_mode_from_proto(
    mode: LocalSandboxSpawnMode,
) -> Result<LocalSandboxSpawnModeRecord, Status> {
    match mode {
        LocalSandboxSpawnMode::StartLine => Ok(LocalSandboxSpawnModeRecord::StartLine),
        LocalSandboxSpawnMode::RandomOnTrack => Ok(LocalSandboxSpawnModeRecord::RandomOnTrack),
        LocalSandboxSpawnMode::InPit => Ok(LocalSandboxSpawnModeRecord::InPit),
        LocalSandboxSpawnMode::RandomStartSlot => Ok(LocalSandboxSpawnModeRecord::RandomStartSlot),
        LocalSandboxSpawnMode::Unspecified => {
            Err(Status::invalid_argument("spawn_mode must be specified"))
        }
    }
}

fn local_time_of_day_mode_from_proto(
    mode: LocalTimeOfDayMode,
) -> Result<LocalTimeOfDayModeRecord, Status> {
    match mode {
        LocalTimeOfDayMode::FixedPreset => Ok(LocalTimeOfDayModeRecord::FixedPreset),
        LocalTimeOfDayMode::AutoByLocalTime => Ok(LocalTimeOfDayModeRecord::AutoByLocalTime),
        LocalTimeOfDayMode::Unspecified => Err(Status::invalid_argument(
            "time_of_day.mode must be specified",
        )),
    }
}

fn runtime_time_of_day_preset_record_from_proto(
    preset: RuntimeTimeOfDayPreset,
) -> Result<RuntimeTimeOfDayPresetRecord, Status> {
    match preset {
        RuntimeTimeOfDayPreset::Morning => Ok(RuntimeTimeOfDayPresetRecord::Morning),
        RuntimeTimeOfDayPreset::Noon => Ok(RuntimeTimeOfDayPresetRecord::Noon),
        RuntimeTimeOfDayPreset::Evening => Ok(RuntimeTimeOfDayPresetRecord::Evening),
        RuntimeTimeOfDayPreset::Night => Ok(RuntimeTimeOfDayPresetRecord::Night),
        RuntimeTimeOfDayPreset::Unspecified => Err(Status::invalid_argument(
            "time_of_day.fixed_preset must be specified",
        )),
    }
}

fn runtime_time_of_day_preset_record_to_proto(
    preset: RuntimeTimeOfDayPresetRecord,
) -> RuntimeTimeOfDayPreset {
    match preset {
        RuntimeTimeOfDayPresetRecord::Morning => RuntimeTimeOfDayPreset::Morning,
        RuntimeTimeOfDayPresetRecord::Noon => RuntimeTimeOfDayPreset::Noon,
        RuntimeTimeOfDayPresetRecord::Evening => RuntimeTimeOfDayPreset::Evening,
        RuntimeTimeOfDayPresetRecord::Night => RuntimeTimeOfDayPreset::Night,
    }
}

fn runtime_time_of_day_preset_record_to_engine(
    preset: RuntimeTimeOfDayPresetRecord,
) -> EngineRuntimeTimeOfDayPreset {
    match preset {
        RuntimeTimeOfDayPresetRecord::Morning => EngineRuntimeTimeOfDayPreset::Morning,
        RuntimeTimeOfDayPresetRecord::Noon => EngineRuntimeTimeOfDayPreset::Noon,
        RuntimeTimeOfDayPresetRecord::Evening => EngineRuntimeTimeOfDayPreset::Evening,
        RuntimeTimeOfDayPresetRecord::Night => EngineRuntimeTimeOfDayPreset::Night,
    }
}

fn weather_type_record_from_proto(weather_type: WeatherType) -> Result<WeatherTypeRecord, Status> {
    match weather_type {
        WeatherType::Clear => Ok(WeatherTypeRecord::Clear),
        WeatherType::PartlyCloudy => Ok(WeatherTypeRecord::PartlyCloudy),
        WeatherType::Overcast => Ok(WeatherTypeRecord::Overcast),
        WeatherType::LightRain => Ok(WeatherTypeRecord::LightRain),
        WeatherType::MediumRain => Ok(WeatherTypeRecord::MediumRain),
        WeatherType::HeavyRain => Ok(WeatherTypeRecord::HeavyRain),
        WeatherType::Unspecified => Err(Status::invalid_argument(
            "weather.weather_type must be specified",
        )),
    }
}

fn weather_type_record_to_proto(weather_type: WeatherTypeRecord) -> WeatherType {
    match weather_type {
        WeatherTypeRecord::Clear => WeatherType::Clear,
        WeatherTypeRecord::PartlyCloudy => WeatherType::PartlyCloudy,
        WeatherTypeRecord::Overcast => WeatherType::Overcast,
        WeatherTypeRecord::LightRain => WeatherType::LightRain,
        WeatherTypeRecord::MediumRain => WeatherType::MediumRain,
        WeatherTypeRecord::HeavyRain => WeatherType::HeavyRain,
    }
}

fn weather_type_record_to_runtime(weather_type: WeatherTypeRecord) -> EngineRuntimeWeatherType {
    match weather_type {
        WeatherTypeRecord::Clear => EngineRuntimeWeatherType::Clear,
        WeatherTypeRecord::PartlyCloudy => EngineRuntimeWeatherType::PartlyCloudy,
        WeatherTypeRecord::Overcast => EngineRuntimeWeatherType::Overcast,
        WeatherTypeRecord::LightRain => EngineRuntimeWeatherType::LightRain,
        WeatherTypeRecord::MediumRain => EngineRuntimeWeatherType::MediumRain,
        WeatherTypeRecord::HeavyRain => EngineRuntimeWeatherType::HeavyRain,
    }
}
