//! Local sandbox configuration store persisted as JSON.

mod error;
mod mappers;
mod model;
mod store;

pub use error::LocalSandboxConfigStoreError;
pub use mappers::{
    local_sandbox_input_from_proto, local_sandbox_input_to_proto, local_sandbox_to_proto,
};
pub use model::{
    LocalGhostModeSettingsRecord, LocalSandboxConfigInputRecord, LocalSandboxConfigRecord,
    LocalSandboxConfigSnapshot, LocalSandboxSpawnModeRecord, LocalTimeOfDayModeRecord,
    LocalTimeOfDaySettingsRecord, LocalWeatherSettingsRecord, RuntimeTimeOfDayPresetRecord,
    WeatherTypeRecord, validate_local_sandbox_config_input,
};
pub use store::LocalSandboxConfigStore;
