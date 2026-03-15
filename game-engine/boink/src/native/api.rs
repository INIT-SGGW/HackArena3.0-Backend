//! Abstractions over optional native Boink symbols.
//!
//! `NativeApi` lazily loads `boink.dll`/`libboink` once and exposes the function
//! pointers we care about. Higher-level code can check whether a symbol exists
//! before invoking the corresponding functionality.

use std::sync::OnceLock;

use tracing::trace;

use crate::native::error::NativeLoadError;
use crate::native::loader::{
    LegacyDisableGhostModeFn, LegacyGetBestLapFn, LegacyGetNumberOfStartPosFn,
    LegacyGetTrackDataFn, LegacyGetVehiclePersonalBestLapFn, LegacyReadVehicleGhostModeStateFn,
    LegacyReadVehicleRaceMetricsFn, LegacySetGhostModeSettingsFn, LegacySetVehicleAtStartPosFn,
    LegacySetVehicleBeforeFinishLineFn, LegacySetVehicleBeforePointFn,
    LegacySetVehicleOrientationFn, LegacySetVehicleRandomPosFn, LegacySetWeatherFn, LegacyStringFn,
    LegacyVersionFn, load_native_library, resolve_optional,
};

/// Lazily resolved optional symbols exposed by a potentially old native library.
pub struct NativeApi {
    get_c_api_version: Option<LegacyVersionFn>,
    get_engine_version: Option<LegacyVersionFn>,
    get_engine_profile: Option<LegacyStringFn>,
    get_last_error: Option<LegacyStringFn>,
    set_weather: Option<LegacySetWeatherFn>,
    set_ghost_mode_settings: Option<LegacySetGhostModeSettingsFn>,
    disable_ghost_mode: Option<LegacyDisableGhostModeFn>,
    get_track_data: Option<LegacyGetTrackDataFn>,
    set_vehicle_orientation: Option<LegacySetVehicleOrientationFn>,
    set_vehicle_before_point: Option<LegacySetVehicleBeforePointFn>,
    set_vehicle_before_finish_line: Option<LegacySetVehicleBeforeFinishLineFn>,
    set_vehicle_random_pos: Option<LegacySetVehicleRandomPosFn>,
    set_vehicle_at_start_pos: Option<LegacySetVehicleAtStartPosFn>,
    get_number_of_start_pos: Option<LegacyGetNumberOfStartPosFn>,
    read_vehicle_ghost_mode_state: Option<LegacyReadVehicleGhostModeStateFn>,
    read_vehicle_race_metrics: Option<LegacyReadVehicleRaceMetricsFn>,
    get_vehicle_personal_best_lap: Option<LegacyGetVehiclePersonalBestLapFn>,
    get_best_lap: Option<LegacyGetBestLapFn>,
}

impl NativeApi {
    /// Returns the shared [`NativeApi`] instance if the native library could be loaded.
    pub fn instance() -> Result<&'static NativeApi, NativeLoadError> {
        static INSTANCE: OnceLock<Result<NativeApi, NativeLoadError>> = OnceLock::new();
        match INSTANCE.get_or_init(NativeApi::load).as_ref() {
            Ok(api) => Ok(api),
            Err(err) => Err(err.clone()),
        }
    }

    /// Returns the function pointer for `boink_get_c_api_version`, when exported.
    #[must_use]
    pub fn boink_get_c_api_version(&self) -> Option<LegacyVersionFn> {
        self.get_c_api_version
    }

    /// Returns the function pointer for `boink_get_engine_version`, when exported.
    #[must_use]
    pub fn boink_get_engine_version(&self) -> Option<LegacyVersionFn> {
        self.get_engine_version
    }

    /// Returns the function pointer for `boink_get_engine_profile`, when exported.
    #[must_use]
    pub fn boink_get_engine_profile(&self) -> Option<LegacyStringFn> {
        self.get_engine_profile
    }

    /// Returns the function pointer for `boink_get_last_error`, when exported.
    #[must_use]
    pub fn boink_get_last_error(&self) -> Option<LegacyStringFn> {
        self.get_last_error
    }

    /// Returns the function pointer for `boink_set_weather`, when exported.
    #[must_use]
    pub fn boink_set_weather(&self) -> Option<LegacySetWeatherFn> {
        self.set_weather
    }

    /// Returns the function pointer for `boink_set_ghost_mode_settings`, when exported.
    #[must_use]
    pub fn boink_set_ghost_mode_settings(&self) -> Option<LegacySetGhostModeSettingsFn> {
        self.set_ghost_mode_settings
    }

    /// Returns the function pointer for `boink_disable_ghost_mode`, when exported.
    #[must_use]
    pub fn boink_disable_ghost_mode(&self) -> Option<LegacyDisableGhostModeFn> {
        self.disable_ghost_mode
    }

    /// Returns the function pointer for `boink_get_track_data`, when exported.
    #[must_use]
    pub fn boink_get_track_data(&self) -> Option<LegacyGetTrackDataFn> {
        self.get_track_data
    }

    /// Returns the function pointer for `boink_set_vehicle_orientation`, when exported.
    #[must_use]
    pub fn boink_set_vehicle_orientation(&self) -> Option<LegacySetVehicleOrientationFn> {
        self.set_vehicle_orientation
    }

    /// Returns the function pointer for `boink_set_vehicle_before_point`, when exported.
    #[must_use]
    pub fn boink_set_vehicle_before_point(&self) -> Option<LegacySetVehicleBeforePointFn> {
        self.set_vehicle_before_point
    }

    /// Returns the function pointer for `boink_set_vehicle_before_finish_line`, when exported.
    #[must_use]
    pub fn boink_set_vehicle_before_finish_line(
        &self,
    ) -> Option<LegacySetVehicleBeforeFinishLineFn> {
        self.set_vehicle_before_finish_line
    }

    /// Returns the function pointer for `boink_set_vehicle_random_pos`, when exported.
    #[must_use]
    pub fn boink_set_vehicle_random_pos(&self) -> Option<LegacySetVehicleRandomPosFn> {
        self.set_vehicle_random_pos
    }

    /// Returns the function pointer for `boink_set_vehicle_at_start_pos`, when exported.
    #[must_use]
    pub fn boink_set_vehicle_at_start_pos(&self) -> Option<LegacySetVehicleAtStartPosFn> {
        self.set_vehicle_at_start_pos
    }

    /// Returns the function pointer for `boink_get_number_of_start_pos`, when exported.
    #[must_use]
    pub fn boink_get_number_of_start_pos(&self) -> Option<LegacyGetNumberOfStartPosFn> {
        self.get_number_of_start_pos
    }

    /// Returns the function pointer for `boink_read_vehicle_ghost_mode_state`, when exported.
    #[must_use]
    pub fn boink_read_vehicle_ghost_mode_state(&self) -> Option<LegacyReadVehicleGhostModeStateFn> {
        self.read_vehicle_ghost_mode_state
    }

    /// Returns the function pointer for `boink_read_vehicle_race_metrics`, when exported.
    #[must_use]
    pub fn boink_read_vehicle_race_metrics(&self) -> Option<LegacyReadVehicleRaceMetricsFn> {
        self.read_vehicle_race_metrics
    }

    /// Returns the function pointer for `boink_get_vehicle_personal_best_lap`, when exported.
    #[must_use]
    pub fn boink_get_vehicle_personal_best_lap(&self) -> Option<LegacyGetVehiclePersonalBestLapFn> {
        self.get_vehicle_personal_best_lap
    }

    /// Returns the function pointer for `boink_get_best_lap`, when exported.
    #[must_use]
    pub fn boink_get_best_lap(&self) -> Option<LegacyGetBestLapFn> {
        self.get_best_lap
    }

    fn load() -> Result<NativeApi, NativeLoadError> {
        let lib = load_native_library()?;

        let get_c_api_version = resolve_optional(lib, b"boink_get_c_api_version\0");
        let get_engine_version = resolve_optional(lib, b"boink_get_engine_version\0");
        let get_engine_profile = resolve_optional(lib, b"boink_get_engine_profile\0");
        let get_last_error = resolve_optional(lib, b"boink_get_last_error\0");
        let set_weather = resolve_optional(lib, b"boink_set_weather\0");
        let set_ghost_mode_settings = resolve_optional(lib, b"boink_set_ghost_mode_settings\0");
        let disable_ghost_mode = resolve_optional(lib, b"boink_disable_ghost_mode\0");
        let get_track_data = resolve_optional(lib, b"boink_get_track_data\0");
        let set_vehicle_orientation = resolve_optional(lib, b"boink_set_vehicle_orientation\0");
        let set_vehicle_before_point = resolve_optional(lib, b"boink_set_vehicle_before_point\0");
        let set_vehicle_before_finish_line =
            resolve_optional(lib, b"boink_set_vehicle_before_finish_line\0");
        let set_vehicle_random_pos = resolve_optional(lib, b"boink_set_vehicle_random_pos\0");
        let set_vehicle_at_start_pos = resolve_optional(lib, b"boink_set_vehicle_at_start_pos\0");
        let get_number_of_start_pos = resolve_optional(lib, b"boink_get_number_of_start_pos\0");
        let read_vehicle_ghost_mode_state =
            resolve_optional(lib, b"boink_read_vehicle_ghost_mode_state\0");
        let read_vehicle_race_metrics = resolve_optional(lib, b"boink_read_vehicle_race_metrics\0");
        let get_vehicle_personal_best_lap =
            resolve_optional(lib, b"boink_get_vehicle_personal_best_lap\0");
        let get_best_lap = resolve_optional(lib, b"boink_get_best_lap\0");

        trace!(
            "Resolved legacy query methods: c_api_version={}, engine_version={}, engine_profile={}, last_error={}, set_weather={}, set_ghost_mode_settings={}, disable_ghost_mode={}, track_data={}, set_vehicle_orientation={}, set_vehicle_before_point={}, set_vehicle_before_finish_line={}, set_vehicle_random_pos={}, set_vehicle_at_start_pos={}, get_number_of_start_pos={}, read_vehicle_ghost_mode_state={}, read_vehicle_race_metrics={}, get_vehicle_personal_best_lap={}, get_best_lap={}",
            get_c_api_version.is_some(),
            get_engine_version.is_some(),
            get_engine_profile.is_some(),
            get_last_error.is_some(),
            set_weather.is_some(),
            set_ghost_mode_settings.is_some(),
            disable_ghost_mode.is_some(),
            get_track_data.is_some(),
            set_vehicle_orientation.is_some(),
            set_vehicle_before_point.is_some(),
            set_vehicle_before_finish_line.is_some(),
            set_vehicle_random_pos.is_some(),
            set_vehicle_at_start_pos.is_some(),
            get_number_of_start_pos.is_some(),
            read_vehicle_ghost_mode_state.is_some(),
            read_vehicle_race_metrics.is_some(),
            get_vehicle_personal_best_lap.is_some(),
            get_best_lap.is_some()
        );

        Ok(NativeApi {
            get_c_api_version,
            get_engine_version,
            get_engine_profile,
            get_last_error,
            set_weather,
            set_ghost_mode_settings,
            disable_ghost_mode,
            get_track_data,
            set_vehicle_orientation,
            set_vehicle_before_point,
            set_vehicle_before_finish_line,
            set_vehicle_random_pos,
            set_vehicle_at_start_pos,
            get_number_of_start_pos,
            read_vehicle_ghost_mode_state,
            read_vehicle_race_metrics,
            get_vehicle_personal_best_lap,
            get_best_lap,
        })
    }
}
