//! Native function table used by legacy mode.
//!
//! With minimum supported legacy C API at `0.16.0`, symbols are treated as
//! stable and mapped directly to `boink_sys` exports.

use std::sync::OnceLock;

type LegacyVersionFn = unsafe extern "C" fn(*mut u32, *mut u32, *mut u32) -> i32;
type LegacyStringFn = unsafe extern "C" fn(*mut std::os::raw::c_char, *mut u32) -> i32;
type LegacySetWeatherFn =
    unsafe extern "C" fn(boink_sys::BoinkHandle, *const boink_sys::BoinkWeather) -> i32;
type LegacySetGhostModeSettingsFn =
    unsafe extern "C" fn(boink_sys::BoinkHandle, *const boink_sys::BoinkGhostModeSettings) -> i32;
type LegacyDisableGhostModeFn = unsafe extern "C" fn(boink_sys::BoinkHandle) -> i32;
type LegacyGetTrackDataFn =
    unsafe extern "C" fn(boink_sys::BoinkHandle, *mut boink_sys::BoinkTrackData) -> i32;
type LegacySetVehicleOrientationFn =
    unsafe extern "C" fn(boink_sys::BoinkHandle, u64, *const boink_sys::BoinkQuaternion) -> i32;
type LegacySetVehicleBeforePointFn =
    unsafe extern "C" fn(boink_sys::BoinkHandle, u64, *const boink_sys::BoinkVec3) -> i32;
type LegacySetVehicleBeforeFinishLineFn = unsafe extern "C" fn(boink_sys::BoinkHandle, u64) -> i32;
type LegacySetVehicleRandomPosFn = unsafe extern "C" fn(boink_sys::BoinkHandle, u64) -> i32;
type LegacySetVehicleBackToTrackFn = unsafe extern "C" fn(boink_sys::BoinkHandle, u64) -> i32;
type LegacySetVehicleToPitstopFn = unsafe extern "C" fn(boink_sys::BoinkHandle, u64) -> i32;
type LegacySetVehicleAtStartPosFn = unsafe extern "C" fn(boink_sys::BoinkHandle, u64, u64) -> i32;
type LegacyGetNumberOfStartPosFn = unsafe extern "C" fn(boink_sys::BoinkHandle, *mut u64) -> i32;
type LegacyReadVehicleGhostModeStateFn = unsafe extern "C" fn(
    boink_sys::BoinkHandle,
    u64,
    *mut boink_sys::BoinkGhostModeRuntimeState,
) -> i32;
type LegacyReadVehicleRaceMetricsFn = unsafe extern "C" fn(
    boink_sys::BoinkHandle,
    u64,
    *mut boink_sys::BoinkVehicleRaceMetrics,
) -> i32;
type LegacyGetVehiclePitstopZoneFn = unsafe extern "C" fn(
    boink_sys::BoinkHandle,
    u64,
    *mut boink_sys::BoinkPitstopZone,
    *mut i32,
) -> i32;
type LegacyGetVehiclePersonalBestLapFn =
    unsafe extern "C" fn(boink_sys::BoinkHandle, u64, *mut u32, *mut u32) -> i32;
type LegacyGetBestLapFn =
    unsafe extern "C" fn(boink_sys::BoinkHandle, *mut u64, *mut u32, *mut u32) -> i32;

/// Native symbols exposed through a single shared table.
pub struct NativeApi {
    get_c_api_version: LegacyVersionFn,
    get_engine_version: LegacyVersionFn,
    get_engine_profile: LegacyStringFn,
    get_last_error: LegacyStringFn,
    set_weather: LegacySetWeatherFn,
    set_ghost_mode_settings: LegacySetGhostModeSettingsFn,
    disable_ghost_mode: LegacyDisableGhostModeFn,
    get_track_data: LegacyGetTrackDataFn,
    set_vehicle_orientation: LegacySetVehicleOrientationFn,
    set_vehicle_before_point: LegacySetVehicleBeforePointFn,
    set_vehicle_before_finish_line: LegacySetVehicleBeforeFinishLineFn,
    set_vehicle_random_pos: LegacySetVehicleRandomPosFn,
    set_vehicle_back_to_track: LegacySetVehicleBackToTrackFn,
    set_vehicle_to_pitstop: LegacySetVehicleToPitstopFn,
    set_vehicle_at_start_pos: LegacySetVehicleAtStartPosFn,
    get_number_of_start_pos: LegacyGetNumberOfStartPosFn,
    read_vehicle_ghost_mode_state: LegacyReadVehicleGhostModeStateFn,
    read_vehicle_race_metrics: LegacyReadVehicleRaceMetricsFn,
    get_vehicle_pitstop_zone: LegacyGetVehiclePitstopZoneFn,
    get_vehicle_personal_best_lap: LegacyGetVehiclePersonalBestLapFn,
    get_best_lap: LegacyGetBestLapFn,
}

impl NativeApi {
    /// Returns the shared native API table.
    pub fn instance() -> &'static NativeApi {
        static INSTANCE: OnceLock<NativeApi> = OnceLock::new();
        INSTANCE.get_or_init(|| NativeApi {
            get_c_api_version: boink_sys::boink_get_c_api_version,
            get_engine_version: boink_sys::boink_get_engine_version,
            get_engine_profile: boink_sys::boink_get_engine_profile,
            get_last_error: boink_sys::boink_get_last_error,
            set_weather: boink_sys::boink_set_weather,
            set_ghost_mode_settings: boink_sys::boink_set_ghost_mode_settings,
            disable_ghost_mode: boink_sys::boink_disable_ghost_mode,
            get_track_data: boink_sys::boink_get_track_data,
            set_vehicle_orientation: boink_sys::boink_set_vehicle_orientation,
            set_vehicle_before_point: boink_sys::boink_set_vehicle_before_point,
            set_vehicle_before_finish_line: boink_sys::boink_set_vehicle_before_finish_line,
            set_vehicle_random_pos: boink_sys::boink_set_vehicle_random_pos,
            set_vehicle_back_to_track: boink_sys::boink_set_vehicle_back_to_track,
            set_vehicle_to_pitstop: boink_sys::boink_set_vehicle_to_pitstop,
            set_vehicle_at_start_pos: boink_sys::boink_set_vehicle_at_start_pos,
            get_number_of_start_pos: boink_sys::boink_get_number_of_start_pos,
            read_vehicle_ghost_mode_state: boink_sys::boink_read_vehicle_ghost_mode_state,
            read_vehicle_race_metrics: boink_sys::boink_read_vehicle_race_metrics,
            get_vehicle_pitstop_zone: boink_sys::boink_get_vehicle_pitstop_zone,
            get_vehicle_personal_best_lap: boink_sys::boink_get_vehicle_personal_best_lap,
            get_best_lap: boink_sys::boink_get_best_lap,
        })
    }

    #[must_use]
    pub fn boink_get_c_api_version(&self) -> LegacyVersionFn {
        self.get_c_api_version
    }
    #[must_use]
    pub fn boink_get_engine_version(&self) -> LegacyVersionFn {
        self.get_engine_version
    }
    #[must_use]
    pub fn boink_get_engine_profile(&self) -> LegacyStringFn {
        self.get_engine_profile
    }
    #[must_use]
    pub fn boink_get_last_error(&self) -> LegacyStringFn {
        self.get_last_error
    }
    #[must_use]
    pub fn boink_set_weather(&self) -> LegacySetWeatherFn {
        self.set_weather
    }
    #[must_use]
    pub fn boink_set_ghost_mode_settings(&self) -> LegacySetGhostModeSettingsFn {
        self.set_ghost_mode_settings
    }
    #[must_use]
    pub fn boink_disable_ghost_mode(&self) -> LegacyDisableGhostModeFn {
        self.disable_ghost_mode
    }
    #[must_use]
    pub fn boink_get_track_data(&self) -> LegacyGetTrackDataFn {
        self.get_track_data
    }
    #[must_use]
    pub fn boink_set_vehicle_orientation(&self) -> LegacySetVehicleOrientationFn {
        self.set_vehicle_orientation
    }
    #[must_use]
    pub fn boink_set_vehicle_before_point(&self) -> LegacySetVehicleBeforePointFn {
        self.set_vehicle_before_point
    }
    #[must_use]
    pub fn boink_set_vehicle_before_finish_line(&self) -> LegacySetVehicleBeforeFinishLineFn {
        self.set_vehicle_before_finish_line
    }
    #[must_use]
    pub fn boink_set_vehicle_random_pos(&self) -> LegacySetVehicleRandomPosFn {
        self.set_vehicle_random_pos
    }
    #[must_use]
    pub fn boink_set_vehicle_back_to_track(&self) -> LegacySetVehicleBackToTrackFn {
        self.set_vehicle_back_to_track
    }
    #[must_use]
    pub fn boink_set_vehicle_to_pitstop(&self) -> LegacySetVehicleToPitstopFn {
        self.set_vehicle_to_pitstop
    }
    #[must_use]
    pub fn boink_set_vehicle_at_start_pos(&self) -> LegacySetVehicleAtStartPosFn {
        self.set_vehicle_at_start_pos
    }
    #[must_use]
    pub fn boink_get_number_of_start_pos(&self) -> LegacyGetNumberOfStartPosFn {
        self.get_number_of_start_pos
    }
    #[must_use]
    pub fn boink_read_vehicle_ghost_mode_state(&self) -> LegacyReadVehicleGhostModeStateFn {
        self.read_vehicle_ghost_mode_state
    }
    #[must_use]
    pub fn boink_read_vehicle_race_metrics(&self) -> LegacyReadVehicleRaceMetricsFn {
        self.read_vehicle_race_metrics
    }
    #[must_use]
    pub fn boink_get_vehicle_pitstop_zone(&self) -> LegacyGetVehiclePitstopZoneFn {
        self.get_vehicle_pitstop_zone
    }
    #[must_use]
    pub fn boink_get_vehicle_personal_best_lap(&self) -> LegacyGetVehiclePersonalBestLapFn {
        self.get_vehicle_personal_best_lap
    }
    #[must_use]
    pub fn boink_get_best_lap(&self) -> LegacyGetBestLapFn {
        self.get_best_lap
    }
}
