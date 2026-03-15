//! Safe wrapper around the native Boink race handle.
//!
//! Once an [`Engine`] is constructed through [`EngineBuilder`](crate::engine::EngineBuilder),
//! this module provides high-level methods to drive the simulation and interact
//! with vehicles while ensuring resources are released correctly.

use boink_sys as sys;
use std::ffi::CString;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use tracing::instrument;

use crate::error::{Error, Result};
use crate::model::math::Vec3;
use crate::model::{
    AcceptedControls, Controls, GhostModeRuntimeState, GhostModeSettings, Quaternion, RaceBestLap,
    TrackData, VehicleBestLap, VehicleRaceMetrics, VehicleState, WeatherParams,
};
#[cfg(feature = "legacy-native-lib")]
use crate::native::api::NativeApi;
use crate::version::ensure_c_api_compatible;
#[cfg(feature = "legacy-native-lib")]
use crate::version::{Version, query_c_api_version};

/// Domain-level configuration of the vehicle model used by the engine.
#[derive(Debug, Clone)]
pub struct VehicleModelConfig {
    /// Mesh resource shared by all vehicles spawned with this model.
    pub mesh: Arc<VehicleMesh>,
    /// Position of the vehicle's center of mass in model space.
    pub center_of_mass: Vec3,
    /// Radius of the vehicle wheels.
    pub wheel_radius: f32,
    /// Rest length of the suspension.
    pub suspension_rest_length: f32,
    /// Total mass of the vehicle.
    pub mass: f32,
    /// Maximum steering angle of the front wheels in degrees.
    pub max_steer_angle_deg: f32,
}

/// Safe wrapper around a native vehicle mesh handle.
#[derive(Debug)]
pub struct VehicleMesh {
    handle: sys::BoinkVehicleMeshHandle,
}

impl VehicleMesh {
    /// Loads a vehicle mesh from a GLB file.
    pub fn load<P: AsRef<Path>>(glb_model_filename: P) -> Result<Self> {
        let c_path = to_cstring(&glb_model_filename)?;
        let mut handle: sys::BoinkVehicleMeshHandle = core::ptr::null_mut();
        tracing::debug!(
            path = %glb_model_filename.as_ref().display(),
            "boink_create_vehicle_mesh"
        );
        let code = unsafe { sys::boink_create_vehicle_mesh(c_path.as_ptr(), &mut handle) };
        tracing::debug!(
            code,
            handle_is_null = handle.is_null(),
            "boink_create_vehicle_mesh result"
        );
        if code == sys::BOINK_OK {
            if handle.is_null() {
                Err(Error::from_null_handle("boink_create_vehicle_mesh"))
            } else {
                Ok(Self { handle })
            }
        } else {
            Err(Error::from_ffi_status(code, "boink_create_vehicle_mesh"))
        }
    }

    pub(crate) fn handle(&self) -> sys::BoinkVehicleMeshHandle {
        self.handle
    }
}

impl Drop for VehicleMesh {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                sys::boink_destroy_vehicle_mesh(self.handle);
            }
        }
    }
}

/// High-level wrapper managing the lifetime of a native Boink race.
pub struct Engine {
    handle: sys::BoinkHandle,
    vehicle_model: sys::BoinkVehicleModel,
    debug_drawer_enabled: bool,
    _mesh_guard: Arc<VehicleMesh>,
    // Makes the type !Send and !Sync since the native engine is not thread-safe.
    _nosend: PhantomData<*mut ()>,
}

impl Engine {
    /// Creates and initializes a new Boink race instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - the native library exposes an incompatible C-API version,
    /// - the vehicle model contains invalid values,
    /// - the native race cannot be created.
    pub(crate) fn new<P: AsRef<Path>>(
        track_glb_filename: P,
        vehicle_model: VehicleModelConfig,
        debug_drawer_enabled: bool,
    ) -> Result<Self> {
        tracing::debug!(debug_drawer_enabled, "boink debug drawer setting");
        ensure_c_api_compatible()?;
        let init = ensure_initialized(debug_drawer_enabled)?;
        Self::validate_vehicle_model(&vehicle_model)?;

        let ffi_model = Self::to_ffi_vehicle_model(&vehicle_model);
        let c_path = to_cstring(&track_glb_filename)?;

        tracing::debug!(
            track = %track_glb_filename.as_ref().display(),
            "boink_create_race"
        );
        let handle = unsafe { sys::boink_create_race(c_path.as_ptr()) };
        if handle.is_null() {
            tracing::debug!("boink_create_race returned null handle");
            return Err(Error::from_null_handle("boink_create_race"));
        }

        tracing::debug!("Boink race initialized");

        Ok(Self {
            handle,
            vehicle_model: ffi_model,
            debug_drawer_enabled: init.debug_drawer_enabled,
            _mesh_guard: vehicle_model.mesh,
            _nosend: PhantomData,
        })
    }

    fn validate_vehicle_model(model: &VehicleModelConfig) -> Result<()> {
        fn finite(v: &Vec3) -> bool {
            v.x.is_finite() && v.y.is_finite() && v.z.is_finite()
        }

        if model.mesh.handle().is_null()
            || !finite(&model.center_of_mass)
            || !model.wheel_radius.is_finite()
            || !model.suspension_rest_length.is_finite()
            || !model.mass.is_finite()
            || !model.max_steer_angle_deg.is_finite()
        {
            return Err(Error::InvalidCarModel(
                "VehicleModel contains invalid values".to_owned(),
            ));
        }

        Ok(())
    }

    fn to_ffi_vehicle_model(model: &VehicleModelConfig) -> sys::BoinkVehicleModel {
        sys::BoinkVehicleModel {
            mesh: model.mesh.handle(),
            center_of_mass: model.center_of_mass.into(),
            wheel_radius: model.wheel_radius,
            suspension_rest_length: model.suspension_rest_length,
            mass: model.mass,
            max_steer_angle: model.max_steer_angle_deg,
        }
    }

    /// Advances the simulation by a fixed time step (seconds).
    #[instrument(skip(self))]
    pub fn step(&mut self, dt_seconds: f32) -> Result<()> {
        tracing::debug!(dt_seconds, "boink_step_race");
        let mut simulated_dt_seconds: f32 = 0.0;
        let code =
            unsafe { sys::boink_step_race(self.handle, dt_seconds, &mut simulated_dt_seconds) };
        tracing::debug!(code, simulated_dt_seconds, "boink_step_race result");

        if code != sys::BOINK_OK {
            tracing::debug!(code = code, "boink_step_race failed");
            return Err(Error::from_ffi_status(code, "boink_step_race"));
        }

        #[cfg(feature = "legacy-native-lib")]
        {
            let should_monitor_step_progress = {
                static STEP_PROGRESS_MONITORING_ENABLED: OnceLock<bool> = OnceLock::new();
                *STEP_PROGRESS_MONITORING_ENABLED.get_or_init(|| {
                    let min_supported = Version::new(0, 10, 0);
                    match query_c_api_version() {
                        Ok(version) if version >= min_supported => true,
                        Ok(_) => {
                            tracing::warn!(
                                min_supported = %min_supported,
                                "Boink does not report simulated dt_seconds in boink_step_race; step progress stall detection is disabled"
                            );
                            false
                        }
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                "Unable to query Boink version; step progress stall detection is disabled"
                            );
                            false
                        }
                    }
                })
            };
            if should_monitor_step_progress
                && (simulated_dt_seconds - dt_seconds).abs() > f32::EPSILON
            {
                let dt_delta_seconds = simulated_dt_seconds - dt_seconds;
                let dt_delta_abs_seconds = dt_delta_seconds.abs();
                tracing::warn!(
                    requested_dt_seconds = dt_seconds,
                    simulated_dt_seconds,
                    delta_dt_seconds = dt_delta_seconds,
                    delta_dt_abs_seconds = dt_delta_abs_seconds,
                    delta_dt_ms = dt_delta_seconds * 1000.0,
                    "Boink simulation advanced by a different dt than requested in step"
                );
            }
        }

        if self.debug_drawer_enabled {
            unsafe {
                sys::boink_update_debug();
            }
        }

        Ok(())
    }

    /// Returns the elapsed duration of the race (seconds).
    #[instrument(skip(self))]
    pub fn race_duration(&self) -> Result<f32> {
        let mut dur: f32 = 0.0;
        tracing::debug!("boink_get_race_duration");
        let code = unsafe { sys::boink_get_race_duration(self.handle, &mut dur) };
        tracing::debug!(code, dur, "boink_get_race_duration result");
        if code == sys::BOINK_OK {
            Ok(dur)
        } else {
            tracing::debug!(code = code, "boink_get_race_duration failed");
            Err(Error::from_ffi_status(code, "boink_get_race_duration"))
        }
    }

    /// Retrieves static track geometry parsed from the loaded track.
    #[instrument(skip(self))]
    pub fn track_data(&self) -> Result<TrackData> {
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            let Some(get_track_data) = api.boink_get_track_data() else {
                static WARNED_MISSING_GET_TRACK_DATA: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_GET_TRACK_DATA.set(()).is_ok() {
                    tracing::warn!(
                        "boink_get_track_data symbol not found in native library; track data queries are unavailable"
                    );
                }
                return Err(Error::Internal(
                    "boink_get_track_data is unavailable in this native library".to_string(),
                ));
            };

            let mut raw: sys::BoinkTrackData = unsafe { core::mem::zeroed() };
            tracing::debug!("boink_get_track_data (legacy dynamic symbol)");
            let code = unsafe { get_track_data(self.handle, &mut raw as *mut _) };
            tracing::debug!(
                code,
                sample_count = raw.centerline_sample_count,
                "boink_get_track_data result"
            );
            if code == sys::BOINK_OK {
                return unsafe { TrackData::try_from_ffi(raw) };
            }
            tracing::debug!(code = code, "boink_get_track_data failed");
            return Err(Error::from_ffi_status(code, "boink_get_track_data"));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            let mut raw: sys::BoinkTrackData = unsafe { core::mem::zeroed() };
            tracing::debug!("boink_get_track_data");
            let code = unsafe { sys::boink_get_track_data(self.handle, &mut raw as *mut _) };
            tracing::debug!(
                code,
                sample_count = raw.centerline_sample_count,
                "boink_get_track_data result"
            );
            if code == sys::BOINK_OK {
                unsafe { TrackData::try_from_ffi(raw) }
            } else {
                tracing::debug!(code = code, "boink_get_track_data failed");
                Err(Error::from_ffi_status(code, "boink_get_track_data"))
            }
        }
    }

    /// Spawns a new vehicle and returns its identifier.
    #[instrument(skip(self))]
    pub fn spawn_vehicle(&mut self) -> Result<u64> {
        let mut vehicle_id = 0u64;
        tracing::debug!("boink_spawn_vehicle");
        let spawn_code =
            unsafe { sys::boink_spawn_vehicle(self.handle, &self.vehicle_model, &mut vehicle_id) };
        tracing::debug!(spawn_code, vehicle_id, "boink_spawn_vehicle result");
        if spawn_code != sys::BOINK_OK {
            tracing::debug!(code = spawn_code, "boink_spawn_vehicle failed");
            return Err(Error::from_ffi_status(spawn_code, "boink_spawn_vehicle"));
        }

        // TODO: Temp solution
        let spawn_pos = sys::BoinkVec3 {
            x: -5.0,
            y: 5.0,
            z: 0.0,
        };
        let set_pos_code =
            unsafe { sys::boink_set_vehicle_position(self.handle, vehicle_id, &spawn_pos) };
        tracing::debug!(
            set_pos_code,
            vehicle_id,
            "boink_set_vehicle_position result (spawn)"
        );
        if set_pos_code != sys::BOINK_OK {
            tracing::debug!(
                code = set_pos_code,
                "boink_set_vehicle_position failed (spawn)"
            );
            return Err(Error::from_ffi_status(
                set_pos_code,
                "boink_set_vehicle_position",
            ));
        }

        // TODO: Temp solution
        let half = std::f32::consts::FRAC_1_SQRT_2;
        let spawn_orientation = Quaternion {
            x: 0.0,
            y: half,
            z: 0.0,
            w: half,
        };
        self.set_vehicle_orientation(vehicle_id, spawn_orientation)?;

        Ok(vehicle_id)
    }

    /// Removes a vehicle with the specified identifier from the race.
    #[instrument(skip(self))]
    pub fn despawn_vehicle(&mut self, vehicle_id: u64) -> Result<()> {
        tracing::debug!(vehicle_id, "boink_despawn_vehicle");
        let code = unsafe { sys::boink_despawn_vehicle(self.handle, vehicle_id) };
        tracing::debug!(code, vehicle_id, "boink_despawn_vehicle result");
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            tracing::debug!(code = code, "boink_despawn_vehicle failed");
            Err(Error::from_ffi_status(code, "boink_despawn_vehicle"))
        }
    }

    /// Applies driver controls to the specified vehicle.
    #[instrument(skip(self, controls))]
    pub fn set_controls(
        &mut self,
        vehicle_id: u64,
        controls: Controls,
    ) -> Result<AcceptedControls> {
        let ffi_controls = controls.as_ffi();
        let mut ffi_accepted_controls = sys::BoinkAcceptedControls {
            accepted_shift: sys::BoinkGearShift::BOINK_GEAR_SHIFT_NONE,
        };
        let code = unsafe {
            sys::boink_set_controls(
                self.handle,
                vehicle_id,
                &ffi_controls,
                &mut ffi_accepted_controls,
            )
        };
        tracing::debug!(code, vehicle_id, "boink_set_controls result");
        if code == sys::BOINK_OK {
            AcceptedControls::try_from(ffi_accepted_controls)
        } else {
            tracing::debug!(code = code, "boink_set_controls failed");
            Err(Error::from_ffi_status(code, "boink_set_controls"))
        }
    }

    /// Updates global weather parameters used by simulation.
    pub fn set_weather(&mut self, weather: WeatherParams) -> Result<()> {
        let ffi_weather = sys::BoinkWeather {
            cloudiness: weather.cloudiness,
            temperature_c: weather.temperature_c,
            rain_intensity: weather.rain_intensity,
        };

        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            let Some(set_weather) = api.boink_set_weather() else {
                static WARNED_MISSING_SET_WEATHER: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_SET_WEATHER.set(()).is_ok() {
                    tracing::warn!(
                        "boink_set_weather symbol not found in native library; weather updates are ignored"
                    );
                }
                return Ok(());
            };

            tracing::debug!(
                cloudiness = weather.cloudiness,
                temperature_c = weather.temperature_c,
                rain_intensity = weather.rain_intensity,
                "boink_set_weather (legacy dynamic symbol)"
            );
            let code = unsafe { set_weather(self.handle, &ffi_weather as *const _) };
            if code == sys::BOINK_OK {
                return Ok(());
            }
            return Err(Error::from_ffi_status(code, "boink_set_weather"));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            tracing::debug!(
                cloudiness = weather.cloudiness,
                temperature_c = weather.temperature_c,
                rain_intensity = weather.rain_intensity,
                "boink_set_weather"
            );
            let code = unsafe { sys::boink_set_weather(self.handle, &ffi_weather as *const _) };
            if code == sys::BOINK_OK {
                Ok(())
            } else {
                Err(Error::from_ffi_status(code, "boink_set_weather"))
            }
        }
    }

    /// Updates global ghost mode settings used by simulation.
    pub fn set_ghost_mode_settings(&mut self, settings: GhostModeSettings) -> Result<()> {
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            if settings.enabled {
                let ffi_settings = sys::BoinkGhostModeSettings {
                    enter_speed_max_mps: settings.enter_speed_max_mps,
                    exit_speed_min_mps: settings.exit_speed_min_mps,
                    enter_delay_ms: settings.enter_delay_ms,
                    exit_delay_ms: settings.exit_delay_ms,
                    until_completed_laps: settings.until_completed_laps,
                    vehicle_overlap_exit_delay_ms: settings.vehicle_overlap_exit_delay_ms,
                };

                let Some(set_ghost_mode_settings) = api.boink_set_ghost_mode_settings() else {
                    static WARNED_MISSING_SET_GHOST_MODE_SETTINGS: OnceLock<()> = OnceLock::new();
                    if WARNED_MISSING_SET_GHOST_MODE_SETTINGS.set(()).is_ok() {
                        tracing::warn!(
                            "boink_set_ghost_mode_settings symbol not found in native library; ghost mode settings updates are ignored"
                        );
                    }
                    return Ok(());
                };

                tracing::debug!(
                    enter_speed_max_mps = settings.enter_speed_max_mps,
                    exit_speed_min_mps = settings.exit_speed_min_mps,
                    enter_delay_ms = settings.enter_delay_ms,
                    exit_delay_ms = settings.exit_delay_ms,
                    until_completed_laps = settings.until_completed_laps,
                    vehicle_overlap_exit_delay_ms = settings.vehicle_overlap_exit_delay_ms,
                    "boink_set_ghost_mode_settings (legacy dynamic symbol)"
                );
                let code =
                    unsafe { set_ghost_mode_settings(self.handle, &ffi_settings as *const _) };
                if code == sys::BOINK_OK {
                    return Ok(());
                }
                return Err(Error::from_ffi_status(
                    code,
                    "boink_set_ghost_mode_settings",
                ));
            }

            let Some(disable_ghost_mode) = api.boink_disable_ghost_mode() else {
                static WARNED_MISSING_DISABLE_GHOST_MODE: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_DISABLE_GHOST_MODE.set(()).is_ok() {
                    tracing::warn!(
                        "boink_disable_ghost_mode symbol not found in native library; ghost mode disable request is ignored"
                    );
                }
                return Ok(());
            };

            tracing::debug!("boink_disable_ghost_mode (legacy dynamic symbol)");
            let code = unsafe { disable_ghost_mode(self.handle) };
            if code == sys::BOINK_OK {
                return Ok(());
            }
            return Err(Error::from_ffi_status(code, "boink_disable_ghost_mode"));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            if settings.enabled {
                let ffi_settings = sys::BoinkGhostModeSettings {
                    enter_speed_max_mps: settings.enter_speed_max_mps,
                    exit_speed_min_mps: settings.exit_speed_min_mps,
                    enter_delay_ms: settings.enter_delay_ms,
                    exit_delay_ms: settings.exit_delay_ms,
                    until_completed_laps: settings.until_completed_laps,
                    vehicle_overlap_exit_delay_ms: settings.vehicle_overlap_exit_delay_ms,
                };

                tracing::debug!(
                    enter_speed_max_mps = settings.enter_speed_max_mps,
                    exit_speed_min_mps = settings.exit_speed_min_mps,
                    enter_delay_ms = settings.enter_delay_ms,
                    exit_delay_ms = settings.exit_delay_ms,
                    until_completed_laps = settings.until_completed_laps,
                    vehicle_overlap_exit_delay_ms = settings.vehicle_overlap_exit_delay_ms,
                    "boink_set_ghost_mode_settings"
                );
                let code = unsafe {
                    sys::boink_set_ghost_mode_settings(self.handle, &ffi_settings as *const _)
                };
                if code == sys::BOINK_OK {
                    Ok(())
                } else {
                    Err(Error::from_ffi_status(
                        code,
                        "boink_set_ghost_mode_settings",
                    ))
                }
            } else {
                tracing::debug!("boink_disable_ghost_mode");
                let code = unsafe { sys::boink_disable_ghost_mode(self.handle) };
                if code == sys::BOINK_OK {
                    Ok(())
                } else {
                    Err(Error::from_ffi_status(code, "boink_disable_ghost_mode"))
                }
            }
        }
    }

    /// Sets the world-space position of a vehicle.
    #[instrument(skip(self, position))]
    pub fn set_vehicle_position(&mut self, vehicle_id: u64, position: Vec3) -> Result<()> {
        let ffi_pos = position.into();
        tracing::debug!(
            vehicle_id,
            x = position.x,
            y = position.y,
            z = position.z,
            "boink_set_vehicle_position"
        );
        let code = unsafe {
            sys::boink_set_vehicle_position(self.handle, vehicle_id, &ffi_pos as *const _)
        };
        tracing::debug!(code, vehicle_id, "boink_set_vehicle_position result");
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            tracing::debug!(code = code, "boink_set_vehicle_position failed");
            Err(Error::from_ffi_status(code, "boink_set_vehicle_position"))
        }
    }

    /// Sets the world-space position of a vehicle to a point before the given point.
    #[instrument(skip(self, point))]
    pub fn set_vehicle_before_point(&mut self, vehicle_id: u64, point: Vec3) -> Result<()> {
        let ffi_point: sys::BoinkVec3 = point.into();

        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            let Some(set_vehicle_before_point) = api.boink_set_vehicle_before_point() else {
                static WARNED_MISSING_SET_VEHICLE_BEFORE_POINT: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_SET_VEHICLE_BEFORE_POINT.set(()).is_ok() {
                    tracing::warn!(
                        "boink_set_vehicle_before_point symbol not found in native library; before-point updates are ignored"
                    );
                }
                return Ok(());
            };
            tracing::debug!(
                vehicle_id,
                x = point.x,
                y = point.y,
                z = point.z,
                "boink_set_vehicle_before_point (legacy dynamic symbol)"
            );
            let code = unsafe {
                set_vehicle_before_point(self.handle, vehicle_id, &ffi_point as *const _)
            };
            if code == sys::BOINK_OK {
                return Ok(());
            }
            return Err(Error::from_ffi_status(
                code,
                "boink_set_vehicle_before_point",
            ));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            tracing::debug!(
                vehicle_id,
                x = point.x,
                y = point.y,
                z = point.z,
                "boink_set_vehicle_before_point"
            );
            let code = unsafe {
                sys::boink_set_vehicle_before_point(self.handle, vehicle_id, &ffi_point as *const _)
            };
            if code == sys::BOINK_OK {
                Ok(())
            } else {
                Err(Error::from_ffi_status(
                    code,
                    "boink_set_vehicle_before_point",
                ))
            }
        }
    }

    /// Sets the world-space position of a vehicle to a point before the finish line.
    #[instrument(skip(self))]
    pub fn set_vehicle_before_finish_line(&mut self, vehicle_id: u64) -> Result<()> {
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            let Some(set_vehicle_before_finish_line) = api.boink_set_vehicle_before_finish_line()
            else {
                static WARNED_MISSING_SET_VEHICLE_BEFORE_FINISH_LINE: OnceLock<()> =
                    OnceLock::new();
                if WARNED_MISSING_SET_VEHICLE_BEFORE_FINISH_LINE
                    .set(())
                    .is_ok()
                {
                    tracing::warn!(
                        "boink_set_vehicle_before_finish_line symbol not found in native library; before-finish-line updates are ignored"
                    );
                }
                return Ok(());
            };
            tracing::debug!(
                vehicle_id,
                "boink_set_vehicle_before_finish_line (legacy dynamic symbol)"
            );
            let code = unsafe { set_vehicle_before_finish_line(self.handle, vehicle_id) };
            if code == sys::BOINK_OK {
                return Ok(());
            }
            return Err(Error::from_ffi_status(
                code,
                "boink_set_vehicle_before_finish_line",
            ));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            tracing::debug!(vehicle_id, "boink_set_vehicle_before_finish_line");
            let code =
                unsafe { sys::boink_set_vehicle_before_finish_line(self.handle, vehicle_id) };
            if code == sys::BOINK_OK {
                Ok(())
            } else {
                Err(Error::from_ffi_status(
                    code,
                    "boink_set_vehicle_before_finish_line",
                ))
            }
        }
    }

    /// Sets the world-space position of a vehicle to a random point.
    #[instrument(skip(self))]
    pub fn set_vehicle_random_pos(&mut self, vehicle_id: u64) -> Result<()> {
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            let Some(set_vehicle_random_pos) = api.boink_set_vehicle_random_pos() else {
                static WARNED_MISSING_SET_VEHICLE_RANDOM_POS: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_SET_VEHICLE_RANDOM_POS.set(()).is_ok() {
                    tracing::warn!(
                        "boink_set_vehicle_random_pos symbol not found in native library; random position updates are ignored"
                    );
                }
                return Ok(());
            };
            tracing::debug!(
                vehicle_id,
                "boink_set_vehicle_random_pos (legacy dynamic symbol)"
            );
            let code = unsafe { set_vehicle_random_pos(self.handle, vehicle_id) };
            if code == sys::BOINK_OK {
                return Ok(());
            }
            return Err(Error::from_ffi_status(code, "boink_set_vehicle_random_pos"));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            tracing::debug!(vehicle_id, "boink_set_vehicle_random_pos");
            let code = unsafe { sys::boink_set_vehicle_random_pos(self.handle, vehicle_id) };
            if code == sys::BOINK_OK {
                Ok(())
            } else {
                Err(Error::from_ffi_status(code, "boink_set_vehicle_random_pos"))
            }
        }
    }

    /// Sets the world-space position of a vehicle at a selected starting position.
    #[instrument(skip(self))]
    pub fn set_vehicle_at_start_pos(&mut self, vehicle_id: u64, position_index: u64) -> Result<()> {
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            let Some(set_vehicle_at_start_pos) = api.boink_set_vehicle_at_start_pos() else {
                static WARNED_MISSING_SET_VEHICLE_AT_START_POS: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_SET_VEHICLE_AT_START_POS.set(()).is_ok() {
                    tracing::warn!(
                        "boink_set_vehicle_at_start_pos symbol not found in native library; start-position updates are ignored"
                    );
                }
                return Ok(());
            };
            tracing::debug!(
                vehicle_id,
                position_index,
                "boink_set_vehicle_at_start_pos (legacy dynamic symbol)"
            );
            let code = unsafe { set_vehicle_at_start_pos(self.handle, vehicle_id, position_index) };
            if code == sys::BOINK_OK {
                return Ok(());
            }
            return Err(Error::from_ffi_status(
                code,
                "boink_set_vehicle_at_start_pos",
            ));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            tracing::debug!(vehicle_id, position_index, "boink_set_vehicle_at_start_pos");
            let code = unsafe {
                sys::boink_set_vehicle_at_start_pos(self.handle, vehicle_id, position_index)
            };
            if code == sys::BOINK_OK {
                Ok(())
            } else {
                Err(Error::from_ffi_status(
                    code,
                    "boink_set_vehicle_at_start_pos",
                ))
            }
        }
    }

    /// Returns number of available start positions.
    #[instrument(skip(self))]
    pub fn get_number_of_start_pos(&self) -> Result<u64> {
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            let Some(get_number_of_start_pos) = api.boink_get_number_of_start_pos() else {
                static WARNED_MISSING_GET_NUMBER_OF_START_POS: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_GET_NUMBER_OF_START_POS.set(()).is_ok() {
                    tracing::warn!(
                        "boink_get_number_of_start_pos symbol not found in native library; start-position count query is unavailable"
                    );
                }
                return Err(Error::Internal(
                    "boink_get_number_of_start_pos is unavailable in this native library"
                        .to_string(),
                ));
            };
            let mut out_number_pos: u64 = 0;
            tracing::debug!("boink_get_number_of_start_pos (legacy dynamic symbol)");
            let code =
                unsafe { get_number_of_start_pos(self.handle, &mut out_number_pos as *mut _) };
            if code == sys::BOINK_OK {
                return Ok(out_number_pos);
            }
            return Err(Error::from_ffi_status(
                code,
                "boink_get_number_of_start_pos",
            ));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            let mut out_number_pos: u64 = 0;
            tracing::debug!("boink_get_number_of_start_pos");
            let code = unsafe {
                sys::boink_get_number_of_start_pos(self.handle, &mut out_number_pos as *mut _)
            };
            if code == sys::BOINK_OK {
                Ok(out_number_pos)
            } else {
                Err(Error::from_ffi_status(
                    code,
                    "boink_get_number_of_start_pos",
                ))
            }
        }
    }

    /// Sets the world-space orientation of a vehicle.
    #[instrument(skip(self, orientation))]
    pub fn set_vehicle_orientation(
        &mut self,
        vehicle_id: u64,
        orientation: Quaternion,
    ) -> Result<()> {
        let ffi_orientation: sys::BoinkQuaternion = orientation.into();

        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            let Some(set_vehicle_orientation) = api.boink_set_vehicle_orientation() else {
                static WARNED_MISSING_SET_VEHICLE_ORIENTATION: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_SET_VEHICLE_ORIENTATION.set(()).is_ok() {
                    tracing::warn!(
                        "boink_set_vehicle_orientation symbol not found in native library; orientation updates are ignored"
                    );
                }
                return Ok(());
            };
            tracing::debug!(
                vehicle_id,
                x = orientation.x,
                y = orientation.y,
                z = orientation.z,
                w = orientation.w,
                "boink_set_vehicle_orientation (legacy dynamic symbol)"
            );
            let code = unsafe {
                set_vehicle_orientation(self.handle, vehicle_id, &ffi_orientation as *const _)
            };
            if code == sys::BOINK_OK {
                return Ok(());
            }
            return Err(Error::from_ffi_status(
                code,
                "boink_set_vehicle_orientation",
            ));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            tracing::debug!(
                vehicle_id,
                x = orientation.x,
                y = orientation.y,
                z = orientation.z,
                w = orientation.w,
                "boink_set_vehicle_orientation"
            );
            let code = unsafe {
                sys::boink_set_vehicle_orientation(
                    self.handle,
                    vehicle_id,
                    &ffi_orientation as *const _,
                )
            };
            if code == sys::BOINK_OK {
                Ok(())
            } else {
                Err(Error::from_ffi_status(
                    code,
                    "boink_set_vehicle_orientation",
                ))
            }
        }
    }

    /// Reads the current state of the specified vehicle.
    #[instrument(skip(self))]
    pub fn read_vehicle_state(&self, vehicle_id: u64) -> Result<VehicleState> {
        let mut raw: sys::BoinkVehicleState = unsafe { core::mem::zeroed() };
        tracing::debug!(vehicle_id, "boink_read_vehicle_state");
        let code =
            unsafe { sys::boink_read_vehicle_state(self.handle, vehicle_id, &mut raw as *mut _) };
        tracing::debug!(code, vehicle_id, "boink_read_vehicle_state result");
        if code == sys::BOINK_OK {
            let mut state = VehicleState::try_from(raw)?;
            state.ghost_mode_runtime = self.read_vehicle_ghost_mode_runtime_state(vehicle_id);
            Ok(state)
        } else {
            tracing::debug!(code = code, "boink_read_vehicle_state failed");
            Err(Error::from_ffi_status(code, "boink_read_vehicle_state"))
        }
    }

    /// Reads race-progress metrics for the specified vehicle.
    #[instrument(skip(self))]
    pub fn read_vehicle_race_metrics(&self, vehicle_id: u64) -> Result<VehicleRaceMetrics> {
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            let Some(read_vehicle_race_metrics) = api.boink_read_vehicle_race_metrics() else {
                static WARNED_MISSING_READ_VEHICLE_RACE_METRICS: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_READ_VEHICLE_RACE_METRICS.set(()).is_ok() {
                    tracing::warn!(
                        "boink_read_vehicle_race_metrics symbol not found in native library; returning default race metrics"
                    );
                }
                return Ok(VehicleRaceMetrics::default());
            };

            let mut raw: sys::BoinkVehicleRaceMetrics = unsafe { core::mem::zeroed() };
            tracing::debug!(
                vehicle_id,
                "boink_read_vehicle_race_metrics (legacy dynamic symbol)"
            );
            let code =
                unsafe { read_vehicle_race_metrics(self.handle, vehicle_id, &mut raw as *mut _) };
            if code == sys::BOINK_OK {
                return Ok(raw.into());
            }
            return Err(Error::from_ffi_status(
                code,
                "boink_read_vehicle_race_metrics",
            ));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            let mut raw: sys::BoinkVehicleRaceMetrics = unsafe { core::mem::zeroed() };
            tracing::debug!(vehicle_id, "boink_read_vehicle_race_metrics");
            let code = unsafe {
                sys::boink_read_vehicle_race_metrics(self.handle, vehicle_id, &mut raw as *mut _)
            };
            if code == sys::BOINK_OK {
                Ok(raw.into())
            } else {
                Err(Error::from_ffi_status(
                    code,
                    "boink_read_vehicle_race_metrics",
                ))
            }
        }
    }

    /// Returns personal best lap data for the specified vehicle.
    ///
    /// Returns `Ok(None)` when the vehicle exists but has no best lap yet.
    #[instrument(skip(self))]
    pub fn get_vehicle_personal_best_lap(&self, vehicle_id: u64) -> Result<Option<VehicleBestLap>> {
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            let Some(get_vehicle_personal_best_lap) = api.boink_get_vehicle_personal_best_lap()
            else {
                static WARNED_MISSING_GET_VEHICLE_PERSONAL_BEST_LAP: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_GET_VEHICLE_PERSONAL_BEST_LAP.set(()).is_ok() {
                    tracing::warn!(
                        "boink_get_vehicle_personal_best_lap symbol not found in native library; returning no data"
                    );
                }
                return Ok(None);
            };

            let mut lap: u32 = 0;
            let mut lap_time_ms: u32 = 0;
            tracing::debug!(
                vehicle_id,
                "boink_get_vehicle_personal_best_lap (legacy dynamic symbol)"
            );
            let code = unsafe {
                get_vehicle_personal_best_lap(
                    self.handle,
                    vehicle_id,
                    &mut lap as *mut _,
                    &mut lap_time_ms as *mut _,
                )
            };
            if code == sys::BOINK_OK {
                return Ok(Some(VehicleBestLap { lap, lap_time_ms }));
            }
            if code == sys::BOINK_NO_DATA {
                return Ok(None);
            }
            return Err(Error::from_ffi_status(
                code,
                "boink_get_vehicle_personal_best_lap",
            ));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            let mut lap: u32 = 0;
            let mut lap_time_ms: u32 = 0;
            tracing::debug!(vehicle_id, "boink_get_vehicle_personal_best_lap");
            let code = unsafe {
                sys::boink_get_vehicle_personal_best_lap(
                    self.handle,
                    vehicle_id,
                    &mut lap as *mut _,
                    &mut lap_time_ms as *mut _,
                )
            };
            if code == sys::BOINK_OK {
                Ok(Some(VehicleBestLap { lap, lap_time_ms }))
            } else if code == sys::BOINK_NO_DATA {
                Ok(None)
            } else {
                Err(Error::from_ffi_status(
                    code,
                    "boink_get_vehicle_personal_best_lap",
                ))
            }
        }
    }

    /// Returns best-lap data in the entire race.
    ///
    /// Returns `Ok(None)` when no best lap is available yet.
    #[instrument(skip(self))]
    pub fn get_best_lap(&self) -> Result<Option<RaceBestLap>> {
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = NativeApi::instance()
                .map_err(|err| Error::Internal(format!("native api unavailable: {err}")))?;
            let Some(get_best_lap) = api.boink_get_best_lap() else {
                static WARNED_MISSING_GET_BEST_LAP: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_GET_BEST_LAP.set(()).is_ok() {
                    tracing::warn!(
                        "boink_get_best_lap symbol not found in native library; returning no data"
                    );
                }
                return Ok(None);
            };

            let mut vehicle_id: u64 = 0;
            let mut lap: u32 = 0;
            let mut lap_time_ms: u32 = 0;
            tracing::debug!("boink_get_best_lap (legacy dynamic symbol)");
            let code = unsafe {
                get_best_lap(
                    self.handle,
                    &mut vehicle_id as *mut _,
                    &mut lap as *mut _,
                    &mut lap_time_ms as *mut _,
                )
            };
            if code == sys::BOINK_OK {
                return Ok(Some(RaceBestLap {
                    vehicle_id,
                    lap,
                    lap_time_ms,
                }));
            }
            if code == sys::BOINK_NO_DATA {
                return Ok(None);
            }
            return Err(Error::from_ffi_status(code, "boink_get_best_lap"));
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            let mut vehicle_id: u64 = 0;
            let mut lap: u32 = 0;
            let mut lap_time_ms: u32 = 0;
            tracing::debug!("boink_get_best_lap");
            let code = unsafe {
                sys::boink_get_best_lap(
                    self.handle,
                    &mut vehicle_id as *mut _,
                    &mut lap as *mut _,
                    &mut lap_time_ms as *mut _,
                )
            };
            if code == sys::BOINK_OK {
                Ok(Some(RaceBestLap {
                    vehicle_id,
                    lap,
                    lap_time_ms,
                }))
            } else if code == sys::BOINK_NO_DATA {
                Ok(None)
            } else {
                Err(Error::from_ffi_status(code, "boink_get_best_lap"))
            }
        }
    }

    fn read_vehicle_ghost_mode_runtime_state(&self, vehicle_id: u64) -> GhostModeRuntimeState {
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = match NativeApi::instance() {
                Ok(api) => api,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        vehicle_id,
                        "native api unavailable; falling back to inactive ghost mode state"
                    );
                    return GhostModeRuntimeState::default();
                }
            };
            let Some(read_vehicle_ghost_mode_state) = api.boink_read_vehicle_ghost_mode_state()
            else {
                static WARNED_MISSING_READ_VEHICLE_GHOST_MODE_STATE: OnceLock<()> = OnceLock::new();
                if WARNED_MISSING_READ_VEHICLE_GHOST_MODE_STATE.set(()).is_ok() {
                    tracing::warn!(
                        "boink_read_vehicle_ghost_mode_state symbol not found in native library; falling back to inactive ghost mode state"
                    );
                }
                return GhostModeRuntimeState::default();
            };

            let mut raw: sys::BoinkGhostModeRuntimeState = unsafe { core::mem::zeroed() };
            tracing::debug!(
                vehicle_id,
                "boink_read_vehicle_ghost_mode_state (legacy dynamic symbol)"
            );
            let code = unsafe {
                read_vehicle_ghost_mode_state(self.handle, vehicle_id, &mut raw as *mut _)
            };
            if code == sys::BOINK_OK {
                return raw.into();
            }
            tracing::warn!(
                vehicle_id,
                code,
                "boink_read_vehicle_ghost_mode_state failed; falling back to inactive ghost mode state"
            );
            return GhostModeRuntimeState::default();
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            let mut raw: sys::BoinkGhostModeRuntimeState = unsafe { core::mem::zeroed() };
            tracing::debug!(vehicle_id, "boink_read_vehicle_ghost_mode_state");
            let code = unsafe {
                sys::boink_read_vehicle_ghost_mode_state(
                    self.handle,
                    vehicle_id,
                    &mut raw as *mut _,
                )
            };
            if code == sys::BOINK_OK {
                raw.into()
            } else {
                tracing::warn!(
                    vehicle_id,
                    code,
                    "boink_read_vehicle_ghost_mode_state failed; falling back to inactive ghost mode state"
                );
                GhostModeRuntimeState::default()
            }
        }
    }

    /// Returns whether the underlying native handle is valid.
    #[inline]
    pub fn is_valid(&self) -> bool {
        !self.handle.is_null()
    }

    /// Returns true if the debug drawer is enabled and should close.
    pub fn should_close_debug(&self) -> bool {
        self.debug_drawer_enabled && unsafe { sys::boink_should_close_debug() }
    }

    /// Runs the debug drawer loop until the debug window closes.
    pub fn run_debug_drawer(&mut self) -> Result<()> {
        let mut prev = unsafe { sys::boink_get_time_debug() };
        while unsafe { !sys::boink_should_close_debug() } {
            let now = unsafe { sys::boink_get_time_debug() };
            let dt = now - prev;
            prev = now;

            self.step(dt)?;
        }

        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if self.is_valid() {
            tracing::debug!("boink_destroy_race");
            unsafe {
                sys::boink_destroy_race(self.handle);
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct InitState {
    debug_drawer_enabled: bool,
}

fn ensure_initialized(debug_drawer_enabled: bool) -> Result<InitState> {
    static INIT: OnceLock<Result<InitState, Error>> = OnceLock::new();
    match INIT.get_or_init(|| {
        tracing::debug!(debug_drawer_enabled, "boink_init");
        let code = unsafe { sys::boink_init(debug_drawer_enabled) };
        tracing::debug!(code, "boink_init result");
        if code == sys::BOINK_OK {
            Ok(InitState {
                debug_drawer_enabled,
            })
        } else {
            Err(Error::from_ffi_status(code, "boink_init"))
        }
    }) {
        Ok(state) => {
            if state.debug_drawer_enabled != debug_drawer_enabled {
                Err(Error::Internal(
                    "boink_init called with a different debug drawer setting".to_string(),
                ))
            } else {
                Ok(*state)
            }
        }
        Err(err) => Err(err.clone()),
    }
}

fn to_cstring<P: AsRef<Path>>(path: P) -> Result<CString> {
    let path = path.as_ref().to_string_lossy();
    CString::new(path.as_bytes()).map_err(|err| Error::InvalidString(err.to_string()))
}
