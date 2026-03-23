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
    TrackData, TyreType, VehicleBestLap, VehiclePitstopState, VehicleRaceMetrics, VehicleState,
    WeatherParams,
};
use crate::version::ensure_c_api_compatible;

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
    /// Initializes native Boink runtime for operations that require initialized engine state
    /// (for example loading vehicle meshes).
    ///
    /// Calling this function multiple times is safe as long as `debug_drawer_enabled`
    /// remains consistent across calls.
    pub fn initialize_runtime(debug_drawer_enabled: bool) -> Result<()> {
        ensure_c_api_compatible()?;
        ensure_initialized(debug_drawer_enabled)?;
        Ok(())
    }

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
        Self::initialize_runtime(debug_drawer_enabled)?;
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
            debug_drawer_enabled,
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

        if (simulated_dt_seconds - dt_seconds).abs() > f32::EPSILON {
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

    /// Updates global ghost mode settings used by simulation.
    pub fn set_ghost_mode_settings(&mut self, settings: GhostModeSettings) -> Result<()> {
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

    /// Sets the world-space position of a vehicle to a point before the finish line.
    #[instrument(skip(self))]
    pub fn set_vehicle_before_finish_line(&mut self, vehicle_id: u64) -> Result<()> {
        tracing::debug!(vehicle_id, "boink_set_vehicle_before_finish_line");
        let code = unsafe { sys::boink_set_vehicle_before_finish_line(self.handle, vehicle_id) };
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            Err(Error::from_ffi_status(
                code,
                "boink_set_vehicle_before_finish_line",
            ))
        }
    }

    /// Sets the world-space position of a vehicle to a random point.
    #[instrument(skip(self))]
    pub fn set_vehicle_random_pos(&mut self, vehicle_id: u64) -> Result<()> {
        tracing::debug!(vehicle_id, "boink_set_vehicle_random_pos");
        let code = unsafe { sys::boink_set_vehicle_random_pos(self.handle, vehicle_id) };
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            Err(Error::from_ffi_status(code, "boink_set_vehicle_random_pos"))
        }
    }

    /// Sets the world-space position of a vehicle to the closest point on track.
    #[instrument(skip(self))]
    pub fn set_vehicle_back_to_track(&mut self, vehicle_id: u64) -> Result<()> {
        tracing::debug!(vehicle_id, "boink_set_vehicle_back_to_track");
        let code = unsafe { sys::boink_set_vehicle_back_to_track(self.handle, vehicle_id) };
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            Err(Error::from_ffi_status(
                code,
                "boink_set_vehicle_back_to_track",
            ))
        }
    }

    /// Sets the world-space position of a vehicle to the pitstop fix zone.
    #[instrument(skip(self))]
    pub fn set_vehicle_to_pitstop(&mut self, vehicle_id: u64) -> Result<()> {
        tracing::debug!(vehicle_id, "boink_set_vehicle_to_pitstop");
        let code = unsafe { sys::boink_set_vehicle_to_pitstop(self.handle, vehicle_id) };
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            Err(Error::from_ffi_status(code, "boink_set_vehicle_to_pitstop"))
        }
    }

    /// Sets the tyre compound for the specified vehicle.
    ///
    /// Returns [`Error::ConditionNotMet`](crate::Error::ConditionNotMet) when
    /// native preconditions are not satisfied (for example, vehicle not in fix zone).
    #[instrument(skip(self))]
    pub fn set_vehicle_tyre_type(&mut self, vehicle_id: u64, tyre_type: TyreType) -> Result<()> {
        let ffi_tyre_type = tyre_type.to_ffi();

        tracing::debug!(vehicle_id, ?tyre_type, "boink_set_vehicle_tyre_type");
        let code =
            unsafe { sys::boink_set_vehicle_tyre_type(self.handle, vehicle_id, ffi_tyre_type) };
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            Err(Error::from_ffi_status(code, "boink_set_vehicle_tyre_type"))
        }
    }

    /// Retrieves the width and depth (length) of the specified vehicle.
    #[instrument(skip(self))]
    pub fn get_vehicle_dimensions(&self, vehicle_id: u64) -> Result<(f32, f32)> {
        let mut width: f32 = 0.0;
        let mut depth: f32 = 0.0;

        tracing::debug!(vehicle_id, "boink_get_vehicle_dimensions");
        let code = unsafe {
            sys::boink_get_vehicle_dimensions(
                self.handle,
                vehicle_id,
                &mut width as *mut _,
                &mut depth as *mut _,
            )
        };
        if code == sys::BOINK_OK {
            Ok((width, depth))
        } else {
            Err(Error::from_ffi_status(code, "boink_get_vehicle_dimensions"))
        }
    }

    /// Sets the world-space position of a vehicle at a selected starting position.
    #[instrument(skip(self))]
    pub fn set_vehicle_at_start_pos(&mut self, vehicle_id: u64, position_index: u64) -> Result<()> {
        tracing::debug!(vehicle_id, position_index, "boink_set_vehicle_at_start_pos");
        let code =
            unsafe { sys::boink_set_vehicle_at_start_pos(self.handle, vehicle_id, position_index) };
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            Err(Error::from_ffi_status(
                code,
                "boink_set_vehicle_at_start_pos",
            ))
        }
    }

    /// Returns number of available start positions.
    #[instrument(skip(self))]
    pub fn get_number_of_start_pos(&self) -> Result<u64> {
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

    /// Sets the world-space orientation of a vehicle.
    #[instrument(skip(self, orientation))]
    pub fn set_vehicle_orientation(
        &mut self,
        vehicle_id: u64,
        orientation: Quaternion,
    ) -> Result<()> {
        let ffi_orientation: sys::BoinkQuaternion = orientation.into();

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
            state.pitstop_state = self.read_vehicle_pitstop_state_with_fallback(vehicle_id);
            Ok(state)
        } else {
            tracing::debug!(code = code, "boink_read_vehicle_state failed");
            Err(Error::from_ffi_status(code, "boink_read_vehicle_state"))
        }
    }

    /// Reads race-progress metrics for the specified vehicle.
    #[instrument(skip(self))]
    pub fn read_vehicle_race_metrics(&self, vehicle_id: u64) -> Result<VehicleRaceMetrics> {
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

    /// Reads runtime pitstop-zone state for the specified vehicle.
    #[instrument(skip(self))]
    pub fn read_vehicle_pitstop_state(&self, vehicle_id: u64) -> Result<VehiclePitstopState> {
        let mut raw_zone = sys::BoinkPitstopZone::BOINK_PITSTOP_ZONE_NONE;
        let mut wheels_num: i32 = 0;
        tracing::debug!(vehicle_id, "boink_get_vehicle_pitstop_zone");
        let code = unsafe {
            sys::boink_get_vehicle_pitstop_zone(
                self.handle,
                vehicle_id,
                &mut raw_zone as *mut _,
                &mut wheels_num as *mut _,
            )
        };
        if code == sys::BOINK_OK {
            VehiclePitstopState::try_from_ffi(raw_zone as u32, wheels_num)
        } else {
            Err(Error::from_ffi_status(
                code,
                "boink_get_vehicle_pitstop_zone",
            ))
        }
    }

    /// Returns personal best lap data for the specified vehicle.
    ///
    /// Returns `Ok(None)` when the vehicle exists but has no best lap yet.
    #[instrument(skip(self))]
    pub fn get_vehicle_personal_best_lap(&self, vehicle_id: u64) -> Result<Option<VehicleBestLap>> {
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

    /// Returns best-lap data in the entire race.
    ///
    /// Returns `Ok(None)` when no best lap is available yet.
    #[instrument(skip(self))]
    pub fn get_best_lap(&self) -> Result<Option<RaceBestLap>> {
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

    fn read_vehicle_ghost_mode_runtime_state(&self, vehicle_id: u64) -> GhostModeRuntimeState {
        let mut raw: sys::BoinkGhostModeRuntimeState = unsafe { core::mem::zeroed() };
        tracing::debug!(vehicle_id, "boink_read_vehicle_ghost_mode_state");
        let code = unsafe {
            sys::boink_read_vehicle_ghost_mode_state(self.handle, vehicle_id, &mut raw as *mut _)
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

    fn read_vehicle_pitstop_state_with_fallback(&self, vehicle_id: u64) -> VehiclePitstopState {
        match self.read_vehicle_pitstop_state(vehicle_id) {
            Ok(state) => state,
            Err(err) => {
                tracing::warn!(
                    vehicle_id,
                    error = %err,
                    "boink_get_vehicle_pitstop_zone failed; falling back to default pitstop state"
                );
                VehiclePitstopState::default()
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
