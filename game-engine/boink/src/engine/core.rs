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
use crate::model::{Controls, VehicleState, WeatherParams};
#[cfg(feature = "legacy-native-lib")]
use crate::native::api::NativeApi;
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
        let code = unsafe { sys::boink_step_race(self.handle, dt_seconds) };
        tracing::debug!(code, "boink_step_race result");
        if code == sys::BOINK_OK {
            if self.debug_drawer_enabled {
                unsafe {
                    sys::boink_update_debug();
                }
            }
            Ok(())
        } else {
            tracing::debug!(code = code, "boink_step_race failed");
            Err(Error::from_ffi_status(code, "boink_step_race"))
        }
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
    pub fn set_controls(&mut self, vehicle_id: u64, controls: Controls) -> Result<()> {
        let ffi_controls = controls.as_ffi();
        let code =
            unsafe { sys::boink_set_controls(self.handle, vehicle_id, &ffi_controls as *const _) };
        tracing::debug!(code, vehicle_id, "boink_set_controls result");
        if code == sys::BOINK_OK {
            Ok(())
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

    /// Sets the world-space position on the track.
    #[instrument(skip(self, position))]
    pub fn set_track_position(&mut self, position: Vec3) -> Result<()> {
        let ffi_pos = position.into();
        tracing::debug!(
            x = position.x,
            y = position.y,
            z = position.z,
            "boink_set_track_position"
        );
        let code = unsafe { sys::boink_set_track_position(self.handle, &ffi_pos as *const _) };
        tracing::debug!(code, "boink_set_track_position result");
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            tracing::debug!(code = code, "boink_set_track_position failed");
            Err(Error::from_ffi_status(code, "boink_set_track_position"))
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
            VehicleState::try_from(raw)
        } else {
            tracing::debug!(code = code, "boink_read_vehicle_state failed");
            Err(Error::from_ffi_status(code, "boink_read_vehicle_state"))
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
