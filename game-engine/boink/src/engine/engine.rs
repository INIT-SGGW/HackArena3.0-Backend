//! Safe wrapper around the native Boink world handle.
//!
//! Once an [`Engine`] is constructed through [`EngineBuilder`](crate::engine::EngineBuilder),
//! this module provides high-level methods to drive the simulation and interact
//! with cars while ensuring resources are released correctly.

use std::marker::PhantomData;

use crate::error::{Error, Result};
use crate::model::math::Vec3;
use crate::model::{CarState, Controls};

use tracing::{debug, instrument};

use boink_sys as sys;

/// Domain-level configuration of the car model used by the engine.
///
/// This structure represents a validated, engine-ready description
/// of the vehicle geometry and steering constraints.
#[derive(Debug, Clone)]
pub(crate) struct CarModelConfig {
    /// Front-left wheel position relative to the car origin (meters).
    pub front_left_wheel: Vec3,
    /// Front-right wheel position relative to the car origin (meters).
    pub front_right_wheel: Vec3,
    /// Rear-left wheel position relative to the car origin (meters).
    pub rear_left_wheel: Vec3,
    /// Rear-right wheel position relative to the car origin (meters).
    pub rear_right_wheel: Vec3,
    /// Maximum steering angle in degrees.
    pub max_steer_angle_deg: f64,
}

/// High-level wrapper managing the lifetime of a native Boink world.
pub struct Engine {
    handle: sys::BoinkHandle,
    // Makes the type !Send and !Sync since the native engine is not thread-safe.
    _nosend: PhantomData<*mut ()>,
}

impl Engine {
    /// Creates and initializes a new Boink simulation world.
    ///
    /// This is the **only** place where:
    /// - FFI calls are performed to create the world,
    /// - the native handle is validated,
    /// - engine invariants are enforced.
    ///
    /// Higher-level helpers such as `EngineBuilder` must delegate
    /// world creation to this method.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - the car model contains invalid values,
    /// - the native world cannot be created,
    /// - the world fails to start at the given simulation time.
    #[instrument(skip(car_model))]
    pub(crate) fn new(car_model: CarModelConfig, start_time_seconds: f64) -> Result<Self> {
        Self::validate_car_model(&car_model)?;

        let ffi_model = Self::to_ffi_car_model(&car_model);

        let handle = unsafe { sys::boink_create_world(&ffi_model as *const sys::BoinkCarModel) };

        if handle.is_null() {
            debug!("boink_create_world returned null handle");
            return Err(Error::NullHandle("boink_create_world"));
        }

        let status = unsafe { sys::boink_begin_world(handle, start_time_seconds) };
        if status != sys::BOINK_OK {
            debug!(status = status, "boink_begin_world failed");
            unsafe {
                sys::boink_destroy_world(handle);
            }

            return Err(Error::from_ffi_status(status as i32, "boink_begin_world"));
        }

        debug!("Boink world initialized");

        Ok(Self {
            handle,
            _nosend: PhantomData,
        })
    }

    fn validate_car_model(model: &CarModelConfig) -> Result<()> {
        fn finite(v: &Vec3) -> bool {
            v.x.is_finite() && v.y.is_finite() && v.z.is_finite()
        }

        if !finite(&model.front_left_wheel)
            || !finite(&model.front_right_wheel)
            || !finite(&model.rear_left_wheel)
            || !finite(&model.rear_right_wheel)
            || !model.max_steer_angle_deg.is_finite()
        {
            return Err(Error::InvalidCarModel(
                "CarModel contains non-finite values".to_owned(),
            ));
        }

        Ok(())
    }

    fn to_ffi_car_model(model: &CarModelConfig) -> sys::BoinkCarModel {
        sys::BoinkCarModel {
            front_left_wheel: Self::to_ffi_vec3(&model.front_left_wheel),
            front_right_wheel: Self::to_ffi_vec3(&model.front_right_wheel),
            rear_left_wheel: Self::to_ffi_vec3(&model.rear_left_wheel),
            rear_right_wheel: Self::to_ffi_vec3(&model.rear_right_wheel),
            max_steer_angle: model.max_steer_angle_deg,
        }
    }

    fn to_ffi_vec3(v: &Vec3) -> sys::BoinkVec3 {
        sys::BoinkVec3 {
            x: v.x,
            y: v.y,
            z: v.z,
        }
    }

    /// Advances the simulation by a fixed time step (seconds).
    #[instrument(skip(self))]
    pub fn step(&mut self, dt_seconds: f64) -> Result<()> {
        let code = unsafe { sys::boink_step(self.handle, dt_seconds) };
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            debug!(code = code, "boink_step failed");
            Err(Error::from_code(code))
        }
    }

    /// Spawns a new car and returns its identifier.
    #[instrument(skip(self))]
    pub fn spawn_car(&mut self) -> Result<u64> {
        let mut car_id = 0u64;
        let code = unsafe { sys::boink_spawn_car(self.handle, &mut car_id as *mut u64) };
        if code == sys::BOINK_OK {
            Ok(car_id)
        } else {
            debug!(code = code, "boink_spawn_car failed");
            Err(Error::from_code(code))
        }
    }

    /// Removes a car with the specified identifier from the world.
    #[instrument(skip(self))]
    pub fn despawn_car(&mut self, car_id: u64) -> Result<()> {
        let code = unsafe { sys::boink_despawn_car(self.handle, car_id) };
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            debug!(code = code, "boink_despawn_car failed");
            Err(Error::from_code(code))
        }
    }

    /// Applies driver controls to the specified car.
    #[instrument(skip(self, controls))]
    pub fn set_controls(&mut self, car_id: u64, controls: Controls) -> Result<()> {
        let ffi_controls = controls.as_ffi();
        let code =
            unsafe { sys::boink_set_controls(self.handle, car_id, &ffi_controls as *const _) };
        if code == sys::BOINK_OK {
            Ok(())
        } else {
            debug!(code = code, "boink_set_controls failed");
            Err(Error::from_code(code))
        }
    }

    /// Reads the current state of the specified car.
    #[instrument(skip(self))]
    pub fn read_car_state(&self, car_id: u64) -> Result<CarState> {
        let mut raw: sys::BoinkCarState = unsafe { core::mem::zeroed() };
        let code = unsafe { sys::boink_read_car_state(self.handle, car_id, &mut raw as *mut _) };
        if code == sys::BOINK_OK {
            CarState::try_from(raw)
        } else {
            debug!(code = code, "boink_read_car_state failed");
            Err(Error::from_code(code))
        }
    }

    /// Returns whether the underlying native handle is valid.
    #[inline]
    pub fn is_valid(&self) -> bool {
        !self.handle.is_null()
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if self.is_valid() {
            unsafe {
                sys::boink_destroy_world(self.handle);
            }
        }
    }
}
