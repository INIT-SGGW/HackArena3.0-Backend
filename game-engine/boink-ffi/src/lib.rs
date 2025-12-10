//! # Boink FFI
//!
//! This crate defines the stable C-compatible interface (ABI) for the Boink
//! engine. It exposes only types and function signatures. The implementation
//! is provided by the native engine library written in C or C++.
//!
//! All exported structs use `#[repr(C)]` and FFI-safe types to ensure ABI
//! stability across supported platforms and architectures.

#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]

use libc::{c_double, c_int, c_uint, c_void};

pub const BOINK_C_API_VERSION_MAJOR: c_uint = 0;
pub const BOINK_C_API_VERSION_MINOR: c_uint = 1;
pub const BOINK_C_API_VERSION_PATCH: c_uint = 0;

/// Indicates successful operation.
pub const BOINK_OK: c_int = 0;

/// Indicates an invalid argument (for example a null pointer or an out-of-range value).
pub const BOINK_ERR_INVALID_ARG: c_int = 1;

/// Indicates that the output buffer was too small.
pub const BOINK_ERR_BUFFER_TOO_SMALL: c_int = 2;

/// Indicates that a requested object or identifier was not found.
pub const BOINK_ERR_NOT_FOUND: c_int = 3;

/// Indicates an internal engine error.
pub const BOINK_ERR_INTERNAL: c_int = 100;

/// Represents an opaque engine handle.
///
/// The pointer refers to an internal world or engine instance allocated
/// and owned by the native C or C++ side.
pub type BoinkHandle = *mut c_void;

/// Represents normalized control inputs of a driver.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkControls {
    /// Throttle demand in the range [0.0, 1.0].
    pub throttle: c_double,
    /// Brake demand in the range [0.0, 1.0].
    pub brake: c_double,
    /// Normalized steering input in the range [-1.0, 1.0].
    ///
    /// Negative values correspond to steering left.
    /// Positive values correspond to steering right.
    pub steer: c_double,
}

/// Represents a 3D vector in world coordinates (meters).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkVec3 {
    /// X component in meters.
    pub x: c_double,
    /// Y component in meters.
    pub y: c_double,
    /// Z component in meters.
    pub z: c_double,
}

/// Describes the geometric and steering properties of a car model.
///
/// The car model is shared by all car entities in the world.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkCarModel {
    /// Position of the front-left wheel relative to the car origin (meters).
    pub front_left_wheel: BoinkVec3,
    /// Position of the front-right wheel relative to the car origin (meters).
    pub front_right_wheel: BoinkVec3,
    /// Position of the rear-left wheel relative to the car origin (meters).
    pub rear_left_wheel: BoinkVec3,
    /// Position of the rear-right wheel relative to the car origin (meters).
    pub rear_right_wheel: BoinkVec3,

    /// Maximum steering angle of the front wheels in degrees.
    pub max_steer_angle: c_double,
}

/// Represents roll–pitch–yaw orientation in radians.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkEulerRPY {
    /// Roll angle in radians.
    pub roll: c_double,
    /// Pitch angle in radians.
    pub pitch: c_double,
    /// Yaw angle in radians.
    pub yaw: c_double,
}

/// Represents the full state of a car at a specific simulation instant.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkCarState {
    /// Unique car identifier.
    pub car_id: u64,

    /// World position of the car in meters.
    pub position: BoinkVec3,

    /// Orientation of the car in roll–pitch–yaw angles (radians).
    pub orientation: BoinkEulerRPY,

    /// Linear speed magnitude of the car in meters per second.
    pub speed: c_double,

    /// Engine speed in revolutions per minute.
    pub engine_rpm: c_double,

    /// Current gear value.
    ///
    /// - -1 – reverse
    /// -  0 – neutral
    /// -  1..8 – forward gears
    pub gear: c_int,

    /// Effective throttle actually applied by the physics engine in the range [0.0, 1.0].
    pub throttle_applied: c_double,

    /// Effective brake actually applied by the physics engine in the range [0.0, 1.0].
    pub brake_applied: c_double,

    /// Steering angles of the front wheels in radians.
    ///
    /// Index mapping:
    ///   [0] = front-left
    ///   [1] = front-right
    pub wheel_angles: [c_double; 2],

    /// Wheel angular speeds in revolutions per minute.
    ///
    /// Index mapping:
    ///   [0] = front-left
    ///   [1] = front-right
    ///   [2] = rear-left
    ///   [3] = rear-right
    pub wheel_speeds: [c_double; 4],
}

unsafe extern "C" {
    /// Retrieves the version of the Boink C API.
    ///
    /// Parameters:
    /// - `out_major` – pointer to receive the major version number.
    /// - `out_minor` – pointer to receive the minor version number.
    /// - `out_patch` – pointer to receive the patch version number.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - An error code on failure.
    pub fn boink_get_c_api_version(
        out_major: *mut c_uint,
        out_minor: *mut c_uint,
        out_patch: *mut c_uint,
    ) -> c_int;

    /// Retrieves the version of the Boink engine library.
    ///
    /// Parameters:
    /// - `out_major` – pointer to receive the major version number.
    /// - `out_minor` – pointer to receive the minor version number.
    /// - `out_patch` – pointer to receive the patch version number.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - An error code on failure.
    pub fn boink_get_engine_version(
        out_major: *mut c_uint,
        out_minor: *mut c_uint,
        out_patch: *mut c_uint,
    ) -> c_int;

    /// Initializes the Boink engine library.
    ///
    /// This function must be called before any other Boink API is used.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - An error code on failure.
    pub fn boink_init() -> c_int;

    /// Creates a new world instance.
    ///
    /// The car model is shared by all car entities in the world.
    ///
    /// Parameters:
    /// - `car_model` – pointer to a car model description. The pointer must
    ///   refer to a valid `BoinkCarModel` for the lifetime of the call.
    ///
    /// Returns:
    /// - A valid `BoinkHandle` on success.
    /// - Null on failure.
    pub fn boink_create_world(car_model: *const BoinkCarModel) -> BoinkHandle;

    /// Starts a simulation in the given world at the specified timepoint.
    ///
    /// The timepoint is expressed in seconds. The car model used in the world
    /// is the one provided during `boink_create_world`.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - An error code on failure.
    pub fn boink_begin_world(h: BoinkHandle, timepoint: c_double) -> c_int;

    /// Destroys a world instance created by `boink_create_world`.
    ///
    /// It is not required to despawn all cars before destroying the world.
    ///
    /// Parameters:
    /// - `h` – handle to the world to destroy. Passing null is allowed and has
    ///   no effect.
    pub fn boink_destroy_world(h: BoinkHandle);

    /// Advances the simulation by a fixed time step.
    ///
    /// Parameters:
    /// - `h` – handle to a valid world.
    /// - `dt_seconds` – time step in seconds.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - An error code on failure.
    pub fn boink_step(h: BoinkHandle, dt_seconds: c_double) -> c_int;

    /// Spawns a new car with a newly generated unique identifier.
    ///
    /// The engine owns the car and manages its lifetime until it is despawned
    /// or the world is destroyed.
    ///
    /// Parameters:
    /// - `h` – handle to a valid world.
    /// - `out_car_id` – non-null pointer that receives the new car identifier.
    ///
    /// Returns:
    /// - `BOINK_OK` on success and writes the identifier to `*out_car_id`.
    /// - An error code if the engine cannot allocate or generate the identifier
    ///   or if the arguments are invalid.
    pub fn boink_spawn_car(h: BoinkHandle, out_car_id: *mut u64) -> c_int;

    /// Removes a car with the specified identifier.
    ///
    /// Parameters:
    /// - `h` – handle to a valid world.
    /// - `car_id` – identifier of the car to despawn.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_NOT_FOUND` if the car does not exist.
    /// - Another error code for other failures.
    pub fn boink_despawn_car(h: BoinkHandle, car_id: u64) -> c_int;

    /// Sets the desired driver controls for the specified car.
    ///
    /// Parameters:
    /// - `h` – handle to a valid world.
    /// - `car_id` – identifier of the car to control.
    /// - `controls` – non-null pointer to the desired control inputs.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_INVALID_ARG` if `controls` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the car does not exist.
    /// - Another error code for other failures.
    pub fn boink_set_controls(h: BoinkHandle, car_id: u64, controls: *const BoinkControls)
    -> c_int;

    /// Reads the current state of the specified car.
    ///
    /// Parameters:
    /// - `h` – handle to a valid world.
    /// - `car_id` – identifier of the car whose state is requested.
    /// - `out_state` – non-null pointer that receives the car state.
    ///
    /// Returns:
    /// - `BOINK_OK` on success and writes the state to `*out_state`.
    /// - `BOINK_ERR_INVALID_ARG` if `out_state` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the car does not exist.
    /// - Another error code for other failures.
    pub fn boink_read_car_state(
        h: BoinkHandle,
        car_id: u64,
        out_state: *mut BoinkCarState,
    ) -> c_int;
}
