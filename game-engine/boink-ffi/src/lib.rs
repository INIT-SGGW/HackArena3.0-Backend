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

use libc::{c_char, c_double, c_float, c_int, c_uint, c_void};

pub const BOINK_C_API_VERSION_MAJOR: c_uint = 1;
pub const BOINK_C_API_VERSION_MINOR: c_uint = 0;
pub const BOINK_C_API_VERSION_PATCH: c_uint = 0;

/// Indicates successful operation.
pub const BOINK_OK: c_int = 0;

/// Indicates that no data is currently available for the request.
/// This is not considered an error.
pub const BOINK_NO_DATA: c_int = 1;

/// Indicates that the operation could not be completed because required conditions were not met.
pub const BOINK_CONDITION_NOT_MET: c_int = 2;

/// Indicates an invalid argument (for example a null pointer or an out-of-range value).
pub const BOINK_ERR_INVALID_ARG: c_int = -1;

/// Indicates that the output buffer was too small.
pub const BOINK_ERR_BUFFER_TOO_SMALL: c_int = -2;

/// Indicates that a requested object or identifier was not found.
pub const BOINK_ERR_NOT_FOUND: c_int = -3;

/// Indicates that the file format is not supported.
pub const BOINK_ERR_UNSUPPORTED_FORMAT: c_int = -4;

/// Indicates an input/output error (for example a file read/write failure).
pub const BOINK_ERR_IO: c_int = -5;

/// Indicates an internal engine error.
pub const BOINK_ERR_INTERNAL: c_int = -100;

/// Represents an opaque engine handle.
///
/// The pointer refers to an internal race or engine instance allocated
/// and owned by the native C or C++ side.
pub type BoinkHandle = *mut c_void;

/// Represents a real-valued numeric type.
///
/// This type is used for floating-point values.
/// Should match the btScalar type.
pub type Real = c_float;

/// Represents a 3D vector in world coordinates (meters).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkVec3 {
    /// X component in meters.
    pub x: Real,
    /// Y component in meters.
    pub y: Real,
    /// Z component in meters.
    pub z: Real,
}

/// Represents an opaque handle to a vehicle mesh resource.
pub type BoinkVehicleMeshHandle = *mut c_void;

/// Describes the geometric and steering properties of a vehicle model.
///
/// The vehicle model is shared by all vehicle entities in the race.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkVehicleModel {
    /// Handle to the mesh of the vehicle.
    pub mesh: BoinkVehicleMeshHandle,
    /// Position of the vehicle's center of mass in model space.
    pub center_of_mass: BoinkVec3,
    /// Radius of the vehicle wheels.
    pub wheel_radius: Real,
    /// Rest length of the suspension.
    pub suspension_rest_length: Real,
    /// Total mass of the vehicle.
    pub mass: Real,
    /// Maximum steering angle of the front wheels in degrees.
    pub max_steer_angle: Real,
}

/// Requested gear-shift operation for a single controls command.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BoinkGearShift {
    /// Do not request a gear shift.
    #[default]
    BOINK_GEAR_SHIFT_NONE = 0,
    /// Request shift by +1 gear.
    BOINK_GEAR_SHIFT_UPSHIFT = 1,
    /// Request shift by -1 gear.
    BOINK_GEAR_SHIFT_DOWNSHIFT = 2,
}

/// Represents normalized control inputs of a driver.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkControls {
    /// Throttle demand in the range [0.0, 1.0].
    pub throttle: Real,
    /// Brake demand in the range [0.0, 1.0].
    pub brake: Real,
    /// Brake balancer demand in the range [0.0, 1.0].
    pub brake_balancer: Real,
    /// Differential lock demand in the range [0.0, 1.0].
    pub differential_lock: Real,
    /// Normalized steering input in the range [-1.0, 1.0].
    ///
    /// Negative values correspond to steering left.
    /// Positive values correspond to steering right.
    pub steer: Real,
    /// Requested gear shift by one step.
    pub gear_shift: BoinkGearShift,
}

/// Represents controls accepted by drivetrain logic.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkAcceptedControls {
    /// Shift operation that was actually executed.
    ///
    /// Returns `BOINK_GEAR_SHIFT_NONE` when no shift was executed.
    pub accepted_shift: BoinkGearShift,
}

/// Represents weather parameters applied globally to the race simulation.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkWeather {
    /// Cloudiness in range [0.0, 1.0].
    pub cloudiness: Real,
    /// Ambient temperature in Celsius.
    pub temperature_c: Real,
    /// Rain intensity in range [0.0, 1.0].
    pub rain_intensity: Real,
}

/// Represents ghost mode settings applied globally to the race simulation.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkGhostModeSettings {
    /// Maximum speed threshold to enter ghost mode in meters per second.
    pub enter_speed_max_mps: Real,
    /// Minimum speed threshold to exit ghost mode in meters per second.
    pub exit_speed_min_mps: Real,
    /// Required time above enter threshold before ghost mode is enabled.
    pub enter_delay_ms: c_uint,
    /// Required time below exit threshold before ghost mode is disabled.
    pub exit_delay_ms: c_uint,
    /// Ghost mode remains enabled until this many laps are completed.
    pub until_completed_laps: c_uint,
    /// Required time after overlap ends before ghost mode may be disabled.
    pub vehicle_overlap_exit_delay_ms: c_uint,
}

/// High-level runtime phase of ghost mode for a single vehicle.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BoinkGhostModePhase {
    /// Ghost mode is disabled for this vehicle and collisions are enabled.
    #[default]
    BOINK_GHOST_MODE_PHASE_INACTIVE = 0,
    /// Ghost mode enter conditions are progressing; collisions are still enabled.
    BOINK_GHOST_MODE_PHASE_PENDING_ENTER = 1,
    /// Ghost mode is enabled for this vehicle and collisions are disabled.
    BOINK_GHOST_MODE_PHASE_ACTIVE = 2,
    /// Ghost mode is still active, but exit countdown is currently running.
    BOINK_GHOST_MODE_PHASE_PENDING_EXIT = 3,
}

/// Bitmask of active ghost-mode blockers.
pub type BoinkGhostModeBlockersMask = c_uint;

/// Active conditions that can keep a vehicle in ghost mode.
///
/// Values are bit flags intended to be OR-combined inside [`BoinkGhostModeBlockersMask`].
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoinkGhostModeBlocker {
    /// completed_laps is below GhostModeSettings.until_completed_laps.
    BOINK_GHOST_MODE_BLOCKER_LAPS_REQUIREMENT_NOT_MET = 1 << 0,
    /// Current speed is not above GhostModeSettings.exit_speed_min_mps.
    BOINK_GHOST_MODE_BLOCKER_EXIT_SPEED_NOT_MET = 1 << 1,
    /// Exit speed condition is met, but exit delay is still counting down.
    BOINK_GHOST_MODE_BLOCKER_EXIT_DELAY_RUNNING = 1 << 2,
    /// Vehicle overlap is currently present and prevents ghost mode exit.
    BOINK_GHOST_MODE_BLOCKER_VEHICLE_OVERLAP_ACTIVE = 1 << 3,
    /// Overlap is cleared, but no-overlap exit delay is still counting down.
    BOINK_GHOST_MODE_BLOCKER_OVERLAP_EXIT_DELAY_RUNNING = 1 << 4,
    /// Vehicle is currently in pit area.
    BOINK_GHOST_MODE_BLOCKER_IN_PIT = 1 << 5,
}

/// Runtime ghost mode state for a single vehicle.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkGhostModeRuntimeState {
    /// Authoritative collision flag for this vehicle at current tick.
    pub can_collide_now: bool,
    /// Current high-level ghost mode phase.
    pub phase: BoinkGhostModePhase,
    /// Bitmask of currently active blockers.
    ///
    /// Uses bitwise OR of [`BoinkGhostModeBlocker`] values.
    pub blockers_mask: BoinkGhostModeBlockersMask,
    /// Remaining time to complete ghost-mode enter countdown.
    pub enter_delay_remaining_ms: c_uint,
    /// Remaining time to complete ghost-mode exit countdown.
    pub exit_delay_remaining_ms: c_uint,
}

/// Available tyre types for the vehicle.
///
/// Values represent the compound currently in use.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BoinkTyreType {
    /// Soft tyre compound.
    #[default]
    BOINK_TYRE_TYPE_SOFT = 0,
    /// Normal tyre compound.
    BOINK_TYRE_TYPE_HARD = 1,
    /// Wet tyre compound.
    BOINK_TYRE_TYPE_WET = 2,
}

/// Active pitstop zones the vehicle can be in.
///
/// Values are intended to represent the current pitstop phase.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BoinkPitstopZone {
    /// Vehicle is not in any pitstop zone.
    #[default]
    BOINK_PITSTOP_ZONE_NONE = 0,
    /// Vehicle is in the pit entry zone.
    BOINK_PITSTOP_ZONE_ENTER = 1 << 0,
    /// Vehicle is in the pit repair zone.
    BOINK_PITSTOP_ZONE_FIX = 1 << 1,
    /// Vehicle is in the pit exit zone.
    BOINK_PITSTOP_ZONE_EXIT = 1 << 2,
}

/// Ground surface type used by side segments around the centerline.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BoinkGroundType {
    /// Standard asphalt/racing surface.
    #[default]
    BOINK_GROUND_TYPE_ASPHALT = 0,
    /// Grass surface.
    BOINK_GROUND_TYPE_GRASS = 1,
    /// Sand trap surface.
    BOINK_GROUND_TYPE_SAND = 2,
    /// Gravel trap surface.
    BOINK_GROUND_TYPE_GRAVEL = 3,
    /// Solid wall or barrier surface.
    BOINK_GROUND_TYPE_WALL = 4,
    /// Kerb/corner stone surface.
    BOINK_GROUND_TYPE_KERB = 5,
}

/// Width segment with associated ground type.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkGroundWidth {
    /// Segment width in meters.
    pub width: Real,
    /// Ground type for this side segment.
    pub r#type: BoinkGroundType,
}

/// Represents one static centerline sample of a race track.
///
/// Returned as part of `BoinkTrackData` by `boink_get_track_data`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkCenterlineSample {
    /// Arc-length position along the lap in meters.
    pub s_m: c_double,
    /// World-space centerline position.
    pub position: BoinkVec3,
    /// Track-forward unit vector.
    pub tangent: BoinkVec3,
    /// Track-local up unit vector.
    pub normal: BoinkVec3,
    /// Track-right unit vector.
    pub right: BoinkVec3,
    /// Drivable half-width to track-left from centerline, meters.
    pub left_width_m: Real,
    /// Drivable half-width to track-right from centerline, meters.
    pub right_width_m: Real,
    /// Drivable half-width to wall-left from centerline, meters.
    pub max_left_width_m: Real,
    /// Number of elements at `left_grounds`.
    pub left_grounds_count: c_uint,
    /// Pointer to `left_grounds_count` elements describing ground types and widths on the left side.
    ///
    /// Can be null only when `left_grounds_count == 0`.
    pub left_grounds: *mut BoinkGroundWidth,
    /// Drivable half-width to wall-right from centerline, meters.
    pub max_right_width_m: Real,
    /// Number of elements at `right_grounds`.
    pub right_grounds_count: c_uint,
    /// Pointer to `right_grounds_count` elements describing ground types and widths on the right side.
    ///
    /// Can be null only when `right_grounds_count == 0`.
    pub right_grounds: *mut BoinkGroundWidth,
    /// Signed centerline curvature [1/m].
    pub curvature_1pm: Real,
    /// Longitudinal slope angle in radians.
    pub grade_rad: Real,
    /// Crossfall/banking angle in radians around tangent.
    pub bank_rad: Real,
}

/// Represents static pitstop geometry for one lap.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkPitstopData {
    /// Number of elements at `enter_centerline_samples`.
    pub enter_centerline_sample_count: c_uint,
    /// Pointer to `enter_centerline_sample_count` elements.
    ///
    /// Can be null only when `enter_centerline_sample_count == 0`.
    pub enter_centerline_samples: *const BoinkCenterlineSample,
    /// Number of elements at `fix_centerline_samples`.
    pub fix_centerline_sample_count: c_uint,
    /// Pointer to `fix_centerline_sample_count` elements.
    ///
    /// Can be null only when `fix_centerline_sample_count == 0`.
    pub fix_centerline_samples: *const BoinkCenterlineSample,
    /// Number of elements at `exit_centerline_samples`.
    pub exit_centerline_sample_count: c_uint,
    /// Pointer to `exit_centerline_sample_count` elements.
    ///
    /// Can be null only when `exit_centerline_sample_count == 0`.
    pub exit_centerline_samples: *const BoinkCenterlineSample,
    /// Pitstop length along centerline in meters.
    pub length_m: Real,
}

/// Represents static track geometry for one lap.
///
/// The `map_id` and `centerline_samples` pointers are owned by the engine
/// and must not be freed or modified by the caller.
///
/// The pointers contained within `pitstop_data` are also owned by the engine
/// and must not be freed or modified by the caller.
///
/// The `left_grounds`/`right_grounds` pointers referenced by each centerline sample
/// are also owned by the engine and must not be freed or modified by the caller.
///
/// These pointers remain valid until `boink_destroy_race(h)` is called.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkTrackData {
    /// Null-terminated UTF-8 track identifier.
    pub map_id: *const c_char,
    /// Track geometry version.
    pub version: c_uint,
    /// Full lap length along centerline in meters.
    pub lap_length_m: c_double,
    /// Number of elements at `centerline_samples`.
    pub centerline_sample_count: c_uint,
    /// Pointer to `centerline_sample_count` elements.
    ///
    /// Can be null only when `centerline_sample_count == 0`.
    pub centerline_samples: *const BoinkCenterlineSample,
    /// Static pitstop geometry data for the track.
    pub pitstop_data: BoinkPitstopData,
}

/// Represents a quaternion rotation (x, y, z, w).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkQuaternion {
    /// X component of the vector part.
    pub x: Real,
    /// Y component of the vector part.
    pub y: Real,
    /// Z component of the vector part.
    pub z: Real,
    /// W component (scalar part).
    pub w: Real,
}

/// Represents the full state of a vehicle at a specific simulation instant.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkVehicleState {
    /// Unique vehicle identifier.
    pub vehicle_id: u64,
    /// World position of the vehicle's chassis in meters.
    pub chassis_position: BoinkVec3,
    /// Orientation of the vehicle as a quaternion (x, y, z, w).
    pub vehicle_orientation: BoinkQuaternion,
    /// Linear speed magnitude of the vehicle in meters per second.
    pub speed: Real,
    /// Engine speed in revolutions per minute.
    pub engine_rpm: Real,
    /// Current gear value.
    ///
    /// - -1 - reverse
    /// -  0 - neutral
    /// -  1..8 - forward gears
    pub gear: c_int,
    /// Effective throttle actually applied by the physics engine in the range [0.0, 1.0].
    pub throttle_applied: Real,
    /// Effective brake actually applied by the physics engine in the range [0.0, 1.0].
    pub brake_applied: Real,
    /// World-space positions of the vehicle wheels.
    ///
    /// Index mapping:
    ///   [0] = front-left
    ///   [1] = front-right
    ///   [2] = rear-left
    ///   [3] = rear-right
    pub wheel_position: [BoinkVec3; 4],
    /// Wheel angular speeds in radians per second.
    ///
    /// Index mapping:
    ///   [0] = front-left
    ///   [1] = front-right
    ///   [2] = rear-left
    ///   [3] = rear-right
    pub wheel_speeds: [Real; 4],
    /// Front-wheel steering orientation in radians.
    ///
    /// Index mapping:
    ///   [0] = front-left
    ///   [1] = front-right
    pub front_wheel_orientation_rad: [Real; 2],
    /// Current tyre health in the range [0.0, 1.0],
    /// where 1.0 represents a new tyre and 0.0 a fully worn tyre.
    ///
    /// Index mapping:
    ///   [0] = front-left
    ///   [1] = front-right
    ///   [2] = rear-left
    ///   [3] = rear-right
    pub tyre_health: [Real; 4],
    /// Current tyre temperature in degrees Celsius.
    ///
    /// Index mapping:
    ///   [0] = front-left
    ///   [1] = front-right
    ///   [2] = rear-left
    ///   [3] = rear-right
    pub tyre_temperature_celsius: [Real; 4],
    /// Current tyre slip vector length.
    /// - 0-1: the tyre maintains grip with the ground
    /// - >1: the tyre has lost traction
    ///
    /// Index mapping:
    ///   [0] = front-left
    ///   [1] = front-right
    ///   [2] = rear-left
    ///   [3] = rear-right
    pub tyre_slip: [Real; 4],
    /// Currently equipped tyre type.
    pub tyre_type: BoinkTyreType,
    /// Indicates whether all four wheels are in contact with the ground.
    pub are_all_wheels_on_ground: bool,
}

/// Represents race-progress metrics of a vehicle at a specific simulation instant.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BoinkVehicleRaceMetrics {
    /// Number of fully completed laps.
    pub completed_laps: c_uint,
    /// Arc-length progress within the current lap in meters.
    ///
    /// The value is expected to be in `[0, lap_length_m)`.
    pub lap_progress_m: Real,
    /// Elapsed time in the currently running lap in milliseconds.
    pub current_lap_time_ms: c_uint,
    /// True when `last_lap_time_ms` contains a valid value.
    pub has_last_lap_time: bool,
    /// Duration of the previously finished lap in milliseconds.
    ///
    /// Valid only when `has_last_lap_time == true`.
    pub last_lap_time_ms: c_uint,
}

unsafe extern "C" {
    /// Retrieves the version of the Boink C API.
    ///
    /// Parameters:
    /// - `out_major` - pointer to receive the major version number.
    /// - `out_minor` - pointer to receive the minor version number.
    /// - `out_patch` - pointer to receive the patch version number.
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
    /// - `out_major` - pointer to receive the major version number.
    /// - `out_minor` - pointer to receive the minor version number.
    /// - `out_patch` - pointer to receive the patch version number.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - An error code on failure.
    pub fn boink_get_engine_version(
        out_major: *mut c_uint,
        out_minor: *mut c_uint,
        out_patch: *mut c_uint,
    ) -> c_int;

    /// Retrieves the build profile string of the Boink engine library.
    ///
    /// Parameters:
    /// - `out_buf` - destination buffer for a null-terminated string.
    /// - `in_out_len` - in: buffer size in bytes; out: required size (incl. null).
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_BUFFER_TOO_SMALL` if the buffer is too small.
    /// - `BOINK_ERR_INVALID_ARG` on invalid pointers.
    pub fn boink_get_engine_profile(out_buf: *mut c_char, in_out_len: *mut c_uint) -> c_int;

    /// Retrieves a human-readable description of the last engine error.
    ///
    /// The error string is thread-local and is updated when a Boink API call fails.
    ///
    /// Parameters:
    /// - `out_buf` - destination buffer for a null-terminated string.
    /// - `in_out_len` - in: buffer size in bytes; out: required size (incl. null).
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_BUFFER_TOO_SMALL` if the buffer is too small.
    /// - `BOINK_ERR_INVALID_ARG` on invalid pointers.
    pub fn boink_get_last_error(out_buf: *mut c_char, in_out_len: *mut c_uint) -> c_int;

    /// Initializes the Boink engine library.
    ///
    /// This function must be called before any other Boink API is used.
    ///
    /// Parameters:
    /// - `debug_drawer_enable`:
    ///   Enables or disables the debug drawer used for visualizing.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - An error code on failure.
    pub fn boink_init(debug_drawer_enable: bool) -> c_int;

    /// Terminates the Boink engine library.
    ///
    /// This function releases all resources allocated by the library.
    /// After calling this function, no other Boink API functions
    /// may be used unless `boink_init` is called again.
    pub fn boink_terminate();

    /// Creates a new race instance using the specified track file.
    ///
    /// The returned handle represents an internal race object managed by the engine.
    /// The race must be destroyed with `boink_destroy_race` when no longer needed.
    ///
    /// Parameters:
    /// - `track_glb_filename`:
    ///   Path to the GLB file containing the track geometry and metadata.
    ///
    /// Returns:
    /// - A valid `BoinkHandle` on success.
    /// - Null on failure.
    pub fn boink_create_race(track_glb_filename: *const c_char) -> BoinkHandle;

    /// Destroys a race instance created by `boink_create_race`.
    ///
    /// It is not required to despawn all vehicles before destroying the race.
    ///
    /// Parameters:
    /// - `h` - handle to the race to destroy. Passing null is allowed and has
    ///   no effect.
    pub fn boink_destroy_race(h: BoinkHandle);

    /// Loads a vehicle mesh from a GLB model file.
    ///
    /// The created mesh can be shared by multiple vehicle instances and must
    /// be destroyed with `boink_destroy_vehicle_mesh` when no longer needed.
    /// The mesh handle must outlive all vehicles that reference it.
    ///
    /// Parameters:
    /// - `glb_model_filename`:
    ///   Path to the GLB file containing the vehicle mesh.
    /// - `out_mesh_handle`:
    ///   Pointer to receive the handle of the created mesh. Must not be null.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - An error code on failure.
    pub fn boink_create_vehicle_mesh(
        glb_model_filename: *const c_char,
        out_mesh_handle: *mut BoinkVehicleMeshHandle,
    ) -> c_int;

    /// Destroys a vehicle mesh created by `boink_create_vehicle_mesh`.
    ///
    /// Passing a null handle has no effect. All vehicles using this mesh
    /// must be destroyed before destroying the mesh.
    ///
    /// Parameters:
    /// - `handle`:
    ///   Handle of the vehicle mesh to destroy.
    pub fn boink_destroy_vehicle_mesh(handle: BoinkVehicleMeshHandle);

    /// Advances the race by a fixed time step.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `dt_seconds` - requested time step in seconds.
    /// - `out_simulated_dt_seconds` - non-null pointer receiving the actual
    ///   simulated step in seconds.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - An error code on failure.
    pub fn boink_step_race(
        h: BoinkHandle,
        dt_seconds: Real,
        out_simulated_dt_seconds: *mut Real,
    ) -> c_int;

    /// Retrieves the duration of the race.
    ///
    /// Parameters:
    /// - h:
    ///   Handle to a valid race.
    /// - out_dur:
    ///   Pointer to a `Real` variable that will receive the elapsed time
    ///   in seconds. Must not be null.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - An error code on failure.
    pub fn boink_get_race_duration(h: BoinkHandle, out_dur: *mut Real) -> c_int;

    /// Retrieves static track geometry for the specified race.
    ///
    /// The returned pointers inside `BoinkTrackData` are owned by the engine.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `out_track_data` - non-null output pointer.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_INVALID_ARG` if `out_track_data` is null.
    /// - Another error code for other failures.
    pub fn boink_get_track_data(h: BoinkHandle, out_track_data: *mut BoinkTrackData) -> c_int;

    /// Reads current pitstop zones and wheel count in pitstop for the specified vehicle.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle whose pitstop zone is requested.
    /// - `out_zone` - non-null pointer that receives the current pitstop zone.
    /// - `out_wheels_num` - non-null pointer that receives number of wheels in pitstop zones.
    ///
    /// Returns:
    /// - `BOINK_OK` on success and writes the zone to `*out_zone`.
    /// - `BOINK_ERR_INVALID_ARG` if `out_zone` or `out_wheels_num` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    ///
    /// Notes:
    /// - `*out_wheels_num` is expected to be in range `0..=4`.
    pub fn boink_get_vehicle_pitstop_zone(
        h: BoinkHandle,
        vehicle_id: u64,
        out_zone: *mut BoinkPitstopZone,
        out_wheels_num: *mut c_int,
    ) -> c_int;

    /// Updates the debug drawer for the current frame.
    ///
    /// This function should be called once per frame if debug visualization
    /// is enabled. It updates internal debug objects and draws them to the screen.
    ///
    /// Has no effect if the debug drawer is not enabled.
    pub fn boink_update_debug();

    /// Retrieves the current debug time.
    ///
    /// Returns the elapsed time in seconds tracked by the debug drawer.
    /// If the debug drawer is not enabled, this function returns 0.
    ///
    /// Returns:
    /// - Elapsed time in seconds.
    pub fn boink_get_time_debug() -> Real;

    /// Checks whether the debug visualization window should close.
    ///
    /// Returns true if either the debug drawer is not enabled or if the user
    /// has requested the debug window to close.
    ///
    /// Returns:
    /// - `true` if the debug window should close or debug is disabled.
    /// - `false` if the debug window is open and should continue running.
    pub fn boink_should_close_debug() -> bool;

    /// Spawns a new vehicle with a newly generated unique identifier.
    ///
    /// The engine owns the vehicle and manages its lifetime until it is despawned
    /// or the race is destroyed.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_model` - pointer to a `BoinkVehicleModel` that defines the
    ///   properties of the vehicle.
    /// - `out_vehicle_id` - non-null pointer that receives the new vehicle identifier.
    ///
    /// Returns:
    /// - `BOINK_OK` on success and writes the identifier to `*out_vehicle_id`.
    /// - An error code if the engine cannot allocate or generate the identifier
    ///   or if the arguments are invalid.
    pub fn boink_spawn_vehicle(
        h: BoinkHandle,
        vehicle_model: *const BoinkVehicleModel,
        out_vehicle_id: *mut u64,
    ) -> c_int;

    /// Removes a vehicle with the specified identifier.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle to despawn.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_despawn_vehicle(h: BoinkHandle, vehicle_id: u64) -> c_int;

    /// Sets the desired driver controls for the specified vehicle.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle to control.
    /// - `controls` - non-null pointer to the desired control inputs.
    /// - `out_accepted_controls` - non-null pointer that receives accepted controls.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_INVALID_ARG` if `controls` or `out_accepted_controls` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_set_controls(
        h: BoinkHandle,
        vehicle_id: u64,
        controls: *const BoinkControls,
        out_accepted_controls: *mut BoinkAcceptedControls,
    ) -> c_int;

    /// Sets the world-space position of a vehicle.
    ///
    /// This immediately updates the specified vehicle's position in the simulation.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle to move.
    /// - `position` - non-null pointer to the new position vector.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_INVALID_ARG` if `position` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_set_vehicle_position(
        h: BoinkHandle,
        vehicle_id: u64,
        position: *const BoinkVec3,
    ) -> c_int;

    /// Sets the world-space position of a vehicle to a point before the given point.
    ///
    /// This immediately updates the specified vehicle's position in the simulation.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle to move.
    /// - `point` - non-null pointer to the point vector.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_INVALID_ARG` if `point` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_set_vehicle_before_point(
        h: BoinkHandle,
        vehicle_id: u64,
        point: *const BoinkVec3,
    ) -> c_int;

    /// Sets the world-space position of a vehicle to a point before the finish line.
    ///
    /// This immediately updates the specified vehicle's position in the simulation.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle to move.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_set_vehicle_before_finish_line(h: BoinkHandle, vehicle_id: u64) -> c_int;

    /// Sets the world-space position of a vehicle to a random point.
    ///
    /// This immediately updates the specified vehicle's position in the simulation.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle to move.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_set_vehicle_random_pos(h: BoinkHandle, vehicle_id: u64) -> c_int;

    /// Sets the world-space position of a vehicle to the closest point on track.
    ///
    /// This immediately updates the specified vehicle's position in the simulation.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle to move.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_set_vehicle_back_to_track(h: BoinkHandle, vehicle_id: u64) -> c_int;

    /// Sets the world-space position of a vehicle to the pitstop fix zone.
    ///
    /// This immediately updates the specified vehicle's position in the simulation.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle to move.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_set_vehicle_to_pitstop(h: BoinkHandle, vehicle_id: u64) -> c_int;

    /// Sets the tyre compound/type for a specific vehicle.
    ///
    /// This immediately updates the vehicle's tyres in the simulation.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle.
    /// - `tyre_type` - tyre compound/type to apply.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - `BOINK_CONDITION_NOT_MET` when the vehicle was not in the pitstop fix zone or was not stationary.
    /// - Another error code for other failures.
    pub fn boink_set_vehicle_tyre_type(
        h: BoinkHandle,
        vehicle_id: u64,
        tyre_type: BoinkTyreType,
    ) -> c_int;

    /// Forces the tyre compound/type for a specific vehicle.
    ///
    /// This immediately updates the vehicle's tyres in the simulation.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle.
    /// - `tyre_type` - tyre compound/type to apply.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - `BOINK_CONDITION_NOT_MET` when the vehicle was not in the pitstop fix zone or was not stationary.
    /// - Another error code for other failures.
    pub fn boink_force_set_vehicle_tyre_type(
        h: BoinkHandle,
        vehicle_id: u64,
        tyre_type: BoinkTyreType,
    ) -> c_int;

    /// Sets the world-space position of a vehicle at a selected starting position.
    ///
    /// This immediately updates the specified vehicle's position in the simulation.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle to move.
    /// - `position_index` - index of a starting position from 1 to max starting positions.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_INVALID_ARG` if `h` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist or `position_index` does not exist.
    /// - Another error code for other failures.
    pub fn boink_set_vehicle_at_start_pos(
        h: BoinkHandle,
        vehicle_id: u64,
        position_index: u64,
    ) -> c_int;

    /// Retrieves the number of available starting positions.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `out_number_pos` - non-null pointer that receives number of available starting positions.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_INVALID_ARG` if `h` or `out_number_pos` is null.
    /// - Another error code for other failures.
    pub fn boink_get_number_of_start_pos(h: BoinkHandle, out_number_pos: *mut u64) -> c_int;

    /// Sets the world-space orientation of a vehicle.
    ///
    /// This immediately updates the specified vehicle's orientation in the simulation.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle to rotate.
    /// - `orientation` - non-null pointer to the new orientation quaternion.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_INVALID_ARG` if `orientation` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_set_vehicle_orientation(
        h: BoinkHandle,
        vehicle_id: u64,
        orientation: *const BoinkQuaternion,
    ) -> c_int;

    /// Reads the current state of the specified vehicle.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle whose state is requested.
    /// - `out_state` - non-null pointer that receives the vehicle state.
    ///
    /// Returns:
    /// - `BOINK_OK` on success and writes the state to `*out_state`.
    /// - `BOINK_ERR_INVALID_ARG` if `out_state` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_read_vehicle_state(
        h: BoinkHandle,
        vehicle_id: u64,
        out_state: *mut BoinkVehicleState,
    ) -> c_int;

    /// Retrieves the dimensions of a specific vehicle.
    ///
    /// Parameters:
    /// - `handle` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle.
    /// - `out_width` - non-null pointer receiving the vehicle width.
    /// - `out_depth` - non-null pointer receiving the vehicle depth (length).
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - An error code on failure.
    pub fn boink_get_vehicle_dimensions(
        handle: BoinkHandle,
        vehicle_id: u64,
        out_width: *mut Real,
        out_depth: *mut Real,
    ) -> c_int;

    /// Reads race-progress metrics for the specified vehicle.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle whose race metrics are requested.
    /// - `out_metrics` - non-null pointer that receives race metrics.
    ///
    /// Returns:
    /// - `BOINK_OK` on success and writes metrics to `*out_metrics`.
    /// - `BOINK_ERR_INVALID_ARG` if `out_metrics` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_read_vehicle_race_metrics(
        h: BoinkHandle,
        vehicle_id: u64,
        out_metrics: *mut BoinkVehicleRaceMetrics,
    ) -> c_int;

    /// Returns the lap number and lap time of the vehicle's personal best lap.
    ///
    /// Output parameters are written only when `BOINK_OK` is returned.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle whose race metrics are requested.
    /// - `out_lap` - non-null pointer that receives lap number.
    /// - `out_lap_time_ms` - non-null pointer that receives the best lap time in milliseconds.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_NO_DATA` if the vehicle exists but has no personal best lap yet.
    /// - `BOINK_ERR_INVALID_ARG` if `out_lap` or `out_lap_time_ms` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - An error code on failure.
    pub fn boink_get_vehicle_personal_best_lap(
        h: BoinkHandle,
        vehicle_id: u64,
        out_lap: *mut c_uint,
        out_lap_time_ms: *mut c_uint,
    ) -> c_int;

    /// Returns the vehicle ID, lap number, and lap time of the best lap in the race.
    ///
    /// Output parameters are written only when `BOINK_OK` is returned.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `out_vehicle_id` - non-null pointer that receives the vehicle identifier.
    /// - `out_lap` - non-null pointer that receives the lap number.
    /// - `out_lap_time_ms` - non-null pointer that receives the best lap time in milliseconds.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_NO_DATA` if no laps have been completed yet.
    /// - `BOINK_ERR_INVALID_ARG` if `out_vehicle_id`, `out_lap`, or `out_lap_time_ms` is null.
    /// - An error code on failure.
    pub fn boink_get_best_lap(
        h: BoinkHandle,
        out_vehicle_id: *mut u64,
        out_lap: *mut c_uint,
        out_lap_time_ms: *mut c_uint,
    ) -> c_int;

    /// Reads runtime ghost mode state for the specified vehicle.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `vehicle_id` - identifier of the vehicle whose ghost-mode state is requested.
    /// - `out_state` - non-null pointer that receives the ghost-mode runtime state.
    ///
    /// Returns:
    /// - `BOINK_OK` on success and writes the state to `*out_state`.
    /// - `BOINK_ERR_INVALID_ARG` if `out_state` is null.
    /// - `BOINK_ERR_NOT_FOUND` if the vehicle does not exist.
    /// - Another error code for other failures.
    pub fn boink_read_vehicle_ghost_mode_state(
        h: BoinkHandle,
        vehicle_id: u64,
        out_state: *mut BoinkGhostModeRuntimeState,
    ) -> c_int;

    /// Sets global weather parameters used by the simulation engine.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `weather` - non-null pointer to weather parameters.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_INVALID_ARG` if `weather` is null.
    /// - Another error code for other failures.
    pub fn boink_set_weather(h: BoinkHandle, weather: *const BoinkWeather) -> c_int;

    /// Sets global ghost mode settings used by the simulation engine.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    /// - `settings` - non-null pointer to ghost mode settings.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - `BOINK_ERR_INVALID_ARG` if `settings` is null.
    /// - Another error code for other failures.
    pub fn boink_set_ghost_mode_settings(
        h: BoinkHandle,
        settings: *const BoinkGhostModeSettings,
    ) -> c_int;

    /// Disables ghost mode for the race.
    ///
    /// Parameters:
    /// - `h` - handle to a valid race.
    ///
    /// Returns:
    /// - `BOINK_OK` on success.
    /// - Another error code for other failures.
    pub fn boink_disable_ghost_mode(h: BoinkHandle) -> c_int;
}
