//! Vehicle state exposed by the safe wrapper.

use boink_sys as sys;

use crate::{
    error::{Error, Result},
    model::{
        ghost::GhostModeRuntimeState,
        math::{Quaternion, Vec3},
    },
};

/// Transmission gear mapping shared with the native engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gear {
    /// Reverse gear.
    Reverse,
    /// Neutral gear.
    Neutral,
    /// Forward gear (1..=8).
    Forward(u8),
}

impl Gear {
    /// Converts from the native `c_int` representation.
    pub(crate) fn from_c(value: i32) -> Result<Self> {
        match value {
            -1 => Ok(Gear::Reverse),
            0 => Ok(Gear::Neutral),
            1..=8 => Ok(Gear::Forward(value as u8)),
            _ => Err(Error::InvalidArg),
        }
    }

    /// Converts to the native representation.
    #[allow(dead_code)]
    pub(crate) fn to_c(self) -> i32 {
        match self {
            Gear::Reverse => -1,
            Gear::Neutral => 0,
            Gear::Forward(n) => n as i32,
        }
    }
}

/// Tyre compound currently equipped on the vehicle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TyreType {
    /// Soft compound tyre.
    Soft,
    /// Hard compound tyre.
    Hard,
    /// Wet compound tyre.
    Wet,
}

impl TryFrom<sys::BoinkTyreType> for TyreType {
    type Error = Error;

    fn try_from(value: sys::BoinkTyreType) -> Result<Self> {
        match value {
            sys::BoinkTyreType::BOINK_TYRE_TYPE_SOFT => Ok(Self::Soft),
            sys::BoinkTyreType::BOINK_TYRE_TYPE_HARD => Ok(Self::Hard),
            sys::BoinkTyreType::BOINK_TYRE_TYPE_WET => Ok(Self::Wet),
        }
    }
}

impl TyreType {
    /// Converts to the FFI representation.
    #[must_use]
    pub(crate) fn to_ffi(self) -> sys::BoinkTyreType {
        match self {
            Self::Soft => sys::BoinkTyreType::BOINK_TYRE_TYPE_SOFT,
            Self::Hard => sys::BoinkTyreType::BOINK_TYRE_TYPE_HARD,
            Self::Wet => sys::BoinkTyreType::BOINK_TYRE_TYPE_WET,
        }
    }
}

/// State of a vehicle at a single simulation instant.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VehicleState {
    /// Unique vehicle identifier.
    pub vehicle_id: u64,
    /// World position of the chassis in meters.
    pub chassis_position: Vec3,
    /// Orientation as a quaternion.
    pub vehicle_orientation: Quaternion,
    /// Linear speed magnitude in m/s.
    pub speed: f32,
    /// Engine speed in RPM.
    pub engine_rpm: f32,
    /// Current transmission gear.
    pub gear: Gear,
    /// Effective throttle actually applied `[0,1]`.
    pub throttle_applied: f32,
    /// Effective brake actually applied `[0,1]`.
    pub brake_applied: f32,
    /// World-space wheel positions: [FL, FR, RL, RR].
    pub wheel_position: [Vec3; 4],
    /// Wheel angular speeds in radians per second: [FL, FR, RL, RR].
    pub wheel_speeds: [f32; 4],
    /// Front-wheel steering orientation in radians: [FL, FR].
    pub front_wheel_orientation_rad: [f32; 2],
    /// Current tyre health in range `[0.0, 1.0]`: [FL, FR, RL, RR].
    pub tyre_health: [f32; 4],
    /// Current tyre temperature in Celsius: [FL, FR, RL, RR].
    pub tyre_temperature_celsius: [f32; 4],
    /// Current tyre slip vector length: [FL, FR, RL, RR].
    ///
    /// - `0..=1`: tyre maintains grip with the ground
    /// - `>1`: tyre has lost traction
    pub tyre_slip: [f32; 4],
    /// Currently equipped tyre compound.
    pub tyre_type: TyreType,
    /// True when all four wheels are on ground.
    pub are_all_wheels_on_ground: bool,
    /// Runtime ghost mode state for this vehicle.
    pub ghost_mode_runtime: GhostModeRuntimeState,
    /// Runtime pitstop-zone state for this vehicle.
    pub pitstop_state: VehiclePitstopState,
}

impl TryFrom<sys::BoinkVehicleState> for VehicleState {
    type Error = Error;

    fn try_from(raw: sys::BoinkVehicleState) -> Result<Self> {
        Ok(Self {
            vehicle_id: raw.vehicle_id,
            chassis_position: raw.chassis_position.into(),
            vehicle_orientation: raw.vehicle_orientation.into(),
            speed: raw.speed,
            engine_rpm: raw.engine_rpm,
            gear: Gear::from_c(raw.gear)?,
            throttle_applied: raw.throttle_applied,
            brake_applied: raw.brake_applied,
            wheel_position: raw.wheel_position.map(Into::into),
            wheel_speeds: raw.wheel_speeds,
            front_wheel_orientation_rad: raw.front_wheel_orientation_rad,
            tyre_health: raw.tyre_health,
            tyre_temperature_celsius: raw.tyre_temperature_celsius,
            tyre_slip: raw.tyre_slip,
            tyre_type: TyreType::try_from(raw.tyre_type)?,
            are_all_wheels_on_ground: raw.are_all_wheels_on_ground,
            ghost_mode_runtime: GhostModeRuntimeState::default(),
            pitstop_state: VehiclePitstopState::default(),
        })
    }
}

/// High-level pitstop zone marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum PitstopZone {
    /// Vehicle is not in any pitstop zone.
    None = 0,
    /// Vehicle is in the pit entry zone.
    Enter = 1 << 0,
    /// Vehicle is in the pit repair zone.
    Fix = 1 << 1,
    /// Vehicle is in the pit exit zone.
    Exit = 1 << 2,
}

/// Bitmask of currently active pitstop zones.
pub type PitstopZoneMask = u32;

/// Runtime pitstop-zone state for a single vehicle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct VehiclePitstopState {
    /// Bitmask of active pitstop zones.
    pub zone_mask: PitstopZoneMask,
    /// Number of wheels currently inside pitstop zones.
    pub wheels_in_pitstop: u8,
}

impl VehiclePitstopState {
    /// Returns true if the given pitstop zone bit is active.
    #[must_use]
    pub fn has_zone(self, zone: PitstopZone) -> bool {
        (self.zone_mask & (zone as PitstopZoneMask)) != 0
    }

    /// Returns true when at least one pitstop-zone bit is active.
    #[must_use]
    pub fn is_in_any_zone(self) -> bool {
        self.zone_mask != 0
    }

    pub(crate) fn try_from_ffi(zone_mask: u32, wheels_num: i32) -> Result<Self> {
        if wheels_num < 0 {
            return Err(Error::Internal(format!(
                "invalid pitstop wheels count: {wheels_num}"
            )));
        }
        let wheels_in_pitstop = wheels_num.min(4) as u8;

        Ok(Self {
            zone_mask,
            wheels_in_pitstop,
        })
    }
}

/// Race-progress metrics of a vehicle at a single simulation instant.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct VehicleRaceMetrics {
    /// Number of fully completed laps.
    pub completed_laps: u32,
    /// Arc-length progress within current lap, in meters.
    pub lap_progress_m: f32,
    /// Elapsed time in the currently running lap, in milliseconds.
    pub current_lap_time_ms: u32,
    /// Previously completed lap time, if available.
    pub last_lap_time_ms: Option<u32>,
}

impl From<sys::BoinkVehicleRaceMetrics> for VehicleRaceMetrics {
    fn from(raw: sys::BoinkVehicleRaceMetrics) -> Self {
        Self {
            completed_laps: raw.completed_laps,
            lap_progress_m: raw.lap_progress_m,
            current_lap_time_ms: raw.current_lap_time_ms,
            last_lap_time_ms: raw.has_last_lap_time.then_some(raw.last_lap_time_ms),
        }
    }
}

/// Best lap data for a specific vehicle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VehicleBestLap {
    /// Lap number in which the best time was achieved.
    pub lap: u32,
    /// Best lap duration in milliseconds.
    pub lap_time_ms: u32,
}

/// Best lap data in the whole race.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaceBestLap {
    /// Vehicle identifier that set the best lap.
    pub vehicle_id: u64,
    /// Lap number in which the best time was achieved.
    pub lap: u32,
    /// Best lap duration in milliseconds.
    pub lap_time_ms: u32,
}
