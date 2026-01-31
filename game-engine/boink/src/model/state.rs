//! Vehicle state exposed by the safe wrapper.

use boink_sys as sys;

use crate::{
    error::{Error, Result},
    model::math::{Quaternion, Vec3},
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
    /// Wheel angular speeds in RPM: [FL, FR, RL, RR].
    pub wheel_speeds: [f32; 4],
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
        })
    }
}
