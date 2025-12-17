//! Car state exposed by the safe wrapper.

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

/// State of a car at a single simulation instant.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CarState {
    /// Unique car identifier.
    pub car_id: u64,
    /// World position in meters.
    pub position: Vec3,
    /// Orientation as a quaternion.
    pub orientation: Quaternion,
    /// Linear speed magnitude in m/s.
    pub speed: f64,
    /// Engine speed in RPM.
    pub engine_rpm: f64,
    /// Current transmission gear.
    pub gear: Gear,
    /// Effective throttle actually applied `[0,1]`.
    pub throttle_applied: f64,
    /// Effective brake actually applied `[0,1]`.
    pub brake_applied: f64,
    /// Steering angles of the front wheels (radians): [front-left, front-right].
    pub wheel_angles: [f64; 2],
    /// Wheel angular speeds in RPM: [FL, FR, RL, RR].
    pub wheel_speeds: [f64; 4],
}

impl TryFrom<sys::BoinkCarState> for CarState {
    type Error = Error;

    fn try_from(raw: sys::BoinkCarState) -> Result<Self> {
        Ok(Self {
            car_id: raw.car_id,
            position: raw.position.into(),
            orientation: raw.orientation.into(),
            speed: raw.speed,
            engine_rpm: raw.engine_rpm,
            gear: Gear::from_c(raw.gear)?,
            throttle_applied: raw.throttle_applied,
            brake_applied: raw.brake_applied,
            wheel_angles: raw.wheel_angles,
            wheel_speeds: raw.wheel_speeds,
        })
    }
}
