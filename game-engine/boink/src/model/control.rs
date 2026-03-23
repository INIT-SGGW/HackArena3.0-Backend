//! Driver control inputs applied to cars.

use boink_sys as sys;

use crate::error::{Error, Result};

/// Requested gear-shift operation for a single controls command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GearShift {
    /// Do not request a gear shift.
    None,
    /// Request shift by +1 gear.
    Upshift,
    /// Request shift by -1 gear.
    Downshift,
}

impl GearShift {
    /// Converts to the FFI representation.
    pub(crate) fn to_ffi(self) -> sys::BoinkGearShift {
        match self {
            GearShift::None => sys::BoinkGearShift::BOINK_GEAR_SHIFT_NONE,
            GearShift::Upshift => sys::BoinkGearShift::BOINK_GEAR_SHIFT_UPSHIFT,
            GearShift::Downshift => sys::BoinkGearShift::BOINK_GEAR_SHIFT_DOWNSHIFT,
        }
    }

    /// Converts from the FFI representation.
    pub(crate) fn from_ffi(value: sys::BoinkGearShift) -> Result<Self> {
        Ok(match value {
            sys::BoinkGearShift::BOINK_GEAR_SHIFT_NONE => GearShift::None,
            sys::BoinkGearShift::BOINK_GEAR_SHIFT_UPSHIFT => GearShift::Upshift,
            sys::BoinkGearShift::BOINK_GEAR_SHIFT_DOWNSHIFT => GearShift::Downshift,
        })
    }
}

/// Driver controls normalized to engine-specific ranges.
///
/// - `throttle` in `[0.0, 1.0]`
/// - `brake` in `[0.0, 1.0]`
/// - `brake_balancer` in `[0.0, 1.0]`
/// - `differential_lock` in `[0.0, 1.0]`
/// - `steer` in `[-1.0, 1.0]`
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Controls {
    /// Throttle demand (0 = idle, 1 = full power).
    pub throttle: f32,
    /// Brake demand (0 = no braking, 1 = full brakes).
    pub brake: f32,
    /// Brake balancer demand.
    pub brake_balancer: f32,
    /// Differential lock demand.
    pub differential_lock: f32,
    /// Steering input (negative = left, positive = right).
    pub steer: f32,
    /// Requested gear shift operation.
    pub gear_shift: GearShift,
}

/// Controls operation accepted by drivetrain logic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptedControls {
    /// Shift operation that was actually executed.
    pub accepted_shift: GearShift,
}

impl Controls {
    /// Creates clamped controls from raw values.
    pub fn new(
        throttle: f32,
        brake: f32,
        brake_balancer: f32,
        differential_lock: f32,
        steer: f32,
        gear_shift: GearShift,
    ) -> Self {
        Self {
            throttle: throttle.clamp(0.0, 1.0),
            brake: brake.clamp(0.0, 1.0),
            brake_balancer: brake_balancer.clamp(0.0, 1.0),
            differential_lock: differential_lock.clamp(0.0, 1.0),
            steer: steer.clamp(-1.0, 1.0),
            gear_shift,
        }
    }

    /// Converts the controls to the FFI representation.
    pub(crate) fn as_ffi(&self) -> sys::BoinkControls {
        sys::BoinkControls {
            throttle: self.throttle,
            brake: self.brake,
            brake_balancer: self.brake_balancer,
            differential_lock: self.differential_lock,
            steer: self.steer,
            gear_shift: self.gear_shift.to_ffi(),
        }
    }
}

impl TryFrom<sys::BoinkAcceptedControls> for AcceptedControls {
    type Error = Error;

    fn try_from(raw: sys::BoinkAcceptedControls) -> Result<Self> {
        Ok(Self {
            accepted_shift: GearShift::from_ffi(raw.accepted_shift)?,
        })
    }
}
