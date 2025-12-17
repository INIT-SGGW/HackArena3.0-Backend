//! Driver control inputs applied to cars.

use boink_sys as sys;

/// Driver controls normalized to engine-specific ranges.
///
/// - `throttle` in `[0.0, 1.0]`
/// - `brake` in `[0.0, 1.0]`
/// - `steer` in `[-1.0, 1.0]`
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Controls {
    /// Throttle demand (0 = idle, 1 = full power).
    pub throttle: f64,
    /// Brake demand (0 = no braking, 1 = full brakes).
    pub brake: f64,
    /// Steering input (negative = left, positive = right).
    pub steer: f64,
}

impl Controls {
    /// Creates clamped controls from raw values.
    pub fn new(throttle: f64, brake: f64, steer: f64) -> Self {
        Self {
            throttle: throttle.clamp(0.0, 1.0),
            brake: brake.clamp(0.0, 1.0),
            steer: steer.clamp(-1.0, 1.0),
        }
    }

    /// Converts the controls to the FFI representation.
    pub(crate) fn as_ffi(&self) -> sys::BoinkControls {
        sys::BoinkControls {
            throttle: self.throttle,
            brake: self.brake,
            steer: self.steer,
        }
    }
}
