//! Basic math helpers shared across the high-level API.

use boink_sys as sys;

/// 3D vector in world coordinates (meters).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Vec3 {
    /// X component in meters.
    pub x: f64,
    /// Y component in meters.
    pub y: f64,
    /// Z component in meters.
    pub z: f64,
}

impl From<sys::BoinkVec3> for Vec3 {
    fn from(v: sys::BoinkVec3) -> Self {
        Self {
            x: v.x,
            y: v.y,
            z: v.z,
        }
    }
}

impl From<Vec3> for sys::BoinkVec3 {
    fn from(v: Vec3) -> Self {
        sys::BoinkVec3 {
            x: v.x,
            y: v.y,
            z: v.z,
        }
    }
}

/// Quaternion representing orientation in 3D space.
///
/// Expected to be normalized (unit quaternion).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Quaternion {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub w: f64,
}

impl Quaternion {
    /// Identity rotation (no rotation).
    pub const IDENTITY: Self = Self {
        x: 0.0,
        y: 0.0,
        z: 0.0,
        w: 1.0,
    };
}

impl From<sys::BoinkQuaternion> for Quaternion {
    fn from(o: sys::BoinkQuaternion) -> Self {
        Self {
            x: o.x,
            y: o.y,
            z: o.z,
            w: o.w,
        }
    }
}

impl From<Quaternion> for sys::BoinkQuaternion {
    fn from(o: Quaternion) -> Self {
        sys::BoinkQuaternion {
            x: o.x,
            y: o.y,
            z: o.z,
            w: o.w,
        }
    }
}
