//! Static track geometry exposed by the high-level wrapper.

use std::{ffi::CStr, slice};

use boink_sys as sys;

use crate::{
    error::{Error, Result},
    model::math::Vec3,
};

/// One static centerline sample in local track frame.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CenterlineSample {
    /// Arc-length position along the lap in meters.
    pub s_m: f64,
    /// World-space centerline position.
    pub position: Vec3,
    /// Track-forward unit vector.
    pub tangent: Vec3,
    /// Track-local up unit vector.
    pub normal: Vec3,
    /// Track-right unit vector.
    pub right: Vec3,
    /// Drivable half-width to track-left from centerline in meters.
    pub left_width_m: f32,
    /// Drivable half-width to track-right from centerline in meters.
    pub right_width_m: f32,
    /// Signed centerline curvature in 1/m.
    pub curvature_1pm: f32,
    /// Longitudinal slope angle in radians.
    pub grade_rad: f32,
    /// Crossfall/banking angle in radians around tangent.
    pub bank_rad: f32,
}

impl From<sys::BoinkCenterlineSample> for CenterlineSample {
    fn from(raw: sys::BoinkCenterlineSample) -> Self {
        Self {
            s_m: raw.s_m,
            position: raw.position.into(),
            tangent: raw.tangent.into(),
            normal: raw.normal.into(),
            right: raw.right.into(),
            left_width_m: raw.left_width_m,
            right_width_m: raw.right_width_m,
            curvature_1pm: raw.curvature_1pm,
            grade_rad: raw.grade_rad,
            bank_rad: raw.bank_rad,
        }
    }
}

/// Static track geometry for one lap.
#[derive(Clone, Debug, PartialEq)]
pub struct TrackData {
    /// Track identifier.
    pub map_id: String,
    /// Track geometry version.
    pub version: u32,
    /// Full lap length along centerline in meters.
    pub lap_length_m: f64,
    /// Ordered centerline samples for one lap.
    pub centerline_samples: Vec<CenterlineSample>,
}

impl TrackData {
    pub(crate) unsafe fn try_from_ffi(raw: sys::BoinkTrackData) -> Result<Self> {
        if raw.map_id.is_null() {
            return Err(Error::Internal(
                "boink_get_track_data returned null map_id".to_string(),
            ));
        }

        let count = raw.centerline_sample_count as usize;
        if count > 0 && raw.centerline_samples.is_null() {
            return Err(Error::Internal(
                "boink_get_track_data returned null centerline_samples".to_string(),
            ));
        }

        let map_id = unsafe { CStr::from_ptr(raw.map_id) }
            .to_str()
            .map_err(|err| Error::Internal(format!("boink_get_track_data invalid map_id: {err}")))?
            .to_string();

        let centerline_samples = if count == 0 {
            Vec::new()
        } else {
            let raw_samples = unsafe { slice::from_raw_parts(raw.centerline_samples, count) };
            raw_samples
                .iter()
                .copied()
                .map(CenterlineSample::from)
                .collect()
        };

        Ok(Self {
            map_id,
            version: raw.version,
            lap_length_m: raw.lap_length_m,
            centerline_samples,
        })
    }
}
