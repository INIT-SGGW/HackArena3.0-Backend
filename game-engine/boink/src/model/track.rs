//! Static track geometry exposed by the high-level wrapper.

use std::{ffi::CStr, slice};

use boink_sys as sys;

use crate::{
    error::{Error, Result},
    model::math::Vec3,
};

/// Ground surface type used by side segments around the centerline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroundType {
    Asphalt,
    Grass,
    Sand,
    Gravel,
    Wall,
    Kerb,
}

impl From<sys::BoinkGroundType> for GroundType {
    fn from(value: sys::BoinkGroundType) -> Self {
        match value {
            sys::BoinkGroundType::BOINK_GROUND_TYPE_ASPHALT => Self::Asphalt,
            sys::BoinkGroundType::BOINK_GROUND_TYPE_GRASS => Self::Grass,
            sys::BoinkGroundType::BOINK_GROUND_TYPE_SAND => Self::Sand,
            sys::BoinkGroundType::BOINK_GROUND_TYPE_GRAVEL => Self::Gravel,
            sys::BoinkGroundType::BOINK_GROUND_TYPE_WALL => Self::Wall,
            sys::BoinkGroundType::BOINK_GROUND_TYPE_KERB => Self::Kerb,
        }
    }
}

/// Width segment with associated ground type.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GroundWidth {
    /// Segment width in meters.
    pub width: f32,
    /// Ground type for this side segment.
    pub ground_type: GroundType,
}

impl From<sys::BoinkGroundWidth> for GroundWidth {
    fn from(raw: sys::BoinkGroundWidth) -> Self {
        Self {
            width: raw.width,
            ground_type: raw.r#type.into(),
        }
    }
}

/// One static centerline sample in local track frame.
#[derive(Clone, Debug, PartialEq)]
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
    /// Drivable half-width to wall-left from centerline in meters.
    pub max_left_width_m: f32,
    /// Ground segments on track-left side.
    pub left_grounds: Vec<GroundWidth>,
    /// Drivable half-width to wall-right from centerline in meters.
    pub max_right_width_m: f32,
    /// Ground segments on track-right side.
    pub right_grounds: Vec<GroundWidth>,
    /// Signed centerline curvature in 1/m.
    pub curvature_1pm: f32,
    /// Longitudinal slope angle in radians.
    pub grade_rad: f32,
    /// Crossfall/banking angle in radians around tangent.
    pub bank_rad: f32,
}

impl CenterlineSample {
    unsafe fn try_from_ffi(raw: sys::BoinkCenterlineSample) -> Result<Self> {
        Ok(Self {
            s_m: raw.s_m,
            position: raw.position.into(),
            tangent: raw.tangent.into(),
            normal: raw.normal.into(),
            right: raw.right.into(),
            left_width_m: raw.left_width_m,
            right_width_m: raw.right_width_m,
            max_left_width_m: raw.max_left_width_m,
            left_grounds: unsafe {
                collect_ground_widths(
                    raw.left_grounds.cast_const(),
                    raw.left_grounds_count,
                    "centerline_samples.left_grounds",
                )?
            },
            max_right_width_m: raw.max_right_width_m,
            right_grounds: unsafe {
                collect_ground_widths(
                    raw.right_grounds.cast_const(),
                    raw.right_grounds_count,
                    "centerline_samples.right_grounds",
                )?
            },
            curvature_1pm: raw.curvature_1pm,
            grade_rad: raw.grade_rad,
            bank_rad: raw.bank_rad,
        })
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
    /// Static pitstop geometry for one lap.
    pub pitstop_data: PitstopData,
}

/// Static pitstop geometry for one lap.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct PitstopData {
    /// Ordered centerline samples for pit entry zone.
    pub enter_centerline_samples: Vec<CenterlineSample>,
    /// Ordered centerline samples for pit repair zone.
    pub fix_centerline_samples: Vec<CenterlineSample>,
    /// Ordered centerline samples for pit exit zone.
    pub exit_centerline_samples: Vec<CenterlineSample>,
    /// Pitstop length along centerline in meters.
    pub length_m: f32,
}

impl TrackData {
    pub(crate) unsafe fn try_from_ffi(raw: sys::BoinkTrackData) -> Result<Self> {
        if raw.map_id.is_null() {
            return Err(Error::Internal(
                "boink_get_track_data returned null map_id".to_string(),
            ));
        }

        let map_id_cstr = unsafe { CStr::from_ptr(raw.map_id) };
        let map_id_bytes = map_id_cstr.to_bytes();
        let map_id = match std::str::from_utf8(map_id_bytes) {
            Ok(value) => value.to_string(),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    byte_len = map_id_bytes.len(),
                    "boink_get_track_data returned non-UTF8 map_id; using lossy decode"
                );
                String::from_utf8_lossy(map_id_bytes).into_owned()
            }
        };

        let centerline_samples = unsafe {
            collect_centerline_samples(
                raw.centerline_samples,
                raw.centerline_sample_count,
                "centerline_samples",
            )?
        };
        let pitstop_data = {
            let raw_pitstop = raw.pitstop_data;
            PitstopData {
                enter_centerline_samples: unsafe {
                    collect_centerline_samples(
                        raw_pitstop.enter_centerline_samples,
                        raw_pitstop.enter_centerline_sample_count,
                        "pitstop_data.enter_centerline_samples",
                    )?
                },
                fix_centerline_samples: unsafe {
                    collect_centerline_samples(
                        raw_pitstop.fix_centerline_samples,
                        raw_pitstop.fix_centerline_sample_count,
                        "pitstop_data.fix_centerline_samples",
                    )?
                },
                exit_centerline_samples: unsafe {
                    collect_centerline_samples(
                        raw_pitstop.exit_centerline_samples,
                        raw_pitstop.exit_centerline_sample_count,
                        "pitstop_data.exit_centerline_samples",
                    )?
                },
                length_m: raw_pitstop.length_m,
            }
        };

        Ok(Self {
            map_id,
            version: raw.version,
            lap_length_m: raw.lap_length_m,
            centerline_samples,
            pitstop_data,
        })
    }
}

unsafe fn collect_centerline_samples(
    ptr: *const sys::BoinkCenterlineSample,
    count: u32,
    field_name: &'static str,
) -> Result<Vec<CenterlineSample>> {
    let count = count as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    if ptr.is_null() {
        return Err(Error::Internal(format!(
            "boink_get_track_data returned null {field_name}"
        )));
    }

    let raw_samples = unsafe { slice::from_raw_parts(ptr, count) };
    raw_samples
        .iter()
        .copied()
        .map(|raw| unsafe { CenterlineSample::try_from_ffi(raw) })
        .collect()
}

unsafe fn collect_ground_widths(
    ptr: *const sys::BoinkGroundWidth,
    count: u32,
    field_name: &'static str,
) -> Result<Vec<GroundWidth>> {
    let count = count as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    if ptr.is_null() {
        return Err(Error::Internal(format!(
            "boink_get_track_data returned null {field_name}"
        )));
    }

    let raw_grounds = unsafe { slice::from_raw_parts(ptr, count) };
    Ok(raw_grounds.iter().copied().map(GroundWidth::from).collect())
}
