//! Version helpers for the Boink native engine.
//!
//! This module provides a small, ergonomic API for querying the runtime
//! versions reported by the loaded native library (via `boink-sys`) and
//! checking compatibility against the wrapper's expected C-API version.

use std::fmt;
#[cfg(feature = "legacy-native-lib")]
use std::sync::Once;

use boink_sys as sys;
use tracing::{info, warn};

use crate::error::{Error, Result};

#[cfg(feature = "legacy-native-lib")]
use super::api::NativeApi;
use super::info::query_engine_profile;

/// Semantic version triple reported by the Boink engine.
///
/// This type is used for both:
/// - the *Boink C API* version (ABI-facing contract),
/// - the *Boink engine library* version (implementation version).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version {
    /// Major version component.
    pub major: u32,
    /// Minor version component.
    pub minor: u32,
    /// Patch version component.
    pub patch: u32,
}

impl Version {
    /// Creates a new [`Version`] value.
    #[must_use]
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Runtime versions queried from the loaded Boink native library.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionInfo {
    /// Version of the Boink C API exposed by the loaded library.
    pub c_api: Version,
    /// Version of the loaded Boink engine library implementation.
    pub engine: Version,
}

/// The C-API version expected by this wrapper crate at compile time.
///
/// This is sourced from the generated FFI constants (mirroring the header
/// macros in `boink_c_api.h`).
pub const REQUIRED_C_API_VERSION: Version = Version::new(
    sys::BOINK_C_API_VERSION_MAJOR,
    sys::BOINK_C_API_VERSION_MINOR,
    sys::BOINK_C_API_VERSION_PATCH,
);

/// Minimal C-API version accepted in `legacy-native-lib` mode.
///
/// Legacy mode supports only native libraries exposing C API `>= 0.16.0`.
#[cfg(feature = "legacy-native-lib")]
pub const MIN_LEGACY_C_API_VERSION: Version = Version::new(0, 16, 0);

/// Queries the loaded native library for the Boink C-API and engine versions.
///
/// # Errors
///
/// Returns an error if the native library reports a non-`BOINK_OK` status code.
pub fn query_versions() -> Result<VersionInfo> {
    Ok(VersionInfo {
        c_api: query_c_api_version()?,
        engine: query_engine_version()?,
    })
}

/// Queries the loaded native library for the Boink C-API version.
#[cfg(not(feature = "legacy-native-lib"))]
pub fn query_c_api_version() -> Result<Version> {
    let mut major: u32 = 0;
    let mut minor: u32 = 0;
    let mut patch: u32 = 0;

    let code = unsafe { sys::boink_get_c_api_version(&mut major, &mut minor, &mut patch) };

    if code == sys::BOINK_OK {
        Ok(Version::new(major, minor, patch))
    } else {
        Err(Error::from_ffi_status(code, "boink_get_c_api_version"))
    }
}

/// Queries the loaded native library for the Boink C-API version.
#[cfg(feature = "legacy-native-lib")]
pub fn query_c_api_version() -> Result<Version> {
    let api = NativeApi::instance();
    query_dynamic_version(api.boink_get_c_api_version(), "boink_get_c_api_version")
}

/// Queries the loaded native library for the Boink engine library version.
#[cfg(not(feature = "legacy-native-lib"))]
pub fn query_engine_version() -> Result<Version> {
    let mut major: u32 = 0;
    let mut minor: u32 = 0;
    let mut patch: u32 = 0;

    let code = unsafe { sys::boink_get_engine_version(&mut major, &mut minor, &mut patch) };

    if code == sys::BOINK_OK {
        Ok(Version::new(major, minor, patch))
    } else {
        Err(Error::from_ffi_status(code, "boink_get_engine_version"))
    }
}

/// Queries the loaded native library for the Boink engine library version.
#[cfg(feature = "legacy-native-lib")]
pub fn query_engine_version() -> Result<Version> {
    let api = NativeApi::instance();
    query_dynamic_version(api.boink_get_engine_version(), "boink_get_engine_version")
}

/// Checks whether the loaded library's C-API version is compatible with this wrapper.
///
/// Compatibility rule (SemVer-style):
/// - `major` must match exactly,
/// - `minor` must be >= required `minor` when `major` matches,
/// - if `minor` is equal, `patch` must be >= required `patch`.
///
/// # Errors
///
/// Returns an error if the version cannot be queried or if it is incompatible.
///
/// # Notes
///
/// This function is intended to be called early (e.g., during service startup)
/// to fail fast when the wrong native library is deployed.
#[cfg(not(feature = "legacy-native-lib"))]
pub fn ensure_c_api_compatible() -> Result<()> {
    let required = REQUIRED_C_API_VERSION;
    match query_c_api_version() {
        Ok(actual) => {
            let engine_version = match query_engine_version() {
                Ok(engine) => engine.to_string(),
                Err(err) => {
                    warn!("Unable to query Boink engine version: {err}");
                    "unknown".to_string()
                }
            };

            info!(
                "Boink versions: c_api={}, engine={}, required={}",
                actual, engine_version, required
            );
            match query_engine_profile() {
                Ok(Some(profile)) => info!("Boink engine profile: {}", profile),
                Ok(None) => {}
                Err(err) => warn!("Failed to query Boink engine profile: {err}"),
            }

            if is_compatible(required, actual) {
                Ok(())
            } else {
                Err(Error::IncompatibleVersion { required, actual })
            }
        }
        Err(err) => {
            warn!("Unable to query Boink C API version from native library: {err}");
            Err(err)
        }
    }
}

#[cfg(feature = "legacy-native-lib")]
pub fn ensure_c_api_compatible() -> Result<()> {
    static LEGACY_WARN_ONCE: Once = Once::new();
    LEGACY_WARN_ONCE.call_once(|| {
        warn!("Legacy compatibility mode enabled; minimum supported C API is 0.16.0");
    });

    let c_api = match query_c_api_version() {
        Ok(version) => version,
        Err(err) => return Err(err),
    };
    let engine_version = query_engine_version()
        .map(|v| v.to_string())
        .unwrap_or_else(|err| {
            warn!("Unable to query Boink engine version: {err}");
            "unknown".to_string()
        });
    info!(
        "Boink versions: c_api={}, engine={}, legacy_min={}, required={}, mode=legacy",
        c_api, engine_version, MIN_LEGACY_C_API_VERSION, REQUIRED_C_API_VERSION
    );
    match query_engine_profile() {
        Ok(Some(profile)) => info!("Boink engine profile: {}", profile),
        Ok(None) => {}
        Err(err) => warn!("Failed to query Boink engine profile: {err}"),
    }
    if is_compatible(MIN_LEGACY_C_API_VERSION, c_api) {
        Ok(())
    } else {
        Err(Error::IncompatibleVersion {
            required: MIN_LEGACY_C_API_VERSION,
            actual: c_api,
        })
    }
}

#[cfg(feature = "legacy-native-lib")]
fn query_dynamic_version(
    func: unsafe extern "C" fn(*mut u32, *mut u32, *mut u32) -> i32,
    func_name: &'static str,
) -> Result<Version> {
    let mut major: u32 = 0;
    let mut minor: u32 = 0;
    let mut patch: u32 = 0;

    let code = unsafe { func(&mut major, &mut minor, &mut patch) };
    if code == sys::BOINK_OK {
        Ok(Version::new(major, minor, patch))
    } else {
        Err(Error::from_ffi_status(code, func_name))
    }
}

fn is_compatible(required: Version, actual: Version) -> bool {
    if actual.major != required.major {
        return false;
    }

    if actual.minor > required.minor {
        return true;
    }

    if actual.minor < required.minor {
        return false;
    }

    actual.patch >= required.patch
}
