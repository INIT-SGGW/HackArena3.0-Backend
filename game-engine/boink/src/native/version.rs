//! Version helpers for the Boink native engine.
//!
//! This module provides a small, ergonomic API for querying the runtime
//! versions reported by the loaded native library (via `boink-sys`) and
//! checking compatibility against the wrapper's expected C-API version.

use core::fmt;

#[cfg(feature = "legacy-native-lib")]
use std::sync::Once;

use boink_sys as sys;
use tracing::{info, warn};

use crate::error::{Error, Result};
#[cfg(feature = "legacy-native-lib")]
use super::api::NativeApi;

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
        Err(Error::from_code(code))
    }
}

/// Queries the loaded native library for the Boink C-API version.
#[cfg(feature = "legacy-native-lib")]
pub fn query_c_api_version() -> Result<Version> {
    match try_dynamic_version_query(b"boink_get_c_api_version\0")? {
        Some(version) => {
            info!(
                "Legacy native C API version: {} (min {})",
                version, REQUIRED_C_API_VERSION
            );
            Ok(version)
        }
        None => {
            static ASSUMED_C_API_WARN_ONCE: Once = Once::new();
            ASSUMED_C_API_WARN_ONCE.call_once(|| {
                warn!(
                    concat!(
                        "Unable to query legacy Boink C API version; assuming {} ",
                        "(min required {}). Missing version info may indicate newer APIs ",
                        "are unavailable."
                    ),
                    REQUIRED_C_API_VERSION, REQUIRED_C_API_VERSION
                );
            });
            Ok(REQUIRED_C_API_VERSION)
        }
    }
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
        Err(Error::from_code(code))
    }
}

/// Queries the loaded native library for the Boink engine library version.
#[cfg(feature = "legacy-native-lib")]
pub fn query_engine_version() -> Result<Version> {
    match try_dynamic_version_query(b"boink_get_engine_version\0")? {
        Some(version) => {
            info!(
                "Legacy native engine version: {} (min {})",
                version, REQUIRED_C_API_VERSION
            );
            Ok(version)
        }
        None => {
            let assumed = Version::new(0, 0, 0);
            warn!(
                "Unable to query legacy engine implementation version; falling back to {}",
                assumed
            );
            Ok(assumed)
        }
    }
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
            info!(
                "Loaded Boink C API version {} (min required {})",
                actual, required
            );

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
        warn!("Legacy compatibility mode enabled; native symbols will be resolved dynamically");
    });

    let _ = query_c_api_version()?;
    Ok(())
}

#[cfg(feature = "legacy-native-lib")]
fn try_dynamic_version_query(symbol: &[u8]) -> Result<Option<Version>> {
    let api = match NativeApi::instance() {
        Ok(api) => api,
        Err(_) => return Ok(None),
    };

    let func = match symbol {
        b"boink_get_c_api_version\0" => api.boink_get_c_api_version(),
        b"boink_get_engine_version\0" => api.boink_get_engine_version(),
        _ => None,
    };

    let func = match func {
        Some(func) => func,
        None => return Ok(None),
    };

    let mut major: u32 = 0;
    let mut minor: u32 = 0;
    let mut patch: u32 = 0;

    let code = unsafe { func(&mut major, &mut minor, &mut patch) };
    if code == sys::BOINK_OK {
        Ok(Some(Version::new(major, minor, patch)))
    } else {
        Err(Error::from_code(code))
    }
}

#[cfg(not(feature = "legacy-native-lib"))]
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
