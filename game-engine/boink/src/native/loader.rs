//! Low-level loader for the native Boink dynamic library.
//!
//! This module handles locating `boink.dll`/`libboink` (optionally using the
//! `BOINK_NATIVE_LIB_DIR` override) and exposes helpers to resolve individual symbols.

use std::{env, path::PathBuf, sync::OnceLock};

use libloading::{Library, library_filename};
use tracing::debug;

use super::error::NativeLoadError;

/// Legacy counterpart of the exported version-querying functions.
pub type LegacyVersionFn = unsafe extern "C" fn(*mut u32, *mut u32, *mut u32) -> i32;
/// Legacy counterpart of the exported string-querying functions.
pub type LegacyStringFn = unsafe extern "C" fn(*mut std::os::raw::c_char, *mut u32) -> i32;
/// Legacy counterpart of weather-setting function.
pub type LegacySetWeatherFn =
    unsafe extern "C" fn(boink_sys::BoinkHandle, *const boink_sys::BoinkWeather) -> i32;
/// Legacy counterpart of track-data query function.
pub type LegacyGetTrackDataFn =
    unsafe extern "C" fn(boink_sys::BoinkHandle, *mut boink_sys::BoinkTrackData) -> i32;

/// Loads the Boink native library once and returns a reference to it.
pub fn load_native_library() -> Result<&'static Library, NativeLoadError> {
    static LIBRARY: OnceLock<Result<&'static Library, NativeLoadError>> = OnceLock::new();
    match LIBRARY.get_or_init(|| {
        let result = load_library_from_env()
            .or_else(load_library_from_default_path)
            .expect("native library loader must yield a result");

        match result {
            Ok(lib) => {
                // Leak the handle so the library stays loaded for the remainder of the process.
                // libloading requires the `Library` to outlive any function pointers derived from it.
                Ok(Box::leak(Box::new(lib)) as &'static Library)
            }
            Err(err) => Err(err),
        }
    }) {
        Ok(lib) => Ok(*lib),
        Err(err) => Err(err.clone()),
    }
}

/// Resolves a symbol from the loaded native library, returning `None` when missing.
pub fn resolve_optional<T>(lib: &'static Library, symbol: &[u8]) -> Option<T>
where
    T: Copy,
{
    unsafe {
        match lib.get::<T>(symbol) {
            Ok(sym) => Some(*sym),
            Err(err) => {
                debug!(
                    "Legacy native library does not export {}: {}",
                    core::str::from_utf8(symbol)
                        .unwrap_or("<??>")
                        .trim_end_matches('\0'),
                    err
                );
                None
            }
        }
    }
}

fn load_library_from_env() -> Option<Result<Library, NativeLoadError>> {
    load_library_from_env_var("BOINK_NATIVE_LIB_DIR")
}

fn load_library_from_env_var(var: &'static str) -> Option<Result<Library, NativeLoadError>> {
    let dir = env::var_os(var)
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())?;

    let mut path = dir;
    path.push(library_filename("boink"));

    Some(match unsafe { Library::new(&path) } {
        Ok(lib) => Ok(lib),
        Err(err) => Err(NativeLoadError::EnvPathLoadFailed {
            var,
            path,
            source: std::sync::Arc::new(err),
        }),
    })
}

fn load_library_from_default_path() -> Option<Result<Library, NativeLoadError>> {
    let filename = library_filename("boink");
    Some(match unsafe { Library::new(&filename) } {
        Ok(lib) => Ok(lib),
        Err(err) => {
            debug!("Native library not found at default path: {:?}", filename);
            Err(NativeLoadError::LibraryNotFound {
                source: std::sync::Arc::new(err),
            })
        }
    })
}
