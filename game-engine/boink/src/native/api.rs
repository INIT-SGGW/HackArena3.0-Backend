//! Abstractions over optional native Boink symbols.
//!
//! `NativeApi` lazily loads `boink.dll`/`libboink` once and exposes the function
//! pointers we care about. Higher-level code can check whether a symbol exists
//! before invoking the corresponding functionality.

use std::sync::OnceLock;

use tracing::trace;

use crate::native::error::NativeLoadError;
use crate::native::loader::{
    LegacyStringFn, LegacyVersionFn, load_native_library, resolve_optional,
};

/// Lazily resolved optional symbols exposed by a potentially old native library.
pub struct NativeApi {
    get_c_api_version: Option<LegacyVersionFn>,
    get_engine_version: Option<LegacyVersionFn>,
    get_engine_profile: Option<LegacyStringFn>,
    get_last_error: Option<LegacyStringFn>,
}

impl NativeApi {
    /// Returns the shared [`NativeApi`] instance if the native library could be loaded.
    pub fn instance() -> Result<&'static NativeApi, NativeLoadError> {
        static INSTANCE: OnceLock<Result<NativeApi, NativeLoadError>> = OnceLock::new();
        match INSTANCE.get_or_init(NativeApi::load).as_ref() {
            Ok(api) => Ok(api),
            Err(err) => Err(err.clone()),
        }
    }

    /// Returns the function pointer for `boink_get_c_api_version`, when exported.
    #[must_use]
    pub fn boink_get_c_api_version(&self) -> Option<LegacyVersionFn> {
        self.get_c_api_version
    }

    /// Returns the function pointer for `boink_get_engine_version`, when exported.
    #[must_use]
    pub fn boink_get_engine_version(&self) -> Option<LegacyVersionFn> {
        self.get_engine_version
    }

    /// Returns the function pointer for `boink_get_engine_profile`, when exported.
    #[must_use]
    pub fn boink_get_engine_profile(&self) -> Option<LegacyStringFn> {
        self.get_engine_profile
    }

    /// Returns the function pointer for `boink_get_last_error`, when exported.
    #[must_use]
    pub fn boink_get_last_error(&self) -> Option<LegacyStringFn> {
        self.get_last_error
    }

    fn load() -> Result<NativeApi, NativeLoadError> {
        let lib = load_native_library()?;

        let get_c_api_version = resolve_optional(lib, b"boink_get_c_api_version\0");
        let get_engine_version = resolve_optional(lib, b"boink_get_engine_version\0");
        let get_engine_profile = resolve_optional(lib, b"boink_get_engine_profile\0");
        let get_last_error = resolve_optional(lib, b"boink_get_last_error\0");

        trace!(
            "Resolved legacy query methods: c_api_version={}, engine_version={}, engine_profile={}, last_error={}",
            get_c_api_version.is_some(),
            get_engine_version.is_some(),
            get_engine_profile.is_some(),
            get_last_error.is_some()
        );

        Ok(NativeApi {
            get_c_api_version,
            get_engine_version,
            get_engine_profile,
            get_last_error,
        })
    }
}
