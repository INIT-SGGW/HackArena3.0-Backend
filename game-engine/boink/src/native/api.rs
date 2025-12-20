//! Abstractions over optional native Boink symbols.
//!
//! `NativeApi` lazily loads `boink.dll`/`libboink` once and exposes the function
//! pointers we care about. Higher-level code can check whether a symbol exists
//! before invoking the corresponding functionality.

use std::sync::OnceLock;

use tracing::trace;

use crate::native::error::NativeLoadError;
use crate::native::loader::{LegacyVersionFn, load_native_library, resolve_optional};

/// Lazily resolved optional symbols exposed by a potentially old native library.
pub struct NativeApi {
    get_c_api_version: Option<LegacyVersionFn>,
    get_engine_version: Option<LegacyVersionFn>,
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

    fn load() -> Result<NativeApi, NativeLoadError> {
        let lib = load_native_library()?;

        let get_c_api_version = resolve_optional(lib, b"boink_get_c_api_version\0");
        let get_engine_version = resolve_optional(lib, b"boink_get_engine_version\0");

        trace!(
            "Resolved legacy version query methods: c_api_version={}, engine_version={}",
            get_c_api_version.is_some(),
            get_engine_version.is_some()
        );

        Ok(NativeApi {
            get_c_api_version,
            get_engine_version,
        })
    }
}
