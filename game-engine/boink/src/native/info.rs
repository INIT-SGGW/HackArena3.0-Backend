//! Helpers for querying engine metadata.

use crate::error::{Error, Result};

use super::strings::query_string;

/// Queries the native library for the engine build profile.
pub(crate) fn query_engine_profile() -> Result<Option<String>> {
    match query_string(boink_sys::boink_get_engine_profile) {
        Ok(value) => Ok(value),
        Err(code) => Err(Error::from_ffi_status(code, "boink_get_engine_profile")),
    }
}
