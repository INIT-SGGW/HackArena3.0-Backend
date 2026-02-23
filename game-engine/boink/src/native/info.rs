//! Helpers for querying optional engine metadata.

use crate::error::{Error, Result};

use super::macros::native_optional_string_query;

/// Queries the native library for the engine build profile.
pub(crate) fn query_engine_profile() -> Result<Option<String>> {
    match native_optional_string_query!(
        boink_get_engine_profile,
        boink_sys::boink_get_engine_profile
    ) {
        Ok(value) => Ok(value),
        Err(code) => Err(Error::from_ffi_status(code, "boink_get_engine_profile")),
    }
}
