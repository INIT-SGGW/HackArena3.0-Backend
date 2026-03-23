//! Raw string queries for the native engine.
//!
//! This module stays minimal to avoid re-entrancy when `Error::from_ffi_status`
//! tries to log the last native error.

use super::strings::query_string;

/// Queries `boink_get_last_error` without mapping the error code.
///
/// Returns the raw native status code on failure to avoid re-entrancy in error logging.
pub(crate) fn query_last_error_raw() -> Result<Option<String>, i32> {
    query_string(boink_sys::boink_get_last_error)
}
