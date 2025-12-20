//! Error handling surface for the Boink safe wrapper.
//!
//! Defines the `Error` enum mapping native status codes to descriptive variants.

use boink_sys as sys;
use thiserror::Error;

use crate::version::Version;

/// Convenient alias for results returned by this crate.
pub type Result<T, E = Error> = core::result::Result<T, E>;

/// High-level error type produced by the Boink wrapper.
#[derive(Debug, Clone, Error)]
pub enum Error {
    /// The native function returned a null handle where a valid one was expected.
    #[error("native call {0} returned a null handle")]
    NullHandle(&'static str),

    /// The provided car model configuration failed validation.
    #[error("invalid car model: {0}")]
    InvalidCarModel(String),

    /// An argument passed to the native layer was invalid.
    #[error("invalid argument")]
    InvalidArg,

    /// A caller-provided buffer was too small for the native output.
    #[error("buffer too small")]
    BufferTooSmall,

    /// A requested resource or identifier could not be found.
    #[error("resource not found")]
    NotFound,

    /// The native engine reported an internal error or the wrapper hit an unexpected condition.
    #[error("internal error: {0}")]
    Internal(String),

    /// A native function returned a non-success status code together with its name.
    #[error("{func} returned native error code {code}")]
    FfiStatus { code: i32, func: &'static str },

    /// The loaded native library is incompatible with the required C-API version.
    #[error("incompatible native library version")]
    IncompatibleVersion {
        /// Required C-API version by this wrapper crate.
        required: Version,
        /// Actual C-API version reported by the native library.
        actual: Version,
    },

    /// Any other native status code not mapped to a dedicated variant.
    #[error("native error code {0}")]
    Native(i32),
}

impl Error {
    /// Converts a raw native status code into an [`Error`].
    ///
    /// # Panics
    ///
    /// This function is intended to be called only for non-success codes. Passing
    /// `BOINK_OK` is considered a logic error and will trigger a debug assertion.
    pub(crate) fn from_code(code: i32) -> Self {
        debug_assert_ne!(
            code,
            sys::BOINK_OK,
            "from_code must not be called with success status"
        );

        match code {
            x if x == sys::BOINK_ERR_INVALID_ARG => Self::InvalidArg,
            x if x == sys::BOINK_ERR_BUFFER_TOO_SMALL => Self::BufferTooSmall,
            x if x == sys::BOINK_ERR_NOT_FOUND => Self::NotFound,
            x if x == sys::BOINK_ERR_INTERNAL => {
                Self::Internal("native engine reported an internal error".to_string())
            }
            other => Self::Native(other),
        }
    }

    /// Converts a raw native status code into an [`Error`], annotating it with
    /// the function name that returned it.
    ///
    /// # Panics
    ///
    /// This function is intended to be called only for non-success codes. Passing
    /// `BOINK_OK` is considered a logic error and will trigger a debug assertion.
    pub(crate) fn from_ffi_status(code: i32, func: &'static str) -> Self {
        debug_assert_ne!(
            code,
            sys::BOINK_OK,
            "from_ffi_status must not be called with success status"
        );

        match code {
            x if x == sys::BOINK_ERR_INVALID_ARG => Self::InvalidArg,
            x if x == sys::BOINK_ERR_BUFFER_TOO_SMALL => Self::BufferTooSmall,
            x if x == sys::BOINK_ERR_NOT_FOUND => Self::NotFound,
            x if x == sys::BOINK_ERR_INTERNAL => {
                Self::Internal(format!("{func} reported an internal error"))
            }
            other => Self::FfiStatus { code: other, func },
        }
    }
}
