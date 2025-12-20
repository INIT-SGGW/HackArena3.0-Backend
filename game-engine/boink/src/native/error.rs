//! Error types produced while loading the native Boink library.

use std::{error::Error as StdError, fmt, path::PathBuf, sync::Arc};

/// Errors returned by the dynamic native loader in legacy mode.
#[derive(Debug, Clone)]
pub enum NativeLoadError {
    /// Failed to load the library from an explicit environment override.
    EnvPathLoadFailed {
        var: &'static str,
        path: PathBuf,
        source: Arc<libloading::Error>,
    },
    /// Failed to load the library from the default search path.
    LibraryNotFound {
        source: Arc<libloading::Error>,
    },
}

impl fmt::Display for NativeLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EnvPathLoadFailed { var, path, source } => write!(
                f,
                "failed to load native library from {} ({}): {}",
                var,
                path.display(),
                source
            ),
            Self::LibraryNotFound { source } => {
                write!(f, "native library not found: {}", source)
            }
        }
    }
}

impl StdError for NativeLoadError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::EnvPathLoadFailed { source, .. } => Some(source.as_ref()),
            Self::LibraryNotFound { source } => Some(source.as_ref()),
        }
    }
}
