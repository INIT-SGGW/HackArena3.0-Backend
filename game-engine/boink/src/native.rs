//! Native integration helpers for the Boink safe wrapper.
//!
//! This module groups functionality related to interacting with the native
//! shared library (version queries, symbol loading, etc.).

#[cfg(feature = "legacy-native-lib")]
#[allow(dead_code)] // Reserved for future dynamic symbol loading extensions.
pub(crate) mod api;
#[cfg(feature = "legacy-native-lib")]
#[allow(dead_code)] // Reserved for future dynamic symbol loading extensions.
pub(crate) mod error;
pub mod info;
#[cfg(feature = "legacy-native-lib")]
#[allow(dead_code)] // Reserved for future dynamic symbol loading extensions.
pub(crate) mod loader;
pub(crate) mod raw;
pub(crate) mod strings;
pub mod version;
