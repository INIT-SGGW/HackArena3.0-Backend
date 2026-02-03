//! Native integration helpers for the Boink safe wrapper.
//!
//! This module groups functionality related to interacting with the native
//! shared library (version queries, optional symbol handling, etc.).

#[cfg(feature = "legacy-native-lib")]
pub(crate) mod api;
#[cfg(feature = "legacy-native-lib")]
pub(crate) mod error;
pub mod info;
#[cfg(feature = "legacy-native-lib")]
pub(crate) mod loader;
pub(crate) mod macros;
pub(crate) mod raw;
pub(crate) mod strings;
pub mod version;
