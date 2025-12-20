//! Native integration helpers for the Boink safe wrapper.
//!
//! This module groups functionality related to interacting with the native
//! shared library (version queries, optional symbol handling, etc.).

#[cfg(feature = "legacy-native-lib")]
pub(crate) mod api;
#[cfg(feature = "legacy-native-lib")]
pub(crate) mod error;
#[cfg(feature = "legacy-native-lib")]
pub(crate) mod loader;
pub mod version;
