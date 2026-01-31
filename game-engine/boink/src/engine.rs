//! High-level engine API.
//!
//! This module exposes the primary [`Engine`] type as well as the
//! [`EngineBuilder`] used to construct configured engine instances.
//!
//! The engine module is intentionally small and focused on lifecycle
//! management and high-level operations. Domain models (car state,
//! controls, etc.) live in [`crate::model`], while versioning and
//! compatibility checks live in [`crate::version`].

mod builder;
mod core;

pub use builder::EngineBuilder;
pub use core::{Engine, VehicleMesh, VehicleModelConfig};
