//! Builders and configuration helpers for constructing [`Engine`] instances.
//!
//! This module keeps the ergonomics and validation logic needed to describe
//! vehicles at a higher level before delegating the actual FFI work to
//! [`crate::engine::Engine`].

use tracing::instrument;

use crate::engine::Engine;
use crate::engine::core::VehicleModelConfig;
use crate::error::Result;
use std::path::{Path, PathBuf};

/// Fluent helper that prepares high-level simulation configuration before
/// handing off to [`Engine`].
pub struct EngineBuilder {
    /// Track GLB filename used to initialize the race.
    track_glb_filename: PathBuf,
    /// Validated vehicle model configuration shared with the engine.
    vehicle_model: VehicleModelConfig,
    /// Enables the debug drawer in the native engine.
    debug_drawer_enabled: bool,
}

impl EngineBuilder {
    /// Creates a new [`EngineBuilder`] from a fully specified vehicle model.
    ///
    /// The positions are expressed in meters in the vehicle's local coordinate
    /// system and must match the expectations of the underlying physics model.
    ///
    /// # Parameters
    ///
    /// * `track_glb_filename` - Path to the GLB track file.
    /// * `vehicle_model` - Vehicle model configuration shared by spawned vehicles.
    pub fn new<P: AsRef<Path>>(track_glb_filename: P, vehicle_model: VehicleModelConfig) -> Self {
        Self {
            track_glb_filename: track_glb_filename.as_ref().to_path_buf(),
            vehicle_model,
            debug_drawer_enabled: false,
        }
    }

    /// Replaces the track GLB file path.
    pub fn with_track_glb<P: AsRef<Path>>(mut self, track_glb_filename: P) -> Self {
        self.track_glb_filename = track_glb_filename.as_ref().to_path_buf();
        self
    }

    /// Replaces the vehicle model configuration.
    pub fn with_vehicle_model(mut self, vehicle_model: VehicleModelConfig) -> Self {
        self.vehicle_model = vehicle_model;
        self
    }

    /// Enables or disables the native debug drawer.
    pub fn with_debug_drawer(mut self, enabled: bool) -> Self {
        self.debug_drawer_enabled = enabled;
        self
    }

    /// Consumes the builder and constructs a new [`Engine`] instance.
    ///
    /// This delegates the actual race creation to [`Engine::new`], ensuring a
    /// single code path performs the FFI calls.
    ///
    /// # Errors
    ///
    /// Forwarded from [`Engine::new`].
    #[instrument(skip(self))]
    pub fn build(self) -> Result<Engine> {
        Engine::new(
            self.track_glb_filename,
            self.vehicle_model,
            self.debug_drawer_enabled,
        )
    }
}
