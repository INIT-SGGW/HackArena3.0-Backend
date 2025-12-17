//! Builders and configuration helpers for constructing [`Engine`] instances.
//!
//! This module keeps the ergonomics and validation logic needed to describe
//! cars at a higher level before delegating the actual FFI work to
//! [`crate::engine::Engine`].

use crate::engine::Engine;
use crate::engine::engine::CarModelConfig;
use crate::error::Result;
use crate::model::math::Vec3;

use tracing::instrument;

/// Fluent helper that prepares high-level simulation configuration before
/// handing off to [`Engine`].
pub struct EngineBuilder {
    /// Validated car model configuration shared with the engine.
    car_model: CarModelConfig,
    /// Initial simulation timestamp for the world (seconds).
    start_time_seconds: f64,
}

impl EngineBuilder {
    /// Creates a new [`EngineBuilder`] from a fully specified car model.
    ///
    /// The positions are expressed in meters in the car's local coordinate
    /// system and must match the expectations of the underlying physics model.
    ///
    /// # Parameters
    ///
    /// * `front_left_wheel` - Position of the front-left wheel relative to the
    ///   car origin (meters).
    /// * `front_right_wheel` - Position of the front-right wheel relative to
    ///   the car origin (meters).
    /// * `rear_left_wheel` - Position of the rear-left wheel relative to the
    ///   car origin (meters).
    /// * `rear_right_wheel` - Position of the rear-right wheel relative to the
    ///   car origin (meters).
    /// * `max_steer_angle_deg` - Maximum steering angle of the front wheels in
    ///   degrees.
    ///
    /// The initial simulation time is set to `0.0` seconds. It can be
    /// customised with [`EngineBuilder::with_start_time_seconds`].
    pub fn new(
        front_left_wheel: Vec3,
        front_right_wheel: Vec3,
        rear_left_wheel: Vec3,
        rear_right_wheel: Vec3,
        max_steer_angle_deg: f64,
    ) -> Self {
        let car_model = CarModelConfig {
            front_left_wheel,
            front_right_wheel,
            rear_left_wheel,
            rear_right_wheel,
            max_steer_angle_deg,
        };

        Self {
            car_model,
            start_time_seconds: 0.0,
        }
    }

    /// Sets the initial simulation time (in seconds) for the world.
    ///
    /// If not called, the default start time is `0.0` seconds.
    ///
    /// # Parameters
    ///
    /// * `time_seconds` - Simulation time at which the world should begin.
    ///
    /// # Returns
    ///
    /// The builder instance for fluent configuration.
    pub fn with_start_time_seconds(mut self, time_seconds: f64) -> Self {
        self.start_time_seconds = time_seconds;
        self
    }

    /// Replaces the entire car model configuration.
    ///
    /// This is a convenience method if the caller wants to construct
    /// the configuration externally (for example, from a track file or
    /// configuration service) and then hand it over to the builder.
    pub fn with_car_model(
        mut self,
        front_left_wheel: Vec3,
        front_right_wheel: Vec3,
        rear_left_wheel: Vec3,
        rear_right_wheel: Vec3,
        max_steer_angle_deg: f64,
    ) -> Self {
        self.car_model = CarModelConfig {
            front_left_wheel,
            front_right_wheel,
            rear_left_wheel,
            rear_right_wheel,
            max_steer_angle_deg,
        };
        self
    }

    /// Consumes the builder and constructs a new [`Engine`] instance.
    ///
    /// This delegates the actual world creation and start sequence to
    /// [`Engine::new`], ensuring a single code path performs the FFI calls.
    ///
    /// # Errors
    ///
    /// Forwarded from [`Engine::new`].
    #[instrument(skip(self))]
    pub fn build(self) -> Result<Engine> {
        Engine::new(self.car_model, self.start_time_seconds)
    }
}
