//! Weather parameters for high-level engine control.

/// Weather parameters expected by the simulation engine.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct WeatherParams {
    /// Cloudiness in range [0.0, 1.0].
    pub cloudiness: f32,
    /// Ambient temperature in Celsius.
    pub temperature_c: f32,
    /// Rain intensity in range [0.0, 1.0].
    pub rain_intensity: f32,
}
