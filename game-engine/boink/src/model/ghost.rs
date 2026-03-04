//! Ghost mode settings for high-level engine control.

/// Ghost mode parameters expected by the simulation engine.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct GhostModeSettings {
    /// Enables or disables ghost mode.
    pub enabled: bool,
    /// Maximum speed threshold to enter ghost mode in meters per second.
    pub enter_speed_max_mps: f32,
    /// Minimum speed threshold to exit ghost mode in meters per second.
    pub exit_speed_min_mps: f32,
    /// Required time above enter threshold before ghost mode is enabled.
    pub enter_delay_ms: u32,
    /// Required time below exit threshold before ghost mode is disabled.
    pub exit_delay_ms: u32,
    /// Ghost mode remains enabled until this many laps are completed.
    pub until_completed_laps: u32,
    /// Required time after overlap ends before ghost mode may be disabled.
    pub vehicle_overlap_exit_delay_ms: u32,
}
