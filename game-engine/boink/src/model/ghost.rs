//! Ghost mode settings for high-level engine control.

/// Rule used to combine ghost-mode entry conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum GhostModeConditionLogic {
    #[default]
    Unspecified = 0,
    And = 1,
    Or = 2,
}

impl GhostModeConditionLogic {
    /// Converts enum to C/proto-compatible numeric representation.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self as i32
    }

    /// Converts numeric representation to typed enum.
    #[must_use]
    pub const fn from_i32(value: i32) -> Self {
        match value {
            1 => Self::And,
            2 => Self::Or,
            _ => Self::Unspecified,
        }
    }
}

/// Ghost mode parameters expected by the simulation engine.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct GhostModeSettings {
    /// Enables or disables ghost mode.
    pub enabled: bool,
    /// Minimum speed to enter ghost mode in meters per second.
    pub min_speed_enter_mps: f32,
    /// Minimum speed to stay in ghost mode in meters per second.
    pub min_speed_exit_mps: f32,
    /// Required time above enter threshold before ghost mode is enabled.
    pub enter_delay_ms: u32,
    /// Required time below exit threshold before ghost mode is disabled.
    pub exit_delay_ms: u32,
    /// Minimum completed laps required for ghost mode logic.
    pub min_completed_laps: u32,
    /// Entry-condition combine rule for speed and completed-laps checks.
    pub condition_logic: GhostModeConditionLogic,
    /// Required time after overlap ends before ghost mode may be disabled.
    pub overlap_exit_delay_ms: u32,
}
