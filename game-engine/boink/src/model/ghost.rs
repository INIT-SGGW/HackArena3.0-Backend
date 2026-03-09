//! Ghost mode settings and runtime state for high-level engine control.

use boink_sys as sys;

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

/// High-level runtime phase of ghost mode for a single vehicle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GhostModePhase {
    /// Ghost mode is disabled for this vehicle and collisions are enabled.
    #[default]
    Inactive,
    /// Ghost mode enter conditions are progressing and collisions are still enabled.
    PendingEnter,
    /// Ghost mode is enabled for this vehicle and collisions are disabled.
    Active,
    /// Ghost mode is still active, but exit countdown is currently running.
    PendingExit,
}

impl From<sys::BoinkGhostModePhase> for GhostModePhase {
    fn from(raw: sys::BoinkGhostModePhase) -> Self {
        match raw {
            sys::BoinkGhostModePhase::BOINK_GHOST_MODE_PHASE_INACTIVE => Self::Inactive,
            sys::BoinkGhostModePhase::BOINK_GHOST_MODE_PHASE_PENDING_ENTER => Self::PendingEnter,
            sys::BoinkGhostModePhase::BOINK_GHOST_MODE_PHASE_ACTIVE => Self::Active,
            sys::BoinkGhostModePhase::BOINK_GHOST_MODE_PHASE_PENDING_EXIT => Self::PendingExit,
        }
    }
}

/// completed_laps is below GhostModeSettings.until_completed_laps.
pub const GHOST_MODE_BLOCKER_LAPS_REQUIREMENT_NOT_MET: u32 =
    sys::BOINK_GHOST_MODE_BLOCKER_LAPS_REQUIREMENT_NOT_MET;
/// Current speed is not above GhostModeSettings.exit_speed_min_mps.
pub const GHOST_MODE_BLOCKER_EXIT_SPEED_NOT_MET: u32 =
    sys::BOINK_GHOST_MODE_BLOCKER_EXIT_SPEED_NOT_MET;
/// Exit speed condition is met, but exit delay is still counting down.
pub const GHOST_MODE_BLOCKER_EXIT_DELAY_RUNNING: u32 =
    sys::BOINK_GHOST_MODE_BLOCKER_EXIT_DELAY_RUNNING;
/// Vehicle overlap is currently present and prevents ghost mode exit.
pub const GHOST_MODE_BLOCKER_VEHICLE_OVERLAP_ACTIVE: u32 =
    sys::BOINK_GHOST_MODE_BLOCKER_VEHICLE_OVERLAP_ACTIVE;
/// Overlap is cleared, but no-overlap exit delay is still counting down.
pub const GHOST_MODE_BLOCKER_OVERLAP_EXIT_DELAY_RUNNING: u32 =
    sys::BOINK_GHOST_MODE_BLOCKER_OVERLAP_EXIT_DELAY_RUNNING;
/// Vehicle is currently in pit area.
pub const GHOST_MODE_BLOCKER_IN_PIT: u32 = sys::BOINK_GHOST_MODE_BLOCKER_IN_PIT;

/// Runtime ghost mode state for a single vehicle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GhostModeRuntimeState {
    /// Authoritative collision flag for this vehicle at current tick.
    pub can_collide_now: bool,
    /// Current high-level ghost mode phase.
    pub phase: GhostModePhase,
    /// Bitmask of currently active blockers.
    pub blockers_mask: u32,
    /// Remaining time to complete ghost-mode enter countdown.
    pub enter_delay_remaining_ms: u32,
    /// Remaining time to complete ghost-mode exit countdown.
    pub exit_delay_remaining_ms: u32,
}

impl Default for GhostModeRuntimeState {
    fn default() -> Self {
        Self {
            can_collide_now: true,
            phase: GhostModePhase::Inactive,
            blockers_mask: 0,
            enter_delay_remaining_ms: 0,
            exit_delay_remaining_ms: 0,
        }
    }
}

impl GhostModeRuntimeState {
    /// Returns true when the given blocker bit is currently active.
    pub const fn has_blocker(&self, blocker_mask: u32) -> bool {
        (self.blockers_mask & blocker_mask) != 0
    }
}

impl From<sys::BoinkGhostModeRuntimeState> for GhostModeRuntimeState {
    fn from(raw: sys::BoinkGhostModeRuntimeState) -> Self {
        Self {
            can_collide_now: raw.can_collide_now,
            phase: raw.phase.into(),
            blockers_mask: raw.blockers_mask,
            enter_delay_remaining_ms: raw.enter_delay_remaining_ms,
            exit_delay_remaining_ms: raw.exit_delay_remaining_ms,
        }
    }
}
