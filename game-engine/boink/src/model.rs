//! Domain data structures shared by the safe Boink wrapper.
//!
//! Collects math helpers, driver controls, and car state representations.

pub mod control;
pub mod ghost;
pub mod math;
pub mod state;
pub mod track;
pub mod weather;

pub use control::{AcceptedControls, Controls, GearShift};
pub use ghost::{
    GHOST_MODE_BLOCKER_EXIT_DELAY_RUNNING, GHOST_MODE_BLOCKER_EXIT_SPEED_NOT_MET,
    GHOST_MODE_BLOCKER_IN_PIT, GHOST_MODE_BLOCKER_LAPS_REQUIREMENT_NOT_MET,
    GHOST_MODE_BLOCKER_OVERLAP_EXIT_DELAY_RUNNING, GHOST_MODE_BLOCKER_VEHICLE_OVERLAP_ACTIVE,
    GhostModePhase, GhostModeRuntimeState, GhostModeSettings,
};
pub use math::{Quaternion, Vec3};
pub use state::{
    Gear, PitstopZone, PitstopZoneMask, RaceBestLap, TyreType, VehicleBestLap, VehiclePitstopState,
    VehicleRaceMetrics, VehicleState,
};
pub use track::{CenterlineSample, GroundType, GroundWidth, PitstopData, TrackData};
pub use weather::WeatherParams;
