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
pub use ghost::GhostModeSettings;
pub use math::{Quaternion, Vec3};
pub use state::{Gear, VehicleState};
pub use track::{CenterlineSample, TrackData};
pub use weather::WeatherParams;
