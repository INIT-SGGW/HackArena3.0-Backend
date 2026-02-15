//! Domain data structures shared by the safe Boink wrapper.
//!
//! Collects math helpers, driver controls, and car state representations.

pub mod control;
pub mod math;
pub mod state;
pub mod weather;

pub use control::Controls;
pub use math::{Quaternion, Vec3};
pub use state::{Gear, VehicleState};
pub use weather::WeatherParams;
