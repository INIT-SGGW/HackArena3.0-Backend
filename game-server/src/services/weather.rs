//! Weather service modules (query + admin).

#[cfg(feature = "official")]
pub mod admin;
#[cfg(feature = "local")]
mod local_events;
#[cfg(feature = "official")]
mod mappers;
pub mod query;
#[cfg(feature = "official")]
mod stochastic;

#[cfg(feature = "official")]
pub use admin::WeatherAdminServiceImpl;
#[cfg(feature = "local")]
pub use local_events::{LocalWeatherEvent, LocalWeatherEventHub, LocalWeatherEventKind};
pub use query::WeatherQueryServiceImpl;
