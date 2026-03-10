//! Weather service modules (query + admin).

#[cfg(feature = "official")]
pub mod admin;
#[cfg(feature = "official")]
mod mappers;
pub mod query;
#[cfg(feature = "official")]
mod stochastic;

#[cfg(feature = "official")]
pub use admin::WeatherAdminServiceImpl;
pub use query::WeatherQueryServiceImpl;
