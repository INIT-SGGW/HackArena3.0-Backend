//! Safe Rust wrapper around the Boink native racing engine.
//!
//! Re-exports the high-level [`Engine`] along with error types and data models.

pub mod engine;
pub mod error;
pub mod model;
pub mod version;

pub use crate::engine::Engine;
pub use crate::error::{Error, Result};
