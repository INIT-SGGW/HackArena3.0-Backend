//! Reserved API surface for future legacy dynamic symbol loading.
//!
//! Legacy mode currently uses direct `boink_sys` calls with a relaxed
//! compatibility gate (`MIN_LEGACY_C_API_VERSION`), so no symbol table is
//! needed at runtime.

#![allow(dead_code)]

/// Marker type kept for future dynamic API wiring.
#[derive(Debug, Default, Clone, Copy)]
pub struct NativeApi;
