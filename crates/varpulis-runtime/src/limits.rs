//! Resource limits for event parsing to prevent denial-of-service attacks.
//!
//! When the `async-runtime` feature is enabled, these constants are re-exported
//! from `varpulis-connector-api` via `varpulis-connectors`. In WASM / sync-only
//! builds the same values are defined locally so that event parsing works
//! without pulling in tokio-dependent crates.

#[cfg(feature = "async-runtime")]
pub use varpulis_connectors::limits::*;

#[cfg(not(feature = "async-runtime"))]
mod wasm_limits {
    /// Maximum event payload size in bytes (1 MiB).
    pub const MAX_EVENT_PAYLOAD_BYTES: usize = 1_048_576;
    /// Maximum number of fields per event.
    pub const MAX_FIELDS_PER_EVENT: usize = 1_024;
    /// Maximum string value size in bytes (256 KiB).
    pub const MAX_STRING_VALUE_BYTES: usize = 262_144;
    /// Maximum JSON nesting depth.
    pub const MAX_JSON_DEPTH: usize = 32;
    /// Maximum elements in a single array value.
    pub const MAX_ARRAY_ELEMENTS: usize = 10_000;
    /// Maximum line length when reading event files (1 MiB).
    pub const MAX_LINE_LENGTH: usize = 1_048_576;
}

#[cfg(not(feature = "async-runtime"))]
pub use wasm_limits::*;
