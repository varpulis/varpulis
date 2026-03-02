//! SmartModule: user-defined WASM processing functions.
//!
//! Inspired by Fluvio's SmartModules, this module allows users to write
//! custom filter and map functions in any language that compiles to WASM
//! (Rust, Go, C, AssemblyScript, etc.).
//!
//! ## Supported Operations
//!
//! - **SmartFilter**: Per-event predicate — returns `true` to keep, `false` to drop
//! - **SmartMap**: Per-event transformation — returns a modified event
//!
//! ## Guest ABI
//!
//! User WASM modules must export the following functions:
//!
//! | Export | Signature | Description |
//! |--------|-----------|-------------|
//! | `memory` | `Memory` | Linear memory (at least 1 page) |
//! | `sm_alloc` | `(i32) -> i32` | Allocate `size` bytes, return pointer |
//! | `sm_dealloc` | `(i32, i32) -> ()` | Free `size` bytes at `ptr` |
//! | `sm_filter` | `(i32, i32) -> i32` | Filter: 0 = drop, non-zero = keep |
//! | `sm_map` | `(i32, i32) -> i64` | Map: returns `(ptr << 32) \| len` |
//!
//! Events are passed as JSON bytes through linear memory. The host
//! serializes `Event` to JSON, allocates space via `sm_alloc`, copies
//! the bytes in, and calls the processing function.
//!
//! ## Resource Limits
//!
//! - **CPU**: Fuel-based instruction counting (configurable per call)
//! - **Memory**: Maximum linear memory size limit
//!
//! ## Example
//!
//! ```rust,no_run
//! use varpulis_wasm::smartmodule::*;
//! use varpulis_core::Event;
//!
//! let engine = SmartModuleEngine::new().unwrap();
//! let config = SmartModuleConfig {
//!     kind: SmartModuleKind::Filter,
//!     max_fuel: Some(1_000_000),
//!     ..Default::default()
//! };
//!
//! let wasm_bytes = std::fs::read("my_filter.wasm").unwrap();
//! let mut instance = engine.instantiate(&wasm_bytes, config).unwrap();
//!
//! let event = Event::new("SensorReading");
//! let keep = instance.filter(&event).unwrap();
//! ```

mod engine;
mod error;

pub use engine::{SmartModuleConfig, SmartModuleEngine, SmartModuleInstance};
pub use error::SmartModuleError;

/// Kind of SmartModule processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmartModuleKind {
    /// Filter events: returns `true` to keep, `false` to drop.
    Filter,
    /// Transform events: returns a modified event.
    Map,
}
