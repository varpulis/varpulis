//! Portable clock abstraction that works in both native and WASM environments.
//!
//! `std::time::Instant` panics in WASM because no monotonic clock is available.
//! This module provides a drop-in replacement using `chrono::Utc::now()` which
//! works everywhere (uses `Date.now()` in WASM via js-sys).

use std::time::Duration;

use chrono::{DateTime, Utc};

/// A portable timestamp that works in both native and WASM.
///
/// Wraps `chrono::DateTime<Utc>` to provide `Instant`-like API for
/// elapsed duration calculations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    /// Capture the current time.
    pub fn now() -> Self {
        Self(Utc::now())
    }

    /// Returns the duration elapsed since this timestamp was created.
    pub fn elapsed(&self) -> Duration {
        let elapsed = Utc::now() - self.0;
        elapsed.to_std().unwrap_or(Duration::ZERO)
    }

    /// Returns the duration between two timestamps.
    pub fn duration_since(&self, earlier: Timestamp) -> Duration {
        let d = self.0 - earlier.0;
        d.to_std().unwrap_or(Duration::ZERO)
    }

    /// Returns the underlying `DateTime<Utc>` for interop.
    pub fn as_datetime(&self) -> DateTime<Utc> {
        self.0
    }

    /// Returns nanoseconds since epoch (for snapshot export).
    pub fn as_nanos_since_epoch(&self) -> i64 {
        self.0.timestamp_nanos_opt().unwrap_or(0)
    }
}

impl std::ops::Add<Duration> for Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: Duration) -> Timestamp {
        Timestamp(self.0 + chrono::Duration::from_std(rhs).unwrap_or(chrono::Duration::zero()))
    }
}

impl std::ops::Sub<Timestamp> for Timestamp {
    type Output = Duration;

    fn sub(self, rhs: Timestamp) -> Duration {
        self.duration_since(rhs)
    }
}
