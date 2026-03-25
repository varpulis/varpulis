//! Circuit breaker for sink connectors.
//!
//! Implements the standard three-state circuit breaker pattern:
//! - **Closed**: normal operation, events sent to sink
//! - **Open**: sink is failing, events rejected immediately (routed to DLQ)
//! - **HalfOpen**: probe period, one request allowed through to test recovery

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Normal operation -- requests pass through.
    Closed,
    /// Circuit tripped -- requests are rejected immediately.
    Open,
    /// Probing -- one request allowed to test recovery.
    HalfOpen,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("closed"),
            Self::Open => f.write_str("open"),
            Self::HalfOpen => f.write_str("half_open"),
        }
    }
}

/// Configuration for a circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit.
    pub failure_threshold: u32,
    /// How long to wait before probing (half-open).
    pub reset_timeout: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(30),
        }
    }
}

/// Thread-safe circuit breaker.
#[derive(Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Mutex<InnerState>,
    // Metrics (lock-free)
    /// Total number of recorded failures (atomic counter).
    pub failures_total: AtomicU64,
    /// Total number of recorded successes (atomic counter).
    pub successes_total: AtomicU64,
    /// Total number of rejected requests while circuit was open (atomic counter).
    pub rejections_total: AtomicU64,
}

#[derive(Debug)]
struct InnerState {
    state: State,
    consecutive_failures: u32,
    last_failure_time: Option<Instant>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration.
    pub const fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Mutex::new(InnerState {
                state: State::Closed,
                consecutive_failures: 0,
                last_failure_time: None,
            }),
            failures_total: AtomicU64::new(0),
            successes_total: AtomicU64::new(0),
            rejections_total: AtomicU64::new(0),
        }
    }

    /// Check if a request should be allowed through.
    pub fn allow_request(&self) -> bool {
        let mut inner = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match inner.state {
            State::Closed => true,
            State::Open => {
                if let Some(last_failure) = inner.last_failure_time {
                    if last_failure.elapsed() >= self.config.reset_timeout {
                        inner.state = State::HalfOpen;
                        return true;
                    }
                }
                self.rejections_total.fetch_add(1, Ordering::Relaxed);
                false
            }
            State::HalfOpen => true,
        }
    }

    /// Record a successful send.
    pub fn record_success(&self) {
        self.successes_total.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.state.lock().unwrap_or_else(|e| e.into_inner());
        inner.consecutive_failures = 0;
        if inner.state == State::HalfOpen {
            inner.state = State::Closed;
        }
    }

    /// Record a failed send.
    pub fn record_failure(&self) {
        self.failures_total.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.state.lock().unwrap_or_else(|e| e.into_inner());
        inner.consecutive_failures += 1;
        inner.last_failure_time = Some(Instant::now());

        match inner.state {
            State::Closed => {
                if inner.consecutive_failures >= self.config.failure_threshold {
                    inner.state = State::Open;
                }
            }
            State::HalfOpen => {
                inner.state = State::Open;
            }
            State::Open => {}
        }
    }

    /// Get the current state (for metrics/logging).
    pub fn state(&self) -> State {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).state
    }

    /// Get the current consecutive failure count.
    pub fn consecutive_failures(&self) -> u32 {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .consecutive_failures
    }

    /// Get the last failure time (if any).
    pub fn last_failure_time(&self) -> Option<Instant> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last_failure_time
    }
}
