//! Circuit breaker for sink connectors.
//!
//! Implements the standard three-state circuit breaker pattern:
//! - **Closed**: normal operation, events sent to sink
//! - **Open**: sink is failing, events rejected immediately (routed to DLQ)
//! - **HalfOpen**: probe period, one request allowed through to test recovery
//!
//! Transitions:
//! - Closed → Open: when failure count exceeds `failure_threshold` within `window`
//! - Open → HalfOpen: after `reset_timeout` elapses
//! - HalfOpen → Closed: on successful probe
//! - HalfOpen → Open: on failed probe

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Closed,
    Open,
    HalfOpen,
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
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Mutex<InnerState>,
    // Metrics (lock-free)
    pub failures_total: AtomicU64,
    pub successes_total: AtomicU64,
    pub rejections_total: AtomicU64,
}

struct InnerState {
    state: State,
    consecutive_failures: u32,
    last_failure_time: Option<Instant>,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
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
    ///
    /// Returns `true` if the circuit is closed or half-open (probe allowed).
    /// Returns `false` if open (caller should route to DLQ).
    pub fn allow_request(&self) -> bool {
        let mut inner = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match inner.state {
            State::Closed => true,
            State::Open => {
                // Check if reset timeout has elapsed → transition to HalfOpen
                if let Some(last_failure) = inner.last_failure_time {
                    if last_failure.elapsed() >= self.config.reset_timeout {
                        inner.state = State::HalfOpen;
                        return true; // Allow one probe request
                    }
                }
                self.rejections_total.fetch_add(1, Ordering::Relaxed);
                false
            }
            State::HalfOpen => true, // Allow probe
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
                // Probe failed → back to open
                inner.state = State::Open;
            }
            State::Open => {}
        }
    }

    /// Get the current state (for metrics/logging).
    pub fn state(&self) -> State {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_starts_closed() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::default());
        assert_eq!(cb.state(), State::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_opens_after_threshold() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            reset_timeout: Duration::from_secs(60),
        });

        assert!(cb.allow_request());
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), State::Closed);

        cb.record_failure(); // 3rd failure → opens
        assert_eq!(cb.state(), State::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_success_resets_count() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            reset_timeout: Duration::from_secs(60),
        });

        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // Resets consecutive count
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), State::Closed); // Still closed (only 2 consecutive)
    }

    #[test]
    fn test_half_open_after_timeout() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            reset_timeout: Duration::from_millis(10),
        });

        cb.record_failure(); // Opens
        assert_eq!(cb.state(), State::Open);

        std::thread::sleep(Duration::from_millis(15));
        assert!(cb.allow_request()); // Transitions to HalfOpen
        assert_eq!(cb.state(), State::HalfOpen);
    }

    #[test]
    fn test_half_open_success_closes() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            reset_timeout: Duration::from_millis(10),
        });

        cb.record_failure();
        std::thread::sleep(Duration::from_millis(15));
        cb.allow_request(); // HalfOpen

        cb.record_success();
        assert_eq!(cb.state(), State::Closed);
    }

    #[test]
    fn test_half_open_failure_reopens() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            reset_timeout: Duration::from_millis(10),
        });

        cb.record_failure();
        std::thread::sleep(Duration::from_millis(15));
        cb.allow_request(); // HalfOpen

        cb.record_failure();
        assert_eq!(cb.state(), State::Open);
    }

    #[test]
    fn test_metrics_counting() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 2,
            reset_timeout: Duration::from_secs(60),
        });

        cb.record_success();
        cb.record_success();
        cb.record_failure();
        cb.record_failure(); // Opens

        // Open circuit rejects
        cb.allow_request();

        assert_eq!(cb.successes_total.load(Ordering::Relaxed), 2);
        assert_eq!(cb.failures_total.load(Ordering::Relaxed), 2);
        assert_eq!(cb.rejections_total.load(Ordering::Relaxed), 1);
    }
}
