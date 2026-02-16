//! Hawkes Process Intensity Tracker
//!
//! Self-exciting point process that tracks temporal density of events.
//! When event types needed for the next NFA transition are in a temporal burst,
//! the intensity spikes and boosts the forecast completion probability.
//!
//! O(1) per-event update via the recursive intensity formula:
//! `intensity(t) = mu + (intensity(t_prev) - mu + alpha) * exp(-beta * dt)`

/// Hawkes process intensity tracker for a single event type.
///
/// Tracks temporal density using a self-exciting point process model.
/// The intensity increases with each event arrival and decays exponentially
/// between events.
///
/// Uses exponential moving averages for parameter estimation, allowing fast
/// adaptation to regime changes (~20-40 events) instead of slow cumulative averaging.
#[derive(Debug, Clone)]
pub struct HawkesIntensity {
    /// Baseline rate (events per nanosecond), estimated online.
    mu: f64,
    /// Excitation magnitude — how much each event raises the intensity.
    alpha: f64,
    /// Decay rate (1/ns) — how fast the intensity returns to baseline.
    beta: f64,
    /// Current intensity value.
    intensity: f64,
    /// Last event timestamp (nanoseconds since epoch).
    last_time_ns: i64,
    /// Total events processed.
    event_count: u64,
    /// EMA of inter-event deltas (ns) for online mu estimation.
    ema_delta_ns: f64,
    /// EMA of squared inter-event deltas for variance estimation.
    ema_delta_sq_ns: f64,
}

/// Minimum events before parameter estimation is attempted.
const MIN_EVENTS_FOR_ESTIMATION: u64 = 10;
/// EMA smoothing factor — alpha=0.05 gives effective window of ~20 events.
const EMA_ALPHA: f64 = 0.05;

impl Default for HawkesIntensity {
    fn default() -> Self {
        Self::new()
    }
}

impl HawkesIntensity {
    /// Create a new Hawkes intensity tracker with sensible defaults.
    pub fn new() -> Self {
        Self {
            mu: 1e-9,      // 1 event/sec baseline (in events/ns)
            alpha: 0.5e-9, // moderate excitation
            beta: 1e-9,    // ~1 second decay half-life
            intensity: 1e-9,
            last_time_ns: 0,
            event_count: 0,
            ema_delta_ns: 0.0,
            ema_delta_sq_ns: 0.0,
        }
    }

    /// Update the intensity with a new event arrival.
    ///
    /// Uses the O(1) recursive formula:
    /// `intensity = mu + (intensity - mu + alpha) * exp(-beta * dt)`
    ///
    /// Parameters are re-estimated every event using EMA, allowing the model
    /// to adapt to regime changes within ~20-40 events.
    pub fn update(&mut self, timestamp_ns: i64) {
        if self.event_count == 0 {
            self.last_time_ns = timestamp_ns;
            self.intensity = self.mu + self.alpha;
            self.event_count = 1;
            return;
        }

        let dt = (timestamp_ns - self.last_time_ns).max(0) as f64;

        // EMA update of inter-event statistics
        if self.event_count == 1 {
            // Initialize EMA with first observation
            self.ema_delta_ns = dt;
            self.ema_delta_sq_ns = dt * dt;
        } else {
            self.ema_delta_ns = EMA_ALPHA * dt + (1.0 - EMA_ALPHA) * self.ema_delta_ns;
            self.ema_delta_sq_ns = EMA_ALPHA * dt * dt + (1.0 - EMA_ALPHA) * self.ema_delta_sq_ns;
        }

        // Recursive intensity update
        let decay = (-self.beta * dt).exp();
        self.intensity = self.mu + (self.intensity - self.mu + self.alpha) * decay;

        self.last_time_ns = timestamp_ns;
        self.event_count += 1;

        // Re-estimate parameters every event after minimum history (cheap with EMA)
        if self.event_count >= MIN_EVENTS_FOR_ESTIMATION {
            self.estimate_parameters();
        }
    }

    /// Get the current intensity decayed to the given timestamp.
    pub fn current_intensity(&self, now_ns: i64) -> f64 {
        if self.event_count == 0 {
            return self.mu;
        }
        let dt = (now_ns - self.last_time_ns).max(0) as f64;
        let decay = (-self.beta * dt).exp();
        self.mu + (self.intensity - self.mu) * decay
    }

    /// Compute the boost factor: ratio of current intensity to baseline.
    ///
    /// Returns a value in `[1.0, 5.0]`. During normal event rates, this is ~1.0.
    /// During bursts, it increases proportionally to temporal density.
    pub fn boost_factor(&self, now_ns: i64) -> f64 {
        if self.mu <= 0.0 {
            return 1.0;
        }
        let current = self.current_intensity(now_ns);
        (current / self.mu).clamp(1.0, 5.0)
    }

    /// Re-estimate baseline rate (mu) and decay rate (beta) from EMA statistics.
    ///
    /// Uses moment matching on EMA-smoothed inter-event deltas:
    /// - mu = 1 / ema_delta (baseline rate = inverse of smoothed inter-event time)
    /// - beta = 1 / stddev (decay inversely proportional to timing variability)
    ///
    /// EMA-based estimation adapts to regime changes in ~20-40 events (effective
    /// window = 1/EMA_ALPHA = 20), compared to hundreds with cumulative averaging.
    fn estimate_parameters(&mut self) {
        let mean_delta = self.ema_delta_ns;
        if mean_delta <= 0.0 {
            return;
        }

        // Baseline rate: inverse of smoothed mean inter-event time
        self.mu = (1.0 / mean_delta).max(1e-15);

        // Variance from EMA: Var ≈ E[X²] - E[X]²
        let variance = self.ema_delta_sq_ns - mean_delta * mean_delta;
        if variance > 0.0 {
            let stddev = variance.sqrt();
            // Decay rate inversely proportional to timing variability
            self.beta = (1.0 / stddev).max(1e-15);
        }

        // Alpha: moderate fraction of mu (self-excitation strength)
        self.alpha = self.mu * 0.5;

        // Update baseline intensity
        self.intensity = self.intensity.max(self.mu);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_boost_is_one() {
        let hawkes = HawkesIntensity::new();
        // Before any events, boost should be 1.0
        let boost = hawkes.boost_factor(0);
        assert!(
            (boost - 1.0).abs() < 1e-6,
            "Initial boost should be 1.0, got {}",
            boost
        );
    }

    #[test]
    fn test_burst_increases_boost() {
        let mut hawkes = HawkesIntensity::new();

        // Send a burst of events 1ms apart (1_000_000 ns)
        for i in 0..20 {
            hawkes.update(i * 1_000_000);
        }

        let boost = hawkes.boost_factor(20 * 1_000_000);
        assert!(
            boost > 1.0,
            "Burst of events should increase boost above 1.0, got {}",
            boost
        );
    }

    #[test]
    fn test_decay_reduces_boost() {
        let mut hawkes = HawkesIntensity::new();

        // Send a burst
        for i in 0..20 {
            hawkes.update(i * 1_000_000);
        }

        let boost_at_burst = hawkes.boost_factor(20 * 1_000_000);

        // Wait a long time (10 seconds later)
        let boost_after_decay = hawkes.boost_factor(20 * 1_000_000 + 10_000_000_000);

        assert!(
            boost_after_decay < boost_at_burst,
            "Boost should decay over time: at_burst={}, after_decay={}",
            boost_at_burst,
            boost_after_decay
        );
    }

    #[test]
    fn test_boost_clamped_at_five() {
        let mut hawkes = HawkesIntensity::new();

        // Send an extreme burst — events 1 microsecond apart
        for i in 0..1000 {
            hawkes.update(i * 1_000);
        }

        let boost = hawkes.boost_factor(1000 * 1_000);
        assert!(
            boost <= 5.0,
            "Boost should be clamped at 5.0, got {}",
            boost
        );
    }

    #[test]
    fn test_single_event_no_panic() {
        let mut hawkes = HawkesIntensity::new();
        hawkes.update(1_000_000_000);
        let boost = hawkes.boost_factor(1_000_000_000);
        assert!(boost >= 1.0, "Single event boost should be >= 1.0");
    }

    #[test]
    fn test_parameter_estimation() {
        let mut hawkes = HawkesIntensity::new();

        // Send 100 events at regular 1-second intervals
        for i in 0..100 {
            hawkes.update(i * 1_000_000_000);
        }

        // After estimation, mu should be approximately 1e-9 (1 event/ns = 1 event/sec)
        assert!(hawkes.mu > 0.0, "mu should be positive after estimation");
        assert!(
            hawkes.beta > 0.0,
            "beta should be positive after estimation"
        );
        assert!(
            hawkes.alpha > 0.0,
            "alpha should be positive after estimation"
        );
    }

    #[test]
    fn test_zero_delta_handled() {
        let mut hawkes = HawkesIntensity::new();

        // Events at exactly the same time
        hawkes.update(1_000_000_000);
        hawkes.update(1_000_000_000);
        hawkes.update(1_000_000_000);

        // Should not panic or produce NaN
        let boost = hawkes.boost_factor(1_000_000_000);
        assert!(boost.is_finite(), "Boost should be finite, got {}", boost);
        assert!(boost >= 1.0, "Boost should be >= 1.0, got {}", boost);
    }
}
