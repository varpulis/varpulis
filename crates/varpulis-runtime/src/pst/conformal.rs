//! Conformal Prediction Intervals
//!
//! Distribution-free calibrated `[lower, upper]` bounds on forecast probabilities
//! with formal coverage guarantees. Uses a sliding window of nonconformity scores
//! from past forecast outcomes.
//!
//! Nonconformity score: `|predicted_probability - actual_outcome|`
//! where actual is 1.0 (pattern completed) or 0.0 (pattern expired/aborted).
//!
//! The prediction interval `[p - q, p + q]` is guaranteed to contain the true
//! outcome with probability >= `coverage` (default 90%), assuming exchangeability.

use std::collections::VecDeque;

/// Conformal calibrator that produces prediction intervals from past forecast outcomes.
#[derive(Debug, Clone)]
pub struct ConformalCalibrator {
    /// Sliding window of nonconformity scores.
    scores: VecDeque<f64>,
    /// Maximum number of scores to retain.
    max_scores: usize,
    /// Target coverage level (e.g., 0.9 for 90%).
    coverage: f64,
    /// Cached quantile value, invalidated on new score insertion.
    cached_quantile: Option<f64>,
}

impl ConformalCalibrator {
    /// Create a calibrator with default settings: 90% coverage, 1000-sample window.
    pub fn with_defaults() -> Self {
        Self {
            scores: VecDeque::with_capacity(1000),
            max_scores: 1000,
            coverage: 0.9,
            cached_quantile: None,
        }
    }

    /// Record the outcome of a past forecast for calibration.
    ///
    /// - `predicted`: the probability that was forecast
    /// - `completed`: whether the pattern actually completed (true) or expired (false)
    pub fn record_outcome(&mut self, predicted: f64, completed: bool) {
        let actual = if completed { 1.0 } else { 0.0 };
        let score = (predicted - actual).abs();
        self.scores.push_back(score);
        if self.scores.len() > self.max_scores {
            self.scores.pop_front();
        }
        self.cached_quantile = None;
    }

    /// Compute a prediction interval around the given probability.
    ///
    /// Returns `(lower, upper)` clamped to `[0.0, 1.0]`.
    /// When no calibration data is available, returns `(0.0, 1.0)` (maximum uncertainty).
    pub fn prediction_interval(&mut self, predicted: f64) -> (f64, f64) {
        if self.scores.is_empty() {
            return (0.0, 1.0);
        }

        let q = self.quantile();
        let lower = (predicted - q).max(0.0);
        let upper = (predicted + q).min(1.0);
        (lower, upper)
    }

    /// Compute the conformal quantile from the score distribution.
    ///
    /// Uses the standard conformal prediction formula:
    /// quantile index = `ceil((n + 1) * (1 - coverage))`
    fn quantile(&mut self) -> f64 {
        if let Some(q) = self.cached_quantile {
            return q;
        }

        let n = self.scores.len();
        if n == 0 {
            return 1.0;
        }

        let mut sorted: Vec<f64> = self.scores.iter().copied().collect();
        sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // Conformal quantile index: ceil((n+1) * (1 - coverage))
        let alpha = 1.0 - self.coverage;
        let idx_f = ((n + 1) as f64 * alpha).ceil();
        let idx = (idx_f as usize).saturating_sub(1).min(n - 1);

        // The quantile is at the (1 - coverage) position from the top
        // We want the score at position (n - 1 - idx) from sorted ascending,
        // which gives us the threshold that covers `coverage` fraction of scores.
        let q = sorted[n - 1 - idx.min(n - 1)];
        self.cached_quantile = Some(q);
        q
    }

    /// Number of calibration samples collected.
    pub fn sample_count(&self) -> usize {
        self.scores.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_returns_full_interval() {
        let mut cal = ConformalCalibrator::with_defaults();
        let (lower, upper) = cal.prediction_interval(0.5);
        assert!(
            (lower - 0.0).abs() < 1e-10,
            "Empty calibrator lower should be 0.0, got {}",
            lower
        );
        assert!(
            (upper - 1.0).abs() < 1e-10,
            "Empty calibrator upper should be 1.0, got {}",
            upper
        );
    }

    #[test]
    fn test_perfect_predictions_narrow_interval() {
        let mut cal = ConformalCalibrator::with_defaults();

        // Record many perfect predictions (predicted ~1.0, actually completed)
        for _ in 0..100 {
            cal.record_outcome(0.95, true);
        }
        // Also record some accurate non-completions
        for _ in 0..100 {
            cal.record_outcome(0.05, false);
        }

        let (lower, upper) = cal.prediction_interval(0.7);
        let width = upper - lower;
        assert!(
            width < 0.5,
            "Perfect predictions should yield narrow interval, got width={}",
            width
        );
    }

    #[test]
    fn test_bad_predictions_wide_interval() {
        let mut cal = ConformalCalibrator::with_defaults();

        // Record many bad predictions (predicted ~0.9, but didn't complete)
        for _ in 0..100 {
            cal.record_outcome(0.9, false);
        }
        // Also: predicted ~0.1, but actually completed
        for _ in 0..100 {
            cal.record_outcome(0.1, true);
        }

        let (lower, upper) = cal.prediction_interval(0.5);
        let width = upper - lower;
        assert!(
            width > 0.5,
            "Bad predictions should yield wide interval, got width={}",
            width
        );
    }

    #[test]
    fn test_buffer_bounded() {
        let mut cal = ConformalCalibrator::with_defaults();

        // Add more scores than the max
        for i in 0..2000 {
            cal.record_outcome(i as f64 / 2000.0, i % 2 == 0);
        }

        assert_eq!(
            cal.sample_count(),
            1000,
            "Score buffer should be bounded at max_scores"
        );
    }

    #[test]
    fn test_lower_le_upper_invariant() {
        let mut cal = ConformalCalibrator::with_defaults();

        // Add mixed predictions
        for i in 0..50 {
            cal.record_outcome(i as f64 / 50.0, i % 3 == 0);
        }

        // Check invariant across a range of predicted values
        for p in [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0] {
            let (lower, upper) = cal.prediction_interval(p);
            assert!(
                lower <= upper,
                "lower ({}) should be <= upper ({}) for predicted={}",
                lower,
                upper,
                p
            );
            assert!(lower >= 0.0, "lower should be >= 0.0, got {}", lower);
            assert!(upper <= 1.0, "upper should be <= 1.0, got {}", upper);
        }
    }

    #[test]
    fn test_interval_clamped_to_unit() {
        let mut cal = ConformalCalibrator::with_defaults();

        // Bad predictions create large nonconformity scores
        for _ in 0..100 {
            cal.record_outcome(1.0, false); // score = 1.0
        }

        let (lower, upper) = cal.prediction_interval(0.5);
        assert!(
            lower >= 0.0,
            "Lower should be clamped >= 0.0, got {}",
            lower
        );
        assert!(
            upper <= 1.0,
            "Upper should be clamped <= 1.0, got {}",
            upper
        );
    }
}
