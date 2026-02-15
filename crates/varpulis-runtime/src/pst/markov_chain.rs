//! Pattern Markov Chain (PMC) = PST + SASE NFA
//!
//! Combines the Prediction Suffix Tree with the SASE NFA to forecast
//! whether a partially-matched pattern will complete and estimate when.
//!
//! Includes Hawkes process intensity modulation for temporal density awareness
//! and conformal prediction intervals for calibrated uncertainty bounds.

use super::conformal::ConformalCalibrator;
use super::hawkes::HawkesIntensity;
use super::online::OnlinePSTLearner;
use super::tree::{PSTConfig, PredictionSuffixTree, SymbolId};
use rustc_hash::FxHashMap;

/// Configuration for the Pattern Markov Chain.
#[derive(Debug, Clone)]
pub struct PMCConfig {
    /// Minimum probability to emit a forecast.
    pub confidence_threshold: f64,
    /// Forecast horizon in nanoseconds.
    pub horizon_ns: u64,
    /// Maximum simulation steps for waiting time estimation.
    pub max_simulation_steps: usize,
    /// Number of events before forecasting starts.
    pub warmup_events: u64,
    /// Enable Hawkes intensity modulation (default true).
    pub hawkes_enabled: bool,
    /// Enable conformal prediction intervals (default true).
    pub conformal_enabled: bool,
}

impl Default for PMCConfig {
    fn default() -> Self {
        Self {
            confidence_threshold: 0.5,
            horizon_ns: 300_000_000_000, // 5 minutes
            max_simulation_steps: 1000,
            warmup_events: 100,
            hawkes_enabled: true,
            conformal_enabled: true,
        }
    }
}

/// Result of a forecast computation.
#[derive(Debug, Clone)]
pub struct ForecastResult {
    /// Probability that the pattern will complete.
    pub probability: f64,
    /// Estimated time to completion in nanoseconds.
    pub expected_time_ns: u64,
    /// Label describing current state (e.g., "state_2_of_4").
    pub state_label: String,
    /// Depth of PST context used for this prediction.
    pub context_depth: usize,
    /// Number of currently active partial match runs.
    pub active_runs: usize,
    /// Lower bound of the conformal prediction interval (0.0–1.0).
    pub forecast_lower: f64,
    /// Upper bound of the conformal prediction interval (0.0–1.0).
    pub forecast_upper: f64,
}

/// Snapshot of an active SASE run for forecast computation.
#[derive(Debug, Clone)]
pub struct RunSnapshot {
    /// Current NFA state index.
    pub current_state: usize,
    /// When this run started (nanoseconds since epoch).
    pub started_at_ns: i64,
}

/// Pattern Markov Chain — combines PST with SASE NFA for pattern forecasting.
///
/// Includes Hawkes process intensity modulation to boost predictions during
/// temporal bursts, and conformal calibration for prediction intervals.
pub struct PatternMarkovChain {
    /// The Prediction Suffix Tree for learning event transitions.
    pst: PredictionSuffixTree,
    /// NFA transition structure: state -> { symbol -> next_state }
    nfa_transitions: Vec<FxHashMap<SymbolId, usize>>,
    /// Accept (final) states in the NFA.
    accept_states: Vec<usize>,
    /// Total number of NFA states.
    num_states: usize,
    /// Configuration.
    config: PMCConfig,
    /// Online learner for incremental PST updates.
    learner: OnlinePSTLearner,
    /// Total events processed.
    events_processed: u64,
    /// Running average inter-event time in nanoseconds.
    avg_inter_event_ns: f64,
    /// Last event timestamp for inter-event time tracking.
    last_event_ns: Option<i64>,
    /// Mapping from NFA event types to PST symbol IDs.
    event_type_to_symbol: FxHashMap<String, SymbolId>,
    /// Hawkes intensity trackers, one per event type symbol.
    hawkes_intensities: FxHashMap<SymbolId, HawkesIntensity>,
    /// Conformal calibrator for prediction intervals.
    conformal: ConformalCalibrator,
    /// Previous run states for outcome tracking: started_at_ns -> last_known_state.
    previous_run_states: FxHashMap<i64, usize>,
    /// Pending forecasts for outcome tracking: started_at_ns -> predicted_probability.
    pending_forecasts: FxHashMap<i64, f64>,
}

impl PatternMarkovChain {
    /// Create a new PMC from SASE NFA transition structure.
    ///
    /// `nfa_states` contains the NFA state information:
    /// each entry is (state_id, event_type, transitions_to).
    pub fn new(
        nfa_event_types: &[String],
        nfa_transitions: Vec<FxHashMap<SymbolId, usize>>,
        accept_states: Vec<usize>,
        num_states: usize,
        pst_config: PSTConfig,
        pmc_config: PMCConfig,
    ) -> Self {
        let max_depth = pst_config.max_depth;
        let mut pst = PredictionSuffixTree::new(pst_config);
        let mut event_type_to_symbol = FxHashMap::default();
        let mut hawkes_intensities = FxHashMap::default();

        // Register all event types from the NFA
        for et in nfa_event_types {
            let id = pst.register_symbol(et);
            event_type_to_symbol.insert(et.clone(), id);
            if pmc_config.hawkes_enabled {
                hawkes_intensities.insert(id, HawkesIntensity::new());
            }
        }

        let learner = OnlinePSTLearner::new(max_depth);

        Self {
            pst,
            nfa_transitions,
            accept_states,
            num_states,
            config: pmc_config,
            learner,
            events_processed: 0,
            avg_inter_event_ns: 0.0,
            last_event_ns: None,
            event_type_to_symbol,
            hawkes_intensities,
            conformal: ConformalCalibrator::with_defaults(),
            previous_run_states: FxHashMap::default(),
            pending_forecasts: FxHashMap::default(),
        }
    }

    /// Process an incoming event: update PST online and compute forecast.
    ///
    /// Returns `Some(ForecastResult)` if there are active runs and the warmup period
    /// has elapsed, `None` otherwise.
    pub fn process(
        &mut self,
        event_type: &str,
        event_timestamp_ns: i64,
        active_runs: &[RunSnapshot],
    ) -> Option<ForecastResult> {
        // --- Outcome tracking: detect completed/expired runs ---
        if self.config.conformal_enabled {
            self.track_outcomes(active_runs);
        }

        // Update inter-event timing
        if let Some(last_ns) = self.last_event_ns {
            let delta = (event_timestamp_ns - last_ns).max(0) as f64;
            // Exponential moving average
            if self.avg_inter_event_ns == 0.0 {
                self.avg_inter_event_ns = delta;
            } else {
                self.avg_inter_event_ns = 0.95 * self.avg_inter_event_ns + 0.05 * delta;
            }
        }
        self.last_event_ns = Some(event_timestamp_ns);

        // Update PST online and (optionally) Hawkes intensity for this event type
        if let Some(&symbol) = self.event_type_to_symbol.get(event_type) {
            self.learner.update(&mut self.pst, symbol);
            if self.config.hawkes_enabled {
                if let Some(hawkes) = self.hawkes_intensities.get_mut(&symbol) {
                    hawkes.update(event_timestamp_ns);
                }
            }
        }

        self.events_processed += 1;

        // Suppress during warmup
        if self.events_processed < self.config.warmup_events {
            return None;
        }

        // No active runs -> no forecast needed
        if active_runs.is_empty() {
            return None;
        }

        // Compute forecast for the most advanced active run
        let best_run = active_runs.iter().max_by_key(|r| r.current_state)?;

        let context = self.learner.current_context();
        let context_depth = context.len();

        // Compute completion probability (Hawkes-modulated or plain PST)
        let probability = if self.config.hawkes_enabled {
            self.compute_completion_probability_hawkes(
                best_run.current_state,
                context,
                event_timestamp_ns,
            )
        } else {
            self.compute_completion_probability_plain(best_run.current_state, context)
        };

        // Store pending forecast for future outcome tracking (conformal only)
        if self.config.conformal_enabled {
            self.pending_forecasts
                .insert(best_run.started_at_ns, probability);
        }

        // Compute conformal prediction interval or return full interval
        let (forecast_lower, forecast_upper) = if self.config.conformal_enabled {
            self.conformal.prediction_interval(probability)
        } else {
            (0.0, 1.0)
        };

        // Estimate waiting time
        let expected_time_ns = self.estimate_waiting_time(best_run.current_state, context);

        // State label
        let state_label = format!("state_{}_of_{}", best_run.current_state, self.num_states);

        Some(ForecastResult {
            probability,
            expected_time_ns,
            state_label,
            context_depth,
            active_runs: active_runs.len(),
            forecast_lower,
            forecast_upper,
        })
    }

    /// Track outcomes of previously observed runs by comparing current active_runs
    /// to the previous snapshot. Runs that disappeared either completed (were in
    /// accept state) or expired.
    fn track_outcomes(&mut self, active_runs: &[RunSnapshot]) {
        // Build a set of current run keys
        let current_keys: FxHashMap<i64, usize> = active_runs
            .iter()
            .map(|r| (r.started_at_ns, r.current_state))
            .collect();

        // Find runs that were in previous_run_states but are no longer in active_runs
        let disappeared: Vec<(i64, usize)> = self
            .previous_run_states
            .iter()
            .filter(|(key, _)| !current_keys.contains_key(key))
            .map(|(&key, &state)| (key, state))
            .collect();

        for (started_at, last_state) in disappeared {
            let completed = self.accept_states.contains(&last_state);
            if let Some(predicted) = self.pending_forecasts.remove(&started_at) {
                self.conformal.record_outcome(predicted, completed);
            }
        }

        // Update previous run states snapshot
        self.previous_run_states = current_keys;
    }

    /// Forward algorithm: compute probability using plain PST-predicted transitions (no Hawkes).
    fn compute_completion_probability_plain(
        &self,
        current_state: usize,
        context: &[SymbolId],
    ) -> f64 {
        if self.accept_states.contains(&current_state) {
            return 1.0;
        }

        let max_steps = self.config.max_simulation_steps.min(50);
        let mut prob = vec![0.0f64; self.num_states];

        for &s in &self.accept_states {
            prob[s] = 1.0;
        }

        for _ in 0..max_steps {
            let mut new_prob = prob.clone();
            for (state, new_p) in new_prob.iter_mut().enumerate().take(self.num_states) {
                if self.accept_states.contains(&state) {
                    continue;
                }
                if let Some(transitions) = self.nfa_transitions.get(state) {
                    let mut state_prob = 0.0;
                    for (&symbol, &next_state) in transitions {
                        let pst_prob = self.pst.predict_symbol(context, symbol);
                        state_prob += pst_prob * prob[next_state];
                    }
                    *new_p = state_prob;
                }
            }
            prob = new_prob;
        }

        prob.get(current_state).copied().unwrap_or(0.0).min(1.0)
    }

    /// Forward algorithm: compute probability using Hawkes-modulated PST-predicted transitions.
    fn compute_completion_probability_hawkes(
        &self,
        current_state: usize,
        context: &[SymbolId],
        now_ns: i64,
    ) -> f64 {
        if self.accept_states.contains(&current_state) {
            return 1.0;
        }

        let max_steps = self.config.max_simulation_steps.min(50);
        let mut prob = vec![0.0f64; self.num_states];

        for &s in &self.accept_states {
            prob[s] = 1.0;
        }

        for _ in 0..max_steps {
            let mut new_prob = prob.clone();
            for (state, new_p) in new_prob.iter_mut().enumerate().take(self.num_states) {
                if self.accept_states.contains(&state) {
                    continue;
                }
                if let Some(transitions) = self.nfa_transitions.get(state) {
                    // Compute Hawkes-modulated transition probabilities
                    let mut raw_probs: Vec<(SymbolId, usize, f64)> = transitions
                        .iter()
                        .map(|(&symbol, &next_state)| {
                            let pst_prob = self.pst.predict_symbol(context, symbol);
                            let boost = self
                                .hawkes_intensities
                                .get(&symbol)
                                .map_or(1.0, |h| h.boost_factor(now_ns));
                            (symbol, next_state, pst_prob * boost)
                        })
                        .collect();

                    // Renormalize so modulated transition probabilities sum correctly
                    let total: f64 = raw_probs.iter().map(|(_, _, p)| p).sum();
                    if total > 0.0 {
                        for entry in &mut raw_probs {
                            entry.2 /= total;
                        }
                    }

                    // Also get the original PST total for scaling
                    let pst_total: f64 = transitions
                        .keys()
                        .map(|&sym| self.pst.predict_symbol(context, sym))
                        .sum();

                    let mut state_prob = 0.0;
                    for &(_, next_state, modulated_prob) in &raw_probs {
                        // Scale by pst_total to preserve original probability magnitude
                        state_prob += modulated_prob * pst_total * prob[next_state];
                    }
                    *new_p = state_prob;
                }
            }
            prob = new_prob;
        }

        prob.get(current_state).copied().unwrap_or(0.0).min(1.0)
    }

    /// Estimate expected waiting time (nanoseconds) to pattern completion
    /// using Monte Carlo simulation on the PMC.
    fn estimate_waiting_time(&self, current_state: usize, context: &[SymbolId]) -> u64 {
        if self.accept_states.contains(&current_state) {
            return 0;
        }

        if self.avg_inter_event_ns <= 0.0 {
            return 0;
        }

        // Simple estimate: remaining states * avg inter-event time
        // Count minimum transitions to accept state via BFS
        let remaining = self.min_transitions_to_accept(current_state);
        if remaining == 0 {
            return 0;
        }

        // Weight by probability of progressing (higher probability = faster)
        let context_prob = if let Some(transitions) = self.nfa_transitions.get(current_state) {
            transitions
                .keys()
                .map(|&sym| self.pst.predict_symbol(context, sym))
                .sum::<f64>()
                .max(0.01)
        } else {
            0.01
        };

        // Expected time = (remaining transitions / progress probability) * avg inter-event
        let expected_events = remaining as f64 / context_prob;
        (expected_events * self.avg_inter_event_ns) as u64
    }

    /// BFS to find minimum transitions from a state to any accept state.
    fn min_transitions_to_accept(&self, start: usize) -> usize {
        if self.accept_states.contains(&start) {
            return 0;
        }

        let mut visited = vec![false; self.num_states];
        let mut queue = std::collections::VecDeque::new();
        queue.push_back((start, 0usize));
        visited[start] = true;

        while let Some((state, depth)) = queue.pop_front() {
            if let Some(transitions) = self.nfa_transitions.get(state) {
                for &next in transitions.values() {
                    if self.accept_states.contains(&next) {
                        return depth + 1;
                    }
                    if next < self.num_states && !visited[next] {
                        visited[next] = true;
                        queue.push_back((next, depth + 1));
                    }
                }
            }
        }

        // Unreachable accept state
        self.num_states
    }

    /// Get the number of events processed.
    pub fn events_processed(&self) -> u64 {
        self.events_processed
    }

    /// Check if warmup is complete.
    pub fn is_warmed_up(&self) -> bool {
        self.events_processed >= self.config.warmup_events
    }

    /// Get the PST node count (for diagnostics).
    pub fn pst_node_count(&self) -> usize {
        self.pst.node_count()
    }

    /// Get the confidence threshold.
    pub fn confidence_threshold(&self) -> f64 {
        self.config.confidence_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_simple_pmc() -> PatternMarkovChain {
        // Simple 3-state NFA: Start --(A)--> S1 --(B)--> Accept
        let event_types = vec!["A".to_string(), "B".to_string()];
        let pst_config = PSTConfig {
            max_depth: 3,
            smoothing: 0.01,
        };
        let pmc_config = PMCConfig {
            warmup_events: 5,
            confidence_threshold: 0.5,
            ..Default::default()
        };

        let mut transitions = vec![FxHashMap::default(); 3];
        // State 0 (start) -> State 1 on symbol 0 (A)
        transitions[0].insert(0, 1);
        // State 1 -> State 2 (accept) on symbol 1 (B)
        transitions[1].insert(1, 2);

        PatternMarkovChain::new(
            &event_types,
            transitions,
            vec![2], // accept state
            3,
            pst_config,
            pmc_config,
        )
    }

    #[test]
    fn test_deterministic_pattern() {
        let mut pmc = make_simple_pmc();

        // Train with deterministic A -> B pattern
        let runs = vec![RunSnapshot {
            current_state: 1,
            started_at_ns: 0,
        }];

        // Warmup with alternating A, B
        for i in 0..10 {
            let event_type = if i % 2 == 0 { "A" } else { "B" };
            pmc.process(event_type, i * 1_000_000_000, &[]);
        }

        // After warmup, with an active run at state 1, should forecast
        let result = pmc.process("A", 10_000_000_000, &runs);
        assert!(result.is_some(), "Should produce forecast after warmup");
        let forecast = result.unwrap();
        assert!(
            forecast.probability > 0.0,
            "Probability should be > 0, got {}",
            forecast.probability
        );
        // Verify conformal interval fields are present
        assert!(
            forecast.forecast_lower <= forecast.forecast_upper,
            "lower ({}) should be <= upper ({})",
            forecast.forecast_lower,
            forecast.forecast_upper
        );
    }

    #[test]
    fn test_warmup_suppression() {
        let mut pmc = make_simple_pmc();
        let runs = vec![RunSnapshot {
            current_state: 1,
            started_at_ns: 0,
        }];

        // During warmup (< 5 events), should return None
        for i in 0..4 {
            let result = pmc.process("A", i * 1_000_000_000, &runs);
            assert!(
                result.is_none(),
                "Should suppress during warmup (event {})",
                i
            );
        }

        // After warmup (>= 5 events), should produce forecast
        let result = pmc.process("A", 4_000_000_000, &runs);
        assert!(result.is_some(), "Should produce forecast after warmup");
    }

    #[test]
    fn test_no_active_runs() {
        let mut pmc = make_simple_pmc();

        // Process events without active runs
        for i in 0..10 {
            let result = pmc.process("A", i * 1_000_000_000, &[]);
            assert!(result.is_none(), "No forecast without active runs");
        }
    }

    #[test]
    fn test_horizon_effect() {
        let mut pmc = make_simple_pmc();
        let runs = vec![RunSnapshot {
            current_state: 1,
            started_at_ns: 0,
        }];

        // Warmup
        for i in 0..10 {
            pmc.process("A", i * 1_000_000_000, &[]);
        }

        // Forecast with active run
        let result = pmc.process("A", 10_000_000_000, &runs).unwrap();
        assert!(
            result.expected_time_ns > 0 || result.probability > 0.9,
            "Should have positive expected time or near-certain probability"
        );
    }

    #[test]
    fn test_hawkes_integration_no_panic() {
        let mut pmc = make_simple_pmc();

        // Send a burst of events and verify Hawkes modulation doesn't panic
        let runs = vec![RunSnapshot {
            current_state: 1,
            started_at_ns: 0,
        }];

        // Rapid burst: events 1ms apart
        for i in 0..100 {
            let event_type = if i % 2 == 0 { "A" } else { "B" };
            pmc.process(event_type, i * 1_000_000, &runs);
        }

        // Should still produce valid forecasts after burst
        let result = pmc.process("A", 100_000_000, &runs);
        assert!(result.is_some(), "Should produce forecast after burst");
        let forecast = result.unwrap();
        assert!(
            forecast.probability.is_finite(),
            "Probability should be finite after burst"
        );
        assert!(
            forecast.forecast_lower.is_finite() && forecast.forecast_upper.is_finite(),
            "Conformal bounds should be finite after burst"
        );
    }

    #[test]
    fn test_conformal_fields_populated() {
        let mut pmc = make_simple_pmc();

        let runs = vec![RunSnapshot {
            current_state: 1,
            started_at_ns: 0,
        }];

        // Warmup
        for i in 0..10 {
            let event_type = if i % 2 == 0 { "A" } else { "B" };
            pmc.process(event_type, i * 1_000_000_000, &[]);
        }

        let result = pmc.process("A", 10_000_000_000, &runs).unwrap();

        // Before any outcome data, conformal interval should be (0.0, 1.0)
        assert!(
            result.forecast_lower >= 0.0,
            "forecast_lower should be >= 0.0"
        );
        assert!(
            result.forecast_upper <= 1.0,
            "forecast_upper should be <= 1.0"
        );
        assert!(
            result.forecast_lower <= result.forecast_upper,
            "lower ({}) <= upper ({})",
            result.forecast_lower,
            result.forecast_upper
        );
    }

    #[test]
    fn test_outcome_tracking() {
        let mut pmc = make_simple_pmc();

        // Warmup
        for i in 0..10 {
            let event_type = if i % 2 == 0 { "A" } else { "B" };
            pmc.process(event_type, i * 1_000_000_000, &[]);
        }

        // Process with an active run
        let runs = vec![RunSnapshot {
            current_state: 1,
            started_at_ns: 100,
        }];
        pmc.process("A", 10_000_000_000, &runs);

        // Run disappears (completed — was at accept state 2 equivalent scenario)
        // Simulate by passing empty runs — the run disappeared
        pmc.process("B", 11_000_000_000, &[]);

        // The conformal calibrator should have recorded an outcome
        assert!(
            pmc.conformal.sample_count() > 0 || pmc.pending_forecasts.is_empty(),
            "After run disappears, either outcome recorded or no pending forecast"
        );
    }

    #[test]
    fn test_hawkes_disabled_no_modulation() {
        // PMC with hawkes_enabled=false should produce the same probability regardless of burst
        let event_types = vec!["A".to_string(), "B".to_string()];
        let pst_config = PSTConfig {
            max_depth: 3,
            smoothing: 0.01,
        };
        let pmc_config = PMCConfig {
            warmup_events: 5,
            confidence_threshold: 0.5,
            hawkes_enabled: false,
            ..Default::default()
        };

        let mut transitions = vec![FxHashMap::default(); 3];
        transitions[0].insert(0, 1);
        transitions[1].insert(1, 2);

        let mut pmc = PatternMarkovChain::new(
            &event_types,
            transitions,
            vec![2],
            3,
            pst_config,
            pmc_config,
        );

        let runs = vec![RunSnapshot {
            current_state: 1,
            started_at_ns: 0,
        }];

        // Warmup with steady events 1s apart
        for i in 0..10 {
            let event_type = if i % 2 == 0 { "A" } else { "B" };
            pmc.process(event_type, i * 1_000_000_000, &[]);
        }

        // Get baseline probability
        let result_steady = pmc.process("A", 10_000_000_000, &runs).unwrap();

        // Now send a rapid burst (1ms apart) — with Hawkes disabled, probability should be similar
        for i in 0..50 {
            let event_type = if i % 2 == 0 { "A" } else { "B" };
            pmc.process(event_type, 11_000_000_000 + i * 1_000_000, &[]);
        }
        let result_burst = pmc.process("A", 11_050_000_000, &runs).unwrap();

        // Without Hawkes, probability should be stable (no burst boost)
        let diff = (result_burst.probability - result_steady.probability).abs();
        assert!(
            diff < 0.3,
            "Without Hawkes, burst should not dramatically change probability: steady={}, burst={}, diff={}",
            result_steady.probability, result_burst.probability, diff
        );

        // Verify hawkes_intensities map is empty when disabled
        assert!(
            pmc.hawkes_intensities.is_empty(),
            "Hawkes intensities should not be populated when disabled"
        );
    }

    #[test]
    fn test_conformal_disabled_full_interval() {
        let event_types = vec!["A".to_string(), "B".to_string()];
        let pst_config = PSTConfig {
            max_depth: 3,
            smoothing: 0.01,
        };
        let pmc_config = PMCConfig {
            warmup_events: 5,
            confidence_threshold: 0.5,
            conformal_enabled: false,
            ..Default::default()
        };

        let mut transitions = vec![FxHashMap::default(); 3];
        transitions[0].insert(0, 1);
        transitions[1].insert(1, 2);

        let mut pmc = PatternMarkovChain::new(
            &event_types,
            transitions,
            vec![2],
            3,
            pst_config,
            pmc_config,
        );

        let runs = vec![RunSnapshot {
            current_state: 1,
            started_at_ns: 0,
        }];

        // Warmup
        for i in 0..10 {
            let event_type = if i % 2 == 0 { "A" } else { "B" };
            pmc.process(event_type, i * 1_000_000_000, &[]);
        }

        let result = pmc.process("A", 10_000_000_000, &runs).unwrap();

        // With conformal disabled, interval should always be (0.0, 1.0)
        assert!(
            (result.forecast_lower - 0.0).abs() < 1e-10,
            "forecast_lower should be 0.0 when conformal disabled, got {}",
            result.forecast_lower
        );
        assert!(
            (result.forecast_upper - 1.0).abs() < 1e-10,
            "forecast_upper should be 1.0 when conformal disabled, got {}",
            result.forecast_upper
        );

        // pending_forecasts should remain empty
        assert!(
            pmc.pending_forecasts.is_empty(),
            "pending_forecasts should be empty when conformal disabled"
        );
    }
}
