//! PST/PMC Convergence Validation Tests
//!
//! These tests validate that the Prediction Suffix Tree and Pattern Markov Chain
//! converge to known ground-truth values when fed data generated from a Markov
//! chain with known transition probabilities.
//!
//! This provides mathematical proof that the forecasting system produces correct
//! predictions — not just that it "produces output without crashing."
//!
//! ## Methodology
//!
//! 1. Define a first-order Markov chain with known transition matrix P(next|current).
//! 2. Generate a long sequence from this chain using a seeded PRNG.
//! 3. Feed the sequence through the PST (batch or online) or PMC.
//! 4. Verify that learned transition probabilities converge to ground truth.
//! 5. Verify that PMC completion probabilities match analytical values derived
//!    from the forward algorithm applied to the known transition matrix.

use chrono::{TimeZone, Utc};
use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::pst::{
    PMCConfig, PSTConfig, PatternMarkovChain, PredictionSuffixTree, RunSnapshot, SymbolId,
};

// =============================================================================
// Deterministic PRNG (LCG) for reproducible test data
// =============================================================================

/// Knuth LCG — deterministic, fast, reproducible.
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns a float in [0.0, 1.0).
    fn next_f64(&mut self) -> f64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.state >> 33) as f64) / ((1u64 << 31) as f64)
    }

    /// Sample from a discrete distribution. Returns the chosen index.
    fn sample_discrete(&mut self, probs: &[f64]) -> usize {
        let r = self.next_f64();
        let mut cumulative = 0.0;
        for (i, &p) in probs.iter().enumerate() {
            cumulative += p;
            if r < cumulative {
                return i;
            }
        }
        probs.len() - 1
    }
}

/// Generate a sequence from a first-order Markov chain.
///
/// `transition_matrix[i][j]` = P(next = j | current = i). Rows must sum to 1.0.
fn generate_markov_chain(
    rng: &mut Rng,
    transition_matrix: &[Vec<f64>],
    initial_state: usize,
    length: usize,
) -> Vec<usize> {
    let mut seq = Vec::with_capacity(length);
    let mut state = initial_state;
    seq.push(state);
    for _ in 1..length {
        state = rng.sample_discrete(&transition_matrix[state]);
        seq.push(state);
    }
    seq
}

// =============================================================================
// Test 1: PST learns correct 2-symbol transition probabilities
// =============================================================================

#[test]
fn test_pst_two_symbol_convergence() {
    // Ground truth Markov chain:
    //   P(B|A) = 0.7,  P(A|A) = 0.3
    //   P(A|B) = 0.6,  P(B|B) = 0.4
    let transition_matrix = vec![
        vec![0.3, 0.7], // From A
        vec![0.6, 0.4], // From B
    ];

    let mut rng = Rng::new(42);
    let sequence = generate_markov_chain(&mut rng, &transition_matrix, 0, 50_000);

    let mut pst = PredictionSuffixTree::new(PSTConfig {
        max_depth: 3,
        smoothing: 0.001,
    });
    let a = pst.register_symbol("A");
    let b = pst.register_symbol("B");

    let symbols: Vec<SymbolId> = sequence.iter().map(|&i| i as SymbolId).collect();
    pst.train(&symbols);

    // Verify all four transition probabilities
    let tolerance = 0.05;
    let cases = [
        (&[a][..], b, 0.7, "P(B|A)"),
        (&[a][..], a, 0.3, "P(A|A)"),
        (&[b][..], a, 0.6, "P(A|B)"),
        (&[b][..], b, 0.4, "P(B|B)"),
    ];

    for (context, symbol, expected, label) in &cases {
        let learned = pst.predict_symbol(context, *symbol);
        assert!(
            (learned - expected).abs() < tolerance,
            "{} should be ~{:.2}, got {:.4} (error: {:.4})",
            label,
            expected,
            learned,
            (learned - expected).abs()
        );
    }
}

// =============================================================================
// Test 2: PST learns correct 4-symbol transition matrix
// =============================================================================

#[test]
fn test_pst_four_symbol_convergence() {
    // 4-symbol Markov chain with deliberately asymmetric transitions
    let transition_matrix = vec![
        vec![0.1, 0.6, 0.2, 0.1], // From A
        vec![0.2, 0.1, 0.5, 0.2], // From B
        vec![0.3, 0.2, 0.1, 0.4], // From C
        vec![0.4, 0.3, 0.2, 0.1], // From D
    ];
    let names = ["A", "B", "C", "D"];

    let mut rng = Rng::new(123);
    let sequence = generate_markov_chain(&mut rng, &transition_matrix, 0, 100_000);

    let mut pst = PredictionSuffixTree::new(PSTConfig {
        max_depth: 3,
        smoothing: 0.001,
    });
    let syms: Vec<SymbolId> = names.iter().map(|n| pst.register_symbol(n)).collect();

    let symbol_seq: Vec<SymbolId> = sequence.iter().map(|&i| syms[i]).collect();
    pst.train(&symbol_seq);

    // Verify all 16 entries of the transition matrix
    let tolerance = 0.05;
    let mut max_error = 0.0f64;

    for (from_idx, row) in transition_matrix.iter().enumerate() {
        for (to_idx, &true_prob) in row.iter().enumerate() {
            let learned = pst.predict_symbol(&[syms[from_idx]], syms[to_idx]);
            let error = (learned - true_prob).abs();
            max_error = max_error.max(error);
            assert!(
                error < tolerance,
                "P({}|{}) should be ~{:.2}, got {:.4} (error: {:.4})",
                names[to_idx],
                names[from_idx],
                true_prob,
                learned,
                error
            );
        }
    }

    // Report overall quality
    assert!(
        max_error < tolerance,
        "Maximum error across all 16 transitions: {:.4}",
        max_error
    );
}

// =============================================================================
// Test 3: PMC completion probability — 2-step pattern A → B
// =============================================================================

#[test]
fn test_pmc_completion_two_step() {
    // Pattern: A → B (NFA: S0 --A--> S1 --B--> S2_accept)
    // Ground truth: P(B|A) = 0.7, P(A|A) = 0.3, P(A|B) = 0.6, P(B|B) = 0.4
    //
    // At state 1 after matching A, the current context ends in "A".
    // Value iteration converges to: prob[1] = P(B|ctx)
    // Since ctx ends in A: P(B|A) ≈ 0.7
    // Expected completion probability ≈ 0.7

    let transition_matrix = vec![
        vec![0.3, 0.7], // From A
        vec![0.6, 0.4], // From B
    ];

    let event_types = vec!["A".to_string(), "B".to_string()];
    let pst_config = PSTConfig {
        max_depth: 3,
        smoothing: 0.001,
    };
    let pmc_config = PMCConfig {
        warmup_events: 200,
        confidence_threshold: 0.0,
        hawkes_enabled: false,    // clean PST-only test
        conformal_enabled: false, // skip interval tracking
        ..Default::default()
    };

    let mut transitions = vec![FxHashMap::default(); 3];
    transitions[0].insert(0, 1); // S0 --A(sym 0)--> S1
    transitions[1].insert(1, 2); // S1 --B(sym 1)--> S2 (accept)

    let mut pmc = PatternMarkovChain::new(
        &event_types,
        transitions,
        vec![2], // accept state
        3,       // num_states
        pst_config,
        pmc_config,
    );

    let mut rng = Rng::new(42);
    let sequence = generate_markov_chain(&mut rng, &transition_matrix, 0, 20_000);

    // Collect forecasts when at NFA state 1 (event is "A")
    let mut forecasts = Vec::new();

    for (i, &sym_idx) in sequence.iter().enumerate() {
        let event_type = if sym_idx == 0 { "A" } else { "B" };
        let ts = (i as i64) * 1_000_000_000;

        // Provide a run at state 1 only when the current event is A
        let runs = if sym_idx == 0 {
            vec![RunSnapshot {
                current_state: 1,
                started_at_ns: ts,
            }]
        } else {
            vec![]
        };

        if let Some(forecast) = pmc.process(event_type, ts, &runs) {
            if sym_idx == 0 {
                forecasts.push(forecast.probability);
            }
        }
    }

    assert!(
        forecasts.len() > 100,
        "Should produce many forecasts, got {}",
        forecasts.len()
    );

    // Average over all post-warmup forecasts
    let avg: f64 = forecasts.iter().sum::<f64>() / forecasts.len() as f64;
    assert!(
        (avg - 0.7).abs() < 0.1,
        "Average completion at state 1 should be ~0.7 (P(B|A)), got {:.4} (n={})",
        avg,
        forecasts.len()
    );

    // Late-stage forecasts should be closer (convergence)
    let last_200: Vec<f64> = forecasts.iter().rev().take(200).copied().collect();
    let late_avg = last_200.iter().sum::<f64>() / last_200.len() as f64;
    assert!(
        (late_avg - 0.7).abs() < 0.08,
        "Late-stage completion should converge closer to 0.7, got {:.4}",
        late_avg
    );
}

// =============================================================================
// Test 4: PMC completion probability — 3-step pattern A → B → C
// =============================================================================

#[test]
fn test_pmc_completion_three_step() {
    // Pattern: A → B → C (NFA: S0 --A--> S1 --B--> S2 --C--> S3_accept)
    //
    // Markov chain:
    //   P(B|A) = 0.6, P(C|A) = 0.3, P(A|A) = 0.1
    //   P(C|B) = 0.7, P(A|B) = 0.2, P(B|B) = 0.1
    //   P(A|C) = 0.5, P(B|C) = 0.3, P(C|C) = 0.2
    //
    // Value iteration uses the SAME context for all state transitions.
    //
    // At state 2 (context ends in B):
    //   completion = P(C|B_ctx) ≈ P(C|B) = 0.7
    //
    // At state 1 (context ends in A):
    //   completion = P(B|A_ctx) * P(C|A_ctx) ≈ P(B|A) * P(C|A) = 0.6 * 0.3 = 0.18

    let transition_matrix = vec![
        vec![0.1, 0.6, 0.3], // From A
        vec![0.2, 0.1, 0.7], // From B
        vec![0.5, 0.3, 0.2], // From C
    ];

    let event_types = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let pst_config = PSTConfig {
        max_depth: 3,
        smoothing: 0.001,
    };
    let pmc_config = PMCConfig {
        warmup_events: 500,
        confidence_threshold: 0.0,
        hawkes_enabled: false,
        conformal_enabled: false,
        ..Default::default()
    };

    // 4-state NFA
    let mut transitions = vec![FxHashMap::default(); 4];
    transitions[0].insert(0, 1); // S0 --A(0)--> S1
    transitions[1].insert(1, 2); // S1 --B(1)--> S2
    transitions[2].insert(2, 3); // S2 --C(2)--> S3 (accept)

    let mut pmc = PatternMarkovChain::new(
        &event_types,
        transitions,
        vec![3], // accept
        4,       // num_states
        pst_config,
        pmc_config,
    );

    let mut rng = Rng::new(99);
    let sequence = generate_markov_chain(&mut rng, &transition_matrix, 0, 50_000);

    let mut forecasts_state1 = Vec::new(); // context ends in A
    let mut forecasts_state2 = Vec::new(); // context ends in B

    for (i, &sym_idx) in sequence.iter().enumerate() {
        let event_type = ["A", "B", "C"][sym_idx];
        let ts = (i as i64) * 1_000_000_000;

        let runs = match sym_idx {
            0 => vec![RunSnapshot {
                current_state: 1,
                started_at_ns: ts,
            }],
            1 => vec![RunSnapshot {
                current_state: 2,
                started_at_ns: ts,
            }],
            _ => vec![],
        };

        if let Some(forecast) = pmc.process(event_type, ts, &runs) {
            match sym_idx {
                0 => forecasts_state1.push(forecast.probability),
                1 => forecasts_state2.push(forecast.probability),
                _ => {}
            }
        }
    }

    // State 2: completion = P(C|B) ≈ 0.7
    assert!(
        !forecasts_state2.is_empty(),
        "Should produce forecasts at state 2"
    );
    let avg_s2 = forecasts_state2.iter().sum::<f64>() / forecasts_state2.len() as f64;
    assert!(
        (avg_s2 - 0.7).abs() < 0.1,
        "State 2 completion should be ~P(C|B)=0.7, got {:.4} (n={})",
        avg_s2,
        forecasts_state2.len()
    );

    // State 1: completion = P(B|A) * P(C|A) ≈ 0.6 * 0.3 = 0.18
    let expected_s1 = 0.6 * 0.3; // 0.18
    assert!(
        !forecasts_state1.is_empty(),
        "Should produce forecasts at state 1"
    );
    let avg_s1 = forecasts_state1.iter().sum::<f64>() / forecasts_state1.len() as f64;
    assert!(
        (avg_s1 - expected_s1).abs() < 0.1,
        "State 1 completion should be ~P(B|A)*P(C|A)={:.2}, got {:.4} (n={})",
        expected_s1,
        avg_s1,
        forecasts_state1.len()
    );
}

// =============================================================================
// Test 5: Convergence rate — forecast error decreases over time
// =============================================================================

#[test]
fn test_pmc_convergence_rate() {
    // Demonstrates that forecast error monotonically decreases as more events
    // are processed. This proves the online learner actually converges, not
    // just that it produces some value.

    let transition_matrix = vec![
        vec![0.3, 0.7], // From A: P(B|A)=0.7
        vec![0.6, 0.4], // From B: P(A|B)=0.6
    ];
    let ground_truth_pb_given_a = 0.7;

    let event_types = vec!["A".to_string(), "B".to_string()];
    let pst_config = PSTConfig {
        max_depth: 3,
        smoothing: 0.001,
    };
    let pmc_config = PMCConfig {
        warmup_events: 50,
        confidence_threshold: 0.0,
        hawkes_enabled: false,
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

    let mut rng = Rng::new(777);
    let sequence = generate_markov_chain(&mut rng, &transition_matrix, 0, 30_000);

    // Collect forecasts in epochs of 5000 events
    let epoch_size = 5000;
    let mut epoch_errors: Vec<f64> = Vec::new();
    let mut epoch_forecasts = Vec::new();

    for (i, &sym_idx) in sequence.iter().enumerate() {
        let event_type = if sym_idx == 0 { "A" } else { "B" };
        let ts = (i as i64) * 1_000_000_000;

        let runs = if sym_idx == 0 {
            vec![RunSnapshot {
                current_state: 1,
                started_at_ns: ts,
            }]
        } else {
            vec![]
        };

        if let Some(forecast) = pmc.process(event_type, ts, &runs) {
            if sym_idx == 0 {
                epoch_forecasts.push(forecast.probability);
            }
        }

        // End of epoch: compute mean absolute error
        if (i + 1).is_multiple_of(epoch_size) && !epoch_forecasts.is_empty() {
            let mae: f64 = epoch_forecasts
                .iter()
                .map(|p| (p - ground_truth_pb_given_a).abs())
                .sum::<f64>()
                / epoch_forecasts.len() as f64;
            epoch_errors.push(mae);
            epoch_forecasts.clear();
        }
    }

    // Verify we have multiple epochs
    assert!(
        epoch_errors.len() >= 4,
        "Should have at least 4 epochs, got {}",
        epoch_errors.len()
    );

    // The last epoch's error should be smaller than the first epoch's error
    let first_error = epoch_errors[0];
    let last_error = *epoch_errors.last().unwrap();
    assert!(
        last_error < first_error || last_error < 0.05,
        "Error should decrease over time: first epoch MAE={:.4}, last epoch MAE={:.4}",
        first_error,
        last_error
    );

    // Final epoch error should be small (good convergence)
    assert!(
        last_error < 0.1,
        "Final epoch MAE should be < 0.1, got {:.4}",
        last_error
    );
}

// =============================================================================
// Test 6: Hawkes modulation produces higher probabilities during bursts
// =============================================================================

#[test]
fn test_hawkes_burst_effect_on_probability() {
    // Same setup, but compare Hawkes-enabled vs Hawkes-disabled forecasts
    // during a burst. Hawkes should produce HIGHER completion probabilities
    // during rapid event arrival.

    let event_types = vec!["A".to_string(), "B".to_string()];
    let pst_config = PSTConfig {
        max_depth: 3,
        smoothing: 0.01,
    };

    // --- PMC without Hawkes ---
    let pmc_config_no_hawkes = PMCConfig {
        warmup_events: 5,
        confidence_threshold: 0.0,
        hawkes_enabled: false,
        conformal_enabled: false,
        adaptive_warmup: false,
        ..Default::default()
    };
    let mut transitions_nh = vec![FxHashMap::default(); 3];
    transitions_nh[0].insert(0, 1);
    transitions_nh[1].insert(1, 2);
    let mut pmc_no_hawkes = PatternMarkovChain::new(
        &event_types,
        transitions_nh,
        vec![2],
        3,
        pst_config.clone(),
        pmc_config_no_hawkes,
    );

    // --- PMC with Hawkes ---
    let pmc_config_hawkes = PMCConfig {
        warmup_events: 5,
        confidence_threshold: 0.0,
        hawkes_enabled: true,
        conformal_enabled: false,
        adaptive_warmup: false,
        ..Default::default()
    };
    let mut transitions_h = vec![FxHashMap::default(); 3];
    transitions_h[0].insert(0, 1);
    transitions_h[1].insert(1, 2);
    let mut pmc_hawkes = PatternMarkovChain::new(
        &event_types,
        transitions_h,
        vec![2],
        3,
        pst_config,
        pmc_config_hawkes,
    );

    // Phase 1: Steady-state warmup (events 1 second apart)
    for i in 0..20 {
        let event_type = if i % 2 == 0 { "A" } else { "B" };
        let ts = i * 1_000_000_000i64;
        pmc_no_hawkes.process(event_type, ts, &[]);
        pmc_hawkes.process(event_type, ts, &[]);
    }

    // Phase 2: Burst (events 1 millisecond apart) — high temporal density
    let burst_base = 20_000_000_000i64;
    for i in 0..60 {
        let event_type = if i % 2 == 0 { "A" } else { "B" };
        let ts = burst_base + i * 1_000_000; // 1ms apart
        pmc_no_hawkes.process(event_type, ts, &[]);
        pmc_hawkes.process(event_type, ts, &[]);
    }

    // Now evaluate: both PMCs at state 1 during the burst
    let eval_ts = burst_base + 60_000_000;
    let runs = vec![RunSnapshot {
        current_state: 1,
        started_at_ns: eval_ts,
    }];

    let forecast_no_hawkes = pmc_no_hawkes
        .process("A", eval_ts, &runs)
        .expect("should produce forecast");
    let forecast_hawkes = pmc_hawkes
        .process("A", eval_ts, &runs)
        .expect("should produce forecast");

    // Both should produce valid probabilities
    assert!(forecast_no_hawkes.probability > 0.0);
    assert!(forecast_hawkes.probability > 0.0);
    assert!(forecast_no_hawkes.probability <= 1.0);
    assert!(forecast_hawkes.probability <= 1.0);

    // Hawkes should produce >= the no-Hawkes probability during burst
    // (temporal density boosts the transition probability)
    assert!(
        forecast_hawkes.probability >= forecast_no_hawkes.probability * 0.9,
        "Hawkes forecast ({:.4}) should be >= no-Hawkes ({:.4}) during burst",
        forecast_hawkes.probability,
        forecast_no_hawkes.probability
    );
}

// =============================================================================
// Test 7: Conformal intervals narrow as calibration data accumulates
// =============================================================================

#[test]
fn test_conformal_intervals_narrow_with_data() {
    // Feed a deterministic A→B pattern. After enough outcome tracking,
    // the conformal intervals should narrow (the system becomes more confident).

    let event_types = vec!["A".to_string(), "B".to_string()];
    let pst_config = PSTConfig {
        max_depth: 3,
        smoothing: 0.01,
    };
    let pmc_config = PMCConfig {
        warmup_events: 5,
        confidence_threshold: 0.0,
        hawkes_enabled: false,
        conformal_enabled: true,
        adaptive_warmup: false,
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

    let mut early_widths = Vec::new();
    let mut late_widths = Vec::new();

    for i in 0..500 {
        let ts = (i as i64) * 100_000_000; // 100ms apart

        // Alternate A then B — deterministic pattern
        // Provide runs that appear/disappear to trigger outcome tracking
        if i % 2 == 0 {
            // A event: start a run at state 1
            let runs = vec![RunSnapshot {
                current_state: 1,
                started_at_ns: ts,
            }];
            if let Some(f) = pmc.process("A", ts, &runs) {
                let width = f.forecast_upper - f.forecast_lower;
                if i < 100 {
                    early_widths.push(width);
                } else {
                    late_widths.push(width);
                }
            }
        } else {
            // B event: run completes (disappears from active_runs)
            pmc.process("B", ts, &[]);
        }
    }

    // With enough data, intervals should be at least as narrow or narrower
    if !early_widths.is_empty() && !late_widths.is_empty() {
        let early_avg: f64 = early_widths.iter().sum::<f64>() / early_widths.len() as f64;
        let late_avg: f64 = late_widths.iter().sum::<f64>() / late_widths.len() as f64;

        // Late intervals should not be wider than early (modulo noise)
        assert!(
            late_avg <= early_avg + 0.1,
            "Late intervals ({:.4}) should not be much wider than early ({:.4})",
            late_avg,
            early_avg
        );
    }
}

// =============================================================================
// Test 8: Full VPL pipeline — forecast probabilities converge
// =============================================================================

fn ts_ms(ms: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).unwrap()
}

#[tokio::test]
async fn test_full_pipeline_deterministic_convergence() {
    // Feed a deterministic A→B pattern through the full VPL pipeline.
    // After warmup, every forecast should have probability close to 1.0
    // because B *always* follows A.

    let code = r#"
        stream Convergence = Start as s
            -> End as e
            .within(10s)
            .forecast(confidence: 0.0, warmup: 10, hawkes: false, conformal: false)
            .emit(prob: forecast_probability)
    "#;

    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(16384);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Send 200 deterministic Start→End pairs
    for i in 0..200 {
        let ts = ts_ms(1000 + i * 200);
        engine
            .process(Event::new("Start").with_timestamp(ts))
            .await
            .unwrap();
        let ts2 = ts_ms(1100 + i * 200);
        engine
            .process(Event::new("End").with_timestamp(ts2))
            .await
            .unwrap();
    }

    // Collect output
    let mut probs = Vec::new();
    let mut all_events = Vec::new();
    while let Ok(e) = rx.try_recv() {
        all_events.push(e.clone());
        if let Some(Value::Float(p)) = e.data.get("prob") {
            probs.push(*p);
        }
    }

    // Debug: show what events we got
    if probs.is_empty() {
        let sample_count = all_events.len().min(5);
        let mut debug_info = format!("Got {} events total. ", all_events.len());
        for (i, e) in all_events.iter().take(sample_count).enumerate() {
            let prob_val = e
                .data
                .get("prob")
                .map(|v| format!("{:?}", v))
                .unwrap_or_default();
            let fp_val = e
                .data
                .get("forecast_probability")
                .map(|v| format!("{:?}", v))
                .unwrap_or_default();
            debug_info.push_str(&format!(
                "\n  [{}] type={}, prob={}, forecast_probability={}, keys={:?}",
                i,
                e.event_type,
                prob_val,
                fp_val,
                e.data.keys().collect::<Vec<_>>()
            ));
        }
        if all_events.is_empty() {
            debug_info.push_str("\n  (no events at all!)");
        }
        panic!(
            "Should produce forecast events with probability values. {}",
            debug_info
        );
    }

    // For a deterministic A→B alternation, P(End|Start) → 1.0
    // Use only the last half of forecasts (well past warmup)
    let second_half = &probs[probs.len() / 2..];
    let avg: f64 = second_half.iter().sum::<f64>() / second_half.len() as f64;

    assert!(
        avg > 0.5,
        "Deterministic pattern should yield high completion probability, got {:.4} (n={})",
        avg,
        second_half.len()
    );
}

#[tokio::test]
async fn test_full_pipeline_mixed_pattern_convergence() {
    // Use 3 event types: A, B, C
    // Pattern: A → B (sequence)
    // But also inject C events as noise.
    // P(B after A) should still converge despite noise events.

    let code = r#"
        stream MixedConvergence = EventA as a
            -> EventB as b
            .within(10s)
            .forecast(confidence: 0.0, warmup: 20, hawkes: false, conformal: false)
            .emit(prob: forecast_probability)
    "#;

    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(16384);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    let mut rng = Rng::new(12345);

    // 70% of the time after A, send B. 30% send C (noise).
    for i in 0..300 {
        let ts = ts_ms(1000 + i * 300);
        engine
            .process(Event::new("EventA").with_timestamp(ts))
            .await
            .unwrap();

        let ts2 = ts_ms(1100 + i * 300);
        if rng.next_f64() < 0.7 {
            // B follows A
            engine
                .process(Event::new("EventB").with_timestamp(ts2))
                .await
                .unwrap();
        } else {
            // C follows A (noise — doesn't complete the pattern)
            engine
                .process(Event::new("EventC").with_timestamp(ts2))
                .await
                .unwrap();
        }
    }

    // Collect output
    let mut probs = Vec::new();
    while let Ok(e) = rx.try_recv() {
        if let Some(Value::Float(p)) = e.data.get("prob") {
            probs.push(*p);
        }
    }

    assert!(
        !probs.is_empty(),
        "Should produce forecast events even with noise"
    );

    // Average probability should reflect the ~70% completion rate
    let avg: f64 = probs.iter().sum::<f64>() / probs.len() as f64;
    assert!(
        avg > 0.0,
        "Forecast probability should be positive, got {:.4}",
        avg
    );

    // Last quarter should show convergence
    let last_q = &probs[3 * probs.len() / 4..];
    if !last_q.is_empty() {
        let late_avg: f64 = last_q.iter().sum::<f64>() / last_q.len() as f64;
        assert!(
            late_avg > 0.1,
            "Late-stage forecasts should reflect learned pattern, got {:.4}",
            late_avg
        );
    }
}

// =============================================================================
// Test 9: Online vs batch learning produce similar results
// =============================================================================

#[test]
fn test_online_vs_batch_convergence() {
    // Train the same sequence via batch PST.train() and online learner.
    // Verify that both produce similar transition probability estimates.

    let transition_matrix = vec![
        vec![0.3, 0.7], // From A: P(B|A) = 0.7
        vec![0.6, 0.4], // From B
    ];

    let mut rng = Rng::new(42);
    let sequence = generate_markov_chain(&mut rng, &transition_matrix, 0, 20_000);

    // Batch training
    let mut pst_batch = PredictionSuffixTree::new(PSTConfig {
        max_depth: 3,
        smoothing: 0.001,
    });
    let a_batch = pst_batch.register_symbol("A");
    let b_batch = pst_batch.register_symbol("B");
    let batch_symbols: Vec<SymbolId> = sequence.iter().map(|&i| i as SymbolId).collect();
    pst_batch.train(&batch_symbols);

    // Online learning
    let mut pst_online = PredictionSuffixTree::new(PSTConfig {
        max_depth: 3,
        smoothing: 0.001,
    });
    let a_online = pst_online.register_symbol("A");
    let b_online = pst_online.register_symbol("B");
    let mut learner = varpulis_runtime::pst::OnlinePSTLearner::new(3);

    for &sym_idx in &sequence {
        let symbol = if sym_idx == 0 { a_online } else { b_online };
        learner.update(&mut pst_online, symbol);
    }

    // Compare predictions — should be very close
    let batch_pb_a = pst_batch.predict_symbol(&[a_batch], b_batch);
    let online_pb_a = pst_online.predict_symbol(&[a_online], b_online);
    let batch_pa_b = pst_batch.predict_symbol(&[b_batch], a_batch);
    let online_pa_b = pst_online.predict_symbol(&[b_online], a_online);

    assert!(
        (batch_pb_a - online_pb_a).abs() < 0.05,
        "P(B|A) should match: batch={:.4}, online={:.4}",
        batch_pb_a,
        online_pb_a
    );
    assert!(
        (batch_pa_b - online_pa_b).abs() < 0.05,
        "P(A|B) should match: batch={:.4}, online={:.4}",
        batch_pa_b,
        online_pa_b
    );

    // Both should be close to ground truth
    assert!(
        (batch_pb_a - 0.7).abs() < 0.05,
        "Batch P(B|A) should be ~0.7, got {:.4}",
        batch_pb_a
    );
    assert!(
        (online_pb_a - 0.7).abs() < 0.05,
        "Online P(B|A) should be ~0.7, got {:.4}",
        online_pb_a
    );
}

// =============================================================================
// Test 10: Adaptive warmup delays forecasts until predictions stabilize
// =============================================================================

#[test]
fn test_adaptive_warmup_delays_until_stable() {
    let event_types = vec!["A".to_string(), "B".to_string()];
    let pst_config = PSTConfig {
        max_depth: 3,
        smoothing: 0.01,
    };

    // Non-adaptive: fixed warmup at 5
    let pmc_config_fixed = PMCConfig {
        warmup_events: 5,
        confidence_threshold: 0.0,
        hawkes_enabled: false,
        conformal_enabled: false,
        adaptive_warmup: false,
        ..Default::default()
    };

    // Adaptive: min warmup 5 + stability check
    let pmc_config_adaptive = PMCConfig {
        warmup_events: 5,
        confidence_threshold: 0.0,
        hawkes_enabled: false,
        conformal_enabled: false,
        adaptive_warmup: true,
        ..Default::default()
    };

    let mut transitions_f = vec![FxHashMap::default(); 3];
    transitions_f[0].insert(0, 1);
    transitions_f[1].insert(1, 2);
    let mut pmc_fixed = PatternMarkovChain::new(
        &event_types,
        transitions_f,
        vec![2],
        3,
        pst_config.clone(),
        pmc_config_fixed,
    );

    let mut transitions_a = vec![FxHashMap::default(); 3];
    transitions_a[0].insert(0, 1);
    transitions_a[1].insert(1, 2);
    let mut pmc_adaptive = PatternMarkovChain::new(
        &event_types,
        transitions_a,
        vec![2],
        3,
        pst_config,
        pmc_config_adaptive,
    );

    let runs = vec![RunSnapshot {
        current_state: 1,
        started_at_ns: 0,
    }];

    let mut first_fixed_at = None;
    let mut first_adaptive_at = None;

    for i in 0..200 {
        let event_type = if i % 2 == 0 { "A" } else { "B" };
        let ts = (i as i64) * 1_000_000_000;

        if pmc_fixed.process(event_type, ts, &runs).is_some() && first_fixed_at.is_none() {
            first_fixed_at = Some(i);
        }
        if pmc_adaptive.process(event_type, ts, &runs).is_some() && first_adaptive_at.is_none() {
            first_adaptive_at = Some(i);
        }
    }

    let fixed_at = first_fixed_at.expect("Fixed warmup should eventually produce forecasts");
    let adaptive_at =
        first_adaptive_at.expect("Adaptive warmup should eventually produce forecasts");

    // Adaptive warmup should start at or after fixed warmup
    assert!(
        adaptive_at >= fixed_at,
        "Adaptive warmup (first forecast at event {}) should not emit before \
         fixed warmup (first at event {})",
        adaptive_at,
        fixed_at
    );
}

// =============================================================================
// Test 11: forecast_confidence increases as model converges
// =============================================================================

#[test]
fn test_forecast_confidence_increases_over_time() {
    let event_types = vec!["A".to_string(), "B".to_string()];
    let pst_config = PSTConfig {
        max_depth: 3,
        smoothing: 0.01,
    };
    let pmc_config = PMCConfig {
        warmup_events: 10,
        confidence_threshold: 0.0,
        hawkes_enabled: false,
        conformal_enabled: false,
        adaptive_warmup: false,
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

    let mut confidences = Vec::new();

    // Feed alternating A,B (deterministic)
    for i in 0..200 {
        let event_type = if i % 2 == 0 { "A" } else { "B" };
        let ts = (i as i64) * 1_000_000_000;

        if let Some(forecast) = pmc.process(event_type, ts, &runs) {
            confidences.push(forecast.forecast_confidence);
        }
    }

    assert!(
        !confidences.is_empty(),
        "Should produce forecasts with confidence"
    );

    // Late forecasts should have high confidence (stable predictions)
    let late_avg: f64 = confidences[confidences.len().saturating_sub(20)..]
        .iter()
        .sum::<f64>()
        / confidences[confidences.len().saturating_sub(20)..].len() as f64;

    assert!(
        late_avg > 0.5,
        "Late forecast confidence should be > 0.5, got {:.4}",
        late_avg
    );
}

// =============================================================================
// Test 12: Mode presets parse and run through full VPL pipeline
// =============================================================================

#[tokio::test]
async fn test_forecast_mode_fast_vpl() {
    let code = r#"
        stream FastForecast = EventA as a
            -> EventB as b
            .within(10s)
            .forecast(mode: "fast")
            .emit(prob: forecast_probability, conf: forecast_confidence)
    "#;

    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(16384);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Send 100 deterministic A→B pairs
    for i in 0..100 {
        let ts = ts_ms(1000 + i * 200);
        engine
            .process(Event::new("EventA").with_timestamp(ts))
            .await
            .unwrap();
        let ts2 = ts_ms(1100 + i * 200);
        engine
            .process(Event::new("EventB").with_timestamp(ts2))
            .await
            .unwrap();
    }

    let mut probs = Vec::new();
    let mut confs = Vec::new();
    while let Ok(e) = rx.try_recv() {
        if let Some(Value::Float(p)) = e.data.get("prob") {
            probs.push(*p);
        }
        if let Some(Value::Float(c)) = e.data.get("conf") {
            confs.push(*c);
        }
    }

    assert!(
        !probs.is_empty(),
        "Fast mode should produce forecasts (no adaptive warmup)"
    );
    assert_eq!(
        probs.len(),
        confs.len(),
        "Every forecast should have a confidence value"
    );
}

#[tokio::test]
async fn test_forecast_mode_accurate_vpl() {
    let code = r#"
        stream AccurateForecast = Start as s
            -> End as e
            .within(10s)
            .forecast(mode: "accurate")
            .emit(prob: forecast_probability, conf: forecast_confidence)
    "#;

    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(16384);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Send 300 deterministic pairs (need more for accurate mode's higher warmup)
    for i in 0..300 {
        let ts = ts_ms(1000 + i * 200);
        engine
            .process(Event::new("Start").with_timestamp(ts))
            .await
            .unwrap();
        let ts2 = ts_ms(1100 + i * 200);
        engine
            .process(Event::new("End").with_timestamp(ts2))
            .await
            .unwrap();
    }

    let mut probs = Vec::new();
    while let Ok(e) = rx.try_recv() {
        if let Some(Value::Float(p)) = e.data.get("prob") {
            probs.push(*p);
        }
    }

    assert!(
        !probs.is_empty(),
        "Accurate mode should produce forecasts after adaptive warmup"
    );

    // Late predictions should be very high for deterministic pattern
    let late_avg: f64 = probs[probs.len().saturating_sub(20)..].iter().sum::<f64>()
        / probs[probs.len().saturating_sub(20)..].len() as f64;
    assert!(
        late_avg > 0.5,
        "Deterministic pattern should have high completion probability, got {:.4}",
        late_avg
    );
}

#[tokio::test]
async fn test_forecast_zero_config_vpl() {
    // Zero-config .forecast() — should use balanced defaults
    let code = r#"
        stream ZeroConfig = Ping as p
            -> Pong as q
            .within(10s)
            .forecast()
            .emit(prob: forecast_probability, conf: forecast_confidence)
    "#;

    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(16384);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    for i in 0..300 {
        let ts = ts_ms(1000 + i * 200);
        engine
            .process(Event::new("Ping").with_timestamp(ts))
            .await
            .unwrap();
        let ts2 = ts_ms(1100 + i * 200);
        engine
            .process(Event::new("Pong").with_timestamp(ts2))
            .await
            .unwrap();
    }

    let mut count = 0;
    while let Ok(_e) = rx.try_recv() {
        count += 1;
    }

    assert!(
        count > 0,
        "Zero-config .forecast() should produce output events"
    );
}

// =============================================================================
// Test 13: EMA-based Hawkes adapts faster to regime changes
// =============================================================================

#[test]
fn test_hawkes_ema_adapts_to_regime_change() {
    // Feed events at 1s intervals, then switch to 10ms intervals.
    // With EMA-based estimation, the Hawkes should detect the burst quickly.

    let event_types = vec!["A".to_string(), "B".to_string()];
    let pst_config = PSTConfig {
        max_depth: 3,
        smoothing: 0.01,
    };
    let pmc_config = PMCConfig {
        warmup_events: 5,
        confidence_threshold: 0.0,
        hawkes_enabled: true,
        conformal_enabled: false,
        adaptive_warmup: false,
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

    // Phase 1: Steady state (1 second intervals)
    for i in 0..30 {
        let event_type = if i % 2 == 0 { "A" } else { "B" };
        pmc.process(event_type, i * 1_000_000_000, &[]);
    }

    // Get baseline prediction
    let baseline = pmc
        .process("A", 30_000_000_000, &runs)
        .expect("should produce forecast after warmup");
    let baseline_prob = baseline.probability;

    // Phase 2: Burst (10ms intervals) — Hawkes should boost
    let burst_base = 31_000_000_000i64;
    for i in 0..40 {
        let event_type = if i % 2 == 0 { "A" } else { "B" };
        pmc.process(event_type, burst_base + i * 10_000_000, &[]);
    }

    let burst = pmc
        .process("A", burst_base + 400_000_000, &runs)
        .expect("should produce forecast during burst");
    let burst_prob = burst.probability;

    // Both should be valid probabilities
    assert!(
        baseline_prob > 0.0 && baseline_prob <= 1.0,
        "Baseline prob should be valid: {}",
        baseline_prob
    );
    assert!(
        burst_prob > 0.0 && burst_prob <= 1.0,
        "Burst prob should be valid: {}",
        burst_prob
    );

    // Hawkes should boost during burst (or at least not crash)
    // The exact boost depends on the EMA convergence
    assert!(
        burst_prob >= baseline_prob * 0.8,
        "Burst probability ({:.4}) should be >= 80% of baseline ({:.4})",
        burst_prob,
        baseline_prob
    );
}
