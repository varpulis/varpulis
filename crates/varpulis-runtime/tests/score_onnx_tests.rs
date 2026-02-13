//! E2E tests for the `.score()` ONNX model inference operator.
//!
//! These tests require the `onnx` feature and a working ONNX Runtime library.
//! Run with:
//!   ORT_DYLIB_PATH=/path/to/libonnxruntime.so cargo test -p varpulis-runtime --features onnx -- score_onnx
//!
//! The test model `fraud_scorer.onnx` is a logistic regression trained on synthetic
//! fraud data with 3 input features (amount, velocity, distance) and 1 output
//! (fraud_probability). It was created from a scikit-learn LogisticRegression
//! exported via skl2onnx.
//!
//! Model source: trained on synthetic data, exported with:
//!   sklearn.linear_model.LogisticRegression → skl2onnx → ONNX opset 13
//!   Coefficients: [0.0189, 0.4091, 0.1198], Intercept: -8.7583
//!   Architecture: Gemm → Sigmoid (single node logistic regression)

#![cfg(feature = "onnx")]

use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

const MODEL_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/fraud_scorer.onnx"
);

/// Helper: parse VPL, create engine, process events, collect outputs.
async fn run_score_scenario(program_source: &str, events: Vec<Event>) -> Vec<Event> {
    let (tx, mut rx) = mpsc::channel::<Event>(100);

    let program = parse(program_source).expect("Failed to parse program");
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load program");

    for event in events {
        engine
            .process(event)
            .await
            .expect("Failed to process event");
    }

    let mut results = Vec::new();
    while let Ok(event) = rx.try_recv() {
        results.push(event);
    }
    results
}

// =============================================================================
// Basic scoring tests
// =============================================================================

#[tokio::test]
async fn test_score_enriches_event_with_fraud_probability() {
    let program = format!(
        r#"stream ScoredTrades = TradeEvent
    .score(model: "{}", inputs: [amount, velocity, distance], outputs: [fraud_prob])
    .emit(amount: amount, fraud_prob: fraud_prob)"#,
        MODEL_PATH
    );

    let events = vec![
        // Normal transaction — should get low fraud_prob (~0.003)
        Event::new("TradeEvent")
            .with_field("amount", 50.0f64)
            .with_field("velocity", 2.0f64)
            .with_field("distance", 10.0f64),
        // Suspicious transaction — should get high fraud_prob (~1.0)
        Event::new("TradeEvent")
            .with_field("amount", 5000.0f64)
            .with_field("velocity", 20.0f64)
            .with_field("distance", 800.0f64),
    ];

    let results = run_score_scenario(&program, events).await;

    assert_eq!(results.len(), 2, "Both events should produce output");

    // Normal transaction: fraud_prob should be low
    let normal_prob = results[0]
        .get_float("fraud_prob")
        .expect("fraud_prob should be present");
    assert!(
        normal_prob < 0.1,
        "Normal tx fraud_prob should be < 0.1, got {}",
        normal_prob
    );

    // Suspicious transaction: fraud_prob should be high
    let fraud_prob = results[1]
        .get_float("fraud_prob")
        .expect("fraud_prob should be present");
    assert!(
        fraud_prob > 0.9,
        "Suspicious tx fraud_prob should be > 0.9, got {}",
        fraud_prob
    );
}

#[tokio::test]
async fn test_score_followed_by_where_filter() {
    // Only emit events where fraud_prob > 0.5 (i.e., the suspicious one)
    let program = format!(
        r#"stream FraudAlerts = TradeEvent
    .score(model: "{}", inputs: [amount, velocity, distance], outputs: [fraud_prob])
    .where(fraud_prob > 0.5)
    .emit(alert_type: "fraud", amount: amount, fraud_prob: fraud_prob)"#,
        MODEL_PATH
    );

    let events = vec![
        // Normal — should be filtered out
        Event::new("TradeEvent")
            .with_field("amount", 10.0f64)
            .with_field("velocity", 1.0f64)
            .with_field("distance", 5.0f64),
        // Suspicious — should pass filter
        Event::new("TradeEvent")
            .with_field("amount", 5000.0f64)
            .with_field("velocity", 20.0f64)
            .with_field("distance", 800.0f64),
        // Normal — should be filtered out
        Event::new("TradeEvent")
            .with_field("amount", 30.0f64)
            .with_field("velocity", 1.5f64)
            .with_field("distance", 8.0f64),
    ];

    let results = run_score_scenario(&program, events).await;

    assert_eq!(
        results.len(),
        1,
        "Only the suspicious event should pass the fraud_prob > 0.5 filter"
    );

    let alert = &results[0];
    assert_eq!(
        alert.data.get("alert_type"),
        Some(&varpulis_core::Value::Str("fraud".into()))
    );

    let prob = alert.get_float("fraud_prob").unwrap();
    assert!(
        prob > 0.9,
        "Fraud probability should be > 0.9, got {}",
        prob
    );
}

#[tokio::test]
async fn test_score_with_where_before_score() {
    // Pre-filter events before scoring (only score high-amount trades)
    let program = format!(
        r#"stream HighValueScored = TradeEvent
    .where(amount > 100)
    .score(model: "{}", inputs: [amount, velocity, distance], outputs: [fraud_prob])
    .emit(amount: amount, fraud_prob: fraud_prob)"#,
        MODEL_PATH
    );

    let events = vec![
        // Below threshold — should be filtered before scoring
        Event::new("TradeEvent")
            .with_field("amount", 50.0f64)
            .with_field("velocity", 2.0f64)
            .with_field("distance", 10.0f64),
        // Above threshold — should be scored
        Event::new("TradeEvent")
            .with_field("amount", 500.0f64)
            .with_field("velocity", 8.0f64)
            .with_field("distance", 200.0f64),
    ];

    let results = run_score_scenario(&program, events).await;

    assert_eq!(
        results.len(),
        1,
        "Only the high-amount event should be scored and emitted"
    );

    assert!(
        results[0].get_float("fraud_prob").is_some(),
        "Scored event should have fraud_prob field"
    );
}

#[tokio::test]
async fn test_score_with_integer_inputs() {
    // Test that integer event fields are correctly converted to f32 for the model
    let program = format!(
        r#"stream ScoredInt = TradeEvent
    .score(model: "{}", inputs: [amount, velocity, distance], outputs: [fraud_prob])
    .emit(fraud_prob: fraud_prob)"#,
        MODEL_PATH
    );

    let events = vec![Event::new("TradeEvent")
        .with_field("amount", 5000i64)
        .with_field("velocity", 20i64)
        .with_field("distance", 800i64)];

    let results = run_score_scenario(&program, events).await;

    assert_eq!(results.len(), 1);

    let prob = results[0]
        .get_float("fraud_prob")
        .expect("fraud_prob should exist");
    assert!(
        prob > 0.9,
        "Suspicious integer inputs should still score high, got {}",
        prob
    );
}

#[tokio::test]
async fn test_score_missing_field_graceful_degradation() {
    // Event missing a required field — should pass through without scoring (graceful)
    let program = format!(
        r#"stream Scored = TradeEvent
    .score(model: "{}", inputs: [amount, velocity, distance], outputs: [fraud_prob])
    .emit(amount: amount)"#,
        MODEL_PATH
    );

    let events = vec![
        // Missing 'distance' field
        Event::new("TradeEvent")
            .with_field("amount", 100.0f64)
            .with_field("velocity", 5.0f64),
    ];

    let results = run_score_scenario(&program, events).await;

    // The event should pass through (graceful degradation) but without fraud_prob
    assert_eq!(
        results.len(),
        1,
        "Event should pass through even with missing input field"
    );
}

// =============================================================================
// Identity model tests (test_model.onnx: 2→2 identity)
// =============================================================================

#[tokio::test]
async fn test_score_identity_model() {
    let identity_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/test_model.onnx"
    );

    let program = format!(
        r#"stream Passthrough = DataEvent
    .score(model: "{}", inputs: [x, y], outputs: [out_x, out_y])
    .emit(out_x: out_x, out_y: out_y)"#,
        identity_path
    );

    let events = vec![Event::new("DataEvent")
        .with_field("x", 3.0f64)
        .with_field("y", 7.0f64)];

    let results = run_score_scenario(&program, events).await;

    assert_eq!(results.len(), 1);

    let out_x = results[0]
        .get_float("out_x")
        .expect("out_x should be present");
    let out_y = results[0]
        .get_float("out_y")
        .expect("out_y should be present");

    // Identity model: output ≈ input (Gemm with identity weights)
    assert!(
        (out_x - 3.0).abs() < 0.01,
        "out_x should be ~3.0, got {}",
        out_x
    );
    assert!(
        (out_y - 7.0).abs() < 0.01,
        "out_y should be ~7.0, got {}",
        out_y
    );
}
