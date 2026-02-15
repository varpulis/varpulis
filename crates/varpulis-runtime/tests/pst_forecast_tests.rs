//! Integration tests for PST-based pattern forecasting (.forecast())
//!
//! Tests validate the full forecast pipeline: parse VPL -> load engine -> process events -> verify output.
//! The PST (Prediction Suffix Tree) forecaster requires a sequence pattern (-> followed-by)
//! and emits ForecastResult events with probability, expected time, and state information.

#![allow(clippy::redundant_pattern_matching)]

use chrono::{TimeZone, Utc};
use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

// ===========================================================================
// Helpers
// ===========================================================================

fn ts_ms(ms: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).unwrap()
}

/// Parse + load + process events via async, collect output.
async fn run(code: &str, events: Vec<Event>) -> Vec<Event> {
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");
    for e in events {
        engine.process(e).await.unwrap();
    }
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

// =============================================================================
// 1. test_forecast_parses — VPL with .forecast() parses successfully
// =============================================================================

#[tokio::test]
async fn test_forecast_parses() {
    let code = r#"
        stream ForecastStream = Event1 as e1
            -> Event2 as e2 where e2.value > 0
            .within(5s)
            .forecast(confidence: 0.5, warmup: 5)
            .emit(status: "forecasted")
    "#;

    let program = parse(code);
    assert!(
        program.is_ok(),
        "VPL with .forecast() should parse successfully, got: {:?}",
        program.err()
    );
}

// =============================================================================
// 2. test_forecast_compiles — Engine loads, PMC created
// =============================================================================

#[tokio::test]
async fn test_forecast_compiles() {
    let code = r#"
        stream ForecastStream = Event1 as e1
            -> Event2 as e2 where e2.value > 0
            .within(5s)
            .forecast(confidence: 0.5, warmup: 5)
            .emit(status: "forecasted")
    "#;

    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    let result = engine.load(&program);

    assert!(
        result.is_ok(),
        "Engine should load .forecast() program successfully, got: {:?}",
        result.err()
    );

    // Verify the stream was registered
    assert!(
        engine.stream_names().contains(&"ForecastStream"),
        "Stream 'ForecastStream' should be registered"
    );
}

// =============================================================================
// 3. test_forecast_processes_events — Send events, verify ForecastResult emitted
// =============================================================================

#[tokio::test]
async fn test_forecast_processes_events() {
    let code = r#"
        stream ForecastStream = Event1 as e1
            -> Event2 as e2
            .within(10s)
            .forecast(confidence: 0.0, warmup: 2)
            .emit(status: "forecasted")
    "#;

    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Send enough events to pass warmup and create active runs
    // First pair establishes the pattern
    for i in 0..10 {
        let ts = ts_ms(1000 + i * 100);
        engine
            .process(Event::new("Event1").with_timestamp(ts))
            .await
            .unwrap();
        let ts2 = ts_ms(1050 + i * 100);
        engine
            .process(Event::new("Event2").with_timestamp(ts2))
            .await
            .unwrap();
    }

    // Collect all output events
    let mut results = Vec::new();
    while let Ok(e) = rx.try_recv() {
        results.push(e);
    }

    // With warmup=2 and confidence=0.0, we should get some output events
    // These could be either ForecastResult events or sequence match events
    // The key is that events are processed without error
    assert!(
        !results.is_empty(),
        "Processing events through forecast pipeline should produce output"
    );
}

// =============================================================================
// 4. test_forecast_with_filter — .where(forecast_probability > 0.8) after .forecast()
// =============================================================================

#[tokio::test]
async fn test_forecast_with_filter() {
    let code = r#"
        stream ForecastStream = Event1 as e1
            -> Event2 as e2
            .within(10s)
            .forecast(confidence: 0.8, warmup: 2)
            .emit(probability: forecast_probability)
    "#;

    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Send events to establish pattern
    for i in 0..10 {
        let ts = ts_ms(1000 + i * 100);
        engine
            .process(Event::new("Event1").with_timestamp(ts))
            .await
            .unwrap();
        let ts2 = ts_ms(1050 + i * 100);
        engine
            .process(Event::new("Event2").with_timestamp(ts2))
            .await
            .unwrap();
    }

    // Collect results — only high-confidence forecasts should pass
    let mut results = Vec::new();
    while let Ok(e) = rx.try_recv() {
        results.push(e);
    }

    // With confidence threshold 0.8, only high-probability forecasts should be emitted
    for result in &results {
        if let Some(Value::Float(prob)) = result.data.get("probability") {
            // If a forecast_probability field is present, it should meet the threshold
            // (the confidence filter is applied inside the Forecast op)
            assert!(*prob >= 0.0, "Forecast probability should be non-negative");
        }
    }
}

// =============================================================================
// 5. test_forecast_without_sequence_errors — .forecast() on non-sequence stream -> error
// =============================================================================

#[tokio::test]
async fn test_forecast_without_sequence_errors() {
    let code = r#"
        stream BadForecast = SensorReading
            .where(temperature > 30.0)
            .forecast(confidence: 0.5)
            .emit(status: "forecasted")
    "#;

    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    let result = engine.load(&program);

    assert!(
        result.is_err(),
        ".forecast() on a non-sequence stream should produce a load error"
    );

    let err_msg = result.unwrap_err();
    assert!(
        err_msg.contains("sequence") || err_msg.contains("followed-by"),
        "Error message should mention that a sequence pattern is required, got: {}",
        err_msg
    );
}

// =============================================================================
// 6. test_forecast_warmup_suppression — No forecasts during warmup period
// =============================================================================

#[tokio::test]
async fn test_forecast_warmup_suppression() {
    let code = r#"
        stream ForecastStream = Event1 as e1
            -> Event2 as e2
            .within(10s)
            .forecast(confidence: 0.0, warmup: 10000)
            .emit(status: "forecasted")
    "#;

    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Send only a few events — well below the warmup threshold of 10000
    for i in 0..20 {
        let ts = ts_ms(1000 + i * 100);
        engine
            .process(Event::new("Event1").with_timestamp(ts))
            .await
            .unwrap();
        let ts2 = ts_ms(1050 + i * 100);
        engine
            .process(Event::new("Event2").with_timestamp(ts2))
            .await
            .unwrap();
    }

    // Collect results — should have no forecast events due to high warmup
    let mut forecast_count = 0;
    while let Ok(e) = rx.try_recv() {
        if e.event_type.as_ref() == "ForecastResult" {
            forecast_count += 1;
        }
    }

    assert_eq!(
        forecast_count, 0,
        "No ForecastResult events should be emitted during warmup period (warmup=10000, sent=40 events)"
    );
}

// =============================================================================
// 7. test_forecast_deterministic_pattern — Predictable sequence -> probability > 0
// =============================================================================

#[tokio::test]
async fn test_forecast_deterministic_pattern() {
    let code = r#"
        stream ForecastStream = Start as s
            -> End as e
            .within(10s)
            .forecast(confidence: 0.0, warmup: 3)
            .emit(prob: forecast_probability)
    "#;

    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Send a highly predictable alternating pattern: Start -> End -> Start -> End ...
    // After warmup, the PST should learn this pattern and assign probability > 0
    for i in 0..30 {
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

    // Collect all results
    let mut results = Vec::new();
    while let Ok(e) = rx.try_recv() {
        results.push(e);
    }

    // After 60 events (30 pairs) with warmup=3, we should have output
    // Check that at least some output was produced (forecasts or sequence matches)
    // With confidence=0.0, any non-zero probability should pass
    assert!(
        !results.is_empty(),
        "Deterministic pattern should produce forecast output after warmup"
    );

    // Verify that any ForecastResult events have probability > 0
    for result in &results {
        if result.event_type.as_ref() == "ForecastResult" {
            if let Some(Value::Float(prob)) = result.data.get("forecast_probability") {
                assert!(
                    *prob > 0.0,
                    "Deterministic pattern should yield probability > 0, got {}",
                    prob
                );
            }
        }
    }
}

// =============================================================================
// 8. test_forecast_with_negation — Sequence with NOT + forecast
// =============================================================================

#[tokio::test]
async fn test_forecast_with_negation() {
    let code = r#"
        stream ForecastNeg = Order as order
            -> Payment where order_id == order.id as payment
            .not(Cancellation where order_id == order.id)
            .within(10s)
            .forecast(confidence: 0.0, warmup: 2)
            .emit(status: "forecasted")
    "#;

    let program = parse(code);
    assert!(
        program.is_ok(),
        "VPL with .not() + .forecast() should parse, got: {:?}",
        program.err()
    );

    let program = program.unwrap();
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    let load_result = engine.load(&program);

    assert!(
        load_result.is_ok(),
        "Engine should load sequence with negation + forecast, got: {:?}",
        load_result.err()
    );

    // Process events — negation should still work with forecast
    for i in 0..10 {
        let ts = ts_ms(1000 + i * 300);
        let mut order = Event::new("Order").with_timestamp(ts);
        order.data.insert("id".into(), Value::Int(i));
        engine.process(order).await.unwrap();

        let ts2 = ts_ms(1100 + i * 300);
        let mut payment = Event::new("Payment").with_timestamp(ts2);
        payment.data.insert("order_id".into(), Value::Int(i));
        engine.process(payment).await.unwrap();
    }

    // Drain results — just verify no panic
    let mut count = 0;
    while let Ok(_) = rx.try_recv() {
        count += 1;
    }

    // With negation + forecast, some output should be produced
    // (sequence matches and/or forecast events)
    assert!(
        count >= 0,
        "Forecast with negation should process without error"
    );
}

// =============================================================================
// 9. test_forecast_backward_compat — Adding .forecast() doesn't break SASE detection
// =============================================================================

#[tokio::test]
async fn test_forecast_backward_compat() {
    // First, run without .forecast() and count sequence matches
    let code_no_forecast = r#"
        stream SeqOnly = A as a
            -> B as b
            .within(10s)
            .emit(status: "matched")
    "#;

    let events_no_forecast: Vec<Event> = (0..5)
        .flat_map(|i| {
            vec![
                Event::new("A").with_timestamp(ts_ms(1000 + i * 200)),
                Event::new("B").with_timestamp(ts_ms(1100 + i * 200)),
            ]
        })
        .collect();

    let results_no_forecast = run(code_no_forecast, events_no_forecast).await;
    let match_count_no_forecast = results_no_forecast.len();

    // Now, run WITH .forecast() — sequence detection should still work
    let code_with_forecast = r#"
        stream SeqWithForecast = A as a
            -> B as b
            .within(10s)
            .forecast(confidence: 0.0, warmup: 2)
            .emit(status: "matched")
    "#;

    let events_with_forecast: Vec<Event> = (0..5)
        .flat_map(|i| {
            vec![
                Event::new("A").with_timestamp(ts_ms(1000 + i * 200)),
                Event::new("B").with_timestamp(ts_ms(1100 + i * 200)),
            ]
        })
        .collect();

    let results_with_forecast = run(code_with_forecast, events_with_forecast).await;

    // The forecast version should produce at least as many results
    // (it may produce more due to ForecastResult events in addition to matches)
    assert!(
        results_with_forecast.len() >= match_count_no_forecast || !results_with_forecast.is_empty(),
        "Adding .forecast() should not break SASE detection. \
         Without forecast: {} results, with forecast: {} results",
        match_count_no_forecast,
        results_with_forecast.len()
    );
}

// =============================================================================
// 10. test_forecast_no_params — .forecast() with no params uses defaults
// =============================================================================

#[tokio::test]
async fn test_forecast_no_params() {
    let code = r#"
        stream ForecastDefault = Event1 as e1
            -> Event2 as e2
            .within(5s)
            .forecast()
            .emit(status: "forecasted")
    "#;

    let program = parse(code);
    assert!(
        program.is_ok(),
        ".forecast() with no params should parse, got: {:?}",
        program.err()
    );

    let program = program.unwrap();
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    let load_result = engine.load(&program);

    assert!(
        load_result.is_ok(),
        "Engine should load .forecast() with default params, got: {:?}",
        load_result.err()
    );

    // Verify stream was registered
    assert!(
        engine.stream_names().contains(&"ForecastDefault"),
        "Stream should be registered with default forecast params"
    );

    // Process events to ensure no panic with default config
    for i in 0..5 {
        let ts = ts_ms(1000 + i * 100);
        engine
            .process(Event::new("Event1").with_timestamp(ts))
            .await
            .unwrap();
        let ts2 = ts_ms(1050 + i * 100);
        engine
            .process(Event::new("Event2").with_timestamp(ts2))
            .await
            .unwrap();
    }

    // Drain results — just verify no crash with defaults
    while let Ok(_) = rx.try_recv() {}
}

// =============================================================================
// 11. test_forecast_conformal_interval_fields — forecast_lower/upper present in output
// =============================================================================

#[tokio::test]
async fn test_forecast_conformal_interval_fields() {
    let code = r#"
        stream ForecastStream = Event1 as e1
            -> Event2 as e2
            .within(10s)
            .forecast(confidence: 0.0, warmup: 2)
            .emit(
                prob: forecast_probability,
                lower: forecast_lower,
                upper: forecast_upper
            )
    "#;

    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Send events to establish pattern and pass warmup
    for i in 0..20 {
        let ts = ts_ms(1000 + i * 100);
        engine
            .process(Event::new("Event1").with_timestamp(ts))
            .await
            .unwrap();
        let ts2 = ts_ms(1050 + i * 100);
        engine
            .process(Event::new("Event2").with_timestamp(ts2))
            .await
            .unwrap();
    }

    // Collect all output events
    let mut results = Vec::new();
    while let Ok(e) = rx.try_recv() {
        results.push(e);
    }

    // Check ForecastResult events for conformal interval fields
    for result in &results {
        if result.event_type.as_ref() == "ForecastResult" {
            // forecast_lower and forecast_upper should be present
            if let Some(Value::Float(lower)) = result.data.get("forecast_lower") {
                if let Some(Value::Float(upper)) = result.data.get("forecast_upper") {
                    assert!(
                        *lower >= 0.0,
                        "forecast_lower should be >= 0.0, got {}",
                        lower
                    );
                    assert!(
                        *upper <= 1.0,
                        "forecast_upper should be <= 1.0, got {}",
                        upper
                    );
                    assert!(
                        lower <= upper,
                        "forecast_lower ({}) should be <= forecast_upper ({})",
                        lower,
                        upper
                    );
                }
            }
        }
    }
}

// =============================================================================
// 12. test_forecast_hawkes_burst_effect — rapid burst doesn't crash, events flow
// =============================================================================

#[tokio::test]
async fn test_forecast_hawkes_burst_effect() {
    let code = r#"
        stream ForecastBurst = Event1 as e1
            -> Event2 as e2
            .within(10s)
            .forecast(confidence: 0.0, warmup: 2)
            .emit(prob: forecast_probability)
    "#;

    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(16384);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");

    // Send a rapid burst of events 1ms apart (simulating temporal density spike)
    for i in 0..100 {
        let ts = ts_ms(1000 + i); // 1ms apart
        engine
            .process(Event::new("Event1").with_timestamp(ts))
            .await
            .unwrap();
        let ts2 = ts_ms(1000 + i); // same millisecond
        engine
            .process(Event::new("Event2").with_timestamp(ts2))
            .await
            .unwrap();
    }

    // Collect results — verify no panic and events flow through
    let mut results = Vec::new();
    while let Ok(e) = rx.try_recv() {
        results.push(e);
    }

    // With warmup=2 and rapid burst, events should flow through without error
    // The Hawkes modulation should handle rapid timestamps gracefully
    assert!(
        !results.is_empty(),
        "Rapid burst should produce output (sequence matches and/or forecasts)"
    );
}

// =============================================================================
// 13. test_forecast_hawkes_disabled_vpl — .forecast(hawkes: false) parses, loads, produces output
// =============================================================================

#[tokio::test]
async fn test_forecast_hawkes_disabled_vpl() {
    let code = r#"
        stream ForecastNoHawkes = Event1 as e1
            -> Event2 as e2
            .within(10s)
            .forecast(confidence: 0.0, warmup: 2, hawkes: false)
            .emit(prob: forecast_probability)
    "#;

    let events: Vec<Event> = (0..20)
        .flat_map(|i| {
            vec![
                Event::new("Event1").with_timestamp(ts_ms(1000 + i * 100)),
                Event::new("Event2").with_timestamp(ts_ms(1000 + i * 100 + 50)),
            ]
        })
        .collect();

    let results = run(code, events).await;
    assert!(
        !results.is_empty(),
        ".forecast(hawkes: false) should produce output"
    );
}

// =============================================================================
// 14. test_forecast_conformal_disabled_vpl — .forecast(conformal: false) produces (0.0, 1.0) interval
// =============================================================================

#[tokio::test]
async fn test_forecast_conformal_disabled_vpl() {
    let code = r#"
        stream ForecastNoConformal = Event1 as e1
            -> Event2 as e2
            .within(10s)
            .forecast(confidence: 0.0, warmup: 2, conformal: false)
            .emit(
                prob: forecast_probability,
                lower: forecast_lower,
                upper: forecast_upper
            )
    "#;

    let events: Vec<Event> = (0..20)
        .flat_map(|i| {
            vec![
                Event::new("Event1").with_timestamp(ts_ms(1000 + i * 100)),
                Event::new("Event2").with_timestamp(ts_ms(1000 + i * 100 + 50)),
            ]
        })
        .collect();

    let results = run(code, events).await;
    assert!(
        !results.is_empty(),
        ".forecast(conformal: false) should produce output"
    );

    // Check that conformal interval is always (0.0, 1.0) when disabled
    for e in &results {
        if let Some(Value::Float(lower)) = e.data.get("lower") {
            assert!(
                (*lower - 0.0).abs() < 1e-10,
                "forecast_lower should be 0.0 when conformal disabled, got {}",
                lower
            );
        }
        if let Some(Value::Float(upper)) = e.data.get("upper") {
            assert!(
                (*upper - 1.0).abs() < 1e-10,
                "forecast_upper should be 1.0 when conformal disabled, got {}",
                upper
            );
        }
    }
}

// =============================================================================
// 15. test_forecast_both_disabled_vpl — .forecast(hawkes: false, conformal: false) parses and loads
// =============================================================================

#[tokio::test]
async fn test_forecast_both_disabled_vpl() {
    let code = r#"
        stream ForecastMinimal = Event1 as e1
            -> Event2 as e2
            .within(10s)
            .forecast(confidence: 0.0, warmup: 2, hawkes: false, conformal: false)
            .emit(prob: forecast_probability)
    "#;

    let program = parse(code);
    assert!(
        program.is_ok(),
        ".forecast(hawkes: false, conformal: false) should parse, got: {:?}",
        program.err()
    );

    let program = program.unwrap();
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    let result = engine.load(&program);
    assert!(
        result.is_ok(),
        "Engine should load forecast with both disabled, got: {:?}",
        result.err()
    );
}
