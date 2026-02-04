//! Integration tests for context-based multi-threaded execution.
//!
//! Tests validate that the ContextOrchestrator correctly:
//! - Isolates streams per context
//! - Routes events to the correct context
//! - Forwards cross-context events
//! - Maintains backward compatibility with no-context programs

use std::sync::Arc;
use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::ContextOrchestrator;

/// Helper: parse program, build orchestrator, send events, collect output.
/// Waits briefly for async processing in context threads.
async fn run_context_scenario(program_source: &str, events: Vec<Event>) -> Vec<Event> {
    let program = parse(program_source).expect("Failed to parse program");

    let (output_tx, mut output_rx) = mpsc::channel::<Event>(1000);

    // Load program into a temporary engine to get context_map
    let (tmp_tx, _tmp_rx) = mpsc::channel(100);
    let mut tmp_engine = Engine::new(tmp_tx);
    tmp_engine.load(&program).expect("Failed to load program");

    if !tmp_engine.has_contexts() {
        // No contexts — run directly through engine
        let (tx, mut rx) = mpsc::channel::<Event>(1000);
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
        return results;
    }

    let orchestrator =
        ContextOrchestrator::build(tmp_engine.context_map(), &program, output_tx, 1000)
            .expect("Failed to build orchestrator");

    // Send all events through the orchestrator
    for event in events {
        orchestrator
            .process(Arc::new(event))
            .await
            .expect("Failed to process event");
    }

    // Allow time for async processing in context threads
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Shut down orchestrator — use spawn_blocking since shutdown() calls
    // thread::join() which is blocking and would stall the Tokio runtime
    tokio::task::spawn_blocking(move || {
        orchestrator.shutdown();
    })
    .await
    .expect("Shutdown task panicked");

    // Collect all output events
    let mut results = Vec::new();
    while let Ok(event) = output_rx.try_recv() {
        results.push(event);
    }

    results
}

// =============================================================================
// Single Context Basic
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_single_context_basic() {
    let program = r#"
        context ingest

        stream HighTemp = SensorReading
            .context(ingest)
            .where(temperature > 100.0)
            .emit(sensor: sensor_id, temp: temperature)
    "#;

    let events = vec![
        Event::new("SensorReading")
            .with_field("sensor_id", "S1")
            .with_field("temperature", 105.5),
        Event::new("SensorReading")
            .with_field("sensor_id", "S2")
            .with_field("temperature", 95.0), // below threshold
        Event::new("SensorReading")
            .with_field("sensor_id", "S3")
            .with_field("temperature", 110.0),
    ];

    let results = run_context_scenario(program, events).await;

    assert_eq!(
        results.len(),
        2,
        "Should emit 2 events (S1 and S3 above threshold)"
    );
    assert_eq!(results[0].event_type, "HighTemp");
    assert_eq!(results[0].get_str("sensor"), Some("S1"));
}

// =============================================================================
// Two Context Pipeline
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_two_context_pipeline() {
    let program = r#"
        context ingest
        context analytics

        stream Filtered = SensorReading
            .context(ingest)
            .where(temperature > 50.0)
            .emit(sensor: sensor_id, temp: temperature)

        stream Analysis = Filtered
            .context(analytics)
            .where(temp > 100.0)
            .emit(alert_sensor: sensor, alert_temp: temp)
    "#;

    let events = vec![
        Event::new("SensorReading")
            .with_field("sensor_id", "S1")
            .with_field("temperature", 105.5),
        Event::new("SensorReading")
            .with_field("sensor_id", "S2")
            .with_field("temperature", 75.0), // passes Filtered but not Analysis
        Event::new("SensorReading")
            .with_field("sensor_id", "S3")
            .with_field("temperature", 30.0), // filtered out by Filtered
    ];

    let results = run_context_scenario(program, events).await;

    // Filtered should emit for S1 and S2 (both > 50)
    // Analysis should emit only for S1 (temp > 100)
    let filtered_events: Vec<_> = results
        .iter()
        .filter(|e| e.event_type == "Filtered")
        .collect();
    let analysis_events: Vec<_> = results
        .iter()
        .filter(|e| e.event_type == "Analysis")
        .collect();

    assert_eq!(filtered_events.len(), 2, "Filtered should emit 2 events");
    assert_eq!(analysis_events.len(), 1, "Analysis should emit 1 event");
    assert_eq!(analysis_events[0].get_str("alert_sensor"), Some("S1"));
}

// =============================================================================
// Three Context Chain
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_three_context_chain() {
    let program = r#"
        context ingest
        context compute
        context alert

        stream Raw = SensorReading
            .context(ingest)
            .where(temperature > 0.0)
            .emit(sensor: sensor_id, temp: temperature)

        stream Computed = Raw
            .context(compute)
            .where(temp > 50.0)
            .emit(device: sensor, value: temp)

        stream Alert = Computed
            .context(alert)
            .where(value > 100.0)
            .emit(critical_device: device, critical_value: value)
    "#;

    let events = vec![
        Event::new("SensorReading")
            .with_field("sensor_id", "S1")
            .with_field("temperature", 150.0),
        Event::new("SensorReading")
            .with_field("sensor_id", "S2")
            .with_field("temperature", 75.0),
    ];

    let results = run_context_scenario(program, events).await;

    let alert_events: Vec<_> = results.iter().filter(|e| e.event_type == "Alert").collect();
    assert_eq!(
        alert_events.len(),
        1,
        "Only S1 should trigger alert (150 > 100)"
    );
    assert_eq!(alert_events[0].get_str("critical_device"), Some("S1"));
}

// =============================================================================
// Context Isolation
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_context_isolation() {
    let program = r#"
        context ctx1
        context ctx2

        stream StreamA = EventA
            .context(ctx1)
            .where(value > 10)
            .emit(result: value)

        stream StreamB = EventB
            .context(ctx2)
            .where(score > 5)
            .emit(output: score)
    "#;

    let events = vec![
        Event::new("EventA").with_field("value", 20),
        Event::new("EventB").with_field("score", 8),
        Event::new("EventA").with_field("value", 5), // below threshold
        Event::new("EventB").with_field("score", 3), // below threshold
    ];

    let results = run_context_scenario(program, events).await;

    let stream_a_results: Vec<_> = results
        .iter()
        .filter(|e| e.event_type == "StreamA")
        .collect();
    let stream_b_results: Vec<_> = results
        .iter()
        .filter(|e| e.event_type == "StreamB")
        .collect();

    assert_eq!(
        stream_a_results.len(),
        1,
        "StreamA should process only EventA events"
    );
    assert_eq!(
        stream_b_results.len(),
        1,
        "StreamB should process only EventB events"
    );
}

// =============================================================================
// No Context Backward Compatibility
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_no_context_backward_compat() {
    let program = r#"
        stream HighTemp = SensorReading
            .where(temperature > 100.0)
            .emit(sensor: sensor_id, temp: temperature)
    "#;

    let events = vec![
        Event::new("SensorReading")
            .with_field("sensor_id", "S1")
            .with_field("temperature", 105.5),
        Event::new("SensorReading")
            .with_field("sensor_id", "S2")
            .with_field("temperature", 95.0),
    ];

    let results = run_context_scenario(program, events).await;

    assert_eq!(results.len(), 1, "Should emit 1 event (S1 above threshold)");
    assert_eq!(results[0].event_type, "HighTemp");
}

// =============================================================================
// Context with Window Aggregate
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_context_with_window_aggregate() {
    let program = r#"
        context compute

        stream AvgTemp = SensorReading
            .context(compute)
            .window(3)
            .aggregate(avg_temp: avg(temperature), count: count())
            .emit(average: avg_temp, total: count)
    "#;

    let events = vec![
        Event::new("SensorReading").with_field("temperature", 100.0),
        Event::new("SensorReading").with_field("temperature", 200.0),
        Event::new("SensorReading").with_field("temperature", 300.0), // triggers window
    ];

    let results = run_context_scenario(program, events).await;

    assert_eq!(
        results.len(),
        1,
        "Should emit 1 aggregation event after 3 events"
    );
    assert_eq!(results[0].event_type, "AvgTemp");

    // Average of 100, 200, 300 = 200
    if let Some(avg) = results[0].get_float("average") {
        assert!(
            (avg - 200.0).abs() < 0.01,
            "Average should be 200.0, got {}",
            avg
        );
    }
}

// =============================================================================
// Context with .to() Connector
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_context_with_to_connector() {
    // This tests that .to() operations work within a context.
    // We use a console connector which won't fail.
    let program = r#"
        context output_ctx

        connector Console = console ()

        stream Alerts = SensorReading
            .context(output_ctx)
            .where(temperature > 100.0)
            .emit(sensor: sensor_id, temp: temperature)
            .to(Console)
    "#;

    let events = vec![Event::new("SensorReading")
        .with_field("sensor_id", "S1")
        .with_field("temperature", 110.0)];

    let results = run_context_scenario(program, events).await;

    assert_eq!(results.len(), 1, "Should emit 1 alert event");
    assert_eq!(results[0].event_type, "Alerts");
}
