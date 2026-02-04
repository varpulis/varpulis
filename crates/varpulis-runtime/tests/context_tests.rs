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
use varpulis_runtime::{ContextOrchestrator, DispatchError};

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

        // Flush any remaining session windows after all events are processed
        if engine.has_session_windows() {
            engine
                .flush_expired_sessions()
                .await
                .expect("Failed to flush expired sessions");
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

// =============================================================================
// Parallel Dispatch via Router
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_parallel_dispatch_via_router() {
    let program_source = r#"
        context ctx1
        context ctx2

        stream StreamA = EventA
            .context(ctx1)
            .where(value > 0)
            .emit(result: value)

        stream StreamB = EventB
            .context(ctx2)
            .where(score > 0)
            .emit(output: score)
    "#;

    let program = parse(program_source).expect("Failed to parse program");
    let (output_tx, mut output_rx) = mpsc::channel::<Event>(1000);

    let (tmp_tx, _tmp_rx) = mpsc::channel(100);
    let mut tmp_engine = Engine::new(tmp_tx);
    tmp_engine.load(&program).expect("Failed to load program");

    let orchestrator =
        ContextOrchestrator::build(tmp_engine.context_map(), &program, output_tx, 1000)
            .expect("Failed to build orchestrator");

    let router = orchestrator.router();

    // Spawn multiple concurrent tasks dispatching through cloned router handles
    let mut handles = Vec::new();
    for task_id in 0..4 {
        let router_clone = router.clone();
        let handle = tokio::spawn(async move {
            for i in 0..50 {
                let event_type = if task_id % 2 == 0 { "EventA" } else { "EventB" };
                let field = if task_id % 2 == 0 { "value" } else { "score" };
                let event = Event::new(event_type).with_field(field, i + 1);
                let shared = Arc::new(event);
                match router_clone.dispatch(shared) {
                    Ok(()) => {}
                    Err(DispatchError::ChannelFull(ev)) => {
                        router_clone.dispatch_await(ev).await.unwrap();
                    }
                    Err(DispatchError::ChannelClosed(_)) => break,
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("Task panicked");
    }

    // Allow time for processing
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        orchestrator.shutdown();
    })
    .await
    .expect("Shutdown task panicked");

    let mut results = Vec::new();
    while let Ok(event) = output_rx.try_recv() {
        results.push(event);
    }

    // 4 tasks x 50 events = 200 events dispatched
    // All values > 0 so all should produce output
    assert_eq!(results.len(), 200, "All 200 events should produce output");

    let stream_a: Vec<_> = results
        .iter()
        .filter(|e| e.event_type == "StreamA")
        .collect();
    let stream_b: Vec<_> = results
        .iter()
        .filter(|e| e.event_type == "StreamB")
        .collect();
    assert_eq!(
        stream_a.len(),
        100,
        "Tasks 0 and 2 produce 100 StreamA events"
    );
    assert_eq!(
        stream_b.len(),
        100,
        "Tasks 1 and 3 produce 100 StreamB events"
    );
}

// =============================================================================
// Try Process Non-Blocking
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_try_process_non_blocking() {
    let program_source = r#"
        context ingest

        stream HighTemp = SensorReading
            .context(ingest)
            .where(temperature > 100.0)
            .emit(sensor: sensor_id, temp: temperature)
    "#;

    let program = parse(program_source).expect("Failed to parse program");
    let (output_tx, _output_rx) = mpsc::channel::<Event>(1000);

    let (tmp_tx, _tmp_rx) = mpsc::channel(100);
    let mut tmp_engine = Engine::new(tmp_tx);
    tmp_engine.load(&program).expect("Failed to load program");

    // Use a very small channel capacity to force ChannelFull
    let orchestrator = ContextOrchestrator::build(tmp_engine.context_map(), &program, output_tx, 2)
        .expect("Failed to build orchestrator");

    let mut full_count = 0;
    let mut ok_count = 0;

    // Send many events rapidly — some should succeed, some should return ChannelFull
    for i in 0..100 {
        let event = Event::new("SensorReading")
            .with_field("sensor_id", format!("S{}", i))
            .with_field("temperature", 50.0);
        let shared = Arc::new(event);
        match orchestrator.try_process(shared) {
            Ok(()) => ok_count += 1,
            Err(DispatchError::ChannelFull(ev)) => {
                full_count += 1;
                // Fallback to async dispatch
                orchestrator.process(ev).await.unwrap();
            }
            Err(DispatchError::ChannelClosed(_)) => break,
        }
    }

    // With capacity=2, we should see some ChannelFull returns
    assert!(ok_count > 0, "Some events should succeed via try_send");
    assert!(
        full_count > 0,
        "Some events should return ChannelFull with capacity=2"
    );

    tokio::task::spawn_blocking(move || {
        orchestrator.shutdown();
    })
    .await
    .expect("Shutdown task panicked");
}

// =============================================================================
// Process Batch
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_process_batch() {
    let program_source = r#"
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

    let program = parse(program_source).expect("Failed to parse program");
    let (output_tx, mut output_rx) = mpsc::channel::<Event>(1000);

    let (tmp_tx, _tmp_rx) = mpsc::channel(100);
    let mut tmp_engine = Engine::new(tmp_tx);
    tmp_engine.load(&program).expect("Failed to load program");

    let orchestrator =
        ContextOrchestrator::build(tmp_engine.context_map(), &program, output_tx, 1000)
            .expect("Failed to build orchestrator");

    let events: Vec<_> = vec![
        Arc::new(Event::new("EventA").with_field("value", 20)),
        Arc::new(Event::new("EventB").with_field("score", 8)),
        Arc::new(Event::new("EventA").with_field("value", 5)), // below threshold
        Arc::new(Event::new("EventB").with_field("score", 3)), // below threshold
    ];

    let errors = orchestrator.process_batch(events);
    assert!(
        errors.is_empty(),
        "No dispatch errors expected with large capacity"
    );

    // Allow time for processing
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        orchestrator.shutdown();
    })
    .await
    .expect("Shutdown task panicked");

    let mut results = Vec::new();
    while let Ok(event) = output_rx.try_recv() {
        results.push(event);
    }

    let stream_a: Vec<_> = results
        .iter()
        .filter(|e| e.event_type == "StreamA")
        .collect();
    let stream_b: Vec<_> = results
        .iter()
        .filter(|e| e.event_type == "StreamB")
        .collect();

    assert_eq!(stream_a.len(), 1, "Only EventA with value=20 passes filter");
    assert_eq!(stream_b.len(), 1, "Only EventB with score=8 passes filter");
}

// =============================================================================
// Session Window
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_session_window_basic() {
    let program = r#"
        stream SessionAvg = SensorReading
            .window(session: 5s)
            .aggregate(avg_temp: avg(temperature), count: count())
            .emit(average: avg_temp, total: count)
    "#;

    let base_time = chrono::Utc::now();
    let events = vec![
        // Session 1: t=0, t=1, t=2 (all within 5s gap)
        Event::new("SensorReading")
            .with_timestamp(base_time)
            .with_field("temperature", 100.0),
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(1))
            .with_field("temperature", 200.0),
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(2))
            .with_field("temperature", 300.0),
        // Gap of 6s -> session 1 closes when this event arrives
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(8))
            .with_field("temperature", 400.0),
    ];

    let results = run_context_scenario(program, events).await;

    assert_eq!(
        results.len(),
        1,
        "Should emit 1 aggregation event when session closes"
    );
    assert_eq!(results[0].event_type, "SessionAvg");

    // Average of session 1: (100 + 200 + 300) / 3 = 200
    if let Some(avg) = results[0].get_float("average") {
        assert!(
            (avg - 200.0).abs() < 0.01,
            "Average should be 200.0, got {}",
            avg
        );
    }
    if let Some(count) = results[0].get_int("total") {
        assert_eq!(count, 3, "Session should have 3 events");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_session_window_in_context() {
    let program = r#"
        context compute

        stream SessionData = SensorReading
            .context(compute)
            .window(session: 3s)
            .aggregate(count: count())
            .emit(event_count: count)
    "#;

    let base_time = chrono::Utc::now();
    let events = vec![
        Event::new("SensorReading")
            .with_timestamp(base_time)
            .with_field("temperature", 100.0),
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(1))
            .with_field("temperature", 200.0),
        // Gap of 4s -> session closes
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(5))
            .with_field("temperature", 300.0),
    ];

    let results = run_context_scenario(program, events).await;

    assert_eq!(results.len(), 1, "Should emit 1 session aggregation");
    assert_eq!(results[0].event_type, "SessionData");
    if let Some(count) = results[0].get_int("event_count") {
        assert_eq!(count, 2, "First session should have 2 events");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_session_window_multiple_closures() {
    // Three sessions: events close two sessions, third stays open (not emitted)
    let program = r#"
        stream SessionCount = SensorReading
            .window(session: 3s)
            .aggregate(count: count(), total: sum(temperature))
            .emit(event_count: count, temp_total: total)
    "#;

    let base_time = chrono::Utc::now();
    let events = vec![
        // Session 1: t=0, t=1
        Event::new("SensorReading")
            .with_timestamp(base_time)
            .with_field("temperature", 10.0),
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(1))
            .with_field("temperature", 20.0),
        // Gap of 4s -> session 1 closes
        // Session 2: t=5, t=6
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(5))
            .with_field("temperature", 30.0),
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(6))
            .with_field("temperature", 40.0),
        // Gap of 5s -> session 2 closes
        // Session 3: t=11 (stays open — no subsequent event to trigger closure)
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(11))
            .with_field("temperature", 50.0),
    ];

    let results = run_context_scenario(program, events).await;

    assert_eq!(
        results.len(),
        2,
        "Should emit 2 closed sessions (third stays open)"
    );

    // Session 1: count=2, total=30
    if let Some(count) = results[0].get_int("event_count") {
        assert_eq!(count, 2, "Session 1 should have 2 events");
    }
    if let Some(total) = results[0].get_float("temp_total") {
        assert!(
            (total - 30.0).abs() < 0.01,
            "Session 1 total should be 30.0, got {}",
            total
        );
    }

    // Session 2: count=2, total=70
    if let Some(count) = results[1].get_int("event_count") {
        assert_eq!(count, 2, "Session 2 should have 2 events");
    }
    if let Some(total) = results[1].get_float("temp_total") {
        assert!(
            (total - 70.0).abs() < 0.01,
            "Session 2 total should be 70.0, got {}",
            total
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_session_window_partitioned() {
    // Two partitions with independent session tracking
    let program = r#"
        stream PerSensorSession = SensorReading
            .partition_by(sensor_id)
            .window(session: 3s)
            .aggregate(
                sensor: last(sensor_id),
                count: count(),
                avg_temp: avg(temperature)
            )
            .emit(sensor: sensor, event_count: count, average: avg_temp)
    "#;

    let base_time = chrono::Utc::now();
    let events = vec![
        // S1 session 1: t=0
        Event::new("SensorReading")
            .with_timestamp(base_time)
            .with_field("sensor_id", "S1")
            .with_field("temperature", 100.0),
        // S2 session 1: t=1
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(1))
            .with_field("sensor_id", "S2")
            .with_field("temperature", 200.0),
        // S1 session 1: t=2 (within 3s gap for S1)
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(2))
            .with_field("sensor_id", "S1")
            .with_field("temperature", 120.0),
        // S2 session 1: t=2 (within 3s gap for S2)
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(2))
            .with_field("sensor_id", "S2")
            .with_field("temperature", 220.0),
        // S1: gap of 5s -> S1 session 1 closes when this arrives at t=7
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(7))
            .with_field("sensor_id", "S1")
            .with_field("temperature", 140.0),
        // S2: gap of 5s -> S2 session 1 closes when this arrives at t=7
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(7))
            .with_field("sensor_id", "S2")
            .with_field("temperature", 240.0),
    ];

    let results = run_context_scenario(program, events).await;

    assert_eq!(
        results.len(),
        2,
        "Should emit 2 session closures (one per partition)"
    );

    // Both sessions should have count=2
    for result in &results {
        if let Some(count) = result.get_int("event_count") {
            assert_eq!(count, 2, "Each partition session should have 2 events");
        }
    }

    // Check that both sensors are represented
    let sensors: Vec<String> = results
        .iter()
        .filter_map(|r| r.get_str("sensor").map(|s| s.to_string()))
        .collect();
    assert!(
        sensors.contains(&"S1".to_string()),
        "S1 session should be emitted"
    );
    assert!(
        sensors.contains(&"S2".to_string()),
        "S2 session should be emitted"
    );

    // Verify averages per sensor
    for result in &results {
        let sensor = result.get_str("sensor").unwrap_or_default().to_string();
        let avg = result.get_float("average").unwrap_or(0.0);
        match sensor.as_str() {
            "S1" => assert!(
                (avg - 110.0).abs() < 0.01,
                "S1 avg should be 110.0, got {}",
                avg
            ),
            "S2" => assert!(
                (avg - 210.0).abs() < 0.01,
                "S2 avg should be 210.0, got {}",
                avg
            ),
            _ => panic!("Unexpected sensor: {}", sensor),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_session_window_cross_context() {
    // Session window in one context, receiving events forwarded from another
    let program = r#"
        context ingest
        context sessions

        stream Validated = SensorReading
            .context(ingest)
            .where(temperature > 0.0)
            .emit(context: sessions, sensor_id: sensor_id, temperature: temperature)

        stream SessionStats = Validated
            .context(sessions)
            .window(session: 3s)
            .aggregate(count: count(), avg_temp: avg(temperature))
            .emit(event_count: count, average: avg_temp)
    "#;

    let base_time = chrono::Utc::now();
    let events = vec![
        // Session 1: t=0, t=1, t=2
        Event::new("SensorReading")
            .with_timestamp(base_time)
            .with_field("sensor_id", "S1")
            .with_field("temperature", 100.0),
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(1))
            .with_field("sensor_id", "S1")
            .with_field("temperature", 200.0),
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(2))
            .with_field("sensor_id", "S1")
            .with_field("temperature", 300.0),
        // Invalid reading — filtered by ingest
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(3))
            .with_field("sensor_id", "S1")
            .with_field("temperature", -10.0),
        // Gap of 5s -> session 1 closes when this arrives
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(8))
            .with_field("sensor_id", "S1")
            .with_field("temperature", 400.0),
    ];

    let results = run_context_scenario(program, events).await;

    // Both streams produce output: Validated emits forwarded events,
    // SessionStats emits aggregated sessions. Filter to session results.
    let session_results: Vec<&Event> = results
        .iter()
        .filter(|e| e.event_type == "SessionStats")
        .collect();

    assert_eq!(
        session_results.len(),
        1,
        "Should emit 1 session aggregation (gap exceeded after 3 valid events), got {} total events ({} SessionStats)",
        results.len(),
        session_results.len()
    );

    if let Some(count) = session_results[0].get_int("event_count") {
        assert_eq!(count, 3, "Session should have 3 events (invalid filtered)");
    }
    if let Some(avg) = session_results[0].get_float("average") {
        assert!(
            (avg - 200.0).abs() < 0.01,
            "Average should be 200.0, got {}",
            avg
        );
    }

    // Validated stream should have emitted 4 events (temperature > 0)
    let validated_count = results
        .iter()
        .filter(|e| e.event_type == "Validated")
        .count();
    assert_eq!(
        validated_count, 4,
        "Validated should forward 4 events (one filtered for temp <= 0)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_session_window_partitioned_in_context() {
    // Partitioned session window running inside a context
    let program = r#"
        context analytics

        stream UserSessions = UserActivity
            .context(analytics)
            .partition_by(user_id)
            .window(session: 3s)
            .aggregate(
                user: last(user_id),
                actions: count()
            )
            .emit(user: user, action_count: actions)
    "#;

    let base_time = chrono::Utc::now();
    let events = vec![
        // Alice session 1: t=0, t=1
        Event::new("UserActivity")
            .with_timestamp(base_time)
            .with_field("user_id", "alice")
            .with_field("action", "login"),
        Event::new("UserActivity")
            .with_timestamp(base_time + chrono::Duration::seconds(1))
            .with_field("user_id", "alice")
            .with_field("action", "view"),
        // Bob session 1: t=2
        Event::new("UserActivity")
            .with_timestamp(base_time + chrono::Duration::seconds(2))
            .with_field("user_id", "bob")
            .with_field("action", "login"),
        // Alice gap=5s -> session closes at t=6
        Event::new("UserActivity")
            .with_timestamp(base_time + chrono::Duration::seconds(6))
            .with_field("user_id", "alice")
            .with_field("action", "login"),
        // Bob gap=5s -> session closes at t=7
        Event::new("UserActivity")
            .with_timestamp(base_time + chrono::Duration::seconds(7))
            .with_field("user_id", "bob")
            .with_field("action", "login"),
    ];

    let results = run_context_scenario(program, events).await;

    assert_eq!(results.len(), 2, "Should emit 2 sessions (one per user)");

    let users: Vec<String> = results
        .iter()
        .filter_map(|r| r.get_str("user").map(|s| s.to_string()))
        .collect();
    assert!(
        users.contains(&"alice".to_string()),
        "Alice session should be emitted"
    );
    assert!(
        users.contains(&"bob".to_string()),
        "Bob session should be emitted"
    );

    for result in &results {
        let user = result.get_str("user").unwrap_or_default().to_string();
        let count = result.get_int("action_count").unwrap_or(0);
        match user.as_str() {
            "alice" => assert_eq!(count, 2, "Alice should have 2 actions"),
            "bob" => assert_eq!(count, 1, "Bob should have 1 action"),
            _ => panic!("Unexpected user: {}", user),
        }
    }
}

// =============================================================================
// Session Window Sweep (stale session closure without trailing event)
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_session_window_sweep_closes_stale() {
    // Session with 3 events and no trailing event to close it.
    // The sweep (flush_expired_sessions) should close it after processing.
    let program = r#"
        stream SessionAvg = SensorReading
            .window(session: 5s)
            .aggregate(avg_temp: avg(temperature), count: count())
            .emit(average: avg_temp, total: count)
    "#;

    let base_time = chrono::Utc::now() - chrono::Duration::seconds(30);
    let events = vec![
        Event::new("SensorReading")
            .with_timestamp(base_time)
            .with_field("temperature", 100.0),
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(1))
            .with_field("temperature", 200.0),
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(2))
            .with_field("temperature", 300.0),
        // No trailing event — session should be closed by the sweep
    ];

    let results = run_context_scenario(program, events).await;

    assert_eq!(
        results.len(),
        1,
        "Sweep should close the stale session and emit 1 aggregation event"
    );
    assert_eq!(results[0].event_type, "SessionAvg");

    if let Some(avg) = results[0].get_float("average") {
        assert!(
            (avg - 200.0).abs() < 0.01,
            "Average should be 200.0, got {}",
            avg
        );
    }
    if let Some(count) = results[0].get_int("total") {
        assert_eq!(count, 3, "Session should have 3 events");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_session_window_sweep_partitioned() {
    // Two partitions with events but no trailing event to close them.
    // The sweep should independently close both partitions.
    let program = r#"
        stream PerSensorSession = SensorReading
            .partition_by(sensor_id)
            .window(session: 3s)
            .aggregate(
                sensor: last(sensor_id),
                count: count()
            )
            .emit(sensor: sensor, event_count: count)
    "#;

    let base_time = chrono::Utc::now() - chrono::Duration::seconds(30);
    let events = vec![
        Event::new("SensorReading")
            .with_timestamp(base_time)
            .with_field("sensor_id", "S1")
            .with_field("temperature", 100.0),
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(1))
            .with_field("sensor_id", "S1")
            .with_field("temperature", 110.0),
        Event::new("SensorReading")
            .with_timestamp(base_time + chrono::Duration::seconds(1))
            .with_field("sensor_id", "S2")
            .with_field("temperature", 200.0),
        // No trailing event — both sessions should be closed by the sweep
    ];

    let results = run_context_scenario(program, events).await;

    assert_eq!(
        results.len(),
        2,
        "Sweep should close both partition sessions"
    );

    let sensors: Vec<String> = results
        .iter()
        .filter_map(|r| r.get_str("sensor").map(|s| s.to_string()))
        .collect();
    assert!(
        sensors.contains(&"S1".to_string()),
        "S1 session should be emitted"
    );
    assert!(
        sensors.contains(&"S2".to_string()),
        "S2 session should be emitted"
    );

    for result in &results {
        let sensor = result.get_str("sensor").unwrap_or_default().to_string();
        let count = result.get_int("event_count").unwrap_or(0);
        match sensor.as_str() {
            "S1" => assert_eq!(count, 2, "S1 should have 2 events"),
            "S2" => assert_eq!(count, 1, "S2 should have 1 event"),
            _ => panic!("Unexpected sensor: {}", sensor),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_session_window_sweep_no_false_close() {
    // Events arrive with a gap that closes session 1, then more events
    // that are still active. The sweep should NOT close the active session
    // prematurely (events are recent enough).
    let program = r#"
        stream SessionCount = SensorReading
            .window(session: 5s)
            .aggregate(count: count())
            .emit(event_count: count)
    "#;

    // Use timestamps far in the past for session 1, then recent for session 2
    let past = chrono::Utc::now() - chrono::Duration::seconds(60);
    let recent = chrono::Utc::now(); // session 2 is "now" — should NOT be swept

    let events = vec![
        // Session 1: old events
        Event::new("SensorReading")
            .with_timestamp(past)
            .with_field("temperature", 10.0),
        Event::new("SensorReading")
            .with_timestamp(past + chrono::Duration::seconds(1))
            .with_field("temperature", 20.0),
        // Gap > 5s closes session 1 when session 2's first event arrives
        // Session 2: recent events (should NOT be swept)
        Event::new("SensorReading")
            .with_timestamp(recent)
            .with_field("temperature", 30.0),
        Event::new("SensorReading")
            .with_timestamp(recent + chrono::Duration::seconds(1))
            .with_field("temperature", 40.0),
    ];

    let results = run_context_scenario(program, events).await;

    // Session 1 closes via gap detection (event-driven).
    // Session 2 closes via sweep (it is still recent, but flush_expired_sessions
    // is called after all events, using Utc::now() which is ~1s after session 2 start).
    // With a 5s gap, session 2 should NOT be swept yet — so only session 1 emits.
    // However, in our test helper, flush_expired_sessions runs immediately after
    // processing, so session 2 (started ~1s ago) should still be active.
    assert_eq!(
        results.len(),
        1,
        "Only session 1 should be emitted (session 2 is still active)"
    );
    if let Some(count) = results[0].get_int("event_count") {
        assert_eq!(count, 2, "Session 1 should have 2 events");
    }
}
