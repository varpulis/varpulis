//! Performance tests for the runtime engine

use std::time::Instant;
use tokio::sync::mpsc;
use varpulis_runtime::{Engine, Event};

#[tokio::test]
async fn test_process_1000_events_under_100ms() {
    let (output_tx, mut output_rx) = mpsc::channel(10000);
    let mut engine = Engine::new(output_tx);

    let vpl = r#"
        event TestEvent:
            id: int
            value: float

        stream TestStream = TestEvent
            .where(value > 0)
            .emit(event_type: "Processed", id: id, value: value)
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    // Warm up
    for i in 0..10 {
        let event = Event::new("TestEvent")
            .with_field("id", i as i64)
            .with_field("value", 100.0f64);
        engine.process(event).await.expect("Failed to process");
    }

    // Drain output events from warm-up
    while output_rx.try_recv().is_ok() {}

    // Timed run
    let start = Instant::now();

    for i in 0..1000 {
        let event = Event::new("TestEvent")
            .with_field("id", i as i64)
            .with_field("value", (i as f64) * 1.5);
        engine.process(event).await.expect("Failed to process");
    }

    let duration = start.elapsed();

    println!("Processed 1000 events in {:?}", duration);
    println!(
        "Throughput: {:.0} events/sec",
        1000.0 / duration.as_secs_f64()
    );

    // Check we got output events
    let mut output_count = 0;
    while output_rx.try_recv().is_ok() {
        output_count += 1;
    }

    println!("Output events: {}", output_count);

    // Performance assertion - should complete in under 100ms
    // (This is a generous limit; actual should be much faster)
    assert!(
        duration.as_millis() < 100,
        "Processing 1000 events took {:?}, expected < 100ms",
        duration
    );
}

#[tokio::test]
async fn test_memory_stable_after_10000_events() {
    let (output_tx, mut output_rx) = mpsc::channel(100);
    let mut engine = Engine::new(output_tx);

    let vpl = r#"
        event DataEvent:
            sensor: str
            reading: float

        stream DataStream = DataEvent
            .partition_by(sensor)
            .window(10)
            .aggregate(
                sensor: last(sensor),
                avg_reading: avg(reading)
            )
            .emit(event_type: "Aggregated", sensor: sensor, avg: avg_reading)
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    // Process many events
    for i in 0..10000 {
        let sensor = format!("sensor_{}", i % 100); // 100 different sensors
        let event = Event::new("DataEvent")
            .with_field("sensor", sensor)
            .with_field("reading", (i as f64) * 0.1);
        engine.process(event).await.expect("Failed to process");

        // Drain output events periodically to avoid channel backup
        while output_rx.try_recv().is_ok() {}
    }

    // Get metrics
    let metrics = engine.metrics();
    println!("Events processed: {}", metrics.events_processed);
    println!("Output events: {}", metrics.output_events_emitted);

    // The test passes if we complete without OOM or panic
    // A more sophisticated test would check actual memory usage
    assert!(metrics.events_processed >= 10000);
}

#[tokio::test]
async fn test_filter_performance() {
    let (output_tx, mut output_rx) = mpsc::channel(10000);
    let mut engine = Engine::new(output_tx);

    let vpl = r#"
        event NumericEvent:
            value: float

        stream Filtered = NumericEvent
            .where(value > 50)
            .emit(event_type: "Match", value: value)
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    let start = Instant::now();

    // Process events
    for i in 0..1000 {
        let event = Event::new("NumericEvent").with_field("value", i as f64);
        engine.process(event).await.expect("Failed to process");

        // Drain output events to avoid channel backup
        while output_rx.try_recv().is_ok() {}
    }

    let duration = start.elapsed();

    println!("Filter test: {:?} for 1000 events", duration);
    println!(
        "Throughput: {:.0} events/sec",
        1000.0 / duration.as_secs_f64()
    );

    // Should complete quickly
    assert!(duration.as_millis() < 500, "Filter test took too long");
}

#[tokio::test]
async fn test_window_cleanup_performance() {
    use std::time::Instant;

    let (output_tx, mut output_rx) = mpsc::channel(10000);
    let mut engine = Engine::new(output_tx);

    let vpl = r#"
        event TickEvent:
            price: float

        stream Ticks = TickEvent
            .window(5)
            .aggregate(
                avg_price: avg(price)
            )
            .emit(event_type: "WindowSummary", avg: avg_price)
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    let start = Instant::now();

    // Send events
    for i in 0..1000 {
        let event = Event::new("TickEvent").with_field("price", 100.0 + (i % 100) as f64);
        engine.process(event).await.expect("Failed to process");
    }

    let duration = start.elapsed();

    // Drain output events
    let mut count = 0;
    while output_rx.try_recv().is_ok() {
        count += 1;
    }

    println!(
        "Window test: {:?} for 1000 events, {} windows completed",
        duration, count
    );

    // Should complete quickly
    assert!(
        duration.as_millis() < 200,
        "Window test took too long: {:?}",
        duration
    );
}
