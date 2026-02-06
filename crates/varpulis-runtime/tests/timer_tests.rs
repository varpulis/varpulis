//! Tests for periodic timer functionality

use std::time::Duration;
use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::timer::TimerManager;

#[tokio::test]
async fn test_timer_stream_parsing_and_registration() {
    let code = r#"
        stream heartbeat = timer(1s)
            .emit(type: "heartbeat")
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    // Check that timer was registered
    let timers = engine.get_timers();
    assert_eq!(timers.len(), 1, "Expected 1 timer");

    let (interval_ns, initial_delay_ns, timer_event_type) = &timers[0];
    assert_eq!(*interval_ns, 1_000_000_000); // 1 second in nanoseconds
    assert_eq!(*initial_delay_ns, None);
    assert_eq!(timer_event_type, "Timer_heartbeat");
}

#[tokio::test]
async fn test_timer_with_initial_delay() {
    let code = r#"
        stream delayed = timer(5s, initial_delay: 2s)
            .emit(type: "delayed")
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    let timers = engine.get_timers();
    assert_eq!(timers.len(), 1);

    let (interval_ns, initial_delay_ns, timer_event_type) = &timers[0];
    assert_eq!(*interval_ns, 5_000_000_000); // 5 seconds
    assert_eq!(*initial_delay_ns, Some(2_000_000_000)); // 2 seconds
    assert_eq!(timer_event_type, "Timer_delayed");
}

#[tokio::test]
async fn test_timer_generates_events() {
    // Create a fast timer for testing (10ms interval)
    let (event_tx, mut event_rx) = mpsc::channel::<Event>(100);

    // Spawn a timer directly using spawn_timer
    let handle = varpulis_runtime::timer::spawn_timer(
        10_000_000, // 10ms in nanoseconds
        None,
        "Timer_test".to_string(),
        event_tx,
    );

    // Wait for a few timer events
    let mut received_count = 0;
    let timeout = tokio::time::timeout(Duration::from_millis(100), async {
        while let Some(event) = event_rx.recv().await {
            assert_eq!(&*event.event_type, "Timer_test");
            assert!(event.data.contains_key("timestamp"));
            received_count += 1;
            if received_count >= 3 {
                break;
            }
        }
    })
    .await;

    // Abort the timer
    handle.abort();

    // We should have received at least some events (may be less if timing is tight)
    assert!(
        timeout.is_ok() || received_count >= 1,
        "Timer should generate events"
    );
}

#[tokio::test]
async fn test_timer_manager() {
    let (event_tx, mut event_rx) = mpsc::channel::<Event>(100);

    let mut manager = TimerManager::new();

    // Spawn two timers
    manager.spawn_timers(
        vec![
            (10_000_000, None, "Timer_A".to_string()), // 10ms
            (20_000_000, None, "Timer_B".to_string()), // 20ms
        ],
        event_tx,
    );

    // Wait for some events
    let mut timer_a_count = 0;
    let mut timer_b_count = 0;

    let _ = tokio::time::timeout(Duration::from_millis(100), async {
        while let Some(event) = event_rx.recv().await {
            match &*event.event_type {
                "Timer_A" => timer_a_count += 1,
                "Timer_B" => timer_b_count += 1,
                _ => {}
            }
            if timer_a_count >= 3 && timer_b_count >= 2 {
                break;
            }
        }
    })
    .await;

    // Stop all timers
    manager.stop_all();

    // Timer A should fire more often than Timer B (2x frequency)
    assert!(
        timer_a_count >= timer_b_count,
        "Timer A (10ms) should fire more often than Timer B (20ms)"
    );
}

#[tokio::test]
async fn test_timer_with_initial_delay_spawning() {
    let (event_tx, mut event_rx) = mpsc::channel::<Event>(100);

    // Spawn a timer with a short initial delay (20ms) and fast interval (10ms)
    let handle = varpulis_runtime::timer::spawn_timer(
        10_000_000,       // 10ms interval
        Some(20_000_000), // 20ms initial delay
        "Timer_delayed".to_string(),
        event_tx,
    );

    // Try to receive immediately - should get nothing due to initial delay
    let immediate_result = tokio::time::timeout(Duration::from_millis(10), event_rx.recv()).await;
    assert!(
        immediate_result.is_err(),
        "Should not receive events before initial delay"
    );

    // Wait for initial delay + interval to pass
    let delayed_result = tokio::time::timeout(Duration::from_millis(50), event_rx.recv()).await;
    assert!(
        delayed_result.is_ok(),
        "Should receive event after initial delay"
    );

    handle.abort();
}

#[tokio::test]
async fn test_timer_event_processing_through_engine() {
    let code = r#"
        stream heartbeat = timer(1s)
            .emit(type: "heartbeat", count: 0)
    "#;

    let program = parse(code).expect("Failed to parse");

    let (output_tx, _output_rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(output_tx);
    engine.load(&program).expect("Failed to load program");

    // Create a timer event manually and process it
    let mut timer_event = Event::new("Timer_heartbeat");
    timer_event.data.insert(
        "timestamp".into(),
        varpulis_core::Value::Int(chrono::Utc::now().timestamp_millis()),
    );

    // Process the timer event through the engine
    let result = engine.process(timer_event).await;
    assert!(
        result.is_ok(),
        "Engine should process timer events: {:?}",
        result.err()
    );
}
