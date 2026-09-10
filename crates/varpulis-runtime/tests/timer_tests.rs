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

/// Timers must be tested on tokio's virtual clock, not the wall clock.
///
/// These tests used to race a real 100 ms budget against real 10 ms and 20 ms
/// timers. On a loaded CI runner the timers get starved, `interval`'s default
/// burst catch-up delivers ticks unevenly, and the counts invert — which is
/// how `test_timer_manager` turned the nightly suite red. `start_paused`
/// makes time virtual: the runtime advances to the next timer deadline
/// whenever it has nothing else to do, so tick counts become exact and the
/// tests run in microseconds regardless of machine load.
#[tokio::test(start_paused = true)]
async fn test_timer_generates_events() {
    let (event_tx, mut event_rx) = mpsc::channel::<Event>(100);

    let handle = varpulis_runtime::timer::spawn_timer(
        10_000_000, // 10 ms
        None,
        "Timer_test".to_string(),
        event_tx,
    );

    // The first tick is consumed by the implementation, so ticks land at
    // 10, 20, 30 ms. Three receives are exactly three ticks.
    for _ in 0..3 {
        let event = event_rx
            .recv()
            .await
            .expect("timer must keep producing events");
        assert_eq!(&*event.event_type, "Timer_test");
        assert!(
            event.data.contains_key("timestamp"),
            "timer events must carry a timestamp"
        );
    }

    handle.abort();
}

/// A 10 ms timer must fire exactly twice as often as a 20 ms one.
///
/// On the virtual clock this is an equality, not a heuristic: by t=60 ms the
/// fast timer has ticked at 10..60 and the slow one at 20, 40, 60, so nine
/// events total, six and three. The previous version asserted only
/// `a >= b` after a real 100 ms race and still managed to fail on macOS.
#[tokio::test(start_paused = true)]
async fn test_timer_manager() {
    let (event_tx, mut event_rx) = mpsc::channel::<Event>(100);

    let mut manager = TimerManager::new();
    manager.spawn_timers(
        vec![
            (10_000_000, None, "Timer_A".to_string()), // 10 ms
            (20_000_000, None, "Timer_B".to_string()), // 20 ms
        ],
        event_tx,
    );

    let mut timer_a_count = 0;
    let mut timer_b_count = 0;

    // Exactly the events due in the first 60 virtual milliseconds.
    for _ in 0..9 {
        let event = event_rx.recv().await.expect("timers must keep producing");
        match &*event.event_type {
            "Timer_A" => timer_a_count += 1,
            "Timer_B" => timer_b_count += 1,
            other => panic!("unexpected timer event type: {other}"),
        }
    }

    manager.stop_all();

    assert_eq!(timer_a_count, 6, "10 ms timer over 60 ms");
    assert_eq!(timer_b_count, 3, "20 ms timer over 60 ms");
}

/// The initial delay must actually hold the first event back.
///
/// On the virtual clock the two waits below are exact: nothing can arrive
/// before 20 ms, and the first tick lands at 30 ms (delay plus one interval).
#[tokio::test(start_paused = true)]
async fn test_timer_with_initial_delay_spawning() {
    let (event_tx, mut event_rx) = mpsc::channel::<Event>(100);

    let handle = varpulis_runtime::timer::spawn_timer(
        10_000_000,       // 10 ms interval
        Some(20_000_000), // 20 ms initial delay
        "Timer_delayed".to_string(),
        event_tx,
    );

    // Nothing may arrive within the initial delay.
    let early = tokio::time::timeout(Duration::from_millis(19), event_rx.recv()).await;
    assert!(
        early.is_err(),
        "no event may arrive before the initial delay elapses"
    );

    // The first tick is one interval after the delay.
    let first = tokio::time::timeout(Duration::from_millis(50), event_rx.recv())
        .await
        .expect("an event must arrive after the initial delay")
        .expect("the timer channel must stay open");
    assert_eq!(&*first.event_type, "Timer_delayed");

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
