//! VPL-level E2E tests for Kleene+ and forecast scenarios.
//! These test the full pipeline: VPL parse → engine → output events.
#![allow(clippy::needless_raw_string_hashes)]

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

fn run_sync(code: &str, events: Vec<Event>) -> Vec<Event> {
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");
    engine.process_batch_sync(events).unwrap();
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

fn temp_event(sensor: &str, temp: i64) -> Event {
    let mut e = Event::new("TempReading");
    e.data.insert("sensor_id".into(), Value::str(sensor));
    e.data.insert("temperature".into(), Value::Int(temp));
    e
}

fn login_event(ip: &str, status: &str) -> Event {
    let mut e = Event::new("AuthEvent");
    e.data.insert("source_ip".into(), Value::str(ip));
    e.data.insert("status".into(), Value::str(status));
    e.data.insert("username".into(), Value::str("admin"));
    e
}

// =========================================================================
// Rising Temperature with Terminator (single clean alert)
// =========================================================================

#[test]
fn test_vpl_rising_temp_single_alert_on_drop() {
    let vpl = r#"
event TempReading:
    sensor_id: str
    temperature: int

pattern StrictlyRising = TempReading as first
    -> all TempReading where temperature > first.temperature as rising
    -> TempReading where temperature < first.temperature as drop
    within 5m partition by sensor_id

stream HighTemp = StrictlyRising
    .longest()
    .where(count(rising) >= 2)
    .emit(
        sensor: first.sensor_id,
        baseline: first.temperature,
        peak: rising.temperature,
        drop_to: drop.temperature,
        num: count(rising)
    )
"#;

    let events = vec![
        temp_event("HVAC-01", 22),
        temp_event("HVAC-01", 41),
        temp_event("HVAC-01", 40), // 40 > 22, still rising relative to first
        temp_event("HVAC-01", 42),
        temp_event("HVAC-01", 25), // 25 > 22, still rising
        temp_event("HVAC-01", 20), // 20 < 22, DROP — triggers completion
    ];

    let outputs = run_sync(vpl, events);
    assert_eq!(
        outputs.len(),
        1,
        "Should produce exactly 1 alert when temperature drops below baseline"
    );
    assert_eq!(outputs[0].get("sensor"), Some(&Value::str("HVAC-01")));
    assert_eq!(outputs[0].get("baseline"), Some(&Value::Int(22)));
    assert_eq!(outputs[0].get("drop_to"), Some(&Value::Int(20)));
}

#[test]
fn test_vpl_rising_temp_no_alert_when_never_drops() {
    let vpl = r#"
event TempReading:
    sensor_id: str
    temperature: int

pattern StrictlyRising = TempReading as first
    -> all TempReading where temperature > first.temperature as rising
    -> TempReading where temperature < first.temperature as drop
    within 5m partition by sensor_id

stream HighTemp = StrictlyRising
    .longest()
    .emit(sensor: first.sensor_id, num: count(rising))
"#;

    // Temperature rises but never drops below baseline
    let events = vec![
        temp_event("HVAC-01", 22),
        temp_event("HVAC-01", 30),
        temp_event("HVAC-01", 35),
        temp_event("HVAC-01", 40),
    ];

    let outputs = run_sync(vpl, events);
    assert_eq!(
        outputs.len(),
        0,
        "No alert without a drop event terminating the Kleene"
    );
}

// =========================================================================
// Partition isolation — sensors don't mix
// =========================================================================

#[test]
fn test_vpl_partition_isolates_sensors() {
    let vpl = r#"
event TempReading:
    sensor_id: str
    temperature: int

pattern Rising = TempReading as first
    -> all TempReading where temperature > first.temperature as rising
    -> TempReading where temperature < first.temperature as drop
    within 5m partition by sensor_id

stream Alert = Rising
    .longest()
    .emit(sensor: first.sensor_id, num: count(rising))
"#;

    let events = vec![
        temp_event("A", 20),  // A baseline
        temp_event("B", 100), // B baseline
        temp_event("A", 30),  // A rising (30 > 20)
        temp_event("B", 50),  // B: 50 < 100, NOT rising
        temp_event("A", 10),  // A drop (10 < 20) → alert
        temp_event("B", 90),  // B: 90 < 100, still not rising → no alert
    ];

    let outputs = run_sync(vpl, events);
    assert_eq!(outputs.len(), 1, "Only sensor A should alert");
    assert_eq!(outputs[0].get("sensor"), Some(&Value::str("A")));
}

// =========================================================================
// Strictly increasing with self-referencing Kleene predicate
// =========================================================================

#[test]
fn test_vpl_strictly_increasing_with_self_ref() {
    let vpl = r#"
event TempReading:
    sensor_id: str
    temperature: int

pattern StrictlyRising = TempReading as first
    -> all TempReading where temperature > rising.temperature as rising
    -> TempReading where temperature < first.temperature as drop
    within 5m partition by sensor_id

stream Alert = StrictlyRising
    .emit(sensor: first.sensor_id, baseline: first.temperature, peak: rising.temperature, drop_to: drop.temperature, num: count(rising))
"#;

    let events = vec![
        temp_event("A", 20),
        temp_event("A", 25),
        temp_event("A", 30),
        temp_event("A", 35),
        temp_event("A", 10),
    ];

    let outputs = run_sync(vpl, events);
    assert!(
        !outputs.is_empty(),
        "Strictly rising with self-referencing Kleene should produce results"
    );

    let best = outputs
        .iter()
        .max_by_key(|o| o.get("num").and_then(|v| v.as_int()).unwrap_or(0))
        .unwrap();
    assert_eq!(best.get("baseline"), Some(&Value::Int(20)));
    assert_eq!(best.get("peak"), Some(&Value::Int(35)));
    assert_eq!(best.get("drop_to"), Some(&Value::Int(10)));
}

// =========================================================================
// Kleene+ Brute Force — unbounded failed logins then success
// =========================================================================

#[test]
fn test_vpl_kleene_brute_force() {
    let vpl = r#"
event AuthEvent:
    source_ip: str
    username: str
    status: str

pattern BruteForce = AuthEvent where status == "failed" as first
    -> all AuthEvent where status == "failed" as fails
    -> AuthEvent where status == "success" as success
    within 30m partition by source_ip

stream Alert = BruteForce
    .emit(ip: first.source_ip, failures: count(fails) + 1, user: success.username)
"#;

    let mut events: Vec<Event> = (0..20)
        .map(|_| login_event("10.0.0.50", "failed"))
        .collect();
    events.push(login_event("10.0.0.50", "success"));

    let outputs = run_sync(vpl, events);
    assert!(
        !outputs.is_empty(),
        "20 failed + 1 success should trigger brute force alert"
    );
    assert_eq!(outputs[0].get("ip"), Some(&Value::str("10.0.0.50")));
}

#[test]
fn test_vpl_kleene_brute_force_no_success_no_alert() {
    let vpl = r#"
event AuthEvent:
    source_ip: str
    username: str
    status: str

pattern BruteForce = AuthEvent where status == "failed" as first
    -> all AuthEvent where status == "failed" as fails
    -> AuthEvent where status == "success" as success
    within 30m partition by source_ip

stream Alert = BruteForce
    .longest()
    .emit(ip: first.source_ip)
"#;

    // Only failures, no success — should NOT trigger
    let events: Vec<Event> = (0..10)
        .map(|_| login_event("10.0.0.50", "failed"))
        .collect();

    let outputs = run_sync(vpl, events);
    assert_eq!(
        outputs.len(),
        0,
        "Failed logins without success should not trigger"
    );
}

// =========================================================================
// A -> all B without terminator — SASE+ STAM emits on each Kleene extension
// =========================================================================

#[test]
fn test_vpl_kleene_without_terminator_emits_multiple() {
    let vpl = r#"
event TempReading:
    sensor_id: str
    temperature: int

pattern Rising = TempReading as first
    -> all TempReading where temperature > first.temperature as rising
    within 5m partition by sensor_id

stream Alert = Rising
    .emit(sensor: first.sensor_id, num: count(rising))
"#;

    let events = vec![
        temp_event("A", 20),
        temp_event("A", 30), // 1st Kleene match → emit
        temp_event("A", 40), // 2nd → emit
        temp_event("A", 50), // 3rd → emit
    ];

    let outputs = run_sync(vpl, events);
    assert!(
        outputs.len() >= 3,
        "A -> all B without terminator should emit on each Kleene extension, got {}",
        outputs.len()
    );
}

// =========================================================================
// Where filter on count — only alert when threshold reached
// =========================================================================

#[test]
fn test_vpl_kleene_count_threshold() {
    let vpl = r#"
event AuthEvent:
    source_ip: str
    username: str
    status: str

pattern BruteForce = AuthEvent where status == "failed" as first
    -> all AuthEvent where status == "failed" as fails
    -> AuthEvent where status == "success" as success
    within 30m partition by source_ip

stream Alert = BruteForce
    .where(count(fails) >= 4)
    .emit(ip: first.source_ip, failures: count(fails) + 1)
"#;

    // 3 failures + success → count(fails) = 2, below threshold
    let mut events_few: Vec<Event> = (0..3).map(|_| login_event("10.0.0.1", "failed")).collect();
    events_few.push(login_event("10.0.0.1", "success"));

    let outputs = run_sync(vpl, events_few);
    assert_eq!(
        outputs.len(),
        0,
        "3 failures (count=2) should not meet threshold of 4"
    );

    // 10 failures + success → count(fails) = 9, above threshold
    let mut events_many: Vec<Event> = (0..10).map(|_| login_event("10.0.0.2", "failed")).collect();
    events_many.push(login_event("10.0.0.2", "success"));

    let outputs = run_sync(vpl, events_many);
    assert!(
        !outputs.is_empty(),
        "10 failures (count=9) should meet threshold of 4"
    );
}
