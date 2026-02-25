//! Smoke tests for demo scenarios - verify VPL parses, loads, and processes events correctly.

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;

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

fn parse_events(text: &str) -> Vec<Event> {
    EventFileParser::parse(text)
        .expect("parse events")
        .into_iter()
        .map(|te| te.event)
        .collect()
}

#[test]
fn test_multi_region_demo_cross_region_fraud() {
    let source = r#"event Transaction:
    account: str
    amount: float
    region: str
    merchant: str

event Chargeback:
    account: str
    amount: float
    region: str
    reason: str

stream GlobalFraud = Transaction as t
    -> Chargeback where account == t.account as c
    .within(24h)
    .where(t.region != c.region)
    .emit(
        alert_type: "cross_region_fraud",
        account: t.account,
        transaction_region: t.region,
        chargeback_region: c.region,
        amount: t.amount
    )"#;

    let events = parse_events(
        r#"Transaction { account: "user-101", amount: 3500.00, region: "us-east", merchant: "electronics-store.com" }
Chargeback { account: "user-101", amount: 3500.00, region: "eu-west", reason: "unauthorized" }"#,
    );

    let outputs = run_sync(source, events);
    println!("Multi-region outputs: {}", outputs.len());
    for o in &outputs {
        println!("  -> type={} data={:?}", o.event_type, o.data);
    }
    assert!(
        !outputs.is_empty(),
        "should produce cross_region_fraud alert"
    );
    assert_eq!(
        outputs[0].data.get("alert_type"),
        Some(&Value::str("cross_region_fraud"))
    );
}

#[test]
fn test_multi_region_demo_same_region_no_alert() {
    let source = r#"event Transaction:
    account: str
    amount: float
    region: str
    merchant: str

event Chargeback:
    account: str
    amount: float
    region: str
    reason: str

stream GlobalFraud = Transaction as t
    -> Chargeback where account == t.account as c
    .within(24h)
    .where(t.region != c.region)
    .emit(
        alert_type: "cross_region_fraud",
        account: t.account,
        transaction_region: t.region,
        chargeback_region: c.region,
        amount: t.amount
    )"#;

    let events = parse_events(
        r#"Transaction { account: "user-501", amount: 80.00, region: "us-east", merchant: "diner.com" }
Chargeback { account: "user-501", amount: 80.00, region: "us-east", reason: "duplicate_charge" }"#,
    );

    let outputs = run_sync(source, events);
    println!("Same-region outputs: {}", outputs.len());
    assert!(
        outputs.is_empty(),
        "same-region chargeback should NOT trigger alert"
    );
}

#[test]
fn test_iot_concurrent_demo_anomaly() {
    let source = r#"stream SensorAlerts = SensorReading
    .concurrent(workers: 4, partition_key: "sensor_id")
    .where(temperature > 85 or humidity > 95 or pressure < 950)
    .emit(
        alert_type: "sensor_anomaly",
        sensor_id: sensor_id,
        zone: zone,
        temperature: temperature,
        humidity: humidity,
        pressure: pressure
    )"#;

    let events = parse_events(
        r#"SensorReading { sensor_id: "s1", zone: "assembly", temperature: 23, humidity: 44, pressure: 1013 }
SensorReading { sensor_id: "s2", zone: "welding", temperature: 92, humidity: 28, pressure: 1011 }
SensorReading { sensor_id: "s3", zone: "paint", temperature: 27, humidity: 58, pressure: 1014 }"#,
    );

    let outputs = run_sync(source, events);
    println!("IoT outputs: {}", outputs.len());
    for o in &outputs {
        println!("  -> type={} data={:?}", o.event_type, o.data);
    }
    assert!(
        !outputs.is_empty(),
        "should produce sensor_anomaly alert for temp 92 > 85"
    );
}

#[test]
fn test_iot_concurrent_demo_normal_no_alert() {
    let source = r#"stream SensorAlerts = SensorReading
    .concurrent(workers: 4, partition_key: "sensor_id")
    .where(temperature > 85 or humidity > 95 or pressure < 950)
    .emit(
        alert_type: "sensor_anomaly",
        sensor_id: sensor_id,
        zone: zone,
        temperature: temperature,
        humidity: humidity,
        pressure: pressure
    )"#;

    let events = parse_events(
        r#"SensorReading { sensor_id: "s1", zone: "assembly", temperature: 22, humidity: 45, pressure: 1013 }
SensorReading { sensor_id: "s2", zone: "welding", temperature: 35, humidity: 30, pressure: 1012 }
SensorReading { sensor_id: "s3", zone: "paint", temperature: 28, humidity: 60, pressure: 1015 }"#,
    );

    let outputs = run_sync(source, events);
    println!("IoT normal outputs: {}", outputs.len());
    assert!(
        outputs.is_empty(),
        "normal readings should NOT trigger alerts"
    );
}

#[test]
fn test_ai_fraud_scoring_demo_parses() {
    // NOTE: .score() requires onnx feature, so just test parse here
    let source = r#"event Transaction:
    transaction_id: str
    account: str
    amount: float
    velocity: float
    distance: float

stream FraudAlerts = Transaction
    .score(model: "/app/models/fraud_scorer.onnx", inputs: [amount, velocity, distance], outputs: [fraud_prob], batch_size: 4)
    .where(fraud_prob > 0.7)
    .emit(
        alert_type: "ai_fraud_alert",
        transaction_id: transaction_id,
        account: account,
        amount: amount,
        fraud_probability: fraud_prob
    )"#;

    let program = parse(source).expect("should parse AI fraud scoring VPL");
    assert!(!program.statements.is_empty());
}
