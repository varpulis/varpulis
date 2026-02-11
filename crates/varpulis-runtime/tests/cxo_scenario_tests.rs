//! CxO Real-Life Scenario Integration Tests
//!
//! These tests validate the 5 flagship scenarios used for CxO presentations.
//! Each scenario uses `.vpl` and `.evt` files from `tests/scenarios/` and
//! verifies that the expected alerts are produced.

use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;

async fn run_scenario(program_source: &str, events_source: &str) -> Vec<Event> {
    let (tx, mut rx) = mpsc::channel::<Event>(1000);

    let program = parse(program_source).expect("Failed to parse program");
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load program");

    let events = EventFileParser::parse(events_source).expect("Failed to parse events");
    for timed_event in events {
        engine
            .process(timed_event.event)
            .await
            .expect("Failed to process event");
    }

    let mut results = Vec::new();
    while let Ok(event) = rx.try_recv() {
        results.push(event);
    }
    results
}

fn count_alerts(results: &[Event], alert_type: &str) -> usize {
    results
        .iter()
        .filter(|e| {
            e.data
                .get("alert_type")
                .is_some_and(|v| v == &varpulis_core::Value::Str(alert_type.into()))
        })
        .count()
}

// =============================================================================
// Scenario 1: Multi-Stage Credit Card Fraud Detection
// =============================================================================

#[tokio::test]
async fn cxo_fraud_account_takeover() {
    let program = include_str!("../../../tests/scenarios/cxo_fraud_detection.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_fraud_detection.evt");

    let results = run_scenario(program, events).await;

    let takeover_count = count_alerts(&results, "account_takeover");
    assert_eq!(
        takeover_count, 1,
        "Should detect exactly 1 account takeover (attacker1)"
    );
}

#[tokio::test]
async fn cxo_fraud_card_testing() {
    let program = include_str!("../../../tests/scenarios/cxo_fraud_detection.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_fraud_detection.evt");

    let results = run_scenario(program, events).await;

    let testing_count = count_alerts(&results, "card_testing");
    assert!(
        testing_count >= 1,
        "Should detect at least 1 card testing sequence (stolen_card_42), got {}",
        testing_count
    );
}

#[tokio::test]
async fn cxo_fraud_impossible_travel() {
    let program = include_str!("../../../tests/scenarios/cxo_fraud_detection.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_fraud_detection.evt");

    let results = run_scenario(program, events).await;

    let travel_count = count_alerts(&results, "impossible_travel");
    assert_eq!(
        travel_count, 1,
        "Should detect exactly 1 impossible travel (traveler1: US → NG)"
    );
}

#[tokio::test]
async fn cxo_fraud_no_false_positives() {
    let program = include_str!("../../../tests/scenarios/cxo_fraud_detection.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_fraud_detection.evt");

    let results = run_scenario(program, events).await;

    // legit_user should not trigger any alerts
    let legit_alerts: Vec<_> = results
        .iter()
        .filter(|e| {
            e.data
                .get("user_id")
                .is_some_and(|v| v == &varpulis_core::Value::Str("legit_user".into()))
        })
        .collect();
    assert_eq!(
        legit_alerts.len(),
        0,
        "Legitimate user should not trigger any fraud alerts"
    );
}

// =============================================================================
// Scenario 2: Predictive Equipment Failure
// =============================================================================

#[tokio::test]
async fn cxo_maintenance_bearing_degradation() {
    let program = include_str!("../../../tests/scenarios/cxo_predictive_maintenance.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_predictive_maintenance.evt");

    let results = run_scenario(program, events).await;

    let bearing_count = count_alerts(&results, "bearing_degradation");
    assert_eq!(
        bearing_count, 1,
        "Should detect bearing degradation on CNC-01 (0.5 → 0.8mm, 60% increase > 30% threshold)"
    );
}

#[tokio::test]
async fn cxo_maintenance_overheating() {
    let program = include_str!("../../../tests/scenarios/cxo_predictive_maintenance.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_predictive_maintenance.evt");

    let results = run_scenario(program, events).await;

    let overheating_count = count_alerts(&results, "overheating");
    assert_eq!(
        overheating_count, 1,
        "Should detect overheating in ZoneA (45°C → 62°C, delta 17°C > 15°C threshold)"
    );
}

#[tokio::test]
async fn cxo_maintenance_healthy_no_alert() {
    let program = include_str!("../../../tests/scenarios/cxo_predictive_maintenance.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_predictive_maintenance.evt");

    let results = run_scenario(program, events).await;

    // CNC-02 has stable vibration (0.3 → 0.31, <30% increase) — no alert
    let cnc02_alerts: Vec<_> = results
        .iter()
        .filter(|e| {
            e.data
                .get("machine_id")
                .is_some_and(|v| v == &varpulis_core::Value::Str("CNC-02".into()))
        })
        .collect();
    assert_eq!(
        cnc02_alerts.len(),
        0,
        "Healthy machine CNC-02 should not trigger degradation alert"
    );
}

// =============================================================================
// Scenario 3: Insider Trading Surveillance
// =============================================================================

#[tokio::test]
async fn cxo_insider_trade_before_news() {
    let program = include_str!("../../../tests/scenarios/cxo_insider_trading.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_insider_trading.evt");

    let results = run_scenario(program, events).await;

    let tbn_count = count_alerts(&results, "trade_before_news");
    assert_eq!(
        tbn_count, 1,
        "Should detect trade-before-news for trader_sus (ACME)"
    );
}

#[tokio::test]
async fn cxo_insider_abnormal_position() {
    let program = include_str!("../../../tests/scenarios/cxo_insider_trading.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_insider_trading.evt");

    let results = run_scenario(program, events).await;

    let apb_count = count_alerts(&results, "abnormal_position_building");
    assert!(
        apb_count >= 1,
        "Should detect abnormal position building for accumulator (WIDG), got {}",
        apb_count
    );
}

// =============================================================================
// Scenario 4: Cyber Kill Chain Detection
// =============================================================================

#[tokio::test]
async fn cxo_cyber_brute_force_lateral() {
    let program = include_str!("../../../tests/scenarios/cxo_cyber_threat.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_cyber_threat.evt");

    let results = run_scenario(program, events).await;

    let bf_count = count_alerts(&results, "brute_force_lateral");
    assert!(
        bf_count >= 1,
        "Should detect brute force + lateral movement to file-server-02, got {}",
        bf_count
    );
}

#[tokio::test]
async fn cxo_cyber_dns_exfiltration() {
    let program = include_str!("../../../tests/scenarios/cxo_cyber_threat.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_cyber_threat.evt");

    let results = run_scenario(program, events).await;

    let dns_count = count_alerts(&results, "dns_exfiltration");
    assert!(
        dns_count >= 1,
        "Should detect DNS exfiltration from workstation-15, got {}",
        dns_count
    );
}

#[tokio::test]
async fn cxo_cyber_privilege_escalation() {
    let program = include_str!("../../../tests/scenarios/cxo_cyber_threat.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_cyber_threat.evt");

    let results = run_scenario(program, events).await;

    let priv_count = count_alerts(&results, "privilege_escalation");
    assert_eq!(
        priv_count, 1,
        "Should detect privilege escalation on dev-box-03"
    );
}

// =============================================================================
// Scenario 5: Patient Safety Monitoring
// =============================================================================

#[tokio::test]
async fn cxo_patient_drug_interaction() {
    let program = include_str!("../../../tests/scenarios/cxo_patient_safety.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_patient_safety.evt");

    let results = run_scenario(program, events).await;

    let di_count = count_alerts(&results, "drug_interaction");
    assert_eq!(
        di_count, 1,
        "Should detect warfarin + aspirin interaction for patient P-101"
    );
}

#[tokio::test]
async fn cxo_patient_vital_deterioration() {
    let program = include_str!("../../../tests/scenarios/cxo_patient_safety.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_patient_safety.evt");

    let results = run_scenario(program, events).await;

    let vd_count = count_alerts(&results, "vital_deterioration");
    assert_eq!(
        vd_count, 1,
        "Should detect vital deterioration for patient P-202 (HR: 90 → 135, 50% increase > 20%)"
    );
}

#[tokio::test]
async fn cxo_patient_dosage_anomaly() {
    let program = include_str!("../../../tests/scenarios/cxo_patient_safety.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_patient_safety.evt");

    let results = run_scenario(program, events).await;

    let da_count = count_alerts(&results, "dosage_anomaly");
    assert_eq!(
        da_count, 1,
        "Should detect dosage anomaly for P-303 (1500mg > 1000mg max)"
    );
}

#[tokio::test]
async fn cxo_patient_healthy_no_alert() {
    let program = include_str!("../../../tests/scenarios/cxo_patient_safety.vpl");
    let events = include_str!("../../../tests/scenarios/cxo_patient_safety.evt");

    let results = run_scenario(program, events).await;

    // P-404 has normal vitals — should not trigger deterioration
    let healthy_alerts: Vec<_> = results
        .iter()
        .filter(|e| {
            e.data
                .get("patient_id")
                .is_some_and(|v| v == &varpulis_core::Value::Str("P-404".into()))
        })
        .collect();
    assert_eq!(
        healthy_alerts.len(),
        0,
        "Healthy patient P-404 should not trigger any alerts"
    );
}
