//! E2E tests for the security demo — ensures all VPL detection rules
//! continue working as the engine evolves.

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

fn parse_jsonl_events(jsonl: &str) -> Vec<Event> {
    EventFileParser::parse(jsonl)
        .expect("parse events")
        .into_iter()
        .map(|te| te.event)
        .collect()
}

fn load_file(path: &str) -> String {
    // Resolve relative to workspace root (two levels up from crate manifest dir)
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let full_path = workspace_root.join(path);
    std::fs::read_to_string(&full_path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {e}", full_path.display()))
}

// =========================================================================
// Phase 1: Sysmon Ingestion
// =========================================================================

#[test]
fn test_sysmon_smoke_ingestion() {
    let vpl = load_file("examples/security-demo/sysmon_smoke.vpl");
    let events = parse_jsonl_events(&load_file(
        "examples/security-demo/data/sysmon_sample.jsonl",
    ));
    let outputs = run_sync(&vpl, events);
    assert!(
        outputs.len() >= 5,
        "Expected 5+ suspicious processes, got {}",
        outputs.len()
    );
    for o in &outputs {
        assert_eq!(
            o.data.get("event_type"),
            Some(&Value::str("SuspiciousProcess"))
        );
    }
}

// =========================================================================
// Phase 2: Scripting -> Discovery
// =========================================================================

#[test]
fn test_scripting_to_discovery() {
    let vpl = load_file("examples/security-demo/detect_scripting_to_discovery.vpl");
    let events = parse_jsonl_events(&load_file(
        "examples/security-demo/data/apt29_scripting_chain.jsonl",
    ));
    let outputs = run_sync(&vpl, events);
    assert!(
        outputs.len() >= 2,
        "Expected 2+ scripting->discovery alerts, got {}",
        outputs.len()
    );
    for o in &outputs {
        assert_eq!(
            o.data.get("rule"),
            Some(&Value::str("scripting_to_discovery"))
        );
        assert_eq!(o.data.get("mitre"), Some(&Value::str("T1059.001,T1057")));
    }
}

// =========================================================================
// Phase 3: Kill Chain Suite
// =========================================================================

#[test]
fn test_credential_dumping() {
    let vpl = load_file("examples/security-demo/detect_credential_dumping.vpl");
    let events = parse_jsonl_events(&load_file(
        "examples/security-demo/data/apt29_credential_dump.jsonl",
    ));
    let outputs = run_sync(&vpl, events);
    assert!(
        outputs.len() >= 3,
        "Expected 3+ credential dumping alerts (mimikatz, procdump, rundll32), got {}",
        outputs.len()
    );
    for o in &outputs {
        assert_eq!(o.data.get("rule"), Some(&Value::str("credential_dumping")));
        assert_eq!(o.data.get("mitre"), Some(&Value::str("T1003.001")));
        assert_eq!(o.data.get("severity"), Some(&Value::str("critical")));
    }
}

#[test]
fn test_lateral_movement() {
    let vpl = load_file("examples/security-demo/detect_lateral_movement.vpl");
    let events = parse_jsonl_events(&load_file(
        "examples/security-demo/data/apt29_lateral_movement.jsonl",
    ));
    let outputs = run_sync(&vpl, events);
    assert!(
        !outputs.is_empty(),
        "Expected 1+ lateral movement alerts, got {}",
        outputs.len()
    );
    assert_eq!(
        outputs[0].data.get("rule"),
        Some(&Value::str("lateral_movement_smb"))
    );
    assert_eq!(outputs[0].data.get("mitre"), Some(&Value::str("T1021.002")));
}

#[test]
fn test_persistence_registry() {
    let vpl = load_file("examples/security-demo/detect_persistence_registry.vpl");
    let events = parse_jsonl_events(&load_file(
        "examples/security-demo/data/apt29_persistence.jsonl",
    ));
    let outputs = run_sync(&vpl, events);
    assert!(
        outputs.len() >= 2,
        "Expected 2+ persistence alerts (WS01 Run + WS02 RunOnce), got {}",
        outputs.len()
    );
    for o in &outputs {
        assert_eq!(
            o.data.get("rule"),
            Some(&Value::str("persistence_registry"))
        );
        assert_eq!(o.data.get("mitre"), Some(&Value::str("T1547.001")));
    }
}

#[test]
fn test_data_staging_exfil() {
    let vpl = load_file("examples/security-demo/detect_data_staging_exfil.vpl");
    let events = parse_jsonl_events(&load_file(
        "examples/security-demo/data/apt29_full_chain.jsonl",
    ));
    let outputs = run_sync(&vpl, events);
    assert!(
        !outputs.is_empty(),
        "Expected 1+ data staging/exfil alert, got {}",
        outputs.len()
    );
    assert_eq!(
        outputs[0].data.get("rule"),
        Some(&Value::str("data_staging_exfil"))
    );
    assert_eq!(
        outputs[0].data.get("mitre"),
        Some(&Value::str("T1560,T1041"))
    );
}

#[test]
fn test_full_killchain() {
    let vpl = load_file("examples/security-demo/detect_full_killchain.vpl");
    let events = parse_jsonl_events(&load_file(
        "examples/security-demo/data/apt29_full_chain.jsonl",
    ));
    let outputs = run_sync(&vpl, events);
    assert!(
        !outputs.is_empty(),
        "Expected 1+ full kill chain alert, got {}",
        outputs.len()
    );
    assert_eq!(
        outputs[0].data.get("rule"),
        Some(&Value::str("full_killchain"))
    );
    assert_eq!(
        outputs[0].data.get("mitre"),
        Some(&Value::str("T1059,T1003.001,T1021.002,T1041"))
    );
    assert_eq!(
        outputs[0].data.get("severity"),
        Some(&Value::str("critical"))
    );
}

// =========================================================================
// Phase 4: Sigma Comparison
// =========================================================================

#[test]
fn test_sigma_misses_renamed_psexec() {
    let vpl = load_file("examples/security-demo/sigma_comparison/sigma_only.vpl");
    let events = parse_jsonl_events(&load_file(
        "examples/security-demo/sigma_comparison/evasion_dataset.jsonl",
    ));
    let outputs = run_sync(&vpl, events);
    assert_eq!(
        outputs.len(),
        0,
        "Sigma-style rule should NOT detect renamed PsExec, but got {} alerts",
        outputs.len()
    );
}

#[test]
fn test_vpl_catches_renamed_psexec() {
    let vpl = load_file("examples/security-demo/sigma_comparison/vpl_behavioral.vpl");
    let events = parse_jsonl_events(&load_file(
        "examples/security-demo/sigma_comparison/evasion_dataset.jsonl",
    ));
    let outputs = run_sync(&vpl, events);
    assert!(
        !outputs.is_empty(),
        "VPL behavioral rule should catch renamed PsExec, got {} alerts",
        outputs.len()
    );
    assert_eq!(
        outputs[0].data.get("rule"),
        Some(&Value::str("behavioral_psexec"))
    );
    assert_eq!(
        outputs[0].data.get("sigma_would_miss"),
        Some(&Value::str("true"))
    );
}

#[test]
fn test_both_catch_normal_psexec() {
    let sigma_vpl = load_file("examples/security-demo/sigma_comparison/sigma_only.vpl");
    let vpl_behavioral = load_file("examples/security-demo/sigma_comparison/vpl_behavioral.vpl");
    let events = parse_jsonl_events(&load_file(
        "examples/security-demo/sigma_comparison/normal_dataset.jsonl",
    ));

    let sigma_outputs = run_sync(&sigma_vpl, events.clone());
    assert!(
        !sigma_outputs.is_empty(),
        "Sigma should detect normal PsExec, got {} alerts",
        sigma_outputs.len()
    );

    let vpl_outputs = run_sync(&vpl_behavioral, events);
    assert!(
        !vpl_outputs.is_empty(),
        "VPL should detect normal PsExec, got {} alerts",
        vpl_outputs.len()
    );
}

// =========================================================================
// Phase 5: Multi-Rule Detection (directory mode)
// =========================================================================

#[test]
fn test_multi_rule_full_chain_detection() {
    // Simulates the detect command: run multiple rule files independently
    // against the same dataset and collect all alerts.
    let rule_files = [
        "examples/security-demo/detect_credential_dumping.vpl",
        "examples/security-demo/detect_lateral_movement.vpl",
        "examples/security-demo/detect_full_killchain.vpl",
    ];
    let events = parse_jsonl_events(&load_file(
        "examples/security-demo/data/apt29_full_chain.jsonl",
    ));

    let mut all_alerts = Vec::new();
    for rule_path in &rule_files {
        let vpl = load_file(rule_path);
        let outputs = run_sync(&vpl, events.clone());
        all_alerts.extend(outputs);
    }

    // Should get alerts from credential dumping, lateral movement, and full killchain
    assert!(
        all_alerts.len() >= 3,
        "Expected 3+ alerts from multi-rule detection, got {}",
        all_alerts.len()
    );

    // Verify we got different rule names
    let rules: std::collections::HashSet<String> = all_alerts
        .iter()
        .filter_map(|e| {
            e.data.get("rule").map(|v| match v {
                Value::Str(s) => s.to_string(),
                other => other.to_string(),
            })
        })
        .collect();
    assert!(
        rules.len() >= 2,
        "Expected alerts from 2+ different rules, got: {:?}",
        rules
    );
}

#[test]
fn test_evasion_coverage_matrix() {
    // Simulates the analyze command: run rules against both baseline
    // and evasion datasets, verify the expected coverage pattern.
    let sigma_vpl = load_file("examples/security-demo/sigma_comparison/sigma_only.vpl");
    let behavioral_vpl = load_file("examples/security-demo/sigma_comparison/vpl_behavioral.vpl");
    let baseline_events = parse_jsonl_events(&load_file(
        "examples/security-demo/sigma_comparison/normal_dataset.jsonl",
    ));
    let evasion_events = parse_jsonl_events(&load_file(
        "examples/security-demo/sigma_comparison/evasion_dataset.jsonl",
    ));

    // Sigma: detects baseline, misses evasion
    let sigma_baseline = run_sync(&sigma_vpl, baseline_events.clone());
    let sigma_evasion = run_sync(&sigma_vpl, evasion_events.clone());
    assert!(!sigma_baseline.is_empty(), "Sigma should detect baseline");
    assert!(sigma_evasion.is_empty(), "Sigma should miss evasion");

    // Behavioral: detects both
    let behavioral_baseline = run_sync(&behavioral_vpl, baseline_events);
    let behavioral_evasion = run_sync(&behavioral_vpl, evasion_events);
    assert!(
        !behavioral_baseline.is_empty(),
        "Behavioral should detect baseline"
    );
    assert!(
        !behavioral_evasion.is_empty(),
        "Behavioral should detect evasion"
    );
}
