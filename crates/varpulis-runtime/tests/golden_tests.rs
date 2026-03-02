//! Golden file snapshot tests for the Varpulis engine.
//!
//! Each fixture directory under `tests/golden/engine/` contains:
//! - `program.vpl` — VPL source
//! - `input.evt` — input events (EventFileParser format)
//! - `output.snap` — insta snapshot of output events (auto-generated)
//!
//! Run `cargo insta review` to interactively approve snapshot changes.

use std::collections::BTreeMap;
use std::path::Path;

use varpulis_parser::parse;
use varpulis_runtime::event_file::EventFileParser;
use varpulis_runtime::{Engine, Event};

/// Serializable event representation for stable snapshots.
///
/// Uses `BTreeMap` for deterministic field ordering and excludes
/// timestamps for reproducibility.
#[derive(Debug, serde::Serialize)]
struct SnapshotEvent {
    event_type: String,
    fields: BTreeMap<String, serde_json::Value>,
}

impl From<&Event> for SnapshotEvent {
    fn from(event: &Event) -> Self {
        let mut fields = BTreeMap::new();
        for (key, value) in &event.data {
            let json_val = match value {
                varpulis_core::Value::Null => serde_json::Value::Null,
                varpulis_core::Value::Bool(b) => serde_json::Value::Bool(*b),
                varpulis_core::Value::Int(i) => serde_json::json!(*i),
                varpulis_core::Value::Float(f) => serde_json::json!(*f),
                varpulis_core::Value::Str(s) => serde_json::Value::String(s.to_string()),
                _ => serde_json::Value::String(format!("{value}")),
            };
            fields.insert(key.to_string(), json_val);
        }
        Self {
            event_type: event.event_type.to_string(),
            fields,
        }
    }
}

/// Run a single fixture: parse VPL, feed events, collect outputs, snapshot.
fn run_fixture(fixture_dir: &Path) -> Vec<SnapshotEvent> {
    let vpl_path = fixture_dir.join("program.vpl");
    let evt_path = fixture_dir.join("input.evt");

    let vpl_source = std::fs::read_to_string(&vpl_path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {}", vpl_path.display(), e));
    let evt_source = std::fs::read_to_string(&evt_path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {}", evt_path.display(), e));

    let program = parse(&vpl_source)
        .unwrap_or_else(|e| panic!("Parse failed for {}: {}", vpl_path.display(), e));

    let (tx, mut rx) = tokio::sync::mpsc::channel(10_000);
    let mut engine = Engine::new(tx);
    engine
        .load(&program)
        .unwrap_or_else(|e| panic!("Load failed for {}: {}", vpl_path.display(), e));

    let timed_events = EventFileParser::parse(&evt_source)
        .unwrap_or_else(|e| panic!("Event parse failed for {}: {}", evt_path.display(), e));

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for timed in &timed_events {
            engine.process(timed.event.clone()).await.unwrap();
        }
    });

    let mut outputs = Vec::new();
    while let Ok(event) = rx.try_recv() {
        outputs.push(SnapshotEvent::from(&event));
    }
    outputs
}

/// Macro to generate one test function per fixture directory.
macro_rules! golden_test {
    ($name:ident, $dir:expr) => {
        #[test]
        fn $name() {
            let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/golden/engine")
                .join($dir);
            let outputs = run_fixture(&fixture_dir);
            insta::assert_yaml_snapshot!(concat!("engine_", $dir), outputs);
        }
    };
}

golden_test!(golden_high_temp_alert, "high_temp_alert");
golden_test!(golden_order_payment, "order_payment");
golden_test!(golden_moving_average, "moving_average");
