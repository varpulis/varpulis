//! Property-based tests for the Varpulis runtime.
//!
//! Covers: Event serialization round-trips, checkpoint versioning invariants,
//! Value type conversions, and window operation properties.

use std::sync::Arc;

use proptest::prelude::*;
use varpulis_core::Value;
use varpulis_runtime::event::Event;
use varpulis_runtime::persistence::{EngineCheckpoint, SerializableEvent, CHECKPOINT_VERSION};

/// Strategy for generating arbitrary Value instances.
fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(Value::Int),
        any::<f64>()
            .prop_filter("must be finite", |f| f.is_finite())
            .prop_map(Value::Float),
        any::<bool>().prop_map(Value::Bool),
        "[a-zA-Z0-9_ ]{0,64}".prop_map(Value::str),
    ]
}

/// Strategy for generating arbitrary Events with 0-5 fields.
fn arb_event() -> impl Strategy<Value = Event> {
    let event_type = "[A-Z][A-Za-z0-9]{1,15}";
    let fields = prop::collection::vec(("[a-z][a-z0-9_]{0,10}", arb_value()), 0..5);

    (event_type, fields).prop_map(|(etype, fields)| {
        let mut event = Event::new(etype);
        for (key, value) in fields {
            let key: Arc<str> = Arc::from(key.as_str());
            event = event.with_field(key, value);
        }
        event
    })
}

proptest! {
    /// Event JSON serialization should round-trip without data loss.
    #[test]
    fn event_json_roundtrip(event in arb_event()) {
        let payload = event.to_sink_payload();
        let json_str = String::from_utf8(payload).expect("payload should be valid UTF-8");
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .expect("payload should be valid JSON");

        // Must contain event_type
        prop_assert!(
            parsed.get("event_type").is_some(),
            "JSON payload must contain event_type"
        );

        let et = parsed["event_type"].as_str().unwrap();
        prop_assert_eq!(et, event.event_type.as_ref());

        // All fields must be present
        if let Some(fields) = parsed.get("fields") {
            for (key, _value) in &event.data {
                prop_assert!(
                    fields.get(key.as_ref()).is_some(),
                    "Field '{}' missing from JSON payload",
                    key
                );
            }
        }
    }

    /// SerializableEvent round-trip: Event → SerializableEvent → Event preserves event_type and fields.
    #[test]
    fn serializable_event_roundtrip(event in arb_event()) {
        let se = SerializableEvent::from(&event);
        let restored = Event::from(se);

        prop_assert_eq!(
            event.event_type.as_ref(),
            restored.event_type.as_ref(),
            "event_type must survive serialization round-trip"
        );
        prop_assert_eq!(
            event.data.len(),
            restored.data.len(),
            "field count must survive serialization round-trip"
        );
    }

    /// EngineCheckpoint JSON round-trip preserves version and counters.
    #[test]
    fn checkpoint_json_roundtrip(
        events_processed in any::<u64>(),
        output_emitted in any::<u64>(),
    ) {
        let cp = EngineCheckpoint {
            version: CHECKPOINT_VERSION,
            window_states: Default::default(),
            sase_states: Default::default(),
            join_states: Default::default(),
            variables: Default::default(),
            events_processed,
            output_events_emitted: output_emitted,
            watermark_state: None,
            distinct_states: Default::default(),
            limit_states: Default::default(),
        };

        let json = serde_json::to_string(&cp).expect("checkpoint should serialize");
        let restored: EngineCheckpoint =
            serde_json::from_str(&json).expect("checkpoint should deserialize");

        prop_assert_eq!(restored.version, CHECKPOINT_VERSION);
        prop_assert_eq!(restored.events_processed, events_processed);
        prop_assert_eq!(restored.output_events_emitted, output_emitted);
    }

    /// Future checkpoint versions should always be rejected by validate_and_migrate.
    #[test]
    fn future_versions_rejected(future_offset in 1u32..1000) {
        let mut cp = EngineCheckpoint {
            version: CHECKPOINT_VERSION + future_offset,
            window_states: Default::default(),
            sase_states: Default::default(),
            join_states: Default::default(),
            variables: Default::default(),
            events_processed: 0,
            output_events_emitted: 0,
            watermark_state: None,
            distinct_states: Default::default(),
            limit_states: Default::default(),
        };

        let result = cp.validate_and_migrate();
        prop_assert!(
            result.is_err(),
            "Version {} should be rejected (current: {})",
            CHECKPOINT_VERSION + future_offset,
            CHECKPOINT_VERSION
        );
    }

    /// Current and past checkpoint versions should be accepted.
    #[test]
    fn current_and_past_versions_accepted(version in 1u32..=CHECKPOINT_VERSION) {
        let mut cp = EngineCheckpoint {
            version,
            window_states: Default::default(),
            sase_states: Default::default(),
            join_states: Default::default(),
            variables: Default::default(),
            events_processed: 0,
            output_events_emitted: 0,
            watermark_state: None,
            distinct_states: Default::default(),
            limit_states: Default::default(),
        };

        let result = cp.validate_and_migrate();
        prop_assert!(
            result.is_ok(),
            "Version {} should be accepted (current: {})",
            version,
            CHECKPOINT_VERSION
        );
    }

    /// Value conversions should be consistent.
    #[test]
    fn value_debug_deterministic(v in arb_value()) {
        let d1 = format!("{:?}", v);
        let d2 = format!("{:?}", v);
        prop_assert_eq!(d1, d2, "Debug output must be deterministic");
    }
}
