//! Checkpoint serialization codec.
//!
//! Provides format-agnostic serialization for checkpoint data.
//! - **Default (JSON)**: Human-readable, backward compatible with all existing checkpoints.
//! - **Binary (MessagePack)**: Compact and fast, enabled via the `binary-codec` feature flag.
//!
//! The codec auto-detects the format on deserialization by inspecting the first byte,
//! so checkpoints written in either format can always be read back regardless of
//! which feature is currently enabled.

use crate::persistence::StoreError;
use serde::{de::DeserializeOwned, Serialize};

/// Serialization format for checkpoint data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointFormat {
    /// JSON (default) — human-readable, universally supported.
    Json,
    /// MessagePack — compact binary format, ~2-4x smaller and faster.
    #[cfg(feature = "binary-codec")]
    MessagePack,
}

impl CheckpointFormat {
    /// Returns the active checkpoint format based on enabled features.
    ///
    /// With the `binary-codec` feature: returns `MessagePack` (compact, fast).
    /// Without: returns `Json` (human-readable, universal).
    pub fn active() -> Self {
        #[cfg(feature = "binary-codec")]
        {
            CheckpointFormat::MessagePack
        }
        #[cfg(not(feature = "binary-codec"))]
        {
            CheckpointFormat::Json
        }
    }
}

/// Serialize a value using the specified format.
pub fn serialize<T: Serialize>(value: &T, format: CheckpointFormat) -> Result<Vec<u8>, StoreError> {
    match format {
        CheckpointFormat::Json => {
            serde_json::to_vec(value).map_err(|e| StoreError::SerializationError(e.to_string()))
        }
        #[cfg(feature = "binary-codec")]
        CheckpointFormat::MessagePack => {
            rmp_serde::to_vec(value).map_err(|e| StoreError::SerializationError(e.to_string()))
        }
    }
}

/// Deserialize a value, auto-detecting the format from the data.
///
/// Detection heuristic:
/// - Bytes starting with `{` (0x7B) or `[` (0x5B) → JSON
/// - Everything else → MessagePack (if feature enabled), otherwise try JSON
pub fn deserialize<T: DeserializeOwned>(data: &[u8]) -> Result<T, StoreError> {
    if data.is_empty() {
        return Err(StoreError::SerializationError(
            "empty checkpoint data".to_string(),
        ));
    }

    if is_json(data) {
        serde_json::from_slice(data).map_err(|e| StoreError::SerializationError(e.to_string()))
    } else {
        #[cfg(feature = "binary-codec")]
        {
            rmp_serde::from_slice(data).map_err(|e| StoreError::SerializationError(e.to_string()))
        }
        #[cfg(not(feature = "binary-codec"))]
        {
            // No binary codec available — try JSON anyway as a fallback
            serde_json::from_slice(data).map_err(|e| StoreError::SerializationError(e.to_string()))
        }
    }
}

/// Check if data looks like JSON (starts with `{` or `[`, ignoring whitespace).
fn is_json(data: &[u8]) -> bool {
    data.iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|&b| b == b'{' || b == b'[')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{EngineCheckpoint, CHECKPOINT_VERSION};

    #[test]
    fn test_json_roundtrip() {
        let cp = EngineCheckpoint {
            version: CHECKPOINT_VERSION,
            window_states: Default::default(),
            sase_states: Default::default(),
            join_states: Default::default(),
            variables: Default::default(),
            events_processed: 42,
            output_events_emitted: 7,
            watermark_state: None,
            distinct_states: Default::default(),
            limit_states: Default::default(),
        };

        let data = serialize(&cp, CheckpointFormat::Json).unwrap();
        assert!(is_json(&data), "JSON serialization should produce JSON");

        let restored: EngineCheckpoint = deserialize(&data).unwrap();
        assert_eq!(restored.events_processed, 42);
        assert_eq!(restored.output_events_emitted, 7);
        assert_eq!(restored.version, CHECKPOINT_VERSION);
    }

    #[cfg(feature = "binary-codec")]
    #[test]
    fn test_msgpack_roundtrip() {
        let cp = EngineCheckpoint {
            version: CHECKPOINT_VERSION,
            window_states: Default::default(),
            sase_states: Default::default(),
            join_states: Default::default(),
            variables: Default::default(),
            events_processed: 1000,
            output_events_emitted: 500,
            watermark_state: None,
            distinct_states: Default::default(),
            limit_states: Default::default(),
        };

        let data = serialize(&cp, CheckpointFormat::MessagePack).unwrap();
        assert!(!is_json(&data), "MessagePack should not look like JSON");

        let restored: EngineCheckpoint = deserialize(&data).unwrap();
        assert_eq!(restored.events_processed, 1000);
        assert_eq!(restored.output_events_emitted, 500);
    }

    #[cfg(feature = "binary-codec")]
    #[test]
    fn test_msgpack_smaller_than_json() {
        use crate::persistence::Checkpoint;
        use std::collections::HashMap;

        let cp = Checkpoint {
            id: 1,
            timestamp_ms: 1700000000000,
            events_processed: 100_000,
            window_states: HashMap::new(),
            pattern_states: HashMap::new(),
            metadata: {
                let mut m = HashMap::new();
                m.insert("stream".to_string(), "TestStream".to_string());
                m.insert("tenant".to_string(), "test-tenant-id".to_string());
                m
            },
            context_states: HashMap::new(),
        };

        let json_data = serialize(&cp, CheckpointFormat::Json).unwrap();
        let msgpack_data = serialize(&cp, CheckpointFormat::MessagePack).unwrap();

        assert!(
            msgpack_data.len() < json_data.len(),
            "MessagePack ({} bytes) should be smaller than JSON ({} bytes)",
            msgpack_data.len(),
            json_data.len()
        );
    }

    #[cfg(feature = "binary-codec")]
    #[test]
    fn test_cross_format_deserialize() {
        let cp = EngineCheckpoint {
            version: CHECKPOINT_VERSION,
            window_states: Default::default(),
            sase_states: Default::default(),
            join_states: Default::default(),
            variables: Default::default(),
            events_processed: 99,
            output_events_emitted: 33,
            watermark_state: None,
            distinct_states: Default::default(),
            limit_states: Default::default(),
        };

        let json_data = serialize(&cp, CheckpointFormat::Json).unwrap();
        let msgpack_data = serialize(&cp, CheckpointFormat::MessagePack).unwrap();

        // Both should deserialize correctly via auto-detect
        let from_json: EngineCheckpoint = deserialize(&json_data).unwrap();
        let from_msgpack: EngineCheckpoint = deserialize(&msgpack_data).unwrap();

        assert_eq!(from_json.events_processed, 99);
        assert_eq!(from_msgpack.events_processed, 99);
    }

    #[test]
    fn test_empty_data_error() {
        let result = deserialize::<EngineCheckpoint>(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_active_format() {
        let format = CheckpointFormat::active();
        #[cfg(feature = "binary-codec")]
        assert_eq!(format, CheckpointFormat::MessagePack);
        #[cfg(not(feature = "binary-codec"))]
        assert_eq!(format, CheckpointFormat::Json);
    }
}
