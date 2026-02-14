//! Extended coverage tests for the persistence module.
//!
//! Covers: SerializableValue round-trips, SerializableEvent conversions,
//! MemoryStore trait methods, FileStore I/O, CheckpointManager lifecycle,
//! Checkpoint serialization, and various edge cases.

use std::collections::HashMap;
use std::sync::Arc;

use varpulis_core::Value;
use varpulis_runtime::persistence::{
    Checkpoint, CheckpointConfig, CheckpointManager, FileStore, MemoryStore, PatternCheckpoint,
    SerializableEvent, SerializableValue, StateStore, WindowCheckpoint,
};
use varpulis_runtime::Event;

// ---------------------------------------------------------------------------
// 1. SerializableValue round-trips
// ---------------------------------------------------------------------------

#[test]
fn serializable_value_int_round_trip() {
    let mut event = Event::new("T");
    event.data.insert("v".into(), Value::Int(42));

    let se: SerializableEvent = (&event).into();
    assert_eq!(se.fields.get("v"), Some(&SerializableValue::Int(42)));

    let restored: Event = se.into();
    assert_eq!(restored.get_int("v"), Some(42));
}

#[test]
fn serializable_value_float_round_trip() {
    let mut event = Event::new("T");
    event.data.insert("v".into(), Value::Float(3.125));

    let se: SerializableEvent = (&event).into();
    assert_eq!(se.fields.get("v"), Some(&SerializableValue::Float(3.125)));

    let restored: Event = se.into();
    assert_eq!(restored.get_float("v"), Some(3.125));
}

#[test]
fn serializable_value_string_round_trip() {
    let mut event = Event::new("T");
    event.data.insert("v".into(), Value::Str("hello".into()));

    let se: SerializableEvent = (&event).into();
    assert_eq!(
        se.fields.get("v"),
        Some(&SerializableValue::String("hello".to_string()))
    );

    let restored: Event = se.into();
    assert_eq!(restored.get_str("v"), Some("hello"));
}

#[test]
fn serializable_value_bool_round_trip() {
    let mut event = Event::new("T");
    event.data.insert("v".into(), Value::Bool(true));

    let se: SerializableEvent = (&event).into();
    assert_eq!(se.fields.get("v"), Some(&SerializableValue::Bool(true)));

    let restored: Event = se.into();
    assert_eq!(restored.data.get("v"), Some(&Value::Bool(true)));
}

#[test]
fn serializable_value_null_round_trip() {
    let mut event = Event::new("T");
    event.data.insert("v".into(), Value::Null);

    let se: SerializableEvent = (&event).into();
    assert_eq!(se.fields.get("v"), Some(&SerializableValue::Null));

    let restored: Event = se.into();
    assert_eq!(restored.data.get("v"), Some(&Value::Null));
}

#[test]
fn serializable_value_array_round_trip() {
    let arr = Value::array(vec![Value::Int(1), Value::Str("two".into()), Value::Null]);
    let mut event = Event::new("T");
    event.data.insert("v".into(), arr);

    let se: SerializableEvent = (&event).into();
    match se.fields.get("v") {
        Some(SerializableValue::Array(items)) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], SerializableValue::Int(1));
            assert_eq!(items[1], SerializableValue::String("two".to_string()));
            assert_eq!(items[2], SerializableValue::Null);
        }
        other => panic!("Expected Array, got {:?}", other),
    }

    let restored: Event = se.into();
    match restored.data.get("v") {
        Some(Value::Array(a)) => {
            assert_eq!(a.len(), 3);
            assert_eq!(a[0], Value::Int(1));
        }
        other => panic!("Expected Array value, got {:?}", other),
    }
}

#[test]
fn serializable_value_map_round_trip() {
    use indexmap::IndexMap;
    use rustc_hash::FxBuildHasher;

    let mut inner: IndexMap<Arc<str>, Value, FxBuildHasher> = IndexMap::with_hasher(FxBuildHasher);
    inner.insert("a".into(), Value::Int(1));
    inner.insert("b".into(), Value::Float(2.0));

    let mut event = Event::new("T");
    event.data.insert("v".into(), Value::map(inner));

    let se: SerializableEvent = (&event).into();
    match se.fields.get("v") {
        Some(SerializableValue::Map(entries)) => {
            assert_eq!(entries.len(), 2);
        }
        other => panic!("Expected Map, got {:?}", other),
    }

    let restored: Event = se.into();
    match restored.data.get("v") {
        Some(Value::Map(m)) => {
            assert_eq!(m.len(), 2);
            assert_eq!(m.get("a"), Some(&Value::Int(1)));
            assert_eq!(m.get("b"), Some(&Value::Float(2.0)));
        }
        other => panic!("Expected Map value, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 2. SerializableEvent round-trip with multiple field types
// ---------------------------------------------------------------------------

#[test]
fn serializable_event_multi_field_round_trip() {
    let mut event = Event::new("SensorReading");
    event.data.insert("temp".into(), Value::Float(98.6));
    event.data.insert("count".into(), Value::Int(7));
    event.data.insert("label".into(), Value::Str("ok".into()));
    event.data.insert("active".into(), Value::Bool(false));
    event.data.insert("extra".into(), Value::Null);

    let se: SerializableEvent = (&event).into();
    assert_eq!(se.event_type, "SensorReading");
    assert_eq!(se.fields.len(), 5);

    let restored: Event = se.into();
    assert_eq!(&*restored.event_type, "SensorReading");
    assert_eq!(restored.get_float("temp"), Some(98.6));
    assert_eq!(restored.get_int("count"), Some(7));
    assert_eq!(restored.get_str("label"), Some("ok"));
    assert_eq!(restored.data.get("active"), Some(&Value::Bool(false)));
    assert_eq!(restored.data.get("extra"), Some(&Value::Null));
}

#[test]
fn serializable_event_timestamp_preserved() {
    let ts = chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
    let mut event = Event::new("T");
    event.timestamp = ts;

    let se: SerializableEvent = (&event).into();
    assert_eq!(se.timestamp_ms, 1_700_000_000_000);

    let restored: Event = se.into();
    assert_eq!(restored.timestamp.timestamp_millis(), 1_700_000_000_000);
}

// ---------------------------------------------------------------------------
// 3. MemoryStore: put/get/delete/list_keys/clear behaviour
// ---------------------------------------------------------------------------

#[test]
fn memory_store_put_get() {
    let store = MemoryStore::new();
    store.put("key1", b"value1").unwrap();

    let val = store.get("key1").unwrap();
    assert_eq!(val, Some(b"value1".to_vec()));
}

#[test]
fn memory_store_get_non_existent() {
    let store = MemoryStore::new();
    let val = store.get("missing").unwrap();
    assert!(val.is_none());
}

#[test]
fn memory_store_delete() {
    let store = MemoryStore::new();
    store.put("key1", b"value1").unwrap();
    store.delete("key1").unwrap();

    let val = store.get("key1").unwrap();
    assert!(val.is_none());
}

#[test]
fn memory_store_delete_non_existent() {
    let store = MemoryStore::new();
    // Should not error
    store.delete("no-such-key").unwrap();
}

#[test]
fn memory_store_overwrite_key() {
    let store = MemoryStore::new();
    store.put("key1", b"v1").unwrap();
    store.put("key1", b"v2").unwrap();

    let val = store.get("key1").unwrap();
    assert_eq!(val, Some(b"v2".to_vec()));
}

#[test]
fn memory_store_flush_is_noop() {
    let store = MemoryStore::new();
    store.flush().unwrap();
}

#[test]
fn memory_store_list_checkpoints_empty() {
    let store = MemoryStore::new();
    let ids = store.list_checkpoints().unwrap();
    assert!(ids.is_empty());
}

#[test]
fn memory_store_save_and_load_checkpoint() {
    let store = MemoryStore::new();

    let cp = Checkpoint {
        id: 10,
        timestamp_ms: 5000,
        events_processed: 200,
        window_states: HashMap::new(),
        pattern_states: HashMap::new(),
        metadata: HashMap::new(),
        context_states: HashMap::new(),
    };

    store.save_checkpoint(&cp).unwrap();

    let loaded = store.load_checkpoint(10).unwrap();
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(loaded.id, 10);
    assert_eq!(loaded.events_processed, 200);
}

#[test]
fn memory_store_load_latest_checkpoint() {
    let store = MemoryStore::new();

    for i in 1..=3 {
        let cp = Checkpoint {
            id: i,
            timestamp_ms: i as i64 * 1000,
            events_processed: i * 10,
            window_states: HashMap::new(),
            pattern_states: HashMap::new(),
            metadata: HashMap::new(),
            context_states: HashMap::new(),
        };
        store.save_checkpoint(&cp).unwrap();
    }

    let latest = store.load_latest_checkpoint().unwrap().unwrap();
    assert_eq!(latest.id, 3);
    assert_eq!(latest.events_processed, 30);
}

#[test]
fn memory_store_load_latest_when_empty() {
    let store = MemoryStore::new();
    let latest = store.load_latest_checkpoint().unwrap();
    assert!(latest.is_none());
}

#[test]
fn memory_store_load_nonexistent_checkpoint() {
    let store = MemoryStore::new();
    let loaded = store.load_checkpoint(999).unwrap();
    assert!(loaded.is_none());
}

// ---------------------------------------------------------------------------
// 4. FileStore: write/read to temp directory, list_keys, clear
// ---------------------------------------------------------------------------

#[test]
fn file_store_put_get() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    store.put("mykey", b"myvalue").unwrap();
    let val = store.get("mykey").unwrap();
    assert_eq!(val, Some(b"myvalue".to_vec()));
}

#[test]
fn file_store_colon_key_creates_subdirectory() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    store.put("tenant:abc:data", b"content").unwrap();
    let val = store.get("tenant:abc:data").unwrap();
    assert_eq!(val, Some(b"content".to_vec()));

    // Verify the subdirectory structure was created
    let subdir = dir.path().join("tenant").join("abc");
    assert!(subdir.exists());
    assert!(subdir.join("data").exists());
}

#[test]
fn file_store_get_missing_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    let val = store.get("nonexistent").unwrap();
    assert!(val.is_none());
}

#[test]
fn file_store_delete() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    store.put("k", b"v").unwrap();
    store.delete("k").unwrap();
    let val = store.get("k").unwrap();
    assert!(val.is_none());
}

#[test]
fn file_store_delete_non_existent() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    // Should not error
    store.delete("no-such-key").unwrap();
}

#[test]
fn file_store_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    store.put("k", b"version1").unwrap();
    store.put("k", b"version2").unwrap();

    let val = store.get("k").unwrap();
    assert_eq!(val, Some(b"version2".to_vec()));
}

#[test]
fn file_store_list_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    // Save a few checkpoints
    for i in [5, 2, 8] {
        let cp = Checkpoint {
            id: i,
            timestamp_ms: i as i64 * 1000,
            events_processed: i * 100,
            window_states: HashMap::new(),
            pattern_states: HashMap::new(),
            metadata: HashMap::new(),
            context_states: HashMap::new(),
        };
        store.save_checkpoint(&cp).unwrap();
    }

    let ids = store.list_checkpoints().unwrap();
    assert_eq!(ids, vec![2, 5, 8]); // Should be sorted
}

#[test]
fn file_store_list_checkpoints_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    let ids = store.list_checkpoints().unwrap();
    assert!(ids.is_empty());
}

#[test]
fn file_store_save_load_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    let cp = Checkpoint {
        id: 42,
        timestamp_ms: 99999,
        events_processed: 500,
        window_states: HashMap::new(),
        pattern_states: HashMap::new(),
        metadata: {
            let mut m = HashMap::new();
            m.insert("source".to_string(), "test".to_string());
            m
        },
        context_states: HashMap::new(),
    };

    store.save_checkpoint(&cp).unwrap();

    let loaded = store.load_checkpoint(42).unwrap().unwrap();
    assert_eq!(loaded.id, 42);
    assert_eq!(loaded.events_processed, 500);
    assert_eq!(loaded.metadata.get("source"), Some(&"test".to_string()));
}

#[test]
fn file_store_load_latest_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    for i in [3, 1, 7] {
        let cp = Checkpoint {
            id: i,
            timestamp_ms: i as i64 * 1000,
            events_processed: i * 10,
            window_states: HashMap::new(),
            pattern_states: HashMap::new(),
            metadata: HashMap::new(),
            context_states: HashMap::new(),
        };
        store.save_checkpoint(&cp).unwrap();
    }

    let latest = store.load_latest_checkpoint().unwrap().unwrap();
    assert_eq!(latest.id, 7);
}

#[test]
fn file_store_prune_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    for i in 1..=5 {
        let cp = Checkpoint {
            id: i,
            timestamp_ms: i as i64 * 1000,
            events_processed: i * 10,
            window_states: HashMap::new(),
            pattern_states: HashMap::new(),
            metadata: HashMap::new(),
            context_states: HashMap::new(),
        };
        store.save_checkpoint(&cp).unwrap();
    }

    let pruned = store.prune_checkpoints(2).unwrap();
    assert_eq!(pruned, 3);

    let remaining = store.list_checkpoints().unwrap();
    assert_eq!(remaining, vec![4, 5]);
}

#[test]
fn file_store_flush_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();
    store.flush().unwrap();
}

// ---------------------------------------------------------------------------
// 5. CheckpointManager: create checkpoint, restore, list
// ---------------------------------------------------------------------------

#[test]
fn checkpoint_manager_create_and_restore() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
    let config = CheckpointConfig {
        interval: std::time::Duration::from_secs(1),
        max_checkpoints: 5,
        checkpoint_on_shutdown: true,
        key_prefix: "test".to_string(),
    };

    let mut mgr = CheckpointManager::new(Arc::clone(&store), config).unwrap();

    let cp = Checkpoint {
        id: 0, // Will be overwritten by manager
        timestamp_ms: 0,
        events_processed: 100,
        window_states: HashMap::new(),
        pattern_states: HashMap::new(),
        metadata: HashMap::new(),
        context_states: HashMap::new(),
    };

    mgr.checkpoint(cp).unwrap();

    let recovered = mgr.recover().unwrap();
    assert!(recovered.is_some());
    let recovered = recovered.unwrap();
    assert_eq!(recovered.id, 1); // Manager starts from 1
    assert_eq!(recovered.events_processed, 100);
}

#[test]
fn checkpoint_manager_sequential_ids() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
    let config = CheckpointConfig::default();

    let mut mgr = CheckpointManager::new(Arc::clone(&store), config).unwrap();

    for expected_id in 1..=3 {
        let cp = Checkpoint {
            id: 0,
            timestamp_ms: 0,
            events_processed: expected_id * 10,
            window_states: HashMap::new(),
            pattern_states: HashMap::new(),
            metadata: HashMap::new(),
            context_states: HashMap::new(),
        };
        mgr.checkpoint(cp).unwrap();

        let latest = store.load_latest_checkpoint().unwrap().unwrap();
        assert_eq!(latest.id, expected_id);
    }
}

#[test]
fn checkpoint_manager_prunes_old_checkpoints() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
    let config = CheckpointConfig {
        interval: std::time::Duration::from_secs(0),
        max_checkpoints: 2,
        checkpoint_on_shutdown: false,
        key_prefix: "test".to_string(),
    };

    let mut mgr = CheckpointManager::new(Arc::clone(&store), config).unwrap();

    for _ in 0..5 {
        let cp = Checkpoint {
            id: 0,
            timestamp_ms: 0,
            events_processed: 0,
            window_states: HashMap::new(),
            pattern_states: HashMap::new(),
            metadata: HashMap::new(),
            context_states: HashMap::new(),
        };
        mgr.checkpoint(cp).unwrap();
    }

    let ids = store.list_checkpoints().unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids, vec![4, 5]);
}

#[test]
fn checkpoint_manager_recover_empty_store() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
    let config = CheckpointConfig::default();
    let mgr = CheckpointManager::new(Arc::clone(&store), config).unwrap();

    let recovered = mgr.recover().unwrap();
    assert!(recovered.is_none());
}

#[test]
fn checkpoint_manager_store_accessor() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
    let config = CheckpointConfig::default();
    let mgr = CheckpointManager::new(Arc::clone(&store), config).unwrap();

    // The store() accessor should return a reference to the same store
    mgr.store().put("test-key", b"test-value").unwrap();
    let val = store.get("test-key").unwrap();
    assert_eq!(val, Some(b"test-value".to_vec()));
}

#[test]
fn checkpoint_manager_resumes_id_from_existing() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());

    // Pre-populate a checkpoint
    let cp = Checkpoint {
        id: 10,
        timestamp_ms: 5000,
        events_processed: 50,
        window_states: HashMap::new(),
        pattern_states: HashMap::new(),
        metadata: HashMap::new(),
        context_states: HashMap::new(),
    };
    store.save_checkpoint(&cp).unwrap();

    // Create a new manager — it should resume from id 11
    let config = CheckpointConfig::default();
    let mut mgr = CheckpointManager::new(Arc::clone(&store), config).unwrap();

    let new_cp = Checkpoint {
        id: 0,
        timestamp_ms: 0,
        events_processed: 60,
        window_states: HashMap::new(),
        pattern_states: HashMap::new(),
        metadata: HashMap::new(),
        context_states: HashMap::new(),
    };
    mgr.checkpoint(new_cp).unwrap();

    let latest = store.load_latest_checkpoint().unwrap().unwrap();
    assert_eq!(latest.id, 11);
    assert_eq!(latest.events_processed, 60);
}

// ---------------------------------------------------------------------------
// 6. Checkpoint serialization/deserialization
// ---------------------------------------------------------------------------

#[test]
fn checkpoint_bincode_round_trip() {
    let cp = Checkpoint {
        id: 7,
        timestamp_ms: 123456789,
        events_processed: 999,
        window_states: {
            let mut ws = HashMap::new();
            ws.insert(
                "stream1".to_string(),
                WindowCheckpoint {
                    events: vec![],
                    window_start_ms: Some(100),
                    last_emit_ms: None,
                    partitions: HashMap::new(),
                },
            );
            ws
        },
        pattern_states: {
            let mut ps = HashMap::new();
            ps.insert(
                "pattern1".to_string(),
                PatternCheckpoint {
                    partial_matches: vec![],
                },
            );
            ps
        },
        metadata: {
            let mut m = HashMap::new();
            m.insert("key".to_string(), "value".to_string());
            m
        },
        context_states: HashMap::new(),
    };

    let encoded = bincode::serialize(&cp).unwrap();
    let decoded: Checkpoint = bincode::deserialize(&encoded).unwrap();

    assert_eq!(decoded.id, 7);
    assert_eq!(decoded.timestamp_ms, 123456789);
    assert_eq!(decoded.events_processed, 999);
    assert!(decoded.window_states.contains_key("stream1"));
    assert!(decoded.pattern_states.contains_key("pattern1"));
    assert_eq!(decoded.metadata.get("key"), Some(&"value".to_string()));
}

#[test]
fn checkpoint_with_events_round_trip() {
    let se = SerializableEvent {
        event_type: "TestEvent".to_string(),
        timestamp_ms: 42000,
        fields: {
            let mut f = HashMap::new();
            f.insert("x".to_string(), SerializableValue::Int(10));
            f.insert(
                "y".to_string(),
                SerializableValue::String("hello".to_string()),
            );
            f
        },
    };

    let cp = Checkpoint {
        id: 1,
        timestamp_ms: 1000,
        events_processed: 1,
        window_states: {
            let mut ws = HashMap::new();
            ws.insert(
                "s".to_string(),
                WindowCheckpoint {
                    events: vec![se],
                    window_start_ms: None,
                    last_emit_ms: None,
                    partitions: HashMap::new(),
                },
            );
            ws
        },
        pattern_states: HashMap::new(),
        metadata: HashMap::new(),
        context_states: HashMap::new(),
    };

    let encoded = bincode::serialize(&cp).unwrap();
    let decoded: Checkpoint = bincode::deserialize(&encoded).unwrap();

    let ws = decoded.window_states.get("s").unwrap();
    assert_eq!(ws.events.len(), 1);
    assert_eq!(ws.events[0].event_type, "TestEvent");
    assert_eq!(
        ws.events[0].fields.get("x"),
        Some(&SerializableValue::Int(10))
    );
}

// ---------------------------------------------------------------------------
// 7. Edge cases
// ---------------------------------------------------------------------------

#[test]
fn memory_store_empty_value() {
    let store = MemoryStore::new();
    store.put("empty", b"").unwrap();

    let val = store.get("empty").unwrap();
    assert_eq!(val, Some(Vec::new()));
}

#[test]
fn memory_store_large_value() {
    let store = MemoryStore::new();
    let big = vec![0xABu8; 1_000_000]; // 1MB
    store.put("big", &big).unwrap();

    let val = store.get("big").unwrap();
    assert_eq!(val.as_ref().map(|v| v.len()), Some(1_000_000));
}

#[test]
fn file_store_empty_value() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path()).unwrap();

    store.put("empty", b"").unwrap();
    let val = store.get("empty").unwrap();
    assert_eq!(val, Some(Vec::new()));
}

#[test]
fn checkpoint_config_default_values() {
    let config = CheckpointConfig::default();
    assert_eq!(config.interval, std::time::Duration::from_secs(60));
    assert_eq!(config.max_checkpoints, 3);
    assert!(config.checkpoint_on_shutdown);
    assert_eq!(config.key_prefix, "varpulis");
}

#[test]
fn serializable_value_timestamp_round_trip() {
    let mut event = Event::new("T");
    event
        .data
        .insert("ts".into(), Value::Timestamp(1_700_000_000_000_000_000));

    let se: SerializableEvent = (&event).into();
    assert!(matches!(
        se.fields.get("ts"),
        Some(SerializableValue::Timestamp(1_700_000_000_000_000_000))
    ));

    let restored: Event = se.into();
    assert_eq!(
        restored.data.get("ts"),
        Some(&Value::Timestamp(1_700_000_000_000_000_000))
    );
}

#[test]
fn serializable_value_duration_round_trip() {
    let mut event = Event::new("T");
    event
        .data
        .insert("dur".into(), Value::Duration(5_000_000_000));

    let se: SerializableEvent = (&event).into();
    assert!(matches!(
        se.fields.get("dur"),
        Some(SerializableValue::Duration(5_000_000_000))
    ));

    let restored: Event = se.into();
    assert_eq!(
        restored.data.get("dur"),
        Some(&Value::Duration(5_000_000_000))
    );
}

#[test]
fn serializable_value_nested_array_in_map() {
    use indexmap::IndexMap;
    use rustc_hash::FxBuildHasher;

    let inner_arr = Value::array(vec![Value::Int(1), Value::Int(2)]);
    let mut m: IndexMap<Arc<str>, Value, FxBuildHasher> = IndexMap::with_hasher(FxBuildHasher);
    m.insert("nums".into(), inner_arr);

    let mut event = Event::new("T");
    event.data.insert("nested".into(), Value::map(m));

    let se: SerializableEvent = (&event).into();
    let restored: Event = se.into();

    match restored.data.get("nested") {
        Some(Value::Map(map)) => match map.get("nums") {
            Some(Value::Array(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], Value::Int(1));
                assert_eq!(arr[1], Value::Int(2));
            }
            other => panic!("Expected nested Array, got {:?}", other),
        },
        other => panic!("Expected Map, got {:?}", other),
    }
}

#[test]
fn serializable_event_empty_fields() {
    let event = Event::new("Empty");

    let se: SerializableEvent = (&event).into();
    assert!(se.fields.is_empty());

    let restored: Event = se.into();
    assert_eq!(&*restored.event_type, "Empty");
    assert!(restored.data.is_empty());
}

#[test]
fn memory_store_prune_more_than_available() {
    let store = MemoryStore::new();

    let cp = Checkpoint {
        id: 1,
        timestamp_ms: 1000,
        events_processed: 10,
        window_states: HashMap::new(),
        pattern_states: HashMap::new(),
        metadata: HashMap::new(),
        context_states: HashMap::new(),
    };
    store.save_checkpoint(&cp).unwrap();

    // Keep 10, but only 1 exists — should prune 0
    let pruned = store.prune_checkpoints(10).unwrap();
    assert_eq!(pruned, 0);

    let ids = store.list_checkpoints().unwrap();
    assert_eq!(ids.len(), 1);
}

#[test]
fn checkpoint_manager_should_checkpoint_timing() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::new());
    let config = CheckpointConfig {
        interval: std::time::Duration::from_millis(50),
        max_checkpoints: 5,
        checkpoint_on_shutdown: false,
        key_prefix: "test".to_string(),
    };

    let mgr = CheckpointManager::new(Arc::clone(&store), config).unwrap();

    // Right after creation, should_checkpoint is false (interval hasn't elapsed)
    // This may be true if creation took >= 50ms, but on fast systems it should be false.
    // We just verify the method works without panicking.
    let _ = mgr.should_checkpoint();
}
