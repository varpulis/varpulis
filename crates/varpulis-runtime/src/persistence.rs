//! State Persistence for Varpulis Engine
//!
//! Provides persistent storage for engine state including:
//! - Window contents (events in active windows)
//! - Aggregation state
//! - Pattern matcher state
//! - Checkpointing and recovery
//!
//! # Example
//! ```ignore
//! use varpulis_runtime::persistence::{StateStore, RocksDbStore, CheckpointConfig};
//!
//! let store = RocksDbStore::open("/tmp/varpulis-state")?;
//! let config = CheckpointConfig::default();
//! engine.enable_persistence(store, config);
//! ```

use crate::event::Event;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

#[cfg(feature = "persistence")]
use std::path::Path;
#[cfg(feature = "persistence")]
use tracing::debug;

/// Configuration for checkpointing
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Interval between checkpoints
    pub interval: Duration,
    /// Maximum number of checkpoints to retain
    pub max_checkpoints: usize,
    /// Whether to checkpoint on shutdown
    pub checkpoint_on_shutdown: bool,
    /// Prefix for checkpoint keys
    pub key_prefix: String,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            max_checkpoints: 3,
            checkpoint_on_shutdown: true,
            key_prefix: "varpulis".to_string(),
        }
    }
}

/// Serializable representation of an event for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEvent {
    pub event_type: String,
    pub timestamp_ms: i64,
    pub fields: HashMap<String, SerializableValue>,
}

/// Serializable value type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Null,
}

impl From<&Event> for SerializableEvent {
    fn from(event: &Event) -> Self {
        let mut fields = HashMap::new();
        for (k, v) in &event.data {
            let sv = match v {
                varpulis_core::Value::Int(i) => SerializableValue::Int(*i),
                varpulis_core::Value::Float(f) => SerializableValue::Float(*f),
                varpulis_core::Value::Bool(b) => SerializableValue::Bool(*b),
                varpulis_core::Value::Str(s) => SerializableValue::String(s.clone()),
                varpulis_core::Value::Null => SerializableValue::Null,
                _ => SerializableValue::Null, // Arrays, maps, timestamps simplified to null for now
            };
            fields.insert(k.clone(), sv);
        }
        Self {
            event_type: event.event_type.clone(),
            timestamp_ms: event.timestamp.timestamp_millis(),
            fields,
        }
    }
}

impl From<SerializableEvent> for Event {
    fn from(se: SerializableEvent) -> Self {
        let mut event = Event::new(&se.event_type);
        event.timestamp = chrono::DateTime::from_timestamp_millis(se.timestamp_ms)
            .unwrap_or_else(chrono::Utc::now);
        for (k, v) in se.fields {
            let value = match v {
                SerializableValue::Int(i) => varpulis_core::Value::Int(i),
                SerializableValue::Float(f) => varpulis_core::Value::Float(f),
                SerializableValue::Bool(b) => varpulis_core::Value::Bool(b),
                SerializableValue::String(s) => varpulis_core::Value::Str(s),
                SerializableValue::Null => varpulis_core::Value::Null,
            };
            event.data.insert(k, value);
        }
        event
    }
}

/// Checkpoint containing all engine state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Checkpoint ID (monotonically increasing)
    pub id: u64,
    /// Timestamp when checkpoint was created
    pub timestamp_ms: i64,
    /// Number of events processed at checkpoint time
    pub events_processed: u64,
    /// Window states by stream name
    pub window_states: HashMap<String, WindowCheckpoint>,
    /// Pattern matcher states
    pub pattern_states: HashMap<String, PatternCheckpoint>,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

/// Checkpoint for window state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowCheckpoint {
    /// Events currently in the window
    pub events: Vec<SerializableEvent>,
    /// Window start timestamp (if applicable)
    pub window_start_ms: Option<i64>,
    /// Last emit timestamp (for sliding windows)
    pub last_emit_ms: Option<i64>,
    /// Partitioned window states
    pub partitions: HashMap<String, PartitionedWindowCheckpoint>,
}

/// Checkpoint for partitioned window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionedWindowCheckpoint {
    pub events: Vec<SerializableEvent>,
    pub window_start_ms: Option<i64>,
}

/// Checkpoint for pattern matcher state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternCheckpoint {
    /// Active partial matches
    pub partial_matches: Vec<PartialMatchCheckpoint>,
}

/// A partial match in progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialMatchCheckpoint {
    /// Current state in the pattern automaton
    pub state: String,
    /// Events matched so far
    pub matched_events: Vec<SerializableEvent>,
    /// Start timestamp
    pub start_ms: i64,
}

/// Error type for state store operations
#[derive(Debug)]
pub enum StoreError {
    /// I/O or storage error
    IoError(String),
    /// Serialization error
    SerializationError(String),
    /// Key not found
    NotFound(String),
    /// Store not initialized
    NotInitialized,
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::IoError(s) => write!(f, "I/O error: {}", s),
            StoreError::SerializationError(s) => write!(f, "Serialization error: {}", s),
            StoreError::NotFound(s) => write!(f, "Key not found: {}", s),
            StoreError::NotInitialized => write!(f, "Store not initialized"),
        }
    }
}

impl std::error::Error for StoreError {}

/// Trait for state storage backends
pub trait StateStore: Send + Sync {
    /// Store a checkpoint
    fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), StoreError>;

    /// Load the latest checkpoint
    fn load_latest_checkpoint(&self) -> Result<Option<Checkpoint>, StoreError>;

    /// Load a specific checkpoint by ID
    fn load_checkpoint(&self, id: u64) -> Result<Option<Checkpoint>, StoreError>;

    /// List all checkpoint IDs
    fn list_checkpoints(&self) -> Result<Vec<u64>, StoreError>;

    /// Delete old checkpoints, keeping only the most recent N
    fn prune_checkpoints(&self, keep: usize) -> Result<usize, StoreError>;

    /// Store arbitrary key-value data
    fn put(&self, key: &str, value: &[u8]) -> Result<(), StoreError>;

    /// Retrieve arbitrary key-value data
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete a key
    fn delete(&self, key: &str) -> Result<(), StoreError>;

    /// Flush all pending writes to disk
    fn flush(&self) -> Result<(), StoreError>;
}

/// In-memory state store for testing
#[derive(Default)]
pub struct MemoryStore {
    data: std::sync::RwLock<HashMap<String, Vec<u8>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StateStore for MemoryStore {
    fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), StoreError> {
        let key = format!("checkpoint:{}", checkpoint.id);
        let data = bincode::serialize(checkpoint)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        self.put(&key, &data)
    }

    fn load_latest_checkpoint(&self) -> Result<Option<Checkpoint>, StoreError> {
        let checkpoints = self.list_checkpoints()?;
        if let Some(id) = checkpoints.last() {
            self.load_checkpoint(*id)
        } else {
            Ok(None)
        }
    }

    fn load_checkpoint(&self, id: u64) -> Result<Option<Checkpoint>, StoreError> {
        let key = format!("checkpoint:{}", id);
        if let Some(data) = self.get(&key)? {
            let checkpoint: Checkpoint = bincode::deserialize(&data)
                .map_err(|e| StoreError::SerializationError(e.to_string()))?;
            Ok(Some(checkpoint))
        } else {
            Ok(None)
        }
    }

    fn list_checkpoints(&self) -> Result<Vec<u64>, StoreError> {
        let data = self
            .data
            .read()
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        let mut ids: Vec<u64> = data
            .keys()
            .filter_map(|k| k.strip_prefix("checkpoint:").and_then(|s| s.parse().ok()))
            .collect();
        ids.sort();
        Ok(ids)
    }

    fn prune_checkpoints(&self, keep: usize) -> Result<usize, StoreError> {
        let checkpoints = self.list_checkpoints()?;
        let to_delete = checkpoints.len().saturating_sub(keep);
        for id in checkpoints.iter().take(to_delete) {
            let key = format!("checkpoint:{}", id);
            self.delete(&key)?;
        }
        Ok(to_delete)
    }

    fn put(&self, key: &str, value: &[u8]) -> Result<(), StoreError> {
        let mut data = self
            .data
            .write()
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        data.insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let data = self
            .data
            .read()
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        Ok(data.get(key).cloned())
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        let mut data = self
            .data
            .write()
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        data.remove(key);
        Ok(())
    }

    fn flush(&self) -> Result<(), StoreError> {
        Ok(()) // No-op for memory store
    }
}

/// RocksDB-based state store
#[cfg(feature = "persistence")]
pub struct RocksDbStore {
    db: rocksdb::DB,
    prefix: String,
}

#[cfg(feature = "persistence")]
impl RocksDbStore {
    /// Open or create a RocksDB store at the given path
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        Self::open_with_prefix(path, "varpulis")
    }

    /// Open with a custom key prefix
    pub fn open_with_prefix<P: AsRef<Path>>(path: P, prefix: &str) -> Result<Self, StoreError> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        // Optimize for write-heavy workload
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024);

        let db = rocksdb::DB::open(&opts, path).map_err(|e| StoreError::IoError(e.to_string()))?;

        info!("Opened RocksDB state store");
        Ok(Self {
            db,
            prefix: prefix.to_string(),
        })
    }

    fn prefixed_key(&self, key: &str) -> String {
        format!("{}:{}", self.prefix, key)
    }
}

#[cfg(feature = "persistence")]
impl StateStore for RocksDbStore {
    fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), StoreError> {
        let key = self.prefixed_key(&format!("checkpoint:{}", checkpoint.id));
        let data = bincode::serialize(checkpoint)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        self.db
            .put(key.as_bytes(), &data)
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        // Also update the "latest" pointer
        let latest_key = self.prefixed_key("checkpoint:latest");
        self.db
            .put(latest_key.as_bytes(), checkpoint.id.to_le_bytes())
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        debug!("Saved checkpoint {} ({} bytes)", checkpoint.id, data.len());
        Ok(())
    }

    fn load_latest_checkpoint(&self) -> Result<Option<Checkpoint>, StoreError> {
        let latest_key = self.prefixed_key("checkpoint:latest");
        if let Some(id_bytes) = self
            .db
            .get(latest_key.as_bytes())
            .map_err(|e| StoreError::IoError(e.to_string()))?
        {
            if id_bytes.len() == 8 {
                let id = u64::from_le_bytes(id_bytes.try_into().unwrap());
                return self.load_checkpoint(id);
            }
        }
        Ok(None)
    }

    fn load_checkpoint(&self, id: u64) -> Result<Option<Checkpoint>, StoreError> {
        let key = self.prefixed_key(&format!("checkpoint:{}", id));
        if let Some(data) = self
            .db
            .get(key.as_bytes())
            .map_err(|e| StoreError::IoError(e.to_string()))?
        {
            let checkpoint: Checkpoint = bincode::deserialize(&data)
                .map_err(|e| StoreError::SerializationError(e.to_string()))?;
            debug!("Loaded checkpoint {}", id);
            Ok(Some(checkpoint))
        } else {
            Ok(None)
        }
    }

    fn list_checkpoints(&self) -> Result<Vec<u64>, StoreError> {
        let prefix = self.prefixed_key("checkpoint:");
        let mut ids = Vec::new();

        let iter = self.db.prefix_iterator(prefix.as_bytes());
        for item in iter {
            let (key, _) = item.map_err(|e| StoreError::IoError(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);
            if let Some(suffix) = key_str.strip_prefix(&prefix) {
                if suffix != "latest" {
                    if let Ok(id) = suffix.parse::<u64>() {
                        ids.push(id);
                    }
                }
            }
        }

        ids.sort();
        Ok(ids)
    }

    fn prune_checkpoints(&self, keep: usize) -> Result<usize, StoreError> {
        let checkpoints = self.list_checkpoints()?;
        let to_delete = checkpoints.len().saturating_sub(keep);

        for id in checkpoints.iter().take(to_delete) {
            let key = self.prefixed_key(&format!("checkpoint:{}", id));
            self.db
                .delete(key.as_bytes())
                .map_err(|e| StoreError::IoError(e.to_string()))?;
        }

        if to_delete > 0 {
            info!("Pruned {} old checkpoints", to_delete);
        }
        Ok(to_delete)
    }

    fn put(&self, key: &str, value: &[u8]) -> Result<(), StoreError> {
        let full_key = self.prefixed_key(key);
        self.db
            .put(full_key.as_bytes(), value)
            .map_err(|e| StoreError::IoError(e.to_string()))
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let full_key = self.prefixed_key(key);
        self.db
            .get(full_key.as_bytes())
            .map_err(|e| StoreError::IoError(e.to_string()))
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        let full_key = self.prefixed_key(key);
        self.db
            .delete(full_key.as_bytes())
            .map_err(|e| StoreError::IoError(e.to_string()))
    }

    fn flush(&self) -> Result<(), StoreError> {
        self.db
            .flush()
            .map_err(|e| StoreError::IoError(e.to_string()))
    }
}

/// Checkpoint manager that handles periodic checkpointing
pub struct CheckpointManager {
    store: Arc<dyn StateStore>,
    config: CheckpointConfig,
    last_checkpoint: Instant,
    next_checkpoint_id: u64,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub fn new(store: Arc<dyn StateStore>, config: CheckpointConfig) -> Result<Self, StoreError> {
        // Load the latest checkpoint ID
        let next_id = store
            .load_latest_checkpoint()?
            .map(|c| c.id + 1)
            .unwrap_or(1);

        Ok(Self {
            store,
            config,
            last_checkpoint: Instant::now(),
            next_checkpoint_id: next_id,
        })
    }

    /// Check if it's time to create a checkpoint
    pub fn should_checkpoint(&self) -> bool {
        self.last_checkpoint.elapsed() >= self.config.interval
    }

    /// Create a checkpoint with the given state
    pub fn checkpoint(&mut self, checkpoint: Checkpoint) -> Result<(), StoreError> {
        let mut checkpoint = checkpoint;
        checkpoint.id = self.next_checkpoint_id;
        checkpoint.timestamp_ms = chrono::Utc::now().timestamp_millis();

        self.store.save_checkpoint(&checkpoint)?;
        self.store.prune_checkpoints(self.config.max_checkpoints)?;
        self.store.flush()?;

        self.last_checkpoint = Instant::now();
        self.next_checkpoint_id += 1;

        info!(
            "Created checkpoint {} ({} events processed)",
            checkpoint.id, checkpoint.events_processed
        );
        Ok(())
    }

    /// Load the latest checkpoint for recovery
    pub fn recover(&self) -> Result<Option<Checkpoint>, StoreError> {
        self.store.load_latest_checkpoint()
    }

    /// Get the underlying store
    pub fn store(&self) -> &Arc<dyn StateStore> {
        &self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_store_checkpoint() {
        let store = MemoryStore::new();

        let checkpoint = Checkpoint {
            id: 1,
            timestamp_ms: 1000,
            events_processed: 100,
            window_states: HashMap::new(),
            pattern_states: HashMap::new(),
            metadata: HashMap::new(),
        };

        store.save_checkpoint(&checkpoint).unwrap();

        let loaded = store.load_checkpoint(1).unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, 1);
        assert_eq!(loaded.events_processed, 100);
    }

    #[test]
    fn test_memory_store_prune() {
        let store = MemoryStore::new();

        for i in 1..=5 {
            let checkpoint = Checkpoint {
                id: i,
                timestamp_ms: i as i64 * 1000,
                events_processed: i * 100,
                window_states: HashMap::new(),
                pattern_states: HashMap::new(),
                metadata: HashMap::new(),
            };
            store.save_checkpoint(&checkpoint).unwrap();
        }

        let checkpoints = store.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 5);

        let pruned = store.prune_checkpoints(2).unwrap();
        assert_eq!(pruned, 3);

        let checkpoints = store.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 2);
        assert_eq!(checkpoints, vec![4, 5]);
    }

    #[test]
    fn test_serializable_event() {
        let mut event = Event::new("TestEvent");
        event
            .data
            .insert("count".to_string(), varpulis_core::Value::Int(42));
        event
            .data
            .insert("value".to_string(), varpulis_core::Value::Float(1.5));
        event.data.insert(
            "name".to_string(),
            varpulis_core::Value::Str("test".to_string()),
        );

        let serializable: SerializableEvent = (&event).into();
        let restored: Event = serializable.into();

        assert_eq!(restored.event_type, "TestEvent");
        assert_eq!(restored.get_int("count"), Some(42));
        assert_eq!(restored.get_float("value"), Some(1.5));
        assert_eq!(restored.get_str("name"), Some("test"));
    }
}
