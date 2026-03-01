//! Runtime event types
//!
//! This module defines the core `Event` type used throughout the Varpulis
//! streaming analytics engine. Events are the fundamental unit of data
//! processed by streams, pattern matchers, and connectors.

pub use crate::value::FxIndexMap;
use crate::Value;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Type alias for field name keys using `Arc<str>` for O(1) cloning.
pub type FieldKey = Arc<str>;

/// A shared reference to an Event for efficient passing through pipelines.
/// Using Arc avoids expensive deep clones when events are processed by
/// multiple streams, windows, or pattern matchers.
pub type SharedEvent = Arc<Event>;

/// A runtime event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Event type name (`Arc<str>` for O(1) clone instead of O(n) String clone).
    pub event_type: Arc<str>,
    /// Timestamp of the event (defaults to current server time if not provided).
    #[serde(default = "Utc::now")]
    pub timestamp: DateTime<Utc>,
    /// Event payload (uses `Arc<str>` keys for O(1) cloning, FxBuildHasher for faster access).
    pub data: FxIndexMap<Arc<str>, Value>,
}

impl Event {
    /// Creates a new event with the given type and current timestamp.
    pub fn new(event_type: impl Into<Arc<str>>) -> Self {
        Self {
            event_type: event_type.into(),
            timestamp: Utc::now(),
            data: IndexMap::with_hasher(FxBuildHasher),
        }
    }

    /// Creates a new event with a specific timestamp (avoids Utc::now() syscall).
    pub fn new_at(event_type: impl Into<Arc<str>>, timestamp: DateTime<Utc>) -> Self {
        Self {
            event_type: event_type.into(),
            timestamp,
            data: IndexMap::with_hasher(FxBuildHasher),
        }
    }

    /// Creates a new event with pre-allocated capacity for fields.
    /// Use this when you know the approximate number of fields in advance.
    pub fn with_capacity(event_type: impl Into<Arc<str>>, capacity: usize) -> Self {
        Self {
            event_type: event_type.into(),
            timestamp: Utc::now(),
            data: IndexMap::with_capacity_and_hasher(capacity, FxBuildHasher),
        }
    }

    /// Creates a new event with pre-allocated capacity and a specific timestamp.
    pub fn with_capacity_at(
        event_type: impl Into<Arc<str>>,
        capacity: usize,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            timestamp,
            data: IndexMap::with_capacity_and_hasher(capacity, FxBuildHasher),
        }
    }

    /// Creates a new event from pre-built fields map.
    /// Use this when you already have the fields constructed (e.g., from JSON parsing).
    pub fn from_fields(event_type: impl Into<Arc<str>>, data: FxIndexMap<Arc<str>, Value>) -> Self {
        Self {
            event_type: event_type.into(),
            timestamp: Utc::now(),
            data,
        }
    }

    /// Creates a new event from pre-built fields map with String keys (converts to `Arc<str>`).
    pub fn from_string_fields(
        event_type: impl Into<Arc<str>>,
        data: FxIndexMap<String, Value>,
    ) -> Self {
        let converted: FxIndexMap<Arc<str>, Value> =
            data.into_iter().map(|(k, v)| (Arc::from(k), v)).collect();
        Self {
            event_type: event_type.into(),
            timestamp: Utc::now(),
            data: converted,
        }
    }

    /// Creates a new event from pre-built fields map with a specific timestamp.
    pub fn from_fields_with_timestamp(
        event_type: impl Into<Arc<str>>,
        timestamp: DateTime<Utc>,
        data: FxIndexMap<Arc<str>, Value>,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            timestamp,
            data,
        }
    }

    /// Sets the event's timestamp (builder pattern).
    pub fn with_timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = ts;
        self
    }

    /// Adds a field to the event (builder pattern).
    pub fn with_field(mut self, key: impl Into<Arc<str>>, value: impl Into<Value>) -> Self {
        self.data.insert(key.into(), value.into());
        self
    }

    /// Looks up a field value by name.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }

    /// Looks up a field and extracts it as `f64`.
    pub fn get_float(&self, key: &str) -> Option<f64> {
        self.data.get(key).and_then(|v| v.as_float())
    }

    /// Looks up a field and extracts it as `i64`.
    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.data.get(key).and_then(|v| v.as_int())
    }

    /// Looks up a field and extracts it as a string slice.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.data.get(key).and_then(|v| v.as_str())
    }

    /// Serialize for sink output: event_type + timestamp + data fields.
    pub fn to_sink_payload(&self) -> Vec<u8> {
        use serde::ser::SerializeMap;
        use serde::Serializer;
        let mut buf = Vec::with_capacity(256);
        let mut ser = serde_json::Serializer::new(&mut buf);
        let mut map = ser.serialize_map(Some(2 + self.data.len())).unwrap();
        map.serialize_entry("event_type", self.event_type.as_ref())
            .unwrap();
        map.serialize_entry("timestamp", &self.timestamp).unwrap();
        for (k, v) in &self.data {
            if k.as_ref() != "timestamp" {
                map.serialize_entry(k.as_ref(), v).unwrap();
            }
        }
        map.end().unwrap();
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_event_new() {
        let event = Event::new("TestEvent");
        assert_eq!(&*event.event_type, "TestEvent");
        assert!(event.data.is_empty());
    }

    #[test]
    fn test_event_new_from_string() {
        let event = Event::new("TestEvent".to_string());
        assert_eq!(&*event.event_type, "TestEvent");
    }

    #[test]
    fn test_event_with_timestamp() {
        let ts = Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap();
        let event = Event::new("Test").with_timestamp(ts);
        assert_eq!(event.timestamp, ts);
    }

    #[test]
    fn test_event_with_field() {
        let event = Event::new("Test")
            .with_field("name", "value")
            .with_field("count", 42i64);

        assert_eq!(event.data.len(), 2);
        assert_eq!(event.get("name"), Some(&Value::Str("value".into())));
        assert_eq!(event.get("count"), Some(&Value::Int(42)));
    }

    #[test]
    fn test_event_get_float() {
        let event = Event::new("Test")
            .with_field("price", 19.99f64)
            .with_field("quantity", 5i64);

        assert_eq!(event.get_float("price"), Some(19.99));
        assert_eq!(event.get_float("quantity"), Some(5.0));
        assert_eq!(event.get_float("missing"), None);
    }

    #[test]
    fn test_event_get_int() {
        let event = Event::new("Test")
            .with_field("count", 42i64)
            .with_field("ratio", 3.7f64);

        assert_eq!(event.get_int("count"), Some(42));
        assert_eq!(event.get_int("ratio"), Some(3));
        assert_eq!(event.get_int("missing"), None);
    }

    #[test]
    fn test_event_get_str() {
        let event = Event::new("Test").with_field("name", "Alice");
        assert_eq!(event.get_str("name"), Some("Alice"));
        assert_eq!(event.get_str("missing"), None);
    }

    #[test]
    fn test_event_overwrite_field() {
        let event = Event::new("Test")
            .with_field("key", "first")
            .with_field("key", "second");

        assert_eq!(event.get_str("key"), Some("second"));
        assert_eq!(event.data.len(), 1);
    }
}
