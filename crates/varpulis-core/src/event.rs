//! Runtime event types
//!
//! This module defines the core `Event` type used throughout the Varpulis
//! streaming analytics engine. Events are the fundamental unit of data
//! processed by streams, pattern matchers, and connectors.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};

pub use crate::value::FxIndexMap;
use crate::Value;

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
    pub const fn with_timestamp(mut self, ts: DateTime<Utc>) -> Self {
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
        let mut map = ser
            .serialize_map(Some(2 + self.data.len()))
            .expect("serialize_map to Vec<u8> should not fail");
        map.serialize_entry("event_type", self.event_type.as_ref())
            .expect("serializing event_type entry should not fail");
        map.serialize_entry("timestamp", &self.timestamp)
            .expect("serializing timestamp entry should not fail");
        for (k, v) in &self.data {
            if k.as_ref() != "timestamp" {
                map.serialize_entry(k.as_ref(), v)
                    .expect("serializing data field entry should not fail");
            }
        }
        map.end()
            .expect("finalizing JSON map serialization should not fail");
        buf
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use chrono::TimeZone;

    use super::*;

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

    #[test]
    fn test_event_new_at_avoids_now() {
        let ts = Utc.with_ymd_and_hms(2020, 6, 15, 12, 0, 0).unwrap();
        let event = Event::new_at("Sensor", ts);
        assert_eq!(&*event.event_type, "Sensor");
        assert_eq!(event.timestamp, ts);
        assert!(event.data.is_empty());
    }

    #[test]
    fn test_event_with_capacity_preallocates() {
        let event = Event::with_capacity("BigEvent", 10)
            .with_field("a", 1i64)
            .with_field("b", 2i64);
        assert_eq!(event.data.len(), 2);
        // Capacity should be at least 10 (may be rounded up by allocator)
        assert!(event.data.capacity() >= 10);
    }

    #[test]
    fn test_event_with_capacity_at() {
        let ts = Utc.with_ymd_and_hms(2025, 3, 1, 0, 0, 0).unwrap();
        let event = Event::with_capacity_at("Batch", 5, ts);
        assert_eq!(&*event.event_type, "Batch");
        assert_eq!(event.timestamp, ts);
        assert!(event.data.capacity() >= 5);
    }

    #[test]
    fn test_event_from_fields() {
        let mut data: FxIndexMap<Arc<str>, Value> = IndexMap::with_hasher(FxBuildHasher);
        data.insert(Arc::from("x"), Value::Float(1.5));
        data.insert(Arc::from("y"), Value::Float(2.5));
        let event = Event::from_fields("Point", data);
        assert_eq!(&*event.event_type, "Point");
        assert_eq!(event.get_float("x"), Some(1.5));
        assert_eq!(event.get_float("y"), Some(2.5));
    }

    #[test]
    fn test_event_from_string_fields_converts_keys() {
        let mut data: FxIndexMap<String, Value> = IndexMap::with_hasher(FxBuildHasher);
        data.insert("name".to_string(), Value::str("Alice"));
        let event = Event::from_string_fields("User", data);
        assert_eq!(event.get_str("name"), Some("Alice"));
    }

    #[test]
    fn test_event_to_sink_payload_json() {
        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let event = Event::new("Order")
            .with_timestamp(ts)
            .with_field("amount", 99i64);
        let payload = event.to_sink_payload();
        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json["event_type"], "Order");
        assert_eq!(json["amount"], 99);
        // timestamp should be present in the output
        assert!(json.get("timestamp").is_some());
    }
}
