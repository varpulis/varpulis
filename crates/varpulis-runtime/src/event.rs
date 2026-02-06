//! Event types for the runtime

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use varpulis_core::Value;

/// Type alias for IndexMap with FxBuildHasher for faster hashing of event fields.
pub type FxIndexMap<K, V> = IndexMap<K, V, FxBuildHasher>;

/// A shared reference to an Event for efficient passing through pipelines.
/// Using Arc avoids expensive deep clones when events are processed by
/// multiple streams, windows, or pattern matchers.
pub type SharedEvent = Arc<Event>;

/// A runtime event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Event type name (Arc<str> for O(1) clone instead of O(n) String clone)
    pub event_type: Arc<str>,
    /// Timestamp of the event (defaults to current server time if not provided)
    #[serde(default = "Utc::now")]
    pub timestamp: DateTime<Utc>,
    /// Event payload (uses FxBuildHasher for faster field access)
    pub data: FxIndexMap<String, Value>,
}

impl Event {
    pub fn new(event_type: impl Into<Arc<str>>) -> Self {
        Self {
            event_type: event_type.into(),
            timestamp: Utc::now(),
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

    /// Creates a new event from pre-built fields map.
    /// Use this when you already have the fields constructed (e.g., from JSON parsing).
    pub fn from_fields(event_type: impl Into<Arc<str>>, data: FxIndexMap<String, Value>) -> Self {
        Self {
            event_type: event_type.into(),
            timestamp: Utc::now(),
            data,
        }
    }

    /// Creates a new event from pre-built fields map with a specific timestamp.
    pub fn from_fields_with_timestamp(
        event_type: impl Into<Arc<str>>,
        timestamp: DateTime<Utc>,
        data: FxIndexMap<String, Value>,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            timestamp,
            data,
        }
    }

    pub fn with_timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = ts;
        self
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.data.insert(key.into(), value.into());
        self
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }

    pub fn get_float(&self, key: &str) -> Option<f64> {
        self.data.get(key).and_then(|v| v.as_float())
    }

    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.data.get(key).and_then(|v| v.as_int())
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.data.get(key).and_then(|v| v.as_str())
    }
}

/// Temperature reading event for HVAC demo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemperatureReading {
    pub sensor_id: String,
    pub zone: String,
    pub value: f64,
    #[serde(default = "Utc::now")]
    pub timestamp: DateTime<Utc>,
}

impl From<TemperatureReading> for Event {
    fn from(r: TemperatureReading) -> Self {
        Event::new("TemperatureReading")
            .with_timestamp(r.timestamp)
            .with_field("sensor_id", r.sensor_id)
            .with_field("zone", r.zone)
            .with_field("value", r.value)
    }
}

/// Humidity reading event for HVAC demo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HumidityReading {
    pub sensor_id: String,
    pub zone: String,
    pub value: f64,
    #[serde(default = "Utc::now")]
    pub timestamp: DateTime<Utc>,
}

impl From<HumidityReading> for Event {
    fn from(r: HumidityReading) -> Self {
        Event::new("HumidityReading")
            .with_timestamp(r.timestamp)
            .with_field("sensor_id", r.sensor_id)
            .with_field("zone", r.zone)
            .with_field("value", r.value)
    }
}

/// HVAC status event for demo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HVACStatus {
    pub unit_id: String,
    pub mode: String,
    pub power_consumption: f64,
    pub fan_speed: i64,
    pub compressor_pressure: f64,
    #[serde(default = "Utc::now")]
    pub timestamp: DateTime<Utc>,
}

impl From<HVACStatus> for Event {
    fn from(s: HVACStatus) -> Self {
        Event::new("HVACStatus")
            .with_timestamp(s.timestamp)
            .with_field("unit_id", s.unit_id)
            .with_field("mode", s.mode)
            .with_field("power_consumption", s.power_consumption)
            .with_field("fan_speed", s.fan_speed)
            .with_field("compressor_pressure", s.compressor_pressure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    // ==========================================================================
    // Event Construction Tests
    // ==========================================================================

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
    fn test_event_with_multiple_fields() {
        let event = Event::new("Order")
            .with_field("id", 123i64)
            .with_field("customer", "Alice")
            .with_field("total", 99.99f64)
            .with_field("premium", true);

        assert_eq!(event.data.len(), 4);
    }

    // ==========================================================================
    // Field Access Tests
    // ==========================================================================

    #[test]
    fn test_event_get() {
        let event = Event::new("Test").with_field("key", "value");
        assert_eq!(event.get("key"), Some(&Value::Str("value".into())));
        assert_eq!(event.get("missing"), None);
    }

    #[test]
    fn test_event_get_float() {
        let event = Event::new("Test")
            .with_field("price", 19.99f64)
            .with_field("quantity", 5i64);

        assert_eq!(event.get_float("price"), Some(19.99));
        assert_eq!(event.get_float("quantity"), Some(5.0)); // int converts to float
        assert_eq!(event.get_float("missing"), None);
    }

    #[test]
    fn test_event_get_int() {
        let event = Event::new("Test")
            .with_field("count", 42i64)
            .with_field("ratio", 3.7f64);

        assert_eq!(event.get_int("count"), Some(42));
        assert_eq!(event.get_int("ratio"), Some(3)); // float truncates to int
        assert_eq!(event.get_int("missing"), None);
    }

    #[test]
    fn test_event_get_str() {
        let event = Event::new("Test").with_field("name", "Alice");
        assert_eq!(event.get_str("name"), Some("Alice"));
        assert_eq!(event.get_str("missing"), None);
    }

    #[test]
    fn test_event_get_str_from_non_string() {
        let event = Event::new("Test").with_field("count", 42i64);
        assert_eq!(event.get_str("count"), None);
    }

    // ==========================================================================
    // From Trait Tests
    // ==========================================================================

    #[test]
    fn test_temperature_reading_to_event() {
        let ts = Utc::now();
        let reading = TemperatureReading {
            sensor_id: "sensor1".to_string(),
            zone: "zone_a".to_string(),
            value: 22.5,
            timestamp: ts,
        };

        let event: Event = reading.into();
        assert_eq!(&*event.event_type, "TemperatureReading");
        assert_eq!(event.get_str("sensor_id"), Some("sensor1"));
        assert_eq!(event.get_str("zone"), Some("zone_a"));
        assert_eq!(event.get_float("value"), Some(22.5));
        assert_eq!(event.timestamp, ts);
    }

    #[test]
    fn test_humidity_reading_to_event() {
        let ts = Utc::now();
        let reading = HumidityReading {
            sensor_id: "humid1".to_string(),
            zone: "zone_b".to_string(),
            value: 65.0,
            timestamp: ts,
        };

        let event: Event = reading.into();
        assert_eq!(&*event.event_type, "HumidityReading");
        assert_eq!(event.get_str("sensor_id"), Some("humid1"));
        assert_eq!(event.get_float("value"), Some(65.0));
    }

    #[test]
    fn test_hvac_status_to_event() {
        let ts = Utc::now();
        let status = HVACStatus {
            unit_id: "hvac1".to_string(),
            mode: "cooling".to_string(),
            power_consumption: 1500.0,
            fan_speed: 3,
            compressor_pressure: 2.5,
            timestamp: ts,
        };

        let event: Event = status.into();
        assert_eq!(&*event.event_type, "HVACStatus");
        assert_eq!(event.get_str("unit_id"), Some("hvac1"));
        assert_eq!(event.get_str("mode"), Some("cooling"));
        assert_eq!(event.get_float("power_consumption"), Some(1500.0));
        assert_eq!(event.get_int("fan_speed"), Some(3));
        assert_eq!(event.get_float("compressor_pressure"), Some(2.5));
    }

    // ==========================================================================
    // Edge Cases
    // ==========================================================================

    #[test]
    fn test_event_overwrite_field() {
        let event = Event::new("Test")
            .with_field("key", "first")
            .with_field("key", "second");

        assert_eq!(event.get_str("key"), Some("second"));
        assert_eq!(event.data.len(), 1);
    }

    #[test]
    fn test_event_empty_string_field() {
        let event = Event::new("Test").with_field("name", "");
        assert_eq!(event.get_str("name"), Some(""));
    }

    #[test]
    fn test_event_negative_values() {
        let event = Event::new("Test")
            .with_field("negative_int", -42i64)
            .with_field("negative_float", -2.5f64);

        assert_eq!(event.get_int("negative_int"), Some(-42));
        assert_eq!(event.get_float("negative_float"), Some(-2.5));
    }

    #[test]
    fn test_event_zero_values() {
        let event = Event::new("Test")
            .with_field("zero_int", 0i64)
            .with_field("zero_float", 0.0f64);

        assert_eq!(event.get_int("zero_int"), Some(0));
        assert_eq!(event.get_float("zero_float"), Some(0.0));
    }
}
