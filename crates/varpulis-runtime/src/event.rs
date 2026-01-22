//! Event types for the runtime

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use varpulis_core::Value;

/// A runtime event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Event type name
    pub event_type: String,
    /// Timestamp of the event
    pub timestamp: DateTime<Utc>,
    /// Event payload
    pub data: IndexMap<String, Value>,
}

impl Event {
    pub fn new(event_type: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            timestamp: Utc::now(),
            data: IndexMap::new(),
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
