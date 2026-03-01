//! Event types for the runtime
//!
//! The core `Event` type is defined in `varpulis-core` and re-exported here
//! for backwards compatibility. Domain-specific event types (demo/HVAC) are
//! defined locally.

// Re-export everything from varpulis-core's event module
pub use varpulis_core::event::*;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
}
