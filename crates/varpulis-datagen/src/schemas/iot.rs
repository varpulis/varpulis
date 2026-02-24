//! IoT sensor event schema.
//!
//! Generates sensor readings: temperature, humidity, pressure, vibration.
//! Anomaly patterns: temperature spikes, sensor drift, rapid oscillation.

use crate::{EventSchema, GeneratedEvent};
use chrono::Utc;
use rand::prelude::*;
use serde_json::json;
use std::collections::HashMap;

pub struct IotSchema {
    rng: StdRng,
    sensors: Vec<SensorState>,
    event_count: u64,
}

struct SensorState {
    id: String,
    zone: String,
    temperature: f64,
    humidity: f64,
    pressure: f64,
    drift: f64,
}

const ZONES: &[&str] = &["zone_a", "zone_b", "zone_c", "zone_d"];

impl IotSchema {
    pub fn new(seed: Option<u64>) -> Self {
        let mut rng = seed.map_or_else(StdRng::from_entropy, StdRng::seed_from_u64);
        let sensors: Vec<SensorState> = (0..8)
            .map(|i| SensorState {
                id: format!("sensor_{:03}", i + 1),
                zone: ZONES[i % ZONES.len()].into(),
                temperature: 20.0 + rng.gen_range(-5.0..5.0),
                humidity: 50.0 + rng.gen_range(-10.0..10.0),
                pressure: 1013.0 + rng.gen_range(-5.0..5.0),
                drift: 0.0,
            })
            .collect();
        Self {
            rng,
            sensors,
            event_count: 0,
        }
    }
}

impl EventSchema for IotSchema {
    fn next_event(&mut self) -> GeneratedEvent {
        self.event_count += 1;
        let idx = self.rng.gen_range(0..self.sensors.len());
        let sensor = &mut self.sensors[idx];
        let is_anomaly;

        // ~3% chance of anomaly
        if self.rng.gen_bool(0.03) {
            is_anomaly = true;
            // Anomaly: temperature spike
            sensor.temperature += self.rng.gen_range(15.0..40.0);
            sensor.drift += 0.5;
        } else {
            is_anomaly = false;
            // Normal: small random walk
            sensor.temperature += self.rng.gen_range(-0.5..0.5) + sensor.drift * 0.1;
            sensor.humidity += self.rng.gen_range(-1.0..1.0);
            sensor.pressure += self.rng.gen_range(-0.3..0.3);
            // Decay drift back to normal
            sensor.drift *= 0.95;
            // Mean-revert temperature slowly
            sensor.temperature += (22.0 - sensor.temperature) * 0.01;
        }

        let event_type = match self.rng.gen_range(0..10) {
            0..=6 => "sensor_reading",
            7..=8 => "sensor_alert",
            _ => "sensor_heartbeat",
        };

        let mut fields = HashMap::new();
        fields.insert("sensor_id".into(), json!(sensor.id.clone()));
        fields.insert("zone".into(), json!(sensor.zone.clone()));
        fields.insert(
            "temperature".into(),
            json!((sensor.temperature * 10.0).round() / 10.0),
        );
        fields.insert(
            "humidity".into(),
            json!((sensor.humidity * 10.0).round() / 10.0),
        );
        fields.insert(
            "pressure".into(),
            json!((sensor.pressure * 10.0).round() / 10.0),
        );

        if event_type == "sensor_alert" {
            fields.insert(
                "alert_type".into(),
                json!(if sensor.temperature > 40.0 {
                    "high_temperature"
                } else if sensor.humidity > 80.0 {
                    "high_humidity"
                } else {
                    "routine_check"
                }),
            );
        }

        GeneratedEvent {
            event_type: event_type.into(),
            timestamp: Utc::now(),
            fields,
            is_anomaly,
        }
    }

    fn event_types(&self) -> Vec<String> {
        vec![
            "sensor_reading".into(),
            "sensor_alert".into(),
            "sensor_heartbeat".into(),
        ]
    }

    fn description(&self) -> &str {
        "IoT sensor readings: temperature, humidity, pressure with anomaly spike injection"
    }
}
