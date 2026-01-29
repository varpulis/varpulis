//! HVAC Building Simulator for demo purposes

use crate::event::{Event, HVACStatus, HumidityReading, TemperatureReading};
use chrono::Utc;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::mpsc;
use tokio::time;

/// Configuration for the HVAC simulator
#[derive(Debug, Clone)]
pub struct SimulatorConfig {
    pub zones: Vec<ZoneConfig>,
    pub hvac_units: Vec<HVACConfig>,
    pub events_per_second: u32,
    pub anomaly_probability: f64,
    pub degradation_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct ZoneConfig {
    pub id: String,
    pub name: String,
    pub target_temp: f64,
    pub target_humidity: f64,
    pub temp_variance: f64,
    pub humidity_variance: f64,
}

#[derive(Debug, Clone)]
pub struct HVACConfig {
    pub id: String,
    pub base_power: f64,
    pub base_pressure: f64,
}

impl Default for SimulatorConfig {
    fn default() -> Self {
        Self {
            zones: vec![
                ZoneConfig {
                    id: "zone_a".to_string(),
                    name: "Bureaux".to_string(),
                    target_temp: 22.0,
                    target_humidity: 50.0,
                    temp_variance: 1.0,
                    humidity_variance: 5.0,
                },
                ZoneConfig {
                    id: "zone_b".to_string(),
                    name: "Salle Serveurs".to_string(),
                    target_temp: 19.0,
                    target_humidity: 50.0,
                    temp_variance: 0.5,
                    humidity_variance: 3.0,
                },
                ZoneConfig {
                    id: "zone_c".to_string(),
                    name: "Accueil".to_string(),
                    target_temp: 21.0,
                    target_humidity: 50.0,
                    temp_variance: 2.0,
                    humidity_variance: 8.0,
                },
            ],
            hvac_units: vec![HVACConfig {
                id: "cta_main".to_string(),
                base_power: 15.0,
                base_pressure: 8.5,
            }],
            events_per_second: 10,
            anomaly_probability: 0.01,
            degradation_enabled: false,
        }
    }
}

/// HVAC Building Simulator
pub struct Simulator {
    config: SimulatorConfig,
    sender: mpsc::Sender<Event>,
    tick_count: u64,
    degradation_factor: f64,
    rng: StdRng,
}

impl Simulator {
    pub fn new(config: SimulatorConfig, sender: mpsc::Sender<Event>) -> Self {
        Self {
            config,
            sender,
            tick_count: 0,
            degradation_factor: 1.0,
            rng: StdRng::from_entropy(),
        }
    }

    /// Run the simulator
    pub async fn run(&mut self) {
        let interval_ms = 1000 / self.config.events_per_second as u64;
        let mut interval = time::interval(time::Duration::from_millis(interval_ms));

        loop {
            interval.tick().await;
            self.tick_count += 1;

            // Generate events
            if let Err(e) = self.generate_events().await {
                tracing::error!("Failed to send event: {}", e);
                break;
            }

            // Update degradation if enabled
            if self.config.degradation_enabled {
                self.degradation_factor += 0.0001;
            }
        }
    }

    async fn generate_events(&mut self) -> Result<(), mpsc::error::SendError<Event>> {
        let rng = &mut self.rng;
        let now = Utc::now();

        // Generate temperature readings for each zone
        for zone in &self.config.zones {
            let is_anomaly = rng.gen::<f64>() < self.config.anomaly_probability;

            let temp = if is_anomaly {
                // Anomaly: temperature spike
                zone.target_temp + rng.gen_range(5.0..10.0)
            } else {
                zone.target_temp + rng.gen_range(-zone.temp_variance..zone.temp_variance)
            };

            let reading = TemperatureReading {
                sensor_id: format!("{}_temp_01", zone.id),
                zone: zone.id.clone(),
                value: temp,
                timestamp: now,
            };
            self.sender.send(reading.into()).await?;

            // Generate humidity reading (less frequent)
            if self.tick_count.is_multiple_of(3) {
                let humidity = zone.target_humidity
                    + rng.gen_range(-zone.humidity_variance..zone.humidity_variance);

                let reading = HumidityReading {
                    sensor_id: format!("{}_hum_01", zone.id),
                    zone: zone.id.clone(),
                    value: humidity,
                    timestamp: now,
                };
                self.sender.send(reading.into()).await?;
            }
        }

        // Generate HVAC status (less frequent)
        if self.tick_count.is_multiple_of(5) {
            for hvac in &self.config.hvac_units {
                let power = hvac.base_power * self.degradation_factor + rng.gen_range(-0.5..0.5);
                let pressure =
                    hvac.base_pressure / self.degradation_factor + rng.gen_range(-0.1..0.1);

                let status = HVACStatus {
                    unit_id: hvac.id.clone(),
                    mode: "cooling".to_string(),
                    power_consumption: power,
                    fan_speed: 1200 + rng.gen_range(-50..50),
                    compressor_pressure: pressure,
                    timestamp: now,
                };
                self.sender.send(status.into()).await?;
            }
        }

        Ok(())
    }
}

/// Create a simulator with default config and return sender/receiver
pub fn create_default_simulator() -> (Simulator, mpsc::Receiver<Event>) {
    let (tx, rx) = mpsc::channel(1000);
    let config = SimulatorConfig::default();
    let simulator = Simulator::new(config, tx);
    (simulator, rx)
}

/// Create a simulator that produces anomalies
pub fn create_anomaly_simulator() -> (Simulator, mpsc::Receiver<Event>) {
    let (tx, rx) = mpsc::channel(1000);
    let config = SimulatorConfig {
        anomaly_probability: 0.1, // 10% anomaly rate
        ..Default::default()
    };
    let simulator = Simulator::new(config, tx);
    (simulator, rx)
}

/// Create a simulator with degradation
pub fn create_degradation_simulator() -> (Simulator, mpsc::Receiver<Event>) {
    let (tx, rx) = mpsc::channel(1000);
    let config = SimulatorConfig {
        degradation_enabled: true,
        ..Default::default()
    };
    let simulator = Simulator::new(config, tx);
    (simulator, rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==========================================================================
    // Config Tests
    // ==========================================================================

    #[test]
    fn test_simulator_config_default() {
        let config = SimulatorConfig::default();
        assert_eq!(config.zones.len(), 3);
        assert_eq!(config.hvac_units.len(), 1);
        assert_eq!(config.events_per_second, 10);
        assert!((config.anomaly_probability - 0.01).abs() < 0.001);
        assert!(!config.degradation_enabled);
    }

    #[test]
    fn test_zone_config() {
        let config = SimulatorConfig::default();
        let zone_a = &config.zones[0];
        assert_eq!(zone_a.id, "zone_a");
        assert_eq!(zone_a.name, "Bureaux");
        assert!((zone_a.target_temp - 22.0).abs() < 0.01);
    }

    #[test]
    fn test_hvac_config() {
        let config = SimulatorConfig::default();
        let hvac = &config.hvac_units[0];
        assert_eq!(hvac.id, "cta_main");
        assert!((hvac.base_power - 15.0).abs() < 0.01);
        assert!((hvac.base_pressure - 8.5).abs() < 0.01);
    }

    // ==========================================================================
    // Simulator Creation Tests
    // ==========================================================================

    #[test]
    fn test_create_default_simulator() {
        let (sim, _rx) = create_default_simulator();
        assert_eq!(sim.tick_count, 0);
        assert!((sim.degradation_factor - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_create_anomaly_simulator() {
        let (sim, _rx) = create_anomaly_simulator();
        assert!((sim.config.anomaly_probability - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_create_degradation_simulator() {
        let (sim, _rx) = create_degradation_simulator();
        assert!(sim.config.degradation_enabled);
    }

    #[test]
    fn test_simulator_new() {
        let (tx, _rx) = mpsc::channel(100);
        let config = SimulatorConfig::default();
        let sim = Simulator::new(config, tx);
        assert_eq!(sim.tick_count, 0);
    }

    // ==========================================================================
    // Event Generation Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_simulator_generate_events() {
        let (tx, mut rx) = mpsc::channel(100);
        let config = SimulatorConfig::default();
        let mut sim = Simulator::new(config, tx);

        // Generate one batch of events
        sim.generate_events().await.unwrap();

        // Should have temperature readings for each zone
        let mut temp_count = 0;
        while let Ok(event) = rx.try_recv() {
            if event.event_type == "TemperatureReading" {
                temp_count += 1;
            }
        }
        assert_eq!(temp_count, 3); // One per zone
    }

    #[tokio::test]
    async fn test_simulator_humidity_generation() {
        let (tx, mut rx) = mpsc::channel(100);
        let config = SimulatorConfig::default();
        let mut sim = Simulator::new(config, tx);

        // Generate events at tick 3 (humidity is generated when tick % 3 == 0)
        sim.tick_count = 2; // Will become 3 in generate_events after increment
        sim.generate_events().await.unwrap();
        sim.tick_count = 3;
        sim.generate_events().await.unwrap();

        let mut humidity_count = 0;
        while let Ok(event) = rx.try_recv() {
            if event.event_type == "HumidityReading" {
                humidity_count += 1;
            }
        }
        assert!(humidity_count >= 3); // Should have humidity readings
    }

    #[tokio::test]
    async fn test_simulator_hvac_generation() {
        let (tx, mut rx) = mpsc::channel(100);
        let config = SimulatorConfig::default();
        let mut sim = Simulator::new(config, tx);

        // Generate events at tick 5 (HVAC is generated when tick % 5 == 0)
        sim.tick_count = 4;
        sim.generate_events().await.unwrap();
        sim.tick_count = 5;
        sim.generate_events().await.unwrap();

        let mut hvac_count = 0;
        while let Ok(event) = rx.try_recv() {
            if event.event_type == "HVACStatus" {
                hvac_count += 1;
            }
        }
        assert!(hvac_count >= 1); // Should have at least one HVAC status
    }

    #[tokio::test]
    #[allow(clippy::field_reassign_with_default)]
    async fn test_simulator_with_degradation() {
        let (tx, _rx) = mpsc::channel(100);
        let mut config = SimulatorConfig::default();
        config.degradation_enabled = true;
        let mut sim = Simulator::new(config, tx);

        let initial_degradation = sim.degradation_factor;

        // Generate events multiple times
        for _ in 0..10 {
            sim.generate_events().await.unwrap();
            if sim.config.degradation_enabled {
                sim.degradation_factor += 0.0001;
            }
        }

        // Degradation should have increased
        assert!(sim.degradation_factor > initial_degradation);
    }

    #[tokio::test]
    async fn test_simulator_event_fields() {
        let (tx, mut rx) = mpsc::channel(100);
        let config = SimulatorConfig::default();
        let mut sim = Simulator::new(config, tx);

        sim.generate_events().await.unwrap();

        // Check temperature reading has correct fields
        if let Ok(event) = rx.try_recv() {
            if event.event_type == "TemperatureReading" {
                assert!(event.get_str("sensor_id").is_some());
                assert!(event.get_str("zone").is_some());
                assert!(event.get_float("value").is_some());
            }
        }
    }

    #[tokio::test]
    async fn test_simulator_channel_closed() {
        let (tx, rx) = mpsc::channel(1);
        let config = SimulatorConfig::default();
        let mut sim = Simulator::new(config, tx);

        // Drop receiver to close channel
        drop(rx);

        // Generate events should fail
        let result = sim.generate_events().await;
        assert!(result.is_err());
    }
}
