//! HVAC Building Simulator for demo purposes

use crate::event::{Event, HVACStatus, HumidityReading, TemperatureReading};
use chrono::Utc;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
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
            if self.tick_count % 3 == 0 {
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
        if self.tick_count % 5 == 0 {
            for hvac in &self.config.hvac_units {
                let power = hvac.base_power * self.degradation_factor
                    + rng.gen_range(-0.5..0.5);
                let pressure = hvac.base_pressure / self.degradation_factor
                    + rng.gen_range(-0.1..0.1);

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
    let mut config = SimulatorConfig::default();
    config.anomaly_probability = 0.1; // 10% anomaly rate
    let simulator = Simulator::new(config, tx);
    (simulator, rx)
}

/// Create a simulator with degradation
pub fn create_degradation_simulator() -> (Simulator, mpsc::Receiver<Event>) {
    let (tx, rx) = mpsc::channel(1000);
    let mut config = SimulatorConfig::default();
    config.degradation_enabled = true;
    let simulator = Simulator::new(config, tx);
    (simulator, rx)
}
