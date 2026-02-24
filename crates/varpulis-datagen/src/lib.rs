//! Event generation library for Varpulis demos and testing.
//!
//! Provides pluggable event schemas (fraud, IoT, trading) with configurable
//! rates, pattern injection, and output formats.

pub mod schemas;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the event generator.
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Schema to use for event generation.
    pub schema: SchemaType,
    /// Events per second to generate.
    pub rate: u64,
    /// Total duration in seconds (0 = infinite).
    pub duration_secs: u64,
    /// Fraction of events that should contain injected anomaly patterns (0.0-1.0).
    pub anomaly_rate: f64,
    /// Random seed for reproducibility (None = random).
    pub seed: Option<u64>,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            schema: SchemaType::Fraud,
            rate: 1000,
            duration_secs: 60,
            anomaly_rate: 0.05,
            seed: None,
        }
    }
}

/// Available event generation schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    Fraud,
    Iot,
    Trading,
}

impl std::str::FromStr for SchemaType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fraud" => Ok(Self::Fraud),
            "iot" => Ok(Self::Iot),
            "trading" => Ok(Self::Trading),
            other => Err(format!(
                "Unknown schema: {other}. Available: fraud, iot, trading"
            )),
        }
    }
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fraud => write!(f, "fraud"),
            Self::Iot => write!(f, "iot"),
            Self::Trading => write!(f, "trading"),
        }
    }
}

/// A generated event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratedEvent {
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub fields: HashMap<String, serde_json::Value>,
    /// Whether this event is part of an injected anomaly pattern.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub is_anomaly: bool,
}

/// Trait for event generation schemas.
pub trait EventSchema: Send + Sync {
    /// Generate the next event.
    fn next_event(&mut self) -> GeneratedEvent;

    /// Get the list of event types this schema can produce.
    fn event_types(&self) -> Vec<String>;

    /// Get a description of the schema.
    fn description(&self) -> &str;
}

/// Create an event schema from a schema type.
pub fn create_schema(schema_type: SchemaType, seed: Option<u64>) -> Box<dyn EventSchema> {
    match schema_type {
        SchemaType::Fraud => Box::new(schemas::fraud::FraudSchema::new(seed)),
        SchemaType::Iot => Box::new(schemas::iot::IotSchema::new(seed)),
        SchemaType::Trading => Box::new(schemas::trading::TradingSchema::new(seed)),
    }
}

/// Generate a batch of events.
pub fn generate_batch(config: &GeneratorConfig, count: usize) -> Vec<GeneratedEvent> {
    let mut schema = create_schema(config.schema, config.seed);
    (0..count).map(|_| schema.next_event()).collect()
}
