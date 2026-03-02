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
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // =========================================================================
    // GeneratorConfig
    // =========================================================================

    #[test]
    fn default_config_has_expected_values() {
        let config = GeneratorConfig::default();
        assert_eq!(config.schema, SchemaType::Fraud);
        assert_eq!(config.rate, 1000);
        assert_eq!(config.duration_secs, 60);
        assert!((config.anomaly_rate - 0.05).abs() < f64::EPSILON);
        assert!(config.seed.is_none());
    }

    // =========================================================================
    // SchemaType
    // =========================================================================

    #[test]
    fn schema_type_from_str_valid() {
        assert_eq!("fraud".parse::<SchemaType>().unwrap(), SchemaType::Fraud);
        assert_eq!("iot".parse::<SchemaType>().unwrap(), SchemaType::Iot);
        assert_eq!(
            "trading".parse::<SchemaType>().unwrap(),
            SchemaType::Trading
        );
    }

    #[test]
    fn schema_type_from_str_case_insensitive() {
        assert_eq!("FRAUD".parse::<SchemaType>().unwrap(), SchemaType::Fraud);
        assert_eq!("IoT".parse::<SchemaType>().unwrap(), SchemaType::Iot);
        assert_eq!(
            "Trading".parse::<SchemaType>().unwrap(),
            SchemaType::Trading
        );
    }

    #[test]
    fn schema_type_from_str_invalid() {
        let err = "unknown".parse::<SchemaType>().unwrap_err();
        assert!(err.contains("Unknown schema"));
        assert!(err.contains("fraud"));
    }

    #[test]
    fn schema_type_display() {
        assert_eq!(SchemaType::Fraud.to_string(), "fraud");
        assert_eq!(SchemaType::Iot.to_string(), "iot");
        assert_eq!(SchemaType::Trading.to_string(), "trading");
    }

    #[test]
    fn schema_type_display_roundtrips() {
        for st in [SchemaType::Fraud, SchemaType::Iot, SchemaType::Trading] {
            let s = st.to_string();
            let parsed: SchemaType = s.parse().unwrap();
            assert_eq!(parsed, st);
        }
    }

    // =========================================================================
    // create_schema factory
    // =========================================================================

    #[test]
    fn create_schema_returns_correct_type() {
        let fraud = create_schema(SchemaType::Fraud, Some(42));
        assert!(fraud.description().contains("fraud") || fraud.description().contains("Banking"));

        let iot = create_schema(SchemaType::Iot, Some(42));
        assert!(iot.description().contains("IoT") || iot.description().contains("sensor"));

        let trading = create_schema(SchemaType::Trading, Some(42));
        assert!(trading.description().contains("Market") || trading.description().contains("trad"));
    }

    // =========================================================================
    // generate_batch
    // =========================================================================

    #[test]
    fn generate_batch_returns_correct_count() {
        let config = GeneratorConfig {
            seed: Some(42),
            ..Default::default()
        };
        assert_eq!(generate_batch(&config, 0).len(), 0);
        assert_eq!(generate_batch(&config, 1).len(), 1);
        assert_eq!(generate_batch(&config, 100).len(), 100);
    }

    #[test]
    fn generate_batch_deterministic_with_seed() {
        let config = GeneratorConfig {
            seed: Some(123),
            ..Default::default()
        };
        let batch1 = generate_batch(&config, 50);
        let batch2 = generate_batch(&config, 50);

        for (a, b) in batch1.iter().zip(batch2.iter()) {
            assert_eq!(a.event_type, b.event_type);
            assert_eq!(a.is_anomaly, b.is_anomaly);
            assert_eq!(a.fields.len(), b.fields.len());
        }
    }

    #[test]
    fn generate_batch_events_have_nonempty_type() {
        let config = GeneratorConfig {
            seed: Some(42),
            ..Default::default()
        };
        for event in generate_batch(&config, 200) {
            assert!(!event.event_type.is_empty());
        }
    }

    // =========================================================================
    // GeneratedEvent serialization
    // =========================================================================

    #[test]
    fn generated_event_json_roundtrip() {
        let config = GeneratorConfig {
            seed: Some(42),
            ..Default::default()
        };
        let events = generate_batch(&config, 10);
        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            let restored: GeneratedEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(restored.event_type, event.event_type);
            assert_eq!(restored.is_anomaly, event.is_anomaly);
        }
    }

    #[test]
    fn is_anomaly_skipped_when_false() {
        let event = GeneratedEvent {
            event_type: "test".into(),
            timestamp: Utc::now(),
            fields: HashMap::new(),
            is_anomaly: false,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(!json.contains("is_anomaly"));
    }

    #[test]
    fn is_anomaly_present_when_true() {
        let event = GeneratedEvent {
            event_type: "test".into(),
            timestamp: Utc::now(),
            fields: HashMap::new(),
            is_anomaly: true,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("is_anomaly"));
    }

    // =========================================================================
    // Fraud schema
    // =========================================================================

    #[test]
    fn fraud_event_types() {
        let schema = schemas::fraud::FraudSchema::new(Some(42));
        let types = schema.event_types();
        assert!(types.contains(&"login".to_string()));
        assert!(types.contains(&"transaction".to_string()));
        assert!(types.contains(&"transfer".to_string()));
        assert!(types.contains(&"card_payment".to_string()));
    }

    #[test]
    fn fraud_generates_valid_events() {
        let mut schema = schemas::fraud::FraudSchema::new(Some(42));
        let allowed: HashSet<&str> = ["login", "transaction", "transfer", "card_payment"]
            .iter()
            .copied()
            .collect();

        for _ in 0..500 {
            let event = schema.next_event();
            assert!(
                allowed.contains(event.event_type.as_str()),
                "Unexpected event type: {}",
                event.event_type
            );
            assert!(event.fields.contains_key("user_id"));
        }
    }

    #[test]
    fn fraud_anomalies_are_injected() {
        let mut schema = schemas::fraud::FraudSchema::new(Some(42));
        let mut anomaly_count = 0;
        let n = 2000;
        for _ in 0..n {
            if schema.next_event().is_anomaly {
                anomaly_count += 1;
            }
        }
        // Anomalies should exist (5% base rate + multi-event sequences)
        assert!(anomaly_count > 0, "No anomalies in {n} events");
        // But not all events should be anomalies
        assert!(anomaly_count < n, "All events were anomalies");
    }

    #[test]
    fn fraud_anomaly_sequence_has_login_then_transfers() {
        let mut schema = schemas::fraud::FraudSchema::new(Some(42));
        let events: Vec<_> = (0..5000).map(|_| schema.next_event()).collect();

        // Find anomaly sequences: login (with new_location) followed by high-value transfers
        let mut found_login = false;
        let mut found_transfer = false;
        for event in &events {
            if event.is_anomaly && event.event_type == "login" {
                found_login = true;
                assert!(event.fields.contains_key("new_location"));
            }
            if event.is_anomaly && event.event_type == "transfer" {
                found_transfer = true;
                let amount = event.fields["amount"].as_f64().unwrap();
                assert!(
                    (5000.0..=50000.0).contains(&amount),
                    "Anomaly transfer amount {amount} out of range"
                );
            }
        }
        assert!(found_login, "No anomaly login found in 5000 events");
        assert!(found_transfer, "No anomaly transfer found in 5000 events");
    }

    #[test]
    fn fraud_login_events_have_required_fields() {
        let mut schema = schemas::fraud::FraudSchema::new(Some(42));
        for _ in 0..500 {
            let event = schema.next_event();
            if event.event_type == "login" && !event.is_anomaly {
                assert!(event.fields.contains_key("city"));
                assert!(event.fields.contains_key("success"));
                assert!(event.fields.contains_key("device"));
            }
        }
    }

    #[test]
    fn fraud_transaction_events_have_amount() {
        let mut schema = schemas::fraud::FraudSchema::new(Some(42));
        for _ in 0..500 {
            let event = schema.next_event();
            if event.event_type == "transaction" || event.event_type == "card_payment" {
                assert!(event.fields.contains_key("amount"));
                assert!(event.fields.contains_key("merchant"));
                let amount = event.fields["amount"].as_f64().unwrap();
                assert!((5.0..=500.0).contains(&amount));
            }
        }
    }

    // =========================================================================
    // IoT schema
    // =========================================================================

    #[test]
    fn iot_event_types() {
        let schema = schemas::iot::IotSchema::new(Some(42));
        let types = schema.event_types();
        assert!(types.contains(&"sensor_reading".to_string()));
        assert!(types.contains(&"sensor_alert".to_string()));
        assert!(types.contains(&"sensor_heartbeat".to_string()));
    }

    #[test]
    fn iot_generates_valid_events() {
        let mut schema = schemas::iot::IotSchema::new(Some(42));
        let allowed: HashSet<&str> = ["sensor_reading", "sensor_alert", "sensor_heartbeat"]
            .iter()
            .copied()
            .collect();

        for _ in 0..500 {
            let event = schema.next_event();
            assert!(
                allowed.contains(event.event_type.as_str()),
                "Unexpected event type: {}",
                event.event_type
            );
            assert!(event.fields.contains_key("sensor_id"));
            assert!(event.fields.contains_key("zone"));
            assert!(event.fields.contains_key("temperature"));
            assert!(event.fields.contains_key("humidity"));
            assert!(event.fields.contains_key("pressure"));
        }
    }

    #[test]
    fn iot_sensors_use_correct_zones() {
        let mut schema = schemas::iot::IotSchema::new(Some(42));
        let valid_zones: HashSet<&str> = ["zone_a", "zone_b", "zone_c", "zone_d"]
            .iter()
            .copied()
            .collect();

        for _ in 0..200 {
            let event = schema.next_event();
            let zone = event.fields["zone"].as_str().unwrap();
            assert!(valid_zones.contains(zone), "Unexpected zone: {zone}");
        }
    }

    #[test]
    fn iot_anomalies_cause_temperature_spike() {
        let mut schema = schemas::iot::IotSchema::new(Some(42));
        let mut found_anomaly = false;
        for _ in 0..2000 {
            let event = schema.next_event();
            if event.is_anomaly {
                found_anomaly = true;
            }
        }
        assert!(found_anomaly, "No anomalies in 2000 IoT events");
    }

    #[test]
    fn iot_alert_events_have_alert_type() {
        let mut schema = schemas::iot::IotSchema::new(Some(42));
        for _ in 0..500 {
            let event = schema.next_event();
            if event.event_type == "sensor_alert" {
                assert!(event.fields.contains_key("alert_type"));
            }
        }
    }

    // =========================================================================
    // Trading schema
    // =========================================================================

    #[test]
    fn trading_event_types() {
        let schema = schemas::trading::TradingSchema::new(Some(42));
        let types = schema.event_types();
        assert!(types.contains(&"trade".to_string()));
        assert!(types.contains(&"quote".to_string()));
        assert!(types.contains(&"order_new".to_string()));
        assert!(types.contains(&"order_cancel".to_string()));
    }

    #[test]
    fn trading_generates_valid_events() {
        let mut schema = schemas::trading::TradingSchema::new(Some(42));
        let allowed: HashSet<&str> = ["trade", "quote", "order_new", "order_cancel"]
            .iter()
            .copied()
            .collect();

        for _ in 0..500 {
            let event = schema.next_event();
            assert!(
                allowed.contains(event.event_type.as_str()),
                "Unexpected event type: {}",
                event.event_type
            );
            assert!(event.fields.contains_key("symbol"));
            assert!(event.fields.contains_key("exchange"));
            assert!(event.fields.contains_key("price"));
        }
    }

    #[test]
    fn trading_prices_are_positive() {
        let mut schema = schemas::trading::TradingSchema::new(Some(42));
        for _ in 0..1000 {
            let event = schema.next_event();
            let price = event.fields["price"].as_f64().unwrap();
            assert!(price > 0.0, "Price must be positive, got {price}");
        }
    }

    #[test]
    fn trading_trade_events_have_volume_and_side() {
        let mut schema = schemas::trading::TradingSchema::new(Some(42));
        for _ in 0..500 {
            let event = schema.next_event();
            if event.event_type == "trade" {
                assert!(event.fields.contains_key("volume"));
                assert!(event.fields.contains_key("side"));
                assert!(event.fields.contains_key("trade_id"));
                let side = event.fields["side"].as_str().unwrap();
                assert!(side == "buy" || side == "sell");
            }
        }
    }

    #[test]
    fn trading_quote_events_have_bid_ask() {
        let mut schema = schemas::trading::TradingSchema::new(Some(42));
        for _ in 0..500 {
            let event = schema.next_event();
            if event.event_type == "quote" {
                assert!(event.fields.contains_key("bid"));
                assert!(event.fields.contains_key("ask"));
                let bid = event.fields["bid"].as_f64().unwrap();
                let ask = event.fields["ask"].as_f64().unwrap();
                assert!(bid < ask, "Bid {bid} should be less than ask {ask}");
            }
        }
    }

    #[test]
    fn trading_anomalies_have_large_volume() {
        let mut schema = schemas::trading::TradingSchema::new(Some(42));
        let mut found_anomaly_trade = false;
        for _ in 0..5000 {
            let event = schema.next_event();
            if event.is_anomaly && event.event_type == "trade" {
                found_anomaly_trade = true;
                let volume = event.fields["volume"].as_u64().unwrap();
                assert!(
                    volume >= 10000,
                    "Anomaly trade volume should be >= 10000, got {volume}"
                );
            }
        }
        assert!(found_anomaly_trade, "No anomaly trades in 5000 events");
    }

    #[test]
    fn trading_uses_known_symbols() {
        let mut schema = schemas::trading::TradingSchema::new(Some(42));
        let valid: HashSet<&str> = [
            "AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "META", "NVDA", "JPM",
        ]
        .iter()
        .copied()
        .collect();

        for _ in 0..200 {
            let event = schema.next_event();
            let sym = event.fields["symbol"].as_str().unwrap();
            assert!(valid.contains(sym), "Unknown symbol: {sym}");
        }
    }

    // =========================================================================
    // Cross-schema determinism
    // =========================================================================

    #[test]
    fn all_schemas_deterministic_with_same_seed() {
        for schema_type in [SchemaType::Fraud, SchemaType::Iot, SchemaType::Trading] {
            let mut s1 = create_schema(schema_type, Some(99));
            let mut s2 = create_schema(schema_type, Some(99));

            for i in 0..100 {
                let e1 = s1.next_event();
                let e2 = s2.next_event();
                assert_eq!(
                    e1.event_type, e2.event_type,
                    "{schema_type} event {i}: types differ"
                );
                assert_eq!(
                    e1.is_anomaly, e2.is_anomaly,
                    "{schema_type} event {i}: anomaly flag differs"
                );
            }
        }
    }

    #[test]
    fn different_seeds_produce_different_sequences() {
        let mut s1 = create_schema(SchemaType::Fraud, Some(1));
        let mut s2 = create_schema(SchemaType::Fraud, Some(2));

        let events1: Vec<_> = (0..50).map(|_| s1.next_event().event_type).collect();
        let events2: Vec<_> = (0..50).map(|_| s2.next_event().event_type).collect();

        // Very unlikely (but not impossible) for 50 events to be identical with different seeds
        assert_ne!(events1, events2);
    }
}
