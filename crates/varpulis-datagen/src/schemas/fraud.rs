//! Fraud detection event schema.
//!
//! Generates banking/payment events: logins, transfers, card transactions.
//! Anomaly patterns: rapid transfers after login from new location, high-value
//! transactions, account enumeration.

use std::collections::HashMap;

use chrono::Utc;
use rand::prelude::*;
use serde_json::json;

use crate::{EventSchema, GeneratedEvent};

const CITIES: &[&str] = &[
    "New York",
    "London",
    "Tokyo",
    "São Paulo",
    "Mumbai",
    "Lagos",
    "Shanghai",
    "Moscow",
    "Sydney",
    "Berlin",
];

const MERCHANTS: &[&str] = &[
    "Amazon",
    "Walmart",
    "Target",
    "BestBuy",
    "Costco",
    "eBay",
    "Shopify Store",
    "Gas Station",
    "Restaurant",
    "ATM",
];

#[derive(Debug)]
pub struct FraudSchema {
    rng: StdRng,
    event_count: u64,
    anomaly_sequence: Option<AnomalyState>,
}

#[derive(Debug)]
enum AnomalyState {
    LoginFromNewCity {
        user_id: String,
        city: String,
        remaining: u8,
    },
}

impl FraudSchema {
    pub fn new(seed: Option<u64>) -> Self {
        Self {
            rng: seed.map_or_else(StdRng::from_entropy, StdRng::seed_from_u64),
            event_count: 0,
            anomaly_sequence: None,
        }
    }

    fn gen_user_id(&mut self) -> String {
        format!("user_{:04}", self.rng.gen_range(1..=500))
    }

    fn gen_account_id(&mut self) -> String {
        format!("acct_{:06}", self.rng.gen_range(100000..=999999))
    }

    fn normal_event(&mut self) -> GeneratedEvent {
        let event_type = match self.rng.gen_range(0..10) {
            0..=3 => "login",
            4..=6 => "transaction",
            7..=8 => "transfer",
            _ => "card_payment",
        };

        let user_id = self.gen_user_id();
        let mut fields = HashMap::new();
        fields.insert("user_id".into(), json!(user_id));

        match event_type {
            "login" => {
                fields.insert(
                    "city".into(),
                    json!(CITIES[self.rng.gen_range(0..CITIES.len())]),
                );
                fields.insert("success".into(), json!(self.rng.gen_bool(0.95)));
                fields.insert(
                    "device".into(),
                    json!(if self.rng.gen_bool(0.7) {
                        "mobile"
                    } else {
                        "desktop"
                    }),
                );
            }
            "transaction" | "card_payment" => {
                fields.insert(
                    "amount".into(),
                    json!(self.rng.gen_range(5.0..500.0_f64).round()),
                );
                fields.insert(
                    "merchant".into(),
                    json!(MERCHANTS[self.rng.gen_range(0..MERCHANTS.len())]),
                );
                fields.insert("account_id".into(), json!(self.gen_account_id()));
                fields.insert("currency".into(), json!("USD"));
            }
            "transfer" => {
                fields.insert(
                    "amount".into(),
                    json!(self.rng.gen_range(10.0..2000.0_f64).round()),
                );
                fields.insert("from_account".into(), json!(self.gen_account_id()));
                fields.insert("to_account".into(), json!(self.gen_account_id()));
                fields.insert("currency".into(), json!("USD"));
            }
            _ => {}
        }

        GeneratedEvent {
            event_type: event_type.into(),
            timestamp: Utc::now(),
            fields,
            is_anomaly: false,
        }
    }

    fn anomaly_event(&mut self) -> GeneratedEvent {
        // Continue existing anomaly sequence or start new one
        if let Some(state) = self.anomaly_sequence.take() {
            match state {
                AnomalyState::LoginFromNewCity {
                    user_id,
                    city,
                    remaining,
                } => {
                    let mut fields = HashMap::new();
                    fields.insert("user_id".into(), json!(user_id));
                    fields.insert(
                        "amount".into(),
                        json!(self.rng.gen_range(5000.0..50000.0_f64).round()),
                    );
                    fields.insert("from_account".into(), json!(self.gen_account_id()));
                    fields.insert("to_account".into(), json!(self.gen_account_id()));
                    fields.insert("currency".into(), json!("USD"));
                    fields.insert("city".into(), json!(city));

                    if remaining > 1 {
                        self.anomaly_sequence = Some(AnomalyState::LoginFromNewCity {
                            user_id,
                            city,
                            remaining: remaining - 1,
                        });
                    }

                    GeneratedEvent {
                        event_type: "transfer".into(),
                        timestamp: Utc::now(),
                        fields,
                        is_anomaly: true,
                    }
                }
            }
        } else {
            // Start new anomaly: login from unusual city, followed by rapid high-value transfers
            let user_id = self.gen_user_id();
            let city = CITIES[self.rng.gen_range(0..CITIES.len())].to_string();

            self.anomaly_sequence = Some(AnomalyState::LoginFromNewCity {
                user_id: user_id.clone(),
                city: city.clone(),
                remaining: self.rng.gen_range(2..=4),
            });

            let mut fields = HashMap::new();
            fields.insert("user_id".into(), json!(user_id));
            fields.insert("city".into(), json!(city));
            fields.insert("success".into(), json!(true));
            fields.insert("device".into(), json!("desktop"));
            fields.insert("new_location".into(), json!(true));

            GeneratedEvent {
                event_type: "login".into(),
                timestamp: Utc::now(),
                fields,
                is_anomaly: true,
            }
        }
    }
}

impl EventSchema for FraudSchema {
    fn next_event(&mut self) -> GeneratedEvent {
        self.event_count += 1;

        // If in the middle of an anomaly sequence, continue it
        if self.anomaly_sequence.is_some() {
            return self.anomaly_event();
        }

        // ~5% chance to start a new anomaly
        if self.rng.gen_bool(0.05) {
            self.anomaly_event()
        } else {
            self.normal_event()
        }
    }

    fn event_types(&self) -> Vec<String> {
        vec![
            "login".into(),
            "transaction".into(),
            "transfer".into(),
            "card_payment".into(),
        ]
    }

    fn description(&self) -> &'static str {
        "Banking fraud detection events: logins, transfers, card payments with anomaly injection"
    }
}
