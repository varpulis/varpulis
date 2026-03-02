//! Trading/market data event schema.
//!
//! Generates market events: trades, quotes, order book updates.
//! Anomaly patterns: flash crashes, spoofing (large orders + cancels), momentum ignition.

use std::collections::HashMap;

use chrono::Utc;
use rand::prelude::*;
use serde_json::json;

use crate::{EventSchema, GeneratedEvent};

const SYMBOLS: &[&str] = &[
    "AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "META", "NVDA", "JPM",
];
const EXCHANGES: &[&str] = &["NYSE", "NASDAQ", "BATS", "IEX"];

#[derive(Debug)]
pub struct TradingSchema {
    rng: StdRng,
    prices: HashMap<String, f64>,
    event_count: u64,
}

impl TradingSchema {
    pub fn new(seed: Option<u64>) -> Self {
        let mut rng = seed.map_or_else(StdRng::from_entropy, StdRng::seed_from_u64);
        let prices: HashMap<String, f64> = SYMBOLS
            .iter()
            .map(|s| (s.to_string(), 100.0 + rng.gen_range(0.0..400.0)))
            .collect();
        Self {
            rng,
            prices,
            event_count: 0,
        }
    }
}

impl EventSchema for TradingSchema {
    fn next_event(&mut self) -> GeneratedEvent {
        self.event_count += 1;
        let symbol = SYMBOLS[self.rng.gen_range(0..SYMBOLS.len())];
        let exchange = EXCHANGES[self.rng.gen_range(0..EXCHANGES.len())];
        let price = self.prices.get(symbol).copied().unwrap_or(150.0);

        // ~2% chance of anomaly (flash crash / spike)
        let (is_anomaly, new_price) = if self.rng.gen_bool(0.02) {
            let crash_pct = self.rng.gen_range(0.03..0.10);
            let p = if self.rng.gen_bool(0.5) {
                price * (1.0 - crash_pct)
            } else {
                price * (1.0 + crash_pct)
            };
            (true, p)
        } else {
            (false, price * (1.0 + self.rng.gen_range(-0.002..0.002)))
        };

        self.prices.insert(symbol.to_string(), new_price);
        let rounded_price = (new_price * 100.0).round() / 100.0;

        let event_type = match self.rng.gen_range(0..10) {
            0..=4 => "trade",
            5..=7 => "quote",
            8 => "order_new",
            _ => "order_cancel",
        };

        let mut fields = HashMap::new();
        fields.insert("symbol".into(), json!(symbol));
        fields.insert("exchange".into(), json!(exchange));
        fields.insert("price".into(), json!(rounded_price));

        match event_type {
            "trade" => {
                let volume = if is_anomaly {
                    self.rng.gen_range(10000..100000)
                } else {
                    self.rng.gen_range(100..5000)
                };
                fields.insert("volume".into(), json!(volume));
                fields.insert(
                    "side".into(),
                    json!(if self.rng.gen_bool(0.5) {
                        "buy"
                    } else {
                        "sell"
                    }),
                );
                fields.insert(
                    "trade_id".into(),
                    json!(format!("T{:08}", self.event_count)),
                );
            }
            "quote" => {
                let spread = self.rng.gen_range(0.01..0.10);
                fields.insert(
                    "bid".into(),
                    json!(((rounded_price - spread / 2.0) * 100.0).round() / 100.0),
                );
                fields.insert(
                    "ask".into(),
                    json!(((rounded_price + spread / 2.0) * 100.0).round() / 100.0),
                );
                fields.insert("bid_size".into(), json!(self.rng.gen_range(100..10000)));
                fields.insert("ask_size".into(), json!(self.rng.gen_range(100..10000)));
            }
            "order_new" | "order_cancel" => {
                fields.insert(
                    "order_id".into(),
                    json!(format!("O{:08}", self.rng.gen_range(1..1000000))),
                );
                fields.insert(
                    "side".into(),
                    json!(if self.rng.gen_bool(0.5) {
                        "buy"
                    } else {
                        "sell"
                    }),
                );
                fields.insert("quantity".into(), json!(self.rng.gen_range(100..50000)));
            }
            _ => {}
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
            "trade".into(),
            "quote".into(),
            "order_new".into(),
            "order_cancel".into(),
        ]
    }

    fn description(&self) -> &'static str {
        "Market data events: trades, quotes, orders with flash crash/spike injection"
    }
}
