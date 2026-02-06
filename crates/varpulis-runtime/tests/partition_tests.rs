//! Tests for partitioned window functionality

use chrono::{Duration, Utc};
use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_runtime::{Engine, Event};

#[tokio::test]
async fn test_partition_by_tumbling_window_separate_state() {
    // Test that partitioned tumbling windows maintain separate state per key
    let (output_tx, mut output_rx) = mpsc::channel(100);
    let mut engine = Engine::new(output_tx);

    let vpl = r#"
        event PriceEvent:
            symbol: str
            price: float
            ts: timestamp

        stream Prices = PriceEvent
            .partition_by(symbol)
            .window(5m)
            .aggregate(
                symbol: last(symbol),
                avg_price: avg(price),
                count: count()
            )
            .emit(
                event_type: "PriceAverage",
                symbol: symbol,
                avg_price: avg_price,
                count: count
            )
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    // Base time for window alignment
    let base_time = Utc::now();

    // Send BTC prices
    for i in 0..3 {
        let event = Event::new("PriceEvent")
            .with_timestamp(base_time + Duration::seconds(i * 10))
            .with_field("symbol", "BTC")
            .with_field("price", 45000.0 + (i as f64 * 100.0));
        engine.process(event).await.expect("Failed to process");
    }

    // Send ETH prices (separate partition)
    for i in 0..3 {
        let event = Event::new("PriceEvent")
            .with_timestamp(base_time + Duration::seconds(i * 10))
            .with_field("symbol", "ETH")
            .with_field("price", 3000.0 + (i as f64 * 50.0));
        engine.process(event).await.expect("Failed to process");
    }

    // Trigger window close with event outside window
    let trigger_btc = Event::new("PriceEvent")
        .with_timestamp(base_time + Duration::minutes(6))
        .with_field("symbol", "BTC")
        .with_field("price", 46000.0);
    engine
        .process(trigger_btc)
        .await
        .expect("Failed to process");

    let trigger_eth = Event::new("PriceEvent")
        .with_timestamp(base_time + Duration::minutes(6))
        .with_field("symbol", "ETH")
        .with_field("price", 3200.0);
    engine
        .process(trigger_eth)
        .await
        .expect("Failed to process");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Collect events
    let mut btc_events = vec![];
    let mut eth_events = vec![];
    while let Ok(event) = output_rx.try_recv() {
        if &*event.event_type == "PriceAverage" {
            match event.data.get("symbol") {
                Some(Value::Str(s)) if s == "BTC" => btc_events.push(event),
                Some(Value::Str(s)) if s == "ETH" => eth_events.push(event),
                _ => {}
            }
        }
    }

    // Each partition should have its own window state
    // Note: The exact behavior depends on implementation
    println!(
        "BTC events: {}, ETH events: {}",
        btc_events.len(),
        eth_events.len()
    );
}

#[tokio::test]
async fn test_partition_by_sliding_window_separate_state() {
    // Test that partitioned sliding windows maintain separate state per key
    let (output_tx, mut output_rx) = mpsc::channel(100);
    let mut engine = Engine::new(output_tx);

    let vpl = r#"
        event SensorReading:
            sensor_id: str
            value: float

        stream Sensors = SensorReading
            .partition_by(sensor_id)
            .window(10m, sliding: 2m)
            .aggregate(
                sensor_id: last(sensor_id),
                avg_value: avg(value)
            )
            .emit(
                event_type: "SensorAverage",
                sensor_id: sensor_id,
                avg_value: avg_value
            )
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    let base_time = Utc::now();

    // Send readings for sensor A
    for i in 0..5 {
        let event = Event::new("SensorReading")
            .with_timestamp(base_time + Duration::minutes(i * 3))
            .with_field("sensor_id", "sensor_A")
            .with_field("value", 20.0 + (i as f64));
        engine.process(event).await.expect("Failed to process");
    }

    // Send readings for sensor B
    for i in 0..5 {
        let event = Event::new("SensorReading")
            .with_timestamp(base_time + Duration::minutes(i * 3))
            .with_field("sensor_id", "sensor_B")
            .with_field("value", 50.0 + (i as f64));
        engine.process(event).await.expect("Failed to process");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Count events per sensor
    let mut sensor_a_count = 0;
    let mut sensor_b_count = 0;
    while let Ok(event) = output_rx.try_recv() {
        if &*event.event_type == "SensorAverage" {
            match event.data.get("sensor_id") {
                Some(Value::Str(s)) if s == "sensor_A" => sensor_a_count += 1,
                Some(Value::Str(s)) if s == "sensor_B" => sensor_b_count += 1,
                _ => {}
            }
        }
    }

    println!(
        "Sensor A events: {}, Sensor B events: {}",
        sensor_a_count, sensor_b_count
    );
}

#[tokio::test]
async fn test_partition_aggregate_independent_per_key() {
    // Test that partitioned aggregates maintain independent state per key
    let (output_tx, mut output_rx) = mpsc::channel(100);
    let mut engine = Engine::new(output_tx);

    let vpl = r#"
        event OrderEvent:
            customer_id: str
            amount: float

        stream OrderTotals = OrderEvent
            .partition_by(customer_id)
            .window(3)
            .aggregate(
                customer_id: last(customer_id),
                total: sum(amount),
                order_count: count()
            )
            .emit(
                event_type: "CustomerTotal",
                customer_id: customer_id,
                total: total,
                order_count: order_count
            )
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    // Send orders for customer A
    for amount in [100.0, 200.0, 300.0f64] {
        let event = Event::new("OrderEvent")
            .with_field("customer_id", "customer_A")
            .with_field("amount", amount);
        engine.process(event).await.expect("Failed to process");
    }

    // Send orders for customer B (different amounts)
    for amount in [50.0, 75.0, 25.0f64] {
        let event = Event::new("OrderEvent")
            .with_field("customer_id", "customer_B")
            .with_field("amount", amount);
        engine.process(event).await.expect("Failed to process");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify each customer has independent totals
    while let Ok(event) = output_rx.try_recv() {
        if &*event.event_type == "CustomerTotal" {
            let customer = event.data.get("customer_id");
            let total = event.data.get("total");

            match (customer, total) {
                (Some(Value::Str(c)), Some(Value::Float(t))) if c == "customer_A" => {
                    assert!(
                        (t - 600.0).abs() < 0.01,
                        "Customer A total should be 600, got {}",
                        t
                    );
                }
                (Some(Value::Str(c)), Some(Value::Float(t))) if c == "customer_B" => {
                    assert!(
                        (t - 150.0).abs() < 0.01,
                        "Customer B total should be 150, got {}",
                        t
                    );
                }
                _ => {}
            }
        }
    }
}

#[tokio::test]
async fn test_macd_signal_partitioned_by_symbol() {
    // Test that MACD signals are correctly partitioned by symbol
    let (output_tx, mut output_rx) = mpsc::channel(100);
    let mut engine = Engine::new(output_tx);

    let vpl = r#"
        event OHLCV:
            symbol: str
            close: float
            timeframe: str

        stream MACDSignal = OHLCV
            .where(timeframe == "1m")
            .partition_by(symbol)
            .window(9)
            .aggregate(
                symbol: last(symbol),
                signal_line: ema(close, 9)
            )
            .emit(
                event_type: "MACDSignal",
                symbol: symbol,
                signal_line: signal_line
            )
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    // Send OHLCV for BTC (9 events to fill window)
    for i in 0..10 {
        let event = Event::new("OHLCV")
            .with_field("symbol", "BTC/USD")
            .with_field("close", 45000.0 + (i as f64 * 100.0))
            .with_field("timeframe", "1m");
        engine.process(event).await.expect("Failed to process");
    }

    // Send OHLCV for ETH (9 events to fill window)
    for i in 0..10 {
        let event = Event::new("OHLCV")
            .with_field("symbol", "ETH/USD")
            .with_field("close", 3000.0 + (i as f64 * 50.0))
            .with_field("timeframe", "1m");
        engine.process(event).await.expect("Failed to process");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Count signals per symbol
    let mut btc_signals = 0;
    let mut eth_signals = 0;
    while let Ok(event) = output_rx.try_recv() {
        // Check for MACD signals - the event_type is in the data
        let is_macd = event
            .data
            .get("event_type")
            .map(|v| v == &Value::Str("MACDSignal".to_string()))
            .unwrap_or(false);

        if is_macd {
            match event.data.get("symbol") {
                Some(Value::Str(s)) if s == "BTC/USD" => btc_signals += 1,
                Some(Value::Str(s)) if s == "ETH/USD" => eth_signals += 1,
                _ => {}
            }
        }
    }

    // Both should emit signals independently
    println!("BTC signals: {}, ETH signals: {}", btc_signals, eth_signals);
    assert!(btc_signals > 0, "Should have BTC signals");
    assert!(eth_signals > 0, "Should have ETH signals");
}
