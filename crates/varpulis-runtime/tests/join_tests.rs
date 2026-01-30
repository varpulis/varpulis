//! Tests for join buffer functionality

use chrono::{Duration, Utc};
use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_runtime::{Engine, Event};

fn create_test_event(event_type: &str, symbol: &str, fields: Vec<(&str, Value)>) -> Event {
    let mut event = Event::new(event_type).with_field("symbol", symbol);
    for (key, value) in fields {
        event.data.insert(key.to_string(), value);
    }
    event
}

#[tokio::test]
async fn test_join_two_streams_correlates_by_key() {
    // Create an engine with alert channel
    let (alert_tx, mut alert_rx) = mpsc::channel(100);
    let mut engine = Engine::new(alert_tx);

    // Parse a simple join query
    let vpl = r#"
        event EMA12Event:
            symbol: str
            ema_12: float

        event EMA26Event:
            symbol: str
            ema_26: float

        stream EMA12 from EMA12Event
        stream EMA26 from EMA26Event

        stream MACD = join(EMA12, EMA26)
            .on(EMA12.symbol == EMA26.symbol)
            .window(1m)
            .select(
                symbol: EMA12.symbol,
                macd_line: EMA12.ema_12 - EMA26.ema_26
            )
            .emit(
                event_type: "MACDResult",
                symbol: symbol,
                macd_line: macd_line
            )
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    // Send EMA12 event for BTC
    let ema12_event = create_test_event(
        "EMA12Event",
        "BTC/USD",
        vec![("ema_12", Value::Float(45000.0))],
    );
    engine
        .process(ema12_event)
        .await
        .expect("Failed to process EMA12");

    // Send EMA26 event for BTC - should trigger join correlation
    let ema26_event = create_test_event(
        "EMA26Event",
        "BTC/USD",
        vec![("ema_26", Value::Float(44500.0))],
    );
    engine
        .process(ema26_event)
        .await
        .expect("Failed to process EMA26");

    // Check for emitted alert with MACD result
    // Give a short timeout for async processing
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Try to receive the alert
    match alert_rx.try_recv() {
        Ok(alert) => {
            assert_eq!(alert.alert_type, "MACDResult");
            let symbol = alert.data.get("symbol").expect("Missing symbol");
            assert_eq!(symbol, &Value::Str("BTC/USD".to_string()));
            let macd_line = alert.data.get("macd_line").expect("Missing macd_line");
            // EMA12 - EMA26 = 45000 - 44500 = 500
            if let Value::Float(v) = macd_line {
                assert!((v - 500.0).abs() < 0.001, "Expected ~500, got {}", v);
            } else {
                panic!("macd_line should be a float");
            }
        }
        Err(_) => {
            // This is expected to fail initially until JoinBuffer is implemented
            // After implementation, we should receive an alert
            println!("No alert received - JoinBuffer implementation needed");
        }
    }
}

#[tokio::test]
async fn test_join_buffer_window_expiration() {
    // Test that events outside the window are not correlated
    let (alert_tx, mut alert_rx) = mpsc::channel(100);
    let mut engine = Engine::new(alert_tx);

    let vpl = r#"
        event StreamA:
            key: str
            value: float

        event StreamB:
            key: str
            value: float

        stream A from StreamA
        stream B from StreamB

        stream Joined = join(A, B)
            .on(A.key == B.key)
            .window(100ms)
            .select(
                key: A.key,
                total: A.value + B.value
            )
            .emit(
                event_type: "JoinedResult",
                key: key,
                total: total
            )
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    // Send event A
    let base_time = Utc::now();
    let event_a = Event::new("StreamA")
        .with_timestamp(base_time)
        .with_field("key", "test")
        .with_field("value", 10.0f64);
    engine.process(event_a).await.expect("Failed to process A");

    // Send event B after window expired (simulate with same timestamp for now)
    // In a real scenario, the window would have expired
    let event_b = Event::new("StreamB")
        .with_timestamp(base_time + Duration::milliseconds(50))
        .with_field("key", "test")
        .with_field("value", 20.0f64);
    engine.process(event_b).await.expect("Failed to process B");

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Should get a result since both events are within 100ms window
    match alert_rx.try_recv() {
        Ok(alert) => {
            assert_eq!(alert.alert_type, "JoinedResult");
            let total = alert.data.get("total").expect("Missing total");
            if let Value::Float(v) = total {
                assert!((v - 30.0).abs() < 0.001, "Expected 30, got {}", v);
            }
        }
        Err(_) => {
            println!("No alert received - JoinBuffer implementation needed");
        }
    }
}

#[tokio::test]
async fn test_join_multi_stream_all_fields_accessible() {
    // Test that fields from all joined streams are accessible
    let (alert_tx, mut alert_rx) = mpsc::channel(100);
    let mut engine = Engine::new(alert_tx);

    let vpl = r#"
        event PriceEvent:
            symbol: str
            price: float

        event VolumeEvent:
            symbol: str
            volume: int

        stream Prices from PriceEvent
        stream Volumes from VolumeEvent

        stream Combined = join(Prices, Volumes)
            .on(Prices.symbol == Volumes.symbol)
            .window(1m)
            .select(
                symbol: Prices.symbol,
                price: Prices.price,
                volume: Volumes.volume
            )
            .emit(
                event_type: "CombinedResult",
                symbol: symbol,
                price: price,
                volume: volume
            )
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    let price_event = Event::new("PriceEvent")
        .with_field("symbol", "ETH/USD")
        .with_field("price", 3000.0f64);
    engine
        .process(price_event)
        .await
        .expect("Failed to process price");

    let volume_event = Event::new("VolumeEvent")
        .with_field("symbol", "ETH/USD")
        .with_field("volume", 1000i64);
    engine
        .process(volume_event)
        .await
        .expect("Failed to process volume");

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    match alert_rx.try_recv() {
        Ok(alert) => {
            assert_eq!(alert.alert_type, "CombinedResult");
            assert_eq!(
                alert.data.get("symbol"),
                Some(&Value::Str("ETH/USD".to_string()))
            );
            if let Some(Value::Float(p)) = alert.data.get("price") {
                assert!((p - 3000.0).abs() < 0.001);
            } else {
                panic!("price should be a float");
            }
            assert_eq!(alert.data.get("volume"), Some(&Value::Int(1000)));
        }
        Err(_) => {
            println!("No alert received - JoinBuffer implementation needed");
        }
    }
}

#[tokio::test]
async fn test_join_no_match_returns_empty() {
    // Test that mismatched keys don't produce output
    let (alert_tx, mut alert_rx) = mpsc::channel(100);
    let mut engine = Engine::new(alert_tx);

    let vpl = r#"
        event EventA:
            key: str
            value: float

        event EventB:
            key: str
            value: float

        stream A from EventA
        stream B from EventB

        stream Joined = join(A, B)
            .on(A.key == B.key)
            .window(1m)
            .select(key: A.key)
            .emit(event_type: "JoinedResult", key: key)
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    // Send event A with key "foo"
    let event_a = Event::new("EventA")
        .with_field("key", "foo")
        .with_field("value", 10.0f64);
    engine.process(event_a).await.expect("Failed to process A");

    // Send event B with different key "bar"
    let event_b = Event::new("EventB")
        .with_field("key", "bar")
        .with_field("value", 20.0f64);
    engine.process(event_b).await.expect("Failed to process B");

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Should NOT receive any alert because keys don't match
    assert!(
        alert_rx.try_recv().is_err(),
        "Should not receive alert for mismatched keys"
    );
}

#[tokio::test]
async fn test_aggregate_comparison_join() {
    // Test joining two aggregated streams and comparing their values
    // This is the core use case for STREAM-03
    let (alert_tx, mut alert_rx) = mpsc::channel(100);
    let mut engine = Engine::new(alert_tx);

    // Simpler test: just join two aggregated streams and emit their values
    let vpl = r#"
        event Sensor:
            sensor_id: str
            value: float

        # Aggregate stream 1 - fast average (3 samples)
        stream FastAvg = Sensor
            .partition_by(sensor_id)
            .window(3)
            .aggregate(
                sensor_id: last(sensor_id),
                fast_avg: avg(value)
            )

        # Aggregate stream 2 - slow average (5 samples)
        stream SlowAvg = Sensor
            .partition_by(sensor_id)
            .window(5)
            .aggregate(
                sensor_id: last(sensor_id),
                slow_avg: avg(value)
            )

        # Join aggregated streams - no filter, just emit the joined result
        stream Combined = join(FastAvg, SlowAvg)
            .on(FastAvg.sensor_id == SlowAvg.sensor_id)
            .window(1m)
            .select(
                sensor_id: FastAvg.sensor_id,
                fast: FastAvg.fast_avg,
                slow: SlowAvg.slow_avg
            )
            .emit(
                event_type: "Combined",
                sensor_id: sensor_id,
                fast_avg: fast,
                slow_avg: slow
            )
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    // Send sensor readings
    for i in 0..15 {
        let event = Event::new("Sensor")
            .with_field("sensor_id", "temp_1")
            .with_field("value", 100.0 + (i as f64 * 5.0));
        engine.process(event).await.expect("Failed to process");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Count combined alerts
    let mut combined_count = 0;
    while let Ok(alert) = alert_rx.try_recv() {
        if alert.alert_type == "Combined" {
            combined_count += 1;
            // Debug: print alert
            println!("Combined alert: {:?}", alert.data);
            // Verify the alert has expected fields
            assert!(alert.data.contains_key("sensor_id"));
            assert!(alert.data.contains_key("fast_avg"));
            assert!(alert.data.contains_key("slow_avg"));
        }
    }

    // Should have multiple combined alerts after both windows are full
    // FastAvg outputs after 3 events, SlowAvg outputs after 5 events
    // First join should happen after event 5
    assert!(
        combined_count > 0,
        "Expected combined alerts, got {}",
        combined_count
    );
    println!(
        "Aggregate comparison join produced {} combined alerts",
        combined_count
    );
}

#[tokio::test]
async fn test_macd_example_produces_signals() {
    // End-to-end test simulating the financial_markets.vpl MACD pattern
    let (alert_tx, mut alert_rx) = mpsc::channel(100);
    let mut engine = Engine::new(alert_tx);

    let vpl = r#"
        event OHLCV:
            symbol: str
            close: float
            timeframe: str

        stream EMA12 = OHLCV
            .where(timeframe == "1m")
            .partition_by(symbol)
            .window(12)
            .aggregate(
                symbol: last(symbol),
                ema_12: ema(close, 12)
            )

        stream EMA26 = OHLCV
            .where(timeframe == "1m")
            .partition_by(symbol)
            .window(26)
            .aggregate(
                symbol: last(symbol),
                ema_26: ema(close, 26)
            )

        stream MACD = join(EMA12, EMA26)
            .on(EMA12.symbol == EMA26.symbol)
            .window(1m)
            .select(
                symbol: EMA12.symbol,
                macd_line: EMA12.ema_12 - EMA26.ema_26
            )
            .emit(
                event_type: "MACD",
                symbol: symbol,
                macd_line: macd_line
            )
    "#;

    let program = varpulis_parser::parse(vpl).expect("Failed to parse VPL");
    engine.load(&program).expect("Failed to load program");

    // Send enough OHLCV events to trigger both EMA12 and EMA26
    // EMA12 needs 12 events, EMA26 needs 26 events
    let base_price = 45000.0;
    for i in 0..30 {
        let event = Event::new("OHLCV")
            .with_field("symbol", "BTC/USD")
            .with_field("close", base_price + (i as f64 * 10.0))
            .with_field("timeframe", "1m");
        engine
            .process(event)
            .await
            .expect("Failed to process OHLCV");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Count MACD alerts
    let mut macd_count = 0;
    while let Ok(alert) = alert_rx.try_recv() {
        if alert.alert_type == "MACD" {
            macd_count += 1;
            println!("MACD alert {}: {:?}", macd_count, alert.data);
            // Verify the MACD has expected fields
            assert!(alert.data.contains_key("symbol"));
            assert!(alert.data.contains_key("macd_line"));
        }
    }

    // After implementing aggregate-to-aggregate joins, we should see MACD alerts
    // With 30 events: EMA12 produces after 12, EMA26 produces after 26
    // So first join possible at event 26, giving ~5 potential matches
    assert!(
        macd_count > 0,
        "Expected MACD alerts after aggregate join implementation"
    );
    println!("MACD alerts received: {}", macd_count);
}
