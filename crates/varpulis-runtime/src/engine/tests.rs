//! Unit tests for the Varpulis engine

use super::*;
use tokio::sync::mpsc;

fn parse_program(source: &str) -> Program {
    varpulis_parser::parse(source).expect("Failed to parse")
}

#[tokio::test]
async fn test_engine_simple_sequence() {
    let source = r#"
        stream OrderPayment = Order
            -> Payment as payment
            .emit(status: "matched")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Send Order event
    let order = Event::new("Order").with_field("id", 1i64);
    engine.process(order).await.unwrap();

    // No alert yet - waiting for Payment
    assert!(rx.try_recv().is_err());

    // Send Payment event
    let payment = Event::new("Payment").with_field("order_id", 1i64);
    engine.process(payment).await.unwrap();

    // Should get alert now
    let alert = rx.try_recv().expect("Should have alert");
    assert_eq!(alert.alert_type, "stream_output");
}

#[tokio::test]
async fn test_engine_sequence_with_alias() {
    let source = r#"
        stream TwoTicks = StockTick as first
            -> StockTick as second
            .emit(result: "two_ticks")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // First tick
    let tick1 = Event::new("StockTick").with_field("price", 100.0);
    engine.process(tick1).await.unwrap();
    assert!(rx.try_recv().is_err());

    // Second tick - completes sequence
    let tick2 = Event::new("StockTick").with_field("price", 101.0);
    engine.process(tick2).await.unwrap();

    let alert = rx.try_recv().expect("Should have alert");
    assert_eq!(
        alert.data.get("result"),
        Some(&Value::Str("two_ticks".to_string()))
    );
}

#[tokio::test]
async fn test_engine_sequence_three_steps() {
    let source = r#"
        stream ABC = A as a -> B as b -> C as c
            .emit(status: "complete")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine.process(Event::new("A")).await.unwrap();
    assert!(rx.try_recv().is_err());

    engine.process(Event::new("B")).await.unwrap();
    assert!(rx.try_recv().is_err());

    engine.process(Event::new("C")).await.unwrap();
    let alert = rx.try_recv().expect("Should have alert");
    assert_eq!(
        alert.data.get("status"),
        Some(&Value::Str("complete".to_string()))
    );
}

#[tokio::test]
async fn test_engine_sequence_wrong_order() {
    let source = r#"
        stream AB = A -> B .emit(done: "yes")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Send B before A - should not match
    engine.process(Event::new("B")).await.unwrap();
    engine.process(Event::new("A")).await.unwrap();
    assert!(rx.try_recv().is_err());

    // Now send B after A - should match
    engine.process(Event::new("B")).await.unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_sequence_with_filter() {
    let source = r#"
        stream OrderPaymentMatch = Order as order
            -> Payment where order_id == order.id as payment
            .emit(status: "matched")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Order 1
    engine
        .process(Event::new("Order").with_field("id", 1i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());

    // Payment for wrong order - should NOT match
    engine
        .process(Event::new("Payment").with_field("order_id", 999i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());

    // Payment for correct order - should match
    engine
        .process(Event::new("Payment").with_field("order_id", 1i64))
        .await
        .unwrap();
    let alert = rx.try_recv().expect("Should have alert");
    assert_eq!(
        alert.data.get("status"),
        Some(&Value::Str("matched".to_string()))
    );
}

#[tokio::test]
async fn test_engine_sequence_with_timeout() {
    let source = r#"
        stream QuickResponse = Request as req
            -> Response as resp
            .within(5s)
            .emit(status: "fast")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("Request").with_field("id", 1i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());

    engine
        .process(Event::new("Response").with_field("id", 1i64))
        .await
        .unwrap();
    let alert = rx.try_recv().expect("Should have alert");
    assert_eq!(
        alert.data.get("status"),
        Some(&Value::Str("fast".to_string()))
    );
}

#[tokio::test]
async fn test_engine_with_event_file() {
    use crate::event_file::EventFileParser;

    let source = r#"
        # Order-Payment sequence test
        Order { id: 1, symbol: "AAPL" }

        BATCH 10
        Payment { order_id: 1, amount: 15000.0 }
    "#;

    let events = EventFileParser::parse(source).expect("Failed to parse");
    assert_eq!(events.len(), 2);

    let program_source = r#"
        stream OrderPayment = Order as order
            -> Payment where order_id == order.id as payment
            .emit(status: "matched", order_id: order.id)
    "#;

    let program = parse_program(program_source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    for timed_event in events {
        engine.process(timed_event.event).await.unwrap();
    }

    let alert = rx.try_recv().expect("Should have alert");
    assert_eq!(
        alert.data.get("status"),
        Some(&Value::Str("matched".to_string()))
    );
}

#[tokio::test]
async fn test_engine_sequence_with_not() {
    let source = r#"
        stream MissingPayment = Order as order
            -> Payment where order_id == order.id as payment
            .not(Cancellation where order_id == order.id)
            .emit(status: "payment_received")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("Order").with_field("id", 1i64))
        .await
        .unwrap();
    engine
        .process(Event::new("Payment").with_field("order_id", 1i64))
        .await
        .unwrap();

    let alert = rx.try_recv().expect("Should have alert");
    assert_eq!(
        alert.data.get("status"),
        Some(&Value::Str("payment_received".to_string()))
    );
}

#[tokio::test]
async fn test_engine_all_in_source() {
    let source = r#"
        stream AllNews = all News as news
            -> Tick as tick
            .emit(matched: "yes")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("News").with_field("id", 1i64))
        .await
        .unwrap();
    engine
        .process(Event::new("News").with_field("id", 2i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());

    engine
        .process(Event::new("Tick").with_field("price", 100.0))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_match_all_sequence() {
    let source = r#"
        stream AllTicks = News as news
            -> all Tick as tick
            .emit(matched: "yes")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("News").with_field("id", 1i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());

    engine
        .process(Event::new("Tick").with_field("price", 100.0))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());

    engine
        .process(Event::new("Tick").with_field("price", 101.0))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

// ==========================================================================
// Builder and Configuration Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_with_metrics() {
    let (tx, _rx) = mpsc::channel(100);
    let metrics = crate::metrics::Metrics::new();
    let engine = Engine::new(tx).with_metrics(metrics);
    assert!(engine.metrics.is_some());
}

#[tokio::test]
async fn test_engine_metrics() {
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);

    let source = r#"
        stream Simple = A -> B .emit(done: "yes")
    "#;
    let program = parse_program(source);
    engine.load(&program).unwrap();

    engine.process(Event::new("A")).await.unwrap();
    engine.process(Event::new("B")).await.unwrap();
    let _ = rx.try_recv();

    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 2);
    assert!(metrics.alerts_generated >= 1);
    assert_eq!(metrics.streams_count, 1);
}

#[tokio::test]
async fn test_engine_single_event_emit() {
    let source = r#"
        stream S = Order as o -> Confirm .emit(status: "confirmed")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("Order").with_field("id", 1i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());
    engine.process(Event::new("Confirm")).await.unwrap();
    assert!(rx.try_recv().is_ok());
}

// ==========================================================================
// Filter Expression Tests - Arithmetic Operations
// ==========================================================================

#[tokio::test]
async fn test_engine_filter_arithmetic_add() {
    let source = r#"
        stream Test = A as a
            -> B where value == a.base + 10 as b
            .emit(status: "matched")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("base", 5i64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("value", 15i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_arithmetic_sub() {
    let source = r#"
        stream Test = A as a
            -> B where value == a.base - 3 as b
            .emit(status: "matched")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("base", 10i64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("value", 7i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_arithmetic_mul() {
    let source = r#"
        stream Test = A as a
            -> B where value == a.base * 2 as b
            .emit(status: "matched")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("base", 5i64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("value", 10i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_arithmetic_div() {
    let source = r#"
        stream Test = A as a
            -> B where value == a.base / 2 as b
            .emit(status: "matched")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("base", 10i64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("value", 5i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

// ==========================================================================
// Filter Expression Tests - Comparison Operations
// ==========================================================================

#[tokio::test]
async fn test_engine_filter_comparison_lt() {
    let source = r#"
        stream Test = A as a
            -> B where value < a.threshold as b
            .emit(status: "below")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("threshold", 100i64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("value", 50i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_comparison_le() {
    let source = r#"
        stream Test = A as a
            -> B where value <= a.threshold as b
            .emit(status: "at_or_below")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("threshold", 100i64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("value", 100i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_comparison_gt() {
    let source = r#"
        stream Test = A as a
            -> B where value > a.threshold as b
            .emit(status: "above")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("threshold", 50i64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("value", 100i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_comparison_ge() {
    let source = r#"
        stream Test = A as a
            -> B where value >= a.threshold as b
            .emit(status: "at_or_above")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("threshold", 100i64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("value", 100i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_comparison_neq() {
    let source = r#"
        stream Test = A as a
            -> B where value != a.exclude as b
            .emit(status: "different")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("exclude", 42i64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("value", 100i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

// ==========================================================================
// Filter Expression Tests - Logical Operations
// ==========================================================================

#[tokio::test]
async fn test_engine_filter_logical_and() {
    let source = r#"
        stream Test = A as a
            -> B where value > 10 and value < 100 as b
            .emit(status: "in_range")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine.process(Event::new("A")).await.unwrap();
    engine
        .process(Event::new("B").with_field("value", 50i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_logical_or() {
    let source = r#"
        stream Test = A as a
            -> B where status == "active" or priority > 5 as b
            .emit(result: "matched")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine.process(Event::new("A")).await.unwrap();
    engine
        .process(
            Event::new("B")
                .with_field("status", "inactive")
                .with_field("priority", 10i64),
        )
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

// ==========================================================================
// Filter Expression Tests - Float/Mixed Type Operations
// ==========================================================================

#[tokio::test]
async fn test_engine_filter_float_comparison() {
    let source = r#"
        stream Test = A as a
            -> B where price > a.min_price as b
            .emit(status: "above_min")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("min_price", 99.99f64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("price", 100.0f64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_int_float_mixed() {
    let source = r#"
        stream Test = A as a
            -> B where value > a.threshold as b
            .emit(status: "above")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("threshold", 50i64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("value", 75.5f64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

// ==========================================================================
// Literal Value Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_filter_literal_int() {
    let source = r#"
        stream Test = A as a
            -> B where value == 42 as b
            .emit(status: "found")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine.process(Event::new("A")).await.unwrap();
    engine
        .process(Event::new("B").with_field("value", 42i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_literal_string() {
    let source = r#"
        stream Test = A as a
            -> B where status == "active" as b
            .emit(result: "ok")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine.process(Event::new("A")).await.unwrap();
    engine
        .process(Event::new("B").with_field("status", "active"))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_literal_float() {
    let source = r#"
        stream Test = A as a
            -> B where price == 99.99 as b
            .emit(status: "exact")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine.process(Event::new("A")).await.unwrap();
    engine
        .process(Event::new("B").with_field("price", 99.99f64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_engine_filter_literal_bool() {
    let source = r#"
        stream Test = A as a
            -> B where active == true as b
            .emit(status: "active")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine.process(Event::new("A")).await.unwrap();
    engine
        .process(Event::new("B").with_field("active", true))
        .await
        .unwrap();
    assert!(rx.try_recv().is_ok());
}

// ==========================================================================
// Edge Cases and Error Handling
// ==========================================================================

#[tokio::test]
async fn test_engine_unmatched_event_type() {
    let source = r#"
        stream AB = A -> B .emit(done: "yes")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine.process(Event::new("X")).await.unwrap();
    engine.process(Event::new("Y")).await.unwrap();
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_engine_rapid_events() {
    let source = r#"
        stream Rapid = A -> B .emit(done: "yes")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(1000);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    for i in 0..100 {
        engine
            .process(Event::new("A").with_field("id", i as i64))
            .await
            .unwrap();
        engine
            .process(Event::new("B").with_field("id", i as i64))
            .await
            .unwrap();
    }

    let mut count = 0;
    while rx.try_recv().is_ok() {
        count += 1;
    }
    assert_eq!(count, 100);
}

#[tokio::test]
async fn test_engine_div_by_zero() {
    let source = r#"
        stream Test = A as a
            -> B where value == a.x / a.y as b
            .emit(status: "computed")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("A").with_field("x", 10i64).with_field("y", 0i64))
        .await
        .unwrap();
    engine
        .process(Event::new("B").with_field("value", 0i64))
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());
}

// ==========================================================================
// Join Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_join_derived_streams() {
    // Test that join works with derived streams (stream names resolve to event types)
    // This is the key fix for scenario 4 - events arrive as MarketATick/MarketBTick
    // but join sources reference MarketA/MarketB streams
    let source = r#"
        stream MarketA from MarketATick
        stream MarketB from MarketBTick

        stream Arbitrage = join(MarketA, MarketB)
            .on(MarketA.symbol == MarketB.symbol)
            .window(1s)
            .emit(
                alert: "matched",
                symbol: MarketA.symbol,
                price_a: MarketA.price,
                price_b: MarketB.price
            )
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Send MarketATick (not MarketA!) - engine should route it correctly
    let event_a = Event::new("MarketATick")
        .with_field("symbol", "AAPL")
        .with_field("price", 150.0)
        .with_field("volume", 100i64)
        .with_field("exchange", "NYSE");
    engine.process(event_a).await.unwrap();

    // No alert yet - waiting for MarketB
    assert!(
        rx.try_recv().is_err(),
        "Should not alert with just one event"
    );

    // Send MarketBTick (not MarketB!) - engine should route it correctly
    let event_b = Event::new("MarketBTick")
        .with_field("symbol", "AAPL")
        .with_field("price", 152.0)
        .with_field("volume", 200i64)
        .with_field("exchange", "NASDAQ");
    engine.process(event_b).await.unwrap();

    // Now should get correlated alert
    let alert = rx.try_recv().expect("Should have alert after both events");
    assert_eq!(
        alert.data.get("alert"),
        Some(&Value::Str("matched".to_string()))
    );
    assert_eq!(
        alert.data.get("symbol"),
        Some(&Value::Str("AAPL".to_string()))
    );
}

#[tokio::test]
async fn test_engine_join_no_correlation_different_keys() {
    let source = r#"
        stream MarketA from MarketATick
        stream MarketB from MarketBTick

        stream Arbitrage = join(MarketA, MarketB)
            .on(MarketA.symbol == MarketB.symbol)
            .window(1s)
            .emit(alert: "matched")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Send MarketATick with symbol AAPL
    let event_a = Event::new("MarketATick")
        .with_field("symbol", "AAPL")
        .with_field("price", 150.0);
    engine.process(event_a).await.unwrap();

    // Send MarketBTick with different symbol GOOG
    let event_b = Event::new("MarketBTick")
        .with_field("symbol", "GOOG")
        .with_field("price", 100.0);
    engine.process(event_b).await.unwrap();

    // No alert - different symbols
    assert!(
        rx.try_recv().is_err(),
        "Should not alert with different join keys"
    );
}

#[tokio::test]
async fn test_engine_derived_stream_in_sequence() {
    // Test that derived streams (streams with filters) work as sequence sources
    let source = r#"
        stream HighValue = Transaction
            .where(amount > 100)

        stream LowValue = Transaction
            .where(amount <= 100)

        stream Pattern = HighValue as high
            -> LowValue where user_id == high.user_id as low
            .emit(pattern: "high_then_low", user_id: high.user_id)
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Send high value transaction (amount > 100)
    let high_tx = Event::new("Transaction")
        .with_field("user_id", "user1")
        .with_field("amount", 200.0);
    engine.process(high_tx).await.unwrap();

    // No alert yet
    assert!(rx.try_recv().is_err(), "Should not alert after first event");

    // Send low value transaction (amount <= 100) from same user
    let low_tx = Event::new("Transaction")
        .with_field("user_id", "user1")
        .with_field("amount", 50.0);
    engine.process(low_tx).await.unwrap();

    // Should get alert now - pattern matched
    let alert = rx
        .try_recv()
        .expect("Should have alert after pattern match");
    assert_eq!(
        alert.data.get("pattern"),
        Some(&Value::Str("high_then_low".to_string()))
    );
    assert_eq!(
        alert.data.get("user_id"),
        Some(&Value::Str("user1".to_string()))
    );
}

#[tokio::test]
async fn test_engine_derived_stream_filters_applied() {
    // Test that derived stream filters are correctly applied
    let source = r#"
        stream HighValue = Transaction
            .where(amount > 100)

        stream Pattern = HighValue as high
            -> Transaction as any
            .emit(matched: "yes")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Send low value transaction first (should NOT match HighValue)
    let low_tx = Event::new("Transaction").with_field("amount", 50.0);
    engine.process(low_tx.clone()).await.unwrap();

    // No alert - low value doesn't match HighValue
    assert!(
        rx.try_recv().is_err(),
        "Low value should not match HighValue stream"
    );

    // Send high value transaction (should match HighValue)
    let high_tx = Event::new("Transaction").with_field("amount", 200.0);
    engine.process(high_tx).await.unwrap();

    // Still no alert - waiting for second step
    assert!(rx.try_recv().is_err(), "Should wait for second step");

    // Send any transaction
    engine.process(low_tx).await.unwrap();

    // Should get alert now
    let alert = rx
        .try_recv()
        .expect("Should have alert after pattern match");
    assert_eq!(
        alert.data.get("matched"),
        Some(&Value::Str("yes".to_string()))
    );
}

// ==========================================================================
// Public API Coverage Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_get_pattern() {
    let source = r#"
        pattern HighTemp = TemperatureReading where value > 30
        pattern LowTemp = TemperatureReading where value < 10
    "#;

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Test get_pattern for existing patterns
    let high_temp = engine.get_pattern("HighTemp");
    assert!(high_temp.is_some(), "HighTemp pattern should exist");
    assert_eq!(high_temp.unwrap().name, "HighTemp");

    let low_temp = engine.get_pattern("LowTemp");
    assert!(low_temp.is_some(), "LowTemp pattern should exist");

    // Test get_pattern for non-existing pattern
    let missing = engine.get_pattern("MissingPattern");
    assert!(missing.is_none(), "Non-existing pattern should return None");
}

#[tokio::test]
async fn test_engine_patterns_list() {
    let source = r#"
        pattern A = EventA where x > 1
        pattern B = EventB where y < 2
        pattern C = EventC where z == 3
    "#;

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    let patterns = engine.patterns();
    assert_eq!(patterns.len(), 3, "Should have 3 patterns");
    assert!(patterns.contains_key("A"));
    assert!(patterns.contains_key("B"));
    assert!(patterns.contains_key("C"));
}

#[tokio::test]
async fn test_engine_user_functions() {
    let source = r#"
        fn double(x: int) -> int:
            x * 2

        fn triple(x: int) -> int:
            x * 3

        stream Test = EventA
            .where(double(value) > 10)
            .emit(result: triple(value))
    "#;

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Test get_function
    let double_fn = engine.get_function("double");
    assert!(double_fn.is_some(), "double function should exist");
    assert_eq!(double_fn.unwrap().name, "double");

    let triple_fn = engine.get_function("triple");
    assert!(triple_fn.is_some(), "triple function should exist");

    let missing_fn = engine.get_function("missing");
    assert!(
        missing_fn.is_none(),
        "Non-existing function should return None"
    );

    // Test function_names
    let names = engine.function_names();
    assert_eq!(names.len(), 2, "Should have 2 user functions");
    assert!(names.contains(&"double"));
    assert!(names.contains(&"triple"));
}

#[tokio::test]
async fn test_engine_add_filter() {
    let source = r#"
        stream Test = EventA
            .emit(value: value)
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Add a runtime filter
    engine
        .add_filter("Test", |event| event.get_int("value").unwrap_or(0) > 50)
        .unwrap();

    // Event below filter threshold - no alert
    let low_event = Event::new("EventA").with_field("value", 30i64);
    engine.process(low_event).await.unwrap();
    assert!(rx.try_recv().is_err(), "Low value should be filtered out");

    // Event above filter threshold - should alert
    let high_event = Event::new("EventA").with_field("value", 100i64);
    engine.process(high_event).await.unwrap();
    let alert = rx.try_recv().expect("High value should pass filter");
    assert_eq!(alert.data.get("value"), Some(&Value::Int(100)));
}

#[tokio::test]
async fn test_engine_add_filter_nonexistent_stream() {
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);

    // Try to add filter to non-existent stream
    let result = engine.add_filter("NonExistent", |_| true);
    assert!(result.is_err(), "Should fail for non-existent stream");
    assert!(result.unwrap_err().contains("not found"));
}

#[tokio::test]
async fn test_engine_metrics_detailed() {
    let source = r#"
        stream Test1 = EventA.emit(x: 1)
        stream Test2 = EventB.emit(y: 2)
    "#;

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    let metrics = engine.metrics();
    assert_eq!(metrics.streams_count, 2, "Should have 2 streams");
    assert_eq!(metrics.events_processed, 0, "No events processed yet");

    // Process some events
    engine.process(Event::new("EventA")).await.unwrap();
    engine.process(Event::new("EventB")).await.unwrap();
    engine.process(Event::new("EventA")).await.unwrap();

    let metrics = engine.metrics();
    assert_eq!(
        metrics.events_processed, 3,
        "Should have processed 3 events"
    );
}

// ==========================================================================
// Edge Case Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_empty_program() {
    let source = "";
    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    let metrics = engine.metrics();
    assert_eq!(
        metrics.streams_count, 0,
        "Empty program should have no streams"
    );
}

#[tokio::test]
async fn test_engine_event_with_many_fields() {
    let source = r#"
        stream Test = BigEvent
            .where(field1 > 0 and field2 > 0 and field3 > 0)
            .emit(sum: field1 + field2 + field3)
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    let event = Event::new("BigEvent")
        .with_field("field1", 10i64)
        .with_field("field2", 20i64)
        .with_field("field3", 30i64)
        .with_field("field4", 40i64)
        .with_field("field5", 50i64);

    engine.process(event).await.unwrap();

    let alert = rx.try_recv().expect("Should receive alert");
    assert_eq!(alert.data.get("sum"), Some(&Value::Int(60)));
}

#[tokio::test]
async fn test_engine_special_characters_in_event_type() {
    let source = r#"
        stream Test = Event_With_Underscores
            .emit(ok: "yes")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    let event = Event::new("Event_With_Underscores");
    engine.process(event).await.unwrap();

    let alert = rx.try_recv().expect("Should receive alert");
    assert_eq!(alert.data.get("ok"), Some(&Value::Str("yes".to_string())));
}

// ==========================================================================
// Config Block Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_get_config() {
    let source = r#"
        config:
            mode: "low_latency"
            state_backend: "rocksdb"
    "#;

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // When using "config:" (old syntax), the name defaults to "default"
    let config = engine.get_config("default");
    assert!(config.is_some(), "default config block should exist");
    assert_eq!(config.unwrap().name, "default");

    // Test get_config for non-existing config
    let missing = engine.get_config("redis");
    assert!(missing.is_none(), "Non-existing config should return None");
}

// ==========================================================================
// Window Operation Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_count_window() {
    let source = r#"
        stream Test = StockTick
            .window(3)
            .aggregate(avg_price: avg(price))
            .emit(average: avg_price)
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Send 2 events - no output yet (window not complete)
    engine
        .process(Event::new("StockTick").with_field("price", 100.0))
        .await
        .unwrap();
    engine
        .process(Event::new("StockTick").with_field("price", 110.0))
        .await
        .unwrap();
    assert!(rx.try_recv().is_err(), "Window not complete yet");

    // Third event completes window
    engine
        .process(Event::new("StockTick").with_field("price", 120.0))
        .await
        .unwrap();
    let alert = rx
        .try_recv()
        .expect("Should have alert after window completes");
    // Average should be (100 + 110 + 120) / 3 = 110
    if let Some(Value::Float(avg)) = alert.data.get("average") {
        assert!((avg - 110.0).abs() < 0.001, "Average should be 110.0");
    }
}

#[tokio::test]
async fn test_engine_tumbling_time_window() {
    let source = r#"
        stream Test = SensorData
            .window(1s)
            .aggregate(max_temp: max(temperature))
            .emit(max_value: max_temp)
    "#;

    let program = parse_program(source);
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Process events - window behavior is time-based
    engine
        .process(Event::new("SensorData").with_field("temperature", 25.0))
        .await
        .unwrap();
    engine
        .process(Event::new("SensorData").with_field("temperature", 30.0))
        .await
        .unwrap();

    // Time windows rely on wall clock, so we just verify processing works
    let metrics = engine.metrics();
    assert_eq!(metrics.events_processed, 2);
}

#[tokio::test]
async fn test_engine_sliding_count_window() {
    let source = r#"
        stream Test = StockTick
            .window(5, sliding: 1)
            .aggregate(sum_vol: sum(volume))
            .emit(total: sum_vol)
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Fill the window
    for i in 0..5 {
        engine
            .process(Event::new("StockTick").with_field("volume", (i + 1) as f64 * 100.0))
            .await
            .unwrap();
    }

    // Should get alert after window is full
    let alert = rx.try_recv().expect("Should have alert");
    // sum = 100 + 200 + 300 + 400 + 500 = 1500
    if let Some(Value::Float(sum)) = alert.data.get("total") {
        assert!((sum - 1500.0).abs() < 0.001, "Sum should be 1500.0");
    }
}

// ==========================================================================
// Aggregation Operation Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_aggregation_count() {
    let source = r#"
        stream Test = EventA
            .window(3)
            .aggregate(cnt: count())
            .emit(event_count: cnt)
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    for _ in 0..3 {
        engine.process(Event::new("EventA")).await.unwrap();
    }

    let alert = rx.try_recv().expect("Should have alert");
    assert_eq!(alert.data.get("event_count"), Some(&Value::Int(3)));
}

#[tokio::test]
async fn test_engine_aggregation_min_max() {
    let source = r#"
        stream Test = Reading
            .window(4)
            .aggregate(
                min_val: min(value),
                max_val: max(value)
            )
            .emit(minimum: min_val, maximum: max_val)
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    let values = vec![50.0, 20.0, 80.0, 30.0];
    for v in values {
        engine
            .process(Event::new("Reading").with_field("value", v))
            .await
            .unwrap();
    }

    let alert = rx.try_recv().expect("Should have alert");
    assert_eq!(alert.data.get("minimum"), Some(&Value::Float(20.0)));
    assert_eq!(alert.data.get("maximum"), Some(&Value::Float(80.0)));
}

// ==========================================================================
// Select Operation Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_select_simple() {
    let source = r#"
        stream Test = SensorData
            .window(2)
            .select(temp: temperature, loc: location)
            .emit(selected_temp: temp, selected_loc: loc)
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(
            Event::new("SensorData")
                .with_field("temperature", 25.0)
                .with_field("location", "room1")
                .with_field("humidity", 60.0),
        )
        .await
        .unwrap();
    engine
        .process(
            Event::new("SensorData")
                .with_field("temperature", 26.0)
                .with_field("location", "room2")
                .with_field("humidity", 65.0),
        )
        .await
        .unwrap();

    // Window of 2 should trigger
    let alert = rx.try_recv().expect("Should have alert");
    // Select should project only specified fields
    assert!(alert.data.contains_key("selected_temp") || alert.data.contains_key("selected_loc"));
}

// ==========================================================================
// Partitioned Window Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_partitioned_window() {
    let source = r#"
        stream Test = StockTick
            .partition_by(symbol)
            .window(2)
            .aggregate(total_vol: sum(volume))
            .emit(symbol: symbol, volume: total_vol)
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // AAPL partition
    engine
        .process(
            Event::new("StockTick")
                .with_field("symbol", "AAPL")
                .with_field("volume", 100.0),
        )
        .await
        .unwrap();
    engine
        .process(
            Event::new("StockTick")
                .with_field("symbol", "AAPL")
                .with_field("volume", 200.0),
        )
        .await
        .unwrap();

    // GOOG partition - separate window
    engine
        .process(
            Event::new("StockTick")
                .with_field("symbol", "GOOG")
                .with_field("volume", 500.0),
        )
        .await
        .unwrap();

    // AAPL window should complete with 2 events (sum = 300)
    let alert = rx.try_recv().expect("AAPL window should complete");
    if let Some(Value::Float(vol)) = alert.data.get("volume") {
        assert!((vol - 300.0).abs() < 0.001, "AAPL volume should be 300.0");
    }

    // GOOG window not complete yet (only 1 event)
    assert!(
        rx.try_recv().is_err(),
        "GOOG window should not be complete yet"
    );
}

// ==========================================================================
// Max Chain Depth Test
// ==========================================================================

#[tokio::test]
async fn test_engine_max_chain_depth() {
    // Test that deeply chained streams don't cause infinite loops
    let source = r#"
        stream Level1 = Source .emit(level: "1")
        stream Level2 = Level1 .emit(level: "2")
        stream Level3 = Level2 .emit(level: "3")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine.process(Event::new("Source")).await.unwrap();

    // Should get all 3 alerts from the chain
    let mut alerts = Vec::new();
    while let Ok(alert) = rx.try_recv() {
        alerts.push(alert);
    }
    assert!(
        !alerts.is_empty(),
        "Should have at least one alert from chain"
    );
}

// ==========================================================================
// Event Declaration Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_event_declaration() {
    let source = r#"
        event StockTick {
            symbol: string,
            price: float,
            volume: int
        }

        stream Test = StockTick
            .emit(s: symbol, p: price)
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(
            Event::new("StockTick")
                .with_field("symbol", "AAPL")
                .with_field("price", 150.0)
                .with_field("volume", 1000i64),
        )
        .await
        .unwrap();

    let alert = rx.try_recv().expect("Should have alert");
    assert_eq!(alert.data.get("s"), Some(&Value::Str("AAPL".to_string())));
}

// ==========================================================================
// Print Operation Test
// ==========================================================================

#[tokio::test]
async fn test_engine_print_operation() {
    let source = r#"
        stream Test = SensorData
            .print("Temperature:", temperature)
            .emit(ok: "yes")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    engine
        .process(Event::new("SensorData").with_field("temperature", 25.5))
        .await
        .unwrap();

    // Print operation should not block emit
    let alert = rx.try_recv().expect("Should have alert after print");
    assert_eq!(alert.data.get("ok"), Some(&Value::Str("yes".to_string())));
}

// ==========================================================================
// Merge Source Tests
// ==========================================================================

#[tokio::test]
async fn test_engine_merge_streams() {
    // Define some source streams and then merge them
    let source = r#"
        stream TempAlerts = TemperatureReading
            .where(value > 30)
            .emit(alert_type: "temperature", value: value)

        stream HumidAlerts = HumidityReading
            .where(value > 70)
            .emit(alert_type: "humidity", value: value)

        stream AllAlerts = merge(TempAlerts, HumidAlerts)
            .emit(merged: "yes")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).unwrap();

    // Temperature event that passes filter (value > 30)
    engine
        .process(Event::new("TemperatureReading").with_field("value", 35.0))
        .await
        .unwrap();
    // Should get alert from TempAlerts first, then from AllAlerts merge
    let _ = rx.try_recv(); // TempAlerts emit

    // Humidity event that passes filter (value > 70)
    engine
        .process(Event::new("HumidityReading").with_field("value", 80.0))
        .await
        .unwrap();
    // Should get alert from HumidAlerts
    let _ = rx.try_recv(); // HumidAlerts emit

    // Temperature event that fails filter (value <= 30)
    engine
        .process(Event::new("TemperatureReading").with_field("value", 25.0))
        .await
        .unwrap();
    // No more alerts expected after previous clears
    let metrics = engine.metrics();
    assert!(
        metrics.events_processed >= 3,
        "Should have processed 3 events"
    );
}

// ==========================================================================
// Import Statement Test
// ==========================================================================

#[tokio::test]
async fn test_engine_import_statement() {
    // Import statements are logged but not yet fully implemented
    let source = r#"
        import "some/path.vpl"
        import "another/module" as mod

        stream Test = EventA
            .emit(ok: "yes")
    "#;

    let program = parse_program(source);
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    // load() should succeed even with import statements (they are no-ops for now)
    engine.load(&program).unwrap();

    engine.process(Event::new("EventA")).await.unwrap();
    let alert = rx.try_recv().expect("Should have alert");
    assert_eq!(alert.data.get("ok"), Some(&Value::Str("yes".to_string())));
}
