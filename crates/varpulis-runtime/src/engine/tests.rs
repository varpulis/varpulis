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
