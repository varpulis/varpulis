//! "A happened, and B did not follow within D."
//!
//! The shape every webMethods use case in `docs/usecases-webmethods.md` asks
//! for — "alert if no 997 acknowledgement arrives within 4 hours" — and the
//! one `.not()` cannot express, because `.not()` cancels an in-flight run when
//! the forbidden event *arrives*.
//!
//! The grammar has always had `-> NOT B where …` in a `pattern` declaration.
//! It parsed, compiled, and emitted nothing, for four separate reasons:
//!
//! 1. The parser marks negation by prefixing the event type with `!`, and the
//!    compiler used that string as the type to match, so the state waited for
//!    an event literally named `!B`.
//! 2. The negation constraint was registered inside the transition loop, which
//!    only runs when the *next event* arrives — the one whose non-arrival is
//!    the point.
//! 3. Confirming a negation moved the run to `Accept` and returned nothing;
//!    the comment there said "will be handled by the main processing loop",
//!    and that loop advances on events.
//! 4. The confirmation ran, and then the same sweep dropped the run for having
//!    a deadline in the past — the deadline that had just completed it.

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

const PATTERN: &str = r"
event Order:
    id: str
event Ack:
    id: str

pattern Unacked =
    Order as o
    -> NOT Ack where id == o.id
    within 5m

stream Alerts = Unacked
    .emit(order: o.id)
";

fn at(kind: &str, id: &str, ms: i64) -> Event {
    use chrono::{TimeZone, Utc};
    let mut e = Event::new(kind);
    e.data.insert("id".into(), Value::Str(id.into()));
    e.timestamp = Utc.timestamp_millis_opt(ms).unwrap();
    e
}

/// Feed events, return the `order` field of every emitted alert.
async fn run(events: Vec<Event>) -> Vec<String> {
    let program = parse(PATTERN).expect("parse");
    let (tx, mut rx) = mpsc::channel(64);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");
    for e in events {
        engine.process(e).await.expect("process");
    }
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        if let Some(Value::Str(s)) = e.data.get("order") {
            out.push(s.to_string());
        }
    }
    out
}

/// The whole point: an order whose acknowledgement never came.
#[tokio::test]
async fn an_order_with_no_acknowledgement_alerts_once_the_deadline_passes() {
    // order-1 at t=0, deadline t=5m. The event at t=10m carries the watermark
    // past it.
    let alerts = run(vec![
        at("Order", "order-1", 0),
        at("Order", "order-2", 600_000),
    ])
    .await;
    assert_eq!(
        alerts,
        vec!["order-1"],
        "the unacknowledged order must alert when its deadline passes"
    );
}

/// And the one that was acknowledged must stay quiet — otherwise the feature
/// is just a delayed copy of its input.
#[tokio::test]
async fn an_acknowledged_order_never_alerts() {
    let alerts = run(vec![
        at("Order", "acked", 0),
        at("Ack", "acked", 60_000),
        at("Order", "later", 600_000),
    ])
    .await;
    assert!(
        !alerts.contains(&"acked".to_string()),
        "an acknowledged order must not alert, got {alerts:?}"
    );
}

/// The acknowledgement has to be for *this* order. A different id must not
/// satisfy the negation — that would make one ack silence every open order.
#[tokio::test]
async fn an_acknowledgement_for_a_different_order_does_not_count() {
    let alerts = run(vec![
        at("Order", "mine", 0),
        at("Ack", "someone-elses", 60_000),
        at("Order", "later", 600_000),
    ])
    .await;
    assert!(
        alerts.contains(&"mine".to_string()),
        "an ack for a different id must not silence this order, got {alerts:?}"
    );
}

/// Before the deadline, nothing. An alert that fires early is worse than one
/// that does not fire.
#[tokio::test]
async fn nothing_alerts_before_the_deadline() {
    let alerts = run(vec![
        at("Order", "recent", 0),
        at("Order", "also-recent", 60_000),
    ])
    .await;
    assert!(
        alerts.is_empty(),
        "no deadline has passed yet, got {alerts:?}"
    );
}
