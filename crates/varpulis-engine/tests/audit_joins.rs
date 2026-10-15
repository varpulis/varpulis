//! A join runs in the engine as it ships, and an outer join emits an
//! unmatched event once, when its window closes.
//!
//! Joins parsed and checked, and the synchronous engine refused them when
//! the program loaded; the joins reference told the reader to write a
//! sequence instead. Once they ran, an outer join emitted an unmatched
//! event at once and again as a pair when its match arrived, so a paid
//! order appeared twice.

mod common;

use common::{at, end, feed, only};
use varpulis_engine::Program;

const EVENTS: &str = r"
event OrderEvent:
    order_id: str
    amount: float
event PaymentEvent:
    order_id: str
    paid: float
";

fn order(time: &str, id: &str, amount: f64) -> String {
    at(
        time,
        &format!(r#""order_id": "{id}", "amount": {amount:?}"#),
    )
}

fn payment(time: &str, id: &str, paid: f64) -> String {
    at(time, &format!(r#""order_id": "{id}", "paid": {paid:?}"#))
}

fn joined(kind: &str) -> String {
    format!(
        r"{EVENTS}
stream Paid = {kind}(
    stream Orders = OrderEvent,
    stream Payments = PaymentEvent on Orders.order_id == Payments.order_id
)
    .window(5m)
    .emit(order: Orders.order_id, amount: Orders.amount, paid: Payments.paid)
"
    )
}

#[test]
fn an_inner_join_emits_the_pair_once_both_sides_are_in() {
    let mut p = Program::compile(&joined("join")).unwrap_or_else(|e| panic!("{e}"));
    assert!(feed(&mut p, "OrderEvent", &order("10:00:00", "o1", 10.0)).is_empty());
    assert!(
        feed(&mut p, "PaymentEvent", &payment("10:00:05", "o2", 5.0)).is_empty(),
        "a payment for an order never seen matches nothing"
    );
    let out = only(feed(
        &mut p,
        "PaymentEvent",
        &payment("10:00:10", "o1", 10.0),
    ));
    assert_eq!(out["order"], "o1");
    assert_eq!(out["amount"], 10.0);
    assert_eq!(out["paid"], 10.0);
    assert!(
        end(&mut p).is_empty(),
        "an inner join emits nothing at the end"
    );
}

#[test]
fn an_inner_join_forgets_an_event_past_its_window() {
    let mut p = Program::compile(&joined("join")).unwrap();
    assert!(feed(&mut p, "OrderEvent", &order("10:00:00", "o1", 10.0)).is_empty());
    assert!(
        feed(&mut p, "PaymentEvent", &payment("10:06:00", "o1", 10.0)).is_empty(),
        "six minutes later the order has left the five-minute window"
    );
}

/// A left event with no match is emitted alone when its window closes: on
/// the clock of the join's sources, or at the end of the input. One that
/// found its match is the pair, and nothing else.
#[test]
fn a_left_join_emits_the_pair_or_the_left_event_alone_at_the_close() {
    let mut p = Program::compile(&joined("left_join")).unwrap();
    assert!(
        feed(&mut p, "OrderEvent", &order("10:00:00", "o1", 10.0)).is_empty(),
        "nothing at arrival: the match may still come"
    );
    assert!(feed(&mut p, "OrderEvent", &order("10:00:01", "o2", 20.0)).is_empty());
    let out = only(feed(
        &mut p,
        "PaymentEvent",
        &payment("10:00:10", "o1", 10.0),
    ));
    assert_eq!(out["order"], "o1");
    assert_eq!(out["paid"], 10.0, "the pair, once");
    assert!(
        feed(&mut p, "PaymentEvent", &payment("10:00:20", "o9", 1.0)).is_empty(),
        "a right event alone emits nothing from a left join, now or later"
    );
    // Both clocks past 10:05:01: o2's window has closed without a match.
    assert!(feed(&mut p, "OrderEvent", &order("10:06:00", "o3", 30.0)).is_empty());
    let out = only(feed(
        &mut p,
        "PaymentEvent",
        &payment("10:06:00", "o8", 1.0),
    ));
    assert_eq!(out["order"], "o2");
    assert_eq!(out["amount"], 20.0);
    assert!(
        out.get("paid").is_none_or(serde_json::Value::is_null),
        "no payment: {out}"
    );
    assert_eq!(
        out["timestamp"], "2026-10-14T10:00:01Z",
        "the order's own time"
    );
    let last = only(end(&mut p));
    assert_eq!(
        last["order"], "o3",
        "the end of the input closes o3's window"
    );
}

/// A right join preserves the right side.
#[test]
fn a_right_join_emits_the_right_event_alone_at_the_close() {
    let mut p = Program::compile(&joined("right_join")).unwrap();
    assert!(feed(&mut p, "OrderEvent", &order("10:00:00", "o1", 10.0)).is_empty());
    assert!(feed(&mut p, "PaymentEvent", &payment("10:00:05", "o9", 9.0)).is_empty());
    let out = end(&mut p);
    assert_eq!(out.len(), 1, "o1 is not preserved: {out:#?}");
    assert_eq!(out[0]["paid"], 9.0);
    assert!(out[0].get("order").is_none_or(serde_json::Value::is_null));
}

/// A full join preserves both sides, each alone at its close, in the order
/// they arrived.
#[test]
fn a_full_join_emits_either_side_alone_at_the_close() {
    let mut p = Program::compile(&joined("full_join")).unwrap();
    assert!(feed(&mut p, "PaymentEvent", &payment("10:00:00", "o9", 1.0)).is_empty());
    assert!(feed(&mut p, "OrderEvent", &order("10:00:01", "o1", 10.0)).is_empty());
    let out = end(&mut p);
    assert_eq!(out.len(), 2, "{out:#?}");
    assert_eq!(out[0]["paid"], 1.0);
    assert!(out[0].get("order").is_none_or(serde_json::Value::is_null));
    assert_eq!(out[1]["order"], "o1");
    assert!(out[1].get("paid").is_none_or(serde_json::Value::is_null));
}

#[test]
fn a_join_survives_a_snapshot() {
    let source = joined("join");
    let mut p = Program::compile(&source).unwrap();
    assert!(feed(&mut p, "OrderEvent", &order("10:00:00", "o1", 10.0)).is_empty());
    let bytes = p.snapshot().unwrap();
    let mut p = Program::compile(&source).unwrap();
    p.restore(&bytes).unwrap();
    let out = only(feed(
        &mut p,
        "PaymentEvent",
        &payment("10:00:10", "o1", 10.0),
    ));
    assert_eq!(out["order"], "o1");
    assert_eq!(out["paid"], 10.0);
}

/// Whether an event matched is in the snapshot: restored, a matched order
/// is not emitted alone when its window closes, an unmatched one is.
#[test]
fn an_outer_join_remembers_its_matches_across_a_snapshot() {
    let source = joined("left_join");
    let mut p = Program::compile(&source).unwrap();
    assert!(feed(&mut p, "OrderEvent", &order("10:00:00", "o1", 10.0)).is_empty());
    assert!(feed(&mut p, "OrderEvent", &order("10:00:01", "o2", 20.0)).is_empty());
    assert_eq!(
        only(feed(
            &mut p,
            "PaymentEvent",
            &payment("10:00:10", "o1", 10.0)
        ))["order"],
        "o1"
    );
    let bytes = p.snapshot().unwrap();
    let mut p = Program::compile(&source).unwrap();
    p.restore(&bytes).unwrap();
    let out = end(&mut p);
    assert_eq!(out.len(), 1, "o1 was paid before the snapshot: {out:#?}");
    assert_eq!(out[0]["order"], "o2");
}

#[test]
fn a_joined_stream_is_aggregated_per_partition_by_a_second_stream() {
    // The tutorial's "Join with Aggregation", as it now reads: the join's
    // window bounds the matching, and a second stream aggregates the pairs
    // per customer under a window of its own.
    let program = r"
event OrderEvent:
    customer_id: str
    amount: float
event CustomerEvent:
    id: str
    name: str
stream CustomerOrders = join(
    stream Orders = OrderEvent,
    stream Customers = CustomerEvent.on(Orders.customer_id == Customers.id)
)
.window(1h)
.select(customer: Customers.name, amount: Orders.amount)

stream CustomerStats = CustomerOrders
    .partition_by(customer)
    .window(1h)
    .aggregate(order_count: count(), total_spent: sum(amount), avg_order: avg(amount))
";
    let mut p = Program::compile(program).unwrap_or_else(|e| panic!("{e}"));
    let cust =
        |time: &str, id: &str, name: &str| at(time, &format!(r#""id": "{id}", "name": "{name}""#));
    let ord = |time: &str, id: &str, amount: f64| {
        at(
            time,
            &format!(r#""customer_id": "{id}", "amount": {amount:?}"#),
        )
    };
    assert!(feed(&mut p, "CustomerEvent", &cust("10:00:00", "c1", "Ann")).is_empty());
    assert!(feed(&mut p, "CustomerEvent", &cust("10:00:01", "c2", "Bob")).is_empty());
    assert!(feed(&mut p, "OrderEvent", &ord("10:00:10", "c1", 10.0)).is_empty());
    assert!(feed(&mut p, "OrderEvent", &ord("10:00:20", "c1", 30.0)).is_empty());
    assert!(feed(&mut p, "OrderEvent", &ord("10:00:30", "c2", 5.0)).is_empty());

    let mut out = end(&mut p);
    out.sort_by_key(|e| e["_partition"].as_str().unwrap_or_default().to_string());
    assert_eq!(out.len(), 2, "{out:#?}");
    assert_eq!(out[0]["_partition"], "Ann");
    assert_eq!(out[0]["order_count"], 2);
    assert_eq!(out[0]["total_spent"], 40.0);
    assert_eq!(out[0]["avg_order"], 20.0);
    assert_eq!(out[1]["_partition"], "Bob");
    assert_eq!(out[1]["order_count"], 1);
    assert_eq!(out[1]["total_spent"], 5.0);
    assert_eq!(out[1]["avg_order"], 5.0);
}
