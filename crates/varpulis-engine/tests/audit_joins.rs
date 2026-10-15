//! A join runs in the engine as it ships.
//!
//! Joins parsed and checked, and the synchronous engine refused them when
//! the program loaded; the joins reference told the reader to write a
//! sequence instead.

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

#[test]
fn a_left_join_emits_the_left_event_at_once_and_the_pair_when_it_comes() {
    let mut p = Program::compile(&joined("left_join")).unwrap();
    let out = only(feed(&mut p, "OrderEvent", &order("10:00:00", "o1", 10.0)));
    assert_eq!(out["order"], "o1");
    assert!(
        out.get("paid").is_none_or(serde_json::Value::is_null),
        "no payment yet: {out}"
    );
    let out = only(feed(
        &mut p,
        "PaymentEvent",
        &payment("10:00:10", "o1", 10.0),
    ));
    assert_eq!(out["order"], "o1");
    assert_eq!(out["paid"], 10.0);
    assert!(
        feed(&mut p, "PaymentEvent", &payment("10:00:20", "o9", 1.0)).is_empty(),
        "a right event alone emits nothing from a left join"
    );
}

#[test]
fn a_full_join_emits_either_side_alone() {
    let mut p = Program::compile(&joined("full_join")).unwrap();
    let out = only(feed(
        &mut p,
        "PaymentEvent",
        &payment("10:00:00", "o9", 1.0),
    ));
    assert_eq!(out["paid"], 1.0);
    assert!(
        out.get("order").is_none_or(serde_json::Value::is_null),
        "{out}"
    );
    let out = only(feed(&mut p, "OrderEvent", &order("10:00:01", "o1", 10.0)));
    assert_eq!(out["order"], "o1");
    assert!(
        out.get("paid").is_none_or(serde_json::Value::is_null),
        "{out}"
    );
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
