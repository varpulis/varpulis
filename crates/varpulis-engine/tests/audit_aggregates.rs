//! Aggregates the reference lists, and what an aggregate the engine does
//! not know does to a program.
//!
//! `collect(field)` has a section of its own in the windows reference and
//! did not exist. An aggregate function the engine did not know — a typo, or
//! `collect` — was dropped from the result in silence, so the field was
//! simply missing from every emit. `percentile(x, 95)` took 95 for a
//! quantile outside 0..1 and quietly computed the median.

mod common;

use common::{at, end, feed, only};
use varpulis_engine::{Error, Program};

const ORDERS: &str = r"
event Order:
    id: str
    region: str
    price: float
";

fn order(second: u32, id: &str, region: &str, price: f64) -> String {
    at(
        &format!("10:00:{second:02}"),
        &format!(r#""id": "{id}", "region": "{region}", "price": {price:?}"#),
    )
}

#[test]
fn collect_gathers_the_windows_values_in_order() {
    let program = format!(
        r"{ORDERS}
stream S = Order
    .window(1m)
    .aggregate(ids: collect(id), n: count())
    .emit(ids: ids, n: n)
"
    );
    let mut p = Program::compile(&program).unwrap();
    assert!(feed(&mut p, "Order", &order(0, "a", "eu", 1.0)).is_empty());
    assert!(feed(&mut p, "Order", &order(10, "b", "eu", 2.0)).is_empty());
    let out = only(end(&mut p));
    assert_eq!(out["ids"], serde_json::json!(["a", "b"]));
    assert_eq!(out["n"], 2);
}

#[test]
fn collect_is_per_partition_and_skips_a_missing_field() {
    let program = format!(
        r"{ORDERS}
stream S = Order
    .partition_by(region)
    .window(1m)
    .aggregate(region: last(region), ids: collect(id), prices: collect(price))
    .emit(region: region, ids: ids, prices: prices)
"
    );
    let mut p = Program::compile(&program).unwrap();
    assert!(feed(&mut p, "Order", &order(0, "a", "eu", 1.0)).is_empty());
    assert!(feed(&mut p, "Order", &order(1, "b", "us", 2.0)).is_empty());
    assert!(feed(
        &mut p,
        "Order",
        &at("10:00:02", r#""id": "c", "region": "eu""#)
    )
    .is_empty());
    let mut out = end(&mut p);
    out.sort_by_key(|e| e["region"].as_str().unwrap().to_string());
    assert_eq!(out.len(), 2);
    assert_eq!(out[0]["region"], "eu");
    assert_eq!(out[0]["ids"], serde_json::json!(["a", "c"]));
    assert_eq!(
        out[0]["prices"],
        serde_json::json!([1.0]),
        "an event without the field contributes nothing to collect"
    );
    assert_eq!(out[1]["region"], "us");
    assert_eq!(out[1]["ids"], serde_json::json!(["b"]));
}

#[test]
fn an_unknown_aggregate_function_is_refused_not_dropped() {
    let program = format!(
        r"{ORDERS}
stream S = Order
    .window(1m)
    .aggregate(total: sumx(price), n: count())
    .emit(total: total, n: n)
"
    );
    let err = Program::compile(&program).unwrap_err();
    assert!(
        err.to_string().contains("sumx"),
        "the error must name the function: {err}"
    );
}

#[test]
fn a_percentile_quantile_outside_zero_to_one_is_refused() {
    for quantile in ["95", "1.5", "-0.1"] {
        let program = format!(
            r"{ORDERS}
stream S = Order
    .window(10)
    .aggregate(p: percentile(price, {quantile}))
    .emit(p: p)
"
        );
        let err = Program::compile(&program).unwrap_err();
        assert!(
            err.to_string().contains("percentile"),
            "percentile(price, {quantile}) must be refused, got: {err}"
        );
    }
}

#[test]
fn a_percentile_quantile_in_range_is_computed() {
    let program = format!(
        r"{ORDERS}
stream S = Order
    .window(4)
    .aggregate(p: percentile(price, 0.5), hi: percentile(price, 1.0))
    .emit(p: p, hi: hi)
"
    );
    let mut p = Program::compile(&program).unwrap();
    for (i, price) in [10.0, 20.0, 30.0].iter().enumerate() {
        assert!(feed(&mut p, "Order", &order(i as u32, "x", "eu", *price)).is_empty());
    }
    let out = only(feed(&mut p, "Order", &order(3, "x", "eu", 40.0)));
    assert_eq!(out["p"], 25.0);
    assert_eq!(out["hi"], 40.0);
}

#[test]
fn check_and_compile_agree_on_an_unknown_aggregate() {
    let program = format!(
        r"{ORDERS}
stream S = Order
    .window(1m)
    .aggregate(ids: collekt(id))
    .emit(ids: ids)
"
    );
    assert!(matches!(Program::check(&program), Err(Error::Invalid(_))));
    assert!(Program::compile(&program).is_err());
}
