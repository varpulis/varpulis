//! `stream S = Order as o` binds `o` for the stream's own operators.
//!
//! The alias was only resolved inside sequence patterns. On a plain stream
//! `o.total` reached into nothing: a `.where(o.total > 100.0)` dropped every
//! event and `.emit(id: o.id)` emitted no `id`, for a form the type system
//! documentation publishes.

mod common;

use common::{at, feed, only};
use varpulis_engine::Program;

const ORDERS: &str = r"
type Address:
    country: str
event Order:
    id: str
    total: float
    region: str
    shipping: Address
";

fn order(id: &str, total: f64, country: &str) -> String {
    at(
        "10:00:00",
        &format!(
            r#""id": "{id}", "total": {total:?}, "region": "{country}", "shipping": {{"country": "{country}"}}"#
        ),
    )
}

#[test]
fn the_source_alias_resolves_in_where_and_emit() {
    let program = format!(
        r"{ORDERS}
stream Big = Order as o
    .where(o.total > 100.0)
    .emit(id: o.id, total: o.total, country: o.shipping.country)
"
    );
    let mut p = Program::compile(&program).unwrap();
    let out = only(feed(&mut p, "Order", &order("o1", 500.0, "FR")));
    assert_eq!(out["id"], "o1");
    assert_eq!(out["total"], 500.0);
    assert_eq!(out["country"], "FR");
    assert!(
        feed(&mut p, "Order", &order("o2", 5.0, "DE")).is_empty(),
        "the filter on the aliased field must still filter"
    );
}

#[test]
fn the_source_alias_resolves_after_a_source_filter() {
    let program = format!(
        r"{ORDERS}
stream Big = Order where total > 100.0 as o
    .emit(id: o.id)
"
    );
    let mut p = Program::compile(&program).unwrap();
    assert_eq!(
        only(feed(&mut p, "Order", &order("o1", 500.0, "FR")))["id"],
        "o1"
    );
    assert!(feed(&mut p, "Order", &order("o2", 5.0, "FR")).is_empty());
}

#[test]
fn the_source_alias_keys_a_partition_and_an_aggregate() {
    let program = format!(
        r"{ORDERS}
stream PerCountry = Order as o
    .partition_by(o.region)
    .window(2)
    .aggregate(country: last(o.region), total: sum(o.total))
    .emit(country: country, total: total)
"
    );
    let mut p = Program::compile(&program).unwrap();
    assert!(feed(&mut p, "Order", &order("o1", 1.0, "FR")).is_empty());
    assert!(feed(&mut p, "Order", &order("o2", 10.0, "DE")).is_empty());
    let out = only(feed(&mut p, "Order", &order("o3", 2.0, "FR")));
    assert_eq!(out["country"], "FR");
    assert_eq!(out["total"], 3.0);
}
