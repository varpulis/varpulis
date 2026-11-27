//! IDX-02: steps whose comparison reaches the engine as an expression
//! (`a.key == key`, `price > a.lo * 1.5`) are filed by the predicate index
//! too, and the index must never change what they match, nor the order.
//!
//! The expressions are evaluated by the runtime's evaluator, as in a
//! compiled program; each pattern runs with the index always on, never on,
//! and in its automatic default, over the same random stream.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use varpulis_core::ast::{BinOp, Expr};
use varpulis_core::{Event, Value};
use varpulis_runtime::engine::evaluator::RuntimeExprEvaluator;
use varpulis_runtime::sase::{
    BackpressureStrategy, MatchResult, Predicate, SaseEngine, SasePattern, SelectionStrategy,
};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn field(name: &str) -> Expr {
    Expr::Ident(name.into())
}

fn member(alias: &str, name: &str) -> Expr {
    Expr::Member {
        expr: Box::new(Expr::Ident(alias.into())),
        member: name.into(),
    }
}

fn bin(op: BinOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn expr(e: Expr) -> Predicate {
    Predicate::Expr(Box::new(e))
}

fn and(left: Predicate, right: Predicate) -> Predicate {
    Predicate::And(Box::new(left), Box::new(right))
}

fn seq_ab(predicate: Predicate) -> SasePattern {
    SasePattern::Seq(vec![
        SasePattern::Event {
            event_type: "A".into(),
            predicate: None,
            alias: Some("a".into()),
        },
        SasePattern::Event {
            event_type: "B".into(),
            predicate: Some(predicate),
            alias: Some("b".into()),
        },
    ])
}

fn patterns() -> Vec<(&'static str, SasePattern)> {
    vec![
        // The captured side first.
        (
            "swapped-eq",
            seq_ab(expr(bin(BinOp::Eq, member("a", "key"), field("key")))),
        ),
        (
            "scaled-lower",
            seq_ab(expr(bin(
                BinOp::Gt,
                field("price"),
                bin(BinOp::Mul, member("a", "lo"), Expr::Float(1.5)),
            ))),
        ),
        (
            "literal-first",
            seq_ab(expr(bin(
                BinOp::Ge,
                field("price"),
                bin(BinOp::Mul, Expr::Int(2), member("a", "lo")),
            ))),
        ),
        (
            "swapped-upper",
            seq_ab(expr(bin(
                BinOp::Ge,
                bin(BinOp::Sub, member("a", "hi"), Expr::Int(1)),
                field("price"),
            ))),
        ),
        (
            // `a.lo` is an integer half the time: integer division truncates.
            "int-division",
            seq_ab(expr(bin(
                BinOp::Lt,
                field("price"),
                bin(BinOp::Div, member("a", "lo"), Expr::Int(2)),
            ))),
        ),
        (
            "shifted-interval",
            seq_ab(and(
                expr(bin(
                    BinOp::Ge,
                    field("price"),
                    bin(BinOp::Add, member("a", "lo"), Expr::Float(0.5)),
                )),
                expr(bin(
                    BinOp::Lt,
                    field("price"),
                    bin(BinOp::Mul, member("a", "hi"), Expr::Float(1.1)),
                )),
            )),
        ),
        (
            "eq-and-expr-interval",
            seq_ab(and(
                expr(bin(BinOp::Eq, member("a", "key"), field("key"))),
                and(
                    expr(bin(BinOp::Le, member("a", "lo"), field("price"))),
                    expr(bin(BinOp::Gt, member("a", "hi"), field("price"))),
                ),
            )),
        ),
        (
            // Strings never order in an expression: the index may file the
            // runs, the predicate refuses them all the same.
            "string-order",
            seq_ab(and(
                expr(bin(BinOp::Eq, member("a", "key"), field("key"))),
                expr(bin(BinOp::Ge, field("name"), member("a", "name"))),
            )),
        ),
    ]
}

fn random_event(rng: &mut Rng, id: i64, at: DateTime<Utc>) -> Event {
    let event_type = ["A", "A", "B", "B", "B"][rng.below(5) as usize];
    let mut event = Event::new(event_type).with_field("id", Value::Int(id));
    event.timestamp = at;

    let k = rng.below(4) as i64;
    event = match rng.below(10) {
        0 => event.with_field("key", Value::Float(k as f64)),
        1 => event.with_field("key", Value::Str(format!("k{k}").into())),
        2 => event.with_field("key", Value::Null),
        3 => event,
        _ => event.with_field("key", Value::Int(k)),
    };

    let price = rng.below(60) as f64 / 2.0;
    event = match rng.below(5) {
        0 => event.with_field("price", Value::Int(price as i64)),
        1 => event.with_field("price", Value::Str("x".into())),
        _ => event.with_field("price", Value::Float(price)),
    };

    let lo = rng.below(20) as i64;
    let hi = lo + rng.below(6) as i64 - 1;
    event = if rng.below(2) == 0 {
        event
            .with_field("lo", Value::Int(lo))
            .with_field("hi", Value::Int(hi))
    } else {
        event
            .with_field("lo", Value::Float(lo as f64))
            .with_field("hi", Value::Float(hi as f64))
    };
    let name = ["a", "b", "c"][rng.below(3) as usize];
    event.with_field("name", Value::Str(name.into()))
}

fn id_of(event: &Event) -> String {
    match event.get("id") {
        Some(Value::Int(id)) => id.to_string(),
        other => format!("{other:?}"),
    }
}

fn describe(m: &MatchResult) -> String {
    let mut captured: Vec<String> = m
        .captured
        .iter()
        .map(|(alias, event)| format!("{alias}={}", id_of(event)))
        .collect();
    captured.sort();
    captured.join(",")
}

fn engine(pattern: &SasePattern, strategy: SelectionStrategy, index: Option<bool>) -> SaseEngine {
    let mut engine = SaseEngine::new(pattern.clone())
        .with_strategy(strategy)
        .with_max_runs(1_000_000)
        .with_backpressure(BackpressureStrategy::Drop);
    if let Some(on) = index {
        engine = engine.with_predicate_index(on);
    }
    engine.set_evaluator(Arc::new(RuntimeExprEvaluator));
    engine
}

#[test]
fn expression_steps_match_the_same_with_and_without_the_index() {
    for (name, pattern) in patterns() {
        for strategy in [
            SelectionStrategy::SkipTillAnyMatch,
            SelectionStrategy::SkipTillNextMatch,
        ] {
            let mut most_parked = 0;
            let mut matches = 0;
            for seed in 1..=6u64 {
                let mut plain = engine(&pattern, strategy, Some(false));
                let mut indexed = engine(&pattern, strategy, Some(true));
                let mut auto = engine(&pattern, strategy, None);
                let mut rng = Rng(seed * 0x9E37_79B9);
                let mut at = DateTime::from_timestamp(1_700_000_000, 0).expect("valid");
                for id in 0..1_200 {
                    at += chrono::Duration::milliseconds(rng.below(1000) as i64);
                    let event = random_event(&mut rng, id, at);
                    let expected: Vec<String> =
                        plain.process(&event).iter().map(describe).collect();
                    let actual: Vec<String> =
                        indexed.process(&event).iter().map(describe).collect();
                    let automatic: Vec<String> =
                        auto.process(&event).iter().map(describe).collect();
                    assert_eq!(
                        actual, expected,
                        "{name} ({strategy:?}, seed {seed}): differ at event {id} {event:?}"
                    );
                    assert_eq!(automatic, expected, "{name}: automatic mode differs");
                    assert_eq!(
                        indexed.stats().active_runs,
                        plain.stats().active_runs,
                        "{name}: active runs differ after event {id}"
                    );
                    matches += expected.len();
                    most_parked = most_parked.max(indexed.parked_run_count());
                }
            }
            if name == "string-order" {
                assert_eq!(matches, 0, "an expression never orders strings");
            } else {
                assert!(matches > 0, "{name}: never matched, the test is vacuous");
            }
            assert!(
                most_parked > 0,
                "{name} ({strategy:?}): the index never filed a run"
            );
        }
    }
}
