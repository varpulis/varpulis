//! `and` / `or` in pattern declarations: precedence, grouping, and the form
//! a pattern without them keeps.

use varpulis_core::ast::{Expr, SasePatternExpr, SasePatternItem, Stmt};
use varpulis_parser::parse;

fn pattern_expr(src: &str) -> SasePatternExpr {
    let program = parse(src).unwrap_or_else(|e| panic!("{src}: {e}"));
    match program.statements.into_iter().next().map(|s| s.node) {
        Some(Stmt::PatternDecl { expr, .. }) => expr,
        other => panic!("expected a pattern declaration, got {other:?}"),
    }
}

/// The expression as a compact string: `A`, `[A, B]` for a sequence,
/// `(A & B)`, `(A | B)`, `A -> (..)` for a sequence with combined steps.
fn shape(expr: &SasePatternExpr) -> String {
    fn item(i: &SasePatternItem) -> String {
        let mut s = i.event_type.clone();
        if let Some(a) = &i.alias {
            s.push_str(" as ");
            s.push_str(a);
        }
        s
    }
    match expr {
        SasePatternExpr::Seq(items) if items.len() == 1 => item(&items[0]),
        SasePatternExpr::Seq(items) => {
            format!(
                "[{}]",
                items.iter().map(item).collect::<Vec<_>>().join(", ")
            )
        }
        SasePatternExpr::FollowedBy(parts) => {
            parts.iter().map(shape).collect::<Vec<_>>().join(" -> ")
        }
        SasePatternExpr::And(parts) => {
            format!(
                "({})",
                parts.iter().map(shape).collect::<Vec<_>>().join(" & ")
            )
        }
        SasePatternExpr::Or(parts) => {
            format!(
                "({})",
                parts.iter().map(shape).collect::<Vec<_>>().join(" | ")
            )
        }
    }
}

#[test]
fn a_pattern_without_and_or_is_one_sequence() {
    let expr = pattern_expr("pattern P = A as a -> B where x > 1 as b -> C");
    assert_eq!(shape(&expr), "[A as a, B as b, C]");
    assert!(matches!(expr, SasePatternExpr::Seq(_)));
}

#[test]
fn and_both_events() {
    assert_eq!(
        shape(&pattern_expr("pattern P = A as a and B as b within 5m")),
        "(A as a & B as b)"
    );
}

#[test]
fn or_either_event() {
    assert_eq!(shape(&pattern_expr("pattern P = A or B")), "(A | B)");
}

#[test]
fn and_binds_tighter_than_or() {
    assert_eq!(
        shape(&pattern_expr("pattern P = A and B or C")),
        "((A & B) | C)"
    );
    assert_eq!(
        shape(&pattern_expr("pattern P = A or B and C")),
        "(A | (B & C))"
    );
}

#[test]
fn both_bind_tighter_than_followed_by() {
    assert_eq!(
        shape(&pattern_expr("pattern P = X -> A or B and C -> Y")),
        "X -> (A | (B & C)) -> Y"
    );
}

#[test]
fn parentheses_group() {
    assert_eq!(
        shape(&pattern_expr("pattern P = (A or B) and C")),
        "((A | B) & C)"
    );
    assert_eq!(
        shape(&pattern_expr("pattern P = X -> ((A -> B) or C) -> Y")),
        "X -> ([A, B] | C) -> Y"
    );
    // Without the inner parentheses `or` binds tighter than `->`.
    assert_eq!(
        shape(&pattern_expr("pattern P = X -> (A -> B or C) -> Y")),
        "[X, A] -> (B | C) -> Y"
    );
}

#[test]
fn chains_flatten() {
    assert_eq!(
        shape(&pattern_expr("pattern P = A and B and C")),
        "(A & B & C)"
    );
    assert_eq!(
        shape(&pattern_expr("pattern P = (A and B) and C")),
        "(A & B & C)"
    );
    assert_eq!(
        shape(&pattern_expr("pattern P = A or (B or C)")),
        "(A | B | C)"
    );
    // A parenthesized sequence inside a sequence is the same sequence.
    assert_eq!(
        shape(&pattern_expr("pattern P = A -> (B -> C) -> D")),
        "[A, B, C, D]"
    );
}

#[test]
fn plain_steps_next_to_a_combined_one_stay_one_sequence() {
    let expr = pattern_expr("pattern P = A -> B -> (C or D) -> E -> F");
    assert_eq!(shape(&expr), "[A, B] -> (C | D) -> [E, F]");
}

#[test]
fn a_filtered_step_is_joined_after_its_alias_or_in_parentheses() {
    // The alias ends the filter.
    let expr = pattern_expr("pattern P = A where x > 1 as a and B");
    assert_eq!(shape(&expr), "(A as a & B)");
    let expr = pattern_expr("pattern P = (A where x > 1) and B");
    assert_eq!(shape(&expr), "(A & B)");
}

#[test]
fn without_either_the_filter_takes_the_and() {
    // `x > 1 and B` is the filter: B is read as a value. The validator flags
    // this (W038) when B is a declared event type.
    let expr = pattern_expr("pattern P = A where x > 1 and B");
    let SasePatternExpr::Seq(items) = &expr else {
        panic!("expected one item, got {expr:?}");
    };
    assert_eq!(items.len(), 1);
    assert!(matches!(items[0].filter, Some(Expr::Binary { .. })));
}

#[test]
fn keywords_need_a_word_boundary() {
    // `Order` and `Android` are event types, not `or` / `and`.
    assert_eq!(
        shape(&pattern_expr("pattern P = Order -> Android")),
        "[Order, Android]"
    );
    assert_eq!(
        shape(&pattern_expr("pattern P = Order or Android")),
        "(Order | Android)"
    );
}

#[test]
fn not_and_all_still_parse_as_steps() {
    assert_eq!(
        shape(&pattern_expr("pattern P = (A and B) -> NOT C within 5m")),
        "(A & B) -> !C"
    );
    assert_eq!(
        shape(&pattern_expr("pattern P = all A as xs -> B or C")),
        "A as xs -> (B | C)"
    );
}

#[test]
fn a_dangling_operator_is_a_parse_error() {
    assert!(parse("pattern P = A and").is_err());
    assert!(parse("pattern P = or B").is_err());
    assert!(parse("pattern P = (A or B").is_err());
}
