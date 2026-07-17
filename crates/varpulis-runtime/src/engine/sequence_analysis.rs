//! Compile-time analysis of which Kleene aliases a sequence pipeline references.
//!
//! When a Kleene/sequence match completes, `pipeline.rs` can build several
//! per-alias structures for the emitted `SequenceMatch` event:
//!
//! - `_events_{alias}`: a `Value::Array` of `Value::Map`, one deep-cloned map
//!   per captured event. This is O(events) memory per match and, because a
//!   Kleene closure emits one incremental match per event, O(events²) over a
//!   run — the source of the OOM this module exists to avoid.
//! - `_agg_{sum,avg,min,max}_{alias}_{field}` and `_agg_distinct_{alias}_{field}`:
//!   per-field aggregates, another O(events) scan per match.
//!
//! The evaluator (`evaluator.rs`) only ever reads those structures through a
//! small, fixed set of syntactic forms:
//!
//! - the `_events_{alias}` array via positional index `alias[i]`, via
//!   `collect(alias.field)`, or via bare `collect(alias)`;
//! - the aggregates via `sum|avg|min|max|distinct_count(alias.field)`.
//!
//! So we can walk the downstream operators at compile time, record which
//! aliases appear in each form, and let the builder skip everything unused.
//!
//! **Correctness bias:** over-inclusion is safe (we build something nothing
//! reads); under-inclusion drops a field a pipeline reads and is a bug. The
//! walk is therefore exhaustive and conservative — the `match` arms carry no
//! `_` wildcard so that adding a new [`RuntimeOp`] or [`Expr`] variant is a
//! compile error here rather than a silent miss.

use varpulis_core::ast::{Arg, Expr};

use super::types::{RuntimeOp, SequenceMatchConfig, TopicSpec};

/// Analyze the downstream operators of a sequence stream and return which
/// aliases need the expensive per-alias structures built.
///
/// `ops` is the operator list *after* the `Sequence` op (at both call sites in
/// `compilation.rs` the analysis runs before `runtime_ops.insert(0, …)`, so the
/// slice already contains exactly the downstream ops).
pub(super) fn analyze_downstream_alias_needs(ops: &[RuntimeOp]) -> SequenceMatchConfig {
    let mut cfg = SequenceMatchConfig::default();
    for op in ops {
        visit_op(op, &mut cfg);
    }
    cfg
}

/// Extract the inner expression from a function argument (positional or named).
#[inline]
fn arg_expr(arg: &Arg) -> &Expr {
    match arg {
        Arg::Positional(e) | Arg::Named(_, e) => e,
    }
}

/// Walk every expression carried by a single operator.
///
/// Every [`RuntimeOp`] variant is listed explicitly (no `_` arm) so a new
/// variant forces a decision here instead of silently escaping the analysis.
fn visit_op(op: &RuntimeOp, cfg: &mut SequenceMatchConfig) {
    match op {
        // --- Operators carrying user expressions we must walk ------------------
        RuntimeOp::WhereExpr(expr) | RuntimeOp::Having(expr) | RuntimeOp::Process(expr) => {
            visit_expr(expr, cfg);
        }
        RuntimeOp::Select(config) => {
            for (_, expr) in &config.fields {
                visit_expr(expr, cfg);
            }
        }
        RuntimeOp::EmitExpr(config) => {
            for (_, expr) in &config.fields {
                visit_expr(expr, cfg);
            }
        }
        RuntimeOp::Print(config) => {
            for expr in &config.exprs {
                visit_expr(expr, cfg);
            }
        }
        RuntimeOp::Pattern(config) => visit_expr(&config.matcher, cfg),
        RuntimeOp::Enrich(config) => visit_expr(&config.key_expr, cfg),
        RuntimeOp::Distinct(state) => {
            if let Some(expr) = &state.expr {
                visit_expr(expr, cfg);
            }
        }
        RuntimeOp::To(config) => {
            if let Some(TopicSpec::Dynamic(expr)) = &config.topic {
                visit_expr(expr, cfg);
            }
        }

        // --- Operators with no downstream-readable expressions -----------------
        // `WhereClosure` is an opaque boxed `Fn` and cannot be introspected; a
        // sequence filter compiled to a closure never reads the `_events_`/`_agg_`
        // structures (it filters raw captured events), so skipping it is safe.
        // `Emit` only carries source-field names / literals, not expressions.
        RuntimeOp::WhereClosure(_)
        | RuntimeOp::Window(_)
        | RuntimeOp::PartitionedWindow(_)
        | RuntimeOp::PartitionedSlidingCountWindow(_)
        | RuntimeOp::Aggregate(_)
        | RuntimeOp::PartitionedAggregate(_)
        | RuntimeOp::Emit(_)
        | RuntimeOp::Log(_)
        | RuntimeOp::Sequence(_)
        | RuntimeOp::TrendAggregate(_)
        | RuntimeOp::Score(_)
        | RuntimeOp::Forecast(_)
        | RuntimeOp::Alert(_)
        | RuntimeOp::Limit(_) => {}

        #[cfg(feature = "arrow")]
        RuntimeOp::PartitionedWindowedColumnarAggregate(_)
        | RuntimeOp::WindowedColumnarAggregate(_) => {}

        #[cfg(feature = "async-runtime")]
        RuntimeOp::Concurrent(_) => {}
    }
}

/// Recursively walk an expression tree, recording alias references and
/// recursing into every sub-expression.
///
/// Every [`Expr`] variant is listed explicitly (no `_` arm) so a new variant
/// forces a decision here instead of silently escaping the analysis.
fn visit_expr(expr: &Expr, cfg: &mut SequenceMatchConfig) {
    match expr {
        // `alias[i]` → positional access, resolved against `_events_{alias}`.
        Expr::Index {
            expr: container,
            index,
        } => {
            if let Expr::Ident(name) = container.as_ref() {
                cfg.aliases_needing_events.insert(name.clone());
            }
            visit_expr(container, cfg);
            visit_expr(index, cfg);
        }

        // `collect(alias.field)` / `collect(alias)` read the `_events_{alias}`
        // array; `sum|avg|min|max|distinct_count(alias.field)` read the
        // `_agg_*` aggregates. Anything else recurses normally.
        Expr::Call { func, args } => {
            if let Expr::Ident(fname) = func.as_ref() {
                if let Some(first) = args.first() {
                    let arg = arg_expr(first);
                    match fname.as_str() {
                        "collect" => match arg {
                            Expr::Member { expr: obj, .. } => {
                                if let Expr::Ident(name) = obj.as_ref() {
                                    cfg.aliases_needing_events.insert(name.clone());
                                }
                            }
                            // `collect(alias)` without a member returns the whole
                            // array (see evaluator). Detect it too — over-include
                            // rather than under-include.
                            Expr::Ident(name) => {
                                cfg.aliases_needing_events.insert(name.clone());
                            }
                            _ => {}
                        },
                        "sum" | "avg" | "min" | "max" | "distinct_count" => {
                            if let Expr::Member { expr: obj, .. } = arg {
                                if let Expr::Ident(name) = obj.as_ref() {
                                    cfg.aliases_needing_aggs.insert(name.clone());
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            visit_expr(func, cfg);
            for a in args {
                visit_expr(arg_expr(a), cfg);
            }
        }

        // Leaves: nothing to recurse into. A bare `Ident(alias)` does NOT read
        // any `_events_`/`_agg_` structure (the evaluator resolves it from
        // bindings/event data), so it triggers no inclusion.
        Expr::Null
        | Expr::Bool(_)
        | Expr::Int(_)
        | Expr::Float(_)
        | Expr::Str(_)
        | Expr::Duration(_)
        | Expr::Timestamp(_)
        | Expr::Ident(_) => {}

        // Composite variants: recurse into every sub-expression.
        Expr::Array(items) => {
            for e in items {
                visit_expr(e, cfg);
            }
        }
        Expr::Map(entries) => {
            for (_, e) in entries {
                visit_expr(e, cfg);
            }
        }
        Expr::Binary { left, right, .. } => {
            visit_expr(left, cfg);
            visit_expr(right, cfg);
        }
        Expr::Unary { expr: inner, .. }
        | Expr::Member { expr: inner, .. }
        | Expr::OptionalMember { expr: inner, .. } => visit_expr(inner, cfg),
        Expr::Slice {
            expr: inner,
            start,
            end,
        } => {
            visit_expr(inner, cfg);
            if let Some(s) = start {
                visit_expr(s, cfg);
            }
            if let Some(e) = end {
                visit_expr(e, cfg);
            }
        }
        Expr::Lambda { body, .. } => visit_expr(body, cfg),
        Expr::If {
            cond,
            then_branch,
            else_branch,
        } => {
            visit_expr(cond, cfg);
            visit_expr(then_branch, cfg);
            visit_expr(else_branch, cfg);
        }
        Expr::Coalesce {
            expr: inner,
            default,
        } => {
            visit_expr(inner, cfg);
            visit_expr(default, cfg);
        }
        Expr::Range { start, end, .. } => {
            visit_expr(start, cfg);
            visit_expr(end, cfg);
        }
        Expr::Block { stmts, result } => {
            for (_, _, value, _) in stmts {
                visit_expr(value, cfg);
            }
            visit_expr(result, cfg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(name: &str) -> Expr {
        Expr::Ident(name.to_string())
    }

    fn member(obj: Expr, m: &str) -> Expr {
        Expr::Member {
            expr: Box::new(obj),
            member: m.to_string(),
        }
    }

    fn call(fname: &str, arg: Expr) -> Expr {
        Expr::Call {
            func: Box::new(ident(fname)),
            args: vec![Arg::Positional(arg)],
        }
    }

    fn analyze(expr: &Expr) -> SequenceMatchConfig {
        let mut cfg = SequenceMatchConfig::default();
        visit_expr(expr, &mut cfg);
        cfg
    }

    #[test]
    fn index_marks_events() {
        // tick[0]
        let e = Expr::Index {
            expr: Box::new(ident("tick")),
            index: Box::new(Expr::Int(0)),
        };
        let cfg = analyze(&e);
        assert!(cfg.aliases_needing_events.contains("tick"));
        assert!(cfg.aliases_needing_aggs.is_empty());
    }

    #[test]
    fn collect_member_marks_events() {
        // collect(tick.price)
        let cfg = analyze(&call("collect", member(ident("tick"), "price")));
        assert!(cfg.aliases_needing_events.contains("tick"));
    }

    #[test]
    fn collect_bare_marks_events() {
        // collect(tick)
        let cfg = analyze(&call("collect", ident("tick")));
        assert!(cfg.aliases_needing_events.contains("tick"));
    }

    #[test]
    fn aggs_mark_aggs_only() {
        for f in ["sum", "avg", "min", "max", "distinct_count"] {
            let cfg = analyze(&call(f, member(ident("tick"), "price")));
            assert!(cfg.aliases_needing_aggs.contains("tick"), "func {f}");
            assert!(
                cfg.aliases_needing_events.is_empty(),
                "func {f} should not need events"
            );
        }
    }

    #[test]
    fn count_first_last_len_need_nothing() {
        // count(tick), first(tick), last(tick) read always-built scalars.
        for f in ["count", "first", "last"] {
            let cfg = analyze(&call(f, ident("tick")));
            assert!(cfg.aliases_needing_events.is_empty(), "func {f}");
            assert!(cfg.aliases_needing_aggs.is_empty(), "func {f}");
        }
        // tick.LEN reads _count_{alias} (always built).
        let cfg = analyze(&member(ident("tick"), "LEN"));
        assert!(cfg.aliases_needing_events.is_empty());
        assert!(cfg.aliases_needing_aggs.is_empty());
    }

    #[test]
    fn bare_ident_needs_nothing() {
        let cfg = analyze(&ident("tick"));
        assert!(cfg.aliases_needing_events.is_empty());
        assert!(cfg.aliases_needing_aggs.is_empty());
    }

    #[test]
    fn nested_collect_inside_user_call_is_found() {
        // myfunc(collect(tick.price))
        let inner = call("collect", member(ident("tick"), "price"));
        let outer = Expr::Call {
            func: Box::new(ident("myfunc")),
            args: vec![Arg::Positional(inner)],
        };
        let cfg = analyze(&outer);
        assert!(cfg.aliases_needing_events.contains("tick"));
    }

    #[test]
    fn deep_nesting_binary_if_index() {
        // if cond then tick[i] else sum(other.v)
        let e = Expr::If {
            cond: Box::new(Expr::Bool(true)),
            then_branch: Box::new(Expr::Index {
                expr: Box::new(ident("tick")),
                index: Box::new(ident("i")),
            }),
            else_branch: Box::new(call("sum", member(ident("other"), "v"))),
        };
        let cfg = analyze(&e);
        assert!(cfg.aliases_needing_events.contains("tick"));
        assert!(cfg.aliases_needing_aggs.contains("other"));
    }
}
