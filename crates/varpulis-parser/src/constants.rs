//! Top-level constants, visible to the expressions streams evaluate.
//!
//! `let threshold = 100` then `.where(x > threshold)` used to read a field
//! named `threshold` from the event: the engine evaluated stream expressions
//! with no program variables in scope, and `const` values were not loaded at
//! all. So the rule compared `x` with nothing (no alert), or with whatever an
//! event carried under that name. A detection written with named thresholds
//! checked "ok" and never did what it said.
//!
//! A top-level `let` or `const` whose value folds to a literal now replaces
//! the name wherever a stream evaluates an expression. Constants are gathered
//! first, in order (one may use an earlier one), so a stream can use a
//! constant declared below it. The name is left alone where something closer
//! binds it: a sequence alias, a lambda parameter, a block's own `let`, and
//! the function position of a call. A `var` is never substituted, since its
//! value can change.

use std::collections::{HashMap, HashSet};

use varpulis_core::ast::*;
use varpulis_core::span::Spanned;

/// Substitute top-level constants into stream expressions.
pub(crate) fn propagate(program: Program) -> Program {
    let known = gather(&program.statements);
    if known.is_empty() {
        return program;
    }
    Program {
        statements: program
            .statements
            .into_iter()
            .map(|s| Spanned::new(stmt(s.node, &known), s.span))
            .collect(),
    }
}

/// The top-level `let` and `const` values that are literals once the earlier
/// constants are in. A name declared `var` anywhere is left out.
fn gather(statements: &[Spanned<Stmt>]) -> HashMap<String, Expr> {
    let mutable: HashSet<&str> = statements
        .iter()
        .filter_map(|s| match &s.node {
            Stmt::VarDecl {
                mutable: true,
                name,
                ..
            } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    let mut known = HashMap::new();
    for s in statements {
        let (name, value) = match &s.node {
            Stmt::VarDecl {
                mutable: false,
                name,
                value,
                ..
            }
            | Stmt::ConstDecl { name, value, .. } => (name, value),
            _ => continue,
        };
        if mutable.contains(name.as_str()) {
            continue;
        }
        let value = crate::optimize::fold_expr(expr(value.clone(), &known, &HashSet::new()));
        if is_literal(&value) {
            known.insert(name.clone(), value);
        } else {
            known.remove(name);
        }
    }
    known
}

fn is_literal(e: &Expr) -> bool {
    match e {
        Expr::Null
        | Expr::Bool(_)
        | Expr::Int(_)
        | Expr::Float(_)
        | Expr::Str(_)
        | Expr::Duration(_)
        | Expr::Timestamp(_) => true,
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => matches!(**expr, Expr::Int(_) | Expr::Float(_)),
        Expr::Array(items) => items.iter().all(is_literal),
        Expr::Map(entries) => entries.iter().all(|(_, v)| is_literal(v)),
        _ => false,
    }
}

fn stmt(node: Stmt, known: &HashMap<String, Expr>) -> Stmt {
    match node {
        Stmt::StreamDecl {
            name,
            type_annotation,
            source,
            ops,
            op_spans,
        } => {
            let bound = bound_by_stream(&source, &ops);
            Stmt::StreamDecl {
                name,
                type_annotation,
                source: stream_source(source, known, &bound),
                ops: ops
                    .into_iter()
                    .map(|op| stream_op(op, known, &bound))
                    .collect(),
                op_spans,
            }
        }
        Stmt::PatternDecl {
            name,
            expr: SasePatternExpr::Seq(items),
            within,
            partition_by,
        } => {
            let bound: HashSet<String> = items
                .iter()
                .flat_map(|i| [Some(i.event_type.clone()), i.alias.clone()])
                .flatten()
                .collect();
            let items = items
                .into_iter()
                .map(|mut item| {
                    item.filter = item.filter.take().map(|f| expr(f, known, &bound));
                    item
                })
                .collect();
            Stmt::PatternDecl {
                name,
                expr: SasePatternExpr::Seq(items),
                within: within.map(|w| expr(w, known, &bound)),
                partition_by: partition_by.map(|p| expr(p, known, &bound)),
            }
        }
        other => other,
    }
}

/// Names a stream binds itself: its source and its aliases.
fn bound_by_stream(source: &StreamSource, ops: &[StreamOp]) -> HashSet<String> {
    let mut bound = HashSet::new();
    match source {
        StreamSource::Ident(name) => {
            bound.insert(name.clone());
        }
        StreamSource::IdentWithAlias { name, alias } => {
            bound.insert(name.clone());
            bound.insert(alias.clone());
        }
        StreamSource::IdentWithFilterAndAlias { name, alias, .. }
        | StreamSource::AllWithAlias { name, alias } => {
            bound.insert(name.clone());
            bound.extend(alias.clone());
        }
        StreamSource::Sequence(decl) => {
            for step in &decl.steps {
                bound.insert(step.alias.clone());
                bound.insert(step.event_type.clone());
            }
        }
        _ => {}
    }
    for op in ops {
        if let StreamOp::FollowedBy(clause) | StreamOp::Not(clause) = op {
            bound.insert(clause.event_type.clone());
            bound.extend(clause.alias.clone());
        }
    }
    bound
}

fn stream_source(
    source: StreamSource,
    known: &HashMap<String, Expr>,
    bound: &HashSet<String>,
) -> StreamSource {
    match source {
        StreamSource::IdentWithFilterAndAlias {
            name,
            filter,
            alias,
        } => StreamSource::IdentWithFilterAndAlias {
            name,
            filter: expr(filter, known, bound),
            alias,
        },
        StreamSource::Sequence(mut decl) => {
            for step in &mut decl.steps {
                step.filter = step.filter.take().map(|f| expr(f, known, bound));
            }
            StreamSource::Sequence(decl)
        }
        other => other,
    }
}

fn stream_op(op: StreamOp, known: &HashMap<String, Expr>, bound: &HashSet<String>) -> StreamOp {
    let e = |x: Expr| expr(x, known, bound);
    let args = |a: Vec<NamedArg>| {
        a.into_iter()
            .map(|arg| NamedArg {
                name: arg.name,
                value: expr(arg.value, known, bound),
            })
            .collect()
    };
    match op {
        StreamOp::Where(x) => StreamOp::Where(e(x)),
        StreamOp::Filter(x) => StreamOp::Filter(e(x)),
        StreamOp::Having(x) => StreamOp::Having(e(x)),
        StreamOp::PartitionBy(x) => StreamOp::PartitionBy(e(x)),
        StreamOp::Within(x) => StreamOp::Within(e(x)),
        StreamOp::Limit(x) => StreamOp::Limit(e(x)),
        StreamOp::AllowedLateness(x) => StreamOp::AllowedLateness(e(x)),
        StreamOp::Emit {
            output_type,
            fields,
            target_context,
        } => StreamOp::Emit {
            output_type,
            fields: args(fields),
            target_context,
        },
        StreamOp::Tap(a) => StreamOp::Tap(args(a)),
        StreamOp::Log(a) => StreamOp::Log(args(a)),
        StreamOp::Print(xs) => StreamOp::Print(xs.into_iter().map(e).collect()),
        StreamOp::Select(items) => StreamOp::Select(
            items
                .into_iter()
                .map(|item| match item {
                    SelectItem::Alias(name, x) => SelectItem::Alias(name, e(x)),
                    field => field,
                })
                .collect(),
        ),
        StreamOp::Aggregate(items) => StreamOp::Aggregate(
            items
                .into_iter()
                .map(|item| AggItem {
                    alias: item.alias,
                    expr: e(item.expr),
                })
                .collect(),
        ),
        StreamOp::Window(w) => StreamOp::Window(WindowArgs {
            duration: e(w.duration),
            sliding: w.sliding.map(e),
            policy: w.policy,
            session_gap: w.session_gap.map(e),
        }),
        StreamOp::FollowedBy(mut clause) => {
            clause.filter = clause.filter.take().map(e);
            StreamOp::FollowedBy(clause)
        }
        StreamOp::Not(mut clause) => {
            clause.filter = clause.filter.take().map(e);
            StreamOp::Not(clause)
        }
        other => other,
    }
}

/// Substitute the constants in `known` into `e`, except the names in `bound`.
fn expr(e: Expr, known: &HashMap<String, Expr>, bound: &HashSet<String>) -> Expr {
    let go = |x: Expr| expr(x, known, bound);
    let boxed = |x: Box<Expr>| Box::new(expr(*x, known, bound));
    match e {
        Expr::Ident(name) => match known.get(&name) {
            Some(value) if !bound.contains(&name) => value.clone(),
            _ => Expr::Ident(name),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: boxed(left),
            right: boxed(right),
        },
        Expr::Unary { op, expr: inner } => Expr::Unary {
            op,
            expr: boxed(inner),
        },
        // The function position names a function, not a value: `lower(x)`
        // stays a call even if the program also has a constant `lower`.
        Expr::Call { func, args } => Expr::Call {
            func: match *func {
                Expr::Ident(name) => Box::new(Expr::Ident(name)),
                other => Box::new(go(other)),
            },
            args: args
                .into_iter()
                .map(|a| match a {
                    Arg::Positional(x) => Arg::Positional(go(x)),
                    Arg::Named(n, x) => Arg::Named(n, go(x)),
                })
                .collect(),
        },
        Expr::Member {
            expr: object,
            member,
        } => Expr::Member {
            expr: boxed(object),
            member,
        },
        Expr::OptionalMember {
            expr: object,
            member,
        } => Expr::OptionalMember {
            expr: boxed(object),
            member,
        },
        Expr::Index {
            expr: object,
            index,
        } => Expr::Index {
            expr: boxed(object),
            index: boxed(index),
        },
        Expr::Slice {
            expr: object,
            start,
            end,
        } => Expr::Slice {
            expr: boxed(object),
            start: start.map(boxed),
            end: end.map(boxed),
        },
        Expr::Array(items) => Expr::Array(items.into_iter().map(go).collect()),
        Expr::Map(entries) => Expr::Map(entries.into_iter().map(|(k, v)| (k, go(v))).collect()),
        Expr::If {
            cond,
            then_branch,
            else_branch,
        } => Expr::If {
            cond: boxed(cond),
            then_branch: boxed(then_branch),
            else_branch: boxed(else_branch),
        },
        Expr::Coalesce { expr: x, default } => Expr::Coalesce {
            expr: boxed(x),
            default: boxed(default),
        },
        Expr::Range {
            start,
            end,
            inclusive,
        } => Expr::Range {
            start: boxed(start),
            end: boxed(end),
            inclusive,
        },
        // A lambda's parameters shadow constants of the same name in its body.
        Expr::Lambda { params, body } => {
            let mut inner = bound.clone();
            inner.extend(params.iter().cloned());
            let body = Box::new(expr(*body, known, &inner));
            Expr::Lambda { params, body }
        }
        // So does a block's own `let`, from where it is bound on.
        Expr::Block { stmts, result } => {
            let mut inner = bound.clone();
            let stmts = stmts
                .into_iter()
                .map(|(name, ty, value, mutable)| {
                    let value = expr(value, known, &inner);
                    inner.insert(name.clone());
                    (name, ty, value, mutable)
                })
                .collect();
            let result = Box::new(expr(*result, known, &inner));
            Expr::Block { stmts, result }
        }
        literal => literal,
    }
}
