//! The alias a plain stream gives its source: `stream S = Order as o`.
//!
//! Inside a sequence pattern an alias names a captured step, and the engine
//! resolves `o.total` against that step. On a stream with no sequence the
//! same alias resolved against nothing: `.where(o.total > 100.0)` dropped
//! every event and `.emit(id: o.id)` emitted no `id`, for the form the type
//! reference publishes (`stream HighValue = Order as o .where(o.total > ...)`).
//!
//! Here `o.field` becomes `field` wherever such a stream evaluates an
//! expression, so the alias reads the event itself: `o.shipping.country` is
//! `shipping.country`, `.partition_by(o.region)` is `.partition_by(region)`.
//! A lambda parameter or a block's own `let` of the same name shadows it.

use std::collections::HashSet;

use varpulis_core::ast::*;
use varpulis_core::span::Spanned;

/// Rewrite the source alias of every plain stream into the event's fields.
pub(crate) fn strip(program: Program) -> Program {
    Program {
        statements: program
            .statements
            .into_iter()
            .map(|s| Spanned::new(stmt(s.node), s.span))
            .collect(),
    }
}

fn stmt(node: Stmt) -> Stmt {
    let Stmt::StreamDecl {
        name,
        type_annotation,
        source,
        ops,
        op_spans,
    } = node
    else {
        return node;
    };
    // A stream with a sequence binds its aliases to captured steps, which the
    // engine resolves itself.
    let has_sequence = ops.iter().any(|op| {
        matches!(
            op,
            StreamOp::FollowedBy(_) | StreamOp::Not(_) | StreamOp::Within(_)
        )
    });
    let alias = match &source {
        StreamSource::IdentWithAlias { alias, .. } => Some(alias.clone()),
        StreamSource::IdentWithFilterAndAlias {
            alias: Some(alias), ..
        } => Some(alias.clone()),
        _ => None,
    };
    let (Some(alias), false) = (alias, has_sequence) else {
        return Stmt::StreamDecl {
            name,
            type_annotation,
            source,
            ops,
            op_spans,
        };
    };
    let bound = HashSet::new();
    let source = match source {
        StreamSource::IdentWithFilterAndAlias {
            name: source_name,
            filter,
            alias: source_alias,
        } => StreamSource::IdentWithFilterAndAlias {
            name: source_name,
            filter: expr(filter, &alias, &bound),
            alias: source_alias,
        },
        other => other,
    };
    Stmt::StreamDecl {
        name,
        type_annotation,
        source,
        ops: ops
            .into_iter()
            .map(|op| stream_op(op, &alias, &bound))
            .collect(),
        op_spans,
    }
}

fn stream_op(op: StreamOp, alias: &str, bound: &HashSet<String>) -> StreamOp {
    let e = |x: Expr| expr(x, alias, bound);
    let args = |a: Vec<NamedArg>| {
        a.into_iter()
            .map(|arg| NamedArg {
                name: arg.name,
                value: expr(arg.value, alias, bound),
            })
            .collect()
    };
    match op {
        StreamOp::Where(x) => StreamOp::Where(e(x)),
        StreamOp::Filter(x) => StreamOp::Filter(e(x)),
        StreamOp::Having(x) => StreamOp::Having(e(x)),
        StreamOp::PartitionBy(x) => StreamOp::PartitionBy(e(x)),
        StreamOp::Limit(x) => StreamOp::Limit(e(x)),
        StreamOp::Distinct(x) => StreamOp::Distinct(x.map(e)),
        StreamOp::Process(x) => StreamOp::Process(e(x)),
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
        StreamOp::Alert(a) => StreamOp::Alert(args(a)),
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
        other => other,
    }
}

/// `alias.member` becomes `member`, anywhere in `e`, unless something closer
/// binds `alias`.
fn expr(e: Expr, alias: &str, bound: &HashSet<String>) -> Expr {
    let go = |x: Expr| expr(x, alias, bound);
    let boxed = |x: Box<Expr>| Box::new(expr(*x, alias, bound));
    match e {
        Expr::Member {
            expr: object,
            member,
        } => match *object {
            Expr::Ident(name) if name == alias && !bound.contains(alias) => Expr::Ident(member),
            object => Expr::Member {
                expr: Box::new(go(object)),
                member,
            },
        },
        Expr::OptionalMember {
            expr: object,
            member,
        } => match *object {
            Expr::Ident(name) if name == alias && !bound.contains(alias) => Expr::Ident(member),
            object => Expr::OptionalMember {
                expr: Box::new(go(object)),
                member,
            },
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
        Expr::Call { func, args } => Expr::Call {
            func: boxed(func),
            args: args
                .into_iter()
                .map(|a| match a {
                    Arg::Positional(x) => Arg::Positional(go(x)),
                    Arg::Named(n, x) => Arg::Named(n, go(x)),
                })
                .collect(),
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
        Expr::Lambda { params, body } => {
            let mut inner = bound.clone();
            inner.extend(params.iter().cloned());
            let body = Box::new(expr(*body, alias, &inner));
            Expr::Lambda { params, body }
        }
        Expr::Block { stmts, result } => {
            let mut inner = bound.clone();
            let stmts = stmts
                .into_iter()
                .map(|(name, ty, value, mutable)| {
                    let value = expr(value, alias, &inner);
                    inner.insert(name.clone());
                    (name, ty, value, mutable)
                })
                .collect();
            let result = Box::new(expr(*result, alias, &inner));
            Expr::Block { stmts, result }
        }
        literal => literal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ops_of(src: &str) -> Vec<StreamOp> {
        let program = crate::parse(src).unwrap();
        program
            .statements
            .into_iter()
            .find_map(|s| match s.node {
                Stmt::StreamDecl { ops, .. } => Some(ops),
                _ => None,
            })
            .unwrap()
    }

    #[test]
    fn the_alias_of_a_plain_stream_reads_the_event() {
        let ops = ops_of(
            "stream S = Order as o .where(o.total > 1.0) .emit(id: o.id, c: o.shipping.country)",
        );
        match &ops[0] {
            StreamOp::Where(Expr::Binary { left, .. }) => {
                assert_eq!(**left, Expr::Ident("total".into()));
            }
            other => panic!("{other:?}"),
        }
        match &ops[1] {
            StreamOp::Emit { fields, .. } => {
                assert_eq!(fields[0].value, Expr::Ident("id".into()));
                assert_eq!(
                    fields[1].value,
                    Expr::Member {
                        expr: Box::new(Expr::Ident("shipping".into())),
                        member: "country".into()
                    }
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_sequence_keeps_its_aliases_for_the_engine() {
        let ops = ops_of("stream S = A as a -> B as b .within(1h) .emit(k: a.k)");
        let emit = ops
            .iter()
            .find_map(|op| match op {
                StreamOp::Emit { fields, .. } => Some(fields[0].value.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            emit,
            Expr::Member {
                expr: Box::new(Expr::Ident("a".into())),
                member: "k".into()
            }
        );
    }

    #[test]
    fn a_lambda_parameter_of_the_same_name_is_left_alone() {
        let ops = ops_of("stream S = Order as o .emit(n: items.map(o => o.price))");
        let StreamOp::Emit { fields, .. } = &ops[0] else {
            panic!()
        };
        let Expr::Call { args, .. } = &fields[0].value else {
            panic!("{:?}", fields[0].value)
        };
        let Arg::Positional(Expr::Lambda { body, .. }) = &args[0] else {
            panic!("{args:?}")
        };
        assert_eq!(
            **body,
            Expr::Member {
                expr: Box::new(Expr::Ident("o".into())),
                member: "price".into()
            }
        );
    }
}
