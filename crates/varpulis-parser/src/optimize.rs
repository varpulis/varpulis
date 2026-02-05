//! AST-level constant folding optimization pass.
//!
//! Evaluates constant arithmetic expressions at compile time, reducing
//! runtime computation. Applied after parsing, before the AST reaches
//! the runtime engine.

use varpulis_core::ast::*;
use varpulis_core::span::Spanned;

/// Fold constants in an entire program.
pub fn fold_program(program: Program) -> Program {
    Program {
        statements: program
            .statements
            .into_iter()
            .map(fold_spanned_stmt)
            .collect(),
    }
}

fn fold_spanned_stmt(s: Spanned<Stmt>) -> Spanned<Stmt> {
    Spanned::new(fold_stmt(s.node), s.span)
}

fn fold_stmt(stmt: Stmt) -> Stmt {
    match stmt {
        Stmt::VarDecl {
            mutable,
            name,
            ty,
            value,
        } => Stmt::VarDecl {
            mutable,
            name,
            ty,
            value: fold_expr(value),
        },
        Stmt::ConstDecl { name, ty, value } => Stmt::ConstDecl {
            name,
            ty,
            value: fold_expr(value),
        },
        Stmt::FnDecl {
            name,
            params,
            ret,
            body,
        } => Stmt::FnDecl {
            name,
            params,
            ret,
            body: body.into_iter().map(fold_spanned_stmt).collect(),
        },
        Stmt::StreamDecl {
            name,
            type_annotation,
            source,
            ops,
        } => Stmt::StreamDecl {
            name,
            type_annotation,
            source,
            ops: ops.into_iter().map(fold_stream_op).collect(),
        },
        Stmt::If {
            cond,
            then_branch,
            elif_branches,
            else_branch,
        } => Stmt::If {
            cond: fold_expr(cond),
            then_branch: then_branch.into_iter().map(fold_spanned_stmt).collect(),
            elif_branches: elif_branches
                .into_iter()
                .map(|(c, b)| (fold_expr(c), b.into_iter().map(fold_spanned_stmt).collect()))
                .collect(),
            else_branch: else_branch.map(|b| b.into_iter().map(fold_spanned_stmt).collect()),
        },
        Stmt::For { var, iter, body } => Stmt::For {
            var,
            iter: fold_expr(iter),
            body: body.into_iter().map(fold_spanned_stmt).collect(),
        },
        Stmt::While { cond, body } => Stmt::While {
            cond: fold_expr(cond),
            body: body.into_iter().map(fold_spanned_stmt).collect(),
        },
        Stmt::Return(Some(expr)) => Stmt::Return(Some(fold_expr(expr))),
        Stmt::Expr(expr) => Stmt::Expr(fold_expr(expr)),
        Stmt::Assignment { name, value } => Stmt::Assignment {
            name,
            value: fold_expr(value),
        },
        Stmt::Emit { event_type, fields } => Stmt::Emit {
            event_type,
            fields: fields.into_iter().map(fold_named_arg).collect(),
        },
        // Pass through unchanged
        other => other,
    }
}

fn fold_named_arg(arg: NamedArg) -> NamedArg {
    NamedArg {
        name: arg.name,
        value: fold_expr(arg.value),
    }
}

fn fold_stream_op(op: StreamOp) -> StreamOp {
    match op {
        StreamOp::Where(expr) => StreamOp::Where(fold_expr(expr)),
        StreamOp::Filter(expr) => StreamOp::Filter(fold_expr(expr)),
        StreamOp::Map(expr) => StreamOp::Map(fold_expr(expr)),
        StreamOp::Process(expr) => StreamOp::Process(fold_expr(expr)),
        StreamOp::OnError(expr) => StreamOp::OnError(fold_expr(expr)),
        StreamOp::Having(expr) => StreamOp::Having(fold_expr(expr)),
        StreamOp::PartitionBy(expr) => StreamOp::PartitionBy(fold_expr(expr)),
        StreamOp::Limit(expr) => StreamOp::Limit(fold_expr(expr)),
        StreamOp::Distinct(opt) => StreamOp::Distinct(opt.map(fold_expr)),
        StreamOp::On(expr) => StreamOp::On(fold_expr(expr)),
        StreamOp::Within(expr) => StreamOp::Within(fold_expr(expr)),
        StreamOp::AllowedLateness(expr) => StreamOp::AllowedLateness(fold_expr(expr)),
        StreamOp::ToExpr(expr) => StreamOp::ToExpr(fold_expr(expr)),
        StreamOp::Emit {
            output_type,
            fields,
            target_context,
        } => StreamOp::Emit {
            output_type,
            fields: fields.into_iter().map(fold_named_arg).collect(),
            target_context,
        },
        StreamOp::Print(exprs) => StreamOp::Print(exprs.into_iter().map(fold_expr).collect()),
        StreamOp::Log(args) => StreamOp::Log(args.into_iter().map(fold_named_arg).collect()),
        StreamOp::Tap(args) => StreamOp::Tap(args.into_iter().map(fold_named_arg).collect()),
        // Pass through unchanged
        other => other,
    }
}

/// Recursively fold constant expressions.
fn fold_expr(expr: Expr) -> Expr {
    match expr {
        // Recurse into binary expressions
        Expr::Binary { op, left, right } => {
            let left = fold_expr(*left);
            let right = fold_expr(*right);
            fold_binary(op, left, right)
        }
        // Recurse into unary expressions
        Expr::Unary { op, expr } => {
            let inner = fold_expr(*expr);
            fold_unary(op, inner)
        }
        // Recurse into function call arguments
        Expr::Call { func, args } => Expr::Call {
            func: Box::new(fold_expr(*func)),
            args: args.into_iter().map(fold_arg).collect(),
        },
        // Recurse into array elements
        Expr::Array(elems) => Expr::Array(elems.into_iter().map(fold_expr).collect()),
        // Recurse into map values
        Expr::Map(entries) => Expr::Map(
            entries
                .into_iter()
                .map(|(k, v)| (k, fold_expr(v)))
                .collect(),
        ),
        // Recurse into lambda body
        Expr::Lambda { params, body } => Expr::Lambda {
            params,
            body: Box::new(fold_expr(*body)),
        },
        // Recurse into if expression
        Expr::If {
            cond,
            then_branch,
            else_branch,
        } => Expr::If {
            cond: Box::new(fold_expr(*cond)),
            then_branch: Box::new(fold_expr(*then_branch)),
            else_branch: Box::new(fold_expr(*else_branch)),
        },
        // Recurse into coalesce
        Expr::Coalesce { expr, default } => Expr::Coalesce {
            expr: Box::new(fold_expr(*expr)),
            default: Box::new(fold_expr(*default)),
        },
        // Recurse into range
        Expr::Range {
            start,
            end,
            inclusive,
        } => Expr::Range {
            start: Box::new(fold_expr(*start)),
            end: Box::new(fold_expr(*end)),
            inclusive,
        },
        // Recurse into member access
        Expr::Member { expr, member } => Expr::Member {
            expr: Box::new(fold_expr(*expr)),
            member,
        },
        // Recurse into optional member access
        Expr::OptionalMember { expr, member } => Expr::OptionalMember {
            expr: Box::new(fold_expr(*expr)),
            member,
        },
        // Recurse into index access
        Expr::Index { expr, index } => Expr::Index {
            expr: Box::new(fold_expr(*expr)),
            index: Box::new(fold_expr(*index)),
        },
        // Recurse into slice
        Expr::Slice { expr, start, end } => Expr::Slice {
            expr: Box::new(fold_expr(*expr)),
            start: start.map(|s| Box::new(fold_expr(*s))),
            end: end.map(|e| Box::new(fold_expr(*e))),
        },
        // Recurse into block expression
        Expr::Block { stmts, result } => Expr::Block {
            stmts: stmts
                .into_iter()
                .map(|(name, ty, val, mutable)| (name, ty, fold_expr(val), mutable))
                .collect(),
            result: Box::new(fold_expr(*result)),
        },
        // Literals and identifiers are already folded
        other => other,
    }
}

fn fold_arg(arg: Arg) -> Arg {
    match arg {
        Arg::Positional(expr) => Arg::Positional(fold_expr(expr)),
        Arg::Named(name, expr) => Arg::Named(name, fold_expr(expr)),
    }
}

/// Try to fold a binary operation on two (possibly constant) operands.
fn fold_binary(op: BinOp, left: Expr, right: Expr) -> Expr {
    // First: try full constant folding (both operands are literals)
    match (&op, &left, &right) {
        // Int OP Int
        (BinOp::Add, Expr::Int(a), Expr::Int(b)) => return Expr::Int(a.wrapping_add(*b)),
        (BinOp::Sub, Expr::Int(a), Expr::Int(b)) => return Expr::Int(a.wrapping_sub(*b)),
        (BinOp::Mul, Expr::Int(a), Expr::Int(b)) => return Expr::Int(a.wrapping_mul(*b)),
        (BinOp::Div, Expr::Int(a), Expr::Int(b)) if *b != 0 => return Expr::Int(a / b),
        (BinOp::Mod, Expr::Int(a), Expr::Int(b)) if *b != 0 => return Expr::Int(a % b),
        (BinOp::Pow, Expr::Int(a), Expr::Int(b)) if *b >= 0 => {
            return Expr::Int(a.wrapping_pow(*b as u32));
        }

        // Float OP Float
        (BinOp::Add, Expr::Float(a), Expr::Float(b)) => return Expr::Float(a + b),
        (BinOp::Sub, Expr::Float(a), Expr::Float(b)) => return Expr::Float(a - b),
        (BinOp::Mul, Expr::Float(a), Expr::Float(b)) => return Expr::Float(a * b),
        (BinOp::Div, Expr::Float(a), Expr::Float(b)) if *b != 0.0 => {
            return Expr::Float(a / b);
        }

        _ => {}
    }

    // Second: identity folding (one operand is a known identity value)
    match (&op, &left, &right) {
        // x * 0 or 0 * x → 0 (integer only)
        (BinOp::Mul, _, Expr::Int(0)) | (BinOp::Mul, Expr::Int(0), _) => {
            return Expr::Int(0);
        }
        // x * 1 → x
        (BinOp::Mul, _, Expr::Int(1)) => return left,
        // 1 * x → x
        (BinOp::Mul, Expr::Int(1), _) => return right,
        // x + 0 → x
        (BinOp::Add, _, Expr::Int(0)) => return left,
        // 0 + x → x
        (BinOp::Add, Expr::Int(0), _) => return right,
        // x - 0 → x
        (BinOp::Sub, _, Expr::Int(0)) => return left,
        // x / 1 → x
        (BinOp::Div, _, Expr::Int(1)) => return left,

        _ => {}
    }

    // Not foldable — reconstruct
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Try to fold a unary operation on a constant operand.
fn fold_unary(op: UnaryOp, inner: Expr) -> Expr {
    match (&op, &inner) {
        (UnaryOp::Neg, Expr::Int(a)) => Expr::Int(-a),
        (UnaryOp::Neg, Expr::Float(a)) => Expr::Float(-a),
        _ => Expr::Unary {
            op,
            expr: Box::new(inner),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to build a binary expression
    fn bin(op: BinOp, left: Expr, right: Expr) -> Expr {
        Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn unary(op: UnaryOp, expr: Expr) -> Expr {
        Expr::Unary {
            op,
            expr: Box::new(expr),
        }
    }

    #[test]
    fn fold_int_addition() {
        let expr = bin(BinOp::Add, Expr::Int(1), Expr::Int(2));
        assert_eq!(fold_expr(expr), Expr::Int(3));
    }

    #[test]
    fn fold_int_subtraction() {
        let expr = bin(BinOp::Sub, Expr::Int(5), Expr::Int(3));
        assert_eq!(fold_expr(expr), Expr::Int(2));
    }

    #[test]
    fn fold_int_multiplication() {
        let expr = bin(BinOp::Mul, Expr::Int(2), Expr::Int(3));
        assert_eq!(fold_expr(expr), Expr::Int(6));
    }

    #[test]
    fn fold_int_division() {
        let expr = bin(BinOp::Div, Expr::Int(10), Expr::Int(3));
        assert_eq!(fold_expr(expr), Expr::Int(3));
    }

    #[test]
    fn fold_int_pow() {
        let expr = bin(BinOp::Pow, Expr::Int(2), Expr::Int(10));
        assert_eq!(fold_expr(expr), Expr::Int(1024));
    }

    #[test]
    fn fold_float_arithmetic() {
        let expr = bin(BinOp::Mul, Expr::Float(2.0), Expr::Float(3.0));
        assert_eq!(fold_expr(expr), Expr::Float(6.0));
    }

    #[test]
    fn fold_identity_mul_zero() {
        // x * 0 → 0
        let expr = bin(BinOp::Mul, Expr::Ident("x".into()), Expr::Int(0));
        assert_eq!(fold_expr(expr), Expr::Int(0));
    }

    #[test]
    fn fold_identity_mul_one() {
        // x * 1 → x
        let expr = bin(BinOp::Mul, Expr::Ident("x".into()), Expr::Int(1));
        assert_eq!(fold_expr(expr), Expr::Ident("x".into()));
    }

    #[test]
    fn fold_identity_add_zero() {
        // x + 0 → x
        let expr = bin(BinOp::Add, Expr::Ident("x".into()), Expr::Int(0));
        assert_eq!(fold_expr(expr), Expr::Ident("x".into()));
    }

    #[test]
    fn fold_nested_expression() {
        // (2 + 3) * 4 → 20
        let inner = bin(BinOp::Add, Expr::Int(2), Expr::Int(3));
        let expr = bin(BinOp::Mul, inner, Expr::Int(4));
        assert_eq!(fold_expr(expr), Expr::Int(20));
    }

    #[test]
    fn fold_unary_neg_int() {
        let expr = unary(UnaryOp::Neg, Expr::Int(5));
        assert_eq!(fold_expr(expr), Expr::Int(-5));
    }

    #[test]
    fn fold_unary_neg_float() {
        let expr = unary(UnaryOp::Neg, Expr::Float(2.75));
        assert_eq!(fold_expr(expr), Expr::Float(-2.75));
    }

    #[test]
    fn preserves_non_constant() {
        // x + 1 stays as Binary
        let expr = bin(BinOp::Add, Expr::Ident("x".into()), Expr::Int(1));
        let folded = fold_expr(expr.clone());
        assert_eq!(folded, expr);
    }

    #[test]
    fn fold_in_call_args() {
        // f(2 * 3) → f(6)
        let call = Expr::Call {
            func: Box::new(Expr::Ident("f".into())),
            args: vec![Arg::Positional(bin(BinOp::Mul, Expr::Int(2), Expr::Int(3)))],
        };
        let folded = fold_expr(call);
        assert_eq!(
            folded,
            Expr::Call {
                func: Box::new(Expr::Ident("f".into())),
                args: vec![Arg::Positional(Expr::Int(6))],
            }
        );
    }

    #[test]
    fn div_by_zero_not_folded() {
        // 1 / 0 stays as Binary
        let expr = bin(BinOp::Div, Expr::Int(1), Expr::Int(0));
        let folded = fold_expr(expr);
        assert!(matches!(folded, Expr::Binary { .. }));
    }

    #[test]
    fn fold_identity_sub_zero() {
        // x - 0 → x
        let expr = bin(BinOp::Sub, Expr::Ident("x".into()), Expr::Int(0));
        assert_eq!(fold_expr(expr), Expr::Ident("x".into()));
    }

    #[test]
    fn fold_identity_div_one() {
        // x / 1 → x
        let expr = bin(BinOp::Div, Expr::Ident("x".into()), Expr::Int(1));
        assert_eq!(fold_expr(expr), Expr::Ident("x".into()));
    }
}
