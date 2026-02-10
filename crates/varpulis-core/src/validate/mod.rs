//! Semantic validation for VPL programs.
//!
//! Two-pass validation:
//! - **Pass 1**: Build symbol table from declarations, detect duplicates.
//! - **Pass 2**: Validate references, operation ordering, parameters, expressions, aggregates.

pub mod builtins;
mod checks;
mod scope;
mod suggest;

use crate::ast::Program;
use crate::span::Span;
use scope::SymbolTable;

/// Severity of a diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

/// A related source location (e.g. "previously declared here").
#[derive(Debug, Clone)]
pub struct RelatedSpan {
    pub span: Span,
    pub message: String,
}

/// A single diagnostic produced by semantic validation.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub severity: Severity,
    pub span: Span,
    pub message: String,
    pub code: Option<&'static str>,
    pub hint: Option<String>,
    pub related: Vec<RelatedSpan>,
}

/// Result of semantic validation.
#[derive(Debug)]
pub struct ValidationResult {
    pub diagnostics: Vec<Diagnostic>,
}

impl ValidationResult {
    /// Returns true if there are no errors (warnings are OK).
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity == Severity::Error)
    }

    /// Format all diagnostics into a human-readable string.
    pub fn format(&self, source: &str) -> String {
        let mut out = String::new();
        for d in &self.diagnostics {
            let (line, col) = position_to_line_col(source, d.span.start);
            let prefix = match d.severity {
                Severity::Error => "error",
                Severity::Warning => "warning",
            };
            let code_str = d.code.map(|c| format!("[{}] ", c)).unwrap_or_default();
            out.push_str(&format!(
                "{}:{}: {}{}{}\n",
                line, col, prefix, code_str, d.message
            ));
            if let Some(ref hint) = d.hint {
                out.push_str(&format!("  hint: {}\n", hint));
            }
            for rel in &d.related {
                let (rl, rc) = position_to_line_col(source, rel.span.start);
                out.push_str(&format!("  {}:{}: {}\n", rl, rc, rel.message));
            }
        }
        out
    }
}

/// Convert byte offset to 1-indexed line:column.
pub fn diagnostic_position(source: &str, position: usize) -> (usize, usize) {
    position_to_line_col(source, position)
}

/// Convert byte offset to 1-indexed line:column.
fn position_to_line_col(source: &str, position: usize) -> (usize, usize) {
    let mut line = 1;
    let mut col = 1;
    for (i, ch) in source.char_indices() {
        if i >= position {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (line, col)
}

/// Internal validator state.
struct Validator {
    source: String,
    symbols: SymbolTable,
    diagnostics: Vec<Diagnostic>,
}

impl Validator {
    fn new(source: &str) -> Self {
        Self {
            source: source.to_string(),
            symbols: SymbolTable::new(),
            diagnostics: Vec::new(),
        }
    }

    /// Extract a source snippet for a span (for diagnostic context).
    fn snippet(&self, span: Span) -> Option<&str> {
        self.source.get(span.start..span.end)
    }

    fn emit(&mut self, severity: Severity, span: Span, code: &'static str, message: String) {
        self.diagnostics.push(Diagnostic {
            severity,
            span,
            message,
            code: Some(code),
            hint: None,
            related: Vec::new(),
        });
    }

    fn emit_with_hint(
        &mut self,
        severity: Severity,
        span: Span,
        code: &'static str,
        message: String,
        hint: String,
    ) {
        self.diagnostics.push(Diagnostic {
            severity,
            span,
            message,
            code: Some(code),
            hint: Some(hint),
            related: Vec::new(),
        });
    }

    fn emit_with_related(
        &mut self,
        severity: Severity,
        span: Span,
        code: &'static str,
        message: String,
        related: Vec<RelatedSpan>,
    ) {
        self.diagnostics.push(Diagnostic {
            severity,
            span,
            message,
            code: Some(code),
            hint: None,
            related,
        });
    }
}

/// Validate a parsed VPL program.
///
/// `source` is the original source text (used for formatting diagnostics).
/// `program` is the parsed AST.
pub fn validate(source: &str, program: &Program) -> ValidationResult {
    let mut v = Validator::new(source);

    // Pass 1: collect declarations and detect duplicates
    checks::pass1_declarations(&mut v, program);

    // Pass 2: semantic checks
    checks::pass2_semantic(&mut v, program);

    ValidationResult {
        diagnostics: v.diagnostics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;
    use crate::span::{Span, Spanned};

    fn make_program(stmts: Vec<Stmt>) -> Program {
        Program {
            statements: stmts
                .into_iter()
                .map(|s| Spanned::new(s, Span::new(0, 10)))
                .collect(),
        }
    }

    #[test]
    fn test_empty_program_no_diagnostics() {
        let prog = make_program(vec![]);
        let result = validate("", &prog);
        assert!(!result.has_errors());
        assert!(result.diagnostics.is_empty());
    }

    #[test]
    fn test_duplicate_event_declarations() {
        let prog = make_program(vec![
            Stmt::EventDecl {
                name: "Foo".to_string(),
                extends: None,
                fields: vec![],
            },
            Stmt::EventDecl {
                name: "Foo".to_string(),
                extends: None,
                fields: vec![],
            },
        ]);
        let result = validate("event Foo {}\nevent Foo {}", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E001")));
    }

    #[test]
    fn test_duplicate_stream_declarations() {
        let prog = make_program(vec![
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("X".to_string()),
                ops: vec![],
            },
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("Y".to_string()),
                ops: vec![],
            },
        ]);
        let result = validate("stream S = X\nstream S = Y", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E002")));
    }

    #[test]
    fn test_unimplemented_map_op() {
        let prog = make_program(vec![Stmt::StreamDecl {
            name: "S".to_string(),
            type_annotation: None,
            source: StreamSource::Ident("X".to_string()),
            ops: vec![StreamOp::Map(Expr::Ident("x".to_string()))],
        }]);
        let result = validate("stream S = X.map(x)", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E090")));
    }

    #[test]
    fn test_having_without_aggregate() {
        let prog = make_program(vec![Stmt::StreamDecl {
            name: "S".to_string(),
            type_annotation: None,
            source: StreamSource::Ident("X".to_string()),
            ops: vec![StreamOp::Having(Expr::Bool(true))],
        }]);
        let result = validate("stream S = X.having(true)", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E010")));
    }

    #[test]
    fn test_aggregate_without_window_warns() {
        let prog = make_program(vec![Stmt::StreamDecl {
            name: "S".to_string(),
            type_annotation: None,
            source: StreamSource::Ident("X".to_string()),
            ops: vec![StreamOp::Aggregate(vec![AggItem {
                alias: "c".to_string(),
                expr: Expr::Call {
                    func: Box::new(Expr::Ident("count".to_string())),
                    args: vec![],
                },
            }])],
        }]);
        let result = validate("stream S = X.aggregate(c: count())", &prog);
        assert!(!result.has_errors()); // warning only
        assert!(result.diagnostics.iter().any(|d| d.code == Some("W001")));
    }

    #[test]
    fn test_unknown_log_param() {
        let prog = make_program(vec![Stmt::StreamDecl {
            name: "S".to_string(),
            type_annotation: None,
            source: StreamSource::Ident("X".to_string()),
            ops: vec![StreamOp::Log(vec![crate::ast::NamedArg {
                name: "lvl".to_string(),
                value: Expr::Str("info".to_string()),
            }])],
        }]);
        let result = validate("stream S = X.log(lvl: \"info\")", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E080")));
    }

    #[test]
    fn test_unknown_aggregate_function() {
        let prog = make_program(vec![Stmt::StreamDecl {
            name: "S".to_string(),
            type_annotation: None,
            source: StreamSource::Ident("X".to_string()),
            ops: vec![
                StreamOp::Window(WindowArgs {
                    duration: Expr::Duration(60_000_000_000),
                    sliding: None,
                    policy: None,
                    session_gap: None,
                }),
                StreamOp::Aggregate(vec![AggItem {
                    alias: "x".to_string(),
                    expr: Expr::Call {
                        func: Box::new(Expr::Ident("median".to_string())),
                        args: vec![Arg::Positional(Expr::Ident("val".to_string()))],
                    },
                }]),
            ],
        }]);
        let result = validate("stream S = X.window(1m).aggregate(x: median(val))", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E070")));
    }

    #[test]
    fn test_where_with_non_bool_literal() {
        let prog = make_program(vec![Stmt::StreamDecl {
            name: "S".to_string(),
            type_annotation: None,
            source: StreamSource::Ident("X".to_string()),
            ops: vec![StreamOp::Where(Expr::Int(42))],
        }]);
        let result = validate("stream S = X.where(42)", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E060")));
    }

    #[test]
    fn test_within_non_duration_literal() {
        let prog = make_program(vec![Stmt::StreamDecl {
            name: "S".to_string(),
            type_annotation: None,
            source: StreamSource::Ident("X".to_string()),
            ops: vec![
                StreamOp::FollowedBy(FollowedByClause {
                    event_type: "A".to_string(),
                    filter: None,
                    alias: None,
                    match_all: false,
                }),
                StreamOp::Within(Expr::Str("bad".to_string())),
            ],
        }]);
        let result = validate("stream S = X -> A .within(\"bad\")", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E061")));
    }

    #[test]
    fn test_valid_program_no_errors() {
        let prog = make_program(vec![
            Stmt::EventDecl {
                name: "Sensor".to_string(),
                extends: None,
                fields: vec![],
            },
            Stmt::ConnectorDecl {
                name: "MyMqtt".to_string(),
                connector_type: "mqtt".to_string(),
                params: vec![],
            },
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("Sensor".to_string()),
                ops: vec![
                    StreamOp::Where(Expr::Binary {
                        op: crate::ast::BinOp::Gt,
                        left: Box::new(Expr::Ident("temp".to_string())),
                        right: Box::new(Expr::Int(25)),
                    }),
                    StreamOp::Window(WindowArgs {
                        duration: Expr::Duration(60_000_000_000),
                        sliding: None,
                        policy: None,
                        session_gap: None,
                    }),
                    StreamOp::Aggregate(vec![AggItem {
                        alias: "avg_temp".to_string(),
                        expr: Expr::Call {
                            func: Box::new(Expr::Ident("avg".to_string())),
                            args: vec![Arg::Positional(Expr::Ident("temp".to_string()))],
                        },
                    }]),
                    StreamOp::Having(Expr::Binary {
                        op: crate::ast::BinOp::Gt,
                        left: Box::new(Expr::Ident("avg_temp".to_string())),
                        right: Box::new(Expr::Int(30)),
                    }),
                    StreamOp::To {
                        connector_name: "MyMqtt".to_string(),
                        params: vec![],
                    },
                ],
            },
        ]);
        let result = validate("", &prog);
        assert!(!result.has_errors());
    }

    #[test]
    fn test_sum_without_field_arg() {
        let prog = make_program(vec![Stmt::StreamDecl {
            name: "S".to_string(),
            type_annotation: None,
            source: StreamSource::Ident("X".to_string()),
            ops: vec![
                StreamOp::Window(WindowArgs {
                    duration: Expr::Duration(60_000_000_000),
                    sliding: None,
                    policy: None,
                    session_gap: None,
                }),
                StreamOp::Aggregate(vec![AggItem {
                    alias: "s".to_string(),
                    expr: Expr::Call {
                        func: Box::new(Expr::Ident("sum".to_string())),
                        args: vec![],
                    },
                }]),
            ],
        }]);
        let result = validate("stream S = X.window(1m).aggregate(s: sum())", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E071")));
    }

    #[test]
    fn test_connector_reference_unknown() {
        let prog = make_program(vec![Stmt::StreamDecl {
            name: "S".to_string(),
            type_annotation: None,
            source: StreamSource::Ident("X".to_string()),
            ops: vec![StreamOp::To {
                connector_name: "UnknownConn".to_string(),
                params: vec![],
            }],
        }]);
        let result = validate("stream S = X.to(UnknownConn)", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E030")));
    }

    #[test]
    fn test_within_outside_sequence() {
        let prog = make_program(vec![Stmt::StreamDecl {
            name: "S".to_string(),
            type_annotation: None,
            source: StreamSource::Ident("X".to_string()),
            ops: vec![
                StreamOp::Where(Expr::Bool(true)),
                StreamOp::Within(Expr::Duration(60_000_000_000)),
            ],
        }]);
        let result = validate("stream S = X.where(true).within(1m)", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E020")));
    }

    #[test]
    fn test_immutable_assignment() {
        let prog = make_program(vec![
            Stmt::VarDecl {
                mutable: false,
                name: "x".to_string(),
                ty: None,
                value: Expr::Int(1),
            },
            Stmt::Assignment {
                name: "x".to_string(),
                value: Expr::Int(2),
            },
        ]);
        let result = validate("let x = 1\nx := 2", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E040")));
    }

    #[test]
    fn test_mutable_assignment_ok() {
        let prog = make_program(vec![
            Stmt::VarDecl {
                mutable: true,
                name: "x".to_string(),
                ty: None,
                value: Expr::Int(1),
            },
            Stmt::Assignment {
                name: "x".to_string(),
                value: Expr::Int(2),
            },
        ]);
        let result = validate("var x = 1\nx := 2", &prog);
        assert!(!result.has_errors());
    }

    #[test]
    fn test_unknown_function_in_expr() {
        let prog = make_program(vec![Stmt::VarDecl {
            mutable: false,
            name: "x".to_string(),
            ty: None,
            value: Expr::Call {
                func: Box::new(Expr::Ident("nonexistent_fn".to_string())),
                args: vec![],
            },
        }]);
        let result = validate("let x = nonexistent_fn()", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E050")));
    }

    #[test]
    fn test_user_function_arity_mismatch() {
        let prog = make_program(vec![
            Stmt::FnDecl {
                name: "add".to_string(),
                params: vec![
                    Param {
                        name: "a".to_string(),
                        ty: crate::types::Type::Int,
                    },
                    Param {
                        name: "b".to_string(),
                        ty: crate::types::Type::Int,
                    },
                ],
                ret: None,
                body: vec![],
            },
            Stmt::VarDecl {
                mutable: false,
                name: "x".to_string(),
                ty: None,
                value: Expr::Call {
                    func: Box::new(Expr::Ident("add".to_string())),
                    args: vec![Arg::Positional(Expr::Int(1))],
                },
            },
        ]);
        let result = validate(
            "fn add(a: int, b: int):\n  return a + b\nlet x = add(1)",
            &prog,
        );
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E051")));
    }

    #[test]
    fn test_duplicate_type_alias() {
        let prog = make_program(vec![
            Stmt::TypeDecl {
                name: "MyType".to_string(),
                ty: crate::types::Type::Int,
            },
            Stmt::TypeDecl {
                name: "MyType".to_string(),
                ty: crate::types::Type::Float,
            },
        ]);
        let result = validate("type MyType = int\ntype MyType = float", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E007")));
    }

    #[test]
    fn test_emit_as_undeclared_type() {
        let prog = make_program(vec![Stmt::StreamDecl {
            name: "S".to_string(),
            type_annotation: None,
            source: StreamSource::Ident("X".to_string()),
            ops: vec![StreamOp::Emit {
                output_type: Some("UnknownAlert".to_string()),
                fields: vec![],
                target_context: None,
            }],
        }]);
        let result = validate("stream S = X.emit as UnknownAlert ()", &prog);
        // W031 warning — undeclared type in emit
        assert!(result.diagnostics.iter().any(|d| d.code == Some("W031")));
    }
}
