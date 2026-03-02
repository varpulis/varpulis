//! Semantic validation for VPL programs.
//!
//! Two-pass validation:
//! - **Pass 1**: Build symbol table from declarations, detect duplicates.
//! - **Pass 2**: Validate references, operation ordering, parameters, expressions, aggregates.

pub mod builtins;
mod checks;
pub mod scope;
mod suggest;

use miette::NamedSource;
use scope::SymbolTable;

use crate::ast::Program;
use crate::span::Span;

/// Severity of a diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// A fatal error that prevents compilation.
    Error,
    /// A non-fatal warning.
    Warning,
}

/// A related source location (e.g. "previously declared here").
#[derive(Debug, Clone)]
pub struct RelatedSpan {
    /// Source location of the related item.
    pub span: Span,
    /// Description of the relationship (e.g., "previously declared here").
    pub message: String,
}

/// A single diagnostic produced by semantic validation.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    /// Error or warning severity.
    pub severity: Severity,
    /// Source location of the diagnostic.
    pub span: Span,
    /// Human-readable diagnostic message.
    pub message: String,
    /// Optional error code (e.g., "E001").
    pub code: Option<&'static str>,
    /// Optional hint for fixing the issue.
    pub hint: Option<String>,
    /// Related source locations for context.
    pub related: Vec<RelatedSpan>,
}

/// Result of semantic validation.
#[derive(Debug)]
pub struct ValidationResult {
    /// All diagnostics produced during validation.
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
            let code_str = d.code.map(|c| format!("[{c}] ")).unwrap_or_default();
            out.push_str(&format!(
                "{}:{}: {}{}{}\n",
                line, col, prefix, code_str, d.message
            ));
            if let Some(ref hint) = d.hint {
                out.push_str(&format!("  hint: {hint}\n"));
            }
            for rel in &d.related {
                let (rl, rc) = position_to_line_col(source, rel.span.start);
                out.push_str(&format!("  {}:{}: {}\n", rl, rc, rel.message));
            }
        }
        out
    }
}

/// A semantic [`Diagnostic`] bundled with source text for rich terminal
/// rendering via [`miette`].
#[derive(Debug)]
pub struct RichDiagnostic {
    message: String,
    src: NamedSource<String>,
    span: miette::SourceSpan,
    help: Option<String>,
    code_str: Option<String>,
    related: Vec<RichRelatedSpan>,
}

impl std::fmt::Display for RichDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RichDiagnostic {}

impl miette::Diagnostic for RichDiagnostic {
    fn code<'a>(&'a self) -> Option<Box<dyn std::fmt::Display + 'a>> {
        self.code_str
            .as_ref()
            .map(|c| Box::new(c.clone()) as Box<dyn std::fmt::Display>)
    }

    fn help<'a>(&'a self) -> Option<Box<dyn std::fmt::Display + 'a>> {
        self.help
            .as_ref()
            .map(|h| Box::new(h.clone()) as Box<dyn std::fmt::Display>)
    }

    fn source_code(&self) -> Option<&dyn miette::SourceCode> {
        Some(&self.src)
    }

    fn labels(&self) -> Option<Box<dyn Iterator<Item = miette::LabeledSpan> + '_>> {
        Some(Box::new(std::iter::once(
            miette::LabeledSpan::new_primary_with_span(Some("here".to_string()), self.span),
        )))
    }

    fn related<'a>(&'a self) -> Option<Box<dyn Iterator<Item = &'a dyn miette::Diagnostic> + 'a>> {
        if self.related.is_empty() {
            None
        } else {
            Some(Box::new(
                self.related.iter().map(|r| r as &dyn miette::Diagnostic),
            ))
        }
    }
}

/// A related span for [`RichDiagnostic`].
#[derive(Debug)]
struct RichRelatedSpan {
    message: String,
    src: NamedSource<String>,
    span: miette::SourceSpan,
}

impl std::fmt::Display for RichRelatedSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RichRelatedSpan {}

impl miette::Diagnostic for RichRelatedSpan {
    fn source_code(&self) -> Option<&dyn miette::SourceCode> {
        Some(&self.src)
    }

    fn labels(&self) -> Option<Box<dyn Iterator<Item = miette::LabeledSpan> + '_>> {
        Some(Box::new(std::iter::once(
            miette::LabeledSpan::new_with_span(Some("related".to_string()), self.span),
        )))
    }
}

impl RichDiagnostic {
    /// Wrap a semantic [`Diagnostic`] with the source text and filename so
    /// that `miette` can render a rich diagnostic.
    pub fn from_diagnostic(d: &Diagnostic, source: &str, filename: &str) -> Self {
        let named = NamedSource::new(filename, source.to_string());
        let related = d
            .related
            .iter()
            .map(|r| RichRelatedSpan {
                message: r.message.clone(),
                src: named.clone(),
                span: r.span.into(),
            })
            .collect();
        Self {
            message: d.message.clone(),
            src: named,
            span: d.span.into(),
            help: d.hint.clone(),
            code_str: d.code.map(String::from),
            related,
        }
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
    let (diagnostics, _symbols) = validate_inner(source, program);
    ValidationResult { diagnostics }
}

/// Validate a parsed VPL program and return both diagnostics and the symbol table.
///
/// This is used by the LSP for go-to-definition and find-references.
pub fn validate_with_symbols(
    source: &str,
    program: &Program,
) -> (ValidationResult, scope::SymbolTable) {
    let (diagnostics, symbols) = validate_inner(source, program);
    (ValidationResult { diagnostics }, symbols)
}

fn validate_inner(source: &str, program: &Program) -> (Vec<Diagnostic>, scope::SymbolTable) {
    let mut v = Validator::new(source);

    // Pass 1: collect declarations and detect duplicates
    checks::pass1_declarations(&mut v, program);

    // Pass 2: semantic checks
    checks::pass2_semantic(&mut v, program);

    (v.diagnostics, v.symbols)
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

    fn event_decl(name: &str) -> Stmt {
        Stmt::EventDecl {
            name: name.to_string(),
            extends: None,
            fields: vec![],
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
            event_decl("X"),
            event_decl("Y"),
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("X".to_string()),
                ops: vec![],
                op_spans: vec![],
            },
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("Y".to_string()),
                ops: vec![],
                op_spans: vec![],
            },
        ]);
        let result = validate("stream S = X\nstream S = Y", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E002")));
    }

    #[test]
    fn test_unimplemented_map_op() {
        let prog = make_program(vec![
            event_decl("X"),
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("X".to_string()),
                ops: vec![StreamOp::Map(Expr::Ident("x".to_string()))],
                op_spans: vec![],
            },
        ]);
        let result = validate("stream S = X.map(x)", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E090")));
    }

    #[test]
    fn test_having_without_aggregate() {
        let prog = make_program(vec![
            event_decl("X"),
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("X".to_string()),
                ops: vec![StreamOp::Having(Expr::Bool(true))],
                op_spans: vec![],
            },
        ]);
        let result = validate("stream S = X.having(true)", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E010")));
    }

    #[test]
    fn test_aggregate_without_window_warns() {
        let prog = make_program(vec![
            event_decl("X"),
            Stmt::StreamDecl {
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
                op_spans: vec![],
            },
        ]);
        let result = validate("stream S = X.aggregate(c: count())", &prog);
        assert!(!result.has_errors()); // warning only
        assert!(result.diagnostics.iter().any(|d| d.code == Some("W001")));
    }

    #[test]
    fn test_unknown_log_param() {
        let prog = make_program(vec![
            event_decl("X"),
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("X".to_string()),
                ops: vec![StreamOp::Log(vec![crate::ast::NamedArg {
                    name: "lvl".to_string(),
                    value: Expr::Str("info".to_string()),
                }])],
                op_spans: vec![],
            },
        ]);
        let result = validate("stream S = X.log(lvl: \"info\")", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E080")));
    }

    #[test]
    fn test_unknown_aggregate_function() {
        let prog = make_program(vec![
            event_decl("X"),
            Stmt::StreamDecl {
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
                            func: Box::new(Expr::Ident("unknown_agg_fn".to_string())),
                            args: vec![Arg::Positional(Expr::Ident("val".to_string()))],
                        },
                    }]),
                ],
                op_spans: vec![],
            },
        ]);
        let result = validate(
            "stream S = X.window(1m).aggregate(x: unknown_agg_fn(val))",
            &prog,
        );
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E070")));
    }

    #[test]
    fn test_where_with_non_bool_literal() {
        let prog = make_program(vec![
            event_decl("X"),
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("X".to_string()),
                ops: vec![StreamOp::Where(Expr::Int(42))],
                op_spans: vec![],
            },
        ]);
        let result = validate("stream S = X.where(42)", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E060")));
    }

    #[test]
    fn test_within_non_duration_literal() {
        let prog = make_program(vec![
            event_decl("X"),
            event_decl("A"),
            Stmt::StreamDecl {
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
                op_spans: vec![],
            },
        ]);
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
                op_spans: vec![],
            },
        ]);
        let result = validate("", &prog);
        assert!(!result.has_errors());
    }

    #[test]
    fn test_sum_without_field_arg() {
        let prog = make_program(vec![
            event_decl("X"),
            Stmt::StreamDecl {
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
                op_spans: vec![],
            },
        ]);
        let result = validate("stream S = X.window(1m).aggregate(s: sum())", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E071")));
    }

    #[test]
    fn test_connector_reference_unknown() {
        let prog = make_program(vec![
            event_decl("X"),
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("X".to_string()),
                ops: vec![StreamOp::To {
                    connector_name: "UnknownConn".to_string(),
                    params: vec![],
                }],
                op_spans: vec![],
            },
        ]);
        let result = validate("stream S = X.to(UnknownConn)", &prog);
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E030")));
    }

    #[test]
    fn test_within_outside_sequence() {
        let prog = make_program(vec![
            event_decl("X"),
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("X".to_string()),
                ops: vec![
                    StreamOp::Where(Expr::Bool(true)),
                    StreamOp::Within(Expr::Duration(60_000_000_000)),
                ],
                op_spans: vec![],
            },
        ]);
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
        let prog = make_program(vec![
            event_decl("X"),
            Stmt::StreamDecl {
                name: "S".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("X".to_string()),
                ops: vec![StreamOp::Emit {
                    output_type: Some("UnknownAlert".to_string()),
                    fields: vec![],
                    target_context: None,
                }],
                op_spans: vec![],
            },
        ]);
        let result = validate("stream S = X.emit as UnknownAlert ()", &prog);
        // E034 error — undeclared type in emit
        assert!(result.has_errors());
        assert!(result.diagnostics.iter().any(|d| d.code == Some("E034")));
    }
}
