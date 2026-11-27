//! What the shipped engine does not run.
//!
//! The language is wider than the engine that ships. `Program` (the library),
//! the CLI and the WASM build all run the synchronous path, and some
//! constructs the parser accepts do nothing there: an `.enrich()` was skipped,
//! a `timer()` never fired, a `context` was dropped, an `import` was logged and
//! forgotten. A rule that compiles and then silently does less than it says is
//! worse than one that is refused, so these are refused at load, each with a
//! code and a hint.
//!
//! This is a separate check from [`super::validate`]: the validator describes
//! the language, this describes one engine. A host that embeds an engine able
//! to run these constructs does not call it.

use super::{Diagnostic, Severity, ValidationResult};
use crate::ast::{Program, Stmt, StreamOp, StreamSource};
use crate::span::Span;

/// `import "file.vpl"`: imports are not resolved.
pub const IMPORT_UNRESOLVED: &str = "E100";
/// `context name` or `.context(name)`: contexts are not run.
pub const CONTEXT_UNSUPPORTED: &str = "E101";
/// `timer(5s)`: timers do not fire.
pub const TIMER_UNSUPPORTED: &str = "E102";
/// `.enrich(...)`: the lookup is not performed.
pub const ENRICH_UNSUPPORTED: &str = "E103";
/// `.alert(webhook: ...)`: the webhook is not called.
pub const ALERT_WEBHOOK_UNSUPPORTED: &str = "E104";

/// Every construct of `program` the synchronous engine would not run, as
/// error diagnostics. Empty when the program runs as written.
pub fn check_sync_engine_support(program: &Program) -> ValidationResult {
    let mut diagnostics = Vec::new();
    for stmt in &program.statements {
        let span = stmt.span;
        match &stmt.node {
            Stmt::Import { path, .. } => diagnostics.push(error(
                span,
                IMPORT_UNRESOLVED,
                format!("import \"{path}\" is not resolved by this engine"),
                "put the imported declarations in the same program",
            )),
            Stmt::ContextDecl { name, .. } => diagnostics.push(error(
                span,
                CONTEXT_UNSUPPORTED,
                format!(
                    "context '{name}' is not run by this engine: one program runs on one thread"
                ),
                "remove the context; run several programs if you need parallelism",
            )),
            Stmt::StreamDecl {
                source,
                ops,
                op_spans,
                ..
            } => {
                if let StreamSource::Timer(_) = source {
                    diagnostics.push(error(
                        span,
                        TIMER_UNSUPPORTED,
                        "timer() never fires in this engine".to_string(),
                        "let the host feed a periodic event instead",
                    ));
                }
                for (i, op) in ops.iter().enumerate() {
                    let op_span = op_spans.get(i).copied().unwrap_or(span);
                    check_op(op, op_span, &mut diagnostics);
                }
            }
            _ => {}
        }
    }
    ValidationResult { diagnostics }
}

fn check_op(op: &StreamOp, span: Span, out: &mut Vec<Diagnostic>) {
    match op {
        StreamOp::Enrich(_) => out.push(error(
            span,
            ENRICH_UNSUPPORTED,
            ".enrich() is not performed by this engine".to_string(),
            "enrich the event in the host before feeding it",
        )),
        StreamOp::Context(name) => out.push(error(
            span,
            CONTEXT_UNSUPPORTED,
            format!(".context({name}) is not run by this engine"),
            "remove the .context() call",
        )),
        StreamOp::Emit {
            target_context: Some(name),
            ..
        } => out.push(error(
            span,
            CONTEXT_UNSUPPORTED,
            format!(".emit(context: {name}) is not run by this engine"),
            "remove the context: argument",
        )),
        StreamOp::Alert(args) if args.iter().any(|a| a.name == "webhook") => out.push(error(
            span,
            ALERT_WEBHOOK_UNSUPPORTED,
            ".alert(webhook: ...) does not call its webhook in this engine".to_string(),
            "route the stream with .to() and let the host deliver it",
        )),
        StreamOp::Fork(paths) => {
            for path in paths {
                for inner in &path.ops {
                    check_op(inner, span, out);
                }
            }
        }
        _ => {}
    }
}

fn error(span: Span, code: &'static str, message: String, hint: &str) -> Diagnostic {
    Diagnostic {
        severity: Severity::Error,
        span,
        message,
        code: Some(code),
        hint: Some(hint.to_string()),
        related: Vec::new(),
    }
}
