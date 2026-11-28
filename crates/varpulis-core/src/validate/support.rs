//! What the shipped engine does not run.
//!
//! The language is wider than the engine that ships. `Program` (the library),
//! the CLI and the WASM build all run the synchronous path, and some
//! constructs the parser accepts do nothing there: an `.enrich()` was skipped,
//! a `context` was dropped, an `import` was logged and forgotten. A rule that
//! compiles and then silently does less than it says is worse than one that
//! is refused, so these are refused at load, each with a code and a hint.
//!
//! `timer()` is run by `Program` when its host says, at load, which clock
//! drives the timers; without that it is refused, as it is by the WASM build,
//! which has no clock at all. [`check_engine_support`] takes what the engine
//! can do; [`check_sync_engine_support`] is the verdict for a host that
//! drives the timers, the one the editor shows.
//!
//! This is a separate check from [`super::validate`]: the validator describes
//! the language, this describes one engine. A host that embeds an engine able
//! to run these constructs does not call it.

use super::{Diagnostic, Severity, ValidationResult};
use crate::ast::{Expr, Program, Stmt, StreamOp, StreamSource, TimerDecl};
use crate::span::Span;

/// `import "file.vpl"`: imports are not resolved.
pub const IMPORT_UNRESOLVED: &str = "E100";
/// `context name` or `.context(name)`: contexts are not run.
pub const CONTEXT_UNSUPPORTED: &str = "E101";
/// `timer(5s)`: timers do not fire in this engine, or this one cannot be
/// fired as written (its period is not a duration literal, or is zero).
pub const TIMER_UNSUPPORTED: &str = "E102";
/// `.enrich(...)`: the lookup is not performed.
pub const ENRICH_UNSUPPORTED: &str = "E103";
/// `.alert(webhook: ...)`: the webhook is not called.
pub const ALERT_WEBHOOK_UNSUPPORTED: &str = "E104";

/// Whether an engine fires `timer()` sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimerSupport {
    /// Fired, on a clock the host drives.
    Run,
    /// The engine could fire them, but the host has not said which clock
    /// drives them, so nothing would: `Program` compiled without a timer
    /// clock.
    NoClock,
    /// Never fired: the engine has no clock (the WASM build).
    Never,
}

/// What an engine runs beyond the constructs every engine runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct EngineSupport {
    /// What becomes of a `timer()` source.
    pub timers: TimerSupport,
}

impl EngineSupport {
    /// `varpulis-engine`'s `Program` with a timer clock, and so the CLI's
    /// `simulate`; the verdict the editor shows.
    pub const WITH_TIMERS: Self = Self {
        timers: TimerSupport::Run,
    };
    /// `varpulis-engine`'s `Program` compiled without a timer clock.
    pub const WITHOUT_TIMER_CLOCK: Self = Self {
        timers: TimerSupport::NoClock,
    };
    /// The WASM build: no clock to fire a timer on.
    pub const WITHOUT_TIMERS: Self = Self {
        timers: TimerSupport::Never,
    };
}

/// Every construct of `program` that the synchronous engine would not run,
/// for a host that drives its timers.
///
/// Error diagnostics, empty when the program runs as written. Same as
/// [`check_engine_support`] with [`EngineSupport::WITH_TIMERS`].
pub fn check_sync_engine_support(program: &Program) -> ValidationResult {
    check_engine_support(program, EngineSupport::WITH_TIMERS)
}

/// Every construct of `program` an engine able to do `support` would not run.
///
/// Error diagnostics, empty when the program runs as written.
pub fn check_engine_support(program: &Program, support: EngineSupport) -> ValidationResult {
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
                if let StreamSource::Timer(decl) = source {
                    match support.timers {
                        TimerSupport::Run => check_timer(decl, span, &mut diagnostics),
                        TimerSupport::NoClock => diagnostics.push(error(
                            span,
                            TIMER_UNSUPPORTED,
                            "timer() would never fire: the program was compiled without a \
                             timer clock"
                                .to_string(),
                            "compile it with Program::compile_with(vpl, \
                             &CompileOptions::new().timer_clock(TimerClock::Host)) and call \
                             Program::fire_due, or TimerClock::EventTime to let the events \
                             fed drive the timers",
                        )),
                        TimerSupport::Never => diagnostics.push(error(
                            span,
                            TIMER_UNSUPPORTED,
                            "timer() never fires in this engine: it has no clock to drive it"
                                .to_string(),
                            "let the host feed a periodic event, or run the program with \
                             varpulis-engine's Program, whose host drives its timers",
                        )),
                    }
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

/// A timer the engine fires has a period and a first delay it can read
/// without evaluating anything: a duration literal. Anything else used to be
/// replaced by one second, or dropped, with a log line.
fn check_timer(decl: &TimerDecl, span: Span, out: &mut Vec<Diagnostic>) {
    match decl.interval {
        Expr::Duration(0) => out.push(error(
            span,
            TIMER_UNSUPPORTED,
            "timer() has a period of zero: it would fire without end".to_string(),
            "give it a period above zero, such as timer(5s)",
        )),
        Expr::Duration(_) => {}
        _ => out.push(error(
            span,
            TIMER_UNSUPPORTED,
            "timer() takes its period as a duration literal; this one is an expression the engine does not evaluate".to_string(),
            "write the period as a literal, such as timer(5s) or timer(1m)",
        )),
    }
    if let Some(delay) = &decl.initial_delay {
        if !matches!(delay.as_ref(), Expr::Duration(_)) {
            out.push(error(
                span,
                TIMER_UNSUPPORTED,
                "timer() takes its initial_delay as a duration literal; this one is an expression the engine does not evaluate".to_string(),
                "write the delay as a literal, such as initial_delay: 30s",
            ));
        }
    }
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
