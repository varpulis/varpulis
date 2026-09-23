//! `Program::check` is what `varpulis check` and a host's own check run, so it
//! has to find what the semantic validator finds. It used to parse and load
//! only: a regular expression that could never compile, an unbounded Kleene
//! closure, an event type misspelled in a program that declares its types, all
//! checked "ok", and the documentation promised the opposite.

use varpulis_engine::{Error, Program};

#[test]
fn check_reports_a_regex_that_cannot_compile() {
    let err = Program::check("stream R = T\n    .where(regex_match(CommandLine, '(?i)powershell(?!.*-nop)'))\n    .emit(c: CommandLine)\n")
        .unwrap_err();
    match err {
        Error::Invalid(text) => {
            assert!(text.contains("error[E052]"), "{text}");
            assert!(
                text.starts_with("2:"),
                "the line of the offending operation: {text}"
            );
        }
        other => panic!("expected a validation error, got {other}"),
    }
}

#[test]
fn check_reports_a_misspelled_event_type_once_types_are_declared() {
    let src = "event SysmonProcessCreate:\n    Image: str\n\nstream R = SysmonProcesCreate\n    .where(Image == 'x')\n    .emit(i: Image)\n";
    let err = Program::check(src).unwrap_err();
    assert!(
        matches!(&err, Error::Invalid(t) if t.contains("error[E033]") && t.contains("SysmonProcessCreate")),
        "{err}"
    );
}

#[test]
fn check_passes_a_program_that_declares_nothing() {
    Program::check("stream R = SysmonProcessCreate\n    .where(ends_with(lower(Image), '\\psexec.exe'))\n    .emit(i: Image)\n").unwrap();
}

#[test]
fn check_returns_warnings_without_failing() {
    let src = "event Auth:\n    ip: str\n    status: str\n\nstream Brute = Auth where status == \"failed\" as first\n    -> all Auth where status == \"failed\" as fails\n    -> Auth where status == \"success\" as ok\n    .within(30m)\n    .emit(ip: first.ip)\n";
    let warnings = Program::check_with_warnings(src).unwrap();
    assert!(
        warnings.iter().any(|w| w.contains("warning[W003]")),
        "{warnings:?}"
    );
}
