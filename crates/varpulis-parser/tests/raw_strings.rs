//! Single-quoted strings are raw, the way Sigma's YAML is.
//!
//! A double-quoted literal keeps every backslash but treats `\"` as a pair, so
//! it cannot end in a backslash: `"\Temp\"` never closes. Windows paths end in
//! one all the time (`'\AppData\Local\Temp\'` is in hundreds of Sigma rules),
//! so a rule ported from Sigma could not say what the rule says. A
//! single-quoted literal has no escapes at all, and `''` stands for one quote.

use varpulis_core::ast::{Expr, Stmt};
use varpulis_parser::parse;

fn const_value(src: &str) -> Expr {
    let program = parse(src).unwrap_or_else(|e| panic!("{src}: {e}"));
    match &program.statements[0].node {
        Stmt::ConstDecl { value, .. } => value.clone(),
        other => panic!("expected a const, got {other:?}"),
    }
}

fn string_of(literal: &str) -> String {
    match const_value(&format!("const S = {literal}")) {
        Expr::Str(s) => s,
        other => panic!("{literal} is not a string: {other:?}"),
    }
}

#[test]
fn a_single_quoted_string_may_end_in_a_backslash() {
    assert_eq!(string_of(r"'\Temp\'"), r"\Temp\");
    assert_eq!(string_of(r"'C:\Windows\'"), r"C:\Windows\");
    assert_eq!(string_of(r"'\\'"), r"\\");
}

#[test]
fn nothing_escapes_in_a_single_quoted_string() {
    assert_eq!(string_of(r"'\tools\new'"), r"\tools\new");
    assert_eq!(string_of(r#"'say "hi"'"#), r#"say "hi""#);
    assert_eq!(string_of("'it''s'"), "it's");
    assert_eq!(string_of("''''"), "'");
    assert_eq!(string_of("''"), "");
}

#[test]
fn a_double_quoted_string_is_unchanged() {
    assert_eq!(string_of(r#""\PsExec.exe""#), r"\PsExec.exe");
    assert_eq!(string_of(r#""a\"b""#), r#"a\"b"#);
    assert_eq!(string_of(r#""it's""#), "it's");
}

#[test]
fn brackets_and_hashes_inside_a_single_quoted_string_are_text() {
    assert_eq!(
        string_of("'((((( # not a comment'"),
        "((((( # not a comment"
    );
    let src = "stream R = T\n    .where(contains(lower(Image), '\\temp\\') or Image == 'a#b')\n    .emit(image: Image)\n";
    parse(src).unwrap_or_else(|e| panic!("{e}"));
}
