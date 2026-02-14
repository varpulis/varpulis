//! Coverage tests for varpulis_core validate module types and formatting.

use varpulis_core::validate::{diagnostic_position, Severity};

#[test]
fn diagnostic_position_first_line() {
    let source = "line1\nline2\nline3";
    let (line, col) = diagnostic_position(source, 0);
    assert_eq!(line, 1);
    assert_eq!(col, 1);
}

#[test]
fn diagnostic_position_second_line() {
    let source = "line1\nline2\nline3";
    let (line, col) = diagnostic_position(source, 6);
    assert_eq!(line, 2);
    assert_eq!(col, 1);
}

#[test]
fn diagnostic_position_middle_of_line() {
    let source = "line1\nline2\nline3";
    let (line, col) = diagnostic_position(source, 8);
    assert_eq!(line, 2);
    assert_eq!(col, 3);
}

#[test]
fn diagnostic_position_third_line() {
    let source = "line1\nline2\nline3";
    let (line, col) = diagnostic_position(source, 12);
    assert_eq!(line, 3);
    assert_eq!(col, 1);
}

#[test]
fn diagnostic_position_end_of_source() {
    let source = "ab\ncd";
    let (line, col) = diagnostic_position(source, 5);
    assert_eq!(line, 2);
    assert_eq!(col, 3);
}

#[test]
fn severity_variants_exist() {
    let _e = Severity::Error;
    let _w = Severity::Warning;
    assert_ne!(Severity::Error, Severity::Warning);
}
