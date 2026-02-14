//! Coverage tests for varpulis_core types, span, and AST Display impls.

use varpulis_core::span::{Span, Spanned};
use varpulis_core::types::Type;

// =============================================================================
// Type Tests
// =============================================================================

#[test]
fn type_is_numeric_int() {
    assert!(Type::Int.is_numeric());
}

#[test]
fn type_is_numeric_float() {
    assert!(Type::Float.is_numeric());
}

#[test]
fn type_is_numeric_false_for_str() {
    assert!(!Type::Str.is_numeric());
}

#[test]
fn type_is_numeric_false_for_bool() {
    assert!(!Type::Bool.is_numeric());
}

#[test]
fn type_is_optional() {
    assert!(Type::Optional(Box::new(Type::Int)).is_optional());
}

#[test]
fn type_is_optional_false_for_plain() {
    assert!(!Type::Int.is_optional());
}

#[test]
fn type_inner_type_array() {
    let t = Type::Array(Box::new(Type::Str));
    assert_eq!(t.inner_type(), Some(&Type::Str));
}

#[test]
fn type_inner_type_optional() {
    let t = Type::Optional(Box::new(Type::Float));
    assert_eq!(t.inner_type(), Some(&Type::Float));
}

#[test]
fn type_inner_type_stream() {
    let t = Type::Stream(Box::new(Type::Int));
    assert_eq!(t.inner_type(), Some(&Type::Int));
}

#[test]
fn type_inner_type_none_for_plain() {
    assert_eq!(Type::Int.inner_type(), None);
    assert_eq!(Type::Str.inner_type(), None);
    assert_eq!(Type::Bool.inner_type(), None);
}

// =============================================================================
// Type Display Tests
// =============================================================================

#[test]
fn type_display_int() {
    assert_eq!(format!("{}", Type::Int), "int");
}

#[test]
fn type_display_float() {
    assert_eq!(format!("{}", Type::Float), "float");
}

#[test]
fn type_display_bool() {
    assert_eq!(format!("{}", Type::Bool), "bool");
}

#[test]
fn type_display_str() {
    assert_eq!(format!("{}", Type::Str), "str");
}

#[test]
fn type_display_timestamp() {
    assert_eq!(format!("{}", Type::Timestamp), "timestamp");
}

#[test]
fn type_display_duration() {
    assert_eq!(format!("{}", Type::Duration), "duration");
}

#[test]
fn type_display_null() {
    assert_eq!(format!("{}", Type::Null), "null");
}

#[test]
fn type_display_array() {
    let t = Type::Array(Box::new(Type::Int));
    assert_eq!(format!("{}", t), "[int]");
}

#[test]
fn type_display_map() {
    let t = Type::Map(Box::new(Type::Str), Box::new(Type::Int));
    assert_eq!(format!("{}", t), "{str: int}");
}

#[test]
fn type_display_tuple() {
    let t = Type::Tuple(vec![Type::Int, Type::Str, Type::Bool]);
    assert_eq!(format!("{}", t), "(int, str, bool)");
}

#[test]
fn type_display_tuple_single() {
    let t = Type::Tuple(vec![Type::Float]);
    assert_eq!(format!("{}", t), "(float)");
}

#[test]
fn type_display_optional() {
    let t = Type::Optional(Box::new(Type::Str));
    assert_eq!(format!("{}", t), "str?");
}

#[test]
fn type_display_stream() {
    let t = Type::Stream(Box::new(Type::Int));
    assert_eq!(format!("{}", t), "Stream<int>");
}

#[test]
fn type_display_named() {
    let t = Type::Named("MyEvent".to_string());
    assert_eq!(format!("{}", t), "MyEvent");
}

#[test]
fn type_display_record() {
    let t = Type::Record(vec![
        ("x".to_string(), Type::Int),
        ("y".to_string(), Type::Float),
    ]);
    assert_eq!(format!("{}", t), "{x: int, y: float}");
}

#[test]
fn type_display_any() {
    assert_eq!(format!("{}", Type::Any), "any");
}

#[test]
fn type_display_unknown() {
    assert_eq!(format!("{}", Type::Unknown), "?");
}

#[test]
fn type_display_nested() {
    let t = Type::Array(Box::new(Type::Optional(Box::new(Type::Named(
        "Foo".to_string(),
    )))));
    assert_eq!(format!("{}", t), "[Foo?]");
}

// =============================================================================
// Span Tests
// =============================================================================

#[test]
fn span_new() {
    let s = Span::new(10, 20);
    assert_eq!(s.start, 10);
    assert_eq!(s.end, 20);
}

#[test]
fn span_dummy() {
    let s = Span::dummy();
    assert_eq!(s.start, 0);
    assert_eq!(s.end, 0);
}

#[test]
fn span_default() {
    let s = Span::default();
    assert_eq!(s.start, 0);
    assert_eq!(s.end, 0);
}

#[test]
fn span_merge() {
    let a = Span::new(5, 15);
    let b = Span::new(10, 25);
    let merged = a.merge(b);
    assert_eq!(merged.start, 5);
    assert_eq!(merged.end, 25);
}

#[test]
fn span_merge_disjoint() {
    let a = Span::new(0, 5);
    let b = Span::new(20, 30);
    let merged = a.merge(b);
    assert_eq!(merged.start, 0);
    assert_eq!(merged.end, 30);
}

#[test]
fn span_merge_same() {
    let a = Span::new(10, 20);
    let merged = a.merge(a);
    assert_eq!(merged.start, 10);
    assert_eq!(merged.end, 20);
}

#[test]
fn span_equality() {
    assert_eq!(Span::new(1, 5), Span::new(1, 5));
    assert_ne!(Span::new(1, 5), Span::new(1, 6));
}

// =============================================================================
// Spanned Tests
// =============================================================================

#[test]
fn spanned_new() {
    let s = Spanned::new(42, Span::new(0, 2));
    assert_eq!(s.node, 42);
    assert_eq!(s.span.start, 0);
    assert_eq!(s.span.end, 2);
}

#[test]
fn spanned_dummy() {
    let s = Spanned::dummy("hello");
    assert_eq!(s.node, "hello");
    assert_eq!(s.span, Span::dummy());
}

#[test]
fn spanned_map() {
    let s = Spanned::new(10, Span::new(5, 15));
    let mapped = s.map(|x| x * 2);
    assert_eq!(mapped.node, 20);
    assert_eq!(mapped.span, Span::new(5, 15));
}

#[test]
fn spanned_map_type_change() {
    let s = Spanned::new(42, Span::new(0, 1));
    let mapped = s.map(|x| format!("{}", x));
    assert_eq!(mapped.node, "42");
}

#[test]
fn spanned_equality() {
    let a = Spanned::new(1, Span::new(0, 1));
    let b = Spanned::new(1, Span::new(0, 1));
    assert_eq!(a, b);
}

#[test]
fn spanned_inequality_node() {
    let a = Spanned::new(1, Span::new(0, 1));
    let b = Spanned::new(2, Span::new(0, 1));
    assert_ne!(a, b);
}

#[test]
fn spanned_inequality_span() {
    let a = Spanned::new(1, Span::new(0, 1));
    let b = Spanned::new(1, Span::new(0, 2));
    assert_ne!(a, b);
}

// =============================================================================
// BinOp::as_str Tests
// =============================================================================

use varpulis_core::ast::BinOp;

#[test]
fn binop_as_str_arithmetic() {
    assert_eq!(BinOp::Add.as_str(), "+");
    assert_eq!(BinOp::Sub.as_str(), "-");
    assert_eq!(BinOp::Mul.as_str(), "*");
    assert_eq!(BinOp::Div.as_str(), "/");
    assert_eq!(BinOp::Mod.as_str(), "%");
    assert_eq!(BinOp::Pow.as_str(), "**");
}

#[test]
fn binop_as_str_comparison() {
    assert_eq!(BinOp::Eq.as_str(), "==");
    assert_eq!(BinOp::NotEq.as_str(), "!=");
    assert_eq!(BinOp::Lt.as_str(), "<");
    assert_eq!(BinOp::Le.as_str(), "<=");
    assert_eq!(BinOp::Gt.as_str(), ">");
    assert_eq!(BinOp::Ge.as_str(), ">=");
    assert_eq!(BinOp::In.as_str(), "in");
    assert_eq!(BinOp::NotIn.as_str(), "not in");
    assert_eq!(BinOp::Is.as_str(), "is");
}

#[test]
fn binop_as_str_logical() {
    assert_eq!(BinOp::And.as_str(), "and");
    assert_eq!(BinOp::Or.as_str(), "or");
    assert_eq!(BinOp::Xor.as_str(), "xor");
}

#[test]
fn binop_as_str_pattern() {
    assert_eq!(BinOp::FollowedBy.as_str(), "->");
}

#[test]
fn binop_as_str_bitwise() {
    assert_eq!(BinOp::BitAnd.as_str(), "&");
    assert_eq!(BinOp::BitOr.as_str(), "|");
    assert_eq!(BinOp::BitXor.as_str(), "^");
    assert_eq!(BinOp::Shl.as_str(), "<<");
    assert_eq!(BinOp::Shr.as_str(), ">>");
}

// =============================================================================
// UnaryOp::as_str Tests
// =============================================================================

use varpulis_core::ast::UnaryOp;

#[test]
fn unaryop_as_str_all() {
    assert_eq!(UnaryOp::Neg.as_str(), "-");
    assert_eq!(UnaryOp::Not.as_str(), "not");
    assert_eq!(UnaryOp::BitNot.as_str(), "~");
}

// =============================================================================
// ConfigValue accessor Tests
// =============================================================================

use varpulis_core::ast::ConfigValue;

#[test]
fn config_value_as_string_str() {
    let v = ConfigValue::Str("hello".to_string());
    assert_eq!(v.as_string(), Some("hello"));
}

#[test]
fn config_value_as_string_ident() {
    let v = ConfigValue::Ident("my_ident".to_string());
    assert_eq!(v.as_string(), Some("my_ident"));
}

#[test]
fn config_value_as_string_non_string() {
    let v = ConfigValue::Int(42);
    assert_eq!(v.as_string(), None);
    let v2 = ConfigValue::Bool(true);
    assert_eq!(v2.as_string(), None);
}

#[test]
fn config_value_as_int_int() {
    let v = ConfigValue::Int(99);
    assert_eq!(v.as_int(), Some(99));
}

#[test]
fn config_value_as_int_float() {
    let v = ConfigValue::Float(3.7);
    assert_eq!(v.as_int(), Some(3));
}

#[test]
fn config_value_as_int_non_numeric() {
    let v = ConfigValue::Str("hello".to_string());
    assert_eq!(v.as_int(), None);
    let v2 = ConfigValue::Bool(false);
    assert_eq!(v2.as_int(), None);
}

#[test]
fn config_value_as_float_float() {
    let v = ConfigValue::Float(2.5);
    assert_eq!(v.as_float(), Some(2.5));
}

#[test]
fn config_value_as_float_int() {
    let v = ConfigValue::Int(10);
    assert_eq!(v.as_float(), Some(10.0));
}

#[test]
fn config_value_as_float_non_numeric() {
    let v = ConfigValue::Str("nope".to_string());
    assert_eq!(v.as_float(), None);
}

#[test]
fn config_value_as_bool_true() {
    let v = ConfigValue::Bool(true);
    assert_eq!(v.as_bool(), Some(true));
}

#[test]
fn config_value_as_bool_false() {
    let v = ConfigValue::Bool(false);
    assert_eq!(v.as_bool(), Some(false));
}

#[test]
fn config_value_as_bool_non_bool() {
    let v = ConfigValue::Int(1);
    assert_eq!(v.as_bool(), None);
    let v2 = ConfigValue::Str("true".to_string());
    assert_eq!(v2.as_bool(), None);
}

// =============================================================================
// Value Display Tests
// =============================================================================

use varpulis_core::Value;

#[test]
fn value_display_null() {
    assert_eq!(format!("{}", Value::Null), "null");
}

#[test]
fn value_display_bool_true() {
    assert_eq!(format!("{}", Value::Bool(true)), "true");
}

#[test]
fn value_display_bool_false() {
    assert_eq!(format!("{}", Value::Bool(false)), "false");
}

#[test]
fn value_display_int() {
    assert_eq!(format!("{}", Value::Int(42)), "42");
}

#[test]
fn value_display_int_negative() {
    assert_eq!(format!("{}", Value::Int(-100)), "-100");
}

#[test]
fn value_display_float() {
    assert_eq!(format!("{}", Value::Float(3.125)), "3.125");
}

#[test]
fn value_display_str() {
    assert_eq!(format!("{}", Value::str("hello")), "\"hello\"");
}

#[test]
fn value_display_duration_days() {
    // 2 days in nanoseconds
    let nanos = 2 * 86400 * 1_000_000_000u64;
    assert_eq!(format!("{}", Value::Duration(nanos)), "2d");
}

#[test]
fn value_display_duration_hours() {
    let nanos = 3 * 3600 * 1_000_000_000u64;
    assert_eq!(format!("{}", Value::Duration(nanos)), "3h");
}

#[test]
fn value_display_duration_minutes() {
    let nanos = 5 * 60 * 1_000_000_000u64;
    assert_eq!(format!("{}", Value::Duration(nanos)), "5m");
}

#[test]
fn value_display_duration_seconds() {
    let nanos = 10 * 1_000_000_000u64;
    assert_eq!(format!("{}", Value::Duration(nanos)), "10s");
}

#[test]
fn value_display_duration_milliseconds() {
    let nanos = 500 * 1_000_000u64;
    assert_eq!(format!("{}", Value::Duration(nanos)), "500ms");
}

#[test]
fn value_display_duration_microseconds() {
    let nanos = 250u64;
    // 250ns => 0us, but let's use a value that is in microseconds
    let nanos2 = 750 * 1_000u64;
    assert_eq!(format!("{}", Value::Duration(nanos2)), "750us");
    // Zero duration
    assert_eq!(format!("{}", Value::Duration(nanos)), "0us");
}

#[test]
fn value_display_array_empty() {
    assert_eq!(format!("{}", Value::array(vec![])), "[]");
}

#[test]
fn value_display_array_single() {
    assert_eq!(format!("{}", Value::array(vec![Value::Int(1)])), "[1]");
}

#[test]
fn value_display_array_multiple() {
    assert_eq!(
        format!(
            "{}",
            Value::array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        ),
        "[1, 2, 3]"
    );
}

#[test]
fn value_display_map_empty() {
    use indexmap::IndexMap;
    use varpulis_core::value::FxIndexMap;
    let m: FxIndexMap<std::sync::Arc<str>, Value> = IndexMap::default();
    assert_eq!(format!("{}", Value::map(m)), "{}");
}

#[test]
fn value_display_map_single() {
    use indexmap::IndexMap;
    use varpulis_core::value::FxIndexMap;
    let mut m: FxIndexMap<std::sync::Arc<str>, Value> = IndexMap::default();
    m.insert(std::sync::Arc::from("key"), Value::Int(42));
    assert_eq!(format!("{}", Value::map(m)), "{key: 42}");
}

#[test]
fn value_display_timestamp() {
    // 2024-01-01T00:00:00Z in nanoseconds
    let ts_nanos = 1_704_067_200_000_000_000i64;
    let display = format!("{}", Value::Timestamp(ts_nanos));
    assert!(display.starts_with("@2024-01-01T00:00:00Z"));
}
