//! Runtime values for VarpulisQL

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

/// Runtime value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
#[derive(Default)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Timestamp(i64), // nanoseconds since epoch
    Duration(u64),  // nanoseconds
    Array(Vec<Value>),
    Map(IndexMap<String, Value>),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::Str(_) => "str",
            Value::Timestamp(_) => "timestamp",
            Value::Duration(_) => "duration",
            Value::Array(_) => "array",
            Value::Map(_) => "map",
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Int(n) => *n != 0,
            Value::Float(n) => *n != 0.0,
            Value::Str(s) => !s.is_empty(),
            Value::Array(a) => !a.is_empty(),
            Value::Map(m) => !m.is_empty(),
            Value::Timestamp(_) | Value::Duration(_) => true,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            Value::Float(n) => Some(*n as i64),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(n) => Some(*n),
            Value::Int(n) => Some(*n as f64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Map(m) => m.get(key),
            _ => None,
        }
    }

    pub fn get_index(&self, idx: usize) -> Option<&Value> {
        match self {
            Value::Array(a) => a.get(idx),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(n) => write!(f, "{}", n),
            Value::Float(n) => write!(f, "{}", n),
            Value::Str(s) => write!(f, "\"{}\"", s),
            Value::Timestamp(ts) => {
                let dt = DateTime::<Utc>::from_timestamp_nanos(*ts);
                write!(f, "@{}", dt.format("%Y-%m-%dT%H:%M:%SZ"))
            }
            Value::Duration(d) => {
                let dur = Duration::from_nanos(*d);
                if dur.as_secs() >= 86400 {
                    write!(f, "{}d", dur.as_secs() / 86400)
                } else if dur.as_secs() >= 3600 {
                    write!(f, "{}h", dur.as_secs() / 3600)
                } else if dur.as_secs() >= 60 {
                    write!(f, "{}m", dur.as_secs() / 60)
                } else if dur.as_secs() > 0 {
                    write!(f, "{}s", dur.as_secs())
                } else if dur.as_millis() > 0 {
                    write!(f, "{}ms", dur.as_millis())
                } else {
                    write!(f, "{}us", dur.as_micros())
                }
            }
            Value::Array(a) => {
                write!(f, "[")?;
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            Value::Map(m) => {
                write!(f, "{{")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", k, v)?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int(n)
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Float(n)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Str(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Str(s.to_string())
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Self {
        Value::Array(v.into_iter().map(Into::into).collect())
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(o: Option<T>) -> Self {
        match o {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==========================================================================
    // Type Name Tests
    // ==========================================================================

    #[test]
    fn test_type_name_null() {
        assert_eq!(Value::Null.type_name(), "null");
    }

    #[test]
    fn test_type_name_bool() {
        assert_eq!(Value::Bool(true).type_name(), "bool");
    }

    #[test]
    fn test_type_name_int() {
        assert_eq!(Value::Int(42).type_name(), "int");
    }

    #[test]
    fn test_type_name_float() {
        assert_eq!(Value::Float(2.5).type_name(), "float");
    }

    #[test]
    fn test_type_name_str() {
        assert_eq!(Value::Str("hello".to_string()).type_name(), "str");
    }

    #[test]
    fn test_type_name_timestamp() {
        assert_eq!(Value::Timestamp(0).type_name(), "timestamp");
    }

    #[test]
    fn test_type_name_duration() {
        assert_eq!(Value::Duration(1000).type_name(), "duration");
    }

    #[test]
    fn test_type_name_array() {
        assert_eq!(Value::Array(vec![]).type_name(), "array");
    }

    #[test]
    fn test_type_name_map() {
        assert_eq!(Value::Map(IndexMap::new()).type_name(), "map");
    }

    // ==========================================================================
    // Truthiness Tests
    // ==========================================================================

    #[test]
    fn test_is_truthy_null() {
        assert!(!Value::Null.is_truthy());
    }

    #[test]
    fn test_is_truthy_bool() {
        assert!(Value::Bool(true).is_truthy());
        assert!(!Value::Bool(false).is_truthy());
    }

    #[test]
    fn test_is_truthy_int() {
        assert!(Value::Int(1).is_truthy());
        assert!(Value::Int(-1).is_truthy());
        assert!(!Value::Int(0).is_truthy());
    }

    #[test]
    fn test_is_truthy_float() {
        assert!(Value::Float(0.1).is_truthy());
        assert!(!Value::Float(0.0).is_truthy());
    }

    #[test]
    fn test_is_truthy_str() {
        assert!(Value::Str("hello".to_string()).is_truthy());
        assert!(!Value::Str("".to_string()).is_truthy());
    }

    #[test]
    fn test_is_truthy_array() {
        assert!(Value::Array(vec![Value::Int(1)]).is_truthy());
        assert!(!Value::Array(vec![]).is_truthy());
    }

    #[test]
    fn test_is_truthy_map() {
        let mut m = IndexMap::new();
        m.insert("key".to_string(), Value::Int(1));
        assert!(Value::Map(m).is_truthy());
        assert!(!Value::Map(IndexMap::new()).is_truthy());
    }

    #[test]
    fn test_is_truthy_timestamp_always_true() {
        assert!(Value::Timestamp(0).is_truthy());
        assert!(Value::Timestamp(1234567890).is_truthy());
    }

    #[test]
    fn test_is_truthy_duration_always_true() {
        assert!(Value::Duration(0).is_truthy());
        assert!(Value::Duration(1000).is_truthy());
    }

    // ==========================================================================
    // Conversion Tests
    // ==========================================================================

    #[test]
    fn test_as_int_from_int() {
        assert_eq!(Value::Int(42).as_int(), Some(42));
    }

    #[test]
    fn test_as_int_from_float() {
        assert_eq!(Value::Float(3.7).as_int(), Some(3));
    }

    #[test]
    fn test_as_int_from_other() {
        assert_eq!(Value::Str("42".to_string()).as_int(), None);
        assert_eq!(Value::Null.as_int(), None);
    }

    #[test]
    fn test_as_float_from_float() {
        assert_eq!(Value::Float(2.5).as_float(), Some(2.5));
    }

    #[test]
    fn test_as_float_from_int() {
        assert_eq!(Value::Int(42).as_float(), Some(42.0));
    }

    #[test]
    fn test_as_float_from_other() {
        assert_eq!(Value::Str("3.14".to_string()).as_float(), None);
    }

    #[test]
    fn test_as_str() {
        assert_eq!(Value::Str("hello".to_string()).as_str(), Some("hello"));
        assert_eq!(Value::Int(42).as_str(), None);
    }

    #[test]
    fn test_as_bool() {
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Bool(false).as_bool(), Some(false));
        assert_eq!(Value::Int(1).as_bool(), None);
    }

    // ==========================================================================
    // Collection Access Tests
    // ==========================================================================

    #[test]
    fn test_get_from_map() {
        let mut m = IndexMap::new();
        m.insert("key".to_string(), Value::Int(42));
        let v = Value::Map(m);
        assert_eq!(v.get("key"), Some(&Value::Int(42)));
        assert_eq!(v.get("missing"), None);
    }

    #[test]
    fn test_get_from_non_map() {
        assert_eq!(Value::Int(42).get("key"), None);
    }

    #[test]
    fn test_get_index_from_array() {
        let v = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        assert_eq!(v.get_index(0), Some(&Value::Int(1)));
        assert_eq!(v.get_index(2), Some(&Value::Int(3)));
        assert_eq!(v.get_index(5), None);
    }

    #[test]
    fn test_get_index_from_non_array() {
        assert_eq!(Value::Int(42).get_index(0), None);
    }

    // ==========================================================================
    // Display Tests
    // ==========================================================================

    #[test]
    fn test_display_null() {
        assert_eq!(format!("{}", Value::Null), "null");
    }

    #[test]
    fn test_display_bool() {
        assert_eq!(format!("{}", Value::Bool(true)), "true");
        assert_eq!(format!("{}", Value::Bool(false)), "false");
    }

    #[test]
    fn test_display_int() {
        assert_eq!(format!("{}", Value::Int(42)), "42");
        assert_eq!(format!("{}", Value::Int(-100)), "-100");
    }

    #[test]
    fn test_display_float() {
        assert_eq!(format!("{}", Value::Float(2.75)), "2.75");
    }

    #[test]
    fn test_display_str() {
        assert_eq!(format!("{}", Value::Str("hello".to_string())), "\"hello\"");
    }

    #[test]
    fn test_display_array() {
        let v = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        assert_eq!(format!("{}", v), "[1, 2]");
    }

    #[test]
    fn test_display_empty_array() {
        assert_eq!(format!("{}", Value::Array(vec![])), "[]");
    }

    #[test]
    fn test_display_duration_seconds() {
        assert_eq!(format!("{}", Value::Duration(5_000_000_000)), "5s");
    }

    #[test]
    fn test_display_duration_minutes() {
        assert_eq!(format!("{}", Value::Duration(120_000_000_000)), "2m");
    }

    #[test]
    fn test_display_duration_hours() {
        assert_eq!(format!("{}", Value::Duration(7_200_000_000_000)), "2h");
    }

    #[test]
    fn test_display_duration_days() {
        assert_eq!(format!("{}", Value::Duration(172_800_000_000_000)), "2d");
    }

    #[test]
    fn test_display_duration_milliseconds() {
        assert_eq!(format!("{}", Value::Duration(500_000_000)), "500ms");
    }

    #[test]
    fn test_display_duration_microseconds() {
        assert_eq!(format!("{}", Value::Duration(500_000)), "500us");
    }

    // ==========================================================================
    // From Trait Tests
    // ==========================================================================

    #[test]
    fn test_from_bool() {
        let v: Value = true.into();
        assert_eq!(v, Value::Bool(true));
    }

    #[test]
    fn test_from_i64() {
        let v: Value = 42i64.into();
        assert_eq!(v, Value::Int(42));
    }

    #[test]
    fn test_from_f64() {
        let v: Value = 2.5f64.into();
        assert_eq!(v, Value::Float(2.5));
    }

    #[test]
    fn test_from_string() {
        let v: Value = "hello".to_string().into();
        assert_eq!(v, Value::Str("hello".to_string()));
    }

    #[test]
    fn test_from_str_ref() {
        let v: Value = "hello".into();
        assert_eq!(v, Value::Str("hello".to_string()));
    }

    #[test]
    fn test_from_vec() {
        let v: Value = vec![1i64, 2i64, 3i64].into();
        assert_eq!(
            v,
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
    }

    #[test]
    fn test_from_option_some() {
        let v: Value = Some(42i64).into();
        assert_eq!(v, Value::Int(42));
    }

    #[test]
    fn test_from_option_none() {
        let v: Value = Option::<i64>::None.into();
        assert_eq!(v, Value::Null);
    }

    #[test]
    fn test_default() {
        let v: Value = Default::default();
        assert_eq!(v, Value::Null);
    }
}
