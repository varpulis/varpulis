//! Runtime values for VPL

use std::borrow::Cow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};

/// Type alias for IndexMap with FxBuildHasher for faster key lookups.
pub type FxIndexMap<K, V> = IndexMap<K, V, FxBuildHasher>;

/// Runtime value
///
/// The Array, Map, and Str variants are optimized for memory efficiency:
/// - Array and Map are boxed to reduce overall enum size
/// - Str uses `Box<str>` instead of String (16 bytes vs 24 bytes)
///   since string values are rarely mutated after creation
///
/// The Map variant uses FxBuildHasher for faster key lookups since map keys
/// are typically short strings.
///
/// Note: PartialEq is implemented manually to ensure consistency with Hash
/// for Float values (NaN equals NaN, -0.0 equals 0.0).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[derive(Default)]
pub enum Value {
    /// Null value (default).
    #[default]
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating-point number.
    Float(f64),
    /// Heap-allocated string (saves 8 bytes vs `String`).
    Str(Box<str>),
    /// Timestamp as nanoseconds since epoch.
    Timestamp(i64),
    /// Duration in nanoseconds.
    Duration(u64),
    /// Boxed array of values.
    Array(Box<Vec<Self>>),
    /// Boxed ordered map with `Arc<str>` keys.
    Map(Box<FxIndexMap<Arc<str>, Self>>),
}

/// Helper function to compare f64 values consistently with Hash impl.
/// Treats NaN == NaN and -0.0 == 0.0.
#[inline]
fn float_eq(a: f64, b: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        true
    } else if a == 0.0 && b == 0.0 {
        // Both are zero (handles -0.0 == 0.0)
        true
    } else {
        a == b
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => float_eq(*a, *b),
            (Self::Str(a), Self::Str(b)) => a == b,
            (Self::Timestamp(a), Self::Timestamp(b)) => a == b,
            (Self::Duration(a), Self::Duration(b)) => a == b,
            (Self::Array(a), Self::Array(b)) => a == b,
            (Self::Map(a), Self::Map(b)) => a == b,
            _ => false,
        }
    }
}

impl Value {
    /// Creates a new Array value from a Vec.
    #[inline]
    pub fn array(v: Vec<Self>) -> Self {
        Self::Array(Box::new(v))
    }

    /// Creates a new Map value from an IndexMap with `Arc<str>` keys.
    #[inline]
    pub fn map(m: FxIndexMap<Arc<str>, Self>) -> Self {
        Self::Map(Box::new(m))
    }

    /// Creates a new Map value from an IndexMap with String keys (converts to `Arc<str>`).
    #[inline]
    pub fn map_from_strings(m: FxIndexMap<String, Self>) -> Self {
        let converted: FxIndexMap<Arc<str>, Self> =
            m.into_iter().map(|(k, v)| (Arc::from(k), v)).collect();
        Self::Map(Box::new(converted))
    }

    /// Returns the type name as a static string (e.g., `"int"`, `"str"`).
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::Str(_) => "str",
            Self::Timestamp(_) => "timestamp",
            Self::Duration(_) => "duration",
            Self::Array(_) => "array",
            Self::Map(_) => "map",
        }
    }

    /// Returns whether this value is truthy (non-null, non-zero, non-empty).
    pub fn is_truthy(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Bool(b) => *b,
            Self::Int(n) => *n != 0,
            Self::Float(n) => *n != 0.0,
            Self::Str(s) => !s.is_empty(),
            Self::Array(a) => !a.is_empty(),
            Self::Map(m) => !m.is_empty(),
            Self::Timestamp(_) | Self::Duration(_) => true,
        }
    }

    /// Creates a Str value from any type that can be converted to a string slice.
    #[inline]
    pub fn str(s: impl AsRef<str>) -> Self {
        Self::Str(s.as_ref().into())
    }

    /// Extracts an integer value, coercing floats by truncation.
    pub const fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(n) => Some(*n),
            Self::Float(n) => Some(*n as i64),
            _ => None,
        }
    }

    /// Extracts a float value, coercing integers by widening.
    pub const fn as_float(&self) -> Option<f64> {
        match self {
            Self::Float(n) => Some(*n),
            Self::Int(n) => Some(*n as f64),
            _ => None,
        }
    }

    /// Extracts a string slice if this value is a `Str`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Extracts a boolean if this value is a `Bool`.
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Looks up a key in a Map value, returns `None` for non-Map values.
    pub fn get(&self, key: &str) -> Option<&Self> {
        match self {
            Self::Map(m) => m.get(key),
            _ => None,
        }
    }

    /// Accesses an element by index in an Array value, returns `None` for non-Array values.
    pub fn get_index(&self, idx: usize) -> Option<&Self> {
        match self {
            Self::Array(a) => a.get(idx),
            _ => None,
        }
    }

    /// Fast partition key extraction: returns `Cow` to avoid allocation for string keys.
    pub fn to_partition_key(&self) -> Cow<'_, str> {
        match self {
            Self::Str(s) => Cow::Borrowed(&**s),
            Self::Int(n) => Cow::Owned(n.to_string()),
            Self::Bool(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
            other => Cow::Owned(format!("{other}")),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(n) => write!(f, "{n}"),
            Self::Str(s) => write!(f, "\"{}\"", &**s),
            Self::Timestamp(ts) => {
                let dt = DateTime::<Utc>::from_timestamp_nanos(*ts);
                write!(f, "@{}", dt.format("%Y-%m-%dT%H:%M:%SZ"))
            }
            Self::Duration(d) => {
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
            Self::Array(a) => {
                write!(f, "[")?;
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            Self::Map(m) => {
                write!(f, "{{")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Self::Int(n)
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Self::Float(n)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self::Str(s.into_boxed_str())
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::Str(s.into())
    }
}

impl From<Box<str>> for Value {
    fn from(s: Box<str>) -> Self {
        Self::Str(s)
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash discriminant first to distinguish variants
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Bool(b) => b.hash(state),
            Self::Int(n) => n.hash(state),
            Self::Float(f) => {
                // Use bit representation for f64 hashing
                // Normalize -0.0 to 0.0 and handle NaN consistently
                let bits = if f.is_nan() {
                    f64::NAN.to_bits()
                } else if *f == 0.0 {
                    0u64
                } else {
                    f.to_bits()
                };
                bits.hash(state);
            }
            Self::Str(s) => s.hash(state),
            Self::Timestamp(ts) => ts.hash(state),
            Self::Duration(d) => d.hash(state),
            Self::Array(arr) => {
                arr.len().hash(state);
                for v in arr.iter() {
                    v.hash(state);
                }
            }
            Self::Map(map) => {
                map.len().hash(state);
                for (k, v) in map.iter() {
                    k.hash(state);
                    v.hash(state);
                }
            }
        }
    }
}

impl Eq for Value {}

impl<T: Into<Self>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Self {
        Self::array(v.into_iter().map(Into::into).collect())
    }
}

impl<T: Into<Self>> From<Option<T>> for Value {
    fn from(o: Option<T>) -> Self {
        match o {
            Some(v) => v.into(),
            None => Self::Null,
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
        assert_eq!(Value::str("hello").type_name(), "str");
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
        assert_eq!(Value::array(vec![]).type_name(), "array");
    }

    #[test]
    fn test_type_name_map() {
        assert_eq!(
            Value::map(IndexMap::with_hasher(FxBuildHasher)).type_name(),
            "map"
        );
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
        assert!(Value::str("hello").is_truthy());
        assert!(!Value::str("").is_truthy());
    }

    #[test]
    fn test_is_truthy_array() {
        assert!(Value::array(vec![Value::Int(1)]).is_truthy());
        assert!(!Value::array(vec![]).is_truthy());
    }

    #[test]
    fn test_is_truthy_map() {
        let mut m: FxIndexMap<Arc<str>, Value> = IndexMap::with_hasher(FxBuildHasher);
        m.insert("key".into(), Value::Int(1));
        assert!(Value::map(m).is_truthy());
        assert!(!Value::map(IndexMap::with_hasher(FxBuildHasher)).is_truthy());
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
        assert_eq!(Value::str("42").as_int(), None);
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
        assert_eq!(Value::str("3.14").as_float(), None);
    }

    #[test]
    fn test_as_str() {
        assert_eq!(Value::str("hello").as_str(), Some("hello"));
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
        let mut m: FxIndexMap<Arc<str>, Value> = IndexMap::with_hasher(FxBuildHasher);
        m.insert("key".into(), Value::Int(42));
        let v = Value::map(m);
        assert_eq!(v.get("key"), Some(&Value::Int(42)));
        assert_eq!(v.get("missing"), None);
    }

    #[test]
    fn test_get_from_non_map() {
        assert_eq!(Value::Int(42).get("key"), None);
    }

    #[test]
    fn test_get_index_from_array() {
        let v = Value::array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
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
        assert_eq!(format!("{}", Value::str("hello")), "\"hello\"");
    }

    #[test]
    fn test_display_array() {
        let v = Value::array(vec![Value::Int(1), Value::Int(2)]);
        assert_eq!(format!("{v}"), "[1, 2]");
    }

    #[test]
    fn test_display_empty_array() {
        assert_eq!(format!("{}", Value::array(vec![])), "[]");
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
        assert_eq!(v, Value::str("hello"));
    }

    #[test]
    fn test_from_str_ref() {
        let v: Value = "hello".into();
        assert_eq!(v, Value::str("hello"));
    }

    #[test]
    fn test_from_vec() {
        let v: Value = vec![1i64, 2i64, 3i64].into();
        assert_eq!(
            v,
            Value::array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
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

    // ==========================================================================
    // Hash Tests
    // ==========================================================================

    #[test]
    fn test_hash_equal_values_same_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        // Same values should have same hash
        assert_eq!(hash_value(&Value::Int(42)), hash_value(&Value::Int(42)));
        assert_eq!(
            hash_value(&Value::str("hello")),
            hash_value(&Value::str("hello"))
        );
        assert_eq!(
            hash_value(&Value::Float(42.5)),
            hash_value(&Value::Float(42.5))
        );
    }

    #[test]
    fn test_hash_different_values_different_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        // Different values should (likely) have different hash
        assert_ne!(hash_value(&Value::Int(1)), hash_value(&Value::Int(2)));
        assert_ne!(hash_value(&Value::Int(0)), hash_value(&Value::str("0")));
    }

    #[test]
    fn test_hash_float_zero_normalized() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        // -0.0 and 0.0 should hash the same
        assert_eq!(
            hash_value(&Value::Float(0.0)),
            hash_value(&Value::Float(-0.0))
        );
    }

    #[test]
    fn test_hash_nan_consistent() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        // All NaN values should hash the same
        assert_eq!(
            hash_value(&Value::Float(f64::NAN)),
            hash_value(&Value::Float(f64::NAN))
        );
    }

    #[test]
    fn test_hash_in_hashset() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(Value::Int(1));
        set.insert(Value::Int(2));
        set.insert(Value::Int(1)); // Duplicate

        assert_eq!(set.len(), 2);

        set.insert(Value::str("hello"));
        set.insert(Value::str("hello")); // Duplicate

        assert_eq!(set.len(), 3);
    }

    // ==========================================================================
    // PartialEq Consistency Tests (VAL-02)
    // ==========================================================================

    #[test]
    fn test_eq_nan_equals_nan() {
        // NaN == NaN should be true (consistent with Hash)
        assert_eq!(Value::Float(f64::NAN), Value::Float(f64::NAN));
    }

    #[test]
    fn test_eq_zero_equals_negative_zero() {
        // 0.0 == -0.0 should be true (consistent with Hash)
        assert_eq!(Value::Float(0.0), Value::Float(-0.0));
    }

    #[test]
    fn test_eq_regular_floats() {
        assert_eq!(Value::Float(1.5), Value::Float(1.5));
        assert_ne!(Value::Float(1.5), Value::Float(2.5));
    }

    #[test]
    fn test_nan_in_hashset() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(Value::Float(f64::NAN));
        set.insert(Value::Float(f64::NAN)); // Should be deduplicated

        // Should only have one NaN value since they're now equal
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_zero_in_hashset() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(Value::Float(0.0));
        set.insert(Value::Float(-0.0)); // Should be deduplicated

        // Should only have one zero value
        assert_eq!(set.len(), 1);
    }

    // ==========================================================================
    // Partition Key Tests
    // ==========================================================================

    #[test]
    fn test_partition_key_str_borrows() {
        let v = Value::str("device-42");
        let key = v.to_partition_key();
        assert_eq!(&*key, "device-42");
        // Str variant should borrow, not allocate
        assert!(matches!(key, Cow::Borrowed(_)));
    }

    #[test]
    fn test_partition_key_int_owned() {
        let v = Value::Int(99);
        let key = v.to_partition_key();
        assert_eq!(&*key, "99");
        assert!(matches!(key, Cow::Owned(_)));
    }

    #[test]
    fn test_partition_key_bool_borrows() {
        assert_eq!(&*Value::Bool(true).to_partition_key(), "true");
        assert_eq!(&*Value::Bool(false).to_partition_key(), "false");
        assert!(matches!(
            Value::Bool(true).to_partition_key(),
            Cow::Borrowed(_)
        ));
    }

    // ==========================================================================
    // Map Construction Helper Tests
    // ==========================================================================

    #[test]
    fn test_map_from_strings_converts_keys() {
        let mut m: FxIndexMap<String, Value> = IndexMap::with_hasher(FxBuildHasher);
        m.insert("alpha".to_string(), Value::Int(1));
        m.insert("beta".to_string(), Value::Int(2));
        let v = Value::map_from_strings(m);
        assert_eq!(v.get("alpha"), Some(&Value::Int(1)));
        assert_eq!(v.get("beta"), Some(&Value::Int(2)));
        assert_eq!(v.type_name(), "map");
    }

    #[test]
    fn test_cross_variant_inequality() {
        // Different variant types should never be equal, even if inner values
        // could be coerced (Int vs Float is handled, but Int vs Str is not)
        assert_ne!(Value::Int(0), Value::Bool(false));
        assert_ne!(Value::Int(0), Value::Null);
        assert_ne!(Value::Str("1".into()), Value::Int(1));
    }
}
