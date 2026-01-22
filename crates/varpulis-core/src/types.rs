//! Type system for VarpulisQL

use serde::{Deserialize, Serialize};
use std::fmt;

/// VarpulisQL type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Type {
    /// 64-bit signed integer
    Int,
    /// 64-bit floating point
    Float,
    /// Boolean
    Bool,
    /// UTF-8 string
    Str,
    /// Timestamp (nanoseconds since epoch)
    Timestamp,
    /// Duration
    Duration,
    /// Null type
    Null,
    /// Array of elements
    Array(Box<Type>),
    /// Map from key to value
    Map(Box<Type>, Box<Type>),
    /// Tuple of types
    Tuple(Vec<Type>),
    /// Optional type (T?)
    Optional(Box<Type>),
    /// Stream of events
    Stream(Box<Type>),
    /// Named type (event or type alias)
    Named(String),
    /// Record type (inline struct)
    Record(Vec<(String, Type)>),
    /// Any type (for polymorphic functions)
    Any,
    /// Unknown type (for type inference)
    Unknown,
}

impl Type {
    pub fn is_numeric(&self) -> bool {
        matches!(self, Type::Int | Type::Float)
    }

    pub fn is_optional(&self) -> bool {
        matches!(self, Type::Optional(_))
    }

    pub fn inner_type(&self) -> Option<&Type> {
        match self {
            Type::Array(t) | Type::Optional(t) | Type::Stream(t) => Some(t),
            _ => None,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Int => write!(f, "int"),
            Type::Float => write!(f, "float"),
            Type::Bool => write!(f, "bool"),
            Type::Str => write!(f, "str"),
            Type::Timestamp => write!(f, "timestamp"),
            Type::Duration => write!(f, "duration"),
            Type::Null => write!(f, "null"),
            Type::Array(t) => write!(f, "[{}]", t),
            Type::Map(k, v) => write!(f, "{{{}: {}}}", k, v),
            Type::Tuple(types) => {
                write!(f, "(")?;
                for (i, t) in types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", t)?;
                }
                write!(f, ")")
            }
            Type::Optional(t) => write!(f, "{}?", t),
            Type::Stream(t) => write!(f, "Stream<{}>", t),
            Type::Named(name) => write!(f, "{}", name),
            Type::Record(fields) => {
                write!(f, "{{")?;
                for (i, (name, ty)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", name, ty)?;
                }
                write!(f, "}}")
            }
            Type::Any => write!(f, "any"),
            Type::Unknown => write!(f, "?"),
        }
    }
}
