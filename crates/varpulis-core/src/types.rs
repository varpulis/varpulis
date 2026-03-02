//! Type system for VPL

use std::fmt;

use serde::{Deserialize, Serialize};

/// VPL type
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
    Array(Box<Self>),
    /// Map from key to value
    Map(Box<Self>, Box<Self>),
    /// Tuple of types
    Tuple(Vec<Self>),
    /// Optional type (T?)
    Optional(Box<Self>),
    /// Stream of events
    Stream(Box<Self>),
    /// Named type (event or type alias)
    Named(String),
    /// Record type (inline struct)
    Record(Vec<(String, Self)>),
    /// Any type (for polymorphic functions)
    Any,
    /// Unknown type (for type inference)
    Unknown,
}

impl Type {
    /// Returns true if this type is `Int` or `Float`.
    pub const fn is_numeric(&self) -> bool {
        matches!(self, Self::Int | Self::Float)
    }

    /// Returns true if this is an `Optional` type.
    pub const fn is_optional(&self) -> bool {
        matches!(self, Self::Optional(_))
    }

    /// Returns the inner type for `Array`, `Optional`, or `Stream` types.
    pub fn inner_type(&self) -> Option<&Self> {
        match self {
            Self::Array(t) | Self::Optional(t) | Self::Stream(t) => Some(t),
            _ => None,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int => write!(f, "int"),
            Self::Float => write!(f, "float"),
            Self::Bool => write!(f, "bool"),
            Self::Str => write!(f, "str"),
            Self::Timestamp => write!(f, "timestamp"),
            Self::Duration => write!(f, "duration"),
            Self::Null => write!(f, "null"),
            Self::Array(t) => write!(f, "[{t}]"),
            Self::Map(k, v) => write!(f, "{{{k}: {v}}}"),
            Self::Tuple(types) => {
                write!(f, "(")?;
                for (i, t) in types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{t}")?;
                }
                write!(f, ")")
            }
            Self::Optional(t) => write!(f, "{t}?"),
            Self::Stream(t) => write!(f, "Stream<{t}>"),
            Self::Named(name) => write!(f, "{name}"),
            Self::Record(fields) => {
                write!(f, "{{")?;
                for (i, (name, ty)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{name}: {ty}")?;
                }
                write!(f, "}}")
            }
            Self::Any => write!(f, "any"),
            Self::Unknown => write!(f, "?"),
        }
    }
}
