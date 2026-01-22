//! Source location tracking

use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// A span in the source code
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Span {
    /// Start byte offset
    pub start: usize,
    /// End byte offset
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub fn merge(self, other: Span) -> Span {
        Span {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }

    pub fn dummy() -> Self {
        Self { start: 0, end: 0 }
    }
}

impl Default for Span {
    fn default() -> Self {
        Self::dummy()
    }
}

/// A value with an associated span
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound = "T: Serialize + for<'a> Deserialize<'a>")]
pub struct Spanned<T> {
    pub node: T,
    pub span: Span,
}

impl<T> Spanned<T> {
    pub fn new(node: T, span: Span) -> Self {
        Self { node, span }
    }

    pub fn dummy(node: T) -> Self {
        Self {
            node,
            span: Span::dummy(),
        }
    }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Spanned<U> {
        Spanned {
            node: f(self.node),
            span: self.span,
        }
    }
}
