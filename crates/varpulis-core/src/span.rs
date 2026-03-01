//! Source location tracking

use serde::{Deserialize, Serialize};

/// A span in the source code
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Span {
    /// Start byte offset
    pub start: usize,
    /// End byte offset
    pub end: usize,
}

impl Span {
    /// Creates a new span from start and end byte offsets.
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// Merges two spans into the smallest span covering both.
    pub fn merge(self, other: Self) -> Self {
        Self {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }

    /// Creates a dummy span (0..0) for synthetic AST nodes.
    pub const fn dummy() -> Self {
        Self { start: 0, end: 0 }
    }
}

impl Default for Span {
    fn default() -> Self {
        Self::dummy()
    }
}

/// A value with an associated span
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound = "T: Serialize + for<'a> Deserialize<'a>")]
pub struct Spanned<T> {
    /// The wrapped AST node.
    pub node: T,
    /// Source location of this node.
    pub span: Span,
}

impl<T> Spanned<T> {
    /// Creates a new spanned value with the given span.
    pub const fn new(node: T, span: Span) -> Self {
        Self { node, span }
    }

    /// Creates a spanned value with a dummy span (0..0).
    pub const fn dummy(node: T) -> Self {
        Self {
            node,
            span: Span::dummy(),
        }
    }

    /// Transforms the wrapped node, preserving the span.
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Spanned<U> {
        Spanned {
            node: f(self.node),
            span: self.span,
        }
    }
}
