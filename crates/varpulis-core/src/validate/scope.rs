//! Symbol table for tracking declarations during validation.

use crate::span::Span;
use std::collections::HashMap;

/// Information about a declared event type.
#[derive(Debug, Clone)]
pub struct EventInfo {
    pub span: Span,
    pub field_names: Vec<String>,
}

/// Information about a declared stream.
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub span: Span,
}

/// Information about a declared function.
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub span: Span,
    pub param_count: usize,
}

/// Information about a declared connector.
#[derive(Debug, Clone)]
pub struct ConnectorInfo {
    pub span: Span,
    pub connector_type: String,
}

/// Information about a declared context.
#[derive(Debug, Clone)]
pub struct ContextInfo {
    pub span: Span,
}

/// Information about a declared pattern.
#[derive(Debug, Clone)]
pub struct PatternInfo {
    pub span: Span,
}

/// Information about a declared variable.
#[derive(Debug, Clone)]
pub struct VarInfo {
    pub span: Span,
    pub mutable: bool,
}

/// Information about a declared type alias.
#[derive(Debug, Clone)]
pub struct TypeInfo {
    pub span: Span,
}

/// Symbol table built during Pass 1.
#[derive(Debug)]
pub struct SymbolTable {
    pub events: HashMap<String, EventInfo>,
    pub streams: HashMap<String, StreamInfo>,
    pub functions: HashMap<String, FunctionInfo>,
    pub connectors: HashMap<String, ConnectorInfo>,
    pub contexts: HashMap<String, ContextInfo>,
    pub patterns: HashMap<String, PatternInfo>,
    pub variables: HashMap<String, VarInfo>,
    pub types: HashMap<String, TypeInfo>,
}

impl SymbolTable {
    pub fn new() -> Self {
        Self {
            events: HashMap::new(),
            streams: HashMap::new(),
            functions: HashMap::new(),
            connectors: HashMap::new(),
            contexts: HashMap::new(),
            patterns: HashMap::new(),
            variables: HashMap::new(),
            types: HashMap::new(),
        }
    }

    /// Check if any declaration table contains the given name.
    pub fn is_declared(&self, name: &str) -> bool {
        self.events.contains_key(name)
            || self.streams.contains_key(name)
            || self.functions.contains_key(name)
            || self.connectors.contains_key(name)
            || self.contexts.contains_key(name)
            || self.patterns.contains_key(name)
            || self.variables.contains_key(name)
            || self.types.contains_key(name)
    }

    /// Collect all declared names for "did you mean?" suggestions.
    pub fn all_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = Vec::new();
        for k in self.events.keys() {
            names.push(k);
        }
        for k in self.streams.keys() {
            names.push(k);
        }
        for k in self.functions.keys() {
            names.push(k);
        }
        for k in self.connectors.keys() {
            names.push(k);
        }
        for k in self.contexts.keys() {
            names.push(k);
        }
        for k in self.patterns.keys() {
            names.push(k);
        }
        for k in self.variables.keys() {
            names.push(k);
        }
        for k in self.types.keys() {
            names.push(k);
        }
        names
    }

    /// Collect connector names for suggestions.
    pub fn connector_names(&self) -> Vec<&str> {
        self.connectors.keys().map(|s| s.as_str()).collect()
    }

    /// Collect context names for suggestions.
    pub fn context_names(&self) -> Vec<&str> {
        self.contexts.keys().map(|s| s.as_str()).collect()
    }

    /// Collect event and stream names for source resolution suggestions.
    pub fn source_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = Vec::new();
        for k in self.events.keys() {
            names.push(k);
        }
        for k in self.streams.keys() {
            names.push(k);
        }
        names
    }

    /// Collect user-declared function names for suggestions.
    pub fn function_names(&self) -> Vec<&str> {
        self.functions.keys().map(|s| s.as_str()).collect()
    }
}
