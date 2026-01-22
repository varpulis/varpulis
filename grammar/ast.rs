// VarpulisQL Abstract Syntax Tree
//
// This module defines the AST types used by the LALRPOP parser.

use std::time::Duration;

/// Top-level statement
#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    StreamDecl {
        name: String,
        source: StreamSource,
        ops: Vec<StreamOp>,
    },
    StreamDeclTyped {
        name: String,
        ty: Type,
        source: StreamSource,
        ops: Vec<StreamOp>,
    },
    EventDecl {
        name: String,
        extends: Option<String>,
        fields: Vec<Field>,
    },
    TypeDecl {
        name: String,
        ty: Type,
    },
    VarDecl {
        mutable: bool,
        name: String,
        ty: Option<Type>,
        value: Expr,
    },
    ConstDecl {
        name: String,
        ty: Option<Type>,
        value: Expr,
    },
    FnDecl {
        name: String,
        params: Vec<Param>,
        ret: Option<Type>,
        body: Vec<Stmt>,
    },
    Config(Vec<ConfigItem>),
    Import(String),
    ImportAs(String, String),
    Expr(Expr),
    If {
        cond: Expr,
        then_branch: Vec<Stmt>,
        elif_branches: Vec<(Expr, Vec<Stmt>)>,
        else_branch: Option<Vec<Stmt>>,
    },
    For {
        var: String,
        iter: Expr,
        body: Vec<Stmt>,
    },
    While {
        cond: Expr,
        body: Vec<Stmt>,
    },
    Return(Option<Expr>),
    Break,
    Continue,
}

/// Stream expression
#[derive(Debug, Clone, PartialEq)]
pub struct StreamExpr {
    pub source: StreamSource,
    pub ops: Vec<StreamOp>,
}

/// Stream source
#[derive(Debug, Clone, PartialEq)]
pub enum StreamSource {
    Ident(String),
    From(String),
    Merge(Vec<StreamDecl>),
    Join(Vec<JoinClause>),
}

/// Inline stream declaration (used in merge/join)
#[derive(Debug, Clone, PartialEq)]
pub struct StreamDecl {
    pub name: String,
    pub source: String,
    pub filter: Option<Expr>,
}

/// Join clause
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub name: String,
    pub source: String,
    pub on: Option<Expr>,
}

/// Stream operation
#[derive(Debug, Clone, PartialEq)]
pub enum StreamOp {
    Where(Expr),
    Select(Vec<SelectItem>),
    Window(WindowArgs),
    Aggregate(Vec<AggItem>),
    PartitionBy(Expr),
    OrderBy(Vec<OrderItem>),
    Limit(Expr),
    Distinct(Option<Expr>),
    Map(Expr),
    Filter(Expr),
    Tap(Vec<(String, Expr)>),
    Emit(Vec<(String, Expr)>),
    To(Expr),
    Pattern(PatternDef),
    AttentionWindow(Vec<(String, Expr)>),
    Concurrent(Vec<(String, Expr)>),
    Process(Expr),
    OnError(Expr),
    Collect,
}

/// Select item
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Field(String),
    Alias(String, Expr),
}

/// Aggregation item
#[derive(Debug, Clone, PartialEq)]
pub struct AggItem {
    pub alias: String,
    pub func: String,
    pub arg: Option<Expr>,
}

/// Order item
#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub desc: bool,
}

/// Window arguments
#[derive(Debug, Clone, PartialEq)]
pub struct WindowArgs {
    pub duration: Expr,
    pub sliding: Option<Expr>,
    pub policy: Option<Expr>,
}

/// Pattern definition
#[derive(Debug, Clone, PartialEq)]
pub struct PatternDef {
    pub name: String,
    pub matcher: Expr,
}

/// Field in event declaration
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    pub ty: Type,
    pub optional: bool,
}

/// Function parameter
#[derive(Debug, Clone, PartialEq)]
pub struct Param {
    pub name: String,
    pub ty: Type,
}

/// Type
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Int,
    Float,
    Bool,
    Str,
    Timestamp,
    Duration,
    Array(Box<Type>),
    Map(Box<Type>, Box<Type>),
    Tuple(Vec<Type>),
    Optional(Box<Type>),
    Stream(Box<Type>),
    Named(String),
}

/// Expression
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    // Literals
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Duration(Duration),
    Timestamp(i64), // nanoseconds since epoch
    Null,
    
    // Collections
    Array(Vec<Expr>),
    Map(Vec<(String, Expr)>),
    
    // Identifiers
    Ident(String),
    
    // Operations
    Binary {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    
    // Access
    Member {
        expr: Box<Expr>,
        member: String,
    },
    OptionalMember {
        expr: Box<Expr>,
        member: String,
    },
    Index {
        expr: Box<Expr>,
        index: Box<Expr>,
    },
    Slice {
        expr: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },
    
    // Call
    Call {
        func: Box<Expr>,
        args: Vec<Arg>,
    },
    
    // Lambda
    Lambda {
        params: Vec<String>,
        body: Box<Expr>,
    },
    
    // Conditional
    If {
        cond: Box<Expr>,
        then_branch: Box<Expr>,
        else_branch: Box<Expr>,
    },
    
    // Coalesce
    Coalesce {
        expr: Box<Expr>,
        default: Box<Expr>,
    },
    
    // Range
    Range {
        start: Box<Expr>,
        end: Box<Expr>,
        inclusive: bool,
    },
}

/// Binary operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    
    // Comparison
    Eq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    NotIn,
    Is,
    
    // Logical
    And,
    Or,
    
    // Bitwise
    BitAnd,
    BitOr,
    BitXor,
    Shl,
    Shr,
}

/// Unary operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
    BitNot,
}

/// Function argument
#[derive(Debug, Clone, PartialEq)]
pub enum Arg {
    Positional(Expr),
    Named(String, Expr),
}

/// Configuration item
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigItem {
    Value(String, ConfigValue),
    Nested(String, Vec<ConfigItem>),
}

/// Configuration value
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Duration(Duration),
    Ident(String),
    Array(Vec<ConfigValue>),
    Map(Vec<(String, ConfigValue)>),
}

// ============================================================================
// Helper functions for parsing
// ============================================================================

/// Parse duration string (e.g., "5s", "10m", "1h")
pub fn parse_duration(s: &str) -> Duration {
    let len = s.len();
    
    // Find where the number ends and the unit begins
    let (num_str, unit) = if s.ends_with("ns") {
        (&s[..len-2], "ns")
    } else if s.ends_with("us") {
        (&s[..len-2], "us")
    } else if s.ends_with("ms") {
        (&s[..len-2], "ms")
    } else {
        (&s[..len-1], &s[len-1..])
    };
    
    let num: u64 = num_str.parse().unwrap_or(0);
    
    match unit {
        "ns" => Duration::from_nanos(num),
        "us" => Duration::from_micros(num),
        "ms" => Duration::from_millis(num),
        "s" => Duration::from_secs(num),
        "m" => Duration::from_secs(num * 60),
        "h" => Duration::from_secs(num * 3600),
        "d" => Duration::from_secs(num * 86400),
        _ => Duration::from_secs(0),
    }
}

/// Parse timestamp string (e.g., "@2026-01-23T10:00:00Z")
pub fn parse_timestamp(s: &str) -> i64 {
    // Remove the @ prefix
    let s = &s[1..];
    
    // TODO: Proper ISO8601 parsing
    // For now, return 0 as placeholder
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("5s"), Duration::from_secs(5));
        assert_eq!(parse_duration("10m"), Duration::from_secs(600));
        assert_eq!(parse_duration("1h"), Duration::from_secs(3600));
        assert_eq!(parse_duration("500ms"), Duration::from_millis(500));
    }
}
