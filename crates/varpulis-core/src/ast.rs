//! Abstract Syntax Tree for VarpulisQL

use crate::span::{Span, Spanned};
use crate::types::Type;
use serde::{Deserialize, Serialize};

/// A complete VarpulisQL program
#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub statements: Vec<Spanned<Stmt>>,
}

/// Top-level statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Stmt {
    /// Stream declaration: `stream X from Y` or `stream X = Y.where(...)`
    StreamDecl {
        name: String,
        type_annotation: Option<Type>,
        source: StreamSource,
        ops: Vec<StreamOp>,
    },
    /// Event declaration: `event X: ...`
    EventDecl {
        name: String,
        extends: Option<String>,
        fields: Vec<Field>,
    },
    /// Type alias: `type X = Y`
    TypeDecl { name: String, ty: Type },
    /// Variable declaration: `let x = ...` or `var x = ...`
    VarDecl {
        mutable: bool,
        name: String,
        ty: Option<Type>,
        value: Expr,
    },
    /// Constant declaration: `const X = ...`
    ConstDecl {
        name: String,
        ty: Option<Type>,
        value: Expr,
    },
    /// Function declaration: `fn x(...) -> T: ...`
    FnDecl {
        name: String,
        params: Vec<Param>,
        ret: Option<Type>,
        body: Vec<Spanned<Stmt>>,
    },
    /// Configuration block
    Config(Vec<ConfigItem>),
    /// Import statement
    Import { path: String, alias: Option<String> },
    /// Expression statement
    Expr(Expr),
    /// If statement
    If {
        cond: Expr,
        then_branch: Vec<Spanned<Stmt>>,
        elif_branches: Vec<(Expr, Vec<Spanned<Stmt>>)>,
        else_branch: Option<Vec<Spanned<Stmt>>>,
    },
    /// For loop
    For {
        var: String,
        iter: Expr,
        body: Vec<Spanned<Stmt>>,
    },
    /// While loop
    While {
        cond: Expr,
        body: Vec<Spanned<Stmt>>,
    },
    /// Return statement
    Return(Option<Expr>),
    /// Break statement
    Break,
    /// Continue statement
    Continue,
}

/// Stream source
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StreamSource {
    /// Reference to existing stream with optional alias
    Ident(String),
    /// Event type with optional alias: `EventType as alias`
    IdentWithAlias { name: String, alias: String },
    /// Event type with all quantifier and optional alias: `all EventType as alias`
    AllWithAlias { name: String, alias: Option<String> },
    /// From event type: `from EventType`
    From(String),
    /// Merge multiple streams
    Merge(Vec<InlineStreamDecl>),
    /// Join multiple streams
    Join(Vec<JoinClause>),
    /// Sequence construct: `sequence(step1: Event1, step2: Event2 where cond, ...)`
    Sequence(SequenceDecl),
}

/// Sequence declaration for temporal event correlation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SequenceDecl {
    /// Whether to match all first events or just one
    pub match_all: bool,
    /// Global timeout for the sequence
    pub timeout: Option<Box<Expr>>,
    /// Named steps in the sequence
    pub steps: Vec<SequenceStepDecl>,
}

/// A single step in a sequence declaration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SequenceStepDecl {
    /// Alias for the captured event
    pub alias: String,
    /// Event type to match
    pub event_type: String,
    /// Optional filter condition
    pub filter: Option<Expr>,
    /// Optional timeout for this step
    pub timeout: Option<Box<Expr>>,
}

/// Inline stream declaration used in merge/join
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InlineStreamDecl {
    pub name: String,
    pub source: String,
    pub filter: Option<Expr>,
}

/// Join clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinClause {
    pub name: String,
    pub source: String,
    pub on: Option<Expr>,
}

/// Stream operation (method call on stream)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StreamOp {
    /// Filter: `.where(cond)`
    Where(Expr),
    /// Projection: `.select(fields)`
    Select(Vec<SelectItem>),
    /// Window: `.window(duration, ...)`
    Window(WindowArgs),
    /// Aggregation: `.aggregate(...)`
    Aggregate(Vec<AggItem>),
    /// Partitioning: `.partition_by(key)`
    PartitionBy(Expr),
    /// Ordering: `.order_by(...)`
    OrderBy(Vec<OrderItem>),
    /// Limit: `.limit(n)`
    Limit(Expr),
    /// Distinct: `.distinct(expr?)`
    Distinct(Option<Expr>),
    /// Map: `.map(fn)`
    Map(Expr),
    /// Filter with lambda: `.filter(fn)`
    Filter(Expr),
    /// Tap for observability: `.tap(...)`
    Tap(Vec<NamedArg>),
    /// Emit to sink: `.emit(...)`
    Emit(Vec<NamedArg>),
    /// Send to destination: `.to(target)`
    To(Expr),
    /// Pattern matching: `.pattern(...)`
    Pattern(PatternDef),
    /// Attention window: `.attention_window(...)`
    AttentionWindow(Vec<NamedArg>),
    /// Concurrent processing: `.concurrent(...)`
    Concurrent(Vec<NamedArg>),
    /// Process with function: `.process(fn)`
    Process(Expr),
    /// Error handler: `.on_error(fn)`
    OnError(Expr),
    /// Collect results: `.collect()`
    Collect,
    /// Join condition: `.on(expr)`
    On(Expr),
    /// Followed-by sequence: `-> EventType where condition as alias`
    FollowedBy(FollowedByClause),
    /// Timeout constraint: `.within(duration)`
    Within(Expr),
    /// Negation: `.not(EventType where condition)`
    Not(FollowedByClause),
}

/// Followed-by clause for temporal sequences
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FollowedByClause {
    /// Event type to match
    pub event_type: String,
    /// Optional filter condition
    pub filter: Option<Expr>,
    /// Optional alias for captured event
    pub alias: Option<String>,
    /// Whether to match all events (true) or just one (false)
    pub match_all: bool,
}

/// Select item in projection
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectItem {
    /// Simple field reference
    Field(String),
    /// Aliased expression: `alias: expr`
    Alias(String, Expr),
}

/// Aggregation item
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggItem {
    pub alias: String,
    pub expr: Expr,
}

/// Order item
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderItem {
    pub expr: Expr,
    pub descending: bool,
}

/// Window arguments
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowArgs {
    pub duration: Expr,
    pub sliding: Option<Expr>,
    pub policy: Option<Expr>,
}

/// Pattern definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PatternDef {
    pub name: String,
    pub matcher: Expr,
}

/// Named argument: `name: value`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedArg {
    pub name: String,
    pub value: Expr,
}

/// Field in event declaration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub ty: Type,
    pub optional: bool,
}

/// Function parameter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Param {
    pub name: String,
    pub ty: Type,
}

/// Expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    // Literals
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Duration(u64), // nanoseconds
    Timestamp(i64), // nanoseconds since epoch

    // Collections
    Array(Vec<Expr>),
    Map(Vec<(String, Expr)>),

    // Identifier
    Ident(String),

    // Binary operation
    Binary {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },

    // Unary operation
    Unary { op: UnaryOp, expr: Box<Expr> },

    // Member access: `expr.member`
    Member { expr: Box<Expr>, member: String },

    // Optional member access: `expr?.member`
    OptionalMember { expr: Box<Expr>, member: String },

    // Index access: `expr[index]`
    Index { expr: Box<Expr>, index: Box<Expr> },

    // Slice: `expr[start:end]`
    Slice {
        expr: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },

    // Function call: `func(args)`
    Call { func: Box<Expr>, args: Vec<Arg> },

    // Lambda: `x => expr` or `(x, y) => expr`
    Lambda {
        params: Vec<String>,
        body: Box<Expr>,
    },

    // Conditional: `if cond then a else b`
    If {
        cond: Box<Expr>,
        then_branch: Box<Expr>,
        else_branch: Box<Expr>,
    },

    // Null coalescing: `expr ?? default`
    Coalesce {
        expr: Box<Expr>,
        default: Box<Expr>,
    },

    // Range: `start..end` or `start..=end`
    Range {
        start: Box<Expr>,
        end: Box<Expr>,
        inclusive: bool,
    },

    // Block expression: `{ let a = 1; let b = 2; a + b }`
    Block {
        stmts: Vec<(String, Option<Type>, Expr, bool)>, // (name, type, value, is_mutable)
        result: Box<Expr>,
    },
}

/// Function argument
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Arg {
    Positional(Expr),
    Named(String, Expr),
}

/// Binary operator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

impl BinOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            BinOp::Add => "+",
            BinOp::Sub => "-",
            BinOp::Mul => "*",
            BinOp::Div => "/",
            BinOp::Mod => "%",
            BinOp::Pow => "**",
            BinOp::Eq => "==",
            BinOp::NotEq => "!=",
            BinOp::Lt => "<",
            BinOp::Le => "<=",
            BinOp::Gt => ">",
            BinOp::Ge => ">=",
            BinOp::In => "in",
            BinOp::NotIn => "not in",
            BinOp::Is => "is",
            BinOp::And => "and",
            BinOp::Or => "or",
            BinOp::BitAnd => "&",
            BinOp::BitOr => "|",
            BinOp::BitXor => "^",
            BinOp::Shl => "<<",
            BinOp::Shr => ">>",
        }
    }
}

/// Unary operator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    Neg,
    Not,
    BitNot,
}

impl UnaryOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            UnaryOp::Neg => "-",
            UnaryOp::Not => "not",
            UnaryOp::BitNot => "~",
        }
    }
}

/// Configuration item
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConfigItem {
    Value(String, ConfigValue),
    Nested(String, Vec<ConfigItem>),
}

/// Configuration value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConfigValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Duration(u64),
    Ident(String),
    Array(Vec<ConfigValue>),
    Map(Vec<(String, ConfigValue)>),
}
