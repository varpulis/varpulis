//! Abstract Syntax Tree for VPL

use crate::span::Spanned;
use crate::types::Type;
use serde::{Deserialize, Serialize};

/// A complete VPL program
#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub statements: Vec<Spanned<Stmt>>,
}

/// Top-level statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Stmt {
    /// Connector declaration: `connector MyMqtt = mqtt (host: "localhost", port: 1883)`
    ConnectorDecl {
        name: String,
        connector_type: String,
        params: Vec<ConnectorParam>,
    },
    /// Stream declaration: `stream X = Y` or `stream X = Y.where(...)`
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
    /// Configuration block with name (e.g., `config mqtt { ... }`)
    ///
    /// **Deprecated:** Use `connector` declarations instead.
    /// ```vpl
    /// connector MyMqtt = mqtt (
    ///     broker: "localhost",
    ///     port: 1883,
    ///     topic: "events/#"
    /// )
    /// ```
    Config {
        name: String,
        items: Vec<ConfigItem>,
    },
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
    /// Emit event statement: `emit EventType(field1: expr1, field2: expr2)`
    Emit {
        event_type: String,
        fields: Vec<NamedArg>,
    },
    /// Variable assignment: `name := value`
    Assignment { name: String, value: Expr },
    /// SASE+ Pattern declaration: `pattern Name = SEQ(A, B+) within 1h partition by user_id`
    PatternDecl {
        name: String,
        expr: SasePatternExpr,
        within: Option<Expr>,
        partition_by: Option<Expr>,
    },
    /// Context declaration: `context name (cores: [0, 1])`
    ContextDecl {
        name: String,
        cores: Option<Vec<usize>>,
    },
}

/// Connector parameter: `host: "localhost"` or `port: 1883`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorParam {
    pub name: String,
    pub value: ConfigValue,
}

/// SASE+ Pattern Expression for complex event processing
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SasePatternExpr {
    /// Sequence: SEQ(A, B, C)
    Seq(Vec<SasePatternItem>),
    /// Conjunction: A AND B
    And(Box<SasePatternExpr>, Box<SasePatternExpr>),
    /// Disjunction: A OR B
    Or(Box<SasePatternExpr>, Box<SasePatternExpr>),
    /// Negation: NOT A
    Not(Box<SasePatternExpr>),
    /// Single event type reference
    Event(String),
    /// Grouped expression
    Group(Box<SasePatternExpr>),
}

/// Item in a SASE+ sequence with optional Kleene operator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SasePatternItem {
    /// Event type name
    pub event_type: String,
    /// Optional alias for the event
    pub alias: Option<String>,
    /// Kleene operator: None, Plus (+), Star (*), Optional (?)
    pub kleene: Option<KleeneOp>,
    /// Optional filter condition
    pub filter: Option<Expr>,
}

/// Kleene operators for SASE+ patterns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KleeneOp {
    /// One or more (+)
    Plus,
    /// Zero or more (*)
    Star,
    /// Zero or one (?)
    Optional,
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
    /// From connector: `EventType.from(Connector, topic: "...", qos: 1)`
    FromConnector {
        event_type: String,
        connector_name: String,
        params: Vec<ConnectorParam>,
    },
    /// Merge multiple streams
    Merge(Vec<InlineStreamDecl>),
    /// Join multiple streams
    Join(Vec<JoinClause>),
    /// Sequence construct: `sequence(step1: Event1, step2: Event2 where cond, ...)`
    Sequence(SequenceDecl),
    /// Periodic timer source: `timer(5s)` or `timer(5s, initial_delay: 1s)`
    Timer(TimerDecl),
}

/// Timer declaration for periodic event generation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerDecl {
    /// Interval between timer fires (duration expression)
    pub interval: Expr,
    /// Optional initial delay before first fire
    pub initial_delay: Option<Box<Expr>>,
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
    /// Having: `.having(cond)` - filter after aggregation
    Having(Expr),
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
    /// Print to console: `.print(...)` or `.print("message", expr)`
    Print(Vec<Expr>),
    /// Log with level: `.log(level: "info", message: "...", data: expr)`
    Log(Vec<NamedArg>),
    /// Emit output: `.emit(fields...)` or `.emit as Type (fields...)`
    Emit {
        /// Optional type cast: `.emit as Alert (...)`
        output_type: Option<String>,
        /// Field mappings
        fields: Vec<NamedArg>,
        /// Optional target context for cross-context emit: `.emit(context: ctx_name, ...)`
        target_context: Option<String>,
    },
    /// Send to connector: `.to(Connector, topic: "...", method: "POST")`
    To {
        connector_name: String,
        params: Vec<ConnectorParam>,
    },
    /// Send to destination (legacy): `.to(target)`
    ToExpr(Expr),
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
    /// Fork into multiple parallel paths: `.fork(path1: ..., path2: ...)`
    Fork(Vec<ForkPath>),
    /// Wait for any path to complete: `.any()` or `.any(n)` for at least n
    Any(Option<usize>),
    /// Wait for all paths to complete: `.all()`
    All,
    /// Wait for first path to complete: `.first()`
    First,
    /// Assign stream to a context: `.context(name)`
    Context(String),
    /// Watermark configuration: `.watermark(out_of_order: 10s)`
    Watermark(Vec<NamedArg>),
    /// Allowed lateness for late data: `.allowed_lateness(30s)`
    AllowedLateness(Expr),
    /// Trend aggregation over Kleene patterns (Hamlet engine):
    /// `.trend_aggregate(count: count_trends(), events: count_events(rising))`
    TrendAggregate(Vec<TrendAggItem>),
}

/// A path in a fork construct
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkPath {
    /// Name of the path
    pub name: String,
    /// Sequence of operations for this path
    pub ops: Vec<StreamOp>,
}

/// Item in a trend_aggregate operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrendAggItem {
    /// Output alias for the aggregation result
    pub alias: String,
    /// Aggregation function name: "count_trends", "count_events", "sum_trends", etc.
    pub func: String,
    /// Optional argument (field reference for sum/avg/min/max, alias for count_events)
    pub arg: Option<Expr>,
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
    /// Session window gap duration (syntax: `.window(session: 5m)`)
    pub session_gap: Option<Expr>,
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
    Duration(u64),  // nanoseconds
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
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },

    // Member access: `expr.member`
    Member {
        expr: Box<Expr>,
        member: String,
    },

    // Optional member access: `expr?.member`
    OptionalMember {
        expr: Box<Expr>,
        member: String,
    },

    // Index access: `expr[index]`
    Index {
        expr: Box<Expr>,
        index: Box<Expr>,
    },

    // Slice: `expr[start:end]`
    Slice {
        expr: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },

    // Function call: `func(args)`
    Call {
        func: Box<Expr>,
        args: Vec<Arg>,
    },

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
    Xor,

    // Pattern operators
    FollowedBy, // -> (A followed by B)

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
            BinOp::Xor => "xor",
            BinOp::FollowedBy => "->",
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

impl ConfigValue {
    /// Get value as string (works for Str and Ident)
    pub fn as_string(&self) -> Option<&str> {
        match self {
            ConfigValue::Str(s) => Some(s),
            ConfigValue::Ident(s) => Some(s),
            _ => None,
        }
    }

    /// Get value as i64
    pub fn as_int(&self) -> Option<i64> {
        match self {
            ConfigValue::Int(i) => Some(*i),
            ConfigValue::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Get value as f64
    pub fn as_float(&self) -> Option<f64> {
        match self {
            ConfigValue::Float(f) => Some(*f),
            ConfigValue::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Get value as bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ConfigValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
}
