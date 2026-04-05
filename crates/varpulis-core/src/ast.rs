//! Abstract Syntax Tree for VPL

use serde::{Deserialize, Serialize};

use crate::span::Spanned;
use crate::types::Type;

/// A complete VPL program
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Program {
    /// The top-level statements that make up the program.
    pub statements: Vec<Spanned<Stmt>>,
}

/// Top-level statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Stmt {
    /// Connector declaration: `connector MyMqtt = mqtt (host: "localhost", port: 1883)`
    ConnectorDecl {
        /// Connector name.
        name: String,
        /// Connector protocol type (e.g., `"mqtt"`, `"kafka"`).
        connector_type: String,
        /// Connection parameters.
        params: Vec<ConnectorParam>,
    },
    /// Stream declaration: `stream X = Y` or `stream X = Y.where(...)`
    StreamDecl {
        /// Stream name.
        name: String,
        /// Optional type annotation for the stream.
        type_annotation: Option<Type>,
        /// Event source for this stream.
        source: StreamSource,
        /// Chained stream operations.
        ops: Vec<StreamOp>,
        /// Per-operation source spans (parallel to `ops`). Empty when spans unavailable.
        #[serde(default)]
        op_spans: Vec<crate::span::Span>,
    },
    /// Event declaration: `event X: ...`
    EventDecl {
        /// Event type name.
        name: String,
        /// Parent event type if this event extends another.
        extends: Option<String>,
        /// Fields declared on this event type.
        fields: Vec<Field>,
    },
    /// Type declaration: alias (`type X = Y`) or struct (`type X: field: T`).
    TypeDecl {
        /// Type name.
        name: String,
        /// The aliased type (alias form only).
        ty: Option<Type>,
        /// Struct fields (struct form only).
        fields: Vec<Field>,
    },
    /// Variable declaration: `let x = ...` or `var x = ...`
    VarDecl {
        /// Whether the variable can be reassigned.
        mutable: bool,
        /// Variable name.
        name: String,
        /// Optional type annotation.
        ty: Option<Type>,
        /// Initial value expression.
        value: Expr,
    },
    /// Constant declaration: `const X = ...`
    ConstDecl {
        /// Constant name.
        name: String,
        /// Optional type annotation.
        ty: Option<Type>,
        /// Constant value expression.
        value: Expr,
    },
    /// Function declaration: `fn x(...) -> T: ...`
    FnDecl {
        /// Function name.
        name: String,
        /// Function parameters.
        params: Vec<Param>,
        /// Optional return type.
        ret: Option<Type>,
        /// Function body statements.
        body: Vec<Spanned<Self>>,
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
        /// Configuration block name.
        name: String,
        /// Configuration key-value items.
        items: Vec<ConfigItem>,
    },
    /// Import statement
    Import {
        /// Module path to import.
        path: String,
        /// Optional import alias.
        alias: Option<String>,
    },
    /// Import a WASM function as a UDF.
    /// Syntax: `import fn name(param: type, ...) -> return_type from "path.wasm"`
    ImportWasmFn {
        /// Function name (registered in UdfRegistry).
        name: String,
        /// Typed parameters.
        params: Vec<Param>,
        /// Return type (None defaults to Any).
        ret: Option<Type>,
        /// Path to the WASM module file.
        wasm_path: String,
    },
    /// Expression statement
    Expr(Expr),
    /// If statement
    If {
        /// Condition expression.
        cond: Expr,
        /// Statements to execute when the condition is true.
        then_branch: Vec<Spanned<Self>>,
        /// Optional else-if branches (condition, body pairs).
        elif_branches: Vec<(Expr, Vec<Spanned<Self>>)>,
        /// Optional else branch.
        else_branch: Option<Vec<Spanned<Self>>>,
    },
    /// For loop
    For {
        /// Loop variable name.
        var: String,
        /// Iterable expression.
        iter: Expr,
        /// Loop body statements.
        body: Vec<Spanned<Self>>,
    },
    /// While loop
    While {
        /// Loop condition.
        cond: Expr,
        /// Loop body statements.
        body: Vec<Spanned<Self>>,
    },
    /// Return statement
    Return(Option<Expr>),
    /// Break statement
    Break,
    /// Continue statement
    Continue,
    /// Emit event statement: `emit EventType(field1: expr1, field2: expr2)`
    Emit {
        /// Event type to emit.
        event_type: String,
        /// Named field values for the emitted event.
        fields: Vec<NamedArg>,
    },
    /// Variable assignment: `name := value`
    Assignment {
        /// Variable name to assign.
        name: String,
        /// New value expression.
        value: Expr,
    },
    /// SASE+ Pattern declaration: `pattern Name = SEQ(A, B+) within 1h partition by user_id`
    PatternDecl {
        /// Pattern name.
        name: String,
        /// SASE+ pattern expression.
        expr: SasePatternExpr,
        /// Optional time window constraint.
        within: Option<Expr>,
        /// Optional partition key expression.
        partition_by: Option<Expr>,
    },
    /// Context declaration: `context name (cores: [0, 1])`
    ContextDecl {
        /// Context name.
        name: String,
        /// Optional CPU core affinity list.
        cores: Option<Vec<usize>>,
    },
}

/// Connector parameter: `host: "localhost"` or `port: 1883`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorParam {
    /// Parameter name.
    pub name: String,
    /// Parameter value.
    pub value: ConfigValue,
}

/// SASE+ Pattern Expression for complex event processing
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SasePatternExpr {
    /// Sequence: SEQ(A, B, C)
    Seq(Vec<SasePatternItem>),
    /// Conjunction: A AND B
    And(Box<Self>, Box<Self>),
    /// Disjunction: A OR B
    Or(Box<Self>, Box<Self>),
    /// Negation: NOT A
    Not(Box<Self>),
    /// Single event type reference
    Event(String),
    /// Grouped expression
    Group(Box<Self>),
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    IdentWithAlias {
        /// Event type or stream name.
        name: String,
        /// Alias for the source.
        alias: String,
    },
    /// Source event type with inline filter and optional alias.
    /// Example: `EventType where status == "failed" as f1`
    IdentWithFilterAndAlias {
        /// Event type or stream name.
        name: String,
        /// Inline filter expression.
        filter: Expr,
        /// Optional alias for the source.
        alias: Option<String>,
    },
    /// Event type with all quantifier and optional alias: `all EventType as alias`
    AllWithAlias {
        /// Event type name.
        name: String,
        /// Optional alias for the source.
        alias: Option<String>,
    },
    /// From connector: `EventType.from(Connector, topic: "...", qos: 1)`
    FromConnector {
        /// Event type to receive from the connector.
        event_type: String,
        /// Connector name to read from.
        connector_name: String,
        /// Additional connector parameters.
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
    /// Name alias for this inline stream.
    pub name: String,
    /// Source event type or stream name.
    pub source: String,
    /// Optional filter condition for this source.
    pub filter: Option<Expr>,
}

/// Join type for stream correlation
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    /// Inner join (default) — emit only when all sources match
    #[default]
    Inner,
    /// Left join — emit when left source has an event, fill nulls for missing right
    Left,
    /// Right join — emit when right source has an event, fill nulls for missing left
    Right,
    /// Full outer join — emit for either side, fill nulls for missing sources
    Full,
}

/// Join clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinClause {
    /// Name alias for this join source.
    pub name: String,
    /// Source event type or stream name.
    pub source: String,
    /// Optional join condition expression.
    pub on: Option<Expr>,
    /// Type of join (inner, left, right, full).
    #[serde(default)]
    pub join_type: JoinType,
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
        /// Target connector name.
        connector_name: String,
        /// Sink parameters (topic, method, etc.).
        params: Vec<ConnectorParam>,
    },
    /// Send to destination (legacy): `.to(target)`
    ToExpr(Expr),
    /// Pattern matching: `.pattern(...)`
    Pattern(PatternDef),
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
    /// ONNX model scoring: `.score(model: "path.onnx", inputs: [...], outputs: [...])`
    Score(ScoreSpec),
    /// PST-based pattern forecasting: `.forecast(confidence: 0.7, horizon: 2m, warmup: 500, max_depth: 5)`
    Forecast(ForecastSpec),
    /// External connector enrichment: `.enrich(WeatherAPI, key: t.city, fields: [forecast, humidity], cache_ttl: 5m)`
    Enrich(EnrichSpec),
    /// Alert notification: `.alert(webhook: "https://...", message: "Alert: {field}")`
    Alert(Vec<NamedArg>),
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

/// Specification for ONNX model scoring
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScoreSpec {
    /// Path to the ONNX model file.
    pub model_path: String,
    /// Input tensor names to feed from event fields.
    pub inputs: Vec<String>,
    /// Output tensor names to extract as event fields.
    pub outputs: Vec<String>,
    /// Enable GPU acceleration (default: false)
    #[serde(default)]
    pub gpu: bool,
    /// Batch size for inference (default: 1, >1 enables batch mode)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// GPU device ID (default: 0)
    #[serde(default)]
    pub device_id: i32,
}

const fn default_batch_size() -> usize {
    1
}

/// Specification for PST-based pattern forecasting
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForecastSpec {
    /// Minimum probability to emit forecast (default 0.5)
    pub confidence: Option<Expr>,
    /// Forecast time horizon (default = within duration)
    pub horizon: Option<Expr>,
    /// Events before forecasting starts (default 100)
    pub warmup: Option<Expr>,
    /// Maximum PST context depth (default 5)
    pub max_depth: Option<Expr>,
    /// Enable Hawkes intensity modulation (default true)
    pub hawkes: Option<Expr>,
    /// Enable conformal prediction intervals (default true)
    pub conformal: Option<Expr>,
    /// Preset mode: "fast", "accurate", or "balanced" (default "balanced")
    pub mode: Option<Expr>,
}

/// Specification for external connector enrichment
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnrichSpec {
    /// Name of the connector to use for lookups
    pub connector_name: String,
    /// Expression to evaluate as the lookup key
    pub key_expr: Box<Expr>,
    /// Fields to extract from the enrichment response
    pub fields: Vec<String>,
    /// Cache TTL (optional, duration expression)
    pub cache_ttl: Option<Expr>,
    /// Timeout for the enrichment request (optional, duration expression)
    pub timeout: Option<Expr>,
    /// Fallback value when lookup fails (optional)
    pub fallback: Option<Expr>,
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
    /// Output field name for the aggregation result.
    pub alias: String,
    /// Aggregation expression (e.g., `avg(temperature)`).
    pub expr: Expr,
}

/// Order item
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderItem {
    /// Expression to sort by.
    pub expr: Expr,
    /// Whether to sort in descending order.
    pub descending: bool,
}

/// Window arguments
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowArgs {
    /// Window duration (tumbling window size).
    pub duration: Expr,
    /// Optional slide interval for sliding windows.
    pub sliding: Option<Expr>,
    /// Optional eviction policy expression.
    pub policy: Option<Expr>,
    /// Session window gap duration (syntax: `.window(session: 5m)`)
    pub session_gap: Option<Expr>,
}

/// Pattern definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PatternDef {
    /// Pattern name.
    pub name: String,
    /// Matcher expression for the pattern.
    pub matcher: Expr,
}

/// Named argument: `name: value`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedArg {
    /// Argument name.
    pub name: String,
    /// Argument value expression.
    pub value: Expr,
}

/// Field in event declaration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    /// Field name.
    pub name: String,
    /// Field type.
    pub ty: Type,
    /// Whether the field is optional.
    pub optional: bool,
}

/// Function parameter
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Param {
    /// Parameter name.
    pub name: String,
    /// Parameter type.
    pub ty: Type,
}

/// Expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// Null literal.
    Null,
    /// Boolean literal.
    Bool(bool),
    /// Integer literal.
    Int(i64),
    /// Floating-point literal.
    Float(f64),
    /// String literal.
    Str(String),
    /// Duration literal in nanoseconds.
    Duration(u64),
    /// Timestamp literal in nanoseconds since epoch.
    Timestamp(i64),

    /// Array literal: `[1, 2, 3]`.
    Array(Vec<Self>),
    /// Map literal: `{key: value, ...}`.
    Map(Vec<(String, Self)>),

    /// Identifier reference.
    Ident(String),

    /// Binary operation: `left op right`.
    Binary {
        /// The binary operator.
        op: BinOp,
        /// Left operand.
        left: Box<Self>,
        /// Right operand.
        right: Box<Self>,
    },

    /// Unary operation: `op expr`.
    Unary {
        /// The unary operator.
        op: UnaryOp,
        /// Operand expression.
        expr: Box<Self>,
    },

    /// Member access: `expr.member`.
    Member {
        /// Object expression.
        expr: Box<Self>,
        /// Member name to access.
        member: String,
    },

    /// Optional member access: `expr?.member`.
    OptionalMember {
        /// Object expression (may be null).
        expr: Box<Self>,
        /// Member name to access.
        member: String,
    },

    /// Index access: `expr[index]`.
    Index {
        /// Collection expression.
        expr: Box<Self>,
        /// Index expression.
        index: Box<Self>,
    },

    /// Slice: `expr[start:end]`.
    Slice {
        /// Collection expression.
        expr: Box<Self>,
        /// Optional start index.
        start: Option<Box<Self>>,
        /// Optional end index.
        end: Option<Box<Self>>,
    },

    /// Function call: `func(args)`.
    Call {
        /// Function expression to call.
        func: Box<Self>,
        /// Arguments to the function.
        args: Vec<Arg>,
    },

    /// Lambda: `x => expr` or `(x, y) => expr`.
    Lambda {
        /// Lambda parameter names.
        params: Vec<String>,
        /// Lambda body expression.
        body: Box<Self>,
    },

    /// Conditional expression: `if cond then a else b`.
    If {
        /// Condition expression.
        cond: Box<Self>,
        /// Value when true.
        then_branch: Box<Self>,
        /// Value when false.
        else_branch: Box<Self>,
    },

    /// Null coalescing: `expr ?? default`.
    Coalesce {
        /// Expression that may be null.
        expr: Box<Self>,
        /// Fallback value when expr is null.
        default: Box<Self>,
    },

    /// Range: `start..end` or `start..=end`.
    Range {
        /// Range start.
        start: Box<Self>,
        /// Range end.
        end: Box<Self>,
        /// Whether the end is inclusive (`..=`).
        inclusive: bool,
    },

    /// Block expression: `{ let a = 1; let b = 2; a + b }`.
    Block {
        /// Local bindings: (name, optional type, value, is_mutable).
        stmts: Vec<(String, Option<Type>, Self, bool)>,
        /// Final expression that produces the block's value.
        result: Box<Self>,
    },
}

/// Function argument
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Arg {
    /// Positional argument.
    Positional(Expr),
    /// Named argument: `name: value`.
    Named(String, Expr),
}

/// Binary operator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinOp {
    /// Addition (`+`).
    Add,
    /// Subtraction (`-`).
    Sub,
    /// Multiplication (`*`).
    Mul,
    /// Division (`/`).
    Div,
    /// Modulo (`%`).
    Mod,
    /// Exponentiation (`**`).
    Pow,

    /// Equality (`==`).
    Eq,
    /// Inequality (`!=`).
    NotEq,
    /// Less than (`<`).
    Lt,
    /// Less than or equal (`<=`).
    Le,
    /// Greater than (`>`).
    Gt,
    /// Greater than or equal (`>=`).
    Ge,
    /// Membership test (`in`).
    In,
    /// Negated membership test (`not in`).
    NotIn,
    /// Type identity test (`is`).
    Is,

    /// Logical AND (`and`).
    And,
    /// Logical OR (`or`).
    Or,
    /// Logical XOR (`xor`).
    Xor,

    /// Temporal followed-by operator (`->`).
    FollowedBy,

    /// Bitwise AND (`&`).
    BitAnd,
    /// Bitwise OR (`|`).
    BitOr,
    /// Bitwise XOR (`^`).
    BitXor,
    /// Left shift (`<<`).
    Shl,
    /// Right shift (`>>`).
    Shr,
}

impl BinOp {
    /// Returns the operator's source-level string representation.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Pow => "**",
            Self::Eq => "==",
            Self::NotEq => "!=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::In => "in",
            Self::NotIn => "not in",
            Self::Is => "is",
            Self::And => "and",
            Self::Or => "or",
            Self::Xor => "xor",
            Self::FollowedBy => "->",
            Self::BitAnd => "&",
            Self::BitOr => "|",
            Self::BitXor => "^",
            Self::Shl => "<<",
            Self::Shr => ">>",
        }
    }
}

/// Unary operator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    /// Arithmetic negation (`-`).
    Neg,
    /// Logical negation (`not`).
    Not,
    /// Bitwise complement (`~`).
    BitNot,
}

impl UnaryOp {
    /// Returns the operator's source-level string representation.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Neg => "-",
            Self::Not => "not",
            Self::BitNot => "~",
        }
    }
}

/// Configuration item
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConfigItem {
    /// Simple key-value pair.
    Value(String, ConfigValue),
    /// Nested configuration block.
    Nested(String, Vec<Self>),
}

/// Configuration value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConfigValue {
    /// Boolean value.
    Bool(bool),
    /// Integer value.
    Int(i64),
    /// Floating-point value.
    Float(f64),
    /// String value.
    Str(String),
    /// Duration value in nanoseconds.
    Duration(u64),
    /// Identifier reference.
    Ident(String),
    /// Array of configuration values.
    Array(Vec<Self>),
    /// Map of key-value pairs.
    Map(Vec<(String, Self)>),
    /// String concatenation: `"prefix-" + field_name + "-suffix"`
    /// Elements are `Str` or `Ident` only.
    Concat(Vec<Self>),
}

impl ConfigValue {
    /// Get value as string (works for Str and Ident)
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::Str(s) => Some(s),
            Self::Ident(s) => Some(s),
            _ => None,
        }
    }

    /// Get value as i64
    pub const fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(i) => Some(*i),
            Self::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Get value as f64
    pub const fn as_float(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            Self::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Get value as bool
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }
}
