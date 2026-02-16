//! Static registries of builtin and aggregate functions.

/// Builtin scalar functions available in VPL expressions.
pub static BUILTIN_FUNCTIONS: &[&str] = &[
    // Math
    "abs",
    "sqrt",
    "floor",
    "ceil",
    "round",
    "log",
    "min",
    "max",
    "clamp",
    // String
    "len",
    "trim",
    "to_upper",
    "to_lower",
    "contains",
    "starts_with",
    "ends_with",
    "replace",
    "split",
    "join",
    "concat",
    // Collection
    "first",
    "last",
    "push",
    "pop",
    "reverse",
    "sort",
    "unique",
    "flatten",
    "zip",
    "range",
    "keys",
    "values",
    // Type conversion
    "to_string",
    "to_int",
    "to_float",
    "type_of",
    // Time
    "now",
    "timestamp",
    "format",
    "parse",
    // Utility
    "print",
    "coalesce",
    "if_null",
];

/// Aggregate functions used in `.aggregate()` operations.
pub static AGGREGATE_FUNCTIONS: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "stddev",
    "first",
    "last",
    "count_distinct",
    "ema",
];

/// Aggregate functions that require at least one field argument.
pub static AGGREGATE_REQUIRES_FIELD: &[&str] = &["sum", "avg", "min", "max", "stddev"];

/// Aggregate functions that require exactly two arguments (field + period).
pub static AGGREGATE_REQUIRES_TWO_ARGS: &[&str] = &["ema"];

/// Valid parameter names for `.log()`.
pub static LOG_PARAMS: &[&str] = &["level", "message", "data"];

/// Valid parameter names for `.watermark()`.
pub static WATERMARK_PARAMS: &[&str] = &["out_of_order"];

/// Valid parameter names for `.forecast()`.
pub static FORECAST_PARAMS: &[&str] = &[
    "confidence",
    "horizon",
    "warmup",
    "max_depth",
    "hawkes",
    "conformal",
    "mode",
];

/// Check if a function name is a known builtin (scalar or aggregate).
pub fn is_known_function(name: &str) -> bool {
    BUILTIN_FUNCTIONS.contains(&name) || AGGREGATE_FUNCTIONS.contains(&name)
}

/// Check if a function name is a known aggregate function.
pub fn is_aggregate_function(name: &str) -> bool {
    AGGREGATE_FUNCTIONS.contains(&name)
}

// =========================================================================
// Connector parameter schemas
// =========================================================================

/// Type of a connector parameter value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamType {
    Str,
    Int,
    Bool,
}

/// Whether a parameter is valid for source (.from()), sink (.to()), or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamContext {
    Source,
    Sink,
    Both,
}

/// Schema definition for a single connector parameter.
#[derive(Debug, Clone)]
pub struct ConnectorParamDef {
    pub name: &'static str,
    pub param_type: ParamType,
    pub required: bool,
    pub description: &'static str,
    pub context: ParamContext,
}

impl ConnectorParamDef {
    /// Check if this param is valid in a given context.
    pub fn valid_in(&self, ctx: ParamContext) -> bool {
        self.context == ParamContext::Both || self.context == ctx
    }
}

static MQTT_PARAMS: &[ConnectorParamDef] = &[
    ConnectorParamDef {
        name: "topic",
        param_type: ParamType::Str,
        required: false,
        description: "MQTT topic to subscribe/publish",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "client_id",
        param_type: ParamType::Str,
        required: false,
        description: "Dedicated MQTT client ID (creates separate connection)",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "qos",
        param_type: ParamType::Int,
        required: false,
        description: "QoS level (0, 1, 2)",
        context: ParamContext::Both,
    },
];

static KAFKA_PARAMS: &[ConnectorParamDef] = &[
    ConnectorParamDef {
        name: "topic",
        param_type: ParamType::Str,
        required: false,
        description: "Kafka topic",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "group_id",
        param_type: ParamType::Str,
        required: false,
        description: "Consumer group ID",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "partition",
        param_type: ParamType::Int,
        required: false,
        description: "Partition number",
        context: ParamContext::Both,
    },
];

static HTTP_PARAMS: &[ConnectorParamDef] = &[
    ConnectorParamDef {
        name: "topic",
        param_type: ParamType::Str,
        required: false,
        description: "URL path suffix",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "method",
        param_type: ParamType::Str,
        required: false,
        description: "HTTP method (GET, POST, PUT, DELETE)",
        context: ParamContext::Sink,
    },
];

static CONSOLE_PARAMS: &[ConnectorParamDef] = &[ConnectorParamDef {
    name: "topic",
    param_type: ParamType::Str,
    required: false,
    description: "Label prefix for console output",
    context: ParamContext::Sink,
}];

/// Look up the parameter schema for a connector type.
///
/// Returns `None` for unknown connector types (forward-compatible).
pub fn connector_params_for_type(connector_type: &str) -> Option<&'static [ConnectorParamDef]> {
    match connector_type {
        "mqtt" => Some(MQTT_PARAMS),
        "kafka" => Some(KAFKA_PARAMS),
        "http" => Some(HTTP_PARAMS),
        "console" => Some(CONSOLE_PARAMS),
        _ => None,
    }
}
