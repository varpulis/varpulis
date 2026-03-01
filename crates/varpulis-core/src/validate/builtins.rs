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
    "median",
    "percentile",
    "p50",
    "p95",
    "p99",
];

/// Aggregate functions that require at least one field argument.
pub static AGGREGATE_REQUIRES_FIELD: &[&str] = &[
    "sum", "avg", "min", "max", "stddev", "median", "p50", "p95", "p99",
];

/// Aggregate functions that require exactly two arguments (field + period/quantile).
pub static AGGREGATE_REQUIRES_TWO_ARGS: &[&str] = &["ema", "percentile"];

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

/// Valid parameter names for `.enrich()`.
pub static ENRICH_PARAMS: &[&str] = &["key", "fields", "cache_ttl", "timeout", "fallback"];

/// Built-in variables available after `.forecast()`.
pub static FORECAST_BUILTIN_VARS: &[&str] = &[
    "forecast_probability",
    "forecast_confidence",
    "forecast_time",
    "forecast_state",
    "forecast_context_depth",
    "forecast_lower",
    "forecast_upper",
];

/// Built-in variables available after `.enrich()`.
pub static ENRICH_BUILTIN_VARS: &[&str] = &["enrich_status", "enrich_latency_ms"];

/// Connector types that support request-response lookups for `.enrich()`.
pub static ENRICH_COMPATIBLE_TYPES: &[&str] = &["http", "database", "redis"];

/// Valid parameter names for `.concurrent()`.
pub static CONCURRENT_PARAMS: &[&str] = &["workers", "partition_key"];

/// Known connector types for validation.
pub static KNOWN_CONNECTOR_TYPES: &[&str] = &[
    "mqtt",
    "kafka",
    "nats",
    "http",
    "console",
    "websocket",
    "file",
    "database",
    "redis",
    "redis_stream",
    "pulsar",
    "postgres_cdc",
];

/// Check if a connector type is known.
pub fn is_known_connector_type(name: &str) -> bool {
    KNOWN_CONNECTOR_TYPES.contains(&name)
}

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
    /// String parameter.
    Str,
    /// Integer parameter.
    Int,
    /// Boolean parameter.
    Bool,
    /// Array of strings parameter.
    StrArray,
}

/// Whether a parameter is valid for source (.from()), sink (.to()), or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamContext {
    /// Valid only for `.from()` (source) operations.
    Source,
    /// Valid only for `.to()` (sink) operations.
    Sink,
    /// Valid for both source and sink operations.
    Both,
}

/// Schema definition for a single connector parameter.
#[derive(Debug, Clone)]
pub struct ConnectorParamDef {
    /// Parameter name.
    pub name: &'static str,
    /// Expected value type.
    pub param_type: ParamType,
    /// Whether this parameter is required.
    pub required: bool,
    /// Human-readable description for diagnostics.
    pub description: &'static str,
    /// Whether valid for source, sink, or both contexts.
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
        name: "host",
        param_type: ParamType::Str,
        required: false,
        description: "MQTT broker hostname",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "port",
        param_type: ParamType::Int,
        required: false,
        description: "MQTT broker port",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "url",
        param_type: ParamType::Str,
        required: false,
        description: "MQTT broker URL",
        context: ParamContext::Both,
    },
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
        name: "brokers",
        param_type: ParamType::StrArray,
        required: false,
        description: "Kafka broker addresses (array of strings)",
        context: ParamContext::Both,
    },
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
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "partition",
        param_type: ParamType::Int,
        required: false,
        description: "Partition number",
        context: ParamContext::Both,
    },
];

static HTTP_PARAMS: &[ConnectorParamDef] = &[ConnectorParamDef {
    name: "base_url",
    param_type: ParamType::Str,
    required: false,
    description: "HTTP base URL for the endpoint",
    context: ParamContext::Both,
}];

static CONSOLE_PARAMS: &[ConnectorParamDef] = &[ConnectorParamDef {
    name: "topic",
    param_type: ParamType::Str,
    required: false,
    description: "Label prefix for console output",
    context: ParamContext::Sink,
}];

static NATS_PARAMS: &[ConnectorParamDef] = &[
    ConnectorParamDef {
        name: "url",
        param_type: ParamType::Str,
        required: false,
        description: "NATS server URL",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "topic",
        param_type: ParamType::Str,
        required: false,
        description: "NATS subject to subscribe/publish",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "queue_group",
        param_type: ParamType::Str,
        required: false,
        description: "Queue group for load-balanced consumption",
        context: ParamContext::Both,
    },
];

static PULSAR_PARAMS: &[ConnectorParamDef] = &[
    ConnectorParamDef {
        name: "url",
        param_type: ParamType::Str,
        required: false,
        description: "Pulsar service URL",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "topic",
        param_type: ParamType::Str,
        required: false,
        description: "Pulsar topic name",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "subscription",
        param_type: ParamType::Str,
        required: false,
        description: "Consumer subscription name",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "consumer_name",
        param_type: ParamType::Str,
        required: false,
        description: "Consumer name identifier",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "batch_size",
        param_type: ParamType::Int,
        required: false,
        description: "Consumer batch size",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "token",
        param_type: ParamType::Str,
        required: false,
        description: "Authentication token",
        context: ParamContext::Both,
    },
];

static REDIS_STREAM_PARAMS: &[ConnectorParamDef] = &[
    ConnectorParamDef {
        name: "url",
        param_type: ParamType::Str,
        required: false,
        description: "Redis server URL",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "stream_key",
        param_type: ParamType::Str,
        required: false,
        description: "Redis stream key name",
        context: ParamContext::Both,
    },
    ConnectorParamDef {
        name: "group",
        param_type: ParamType::Str,
        required: false,
        description: "Consumer group name",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "consumer",
        param_type: ParamType::Str,
        required: false,
        description: "Consumer name within the group",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "batch_size",
        param_type: ParamType::Int,
        required: false,
        description: "Number of messages to read per batch",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "max_len",
        param_type: ParamType::Int,
        required: false,
        description: "Approximate max stream length for XADD MAXLEN ~",
        context: ParamContext::Sink,
    },
];

/// Look up the parameter schema for a connector type.
///
/// Returns `None` for unknown connector types (forward-compatible).
pub fn connector_params_for_type(connector_type: &str) -> Option<&'static [ConnectorParamDef]> {
    match connector_type {
        "mqtt" => Some(MQTT_PARAMS),
        "kafka" => Some(KAFKA_PARAMS),
        "http" => Some(HTTP_PARAMS),
        "nats" => Some(NATS_PARAMS),
        "console" => Some(CONSOLE_PARAMS),
        "pulsar" => Some(PULSAR_PARAMS),
        "redis_stream" => Some(REDIS_STREAM_PARAMS),
        "postgres_cdc" => Some(POSTGRES_CDC_PARAMS),
        _ => None,
    }
}

static POSTGRES_CDC_PARAMS: &[ConnectorParamDef] = &[
    ConnectorParamDef {
        name: "host",
        param_type: ParamType::Str,
        required: true,
        description: "PostgreSQL hostname",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "port",
        param_type: ParamType::Int,
        required: false,
        description: "PostgreSQL port (default 5432)",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "dbname",
        param_type: ParamType::Str,
        required: true,
        description: "Database name",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "user",
        param_type: ParamType::Str,
        required: false,
        description: "Database user (default \"postgres\")",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "password",
        param_type: ParamType::Str,
        required: false,
        description: "Database password",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "slot_name",
        param_type: ParamType::Str,
        required: false,
        description: "Logical replication slot name",
        context: ParamContext::Source,
    },
    ConnectorParamDef {
        name: "publication",
        param_type: ParamType::Str,
        required: false,
        description: "PostgreSQL publication name",
        context: ParamContext::Source,
    },
];
