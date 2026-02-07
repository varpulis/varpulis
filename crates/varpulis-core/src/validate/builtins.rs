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

/// Valid parameter names for `.attention_window()`.
pub static ATTENTION_WINDOW_PARAMS: &[&str] = &[
    "duration",
    "heads",
    "num_heads",
    "dim",
    "embedding_dim",
    "threshold",
];

/// Valid parameter names for `.watermark()`.
pub static WATERMARK_PARAMS: &[&str] = &["out_of_order"];

/// Check if a function name is a known builtin (scalar or aggregate).
pub fn is_known_function(name: &str) -> bool {
    BUILTIN_FUNCTIONS.contains(&name) || AGGREGATE_FUNCTIONS.contains(&name)
}

/// Check if a function name is a known aggregate function.
pub fn is_aggregate_function(name: &str) -> bool {
    AGGREGATE_FUNCTIONS.contains(&name)
}
