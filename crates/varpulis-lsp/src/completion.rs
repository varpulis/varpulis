//! Code completion provider

use tower_lsp::lsp_types::{
    CompletionItem, CompletionItemKind, Documentation, InsertTextFormat, MarkupContent, MarkupKind,
    Position,
};
use varpulis_core::validate::builtins::{self, ParamContext, ParamType};

/// Get completion items for a position in the document
pub fn get_completions(text: &str, position: Position) -> Vec<CompletionItem> {
    let context = get_completion_context(text, position);

    match context {
        CompletionContext::StreamOperation => get_stream_operation_completions(),
        CompletionContext::TopLevel => get_top_level_completions(),
        CompletionContext::AfterAt => get_timestamp_completions(),
        CompletionContext::InWindow => get_window_completions(),
        CompletionContext::InAggregate => get_aggregation_completions(),
        CompletionContext::InPattern => get_pattern_completions(),
        CompletionContext::Type => get_type_completions(),
        CompletionContext::Expression => get_expression_completions(),
        CompletionContext::InFromParams(connector_type) => {
            get_connector_param_completions(connector_type.as_deref(), ParamContext::Source)
        }
        CompletionContext::InToParams(connector_type) => {
            get_connector_param_completions(connector_type.as_deref(), ParamContext::Sink)
        }
        CompletionContext::ConnectorType => get_connector_type_completions(),
        CompletionContext::InForecastParams => get_forecast_param_completions(),
        CompletionContext::InEmit => get_emit_field_completions(text, position),
        CompletionContext::InWhere => get_where_field_completions(text, position),
        CompletionContext::InSelect => get_select_field_completions(text, position),
        CompletionContext::InPartitionBy => get_where_field_completions(text, position),
    }
}

#[derive(Debug)]
enum CompletionContext {
    StreamOperation,
    TopLevel,
    AfterAt,
    InWindow,
    InAggregate,
    InPattern,
    Type,
    Expression,
    /// Inside `.from(Connector, <cursor>)` with resolved connector type
    InFromParams(Option<String>),
    /// Inside `.to(Connector, <cursor>)` with resolved connector type
    InToParams(Option<String>),
    /// After `connector Name = <cursor>`
    ConnectorType,
    /// Inside `.forecast(<cursor>)`
    InForecastParams,
    /// Inside `.emit(<cursor>)` — suggest fields from upstream event/stream
    InEmit,
    /// Inside `.where(<cursor>)` — suggest fields from source event
    InWhere,
    /// Inside `.select(<cursor>)` — suggest fields from source event
    InSelect,
    /// Inside `.partition_by(<cursor>)` — suggest fields from source event
    InPartitionBy,
}

/// Determine what kind of completion to provide based on context
fn get_completion_context(text: &str, position: Position) -> CompletionContext {
    let lines: Vec<&str> = text.lines().collect();
    let line = lines.get(position.line as usize).unwrap_or(&"");
    let col = position.character as usize;
    let prefix = &line[..col.min(line.len())];

    // Check if we're inside .from(...) or .to(...) params — before other checks
    if let Some(ctx) = detect_connector_param_context(prefix, text) {
        return ctx;
    }

    // Check if we're after `connector Name = ` (suggest connector types)
    if let Some(idx) = prefix.find("connector ") {
        let after = &prefix[idx + 10..];
        // Skip the name, look for `= `
        if let Some(eq_idx) = after.find('=') {
            let after_eq = after[eq_idx + 1..].trim_start();
            if after_eq.is_empty() || (!after_eq.contains('(') && !after_eq.contains(')')) {
                return CompletionContext::ConnectorType;
            }
        }
    }

    // Check if we're after a dot (stream operation)
    if prefix.trim_end().ends_with('.') {
        return CompletionContext::StreamOperation;
    }

    // Check if we're after @ (timestamp literal)
    if prefix.trim_end().ends_with('@') {
        return CompletionContext::AfterAt;
    }

    // Check if we're inside .forecast(...) params
    if prefix.contains(".forecast(") && !prefix.ends_with(')') {
        return CompletionContext::InForecastParams;
    }

    // Check if we're inside .emit(...) — suggest fields
    if prefix.contains(".emit(") && !prefix.ends_with(')') {
        return CompletionContext::InEmit;
    }

    // Check if we're inside .where(...) — suggest fields for boolean expressions
    if prefix.contains(".where(") && !prefix.ends_with(')') {
        return CompletionContext::InWhere;
    }

    // Check if we're inside .select(...) — suggest fields for projection
    if prefix.contains(".select(") && !prefix.ends_with(')') {
        return CompletionContext::InSelect;
    }

    // Check if we're inside .partition_by(...) — suggest fields
    if prefix.contains(".partition_by(") && !prefix.ends_with(')') {
        return CompletionContext::InPartitionBy;
    }

    // Check if we're inside a window() call
    if prefix.contains(".window(") && !prefix.contains(')') {
        return CompletionContext::InWindow;
    }

    // Check if we're inside an aggregate() call
    if prefix.contains(".aggregate(") && !prefix.ends_with(')') {
        return CompletionContext::InAggregate;
    }

    // Check if we're in a pattern context
    if prefix.contains("pattern ") {
        return CompletionContext::InPattern;
    }

    // Check if we're in a type annotation context
    if prefix.trim_end().ends_with(':') {
        return CompletionContext::Type;
    }

    // Check if at top level (line starts with no indentation or is empty)
    if prefix.trim().is_empty() || !line.starts_with(char::is_whitespace) {
        return CompletionContext::TopLevel;
    }

    CompletionContext::Expression
}

/// Detect if cursor is inside `.from(Connector, ...)` or `.to(Connector, ...)`.
///
/// Returns the appropriate context with the resolved connector type if found.
fn detect_connector_param_context(prefix: &str, full_text: &str) -> Option<CompletionContext> {
    // Look for .from( or .to( in the prefix, with no closing )
    let (op_name, connector_name) = if let Some(idx) = prefix.rfind(".from(") {
        let after = &prefix[idx + 6..]; // after ".from("
        if after.contains(')') {
            return None; // already closed
        }
        // First token after the paren is the connector name
        let connector = extract_first_identifier(after)?;
        // Need at least a comma after the connector name to be in param context
        if !after[connector.len()..].trim_start().starts_with(',') && after.trim() != connector {
            return None;
        }
        ("from", connector)
    } else if let Some(idx) = prefix.rfind(".to(") {
        let after = &prefix[idx + 4..]; // after ".to("
        if after.contains(')') {
            return None;
        }
        let connector = extract_first_identifier(after)?;
        if !after[connector.len()..].trim_start().starts_with(',') && after.trim() != connector {
            return None;
        }
        ("to", connector)
    } else {
        return None;
    };

    // Resolve connector type by scanning for `connector <name> = <type>(...)`
    let connector_type = resolve_connector_type(full_text, &connector_name);

    match op_name {
        "from" => Some(CompletionContext::InFromParams(connector_type)),
        "to" => Some(CompletionContext::InToParams(connector_type)),
        _ => None,
    }
}

/// Extract the first identifier from text (e.g., "MyConn, topic:" → "MyConn").
fn extract_first_identifier(text: &str) -> Option<String> {
    let trimmed = text.trim_start();
    if trimmed.is_empty() {
        return None;
    }
    let first_char = trimmed.chars().next()?;
    if !first_char.is_alphabetic() && first_char != '_' {
        return None;
    }
    let end = trimmed
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(trimmed.len());
    Some(trimmed[..end].to_string())
}

/// Scan the document for `connector <name> = <type>(...)` and return the type.
fn resolve_connector_type(text: &str, connector_name: &str) -> Option<String> {
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("connector ") {
            let rest = rest.trim_start();
            if let Some(after_name) = rest.strip_prefix(connector_name) {
                let after_name = after_name.trim_start();
                if let Some(after_eq) = after_name.strip_prefix('=') {
                    let type_part = after_eq.trim_start();
                    // Type is the identifier before the '('
                    let type_end = type_part.find('(').unwrap_or(type_part.len());
                    let connector_type = type_part[..type_end].trim();
                    if !connector_type.is_empty() {
                        return Some(connector_type.to_string());
                    }
                }
            }
        }
    }
    None
}

/// Generate completion items for connector parameters.
fn get_connector_param_completions(
    connector_type: Option<&str>,
    ctx: ParamContext,
) -> Vec<CompletionItem> {
    let Some(connector_type) = connector_type else {
        return Vec::new();
    };

    let Some(schema) = builtins::connector_params_for_type(connector_type) else {
        return Vec::new();
    };

    schema
        .iter()
        .filter(|p| p.valid_in(ctx))
        .map(|p| {
            let insert = match p.param_type {
                ParamType::Str => format!("{}: \"$1\"", p.name),
                ParamType::Int => format!("{}: $1", p.name),
                ParamType::Bool => format!("{}: ${{1:true}}", p.name),
                ParamType::StrArray => format!("{}: [\"$1\"]", p.name),
            };
            let type_label = match p.param_type {
                ParamType::Str => "string",
                ParamType::Int => "integer",
                ParamType::Bool => "boolean",
                ParamType::StrArray => "string[]",
            };
            completion_item(
                p.name,
                CompletionItemKind::PROPERTY,
                &format!("{} ({}) — {}", p.name, type_label, p.description),
                &insert,
                Some(&format!("{}: {}", p.name, type_label)),
            )
        })
        .collect()
}

fn get_stream_operation_completions() -> Vec<CompletionItem> {
    vec![
        completion_item(
            "where",
            CompletionItemKind::METHOD,
            "Filter events by condition",
            "where($1)",
            Some(".where(condition)"),
        ),
        completion_item(
            "select",
            CompletionItemKind::METHOD,
            "Transform/project events",
            "select({ $1 })",
            Some(".select({ field: expr })"),
        ),
        completion_item(
            "aggregate",
            CompletionItemKind::METHOD,
            "Aggregate events in window",
            "aggregate({ $1 })",
            Some(".aggregate({ field: agg_func() })"),
        ),
        completion_item(
            "window",
            CompletionItemKind::METHOD,
            "Define time window",
            "window($1)",
            Some(".window(tumbling(5m))"),
        ),
        completion_item(
            "partition_by",
            CompletionItemKind::METHOD,
            "Partition stream by key",
            "partition_by($1)",
            Some(".partition_by(key)"),
        ),
        completion_item(
            "emit",
            CompletionItemKind::METHOD,
            "Output to console",
            "emit()",
            Some(".emit()"),
        ),
        completion_item(
            "to",
            CompletionItemKind::METHOD,
            "Output to sink. Supports dynamic topic: topic: field or topic: \"prefix-\" + field",
            "to(${1:Connector}, topic: \"${2:topic}\")",
            Some(".to(Connector, topic: \"topic\")"),
        ),
        completion_item(
            "join",
            CompletionItemKind::METHOD,
            "Inner join with another stream",
            "join($1).on($2)",
            Some(".join(other_stream).on(condition)"),
        ),
        completion_item(
            "left_join",
            CompletionItemKind::METHOD,
            "Left outer join (emit even without right match)",
            "left_join(stream ${1:name} = ${2:Source}.on(${3:key}), stream ${4:name2} = ${5:Source2}.on(${6:key}))",
            Some("left_join(stream a = A.on(id), stream b = B.on(id))"),
        ),
        completion_item(
            "right_join",
            CompletionItemKind::METHOD,
            "Right outer join (emit even without left match)",
            "right_join(stream ${1:name} = ${2:Source}.on(${3:key}), stream ${4:name2} = ${5:Source2}.on(${6:key}))",
            Some("right_join(stream a = A.on(id), stream b = B.on(id))"),
        ),
        completion_item(
            "full_join",
            CompletionItemKind::METHOD,
            "Full outer join (emit for either side)",
            "full_join(stream ${1:name} = ${2:Source}.on(${3:key}), stream ${4:name2} = ${5:Source2}.on(${6:key}))",
            Some("full_join(stream a = A.on(id), stream b = B.on(id))"),
        ),
        completion_item(
            "distinct",
            CompletionItemKind::METHOD,
            "Remove duplicates",
            "distinct()",
            Some(".distinct()"),
        ),
        completion_item(
            "limit",
            CompletionItemKind::METHOD,
            "Limit number of events",
            "limit($1)",
            Some(".limit(n)"),
        ),
        completion_item(
            "order_by",
            CompletionItemKind::METHOD,
            "Order events by field",
            "order_by($1)",
            Some(".order_by(field)"),
        ),
        completion_item(
            "tap",
            CompletionItemKind::METHOD,
            "Side-effect for debugging",
            "tap(|e| => $1)",
            Some(".tap(|e| => print(e))"),
        ),
        completion_item(
            "forecast",
            CompletionItemKind::METHOD,
            "PST-based pattern forecasting",
            "forecast(confidence: ${1:0.7}, horizon: ${2:2m}, warmup: ${3:500})",
            Some(".forecast(confidence: 0.7, horizon: 2m)"),
        ),
        completion_item(
            "enrich",
            CompletionItemKind::METHOD,
            "Enrich events from external connector",
            "enrich(${1:Connector}, key: ${2:expr}, fields: [${3:field}], cache_ttl: ${4:5m})",
            Some(".enrich(Connector, key: e.id, fields: [name, category], cache_ttl: 5m)"),
        ),
        completion_item(
            "trend_aggregate",
            CompletionItemKind::METHOD,
            "Hamlet-based trend aggregation over patterns",
            "trend_aggregate(${1:field}: ${2:sum(value)})",
            Some(".trend_aggregate(total: sum(value))"),
        ),
        completion_item(
            "merge",
            CompletionItemKind::METHOD,
            "Merge multiple streams",
            "merge(${1:stream1}, ${2:stream2})",
            Some(".merge(streamA, streamB)"),
        ),
        completion_item(
            "alert",
            CompletionItemKind::METHOD,
            "Send alert notification via webhook",
            "alert(webhook: \"${1:https://...}\", message: \"${2:Alert: {field}}\")",
            Some(".alert(webhook: \"https://hooks.slack.com/...\", message: \"Fraud: {amount}\")"),
        ),
        completion_item(
            "log",
            CompletionItemKind::METHOD,
            "Log events for debugging",
            "log(level: \"${1:info}\", message: \"${2:debug}\")",
            Some(".log(level: \"info\", message: \"event received\")"),
        ),
        completion_item(
            "print",
            CompletionItemKind::METHOD,
            "Print events to stdout",
            "print()",
            Some(".print()"),
        ),
        completion_item(
            "from",
            CompletionItemKind::METHOD,
            "Bind to connector source",
            "from(${1:Connector}, topic: \"${2:topic}\")",
            Some(".from(Connector, topic: \"...\")"),
        ),
        completion_item(
            "within",
            CompletionItemKind::METHOD,
            "Time constraint for sequence patterns",
            "within(${1:5m})",
            Some(".within(5m)"),
        ),
    ]
}

fn get_top_level_completions() -> Vec<CompletionItem> {
    vec![
        completion_item(
            "connector",
            CompletionItemKind::KEYWORD,
            "Declare a data source/sink connection",
            "connector ${1:Name} = ${2:mqtt}(${3:host: \"localhost\", port: 1883})",
            Some("connector Name = mqtt(host: \"localhost\", port: 1883)"),
        ),
        completion_item(
            "event",
            CompletionItemKind::KEYWORD,
            "Declare an event type",
            "event ${1:Name}:\n    ${2:field}: ${3:str}",
            Some("event Name:\n    field: str"),
        ),
        completion_item(
            "stream",
            CompletionItemKind::KEYWORD,
            "Declare a data stream",
            "stream ${1:Name} = ${2:EventType}.from(${3:Connector}, topic: \"${4:topic}\")\n    .where($5)\n    .emit($6)",
            Some("stream Name = EventType.from(Connector, topic: \"...\")"),
        ),
        completion_item(
            "stream (sequence)",
            CompletionItemKind::KEYWORD,
            "Declare a sequence pattern stream",
            "stream ${1:Name} = ${2:EventA} as ${3:a} -> ${4:EventB} as ${5:b}\n    .within(${6:5m})\n    .where(${7:condition})\n    .emit($8)",
            Some("stream Name = EventA as a -> EventB as b\n    .within(5m)"),
        ),
        completion_item(
            "context",
            CompletionItemKind::KEYWORD,
            "Declare an execution context",
            "context ${1:name}(${2:cores: [1, 2]})",
            Some("context name(cores: [1, 2])"),
        ),
        completion_item(
            "pattern",
            CompletionItemKind::KEYWORD,
            "Declare a SASE+ pattern",
            "pattern ${1:Name} = ${2:EventA} -> ${3:EventB} within ${4:5m}",
            Some("pattern Name = EventA -> EventB within duration"),
        ),
        completion_item(
            "let",
            CompletionItemKind::KEYWORD,
            "Declare immutable variable",
            "let ${1:name} = ${2:value}",
            Some("let name = value"),
        ),
        completion_item(
            "var",
            CompletionItemKind::KEYWORD,
            "Declare mutable variable",
            "var ${1:name} = ${2:value}",
            Some("var name = value"),
        ),
        completion_item(
            "const",
            CompletionItemKind::KEYWORD,
            "Declare constant",
            "const ${1:NAME} = ${2:value}",
            Some("const NAME = value"),
        ),
        completion_item(
            "fn",
            CompletionItemKind::KEYWORD,
            "Declare function",
            "fn ${1:name}(${2:params}) -> ${3:ReturnType} {\n    $4\n}",
            Some("fn name(params) -> ReturnType { ... }"),
        ),
    ]
}

fn get_timestamp_completions() -> Vec<CompletionItem> {
    vec![
        completion_item(
            "timestamp",
            CompletionItemKind::VALUE,
            "Current date/time",
            "@${1:2024}-${2:01}-${3:01}T${4:00}:${5:00}:${6:00}Z",
            Some("@2024-01-01T00:00:00Z"),
        ),
        completion_item(
            "date",
            CompletionItemKind::VALUE,
            "Date only",
            "@${1:2024}-${2:01}-${3:01}",
            Some("@2024-01-01"),
        ),
    ]
}

fn get_window_completions() -> Vec<CompletionItem> {
    vec![
        completion_item(
            "tumbling",
            CompletionItemKind::FUNCTION,
            "Non-overlapping fixed windows",
            "tumbling(${1:5m})",
            Some("tumbling(duration)"),
        ),
        completion_item(
            "sliding",
            CompletionItemKind::FUNCTION,
            "Overlapping sliding windows",
            "sliding(${1:10m}, ${2:1m})",
            Some("sliding(size, slide)"),
        ),
        completion_item(
            "session_window",
            CompletionItemKind::FUNCTION,
            "Gap-based session windows",
            "session_window(${1:30s})",
            Some("session_window(gap)"),
        ),
    ]
}

fn get_aggregation_completions() -> Vec<CompletionItem> {
    vec![
        completion_item(
            "sum",
            CompletionItemKind::FUNCTION,
            "Sum of values",
            "sum(${1:field})",
            Some("sum(field)"),
        ),
        completion_item(
            "avg",
            CompletionItemKind::FUNCTION,
            "Average of values",
            "avg(${1:field})",
            Some("avg(field)"),
        ),
        completion_item(
            "count",
            CompletionItemKind::FUNCTION,
            "Count of events",
            "count()",
            Some("count()"),
        ),
        completion_item(
            "min",
            CompletionItemKind::FUNCTION,
            "Minimum value",
            "min(${1:field})",
            Some("min(field)"),
        ),
        completion_item(
            "max",
            CompletionItemKind::FUNCTION,
            "Maximum value",
            "max(${1:field})",
            Some("max(field)"),
        ),
        completion_item(
            "stddev",
            CompletionItemKind::FUNCTION,
            "Standard deviation",
            "stddev(${1:field})",
            Some("stddev(field)"),
        ),
        completion_item(
            "variance",
            CompletionItemKind::FUNCTION,
            "Variance",
            "variance(${1:field})",
            Some("variance(field)"),
        ),
        completion_item(
            "first",
            CompletionItemKind::FUNCTION,
            "First value in window",
            "first(${1:field})",
            Some("first(field)"),
        ),
        completion_item(
            "last",
            CompletionItemKind::FUNCTION,
            "Last value in window",
            "last(${1:field})",
            Some("last(field)"),
        ),
        completion_item(
            "collect",
            CompletionItemKind::FUNCTION,
            "Collect all values",
            "collect(${1:field})",
            Some("collect(field)"),
        ),
        completion_item(
            "distinct",
            CompletionItemKind::FUNCTION,
            "Distinct values",
            "distinct(${1:field})",
            Some("distinct(field)"),
        ),
        completion_item(
            "median",
            CompletionItemKind::FUNCTION,
            "Median (50th percentile)",
            "median(${1:field})",
            Some("median(field)"),
        ),
        completion_item(
            "percentile",
            CompletionItemKind::FUNCTION,
            "Percentile (0.0–1.0 quantile)",
            "percentile(${1:field}, ${2:0.99})",
            Some("percentile(field, 0.99)"),
        ),
        completion_item(
            "p50",
            CompletionItemKind::FUNCTION,
            "50th percentile",
            "p50(${1:field})",
            Some("p50(field)"),
        ),
        completion_item(
            "p95",
            CompletionItemKind::FUNCTION,
            "95th percentile",
            "p95(${1:field})",
            Some("p95(field)"),
        ),
        completion_item(
            "p99",
            CompletionItemKind::FUNCTION,
            "99th percentile",
            "p99(${1:field})",
            Some("p99(field)"),
        ),
    ]
}

fn get_pattern_completions() -> Vec<CompletionItem> {
    vec![
        completion_item(
            "->",
            CompletionItemKind::KEYWORD,
            "Sequence operator (followed by)",
            "-> ${1:EventType}",
            Some("EventA -> EventB"),
        ),
        completion_item(
            "all",
            CompletionItemKind::KEYWORD,
            "Kleene+ (one or more)",
            "all ${1:EventType}",
            Some("-> all EventType"),
        ),
        completion_item(
            "NOT",
            CompletionItemKind::KEYWORD,
            "Event must not occur",
            "NOT ${1:EventType}",
            Some("-> NOT EventType"),
        ),
        completion_item(
            "within",
            CompletionItemKind::KEYWORD,
            "Time constraint",
            "within ${1:5m}",
            Some("within duration"),
        ),
    ]
}

fn get_type_completions() -> Vec<CompletionItem> {
    vec![
        completion_item(
            "int",
            CompletionItemKind::TYPE_PARAMETER,
            "Integer type",
            "int",
            None,
        ),
        completion_item(
            "float",
            CompletionItemKind::TYPE_PARAMETER,
            "Float type",
            "float",
            None,
        ),
        completion_item(
            "bool",
            CompletionItemKind::TYPE_PARAMETER,
            "Boolean type",
            "bool",
            None,
        ),
        completion_item(
            "str",
            CompletionItemKind::TYPE_PARAMETER,
            "String type",
            "str",
            None,
        ),
        completion_item(
            "timestamp",
            CompletionItemKind::TYPE_PARAMETER,
            "Timestamp type",
            "timestamp",
            None,
        ),
        completion_item(
            "duration",
            CompletionItemKind::TYPE_PARAMETER,
            "Duration type",
            "duration",
            None,
        ),
        completion_item(
            "list",
            CompletionItemKind::TYPE_PARAMETER,
            "List type",
            "list",
            None,
        ),
        completion_item(
            "map",
            CompletionItemKind::TYPE_PARAMETER,
            "Map type",
            "map",
            None,
        ),
        completion_item(
            "any",
            CompletionItemKind::TYPE_PARAMETER,
            "Any type",
            "any",
            None,
        ),
        completion_item(
            "Stream",
            CompletionItemKind::TYPE_PARAMETER,
            "Stream type",
            "Stream<${1:T}>",
            Some("Stream<T>"),
        ),
    ]
}

fn get_expression_completions() -> Vec<CompletionItem> {
    let mut items = vec![
        completion_item(
            "if",
            CompletionItemKind::KEYWORD,
            "Conditional expression",
            "if ${1:condition} then\n    ${2:expr}\nelse\n    ${3:expr}",
            Some("if ... then ... else ..."),
        ),
        completion_item(
            "match",
            CompletionItemKind::KEYWORD,
            "Pattern matching",
            "match ${1:value} {\n    ${2:pattern} => ${3:result},\n    _ => ${4:default}\n}",
            Some("match value { pattern => result }"),
        ),
        completion_item(
            "for",
            CompletionItemKind::KEYWORD,
            "For loop",
            "for ${1:item} in ${2:collection} {\n    $3\n}",
            Some("for item in collection { ... }"),
        ),
        // Built-in functions
        completion_item(
            "now",
            CompletionItemKind::FUNCTION,
            "Current timestamp",
            "now()",
            None,
        ),
        completion_item(
            "len",
            CompletionItemKind::FUNCTION,
            "Length of collection",
            "len(${1:collection})",
            None,
        ),
        completion_item(
            "abs",
            CompletionItemKind::FUNCTION,
            "Absolute value",
            "abs(${1:number})",
            None,
        ),
        completion_item(
            "sqrt",
            CompletionItemKind::FUNCTION,
            "Square root",
            "sqrt(${1:number})",
            None,
        ),
        completion_item(
            "floor",
            CompletionItemKind::FUNCTION,
            "Round down",
            "floor(${1:number})",
            None,
        ),
        completion_item(
            "ceil",
            CompletionItemKind::FUNCTION,
            "Round up",
            "ceil(${1:number})",
            None,
        ),
        completion_item(
            "round",
            CompletionItemKind::FUNCTION,
            "Round to nearest",
            "round(${1:number})",
            None,
        ),
        completion_item(
            "map",
            CompletionItemKind::FUNCTION,
            "Transform collection",
            "map(${1:collection}, |${2:x}| => ${3:expr})",
            None,
        ),
        completion_item(
            "filter",
            CompletionItemKind::FUNCTION,
            "Filter collection",
            "filter(${1:collection}, |${2:x}| => ${3:condition})",
            None,
        ),
        completion_item(
            "reduce",
            CompletionItemKind::FUNCTION,
            "Reduce collection",
            "reduce(${1:collection}, ${2:initial}, |${3:acc}, ${4:x}| => ${5:expr})",
            None,
        ),
        // Logical
        completion_item(
            "true",
            CompletionItemKind::CONSTANT,
            "Boolean true",
            "true",
            None,
        ),
        completion_item(
            "false",
            CompletionItemKind::CONSTANT,
            "Boolean false",
            "false",
            None,
        ),
        completion_item(
            "null",
            CompletionItemKind::CONSTANT,
            "Null value",
            "null",
            None,
        ),
        completion_item(
            "and",
            CompletionItemKind::KEYWORD,
            "Logical AND",
            "and",
            None,
        ),
        completion_item("or", CompletionItemKind::KEYWORD, "Logical OR", "or", None),
        completion_item(
            "not",
            CompletionItemKind::KEYWORD,
            "Logical NOT",
            "not",
            None,
        ),
    ];

    // Add aggregation functions that can be used in expressions
    items.extend(get_aggregation_completions());

    items
}

fn completion_item(
    label: &str,
    kind: CompletionItemKind,
    detail: &str,
    insert_text: &str,
    doc: Option<&str>,
) -> CompletionItem {
    CompletionItem {
        label: label.to_string(),
        kind: Some(kind),
        detail: Some(detail.to_string()),
        documentation: doc.map(|d| {
            Documentation::MarkupContent(MarkupContent {
                kind: MarkupKind::Markdown,
                value: format!("```vpl\n{d}\n```"),
            })
        }),
        insert_text: Some(insert_text.to_string()),
        insert_text_format: Some(InsertTextFormat::SNIPPET),
        ..Default::default()
    }
}

fn get_connector_type_completions() -> Vec<CompletionItem> {
    vec![
        completion_item(
            "mqtt",
            CompletionItemKind::CLASS,
            "MQTT message broker",
            "mqtt(host: \"${1:localhost}\", port: ${2:1883})",
            Some("mqtt(host: \"localhost\", port: 1883)"),
        ),
        completion_item(
            "kafka",
            CompletionItemKind::CLASS,
            "Apache Kafka cluster",
            "kafka(brokers: [\"${1:localhost:9092}\"])",
            Some("kafka(brokers: [\"localhost:9092\"])"),
        ),
        completion_item(
            "nats",
            CompletionItemKind::CLASS,
            "NATS messaging system",
            "nats(url: \"${1:nats://localhost:4222}\")",
            Some("nats(url: \"nats://localhost:4222\")"),
        ),
        completion_item(
            "http",
            CompletionItemKind::CLASS,
            "HTTP API endpoint",
            "http(base_url: \"${1:https://api.example.com}\")",
            Some("http(base_url: \"https://api.example.com\")"),
        ),
        completion_item(
            "websocket",
            CompletionItemKind::CLASS,
            "WebSocket connection",
            "websocket(url: \"${1:ws://localhost:8080}\")",
            Some("websocket(url: \"ws://localhost:8080\")"),
        ),
        completion_item(
            "file",
            CompletionItemKind::CLASS,
            "File source/sink",
            "file(path: \"${1:data.jsonl}\")",
            Some("file(path: \"data.jsonl\")"),
        ),
        completion_item(
            "postgres_cdc",
            CompletionItemKind::CLASS,
            "PostgreSQL CDC (change data capture via logical replication)",
            "postgres_cdc(host: \"${1:localhost}\", dbname: \"${2:mydb}\", publication: \"${3:my_pub}\")",
            Some("postgres_cdc(host: \"localhost\", dbname: \"mydb\", publication: \"my_pub\")"),
        ),
    ]
}

fn get_forecast_param_completions() -> Vec<CompletionItem> {
    vec![
        completion_item(
            "mode",
            CompletionItemKind::PROPERTY,
            "Preset mode: \"fast\", \"balanced\" (default), or \"accurate\"",
            "mode: \"${1:balanced}\"",
            Some("mode: \"balanced\""),
        ),
        completion_item(
            "confidence",
            CompletionItemKind::PROPERTY,
            "Minimum probability to emit forecast (0.0-1.0, default 0.5)",
            "confidence: ${1:0.7}",
            Some("confidence: 0.7"),
        ),
        completion_item(
            "horizon",
            CompletionItemKind::PROPERTY,
            "Forecast time window (default = within duration)",
            "horizon: ${1:2m}",
            Some("horizon: 2m"),
        ),
        completion_item(
            "warmup",
            CompletionItemKind::PROPERTY,
            "Min events before forecasting starts (default 100)",
            "warmup: ${1:500}",
            Some("warmup: 500"),
        ),
        completion_item(
            "max_depth",
            CompletionItemKind::PROPERTY,
            "PST context tree depth (default 3)",
            "max_depth: ${1:5}",
            Some("max_depth: 5"),
        ),
        completion_item(
            "hawkes",
            CompletionItemKind::PROPERTY,
            "Enable Hawkes intensity modulation (default true)",
            "hawkes: ${1:true}",
            Some("hawkes: true"),
        ),
        completion_item(
            "conformal",
            CompletionItemKind::PROPERTY,
            "Enable conformal prediction intervals (default true)",
            "conformal: ${1:true}",
            Some("conformal: true"),
        ),
    ]
}

/// Find the source event type(s) for the stream enclosing the given cursor position.
///
/// Scans backwards from `position` to find the nearest `stream ... = EventType` declaration.
/// For sequence patterns (`EventA as a -> EventB as b`), returns all event types.
/// Returns `None` if no enclosing stream is found.
fn find_enclosing_stream_event_types(text: &str, position: Position) -> Option<Vec<String>> {
    let lines: Vec<&str> = text.lines().collect();
    // Scan backwards from cursor line to find the stream declaration
    for i in (0..=position.line as usize).rev() {
        let line = lines.get(i)?;
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("stream ") {
            // Parse: stream Name = EventType... or stream Name = EventA as a -> EventB as b...
            if let Some(eq_idx) = rest.find('=') {
                let after_eq = rest[eq_idx + 1..].trim();
                // For multi-line declarations (merge, join), collect continuation lines
                let mut full_rhs = after_eq.to_string();
                if after_eq.starts_with("merge(") || after_eq.starts_with("join(") {
                    // Count parens to find closing paren
                    let mut depth: i32 = 0;
                    for c in full_rhs.chars() {
                        match c {
                            '(' => depth += 1,
                            ')' => depth -= 1,
                            _ => {}
                        }
                    }
                    // If unclosed, append following lines
                    let mut j = i + 1;
                    while depth > 0 && j < lines.len() && j <= position.line as usize {
                        full_rhs.push(' ');
                        full_rhs.push_str(lines[j].trim());
                        for c in lines[j].chars() {
                            match c {
                                '(' => depth += 1,
                                ')' => depth -= 1,
                                _ => {}
                            }
                        }
                        j += 1;
                    }
                }
                return Some(extract_event_types_from_stream_rhs(&full_rhs));
            }
        }
        // If we hit another top-level declaration, stop searching
        if !trimmed.is_empty()
            && !line.starts_with(' ')
            && !line.starts_with('\t')
            && (trimmed.starts_with("event ")
                || trimmed.starts_with("connector ")
                || trimmed.starts_with("pattern ")
                || trimmed.starts_with("fn ")
                || trimmed.starts_with("let ")
                || trimmed.starts_with("var ")
                || trimmed.starts_with("const "))
        {
            return None;
        }
    }
    None
}

/// Extract event type names from the RHS of a stream declaration.
///
/// Handles:
/// - `EventType.from(...)` → `["EventType"]`
/// - `EventType.where(...)` → `["EventType"]`
/// - `EventA as a -> EventB as b` → `["EventA", "EventB"]`
/// - `merge(stream high = Source.where(...), stream low = Source2.where(...))` → `["Source", "Source2"]`
/// - `merge(StreamA, StreamB)` → `["StreamA", "StreamB"]`
fn extract_event_types_from_stream_rhs(rhs: &str) -> Vec<String> {
    let mut types = Vec::new();

    // Handle merge(...) syntax
    if let Some(rest) = rhs.strip_prefix("merge(") {
        // Extract event types from inline stream list
        // Formats: "stream high = Source.where(...), stream low = Source2" or "StreamA, StreamB"
        let content = rest.trim_end_matches(')');
        for part in split_merge_parts(content) {
            let trimmed = part.trim();
            if let Some(after_stream) = trimmed.strip_prefix("stream ") {
                // "stream high = Source.where(...)" → extract Source after '='
                if let Some(eq_idx) = after_stream.find('=') {
                    let source_part = after_stream[eq_idx + 1..].trim();
                    let end = source_part
                        .find(|c: char| !c.is_alphanumeric() && c != '_')
                        .unwrap_or(source_part.len());
                    let name = &source_part[..end];
                    if !name.is_empty() {
                        types.push(name.to_string());
                    }
                }
            } else {
                // Simple identifier: "StreamA"
                let end = trimmed
                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(trimmed.len());
                let name = &trimmed[..end];
                if !name.is_empty() {
                    types.push(name.to_string());
                }
            }
        }
        return types;
    }

    // Split on `->` for sequence patterns, then extract the first identifier from each part
    for part in rhs.split("->") {
        let trimmed = part.trim();
        // The event type is the first PascalCase identifier
        let end = trimmed
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(trimmed.len());
        let name = &trimmed[..end];
        if !name.is_empty()
            && name
                .chars()
                .next()
                .is_some_and(|c| c.is_uppercase() || c == '_')
        {
            types.push(name.to_string());
        }
    }
    types
}

/// Split merge content by commas, respecting parenthesized sub-expressions.
fn split_merge_parts(content: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in content.char_indices() {
        match c {
            '(' => depth += 1,
            ')' if depth > 0 => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&content[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < content.len() {
        parts.push(&content[start..]);
    }
    parts
}

/// Collect (field_name, type_name) pairs from event declarations matching the given event type names.
/// If `event_types` is `None`, collects from all events (fallback).
fn collect_event_fields(text: &str, event_types: Option<&[String]>) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    let mut in_event = false;
    let mut current_event_matches = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("event ") {
            in_event = true;
            let event_name = rest.trim_end_matches(':').trim();
            current_event_matches = match event_types {
                Some(types) => types.iter().any(|t| t == event_name),
                None => true,
            };
            continue;
        }
        if in_event {
            if trimmed.is_empty()
                || (!trimmed.starts_with(char::is_whitespace)
                    && !line.starts_with(' ')
                    && !line.starts_with('\t'))
            {
                in_event = false;
                current_event_matches = false;
            } else if current_event_matches {
                if let Some(colon_idx) = trimmed.find(':') {
                    let field_name = trimmed[..colon_idx].trim();
                    let type_name = trimmed[colon_idx + 1..].trim();
                    if !field_name.is_empty()
                        && field_name
                            .chars()
                            .next()
                            .is_some_and(|c| c.is_alphabetic() || c == '_')
                    {
                        fields.push((field_name.to_string(), type_name.to_string()));
                    }
                }
            }
        }
    }
    fields
}

/// Forecast built-in variables (name, description).
const FORECAST_BUILTIN_VARS: &[(&str, &str)] = &[
    ("forecast_probability", "Completion probability (0.0-1.0)"),
    ("forecast_confidence", "Prediction stability (0.0-1.0)"),
    ("forecast_time", "Expected time to completion (ns)"),
    ("forecast_state", "Current NFA state label"),
    ("forecast_context_depth", "PST context depth used"),
    ("forecast_lower", "Conformal interval lower bound"),
    ("forecast_upper", "Conformal interval upper bound"),
];

/// Enrich built-in variables (name, description).
const ENRICH_BUILTIN_VARS: &[(&str, &str)] = &[
    (
        "enrich_status",
        "Enrich lookup status (ok/error/cached/timeout)",
    ),
    ("enrich_latency_ms", "Enrich lookup latency in milliseconds"),
];

/// Suggest fields available in .emit() based on upstream event fields and built-in variables
fn get_emit_field_completions(text: &str, position: Position) -> Vec<CompletionItem> {
    let mut items = Vec::new();
    let event_types = find_enclosing_stream_event_types(text, position);

    for (field_name, _type_name) in collect_event_fields(text, event_types.as_deref()) {
        items.push(completion_item(
            &field_name,
            CompletionItemKind::FIELD,
            "Event field",
            &format!("{field_name}: {field_name}"),
            Some(&format!("{field_name}: {field_name}")),
        ));
    }

    // Add forecast built-in variables if .forecast() is present
    if text.contains(".forecast(") {
        for (name, desc) in FORECAST_BUILTIN_VARS {
            items.push(completion_item(
                name,
                CompletionItemKind::VARIABLE,
                desc,
                &format!("{name}: {name}"),
                Some(&format!("{name}: {name}")),
            ));
        }
    }

    // Add enrich built-in variables if .enrich() is present
    if text.contains(".enrich(") {
        for (name, desc) in ENRICH_BUILTIN_VARS {
            items.push(completion_item(
                name,
                CompletionItemKind::VARIABLE,
                desc,
                &format!("{name}: {name}"),
                Some(&format!("{name}: {name}")),
            ));
        }
    }

    // Add common emit patterns
    if items.is_empty() {
        items.push(completion_item(
            "alert",
            CompletionItemKind::FIELD,
            "Alert type label",
            "alert: \"${1:ALERT_TYPE}\"",
            Some("alert: \"ALERT_TYPE\""),
        ));
        items.push(completion_item(
            "severity",
            CompletionItemKind::FIELD,
            "Alert severity level",
            "severity: \"${1:warning}\"",
            Some("severity: \"warning\""),
        ));
    }

    items
}

/// Suggest fields available in .where() — bare field names for boolean expressions
fn get_where_field_completions(text: &str, position: Position) -> Vec<CompletionItem> {
    let mut items = Vec::new();
    let event_types = find_enclosing_stream_event_types(text, position);

    for (field_name, type_name) in collect_event_fields(text, event_types.as_deref()) {
        items.push(completion_item(
            &field_name,
            CompletionItemKind::FIELD,
            &format!("Event field ({type_name})"),
            &field_name,
            Some(&field_name),
        ));
    }

    // Add forecast built-in variables if .forecast() is present
    if text.contains(".forecast(") {
        for (name, desc) in FORECAST_BUILTIN_VARS {
            items.push(completion_item(
                name,
                CompletionItemKind::VARIABLE,
                desc,
                name,
                Some(name),
            ));
        }
    }

    // Add enrich built-in variables if .enrich() is present
    if text.contains(".enrich(") {
        for (name, desc) in ENRICH_BUILTIN_VARS {
            items.push(completion_item(
                name,
                CompletionItemKind::VARIABLE,
                desc,
                name,
                Some(name),
            ));
        }
    }

    items
}

/// Suggest fields available in .select() — `field: field` pattern like emit
fn get_select_field_completions(text: &str, position: Position) -> Vec<CompletionItem> {
    let mut items = Vec::new();
    let event_types = find_enclosing_stream_event_types(text, position);

    for (field_name, _type_name) in collect_event_fields(text, event_types.as_deref()) {
        items.push(completion_item(
            &field_name,
            CompletionItemKind::FIELD,
            "Event field",
            &format!("{field_name}: {field_name}"),
            Some(&format!("{field_name}: {field_name}")),
        ));
    }

    // Add forecast built-in variables if .forecast() is present
    if text.contains(".forecast(") {
        for (name, desc) in FORECAST_BUILTIN_VARS {
            items.push(completion_item(
                name,
                CompletionItemKind::VARIABLE,
                desc,
                &format!("{name}: {name}"),
                Some(&format!("{name}: {name}")),
            ));
        }
    }

    // Add enrich built-in variables if .enrich() is present
    if text.contains(".enrich(") {
        for (name, desc) in ENRICH_BUILTIN_VARS {
            items.push(completion_item(
                name,
                CompletionItemKind::VARIABLE,
                desc,
                &format!("{name}: {name}"),
                Some(&format!("{name}: {name}")),
            ));
        }
    }

    items
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_operation_completions_after_dot() {
        let text = "stream X = Event.from(Connector).\n";
        let position = Position {
            line: 0,
            character: 33,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"where"));
        assert!(labels.contains(&"emit"));
        assert!(labels.contains(&"window"));
    }

    #[test]
    fn test_top_level_completions() {
        let text = "";
        let position = Position {
            line: 0,
            character: 0,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"stream"));
        assert!(labels.contains(&"event"));
        assert!(labels.contains(&"connector"));
        assert!(labels.contains(&"pattern"));
        assert!(labels.contains(&"fn"));
    }

    #[test]
    fn test_aggregation_completions_in_aggregate() {
        let text = "stream X = Y.aggregate(";
        let position = Position {
            line: 0,
            character: 23,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"sum"));
        assert!(labels.contains(&"avg"));
        assert!(labels.contains(&"count"));
        assert!(labels.contains(&"min"));
        assert!(labels.contains(&"max"));
    }

    #[test]
    fn test_type_completions_after_colon() {
        let text = "event X:\n    field:";
        let position = Position {
            line: 1,
            character: 10,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"int"));
        assert!(labels.contains(&"float"));
        assert!(labels.contains(&"str"));
        assert!(labels.contains(&"timestamp"));
    }

    #[test]
    fn test_pattern_completions() {
        let text = "pattern X = ";
        let position = Position {
            line: 0,
            character: 12,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"->"));
        assert!(labels.contains(&"all"));
        assert!(labels.contains(&"NOT"));
    }

    #[test]
    fn test_window_completions() {
        let text = "stream X = Y.window(";
        let position = Position {
            line: 0,
            character: 20,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"tumbling"));
        assert!(labels.contains(&"sliding"));
    }

    #[test]
    fn test_completion_item_has_snippet_format() {
        let item = completion_item(
            "test",
            CompletionItemKind::FUNCTION,
            "Test item",
            "test($1)",
            Some("test(arg)"),
        );
        assert_eq!(item.insert_text_format, Some(InsertTextFormat::SNIPPET));
        assert_eq!(item.insert_text, Some("test($1)".to_string()));
    }

    #[test]
    fn test_from_params_mqtt_completions() {
        let text = "connector MyMqtt = mqtt(url: \"localhost\")\nstream X = Event.from(MyMqtt, ";
        let position = Position {
            line: 1,
            character: 30,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"topic"));
        assert!(labels.contains(&"client_id"));
        assert!(labels.contains(&"qos"));
    }

    #[test]
    fn test_to_params_kafka_completions() {
        let text =
            "connector MyKafka = kafka(url: \"localhost\")\nstream X = Event\n    .to(MyKafka, ";
        let position = Position {
            line: 2,
            character: 18,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"topic"));
        assert!(labels.contains(&"partition"));
        assert!(labels.contains(&"group_id"));
    }

    #[test]
    fn test_where_field_completions() {
        let text = "event Temperature:\n    value: float\n    zone: str\n\nstream X = Temperature\n    .where(";
        let position = Position {
            line: 5,
            character: 11,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(
            labels.contains(&"value"),
            "Expected 'value' in completions, got: {labels:?}"
        );
        assert!(labels.contains(&"zone"));
    }

    #[test]
    fn test_select_field_completions() {
        let text =
            "event Trade:\n    price: float\n    volume: int\n\nstream X = Trade\n    .select(";
        let position = Position {
            line: 5,
            character: 12,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"price"));
        assert!(labels.contains(&"volume"));
    }

    #[test]
    fn test_where_with_forecast_builtins() {
        let text = "event A:\n    x: int\n\nstream S = A as a -> A as b\n    .forecast(confidence: 0.7)\n    .where(";
        let position = Position {
            line: 5,
            character: 11,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"x"));
        assert!(labels.contains(&"forecast_probability"));
        assert!(labels.contains(&"forecast_time"));
    }

    #[test]
    fn test_http_connector_uses_base_url() {
        let text = "connector C = ";
        let position = Position {
            line: 0,
            character: 14,
        };
        let completions = get_completions(text, position);
        let http_item = completions.iter().find(|c| c.label == "http").unwrap();
        let insert = http_item.insert_text.as_deref().unwrap();
        assert!(
            insert.contains("base_url"),
            "HTTP snippet should use base_url, got: {insert}"
        );
        assert!(
            !insert.contains("http(url:"),
            "Should not contain http(url:"
        );
    }

    #[test]
    fn test_where_only_shows_source_event_fields() {
        // Two event types, stream uses only Temperature — should NOT suggest 'price' or 'volume'
        let text = "event Temperature:\n    value: float\n    zone: str\n\nevent Trade:\n    price: float\n    volume: int\n\nstream X = Temperature\n    .where(";
        let position = Position {
            line: 9,
            character: 11,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"value"));
        assert!(labels.contains(&"zone"));
        assert!(
            !labels.contains(&"price"),
            "Should not suggest fields from unrelated event Trade"
        );
        assert!(!labels.contains(&"volume"));
    }

    #[test]
    fn test_where_sequence_shows_all_event_fields() {
        // Sequence pattern with two event types — should suggest fields from both
        let text = "event Login:\n    user_id: str\n\nevent Purchase:\n    amount: float\n\nstream S = Login as l -> Purchase as p\n    .where(";
        let position = Position {
            line: 7,
            character: 11,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"user_id"));
        assert!(labels.contains(&"amount"));
    }

    #[test]
    fn test_where_same_line_only_source_fields() {
        // User's exact test case: A and B declared, stream uses A.where(
        let text = "event A:\n    id: str\n\nevent B:\n    name: str\n\nstream S = A.where(";
        let position = Position {
            line: 6,
            character: 19,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(
            labels.contains(&"id"),
            "Should suggest 'id' from event A, got: {labels:?}"
        );
        assert!(
            !labels.contains(&"name"),
            "Should NOT suggest 'name' from event B, got: {labels:?}"
        );
    }

    #[test]
    fn test_resolve_connector_type_from_document() {
        let text = "connector MarketData = mqtt(url: \"broker.example.com\")\n";
        assert_eq!(
            resolve_connector_type(text, "MarketData"),
            Some("mqtt".to_string())
        );
    }

    #[test]
    fn test_merge_simple_completions() {
        // merge(StreamA, StreamB) — both are event type names
        let text = "event Temperature:\n    value: float\n\nevent Humidity:\n    level: float\n\nstream Combined = merge(Temperature, Humidity)\n    .where(";
        let position = Position {
            line: 7,
            character: 11,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(
            labels.contains(&"value"),
            "Should suggest 'value' from Temperature, got: {labels:?}"
        );
        assert!(
            labels.contains(&"level"),
            "Should suggest 'level' from Humidity, got: {labels:?}"
        );
    }

    #[test]
    fn test_merge_inline_stream_completions() {
        // merge(stream high = Source.where(...), stream low = Source.where(...))
        let text = "event Source:\n    value: float\n    zone: str\n\nstream Combined = merge(\n    stream high = Source.where(value > 100),\n    stream low = Source.where(value < 10)\n)\n    .where(";
        let position = Position {
            line: 8,
            character: 11,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(
            labels.contains(&"value"),
            "Should suggest 'value' from Source, got: {labels:?}"
        );
        assert!(
            labels.contains(&"zone"),
            "Should suggest 'zone' from Source, got: {labels:?}"
        );
    }

    #[test]
    fn test_extract_event_types_merge_simple() {
        let types = extract_event_types_from_stream_rhs("merge(StreamA, StreamB)");
        assert_eq!(types, vec!["StreamA", "StreamB"]);
    }

    #[test]
    fn test_extract_event_types_merge_inline() {
        let types = extract_event_types_from_stream_rhs(
            "merge( stream high = Source.where(value > 100), stream low = Source.where(value < 10) )",
        );
        assert_eq!(types, vec!["Source", "Source"]);
    }
}
