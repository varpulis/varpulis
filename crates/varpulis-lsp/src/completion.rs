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

    // Check if we're after a dot (stream operation)
    if prefix.trim_end().ends_with('.') {
        return CompletionContext::StreamOperation;
    }

    // Check if we're after @ (timestamp literal)
    if prefix.trim_end().ends_with('@') {
        return CompletionContext::AfterAt;
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
    if prefix.contains("pattern ") || prefix.contains("SEQ(") || prefix.contains("AND(") {
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
            };
            let type_label = match p.param_type {
                ParamType::Str => "string",
                ParamType::Int => "integer",
                ParamType::Bool => "boolean",
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
            "Output to sink",
            "to(\"$1\")",
            Some(".to(\"sink://...\")"),
        ),
        completion_item(
            "join",
            CompletionItemKind::METHOD,
            "Join with another stream",
            "join($1).on($2)",
            Some(".join(other_stream).on(condition)"),
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
    ]
}

fn get_top_level_completions() -> Vec<CompletionItem> {
    vec![
        completion_item(
            "stream",
            CompletionItemKind::KEYWORD,
            "Declare a data stream",
            "stream ${1:Name} = ${2:Source}\n    .where($3)\n    .emit()",
            Some("stream Name = Source"),
        ),
        completion_item(
            "event",
            CompletionItemKind::KEYWORD,
            "Declare an event type",
            "event ${1:Name} {\n    ${2:field}: ${3:type}\n}",
            Some("event Name { field: type }"),
        ),
        completion_item(
            "pattern",
            CompletionItemKind::KEYWORD,
            "Declare a SASE+ pattern",
            "pattern ${1:Name} = SEQ(${2:a}: ${3:EventType}) within ${4:5m}",
            Some("pattern Name = SEQ(...) within duration"),
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
        completion_item(
            "config",
            CompletionItemKind::KEYWORD,
            "Configuration block",
            "config {\n    $1\n}",
            Some("config { ... }"),
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
    ]
}

fn get_pattern_completions() -> Vec<CompletionItem> {
    vec![
        completion_item(
            "SEQ",
            CompletionItemKind::KEYWORD,
            "Sequence of events",
            "SEQ(${1:a}: ${2:EventA}, ${3:b}: ${4:EventB})",
            Some("SEQ(a: EventA, b: EventB)"),
        ),
        completion_item(
            "AND",
            CompletionItemKind::KEYWORD,
            "All events must occur",
            "AND(${1:a}: ${2:EventA}, ${3:b}: ${4:EventB})",
            Some("AND(a: EventA, b: EventB)"),
        ),
        completion_item(
            "OR",
            CompletionItemKind::KEYWORD,
            "Any event may occur",
            "OR(${1:a}: ${2:EventA}, ${3:b}: ${4:EventB})",
            Some("OR(a: EventA, b: EventB)"),
        ),
        completion_item(
            "NOT",
            CompletionItemKind::KEYWORD,
            "Event must not occur",
            "NOT(${1:EventType})",
            Some("NOT(EventType)"),
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
                value: format!("```vpl\n{}\n```", d),
            })
        }),
        insert_text: Some(insert_text.to_string()),
        insert_text_format: Some(InsertTextFormat::SNIPPET),
        ..Default::default()
    }
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
        let text = "pattern X = SEQ(";
        let position = Position {
            line: 0,
            character: 16,
        };
        let completions = get_completions(text, position);
        let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
        assert!(labels.contains(&"SEQ"));
        assert!(labels.contains(&"AND"));
        assert!(labels.contains(&"OR"));
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
        // group_id is source-only, should not appear in .to() context
        assert!(!labels.contains(&"group_id"));
    }

    #[test]
    fn test_resolve_connector_type_from_document() {
        let text = "connector MarketData = mqtt(url: \"broker.example.com\")\n";
        assert_eq!(
            resolve_connector_type(text, "MarketData"),
            Some("mqtt".to_string())
        );
    }
}
