//! Schema inference command for Varpulis
//!
//! Reads event files (.evt or JSONL) and infers VPL `event` type declarations
//! by sampling event data and applying type promotion rules.

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum InferredType {
    Int,
    Float,
    Str,
    Bool,
    Null,
    Array,
}

/// Infer a type from a serde_json::Value
fn infer_json_type(value: &serde_json::Value) -> InferredType {
    match value {
        serde_json::Value::Null => InferredType::Null,
        serde_json::Value::Bool(_) => InferredType::Bool,
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                InferredType::Int
            } else {
                InferredType::Float
            }
        }
        serde_json::Value::String(_) => InferredType::Str,
        serde_json::Value::Array(_) => InferredType::Array,
        serde_json::Value::Object(_) => InferredType::Str, // Nested objects treated as str
    }
}

/// Infer a type from a .evt field value string
fn infer_evt_type(value: &str) -> InferredType {
    let value = value.trim();

    if value == "null" || value == "nil" {
        return InferredType::Null;
    }
    if value == "true" || value == "false" {
        return InferredType::Bool;
    }
    if value.starts_with('[') && value.ends_with(']') {
        return InferredType::Array;
    }
    // Quoted string
    if (value.starts_with('"') && value.ends_with('"'))
        || (value.starts_with('\'') && value.ends_with('\''))
    {
        return InferredType::Str;
    }
    // Try integer
    if value.parse::<i64>().is_ok() {
        return InferredType::Int;
    }
    // Try float
    if value.parse::<f64>().is_ok() {
        return InferredType::Float;
    }
    // Unquoted string / identifier
    InferredType::Str
}

/// Apply type promotion rules to a set of observed types and return the VPL type name
fn promote_types(types: &HashSet<InferredType>) -> &'static str {
    // Filter out Null for promotion decisions
    let non_null: HashSet<&InferredType> =
        types.iter().filter(|t| **t != InferredType::Null).collect();

    // If only Null was observed, default to str
    if non_null.is_empty() {
        return "str";
    }

    // Any set with Array -> list
    if non_null.contains(&InferredType::Array) {
        return "list";
    }

    // Single non-null type
    if non_null.len() == 1 {
        let t = non_null.into_iter().next().unwrap();
        return match t {
            InferredType::Int => "int",
            InferredType::Float => "float",
            InferredType::Str => "str",
            InferredType::Bool => "bool",
            InferredType::Null => unreachable!(),
            InferredType::Array => "list",
        };
    }

    // {Int, Float} -> float
    if non_null.contains(&InferredType::Int)
        && non_null.contains(&InferredType::Float)
        && non_null.len() == 2
    {
        return "float";
    }

    // Any mix involving Str -> str
    if non_null.contains(&InferredType::Str) {
        return "str";
    }

    // {Int, Float} already handled; {Int, Bool} or {Float, Bool} or other combos -> str
    "str"
}

/// Try to parse a line as JSONL and extract event_type + field types
fn parse_jsonl_line(
    line: &str,
    schema: &mut BTreeMap<String, BTreeMap<String, HashSet<InferredType>>>,
) -> Result<bool> {
    let json: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => return Ok(false),
    };

    let event_type = match json.get("event_type").and_then(|v| v.as_str()) {
        Some(et) => et.to_string(),
        None => return Ok(false),
    };

    let fields = schema.entry(event_type).or_default();

    if let Some(data) = json.get("data").and_then(|v| v.as_object()) {
        // Nested format: {"event_type": "X", "data": {fields...}}
        for (key, value) in data {
            let types = fields.entry(key.clone()).or_default();
            types.insert(infer_json_type(value));
        }
    } else if let Some(obj) = json.as_object() {
        // Flat format: {"event_type": "X", "field1": val1, "field2": val2}
        for (key, value) in obj {
            if key == "event_type" || key == "timestamp" {
                continue;
            }
            let types = fields.entry(key.clone()).or_default();
            types.insert(infer_json_type(value));
        }
    }

    Ok(true)
}

/// Try to parse a line as .evt format and extract event_type + field types.
/// Format: EventType { field: value, field2: value2 }
fn parse_evt_line(
    line: &str,
    schema: &mut BTreeMap<String, BTreeMap<String, HashSet<InferredType>>>,
) -> Result<bool> {
    let line = line.trim().trim_end_matches(';');

    let brace_pos = match line.find('{') {
        Some(pos) => pos,
        None => return Ok(false),
    };

    let event_type = line[..brace_pos].trim().to_string();
    if event_type.is_empty() {
        return Ok(false);
    }

    let rest = &line[brace_pos..];
    if !rest.ends_with('}') {
        return Ok(false);
    }

    let content = rest.trim_start_matches('{').trim_end_matches('}').trim();

    let fields = schema.entry(event_type).or_default();

    if content.is_empty() {
        return Ok(true);
    }

    // Split fields by comma, respecting nesting
    for field_str in split_evt_fields(content) {
        let field_str = field_str.trim();
        if field_str.is_empty() {
            continue;
        }

        if let Some(colon_pos) = field_str.find(':') {
            let field_name = field_str[..colon_pos].trim().to_string();
            let field_value = field_str[colon_pos + 1..].trim();
            let types = fields.entry(field_name).or_default();
            types.insert(infer_evt_type(field_value));
        }
    }

    Ok(true)
}

/// Split .evt fields by comma, respecting nested structures (arrays, strings).
fn split_evt_fields(content: &str) -> Vec<&str> {
    let bytes = content.as_bytes();
    let mut fields = Vec::new();
    let mut field_start = 0;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape_next = false;

    for i in 0..bytes.len() {
        if escape_next {
            escape_next = false;
            continue;
        }
        match bytes[i] {
            b'\\' => {
                escape_next = true;
            }
            b'"' | b'\'' => {
                in_string = !in_string;
            }
            b'{' | b'[' | b'(' if !in_string => {
                depth += 1;
            }
            b'}' | b']' | b')' if !in_string => {
                depth -= 1;
            }
            b',' if !in_string && depth == 0 => {
                let field = content[field_start..i].trim();
                if !field.is_empty() {
                    fields.push(field);
                }
                field_start = i + 1;
            }
            _ => {}
        }
    }

    let last = content[field_start..].trim();
    if !last.is_empty() {
        fields.push(last);
    }

    fields
}

/// Format the inferred schema as VPL event declarations
fn format_vpl_declarations(
    schema: &BTreeMap<String, BTreeMap<String, HashSet<InferredType>>>,
) -> String {
    let mut output = String::new();
    let mut first = true;

    for (event_type, fields) in schema {
        if !first {
            output.push('\n');
        }
        first = false;

        output.push_str(&format!("event {event_type}:\n"));

        for (field_name, types) in fields {
            let vpl_type = promote_types(types);
            output.push_str(&format!("    {field_name}: {vpl_type}\n"));
        }
    }

    output
}

/// Run schema inference on an event file
pub fn run_infer(input: &Path, output: Option<&Path>, sample_size: usize) -> Result<()> {
    let file =
        File::open(input).with_context(|| format!("Failed to open file: {}", input.display()))?;
    let reader = BufReader::new(file);

    // event_type -> field_name -> set of observed types
    let mut schema: BTreeMap<String, BTreeMap<String, HashSet<InferredType>>> = BTreeMap::new();
    let mut events_parsed = 0;

    for line in reader.lines() {
        if events_parsed >= sample_size {
            break;
        }

        let line = line.with_context(|| "Failed to read line")?;
        let line = line.trim().to_string();

        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
            continue;
        }

        // Skip BATCH directives
        if line.starts_with("BATCH") {
            continue;
        }

        // Strip @Ns timing prefix if present
        let line = if line.starts_with('@') {
            match line.find(char::is_whitespace) {
                Some(pos) => line[pos..].trim().to_string(),
                None => continue,
            }
        } else {
            line
        };

        // Auto-detect format: try JSONL first (starts with '{'), fall back to .evt
        let parsed = if line.starts_with('{') {
            parse_jsonl_line(&line, &mut schema)?
        } else {
            parse_evt_line(&line, &mut schema)?
        };

        if parsed {
            events_parsed += 1;
        }
    }

    let declarations = format_vpl_declarations(&schema);

    if let Some(output_path) = output {
        std::fs::write(output_path, &declarations)
            .with_context(|| format!("Failed to write to {}", output_path.display()))?;
        println!(
            "Inferred {} event type(s) from {} event(s), written to {}",
            schema.len(),
            events_parsed,
            output_path.display()
        );
    } else {
        print!("{declarations}");
        eprintln!(
            "# Inferred {} event type(s) from {} event(s)",
            schema.len(),
            events_parsed,
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_promotion_single_int() {
        let mut types = HashSet::new();
        types.insert(InferredType::Int);
        assert_eq!(promote_types(&types), "int");
    }

    #[test]
    fn test_type_promotion_single_float() {
        let mut types = HashSet::new();
        types.insert(InferredType::Float);
        assert_eq!(promote_types(&types), "float");
    }

    #[test]
    fn test_type_promotion_single_str() {
        let mut types = HashSet::new();
        types.insert(InferredType::Str);
        assert_eq!(promote_types(&types), "str");
    }

    #[test]
    fn test_type_promotion_single_bool() {
        let mut types = HashSet::new();
        types.insert(InferredType::Bool);
        assert_eq!(promote_types(&types), "bool");
    }

    #[test]
    fn test_type_promotion_int_float() {
        let mut types = HashSet::new();
        types.insert(InferredType::Int);
        types.insert(InferredType::Float);
        assert_eq!(promote_types(&types), "float");
    }

    #[test]
    fn test_type_promotion_int_str() {
        let mut types = HashSet::new();
        types.insert(InferredType::Int);
        types.insert(InferredType::Str);
        assert_eq!(promote_types(&types), "str");
    }

    #[test]
    fn test_type_promotion_float_str() {
        let mut types = HashSet::new();
        types.insert(InferredType::Float);
        types.insert(InferredType::Str);
        assert_eq!(promote_types(&types), "str");
    }

    #[test]
    fn test_type_promotion_null_int() {
        let mut types = HashSet::new();
        types.insert(InferredType::Null);
        types.insert(InferredType::Int);
        assert_eq!(promote_types(&types), "int");
    }

    #[test]
    fn test_type_promotion_null_only() {
        let mut types = HashSet::new();
        types.insert(InferredType::Null);
        assert_eq!(promote_types(&types), "str");
    }

    #[test]
    fn test_type_promotion_array() {
        let mut types = HashSet::new();
        types.insert(InferredType::Array);
        types.insert(InferredType::Int);
        assert_eq!(promote_types(&types), "list");
    }

    #[test]
    fn test_parse_jsonl_line() {
        let mut schema = BTreeMap::new();
        let line = r#"{"event_type": "SensorReading", "data": {"temperature": 23.5, "zone": "A1", "count": 10}}"#;
        let result = parse_jsonl_line(line, &mut schema).unwrap();
        assert!(result);
        assert!(schema.contains_key("SensorReading"));
        let fields = &schema["SensorReading"];
        assert!(fields["temperature"].contains(&InferredType::Float));
        assert!(fields["zone"].contains(&InferredType::Str));
        assert!(fields["count"].contains(&InferredType::Int));
    }

    #[test]
    fn test_parse_jsonl_int_and_float_promotion() {
        let mut schema = BTreeMap::new();
        let line1 = r#"{"event_type": "Metric", "data": {"value": 10}}"#;
        let line2 = r#"{"event_type": "Metric", "data": {"value": 10.5}}"#;
        parse_jsonl_line(line1, &mut schema).unwrap();
        parse_jsonl_line(line2, &mut schema).unwrap();
        let fields = &schema["Metric"];
        assert!(fields["value"].contains(&InferredType::Int));
        assert!(fields["value"].contains(&InferredType::Float));
        assert_eq!(promote_types(&fields["value"]), "float");
    }

    #[test]
    fn test_parse_evt_line() {
        let mut schema = BTreeMap::new();
        let line = r#"StockTick { symbol: "AAPL", price: 150.0, volume: 1000 }"#;
        let result = parse_evt_line(line, &mut schema).unwrap();
        assert!(result);
        assert!(schema.contains_key("StockTick"));
        let fields = &schema["StockTick"];
        assert!(fields["symbol"].contains(&InferredType::Str));
        assert!(fields["price"].contains(&InferredType::Float));
        assert!(fields["volume"].contains(&InferredType::Int));
    }

    #[test]
    fn test_parse_evt_line_with_bool_and_null() {
        let mut schema = BTreeMap::new();
        let line = r"Alert { active: true, message: null }";
        let result = parse_evt_line(line, &mut schema).unwrap();
        assert!(result);
        let fields = &schema["Alert"];
        assert!(fields["active"].contains(&InferredType::Bool));
        assert!(fields["message"].contains(&InferredType::Null));
    }

    #[test]
    fn test_parse_evt_line_with_array() {
        let mut schema = BTreeMap::new();
        let line = r#"Data { values: [1, 2, 3], name: "test" }"#;
        let result = parse_evt_line(line, &mut schema).unwrap();
        assert!(result);
        let fields = &schema["Data"];
        assert!(fields["values"].contains(&InferredType::Array));
        assert!(fields["name"].contains(&InferredType::Str));
    }

    #[test]
    fn test_format_vpl_declarations_sorted() {
        let mut schema = BTreeMap::new();

        let mut fields_b = BTreeMap::new();
        let mut types = HashSet::new();
        types.insert(InferredType::Str);
        fields_b.insert("name".to_string(), types);
        schema.insert("Bravo".to_string(), fields_b);

        let mut fields_a = BTreeMap::new();
        let mut types = HashSet::new();
        types.insert(InferredType::Int);
        fields_a.insert("count".to_string(), types);
        schema.insert("Alpha".to_string(), fields_a);

        let output = format_vpl_declarations(&schema);
        assert_eq!(
            output,
            "event Alpha:\n    count: int\n\nevent Bravo:\n    name: str\n"
        );
    }

    #[test]
    fn test_format_vpl_fields_sorted() {
        let mut schema = BTreeMap::new();
        let mut fields = BTreeMap::new();

        let mut types_z = HashSet::new();
        types_z.insert(InferredType::Str);
        fields.insert("zone".to_string(), types_z);

        let mut types_a = HashSet::new();
        types_a.insert(InferredType::Float);
        fields.insert("amount".to_string(), types_a);

        schema.insert("Event".to_string(), fields);

        let output = format_vpl_declarations(&schema);
        assert_eq!(output, "event Event:\n    amount: float\n    zone: str\n");
    }

    #[test]
    fn test_infer_evt_type() {
        assert_eq!(infer_evt_type("42"), InferredType::Int);
        assert_eq!(infer_evt_type("3.14"), InferredType::Float);
        assert_eq!(infer_evt_type("\"hello\""), InferredType::Str);
        assert_eq!(infer_evt_type("'hello'"), InferredType::Str);
        assert_eq!(infer_evt_type("true"), InferredType::Bool);
        assert_eq!(infer_evt_type("false"), InferredType::Bool);
        assert_eq!(infer_evt_type("null"), InferredType::Null);
        assert_eq!(infer_evt_type("[1, 2]"), InferredType::Array);
        assert_eq!(infer_evt_type("unquoted"), InferredType::Str);
    }

    #[test]
    fn test_run_infer_evt_file() {
        let dir = std::env::temp_dir().join("varpulis_infer_test_evt");
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("test.evt");
        std::fs::write(
            &input,
            r#"# Test events
StockTick { symbol: "AAPL", price: 150.0, volume: 1000 }
StockTick { symbol: "GOOG", price: 2800.5, volume: 500 }
Alert { severity: "high", active: true }
"#,
        )
        .unwrap();

        let output = dir.join("output.vpl");
        run_infer(&input, Some(&output), 100).unwrap();

        let content = std::fs::read_to_string(&output).unwrap();
        assert!(content.contains("event Alert:"));
        assert!(content.contains("event StockTick:"));
        assert!(content.contains("    price: float"));
        assert!(content.contains("    symbol: str"));
        assert!(content.contains("    volume: int"));
        assert!(content.contains("    severity: str"));
        assert!(content.contains("    active: bool"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_run_infer_jsonl_file() {
        let dir = std::env::temp_dir().join("varpulis_infer_test_jsonl");
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("test.jsonl");
        std::fs::write(
            &input,
            r#"{"event_type": "Metric", "data": {"value": 10, "host": "server1"}}
{"event_type": "Metric", "data": {"value": 20.5, "host": "server2"}}
{"event_type": "Alert", "data": {"message": "disk full", "critical": true}}
"#,
        )
        .unwrap();

        let output = dir.join("output.vpl");
        run_infer(&input, Some(&output), 100).unwrap();

        let content = std::fs::read_to_string(&output).unwrap();
        assert!(content.contains("event Alert:"));
        assert!(content.contains("event Metric:"));
        // value saw Int(10) and Float(20.5), should promote to float
        assert!(content.contains("    value: float"));
        assert!(content.contains("    host: str"));
        assert!(content.contains("    critical: bool"));
        assert!(content.contains("    message: str"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_sample_size_limit() {
        let dir = std::env::temp_dir().join("varpulis_infer_test_sample");
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("test.evt");
        // Write 10 events but sample only 2
        let mut content = String::new();
        for i in 0..10 {
            content.push_str(&format!("Tick {{ id: {i} }}\n"));
        }
        std::fs::write(&input, &content).unwrap();

        let mut schema: BTreeMap<String, BTreeMap<String, HashSet<InferredType>>> = BTreeMap::new();

        let file = File::open(&input).unwrap();
        let reader = BufReader::new(file);
        let mut count = 0;
        for line in reader.lines() {
            if count >= 2 {
                break;
            }
            let line = line.unwrap();
            let line = line.trim().to_string();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if parse_evt_line(&line, &mut schema).unwrap() {
                count += 1;
            }
        }

        assert_eq!(count, 2);
        assert!(schema.contains_key("Tick"));

        std::fs::remove_dir_all(&dir).ok();
    }
}
