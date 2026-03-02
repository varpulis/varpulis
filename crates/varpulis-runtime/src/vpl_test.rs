//! VPL Test DSL — declarative test format for Varpulis programs.
//!
//! `.vpl.test` files are self-contained test fixtures that specify a VPL program,
//! input events, and expected outputs in a single file. The test runner discovers
//! these files automatically and executes them as integration tests.
//!
//! # File Format
//!
//! ```text
//! # Test: descriptive name
//! # Comments start with # or //
//!
//! === VPL ===
//! stream HighTemp = SensorReading
//!     .where(temperature > 100)
//!     .emit(sensor: sensor_id, temp: temperature)
//!
//! === INPUT ===
//! SensorReading { sensor_id: "S1", temperature: 105 }
//! SensorReading { sensor_id: "S2", temperature: 50 }
//! SensorReading { sensor_id: "S3", temperature: 200 }
//!
//! === EXPECT ===
//! HighTemp { sensor: "S1", temp: 105 }
//! HighTemp { sensor: "S3", temp: 200 }
//! ```
//!
//! # Sections
//!
//! - `=== VPL ===` — The VPL program source (required)
//! - `=== INPUT ===` — Input events in `.evt` format (required)
//! - `=== EXPECT ===` — Expected output events (optional, order matters)
//! - `=== EXPECT_COUNT ===` — Expected number of output events (optional)
//! - `=== EXPECT_NONE ===` — Asserts zero output events (optional)
//! - `=== EXPECT_ERROR ===` — Expected parse or compilation error substring (optional)
//! - `=== EXPECT_FIELDS ===` — Assert specific fields on output events (optional)

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use varpulis_core::Value;
use varpulis_parser::parse;

use crate::event_file::EventFileParser;
use crate::Engine;

/// A parsed `.vpl.test` fixture.
#[derive(Debug)]
pub struct VplTestFixture {
    /// File path for error reporting.
    pub path: PathBuf,
    /// Name extracted from first comment line, or filename.
    pub name: String,
    /// VPL program source.
    pub vpl: String,
    /// Input events source (`.evt` format).
    pub input: String,
    /// Expected output events (parsed from `.evt`-like format).
    pub expect: Option<String>,
    /// Expected output count.
    pub expect_count: Option<usize>,
    /// Expect no output events.
    pub expect_none: bool,
    /// Expected error substring (parse or compile error).
    pub expect_error: Option<String>,
    /// Expected fields on output events.
    pub expect_fields: Option<String>,
}

/// Result of running a single test fixture.
#[derive(Debug)]
pub struct VplTestResult {
    /// Fixture name.
    pub name: String,
    /// Whether the test passed.
    pub passed: bool,
    /// Failure message if any.
    pub failure: Option<String>,
    /// Actual output events (for diagnostics).
    pub output_events: Vec<OutputEvent>,
}

/// Simplified output event for comparison.
#[derive(Debug, Clone, PartialEq)]
pub struct OutputEvent {
    /// Event type name.
    pub event_type: String,
    /// Fields as sorted key-value pairs.
    pub fields: BTreeMap<String, TestValue>,
}

/// A value in test comparison (simplified from `Value`).
#[derive(Debug, Clone)]
pub enum TestValue {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
}

impl PartialEq for TestValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => (a - b).abs() < 1e-6,
            (Self::Str(a), Self::Str(b)) => a == b,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Null, Self::Null) => true,
            // Allow int/float cross-comparison
            (Self::Int(a), Self::Float(b)) | (Self::Float(b), Self::Int(a)) => {
                (*a as f64 - b).abs() < 1e-6
            }
            _ => false,
        }
    }
}

impl std::fmt::Display for TestValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Str(v) => write!(f, "\"{v}\""),
            Self::Bool(v) => write!(f, "{v}"),
            Self::Null => write!(f, "null"),
        }
    }
}

impl From<&Value> for TestValue {
    fn from(v: &Value) -> Self {
        match v {
            Value::Int(i) => Self::Int(*i),
            Value::Float(f) => Self::Float(*f),
            Value::Str(s) => Self::Str(s.to_string()),
            Value::Bool(b) => Self::Bool(*b),
            Value::Null => Self::Null,
            other => Self::Str(format!("{other}")),
        }
    }
}

impl std::fmt::Display for OutputEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{ ", self.event_type)?;
        let fields: Vec<_> = self
            .fields
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect();
        write!(f, "{}", fields.join(", "))?;
        write!(f, " }}")
    }
}

/// Parse a `.vpl.test` file into a fixture.
pub fn parse_fixture(path: &Path) -> Result<VplTestFixture, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read {}: {e}", path.display()))?;
    parse_fixture_str(&content, path)
}

/// Parse a `.vpl.test` fixture from a string.
pub fn parse_fixture_str(content: &str, path: &Path) -> Result<VplTestFixture, String> {
    let mut name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .trim_end_matches(".vpl")
        .to_string();

    // Extract name from first comment line
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(comment) = trimmed.strip_prefix('#') {
            let comment = comment.trim();
            if let Some(test_name) = comment.strip_prefix("Test:") {
                name = test_name.trim().to_string();
            }
            break;
        }
        break;
    }

    // Split into sections
    let sections = parse_sections(content)?;

    let vpl = sections
        .get("VPL")
        .ok_or_else(|| format!("{}: missing === VPL === section", path.display()))?
        .clone();

    let input = sections.get("INPUT").cloned().unwrap_or_default();

    let expect = sections.get("EXPECT").cloned();
    let expect_none = sections.contains_key("EXPECT_NONE");
    let expect_error = sections.get("EXPECT_ERROR").map(|s| s.trim().to_string());
    let expect_fields = sections.get("EXPECT_FIELDS").cloned();

    let expect_count = sections.get("EXPECT_COUNT").map(|s| {
        s.trim()
            .parse::<usize>()
            .map_err(|_| format!("{}: invalid EXPECT_COUNT value: {s}", path.display()))
    });
    let expect_count = match expect_count {
        Some(Ok(n)) => Some(n),
        Some(Err(e)) => return Err(e),
        None => None,
    };

    Ok(VplTestFixture {
        path: path.to_path_buf(),
        name,
        vpl,
        input,
        expect,
        expect_count,
        expect_none,
        expect_error,
        expect_fields,
    })
}

/// Parse section headers `=== NAME ===` and return content by section name.
fn parse_sections(content: &str) -> Result<BTreeMap<String, String>, String> {
    let mut sections = BTreeMap::new();
    let mut current_section: Option<String> = None;
    let mut current_content = String::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(section_name) = parse_section_header(trimmed) {
            // Save previous section
            if let Some(name) = current_section.take() {
                sections.insert(name, current_content.trim().to_string());
            }
            current_section = Some(section_name);
            current_content = String::new();
        } else if current_section.is_some() {
            current_content.push_str(line);
            current_content.push('\n');
        }
        // Lines before first section are ignored (comments/metadata)
    }

    // Save last section
    if let Some(name) = current_section {
        sections.insert(name, current_content.trim().to_string());
    }

    Ok(sections)
}

/// Parse `=== SECTION_NAME ===` and return the name.
fn parse_section_header(line: &str) -> Option<String> {
    let line = line.trim();
    if line.starts_with("===") && line.ends_with("===") {
        let inner = line.trim_start_matches('=').trim_end_matches('=').trim();
        if !inner.is_empty() {
            return Some(inner.to_string());
        }
    }
    None
}

/// Parse expected output events from the EXPECT section.
fn parse_expected_events(expect_source: &str) -> Result<Vec<OutputEvent>, String> {
    let mut events = Vec::new();

    for (line_num, line) in expect_source.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("//") {
            continue;
        }

        // Parse: EventType { field1: value1, field2: value2 }
        let event = parse_expected_event_line(trimmed)
            .map_err(|e| format!("EXPECT line {}: {e}", line_num + 1))?;
        events.push(event);
    }

    Ok(events)
}

/// Parse a single expected event line: `EventType { key: value, ... }`
fn parse_expected_event_line(line: &str) -> Result<OutputEvent, String> {
    let brace_pos = line
        .find('{')
        .ok_or_else(|| format!("expected '{{' in event line: {line}"))?;

    let event_type = line[..brace_pos].trim().to_string();
    if event_type.is_empty() {
        return Err(format!("empty event type in: {line}"));
    }

    let fields_str = line[brace_pos + 1..].trim().trim_end_matches('}').trim();

    let mut fields = BTreeMap::new();
    if !fields_str.is_empty() {
        // Split on commas, but respect quoted strings
        for field in split_fields(fields_str) {
            let field = field.trim();
            if field.is_empty() {
                continue;
            }
            let colon_pos = field
                .find(':')
                .ok_or_else(|| format!("expected ':' in field: {field}"))?;

            let key = field[..colon_pos].trim().to_string();
            let value_str = field[colon_pos + 1..].trim();

            let value = parse_test_value(value_str)?;
            fields.insert(key, value);
        }
    }

    Ok(OutputEvent { event_type, fields })
}

/// Split fields on commas, respecting quoted strings.
fn split_fields(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;

    for (i, c) in s.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        result.push(&s[start..]);
    }
    result
}

/// Parse a test value from a string representation.
fn parse_test_value(s: &str) -> Result<TestValue, String> {
    let s = s.trim();
    if s == "null" {
        return Ok(TestValue::Null);
    }
    if s == "true" {
        return Ok(TestValue::Bool(true));
    }
    if s == "false" {
        return Ok(TestValue::Bool(false));
    }
    // Quoted string
    if s.starts_with('"') && s.ends_with('"') {
        return Ok(TestValue::Str(s[1..s.len() - 1].to_string()));
    }
    // Try integer
    if let Ok(i) = s.parse::<i64>() {
        return Ok(TestValue::Int(i));
    }
    // Try float
    if let Ok(f) = s.parse::<f64>() {
        return Ok(TestValue::Float(f));
    }
    // Unquoted string (e.g., field references)
    Ok(TestValue::Str(s.to_string()))
}

/// Convert an engine output event to a test output event.
fn event_to_output(event: &crate::Event) -> OutputEvent {
    let mut fields = BTreeMap::new();
    for (key, value) in &event.data {
        fields.insert(key.to_string(), TestValue::from(value));
    }
    OutputEvent {
        event_type: event.event_type.to_string(),
        fields,
    }
}

/// Run a single test fixture and return the result.
pub fn run_fixture(fixture: &VplTestFixture) -> VplTestResult {
    let name = fixture.name.clone();

    // Handle EXPECT_ERROR — expect a parse or compile failure
    if let Some(ref expected_error) = fixture.expect_error {
        return run_error_fixture(fixture, expected_error);
    }

    // Parse program
    let program = match parse(&fixture.vpl) {
        Ok(p) => p,
        Err(e) => {
            return VplTestResult {
                name,
                passed: false,
                failure: Some(format!("VPL parse error: {e}")),
                output_events: Vec::new(),
            };
        }
    };

    // Create engine
    let (tx, mut rx) = tokio::sync::mpsc::channel(10_000);
    let mut engine = Engine::new(tx);
    if let Err(e) = engine.load(&program) {
        return VplTestResult {
            name,
            passed: false,
            failure: Some(format!("Engine load error: {e}")),
            output_events: Vec::new(),
        };
    }

    // Parse and process input events
    let timed_events = match EventFileParser::parse(&fixture.input) {
        Ok(events) => events,
        Err(e) => {
            return VplTestResult {
                name,
                passed: false,
                failure: Some(format!("Input event parse error: {e}")),
                output_events: Vec::new(),
            };
        }
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let process_result = rt.block_on(async {
        for timed in &timed_events {
            engine.process(timed.event.clone()).await?;
        }
        Ok::<(), crate::engine::error::EngineError>(())
    });

    if let Err(e) = process_result {
        return VplTestResult {
            name,
            passed: false,
            failure: Some(format!("Event processing error: {e}")),
            output_events: Vec::new(),
        };
    }

    // Collect output events
    let mut output_events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        output_events.push(event_to_output(&event));
    }

    // Run assertions
    let mut failures = Vec::new();

    // Check EXPECT_NONE
    if fixture.expect_none && !output_events.is_empty() {
        failures.push(format!(
            "expected no output events, got {}:\n{}",
            output_events.len(),
            format_events(&output_events)
        ));
    }

    // Check EXPECT_COUNT
    if let Some(expected_count) = fixture.expect_count {
        if output_events.len() != expected_count {
            failures.push(format!(
                "expected {expected_count} output events, got {}",
                output_events.len()
            ));
        }
    }

    // Check EXPECT (exact event matching)
    if let Some(ref expect_source) = fixture.expect {
        match parse_expected_events(expect_source) {
            Ok(expected) => {
                if output_events.len() != expected.len() {
                    failures.push(format!(
                        "expected {} output events, got {}:\n  expected:\n{}\n  actual:\n{}",
                        expected.len(),
                        output_events.len(),
                        format_events_indented(&expected),
                        format_events_indented(&output_events)
                    ));
                } else {
                    for (i, (actual, expected)) in
                        output_events.iter().zip(expected.iter()).enumerate()
                    {
                        if let Some(mismatch) = compare_events(actual, expected) {
                            failures.push(format!("event[{i}]: {mismatch}"));
                        }
                    }
                }
            }
            Err(e) => {
                failures.push(format!("EXPECT parse error: {e}"));
            }
        }
    }

    // Check EXPECT_FIELDS (partial field matching)
    if let Some(ref expect_fields_source) = fixture.expect_fields {
        match parse_field_assertions(expect_fields_source) {
            Ok(assertions) => {
                for assertion in &assertions {
                    if let Some(failure) = check_field_assertion(assertion, &output_events) {
                        failures.push(failure);
                    }
                }
            }
            Err(e) => {
                failures.push(format!("EXPECT_FIELDS parse error: {e}"));
            }
        }
    }

    let passed = failures.is_empty();
    let failure = if failures.is_empty() {
        None
    } else {
        Some(failures.join("\n"))
    };

    VplTestResult {
        name,
        passed,
        failure,
        output_events,
    }
}

/// Run a fixture that expects a parse/compile error.
fn run_error_fixture(fixture: &VplTestFixture, expected_error: &str) -> VplTestResult {
    let name = fixture.name.clone();

    let expected_lower = expected_error.to_lowercase();

    // Try to parse
    let program = match parse(&fixture.vpl) {
        Ok(p) => p,
        Err(e) => {
            let error_msg = format!("{e}");
            if error_msg.to_lowercase().contains(&expected_lower) {
                return VplTestResult {
                    name,
                    passed: true,
                    failure: None,
                    output_events: Vec::new(),
                };
            }
            return VplTestResult {
                name,
                passed: false,
                failure: Some(format!(
                    "expected error containing \"{expected_error}\", got: {error_msg}"
                )),
                output_events: Vec::new(),
            };
        }
    };

    // Try to load (compile)
    let (tx, _rx) = tokio::sync::mpsc::channel(10_000);
    let mut engine = Engine::new(tx);
    match engine.load(&program) {
        Ok(()) => VplTestResult {
            name,
            passed: false,
            failure: Some(format!(
                "expected error containing \"{expected_error}\", but program compiled successfully"
            )),
            output_events: Vec::new(),
        },
        Err(e) => {
            let error_msg = format!("{e}");
            if error_msg.to_lowercase().contains(&expected_lower) {
                VplTestResult {
                    name,
                    passed: true,
                    failure: None,
                    output_events: Vec::new(),
                }
            } else {
                VplTestResult {
                    name,
                    passed: false,
                    failure: Some(format!(
                        "expected error containing \"{expected_error}\", got: {error_msg}"
                    )),
                    output_events: Vec::new(),
                }
            }
        }
    }
}

/// Compare two events, returning a mismatch description if they differ.
fn compare_events(actual: &OutputEvent, expected: &OutputEvent) -> Option<String> {
    if actual.event_type != expected.event_type {
        return Some(format!(
            "event type mismatch: expected \"{}\", got \"{}\"",
            expected.event_type, actual.event_type
        ));
    }

    // Check all expected fields are present and match
    for (key, expected_value) in &expected.fields {
        match actual.fields.get(key) {
            Some(actual_value) => {
                if actual_value != expected_value {
                    return Some(format!(
                        "field \"{key}\": expected {expected_value}, got {actual_value}"
                    ));
                }
            }
            None => {
                return Some(format!(
                    "missing expected field \"{key}\" (expected {expected_value})"
                ));
            }
        }
    }

    None
}

/// A field assertion for EXPECT_FIELDS.
#[derive(Debug)]
struct FieldAssertion {
    /// Which event index (0-based), or `all` for all events.
    event_index: EventSelector,
    /// Field name.
    field: String,
    /// Expected value.
    value: TestValue,
}

#[derive(Debug)]
enum EventSelector {
    /// A specific event index.
    Index(usize),
    /// All events must match.
    All,
    /// Any event must match.
    Any,
}

/// Parse EXPECT_FIELDS section.
///
/// Format:
/// ```text
/// [0].field_name = value
/// [all].field_name = value
/// [any].field_name = value
/// ```
fn parse_field_assertions(source: &str) -> Result<Vec<FieldAssertion>, String> {
    let mut assertions = Vec::new();

    for (line_num, line) in source.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("//") {
            continue;
        }

        // Parse [index].field = value
        if !trimmed.starts_with('[') {
            return Err(format!(
                "line {}: expected '[index].field = value', got: {trimmed}",
                line_num + 1
            ));
        }

        let bracket_end = trimmed
            .find(']')
            .ok_or_else(|| format!("line {}: missing ']'", line_num + 1))?;

        let selector_str = &trimmed[1..bracket_end];
        let event_index = match selector_str {
            "all" => EventSelector::All,
            "any" => EventSelector::Any,
            s => EventSelector::Index(
                s.parse::<usize>()
                    .map_err(|_| format!("line {}: invalid index: {s}", line_num + 1))?,
            ),
        };

        let rest = trimmed[bracket_end + 1..].trim();
        let rest = rest
            .strip_prefix('.')
            .ok_or_else(|| format!("line {}: expected '.' after ']'", line_num + 1))?;

        let eq_pos = rest
            .find('=')
            .ok_or_else(|| format!("line {}: expected '=' in assertion", line_num + 1))?;

        let field = rest[..eq_pos].trim().to_string();
        let value_str = rest[eq_pos + 1..].trim();
        let value = parse_test_value(value_str)?;

        assertions.push(FieldAssertion {
            event_index,
            field,
            value,
        });
    }

    Ok(assertions)
}

/// Check a single field assertion against output events.
fn check_field_assertion(assertion: &FieldAssertion, events: &[OutputEvent]) -> Option<String> {
    match &assertion.event_index {
        EventSelector::Index(i) => {
            let event = events.get(*i)?;
            match event.fields.get(&assertion.field) {
                Some(actual) if actual == &assertion.value => None,
                Some(actual) => Some(format!(
                    "[{i}].{}: expected {}, got {actual}",
                    assertion.field, assertion.value
                )),
                None => Some(format!("[{i}].{}: field not found", assertion.field)),
            }
        }
        EventSelector::All => {
            for (i, event) in events.iter().enumerate() {
                match event.fields.get(&assertion.field) {
                    Some(actual) if actual == &assertion.value => {}
                    Some(actual) => {
                        return Some(format!(
                            "[all].{}: event[{i}] expected {}, got {actual}",
                            assertion.field, assertion.value
                        ));
                    }
                    None => {
                        return Some(format!(
                            "[all].{}: event[{i}] field not found",
                            assertion.field
                        ));
                    }
                }
            }
            None
        }
        EventSelector::Any => {
            for event in events {
                if let Some(actual) = event.fields.get(&assertion.field) {
                    if actual == &assertion.value {
                        return None;
                    }
                }
            }
            Some(format!(
                "[any].{}: no event has value {}",
                assertion.field, assertion.value
            ))
        }
    }
}

/// Discover all `.vpl.test` files under a directory.
pub fn discover_fixtures(dir: &Path) -> Vec<PathBuf> {
    let mut fixtures = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                fixtures.extend(discover_fixtures(&path));
            } else if path
                .file_name()
                .is_some_and(|n| n.to_string_lossy().ends_with(".vpl.test"))
            {
                fixtures.push(path);
            }
        }
    }
    fixtures.sort();
    fixtures
}

/// Run all `.vpl.test` files under a directory and return results.
pub fn run_all(dir: &Path) -> Vec<VplTestResult> {
    let fixtures = discover_fixtures(dir);
    let mut results = Vec::new();

    for path in fixtures {
        match parse_fixture(&path) {
            Ok(fixture) => results.push(run_fixture(&fixture)),
            Err(e) => results.push(VplTestResult {
                name: path.display().to_string(),
                passed: false,
                failure: Some(e),
                output_events: Vec::new(),
            }),
        }
    }

    results
}

fn format_events(events: &[OutputEvent]) -> String {
    events
        .iter()
        .map(|e| format!("  {e}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_events_indented(events: &[OutputEvent]) -> String {
    events
        .iter()
        .map(|e| format!("    {e}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_parse_section_header() {
        assert_eq!(parse_section_header("=== VPL ==="), Some("VPL".to_string()));
        assert_eq!(
            parse_section_header("=== INPUT ==="),
            Some("INPUT".to_string())
        );
        assert_eq!(
            parse_section_header("=== EXPECT ==="),
            Some("EXPECT".to_string())
        );
        assert_eq!(
            parse_section_header("=== EXPECT_COUNT ==="),
            Some("EXPECT_COUNT".to_string())
        );
        assert_eq!(parse_section_header("not a section"), None);
        assert_eq!(parse_section_header("=== ==="), None);
    }

    #[test]
    fn test_parse_sections() {
        let content = r"
# A test comment
=== VPL ===
stream X = Y

=== INPUT ===
Y { a: 1 }

=== EXPECT ===
X { a: 1 }
";
        let sections = parse_sections(content).unwrap();
        assert_eq!(sections.len(), 3);
        assert!(sections.get("VPL").unwrap().contains("stream X = Y"));
        assert!(sections.get("INPUT").unwrap().contains("Y { a: 1 }"));
        assert!(sections.get("EXPECT").unwrap().contains("X { a: 1 }"));
    }

    #[test]
    fn test_parse_expected_event_line() {
        let event = parse_expected_event_line(r#"HighTemp { sensor: "S1", temp: 105 }"#).unwrap();
        assert_eq!(event.event_type, "HighTemp");
        assert_eq!(
            event.fields.get("sensor"),
            Some(&TestValue::Str("S1".to_string()))
        );
        assert_eq!(event.fields.get("temp"), Some(&TestValue::Int(105)));
    }

    #[test]
    fn test_parse_expected_event_no_fields() {
        let event = parse_expected_event_line("Alert {}").unwrap();
        assert_eq!(event.event_type, "Alert");
        assert!(event.fields.is_empty());
    }

    #[test]
    fn test_parse_test_value() {
        assert_eq!(parse_test_value("42").unwrap(), TestValue::Int(42));
        assert_eq!(parse_test_value("1.23").unwrap(), TestValue::Float(1.23));
        assert_eq!(
            parse_test_value("\"hello\"").unwrap(),
            TestValue::Str("hello".to_string())
        );
        assert_eq!(parse_test_value("true").unwrap(), TestValue::Bool(true));
        assert_eq!(parse_test_value("null").unwrap(), TestValue::Null);
    }

    #[test]
    fn test_parse_fixture_str() {
        let content = r#"# Test: high temperature alert
=== VPL ===
stream HighTemp = SensorReading
    .where(temperature > 100)
    .emit(sensor: sensor_id, temp: temperature)

=== INPUT ===
SensorReading { sensor_id: "S1", temperature: 105 }
SensorReading { sensor_id: "S2", temperature: 50 }

=== EXPECT ===
HighTemp { sensor: "S1", temp: 105 }
"#;

        let fixture = parse_fixture_str(content, &PathBuf::from("test.vpl.test")).unwrap();
        assert_eq!(fixture.name, "high temperature alert");
        assert!(fixture.vpl.contains("stream HighTemp"));
        assert!(fixture.input.contains("SensorReading"));
        assert!(fixture.expect.is_some());
    }

    #[test]
    fn test_run_fixture_high_temp() {
        let content = r#"# Test: high temperature filter
=== VPL ===
stream HighTemp = SensorReading
    .where(temperature > 100)
    .emit(sensor: sensor_id, temp: temperature)

=== INPUT ===
SensorReading { sensor_id: "S1", temperature: 105 }
SensorReading { sensor_id: "S2", temperature: 50 }
SensorReading { sensor_id: "S3", temperature: 200 }

=== EXPECT ===
HighTemp { sensor: "S1", temp: 105 }
HighTemp { sensor: "S3", temp: 200 }
"#;

        let fixture = parse_fixture_str(content, &PathBuf::from("test.vpl.test")).unwrap();
        let result = run_fixture(&fixture);
        assert!(result.passed, "Test failed: {:?}", result.failure);
        assert_eq!(result.output_events.len(), 2);
    }

    #[test]
    fn test_run_fixture_expect_count() {
        let content = r"
=== VPL ===
stream PassThrough = Event
    .where(value > 0)
    .emit(v: value)

=== INPUT ===
Event { value: 1 }
Event { value: -1 }
Event { value: 2 }

=== EXPECT_COUNT ===
2
";

        let fixture = parse_fixture_str(content, &PathBuf::from("test.vpl.test")).unwrap();
        let result = run_fixture(&fixture);
        assert!(result.passed, "Test failed: {:?}", result.failure);
    }

    #[test]
    fn test_run_fixture_expect_none() {
        let content = r"
=== VPL ===
stream HighTemp = SensorReading
    .where(temperature > 1000)
    .emit(temp: temperature)

=== INPUT ===
SensorReading { temperature: 50 }
SensorReading { temperature: 100 }

=== EXPECT_NONE ===
";

        let fixture = parse_fixture_str(content, &PathBuf::from("test.vpl.test")).unwrap();
        let result = run_fixture(&fixture);
        assert!(result.passed, "Test failed: {:?}", result.failure);
    }

    #[test]
    fn test_run_fixture_expect_error() {
        let content = r"
=== VPL ===
stream Bad = Event
    .where(>>>{{{invalid syntax

=== EXPECT_ERROR ===
expected
";

        let fixture = parse_fixture_str(content, &PathBuf::from("test.vpl.test")).unwrap();
        let result = run_fixture(&fixture);
        assert!(result.passed, "Test failed: {:?}", result.failure);
    }

    #[test]
    fn test_compare_events_match() {
        let actual = OutputEvent {
            event_type: "Alert".to_string(),
            fields: BTreeMap::from([
                ("temp".to_string(), TestValue::Int(105)),
                ("sensor".to_string(), TestValue::Str("S1".to_string())),
            ]),
        };
        let expected = OutputEvent {
            event_type: "Alert".to_string(),
            fields: BTreeMap::from([
                ("temp".to_string(), TestValue::Int(105)),
                ("sensor".to_string(), TestValue::Str("S1".to_string())),
            ]),
        };
        assert!(compare_events(&actual, &expected).is_none());
    }

    #[test]
    fn test_compare_events_partial_match() {
        // Expected only checks specified fields (partial match)
        let actual = OutputEvent {
            event_type: "Alert".to_string(),
            fields: BTreeMap::from([
                ("temp".to_string(), TestValue::Int(105)),
                ("sensor".to_string(), TestValue::Str("S1".to_string())),
                ("extra".to_string(), TestValue::Bool(true)),
            ]),
        };
        let expected = OutputEvent {
            event_type: "Alert".to_string(),
            fields: BTreeMap::from([("temp".to_string(), TestValue::Int(105))]),
        };
        // Partial match — expected only asserts on fields it specifies
        assert!(compare_events(&actual, &expected).is_none());
    }

    #[test]
    fn test_split_fields() {
        let fields = split_fields(r#"name: "hello, world", value: 42"#);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].trim(), r#"name: "hello, world""#);
        assert_eq!(fields[1].trim(), "value: 42");
    }

    #[test]
    fn test_parse_field_assertions() {
        let source = r#"
[0].temp = 105
[all].event_type = "Alert"
[any].sensor = "S1"
"#;
        let assertions = parse_field_assertions(source).unwrap();
        assert_eq!(assertions.len(), 3);
    }
}
