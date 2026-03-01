//! CLI command handler tests.
//!
//! Tests cover: VPL validation edge cases, config file loading,
//! import resolution, simulation pipeline, and project config discovery.
//! All tests use tempfile for file I/O — no live services needed.

use std::io::Write;
use tempfile::NamedTempFile;
use varpulis_cli::config::{Config, ConfigError, ProjectConfig};

// =============================================================================
// VPL validation edge cases
// =============================================================================

#[test]
fn test_check_syntax_empty_source() {
    let result = varpulis_cli::check_syntax("");
    assert!(result.is_ok());
}

#[test]
fn test_check_syntax_comments_only() {
    let source = "# This is a comment\n# Another comment\n";
    let result = varpulis_cli::check_syntax(source);
    assert!(result.is_ok());
}

#[test]
fn test_check_syntax_connector_declaration() {
    let source = r#"
        connector mqtt_in = mqtt(broker: "localhost", topic: "sensors/#")
    "#;
    let result = varpulis_cli::check_syntax(source);
    assert!(result.is_ok());
}

#[test]
fn test_validate_simple_filter() {
    let source = r#"
        stream HighValue = TestEvent
            .where(value > 50)
            .emit(alert: "high")
    "#;
    let result = varpulis_cli::validate_program(source);
    assert!(result.is_ok());
}

#[test]
fn test_validate_window_and_emit() {
    let source = r"
        stream AvgTemp = TempReading
            .window(1m)
            .emit(total: sum(temperature), n: count())
    ";
    let result = varpulis_cli::validate_program(source);
    assert!(result.is_ok());
}

#[test]
fn test_check_syntax_invalid_field_ref() {
    // Truly invalid syntax — unclosed paren
    let source = "stream Bad = Events .where(";
    let result = varpulis_cli::check_syntax(source);
    assert!(result.is_err());
}

// =============================================================================
// Config loading from tempfiles
// =============================================================================

#[test]
fn test_config_load_yaml() {
    let mut file = NamedTempFile::with_suffix(".yaml").unwrap();
    writeln!(
        file,
        r#"
server:
  port: 9042
  bind: "127.0.0.1"
"#
    )
    .unwrap();

    let config = Config::load(file.path()).unwrap();
    assert_eq!(config.server.port, 9042);
}

#[test]
fn test_config_load_toml() {
    let mut file = NamedTempFile::with_suffix(".toml").unwrap();
    writeln!(
        file,
        r#"
[server]
port = 9043
bind = "0.0.0.0"
"#
    )
    .unwrap();

    let config = Config::load(file.path()).unwrap();
    assert_eq!(config.server.port, 9043);
}

#[test]
fn test_config_load_nonexistent_file() {
    let result = Config::load("/tmp/nonexistent_varpulis_config_12345.yaml");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ConfigError::IoError(_, _)));
}

#[test]
fn test_config_load_invalid_yaml() {
    let mut file = NamedTempFile::with_suffix(".yaml").unwrap();
    // Write invalid YAML: a sequence where a mapping is expected
    write!(file, "- this\n- is: [not: valid").unwrap();

    let result = Config::load(file.path());
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ConfigError::ParseError(_)));
}

// =============================================================================
// Import resolution
// =============================================================================

#[test]
fn test_resolve_imports_simple() {
    let dir = tempfile::tempdir().unwrap();

    // Create the imported file
    let helper_path = dir.path().join("helpers.vpl");
    std::fs::write(&helper_path, "fn double(x: float) -> float:\n    x * 2.0\n").unwrap();

    // Create main source with import (using absolute path)
    let main_source = format!(
        "import \"{}\"\n\nstream Test = Events\n    .where(value > 10)\n    .emit(result: value)\n",
        helper_path.display()
    );

    let mut program = varpulis_parser::parse(&main_source).expect("Main program should parse");
    let base_path = dir.path().to_path_buf();
    let result = varpulis_cli::resolve_imports(&mut program, Some(&base_path));
    assert!(result.is_ok(), "Import resolution failed: {result:?}");
    // Should have both the imported function and the main stream
    assert!(
        program.statements.len() >= 2,
        "Expected at least 2 statements after import, got {}",
        program.statements.len()
    );
}

#[test]
fn test_resolve_imports_circular_detected() {
    let dir = tempfile::tempdir().unwrap();

    let a_path = dir.path().join("a.vpl");
    let b_path = dir.path().join("b.vpl");

    std::fs::write(
        &a_path,
        format!(
            "import \"{}\"\nstream A = Events .where(x > 1)\n",
            b_path.display()
        ),
    )
    .unwrap();

    std::fs::write(
        &b_path,
        format!(
            "import \"{}\"\nstream B = Events .where(x > 2)\n",
            a_path.display()
        ),
    )
    .unwrap();

    let source = std::fs::read_to_string(&a_path).unwrap();
    let mut program = varpulis_parser::parse(&source).expect("Should parse");
    let base_path = dir.path().to_path_buf();
    // Circular imports should be handled gracefully (skipped, not infinite loop)
    let result = varpulis_cli::resolve_imports(&mut program, Some(&base_path));
    assert!(
        result.is_ok(),
        "Circular imports should be skipped gracefully"
    );
}

#[test]
fn test_resolve_imports_depth_limit() {
    let dir = tempfile::tempdir().unwrap();

    // Create a chain of imports deeper than MAX_IMPORT_DEPTH (10)
    let mut paths = Vec::new();
    for i in 0..13 {
        paths.push(dir.path().join(format!("level_{i}.vpl")));
    }

    // Each file imports the next
    for i in 0..12 {
        let content = format!(
            "import \"{}\"\nstream L{} = Events .where(x > {})\n",
            paths[i + 1].display(),
            i,
            i
        );
        std::fs::write(&paths[i], &content).unwrap();
    }
    // Last file has no imports
    std::fs::write(&paths[12], "stream Last = Events .where(x > 99)\n").unwrap();

    let source = std::fs::read_to_string(&paths[0]).unwrap();
    let mut program = varpulis_parser::parse(&source).expect("Should parse");
    let base_path = dir.path().to_path_buf();
    let result = varpulis_cli::resolve_imports(&mut program, Some(&base_path));
    assert!(result.is_err(), "Should fail with depth limit exceeded");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("depth limit"),
        "Expected depth limit error, got: {err_msg}"
    );
}

#[test]
fn test_resolve_imports_missing_file() {
    let dir = tempfile::tempdir().unwrap();

    let source = "import \"nonexistent_file.vpl\"\nstream Test = Events .where(x > 0)\n";

    let mut program = varpulis_parser::parse(source).expect("Should parse");
    let base_path = dir.path().to_path_buf();
    let result = varpulis_cli::resolve_imports(&mut program, Some(&base_path));
    assert!(result.is_err(), "Should fail for missing import file");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("resolve import") || err_msg.contains("read import"),
        "Expected import error, got: {err_msg}"
    );
}

// =============================================================================
// Simulation pipeline
// =============================================================================

#[tokio::test]
async fn test_simulate_valid_events() {
    let vpl = r#"
        stream HighValue = TestEvent
            .where(value > 50)
            .emit(alert: "high", original_value: value)
    "#;

    let events = vec![
        varpulis_runtime::event::Event::new("TestEvent").with_field("value", 100i64),
        varpulis_runtime::event::Event::new("TestEvent").with_field("value", 10i64),
        varpulis_runtime::event::Event::new("TestEvent").with_field("value", 75i64),
    ];

    let results = varpulis_cli::simulate_from_source(vpl, events).await;
    assert!(results.is_ok(), "Simulation failed: {results:?}");
    let output = results.unwrap();
    // Should emit 2 events (value=100 and value=75 pass the filter)
    assert_eq!(
        output.len(),
        2,
        "Expected 2 output events, got {}",
        output.len()
    );
}

#[tokio::test]
async fn test_simulate_parse_error() {
    let vpl = "stream Invalid = {broken syntax";
    let events = vec![];
    let result = varpulis_cli::simulate_from_source(vpl, events).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Parse error"));
}

#[tokio::test]
async fn test_simulate_empty_events() {
    let vpl = r#"
        stream Test = Events
            .where(value > 0)
            .emit(alert: "triggered")
    "#;

    let results = varpulis_cli::simulate_from_source(vpl, vec![]).await;
    assert!(results.is_ok());
    assert_eq!(results.unwrap().len(), 0);
}

#[tokio::test]
async fn test_simulate_no_matching_events() {
    let vpl = r#"
        stream HighOnly = TestEvent
            .where(value > 1000)
            .emit(alert: "very_high")
    "#;

    let events = vec![
        varpulis_runtime::event::Event::new("TestEvent").with_field("value", 10i64),
        varpulis_runtime::event::Event::new("TestEvent").with_field("value", 20i64),
    ];

    let results = varpulis_cli::simulate_from_source(vpl, events).await;
    assert!(results.is_ok());
    assert_eq!(results.unwrap().len(), 0);
}

// =============================================================================
// Project config (.varpulis.toml)
// =============================================================================

#[test]
fn test_project_config_with_toml() {
    let dir = tempfile::tempdir().unwrap();

    let config_path = dir.path().join(".varpulis.toml");
    std::fs::write(
        &config_path,
        r#"
[remote]
url = "http://localhost:9000"
api_key = "test-key-123"

[deploy]
name = "my-pipeline"
"#,
    )
    .unwrap();

    let config = ProjectConfig::discover(dir.path());
    assert!(config.is_some(), "Should discover .varpulis.toml");
    let config = config.unwrap();
    assert_eq!(config.remote.url.as_deref(), Some("http://localhost:9000"));
    assert_eq!(config.remote.api_key.as_deref(), Some("test-key-123"));
    assert_eq!(config.deploy.name.as_deref(), Some("my-pipeline"));
}

#[test]
fn test_project_config_without_toml() {
    let dir = tempfile::tempdir().unwrap();
    // No .varpulis.toml created
    let config = ProjectConfig::discover(dir.path());
    assert!(
        config.is_none(),
        "Should return None without .varpulis.toml"
    );
}
