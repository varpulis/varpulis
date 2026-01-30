//! Tests for mutable variables functionality

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::{Alert, Engine};

#[tokio::test]
async fn test_var_declaration() {
    let code = r#"
        var threshold: float = 10.0
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    // Check that variable was registered
    let value = engine.get_variable("threshold");
    assert!(value.is_some(), "Variable 'threshold' should exist");
    assert_eq!(*value.unwrap(), Value::Float(10.0));
}

#[tokio::test]
async fn test_let_declaration() {
    let code = r#"
        let max_count: int = 100
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    let value = engine.get_variable("max_count");
    assert!(value.is_some(), "Variable 'max_count' should exist");
    assert_eq!(*value.unwrap(), Value::Int(100));
}

#[tokio::test]
async fn test_assignment_to_mutable_var() {
    let code = r#"
        var counter: int = 0
        counter := 5
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    let value = engine.get_variable("counter");
    assert!(value.is_some());
    assert_eq!(*value.unwrap(), Value::Int(5));
}

#[tokio::test]
async fn test_assignment_to_immutable_var_fails() {
    let code = r#"
        let constant: int = 42
        constant := 100
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    let result = engine.load(&program);

    assert!(
        result.is_err(),
        "Assignment to immutable variable should fail"
    );
    assert!(result.unwrap_err().contains("immutable"));
}

#[tokio::test]
async fn test_assignment_with_expression() {
    let code = r#"
        var base: int = 10
        var multiplier: int = 3
        base := base * multiplier + 5
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    let value = engine.get_variable("base");
    assert!(value.is_some());
    assert_eq!(*value.unwrap(), Value::Int(35)); // 10 * 3 + 5 = 35
}

#[tokio::test]
async fn test_implicit_mutable_on_first_assignment() {
    // Assignment to non-existent variable creates it as mutable
    let code = r#"
        new_var := 42
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    let value = engine.get_variable("new_var");
    assert!(value.is_some());
    assert_eq!(*value.unwrap(), Value::Int(42));
}

#[tokio::test]
async fn test_multiple_assignments() {
    let code = r#"
        var counter: int = 0
        counter := 1
        counter := 2
        counter := 3
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    let value = engine.get_variable("counter");
    assert!(value.is_some());
    assert_eq!(*value.unwrap(), Value::Int(3));
}

#[tokio::test]
async fn test_var_with_float_expression() {
    let code = r#"
        var pi: float = 3.14159
        var radius: float = 5.0
        var area: float = pi * radius * radius
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    let area = engine.get_variable("area");
    assert!(area.is_some());
    if let Value::Float(v) = area.unwrap() {
        assert!(
            (v - 78.53975).abs() < 0.001,
            "Area should be ~78.54, got {}",
            v
        );
    } else {
        panic!("Expected Float value");
    }
}

#[tokio::test]
async fn test_var_with_string() {
    let code = r#"
        var message: str = "hello"
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    let value = engine.get_variable("message");
    assert!(value.is_some());
    assert_eq!(*value.unwrap(), Value::Str("hello".to_string()));
}

#[tokio::test]
async fn test_var_with_bool() {
    let code = r#"
        var enabled: bool = true
        enabled := false
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    let value = engine.get_variable("enabled");
    assert!(value.is_some());
    assert_eq!(*value.unwrap(), Value::Bool(false));
}

#[tokio::test]
async fn test_variables_api() {
    let code = r#"
        var a: int = 1
        var b: int = 2
        let c: int = 3
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    let vars = engine.variables();
    assert_eq!(vars.len(), 3);
    assert!(vars.contains_key("a"));
    assert!(vars.contains_key("b"));
    assert!(vars.contains_key("c"));
}

#[tokio::test]
async fn test_set_variable_api() {
    let code = r#"
        var counter: int = 0
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    // Use API to set variable
    engine
        .set_variable("counter", Value::Int(42))
        .expect("Should succeed");
    assert_eq!(*engine.get_variable("counter").unwrap(), Value::Int(42));
}

#[tokio::test]
async fn test_set_immutable_variable_fails() {
    let code = r#"
        let constant: int = 100
    "#;

    let program = parse(code).expect("Failed to parse");

    let (alert_tx, _alert_rx) = mpsc::channel::<Alert>(100);
    let mut engine = Engine::new(alert_tx);
    engine.load(&program).expect("Failed to load program");

    // Try to set immutable variable via API
    let result = engine.set_variable("constant", Value::Int(200));
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("immutable"));
}
