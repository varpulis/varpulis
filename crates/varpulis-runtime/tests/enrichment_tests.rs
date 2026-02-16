//! Integration tests for .enrich() — external connector enrichment
//!
//! Tests cover:
//! 1. Parser: VPL with .enrich() parses correctly
//! 2. Validation: connector compatibility checks
//! 3. Engine: .enrich() compiles and loads
//! 4. Cache: TTL expiry, max entries eviction, hit/miss stats

use std::collections::HashMap;
use std::time::Duration;

use varpulis_core::ast::*;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::enrichment::EnrichmentCache;

// ============================================================================
// Parser tests
// ============================================================================

#[test]
fn test_enrich_parses_full_params() {
    let code = r#"
        connector WeatherAPI = http(url: "https://api.weather.com/v1")
        stream Enriched = Temperature as t
            .enrich(WeatherAPI, key: t.city, fields: [forecast, humidity], cache_ttl: 5m, timeout: 2s, fallback: "unknown")
            .emit(city: t.city)
    "#;

    let program = parse(code).expect("parse should succeed");
    let stmts = &program.statements;
    assert!(stmts.len() >= 2, "Expected at least 2 statements");

    // Find the stream declaration
    let stream_stmt = stmts
        .iter()
        .find(|s| matches!(&s.node, Stmt::StreamDecl { .. }))
        .expect("stream decl");

    if let Stmt::StreamDecl { ops, .. } = &stream_stmt.node {
        let enrich_op = ops
            .iter()
            .find(|op| matches!(op, StreamOp::Enrich(_)))
            .expect("should have Enrich op");

        if let StreamOp::Enrich(spec) = enrich_op {
            assert_eq!(spec.connector_name, "WeatherAPI");
            assert_eq!(spec.fields, vec!["forecast", "humidity"]);
            assert!(spec.cache_ttl.is_some());
            assert!(spec.timeout.is_some());
            assert!(spec.fallback.is_some());
        }
    } else {
        panic!("Expected StreamDecl");
    }
}

#[test]
fn test_enrich_parses_minimal_params() {
    let code = r#"
        connector MyAPI = http(url: "https://example.com")
        stream Enriched = Event as e
            .enrich(MyAPI, key: e.id, fields: [name])
            .emit()
    "#;

    let program = parse(code).expect("parse should succeed");
    let stream_stmt = program
        .statements
        .iter()
        .find(|s| matches!(&s.node, Stmt::StreamDecl { .. }))
        .expect("stream decl");

    if let Stmt::StreamDecl { ops, .. } = &stream_stmt.node {
        let enrich_op = ops
            .iter()
            .find(|op| matches!(op, StreamOp::Enrich(_)))
            .expect("should have Enrich op");

        if let StreamOp::Enrich(spec) = enrich_op {
            assert_eq!(spec.connector_name, "MyAPI");
            assert_eq!(spec.fields, vec!["name"]);
            assert!(spec.cache_ttl.is_none());
            assert!(spec.timeout.is_none());
            assert!(spec.fallback.is_none());
        }
    }
}

#[test]
fn test_enrich_parses_multiple_fields() {
    let code = r#"
        connector DB = database(url: "postgres://localhost/test", query: "SELECT * FROM users WHERE id = $1")
        stream WithUser = Order as o
            .enrich(DB, key: o.user_id, fields: [name, email, tier, country])
            .emit()
    "#;

    let program = parse(code).expect("parse should succeed");
    let stream_stmt = program
        .statements
        .iter()
        .find(|s| matches!(&s.node, Stmt::StreamDecl { .. }))
        .expect("stream decl");

    if let Stmt::StreamDecl { ops, .. } = &stream_stmt.node {
        let enrich_op = ops
            .iter()
            .find(|op| matches!(op, StreamOp::Enrich(_)))
            .expect("should have Enrich op");

        if let StreamOp::Enrich(spec) = enrich_op {
            assert_eq!(spec.fields, vec!["name", "email", "tier", "country"]);
        }
    }
}

#[test]
fn test_enrich_chained_with_where_emit() {
    let code = r#"
        connector Redis = redis(url: "redis://localhost:6379")
        stream Premium = Click as c
            .enrich(Redis, key: c.user_id, fields: [user_tier, prefs], cache_ttl: 10m)
            .where(user_tier == "premium")
            .emit(user: c.user_id, tier: user_tier)
    "#;

    let program = parse(code).expect("parse should succeed");
    let stream_stmt = program
        .statements
        .iter()
        .find(|s| matches!(&s.node, Stmt::StreamDecl { .. }))
        .expect("stream decl");

    if let Stmt::StreamDecl { ops, .. } = &stream_stmt.node {
        let op_types: Vec<&str> = ops
            .iter()
            .map(|op| match op {
                StreamOp::Enrich(_) => "enrich",
                StreamOp::Where(_) => "where",
                StreamOp::Emit { .. } => "emit",
                _ => "other",
            })
            .collect();
        assert_eq!(op_types, vec!["enrich", "where", "emit"]);
    }
}

// ============================================================================
// Engine load tests
// ============================================================================

#[tokio::test]
async fn test_enrich_engine_loads() {
    use tokio::sync::mpsc;
    use varpulis_runtime::engine::Engine;

    let code = r#"
        connector API = http(url: "https://example.com/api")
        stream Enriched = Event as e
            .enrich(API, key: e.id, fields: [name, category], cache_ttl: 5m)
            .emit()
    "#;

    let program = parse(code).expect("parse");
    let (tx, _rx) = mpsc::channel(4096);
    let mut engine = Engine::new(tx);
    let result = engine.load(&program);

    assert!(
        result.is_ok(),
        "Engine should load .enrich() program successfully, got: {:?}",
        result.err()
    );

    assert!(
        engine.stream_names().contains(&"Enriched"),
        "Stream 'Enriched' should be registered"
    );
}

#[tokio::test]
async fn test_enrich_requires_compatible_connector() {
    let code = r#"
        connector MqttSource = mqtt(url: "mqtt://localhost:1883")
        stream Bad = Event as e
            .enrich(MqttSource, key: e.id, fields: [name])
            .emit()
    "#;

    let program = parse(code).expect("parse");
    let diagnostics = varpulis_core::validate::validate(code, &program);
    // Should contain error for incompatible connector type
    let has_error = diagnostics
        .diagnostics
        .iter()
        .any(|d| d.message.contains("E032") || d.message.contains("not compatible"));
    assert!(
        has_error,
        "Should reject mqtt connector for .enrich(), got diagnostics: {:?}",
        diagnostics.diagnostics
    );
}

// ============================================================================
// Cache unit tests
// ============================================================================

#[test]
fn test_cache_insert_and_get() {
    let cache = EnrichmentCache::new(Duration::from_secs(60));

    let mut fields = HashMap::new();
    fields.insert("name".to_string(), Value::str("Alice"));
    fields.insert("age".to_string(), Value::Int(30));

    cache.insert("user:123".to_string(), fields.clone());

    let result = cache.get("user:123");
    assert!(result.is_some());
    let retrieved = result.unwrap();
    assert_eq!(retrieved.get("name"), Some(&Value::str("Alice")));
    assert_eq!(retrieved.get("age"), Some(&Value::Int(30)));
}

#[test]
fn test_cache_miss_on_unknown_key() {
    let cache = EnrichmentCache::new(Duration::from_secs(60));
    assert!(cache.get("nonexistent").is_none());
}

#[test]
fn test_cache_ttl_expiry() {
    let cache = EnrichmentCache::new(Duration::from_millis(10));

    let mut fields = HashMap::new();
    fields.insert("name".to_string(), Value::str("Bob"));
    cache.insert("user:456".to_string(), fields);

    // Wait for TTL to expire
    std::thread::sleep(Duration::from_millis(20));

    assert!(
        cache.get("user:456").is_none(),
        "Expired entry should not be returned"
    );
}

#[test]
fn test_cache_stats() {
    let cache = EnrichmentCache::new(Duration::from_secs(60));

    let mut fields = HashMap::new();
    fields.insert("name".to_string(), Value::str("Carol"));
    cache.insert("user:789".to_string(), fields);

    // 1 hit
    cache.get("user:789");
    // 1 miss
    cache.get("nonexistent");

    let (hits, misses) = cache.stats();
    assert_eq!(hits, 1);
    assert_eq!(misses, 1);
}

#[test]
fn test_cache_eviction_at_capacity() {
    let cache = EnrichmentCache::new(Duration::from_secs(3600));

    // Insert enough entries to trigger eviction
    for i in 0..100_010 {
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), Value::Int(i));
        cache.insert(format!("key:{}", i), fields);
    }

    // Verify no panics and latest entry is still accessible
    let result = cache.get("key:100009");
    assert!(result.is_some(), "Latest entry should still be accessible");
}
