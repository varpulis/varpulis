//! Additional coverage tests for attention.rs
//!
//! Targets uncovered paths in:
//! - EmbeddingEngine: configured numeric/categorical features, generate embeddings
//! - AttentionEngine: add_events_batch, compute_attention_batch
//! - AttentionWindow: windowed attention computation
//! - HNSW index: nearest neighbor search operations
//! - Configured features: numeric fields, categorical fields
//! - Cross-attention: attention between different event types
//! - Edge cases: empty batches, single event, very large feature values, missing fields

#![allow(clippy::field_reassign_with_default)]

use indexmap::IndexMap;
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::time::Duration;
use varpulis_core::Value;
use varpulis_runtime::attention::*;
use varpulis_runtime::Event;

// ============================================================================
// HELPERS
// ============================================================================

fn create_event(event_type: &str, data: Vec<(&str, Value)>) -> Event {
    let mut event_data = IndexMap::with_hasher(FxBuildHasher);
    for (k, v) in data {
        event_data.insert(k.into(), v);
    }
    Event {
        event_type: event_type.into(),
        timestamp: chrono::Utc::now(),
        data: event_data,
    }
}

// ============================================================================
// 1. EmbeddingEngine - Configured Features
// ============================================================================

#[test]
fn embed_with_zscore_numeric_feature() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![NumericFeatureConfig {
            field: "temperature".to_string(),
            transform: NumericTransform::ZScore {
                mean: 20.0,
                std: 5.0,
            },
            weight: 1.0,
            normalization: None,
        }],
        categorical_features: vec![],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Sensor", vec![("temperature", Value::Float(25.0))]);

    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);

    let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 0.01, "Should be normalized");
}

#[test]
fn embed_with_cyclical_numeric_feature() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![NumericFeatureConfig {
            field: "hour".to_string(),
            transform: NumericTransform::Cyclical { period: 24.0 },
            weight: 0.8,
            normalization: None,
        }],
        categorical_features: vec![],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Log", vec![("hour", Value::Int(12))]);

    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn embed_with_bucketize_numeric_feature() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![NumericFeatureConfig {
            field: "latency".to_string(),
            transform: NumericTransform::Bucketize {
                boundaries: vec![10.0, 50.0, 100.0, 500.0],
            },
            weight: 1.0,
            normalization: None,
        }],
        categorical_features: vec![],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Request", vec![("latency", Value::Float(75.0))]);

    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn embed_with_onehot_categorical_feature() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![],
        categorical_features: vec![CategoricalFeatureConfig {
            field: "status".to_string(),
            method: CategoricalMethod::OneHot {
                vocab: vec!["ok".to_string(), "warn".to_string(), "error".to_string()],
            },
            dim: 3,
            weight: 1.0,
        }],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Alert", vec![("status", Value::Str("warn".into()))]);

    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn embed_with_lookup_categorical_feature() {
    let mut embeddings = FxHashMap::default();
    embeddings.insert("NYC".to_string(), vec![0.1, 0.2, 0.3, 0.4]);
    embeddings.insert("LON".to_string(), vec![0.5, 0.6, 0.7, 0.8]);

    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![],
        categorical_features: vec![CategoricalFeatureConfig {
            field: "city".to_string(),
            method: CategoricalMethod::Lookup { embeddings },
            dim: 4,
            weight: 1.0,
        }],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Location", vec![("city", Value::Str("NYC".into()))]);

    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn embed_with_multiple_numeric_and_categorical() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![
            NumericFeatureConfig {
                field: "price".to_string(),
                transform: NumericTransform::LogScale,
                weight: 0.5,
                normalization: None,
            },
            NumericFeatureConfig {
                field: "volume".to_string(),
                transform: NumericTransform::Identity,
                weight: 0.3,
                normalization: None,
            },
        ],
        categorical_features: vec![CategoricalFeatureConfig {
            field: "exchange".to_string(),
            method: CategoricalMethod::Hash,
            dim: 8,
            weight: 0.2,
        }],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event(
        "Trade",
        vec![
            ("price", Value::Float(150.0)),
            ("volume", Value::Int(1000)),
            ("exchange", Value::Str("NYSE".into())),
        ],
    );

    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
    let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 0.01);
}

#[test]
fn embed_learned_type_falls_back_to_rule_based() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::Learned,
        numeric_features: vec![],
        categorical_features: vec![],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Test", vec![("v", Value::Float(1.0))]);
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn embed_composite_type_falls_back_to_rule_based() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::Composite,
        numeric_features: vec![],
        categorical_features: vec![],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Test", vec![("v", Value::Int(42))]);
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

// ============================================================================
// 2. AttentionEngine: add_events_batch
// ============================================================================

#[test]
fn add_events_batch_empty() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);

    engine.add_events_batch(vec![]);
    assert_eq!(engine.stats().history_size, 0);
    assert_eq!(engine.stats().total_events, 0);
}

#[test]
fn add_events_batch_single() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);

    let event = create_event("Trade", vec![("price", Value::Float(100.0))]);
    engine.add_events_batch(vec![event]);

    assert_eq!(engine.stats().history_size, 1);
    assert_eq!(engine.stats().total_events, 1);
}

#[test]
fn add_events_batch_multiple() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);

    let events: Vec<Event> = (0..10)
        .map(|i| create_event("Trade", vec![("price", Value::Float(100.0 + i as f64))]))
        .collect();

    engine.add_events_batch(events);

    assert_eq!(engine.stats().history_size, 10);
    assert_eq!(engine.stats().total_events, 10);
}

#[test]
fn add_events_batch_exceeds_max_history() {
    let config = AttentionConfig {
        max_history: 5,
        ..Default::default()
    };
    let mut engine = AttentionEngine::new(config);

    let events: Vec<Event> = (0..20)
        .map(|i| create_event("Trade", vec![("price", Value::Float(i as f64))]))
        .collect();

    engine.add_events_batch(events);

    assert_eq!(engine.stats().history_size, 5);
    assert_eq!(engine.stats().total_events, 20);
}

#[test]
fn add_events_batch_then_compute() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);

    let events: Vec<Event> = (0..5)
        .map(|i| create_event("Trade", vec![("price", Value::Float(100.0 + i as f64))]))
        .collect();

    engine.add_events_batch(events);

    let query = create_event("Trade", vec![("price", Value::Float(102.0))]);
    let result = engine.compute_attention(&query);

    assert_eq!(result.scores.len(), 5);
}

// ============================================================================
// 3. AttentionEngine: compute_attention_batch
// ============================================================================

#[test]
fn compute_attention_batch_empty_history() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);

    let events = vec![
        create_event("Trade", vec![("price", Value::Float(100.0))]),
        create_event("Trade", vec![("price", Value::Float(200.0))]),
    ];

    let results = engine.compute_attention_batch(&events);
    assert_eq!(results.len(), 2);
    assert!(results[0].scores.is_empty());
    assert!(results[1].scores.is_empty());

    // With empty history, the early return path doesn't add events to history
    // This tests the empty-history early return branch
    assert_eq!(engine.stats().history_size, 0);
}

#[test]
fn compute_attention_batch_with_history() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);

    // Add some history
    for i in 0..5 {
        engine.add_event(create_event(
            "Trade",
            vec![("price", Value::Float(100.0 + i as f64))],
        ));
    }

    let batch = vec![
        create_event("Trade", vec![("price", Value::Float(102.0))]),
        create_event("Trade", vec![("price", Value::Float(103.0))]),
        create_event("Trade", vec![("price", Value::Float(104.0))]),
    ];

    let results = engine.compute_attention_batch(&batch);
    assert_eq!(results.len(), 3);

    for result in &results {
        assert_eq!(
            result.scores.len(),
            5,
            "Each should see all 5 history events"
        );
        assert_eq!(result.head_weights.len(), 4);
        assert_eq!(result.context.len(), 64);
    }

    // Batch events should now also be in history
    assert_eq!(engine.stats().history_size, 8);
}

#[test]
fn compute_attention_batch_updates_stats() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);

    engine.add_event(create_event("Trade", vec![]));

    let batch = vec![create_event("Trade", vec![]), create_event("Trade", vec![])];

    let _ = engine.compute_attention_batch(&batch);

    let stats = engine.stats();
    assert_eq!(stats.computations, 2);
    assert_eq!(stats.total_events, 3); // 1 added + 2 from batch
}

// ============================================================================
// 4. HNSW index: nearest neighbor search
// ============================================================================

#[test]
fn hnsw_used_for_large_history() {
    let config = AttentionConfig {
        max_history: 200,
        ..Default::default()
    };
    let mut engine = AttentionEngine::new(config);

    // Add enough events to trigger HNSW search (min_size = 32)
    for i in 0..50 {
        engine.add_event(create_event(
            "Trade",
            vec![("price", Value::Float(i as f64))],
        ));
    }

    let query = create_event("Trade", vec![("price", Value::Float(25.0))]);
    let result = engine.compute_attention(&query);

    // HNSW may return fewer candidates than total history
    // The important thing is it does not crash and returns results
    assert!(!result.scores.is_empty());
}

#[test]
fn engine_without_hnsw() {
    let config = AttentionConfig {
        max_history: 200,
        ..Default::default()
    };
    let mut engine = AttentionEngine::new_without_hnsw(config);

    for i in 0..50 {
        engine.add_event(create_event(
            "Trade",
            vec![("price", Value::Float(i as f64))],
        ));
    }

    let query = create_event("Trade", vec![("price", Value::Float(25.0))]);
    let result = engine.compute_attention(&query);

    // Without HNSW, all history events are candidates
    assert_eq!(result.scores.len(), 50);
}

// ============================================================================
// 5. Configured features
// ============================================================================

#[test]
fn configured_numeric_field_with_bool_value() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![NumericFeatureConfig {
            field: "active".to_string(),
            transform: NumericTransform::Identity,
            weight: 1.0,
            normalization: None,
        }],
        categorical_features: vec![],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    // Bool should be convertible to f64 via value_to_f64
    let event = create_event("Status", vec![("active", Value::Bool(true))]);
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn configured_categorical_field_with_int_value() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![],
        categorical_features: vec![CategoricalFeatureConfig {
            field: "code".to_string(),
            method: CategoricalMethod::Hash,
            dim: 8,
            weight: 1.0,
        }],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    // Int should be convertible to string via value_to_string
    let event = create_event("Response", vec![("code", Value::Int(200))]);
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn configured_categorical_field_with_float_value() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![],
        categorical_features: vec![CategoricalFeatureConfig {
            field: "score".to_string(),
            method: CategoricalMethod::Hash,
            dim: 8,
            weight: 1.0,
        }],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Test", vec![("score", Value::Float(3.125))]);
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn configured_categorical_field_with_bool_value() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![],
        categorical_features: vec![CategoricalFeatureConfig {
            field: "flag".to_string(),
            method: CategoricalMethod::Hash,
            dim: 8,
            weight: 1.0,
        }],
        model_path: None,
    };

    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Test", vec![("flag", Value::Bool(false))]);
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

// ============================================================================
// 6. Cross-attention: different event types
// ============================================================================

#[test]
fn cross_attention_different_event_types() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);

    // Add different types of events to history
    engine.add_event(create_event(
        "Trade",
        vec![("price", Value::Float(100.0)), ("volume", Value::Int(1000))],
    ));
    engine.add_event(create_event(
        "Order",
        vec![("price", Value::Float(99.0)), ("quantity", Value::Int(500))],
    ));
    engine.add_event(create_event(
        "Quote",
        vec![("bid", Value::Float(99.5)), ("ask", Value::Float(100.5))],
    ));

    // Query with a Trade event
    let query = create_event(
        "Trade",
        vec![("price", Value::Float(100.5)), ("volume", Value::Int(1200))],
    );

    let result = engine.compute_attention(&query);
    assert_eq!(
        result.scores.len(),
        3,
        "Should attend to all 3 history events"
    );
}

#[test]
fn attention_score_between_different_types() {
    let config = AttentionConfig::default();
    let engine = AttentionEngine::new(config);

    let trade = create_event("Trade", vec![("price", Value::Float(100.0))]);
    let order = create_event("Order", vec![("price", Value::Float(100.0))]);

    let score = engine.attention_score(&trade, &order);
    assert!(score.is_finite());
}

// ============================================================================
// 7. Edge Cases
// ============================================================================

#[test]
fn attention_window_clears_on_expiry() {
    let config = AttentionConfig::default();
    // Very short duration to test window reset
    let mut window = AttentionWindow::new(config, Duration::from_millis(1));

    // Process first event
    let e1 = create_event("Trade", vec![("price", Value::Float(100.0))]);
    let _ = window.process(e1);

    // Sleep to ensure window expires
    std::thread::sleep(Duration::from_millis(10));

    // Process second event - should trigger window clear
    let e2 = create_event("Trade", vec![("price", Value::Float(200.0))]);
    let result = window.process(e2);

    // After clear, no history to attend to
    assert!(result.scores.is_empty());
}

#[test]
fn attention_window_stats_and_history() {
    let config = AttentionConfig::default();
    let mut window = AttentionWindow::new(config, Duration::from_secs(60));

    let e1 = create_event("Trade", vec![("v", Value::Int(1))]);
    let e2 = create_event("Trade", vec![("v", Value::Int(2))]);

    window.process(e1);
    window.process(e2);

    let stats = window.stats();
    assert_eq!(stats.history_size, 2);

    let history = window.history();
    assert_eq!(history.len(), 2);
}

#[test]
fn embed_auto_with_null_value() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);

    let event = create_event("Test", vec![("empty", Value::Null)]);
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn cache_ttl_expiration() {
    let config = CacheConfig {
        max_size: 100,
        ttl: Duration::from_millis(1), // Very short TTL
        enabled: true,
    };
    let mut cache = EmbeddingCache::new(config);

    cache.insert(42, vec![1.0, 2.0, 3.0]);

    // Sleep to expire
    std::thread::sleep(Duration::from_millis(10));

    let result = cache.get(42);
    assert!(result.is_none(), "Entry should be expired");

    let stats = cache.stats();
    assert_eq!(stats.misses, 1);
}

#[test]
fn attention_stats_check_performance_no_warnings() {
    let stats = AttentionStats {
        history_size: 100,
        max_history: 1000,
        computations: 10,
        total_events: 100,
        cache_stats: CacheStats {
            size: 50,
            capacity: 10000,
            hits: 80,
            misses: 20,
            hit_rate: 0.8,
        },
        avg_compute_time_us: 500.0,
        max_compute_time_us: 2000,
        total_ops: 10000,
        ops_per_sec: 1_000_000.0,
    };

    assert!(
        stats.check_performance().is_none(),
        "Should have no warnings for normal stats"
    );
}

#[test]
fn attention_stats_check_performance_with_warnings() {
    let stats = AttentionStats {
        history_size: 15000, // > 10K
        max_history: 20000,
        computations: 100,
        total_events: 15000,
        cache_stats: CacheStats {
            size: 100,
            capacity: 10000,
            hits: 0,
            misses: 0,
            hit_rate: 0.0,
        },
        avg_compute_time_us: 50_000.0, // > 10ms
        max_compute_time_us: 200_000,  // > 100ms
        total_ops: 100000,
        ops_per_sec: 100.0,
    };

    let warnings = stats.check_performance();
    assert!(warnings.is_some());
    let text = warnings.unwrap();
    assert!(
        text.contains("History size"),
        "Should warn about history size"
    );
    assert!(
        text.contains("Avg compute"),
        "Should warn about avg compute time"
    );
    assert!(
        text.contains("Max compute"),
        "Should warn about max compute time"
    );
}

#[test]
fn attention_stats_estimated_throughput() {
    let stats = AttentionStats {
        history_size: 100,
        max_history: 1000,
        computations: 10,
        total_events: 100,
        cache_stats: CacheStats {
            size: 0,
            capacity: 100,
            hits: 0,
            misses: 0,
            hit_rate: 0.0,
        },
        avg_compute_time_us: 1000.0, // 1ms
        max_compute_time_us: 2000,
        total_ops: 10000,
        ops_per_sec: 1000.0,
    };

    let throughput = stats.estimated_throughput();
    assert!(
        (throughput - 1000.0).abs() < 0.1,
        "1ms per op = 1000 ops/sec"
    );
}

#[test]
fn attention_stats_estimated_throughput_zero() {
    let stats = AttentionStats {
        history_size: 0,
        max_history: 1000,
        computations: 0,
        total_events: 0,
        cache_stats: CacheStats {
            size: 0,
            capacity: 100,
            hits: 0,
            misses: 0,
            hit_rate: 0.0,
        },
        avg_compute_time_us: 0.0,
        max_compute_time_us: 0,
        total_ops: 0,
        ops_per_sec: 0.0,
    };

    let throughput = stats.estimated_throughput();
    assert!(
        throughput.is_infinite(),
        "Zero compute time should mean infinite throughput"
    );
}

#[test]
fn cache_stats_zero_operations() {
    let config = CacheConfig::default();
    let cache = EmbeddingCache::new(config);

    let stats = cache.stats();
    assert_eq!(stats.size, 0);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 0);
    assert!((stats.hit_rate - 0.0).abs() < 0.001);
}

#[test]
fn compute_attention_batch_max_history_overflow() {
    let config = AttentionConfig {
        max_history: 5,
        ..Default::default()
    };
    let mut engine = AttentionEngine::new(config);

    // Add 3 events first
    for i in 0..3 {
        engine.add_event(create_event(
            "Trade",
            vec![("price", Value::Float(i as f64))],
        ));
    }

    // Batch of 5 more events -> total 8 > max_history 5
    let batch: Vec<Event> = (3..8)
        .map(|i| create_event("Trade", vec![("price", Value::Float(i as f64))]))
        .collect();

    let results = engine.compute_attention_batch(&batch);
    assert_eq!(results.len(), 5);

    // History should be capped at 5
    assert_eq!(engine.stats().history_size, 5);
}

#[test]
fn add_events_batch_with_mixed_types() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);

    let events = vec![
        create_event("Trade", vec![("price", Value::Float(100.0))]),
        create_event("Order", vec![("qty", Value::Int(500))]),
        create_event("Quote", vec![("bid", Value::Float(99.0))]),
    ];

    engine.add_events_batch(events);
    assert_eq!(engine.stats().history_size, 3);

    let query = create_event("Trade", vec![("price", Value::Float(100.0))]);
    let result = engine.compute_attention(&query);
    assert_eq!(result.scores.len(), 3);
}
