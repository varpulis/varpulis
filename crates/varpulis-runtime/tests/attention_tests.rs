//! Comprehensive tests for the Attention Engine
//!
//! Test coverage targets:
//! - EmbeddingEngine: 100%
//! - EmbeddingCache: 100%
//! - AttentionEngine: 100%
//! - AttentionWindow: 100%
//! - Edge cases and error handling
//! - Business scenarios (Trading, HVAC, Fraud)

use chrono::Utc;
use indexmap::IndexMap;
use std::time::Duration;
use varpulis_core::Value;
use varpulis_runtime::attention::*;
use varpulis_runtime::Event;

// ============================================================================
// TEST HELPERS
// ============================================================================

fn create_event(event_type: &str, data: Vec<(&str, Value)>) -> Event {
    let mut event_data = IndexMap::new();
    for (k, v) in data {
        event_data.insert(k.to_string(), v);
    }
    Event {
        event_type: event_type.to_string(),
        timestamp: Utc::now(),
        data: event_data,
    }
}

// ============================================================================
// EMBEDDING CONFIG TESTS
// ============================================================================

#[test]
fn test_attention_config_default() {
    let config = AttentionConfig::default();
    assert_eq!(config.num_heads, 4);
    assert_eq!(config.embedding_dim, 64);
    assert_eq!(config.threshold, 0.0);
    assert_eq!(config.max_history, 1000);
}

#[test]
fn test_cache_config_default() {
    let config = CacheConfig::default();
    assert_eq!(config.max_size, 10000);
    assert_eq!(config.ttl, Duration::from_secs(300));
    assert!(config.enabled);
}

#[test]
fn test_embedding_config_default() {
    let config = EmbeddingConfig::default();
    assert_eq!(config.embedding_type, EmbeddingType::RuleBased);
    assert!(config.numeric_features.is_empty());
    assert!(config.categorical_features.is_empty());
    assert!(config.model_path.is_none());
}

// ============================================================================
// EMBEDDING ENGINE TESTS - CREATION
// ============================================================================

#[test]
fn test_embedding_engine_creation_default() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    assert_eq!(engine.embedding_dim, 64);
    assert_eq!(engine.num_heads, 4);
}

#[test]
fn test_embedding_engine_creation_custom_dims() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 128, 8);
    assert_eq!(engine.embedding_dim, 128);
    assert_eq!(engine.num_heads, 8);
}

#[test]
fn test_embedding_engine_creation_single_head() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 32, 1);
    assert_eq!(engine.embedding_dim, 32);
    assert_eq!(engine.num_heads, 1);
}

// ============================================================================
// EMBEDDING ENGINE TESTS - AUTO EMBEDDING
// ============================================================================

#[test]
fn test_embed_auto_simple_event() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let event = create_event("TestEvent", vec![
        ("value", Value::Float(42.0)),
    ]);
    
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
    
    // Verify normalization
    let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 0.01, "Embedding should be L2 normalized");
}

#[test]
fn test_embed_auto_multiple_fields() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let event = create_event("Trade", vec![
        ("price", Value::Float(100.5)),
        ("volume", Value::Int(1000)),
        ("symbol", Value::Str("AAPL".to_string())),
        ("active", Value::Bool(true)),
    ]);
    
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn test_embed_auto_empty_data() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let event = create_event("EmptyEvent", vec![]);
    let embedding = engine.embed(&event);
    
    assert_eq!(embedding.len(), 64);
    // Should still have event type contribution
    assert!(!embedding.iter().all(|&x| x == 0.0));
}

#[test]
fn test_embed_deterministic() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let event = create_event("Trade", vec![
        ("price", Value::Float(100.0)),
        ("volume", Value::Int(500)),
    ]);
    
    let emb1 = engine.embed(&event);
    let emb2 = engine.embed(&event);
    
    assert_eq!(emb1, emb2, "Embeddings must be deterministic");
}

#[test]
fn test_embed_different_events_differ() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let event1 = create_event("Trade", vec![("price", Value::Float(100.0))]);
    let event2 = create_event("Trade", vec![("price", Value::Float(200.0))]);
    
    let emb1 = engine.embed(&event1);
    let emb2 = engine.embed(&event2);
    
    assert_ne!(emb1, emb2, "Different events should have different embeddings");
}

#[test]
fn test_embed_different_types_differ() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let event1 = create_event("TypeA", vec![("value", Value::Int(100))]);
    let event2 = create_event("TypeB", vec![("value", Value::Int(100))]);
    
    let emb1 = engine.embed(&event1);
    let emb2 = engine.embed(&event2);
    
    assert_ne!(emb1, emb2, "Different event types should have different embeddings");
}

// ============================================================================
// EMBEDDING ENGINE TESTS - CONFIGURED FEATURES
// ============================================================================

#[test]
fn test_embed_with_numeric_features() {
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
                transform: NumericTransform::Normalize,
                weight: 0.5,
                normalization: None,
            },
        ],
        categorical_features: vec![],
        model_path: None,
    };
    
    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Trade", vec![
        ("price", Value::Float(100.0)),
        ("volume", Value::Int(1000)),
    ]);
    
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
    
    let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 0.01);
}

#[test]
fn test_embed_with_categorical_features() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![],
        categorical_features: vec![
            CategoricalFeatureConfig {
                field: "symbol".to_string(),
                method: CategoricalMethod::Hash,
                dim: 16,
                weight: 1.0,
            },
        ],
        model_path: None,
    };
    
    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Trade", vec![
        ("symbol", Value::Str("AAPL".to_string())),
    ]);
    
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn test_embed_with_mixed_features() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![
            NumericFeatureConfig {
                field: "price".to_string(),
                transform: NumericTransform::Identity,
                weight: 0.3,
                normalization: None,
            },
        ],
        categorical_features: vec![
            CategoricalFeatureConfig {
                field: "market".to_string(),
                method: CategoricalMethod::OneHot { 
                    vocab: vec!["NYSE".to_string(), "NASDAQ".to_string()] 
                },
                dim: 2,
                weight: 0.7,
            },
        ],
        model_path: None,
    };
    
    let engine = EmbeddingEngine::new(config, 64, 4);
    let event = create_event("Trade", vec![
        ("price", Value::Float(150.0)),
        ("market", Value::Str("NASDAQ".to_string())),
    ]);
    
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn test_embed_missing_configured_field() {
    let config = EmbeddingConfig {
        embedding_type: EmbeddingType::RuleBased,
        numeric_features: vec![
            NumericFeatureConfig {
                field: "price".to_string(),
                transform: NumericTransform::Identity,
                weight: 1.0,
                normalization: None,
            },
        ],
        categorical_features: vec![],
        model_path: None,
    };
    
    let engine = EmbeddingEngine::new(config, 64, 4);
    // Event missing "price" field
    let event = create_event("Trade", vec![
        ("volume", Value::Int(1000)),
    ]);
    
    let embedding = engine.embed(&event);
    // Should still produce valid embedding
    assert_eq!(embedding.len(), 64);
}

// ============================================================================
// EMBEDDING ENGINE TESTS - NUMERIC TRANSFORMS
// ============================================================================

#[test]
fn test_transform_identity() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    assert_eq!(engine.transform_numeric(42.0, &NumericTransform::Identity), 42.0);
    assert_eq!(engine.transform_numeric(-10.0, &NumericTransform::Identity), -10.0);
    assert_eq!(engine.transform_numeric(0.0, &NumericTransform::Identity), 0.0);
}

#[test]
fn test_transform_log_scale() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let result = engine.transform_numeric(100.0, &NumericTransform::LogScale);
    assert!((result - (101.0_f64).ln()).abs() < 0.001);
    
    // Negative values
    let neg_result = engine.transform_numeric(-100.0, &NumericTransform::LogScale);
    assert!(neg_result < 0.0);
    assert!((neg_result.abs() - (101.0_f64).ln()).abs() < 0.001);
    
    // Zero
    let zero_result = engine.transform_numeric(0.0, &NumericTransform::LogScale);
    assert!((zero_result - 0.0).abs() < 0.001);
}

#[test]
fn test_transform_normalize() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    // Large positive -> close to 1
    let large = engine.transform_numeric(100.0, &NumericTransform::Normalize);
    assert!(large > 0.99);
    
    // Large negative -> close to 0
    let small = engine.transform_numeric(-100.0, &NumericTransform::Normalize);
    assert!(small < 0.01);
    
    // Zero -> 0.5
    let zero = engine.transform_numeric(0.0, &NumericTransform::Normalize);
    assert!((zero - 0.5).abs() < 0.01);
}

#[test]
fn test_transform_zscore() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let transform = NumericTransform::ZScore { mean: 100.0, std: 10.0 };
    
    // At mean
    assert!((engine.transform_numeric(100.0, &transform) - 0.0).abs() < 0.001);
    // One std above
    assert!((engine.transform_numeric(110.0, &transform) - 1.0).abs() < 0.001);
    // Two std below
    assert!((engine.transform_numeric(80.0, &transform) - (-2.0)).abs() < 0.001);
}

#[test]
fn test_transform_zscore_zero_std() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let transform = NumericTransform::ZScore { mean: 100.0, std: 0.0 };
    
    // Should return 0 when std is 0
    assert_eq!(engine.transform_numeric(150.0, &transform), 0.0);
}

#[test]
fn test_transform_cyclical() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let transform = NumericTransform::Cyclical { period: 24.0 };
    
    let h0 = engine.transform_numeric(0.0, &transform);
    let h6 = engine.transform_numeric(6.0, &transform);
    let h12 = engine.transform_numeric(12.0, &transform);
    let h24 = engine.transform_numeric(24.0, &transform);
    
    // 0 and 24 should be same (full cycle)
    assert!((h0 - h24).abs() < 0.01);
    // 6 should be at peak (sin(π/2) = 1)
    assert!((h6 - 1.0).abs() < 0.01);
    // 12 should be back to 0
    assert!((h12 - 0.0).abs() < 0.01);
}

#[test]
fn test_transform_bucketize() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let transform = NumericTransform::Bucketize { 
        boundaries: vec![10.0, 20.0, 30.0, 40.0] 
    };
    
    // Below first boundary
    assert!((engine.transform_numeric(5.0, &transform) - 0.0).abs() < 0.001);
    // Between 10 and 20
    assert!((engine.transform_numeric(15.0, &transform) - 0.25).abs() < 0.001);
    // Between 20 and 30
    assert!((engine.transform_numeric(25.0, &transform) - 0.5).abs() < 0.001);
    // Above all
    assert!((engine.transform_numeric(50.0, &transform) - 1.0).abs() < 0.001);
}

#[test]
fn test_transform_bucketize_empty() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let transform = NumericTransform::Bucketize { boundaries: vec![] };
    
    // Empty boundaries should return 0
    assert_eq!(engine.transform_numeric(100.0, &transform), 0.0);
}

// ============================================================================
// EMBEDDING ENGINE TESTS - CATEGORICAL METHODS
// ============================================================================

#[test]
fn test_categorical_onehot() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let cat_config = CategoricalFeatureConfig {
        field: "color".to_string(),
        method: CategoricalMethod::OneHot { 
            vocab: vec!["red".to_string(), "green".to_string(), "blue".to_string()] 
        },
        dim: 3,
        weight: 1.0,
    };
    
    let red = engine.embed_categorical("red", &cat_config);
    let green = engine.embed_categorical("green", &cat_config);
    let blue = engine.embed_categorical("blue", &cat_config);
    let unknown = engine.embed_categorical("yellow", &cat_config);
    
    assert_eq!(red, vec![1.0, 0.0, 0.0]);
    assert_eq!(green, vec![0.0, 1.0, 0.0]);
    assert_eq!(blue, vec![0.0, 0.0, 1.0]);
    assert_eq!(unknown, vec![0.0, 0.0, 0.0]); // Unknown = all zeros
}

#[test]
fn test_categorical_hash_deterministic() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let cat_config = CategoricalFeatureConfig {
        field: "symbol".to_string(),
        method: CategoricalMethod::Hash,
        dim: 16,
        weight: 1.0,
    };
    
    let emb1 = engine.embed_categorical("AAPL", &cat_config);
    let emb2 = engine.embed_categorical("AAPL", &cat_config);
    
    assert_eq!(emb1, emb2, "Hash embedding should be deterministic");
}

#[test]
fn test_categorical_hash_different_values() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let cat_config = CategoricalFeatureConfig {
        field: "symbol".to_string(),
        method: CategoricalMethod::Hash,
        dim: 16,
        weight: 1.0,
    };
    
    let aapl = engine.embed_categorical("AAPL", &cat_config);
    let goog = engine.embed_categorical("GOOG", &cat_config);
    let msft = engine.embed_categorical("MSFT", &cat_config);
    
    assert_ne!(aapl, goog);
    assert_ne!(goog, msft);
    assert_ne!(aapl, msft);
}

#[test]
fn test_categorical_lookup() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let mut embeddings = std::collections::HashMap::new();
    embeddings.insert("AAPL".to_string(), vec![0.1, 0.2, 0.3, 0.4]);
    embeddings.insert("GOOG".to_string(), vec![0.5, 0.6, 0.7, 0.8]);
    
    let cat_config = CategoricalFeatureConfig {
        field: "symbol".to_string(),
        method: CategoricalMethod::Lookup { embeddings },
        dim: 4,
        weight: 1.0,
    };
    
    let aapl = engine.embed_categorical("AAPL", &cat_config);
    let goog = engine.embed_categorical("GOOG", &cat_config);
    let unknown = engine.embed_categorical("TSLA", &cat_config);
    
    assert_eq!(aapl, vec![0.1, 0.2, 0.3, 0.4]);
    assert_eq!(goog, vec![0.5, 0.6, 0.7, 0.8]);
    assert_eq!(unknown, vec![0.0, 0.0, 0.0, 0.0]);
}

// ============================================================================
// EMBEDDING ENGINE TESTS - PROJECTIONS
// ============================================================================

#[test]
fn test_projection_dimensions() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let embedding = vec![0.5f32; 64];
    let head_dim = 64 / 4;
    
    let q = engine.project(&embedding, 0, ProjectionType::Query);
    let k = engine.project(&embedding, 0, ProjectionType::Key);
    let v = engine.project(&embedding, 0, ProjectionType::Value);
    
    assert_eq!(q.len(), head_dim);
    assert_eq!(k.len(), head_dim);
    assert_eq!(v.len(), head_dim);
}

#[test]
fn test_projection_different_heads() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let embedding = vec![1.0f32; 64];
    
    let q0 = engine.project(&embedding, 0, ProjectionType::Query);
    let q1 = engine.project(&embedding, 1, ProjectionType::Query);
    let q2 = engine.project(&embedding, 2, ProjectionType::Query);
    let q3 = engine.project(&embedding, 3, ProjectionType::Query);
    
    // Different heads should produce different projections
    assert_ne!(q0, q1);
    assert_ne!(q1, q2);
    assert_ne!(q2, q3);
}

#[test]
fn test_projection_types_differ() {
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 64, 4);
    
    let embedding = vec![1.0f32; 64];
    
    let q = engine.project(&embedding, 0, ProjectionType::Query);
    let k = engine.project(&embedding, 0, ProjectionType::Key);
    let v = engine.project(&embedding, 0, ProjectionType::Value);
    
    assert_ne!(q, k);
    assert_ne!(k, v);
    assert_ne!(q, v);
}

// ============================================================================
// EMBEDDING CACHE TESTS
// ============================================================================

#[test]
fn test_cache_insert_and_get() {
    let config = CacheConfig::default();
    let mut cache = EmbeddingCache::new(config);
    
    let embedding = vec![1.0, 2.0, 3.0];
    cache.insert(42, embedding.clone());
    
    let retrieved = cache.get(42);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap(), embedding);
}

#[test]
fn test_cache_miss() {
    let config = CacheConfig::default();
    let mut cache = EmbeddingCache::new(config);
    
    assert!(cache.get(999).is_none());
    
    let stats = cache.stats();
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 0);
}

#[test]
fn test_cache_hit_stats() {
    let config = CacheConfig::default();
    let mut cache = EmbeddingCache::new(config);
    
    cache.insert(1, vec![1.0]);
    
    let _ = cache.get(1); // Hit
    let _ = cache.get(1); // Hit
    let _ = cache.get(2); // Miss
    
    let stats = cache.stats();
    assert_eq!(stats.hits, 2);
    assert_eq!(stats.misses, 1);
    assert!((stats.hit_rate - 2.0/3.0).abs() < 0.01);
}

#[test]
fn test_cache_lru_eviction() {
    let config = CacheConfig {
        max_size: 3,
        ttl: Duration::from_secs(60),
        enabled: true,
    };
    let mut cache = EmbeddingCache::new(config);
    
    cache.insert(1, vec![1.0]);
    cache.insert(2, vec![2.0]);
    cache.insert(3, vec![3.0]);
    
    // Access 1 to make it recently used
    let _ = cache.get(1);
    
    // Insert 4, should evict 2 (oldest accessed)
    cache.insert(4, vec![4.0]);
    
    assert!(cache.get(1).is_some());
    assert!(cache.get(2).is_none()); // Evicted
    assert!(cache.get(3).is_some());
    assert!(cache.get(4).is_some());
}

#[test]
fn test_cache_disabled() {
    let config = CacheConfig {
        max_size: 100,
        ttl: Duration::from_secs(60),
        enabled: false,
    };
    let mut cache = EmbeddingCache::new(config);
    
    cache.insert(42, vec![1.0, 2.0]);
    assert!(cache.get(42).is_none());
}

#[test]
fn test_cache_clear() {
    let config = CacheConfig::default();
    let mut cache = EmbeddingCache::new(config);
    
    cache.insert(1, vec![1.0]);
    cache.insert(2, vec![2.0]);
    cache.insert(3, vec![3.0]);
    
    cache.clear();
    
    assert!(cache.get(1).is_none());
    assert!(cache.get(2).is_none());
    assert!(cache.get(3).is_none());
    assert_eq!(cache.stats().size, 0);
}

#[test]
fn test_cache_overwrite() {
    let config = CacheConfig::default();
    let mut cache = EmbeddingCache::new(config);
    
    cache.insert(42, vec![1.0]);
    cache.insert(42, vec![2.0]);
    
    let retrieved = cache.get(42).unwrap();
    assert_eq!(retrieved, vec![2.0]);
}

// ============================================================================
// ATTENTION ENGINE TESTS - CREATION
// ============================================================================

#[test]
fn test_attention_engine_creation() {
    let config = AttentionConfig::default();
    let engine = AttentionEngine::new(config);
    
    let stats = engine.stats();
    assert_eq!(stats.history_size, 0);
    assert_eq!(stats.computations, 0);
    assert_eq!(stats.total_events, 0);
}

#[test]
fn test_attention_engine_custom_config() {
    let config = AttentionConfig {
        num_heads: 8,
        embedding_dim: 128,
        threshold: 0.5,
        max_history: 500,
        ..Default::default()
    };
    let engine = AttentionEngine::new(config);
    
    let stats = engine.stats();
    assert_eq!(stats.max_history, 500);
}

// ============================================================================
// ATTENTION ENGINE TESTS - EVENT MANAGEMENT
// ============================================================================

#[test]
fn test_attention_add_event() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    let event = create_event("Trade", vec![("price", Value::Float(100.0))]);
    engine.add_event(event);
    
    let stats = engine.stats();
    assert_eq!(stats.history_size, 1);
    assert_eq!(stats.total_events, 1);
}

#[test]
fn test_attention_add_multiple_events() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    for i in 0..10 {
        let event = create_event("Trade", vec![("price", Value::Float(i as f64))]);
        engine.add_event(event);
    }
    
    let stats = engine.stats();
    assert_eq!(stats.history_size, 10);
    assert_eq!(stats.total_events, 10);
}

#[test]
fn test_attention_history_limit() {
    let mut config = AttentionConfig::default();
    config.max_history = 5;
    let mut engine = AttentionEngine::new(config);
    
    for i in 0..20 {
        let event = create_event("Trade", vec![("price", Value::Float(i as f64))]);
        engine.add_event(event);
    }
    
    let stats = engine.stats();
    assert_eq!(stats.history_size, 5);
    assert_eq!(stats.total_events, 20);
}

#[test]
fn test_attention_clear_history() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    for i in 0..10 {
        let event = create_event("Trade", vec![("price", Value::Float(i as f64))]);
        engine.add_event(event);
    }
    
    engine.clear_history();
    
    assert_eq!(engine.stats().history_size, 0);
}

#[test]
fn test_attention_get_history() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    for i in 0..3 {
        let event = create_event("Trade", vec![("idx", Value::Int(i))]);
        engine.add_event(event);
    }
    
    let history = engine.get_history();
    assert_eq!(history.len(), 3);
}

// ============================================================================
// ATTENTION ENGINE TESTS - COMPUTE ATTENTION
// ============================================================================

#[test]
fn test_compute_attention_empty_history() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    let event = create_event("Trade", vec![("price", Value::Float(100.0))]);
    let result = engine.compute_attention(&event);
    
    assert!(result.scores.is_empty());
    assert_eq!(result.head_weights.len(), 4);
    assert_eq!(result.context.len(), 64);
}

#[test]
fn test_compute_attention_with_history() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    for i in 0..5 {
        let event = create_event("Trade", vec![("price", Value::Float(100.0 + i as f64))]);
        engine.add_event(event);
    }
    
    let current = create_event("Trade", vec![("price", Value::Float(102.5))]);
    let result = engine.compute_attention(&current);
    
    assert_eq!(result.scores.len(), 5);
    assert_eq!(result.head_weights.len(), 4);
    assert_eq!(result.context.len(), 64);
}

#[test]
fn test_compute_attention_increments_stats() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    engine.add_event(create_event("Trade", vec![]));
    
    let current = create_event("Trade", vec![]);
    let _ = engine.compute_attention(&current);
    let _ = engine.compute_attention(&current);
    let _ = engine.compute_attention(&current);
    
    assert_eq!(engine.stats().computations, 3);
}

#[test]
fn test_compute_attention_threshold_filtering() {
    let mut config = AttentionConfig::default();
    config.threshold = 100.0; // Very high threshold
    let mut engine = AttentionEngine::new(config);
    
    for i in 0..10 {
        let event = create_event("Trade", vec![("price", Value::Float(i as f64))]);
        engine.add_event(event);
    }
    
    let current = create_event("Trade", vec![("price", Value::Float(5.0))]);
    let result = engine.compute_attention(&current);
    
    // All scores should be filtered out with high threshold
    assert!(result.scores.is_empty() || result.scores.iter().all(|(_, s)| *s >= 100.0));
}

// ============================================================================
// ATTENTION ENGINE TESTS - ATTENTION SCORE
// ============================================================================

#[test]
fn test_attention_score_deterministic() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    let e1 = create_event("Trade", vec![("price", Value::Float(100.0))]);
    let e2 = create_event("Trade", vec![("price", Value::Float(150.0))]);
    
    let score1 = engine.attention_score(&e1, &e2);
    let score2 = engine.attention_score(&e1, &e2);
    
    assert_eq!(score1, score2, "Attention score must be deterministic");
}

#[test]
fn test_attention_score_symmetric_tendency() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    let e1 = create_event("Trade", vec![("price", Value::Float(100.0))]);
    let e2 = create_event("Trade", vec![("price", Value::Float(100.0))]);
    
    let score12 = engine.attention_score(&e1, &e2);
    let score21 = engine.attention_score(&e2, &e1);
    
    // Same events should have same score regardless of order
    assert_eq!(score12, score21);
}

#[test]
fn test_attention_score_similar_vs_different() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    let base = create_event("Trade", vec![
        ("price", Value::Float(100.0)),
        ("volume", Value::Int(1000)),
    ]);
    
    let similar = create_event("Trade", vec![
        ("price", Value::Float(101.0)),
        ("volume", Value::Int(1010)),
    ]);
    
    let different = create_event("Order", vec![
        ("price", Value::Float(500.0)),
        ("volume", Value::Int(50000)),
    ]);
    
    let score_similar = engine.attention_score(&base, &similar);
    let score_different = engine.attention_score(&base, &different);
    
    // Similar events should correlate more
    assert!(score_similar > score_different,
        "Similar: {}, Different: {}", score_similar, score_different);
}

// ============================================================================
// ATTENTION WINDOW TESTS
// ============================================================================

#[test]
fn test_attention_window_creation() {
    let config = AttentionConfig::default();
    let window = AttentionWindow::new(config, Duration::from_secs(60));
    
    assert_eq!(window.stats().history_size, 0);
}

#[test]
fn test_attention_window_process_single() {
    let config = AttentionConfig::default();
    let mut window = AttentionWindow::new(config, Duration::from_secs(60));
    
    let event = create_event("Trade", vec![("price", Value::Float(100.0))]);
    let result = window.process(event);
    
    // First event has no history to attend to
    assert!(result.scores.is_empty());
    assert_eq!(window.history().len(), 1);
}

#[test]
fn test_attention_window_process_multiple() {
    let config = AttentionConfig::default();
    let mut window = AttentionWindow::new(config, Duration::from_secs(60));
    
    let e1 = create_event("Trade", vec![("price", Value::Float(100.0))]);
    let e2 = create_event("Trade", vec![("price", Value::Float(105.0))]);
    let e3 = create_event("Trade", vec![("price", Value::Float(110.0))]);
    
    let _ = window.process(e1);
    let r2 = window.process(e2);
    let r3 = window.process(e3);
    
    assert_eq!(r2.scores.len(), 1); // Sees e1
    assert_eq!(r3.scores.len(), 2); // Sees e1, e2
    assert_eq!(window.history().len(), 3);
}

#[test]
fn test_attention_window_attention_score() {
    let config = AttentionConfig::default();
    let mut window = AttentionWindow::new(config, Duration::from_secs(60));
    
    let e1 = create_event("Trade", vec![("price", Value::Float(100.0))]);
    let e2 = create_event("Trade", vec![("price", Value::Float(100.0))]);
    
    let score = window.attention_score(&e1, &e2);
    assert!(score.is_finite());
}

// ============================================================================
// INTEGRATION TESTS - TRADING SCENARIO
// ============================================================================

#[test]
fn test_trading_price_correlation() {
    let config = AttentionConfig {
        num_heads: 4,
        embedding_dim: 64,
        threshold: -10.0, // Low to get all scores
        max_history: 100,
        ..Default::default()
    };
    let mut engine = AttentionEngine::new(config);
    
    // Add AAPL trades
    for i in 0..5 {
        let event = create_event("Trade", vec![
            ("symbol", Value::Str("AAPL".to_string())),
            ("price", Value::Float(150.0 + i as f64)),
            ("volume", Value::Int(1000 + i * 100)),
        ]);
        engine.add_event(event);
    }
    
    // Add GOOG trades
    for i in 0..5 {
        let event = create_event("Trade", vec![
            ("symbol", Value::Str("GOOG".to_string())),
            ("price", Value::Float(2800.0 + i as f64 * 10.0)),
            ("volume", Value::Int(500 + i * 50)),
        ]);
        engine.add_event(event);
    }
    
    // Query similar to AAPL
    let query = create_event("Trade", vec![
        ("symbol", Value::Str("AAPL".to_string())),
        ("price", Value::Float(152.0)),
        ("volume", Value::Int(1100)),
    ]);
    
    let result = engine.compute_attention(&query);
    assert_eq!(result.scores.len(), 10);
}

#[test]
fn test_trading_volume_spike() {
    let config = AttentionConfig::default();
    let mut window = AttentionWindow::new(config, Duration::from_secs(300));
    
    // Normal volume trades
    for _ in 0..10 {
        let event = create_event("Trade", vec![
            ("symbol", Value::Str("AAPL".to_string())),
            ("volume", Value::Int(1000)),
        ]);
        window.process(event);
    }
    
    // Volume spike
    let spike = create_event("Trade", vec![
        ("symbol", Value::Str("AAPL".to_string())),
        ("volume", Value::Int(100000)),
    ]);
    
    let result = window.process(spike);
    // After 10 events, there should be history
    assert_eq!(window.history().len(), 11);
}

// ============================================================================
// INTEGRATION TESTS - FRAUD DETECTION SCENARIO
// ============================================================================

#[test]
fn test_fraud_detection_unusual_amount() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    // Normal transactions
    for i in 0..10 {
        let event = create_event("Transaction", vec![
            ("user_id", Value::Str("user123".to_string())),
            ("amount", Value::Float(50.0 + i as f64 * 10.0)),
            ("location", Value::Str("NYC".to_string())),
        ]);
        engine.add_event(event);
    }
    
    // Suspicious transaction
    let suspicious = create_event("Transaction", vec![
        ("user_id", Value::Str("user123".to_string())),
        ("amount", Value::Float(50000.0)),
        ("location", Value::Str("Lagos".to_string())),
    ]);
    
    let normal = create_event("Transaction", vec![
        ("user_id", Value::Str("user123".to_string())),
        ("amount", Value::Float(75.0)),
        ("location", Value::Str("NYC".to_string())),
    ]);
    
    // Normal transaction should have higher correlation with history
    // Clone the first event from history to avoid borrow issues
    let first_event = engine.get_history()[0].clone();
    let score_normal = engine.attention_score(&first_event, &normal);
    let score_suspicious = engine.attention_score(&first_event, &suspicious);
    
    assert!(score_normal > score_suspicious,
        "Normal tx should correlate more than suspicious: {} vs {}", 
        score_normal, score_suspicious);
}

#[test]
fn test_fraud_detection_velocity() {
    let config = AttentionConfig::default();
    let mut window = AttentionWindow::new(config, Duration::from_secs(60));
    
    // Rapid transactions (potential fraud velocity)
    for i in 0..20 {
        let event = create_event("Transaction", vec![
            ("user_id", Value::Str("user456".to_string())),
            ("amount", Value::Float(100.0)),
            ("merchant", Value::Str(format!("merchant_{}", i % 5))),
        ]);
        let result = window.process(event);
        
        if i > 0 {
            assert!(!result.scores.is_empty());
        }
    }
    
    assert_eq!(window.history().len(), 20);
}

// ============================================================================
// INTEGRATION TESTS - HVAC DEGRADATION SCENARIO
// ============================================================================

#[test]
fn test_hvac_normal_operation() {
    let config = AttentionConfig::default();
    let mut window = AttentionWindow::new(config, Duration::from_secs(3600));
    
    // Normal HVAC readings
    for i in 0..10 {
        let event = create_event("HVACReading", vec![
            ("unit_id", Value::Str("HVAC-001".to_string())),
            ("temperature", Value::Float(22.0 + (i as f64 * 0.1))),
            ("power", Value::Float(100.0 + (i as f64 * 2.0))),
            ("efficiency", Value::Float(0.85)),
        ]);
        window.process(event);
    }
    
    let history = window.history();
    assert_eq!(history.len(), 10);
}

#[test]
fn test_hvac_degradation_detection() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    // Normal readings
    for _ in 0..5 {
        let event = create_event("HVACReading", vec![
            ("unit_id", Value::Str("HVAC-001".to_string())),
            ("temperature", Value::Float(22.0)),
            ("power", Value::Float(100.0)),
            ("efficiency", Value::Float(0.85)),
        ]);
        engine.add_event(event);
    }
    
    // Degraded readings
    for i in 0..5 {
        let event = create_event("HVACReading", vec![
            ("unit_id", Value::Str("HVAC-001".to_string())),
            ("temperature", Value::Float(24.0 + i as f64)),
            ("power", Value::Float(130.0 + i as f64 * 10.0)),
            ("efficiency", Value::Float(0.75 - i as f64 * 0.05)),
        ]);
        engine.add_event(event);
    }
    
    // Current degraded reading
    let current = create_event("HVACReading", vec![
        ("unit_id", Value::Str("HVAC-001".to_string())),
        ("temperature", Value::Float(28.0)),
        ("power", Value::Float(180.0)),
        ("efficiency", Value::Float(0.55)),
    ]);
    
    let result = engine.compute_attention(&current);
    assert_eq!(result.scores.len(), 10);
}

// ============================================================================
// EDGE CASES AND ROBUSTNESS TESTS
// ============================================================================

#[test]
fn test_edge_empty_event_type() {
    let config = AttentionConfig::default();
    let engine = EmbeddingEngine::new(config.embedding_config, 64, 4);
    
    let event = create_event("", vec![("value", Value::Int(42))]);
    let embedding = engine.embed(&event);
    
    assert_eq!(embedding.len(), 64);
}

#[test]
fn test_edge_very_large_values() {
    let config = AttentionConfig::default();
    let engine = EmbeddingEngine::new(config.embedding_config, 64, 4);
    
    let event = create_event("Test", vec![
        ("big", Value::Float(1e100)),
        ("small", Value::Float(1e-100)),
    ]);
    
    let embedding = engine.embed(&event);
    assert!(embedding.iter().all(|x| x.is_finite()));
}

#[test]
fn test_edge_negative_values() {
    let config = AttentionConfig::default();
    let engine = EmbeddingEngine::new(config.embedding_config, 64, 4);
    
    let event = create_event("Test", vec![
        ("negative", Value::Float(-1000.0)),
        ("neg_int", Value::Int(-999)),
    ]);
    
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn test_edge_unicode_strings() {
    let config = AttentionConfig::default();
    let engine = EmbeddingEngine::new(config.embedding_config, 64, 4);
    
    let event = create_event("Test", vec![
        ("emoji", Value::Str("🚀💰📈".to_string())),
        ("chinese", Value::Str("股票交易".to_string())),
        ("arabic", Value::Str("تداول".to_string())),
    ]);
    
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn test_edge_very_long_string() {
    let config = AttentionConfig::default();
    let engine = EmbeddingEngine::new(config.embedding_config, 64, 4);
    
    let long_string = "x".repeat(10000);
    let event = create_event("Test", vec![
        ("long", Value::Str(long_string)),
    ]);
    
    let embedding = engine.embed(&event);
    assert_eq!(embedding.len(), 64);
}

#[test]
fn test_edge_many_fields() {
    let config = AttentionConfig::default();
    let engine = EmbeddingEngine::new(config.embedding_config, 64, 4);
    
    let mut data = Vec::new();
    for i in 0..100 {
        data.push((format!("field_{}", i).leak() as &str, Value::Float(i as f64)));
    }
    
    let event = create_event("Test", data);
    let embedding = engine.embed(&event);
    
    assert_eq!(embedding.len(), 64);
}

#[test]
fn test_edge_special_timestamp() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    // Use epoch time as a special case
    let mut event = create_event("Trade", vec![("price", Value::Float(100.0))]);
    event.timestamp = chrono::DateTime::from_timestamp(0, 0).unwrap();
    engine.add_event(event.clone());
    
    let result = engine.compute_attention(&event);
    assert!(result.scores.len() <= 1);
}

#[test]
fn test_edge_concurrent_access_simulation() {
    let config = AttentionConfig::default();
    let mut engine = AttentionEngine::new(config);
    
    // Simulate rapid sequential access
    for i in 0..1000 {
        let event = create_event("Event", vec![("idx", Value::Int(i))]);
        engine.add_event(event.clone());
        
        if i % 10 == 0 {
            let _ = engine.compute_attention(&event);
        }
    }
    
    let stats = engine.stats();
    assert!(stats.total_events >= 1000);
    assert!(stats.computations >= 100);
}

#[test]
fn test_edge_zero_embedding_dim() {
    // This is an edge case that should be handled gracefully
    let config = EmbeddingConfig::default();
    let engine = EmbeddingEngine::new(config, 0, 1);
    
    let event = create_event("Test", vec![]);
    let embedding = engine.embed(&event);
    
    assert_eq!(embedding.len(), 0);
}

#[test]
fn test_edge_single_head() {
    let config = AttentionConfig {
        num_heads: 1,
        embedding_dim: 32,
        ..Default::default()
    };
    let mut engine = AttentionEngine::new(config);
    
    engine.add_event(create_event("Test", vec![("v", Value::Int(1))]));
    engine.add_event(create_event("Test", vec![("v", Value::Int(2))]));
    
    let result = engine.compute_attention(&create_event("Test", vec![("v", Value::Int(3))]));
    
    assert_eq!(result.head_weights.len(), 1);
}

#[test]
fn test_edge_max_history_one() {
    let mut config = AttentionConfig::default();
    config.max_history = 1;
    let mut engine = AttentionEngine::new(config);
    
    for i in 0..10 {
        engine.add_event(create_event("Test", vec![("i", Value::Int(i))]));
    }
    
    assert_eq!(engine.stats().history_size, 1);
}
