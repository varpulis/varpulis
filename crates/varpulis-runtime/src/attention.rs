//! Attention Engine for Varpulis
//!
//! Implements deterministic attention mechanism for event correlation.
//! Unlike probabilistic LLMs, attention here is:
//! - Computed reproducibly (same inputs = same outputs)
//! - No generation, only correlation scoring
//! - Embeddings are rule-based or loaded from pre-trained models

use crate::event::Event;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use varpulis_core::Value;

// ============================================================================
// CONFIGURATION
// ============================================================================

/// Configuration for the attention engine
#[derive(Debug, Clone)]
pub struct AttentionConfig {
    pub num_heads: usize,
    pub embedding_dim: usize,
    pub threshold: f32,
    pub max_history: usize,
    pub cache_config: CacheConfig,
    pub embedding_config: EmbeddingConfig,
}

impl Default for AttentionConfig {
    fn default() -> Self {
        Self {
            num_heads: 4,
            embedding_dim: 64,
            threshold: 0.0,
            max_history: 1000,
            cache_config: CacheConfig::default(),
            embedding_config: EmbeddingConfig::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub max_size: usize,
    pub ttl: Duration,
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size: 10000,
            ttl: Duration::from_secs(300),
            enabled: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EmbeddingConfig {
    pub embedding_type: EmbeddingType,
    pub numeric_features: Vec<NumericFeatureConfig>,
    pub categorical_features: Vec<CategoricalFeatureConfig>,
    pub model_path: Option<String>,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            embedding_type: EmbeddingType::RuleBased,
            numeric_features: Vec::new(),
            categorical_features: Vec::new(),
            model_path: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum EmbeddingType {
    RuleBased,
    Learned,
    Composite,
}

#[derive(Debug, Clone)]
pub struct NumericFeatureConfig {
    pub field: String,
    pub transform: NumericTransform,
    pub weight: f32,
    pub normalization: Option<(f64, f64)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NumericTransform {
    Identity,
    LogScale,
    Normalize,
    ZScore { mean: f64, std: f64 },
    Cyclical { period: f64 },
    Bucketize { boundaries: Vec<f64> },
}

#[derive(Debug, Clone)]
pub struct CategoricalFeatureConfig {
    pub field: String,
    pub method: CategoricalMethod,
    pub dim: usize,
    pub weight: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CategoricalMethod {
    OneHot { vocab: Vec<String> },
    Hash,
    Lookup { embeddings: HashMap<String, Vec<f32>> },
}

// ============================================================================
// EMBEDDING ENGINE
// ============================================================================

pub struct EmbeddingEngine {
    config: EmbeddingConfig,
    query_projections: Vec<Vec<f32>>,
    key_projections: Vec<Vec<f32>>,
    value_projections: Vec<Vec<f32>>,
    pub embedding_dim: usize,
    pub num_heads: usize,
}

impl EmbeddingEngine {
    pub fn new(config: EmbeddingConfig, embedding_dim: usize, num_heads: usize) -> Self {
        let head_dim = embedding_dim / num_heads;
        let mut query_projections = Vec::with_capacity(num_heads);
        let mut key_projections = Vec::with_capacity(num_heads);
        let mut value_projections = Vec::with_capacity(num_heads);

        for head in 0..num_heads {
            let seed = head as f32;
            query_projections.push(Self::init_projection(embedding_dim, head_dim, seed, 0.0));
            key_projections.push(Self::init_projection(embedding_dim, head_dim, seed, 1.0));
            value_projections.push(Self::init_projection(embedding_dim, head_dim, seed, 2.0));
        }

        Self { config, query_projections, key_projections, value_projections, embedding_dim, num_heads }
    }

    fn init_projection(input_dim: usize, output_dim: usize, seed: f32, offset: f32) -> Vec<f32> {
        let size = input_dim * output_dim;
        let scale = (2.0 / (input_dim + output_dim) as f32).sqrt();
        (0..size).map(|i| {
            let x = (i as f32 + seed * 1000.0 + offset * 10000.0) * 0.1;
            (x.sin() * 43_758.547).fract() * 2.0 - 1.0
        } * scale).collect()
    }

    pub fn embed(&self, event: &Event) -> Vec<f32> {
        match self.config.embedding_type {
            EmbeddingType::RuleBased => self.embed_rule_based(event),
            EmbeddingType::Learned => self.embed_rule_based(event), // Fallback
            EmbeddingType::Composite => self.embed_rule_based(event),
        }
    }

    fn embed_rule_based(&self, event: &Event) -> Vec<f32> {
        let mut embedding = vec![0.0f32; self.embedding_dim];
        
        if self.config.numeric_features.is_empty() && self.config.categorical_features.is_empty() {
            embedding = self.embed_auto(event);
        } else {
            let mut idx = 0;
            let features_count = self.config.numeric_features.len() + self.config.categorical_features.len();
            let dim_per_feature = self.embedding_dim / features_count.max(1);

            for feat in &self.config.numeric_features {
                if let Some(value) = event.data.get(&feat.field) {
                    if let Some(num) = value_to_f64(value) {
                        let transformed = self.transform_numeric(num, &feat.transform);
                        for i in 0..dim_per_feature.min(self.embedding_dim - idx) {
                            let phase = i as f32 * std::f32::consts::PI / dim_per_feature as f32;
                            embedding[idx + i] = (transformed as f32) * phase.cos() * feat.weight;
                        }
                        idx += dim_per_feature;
                    }
                }
            }

            for feat in &self.config.categorical_features {
                if let Some(value) = event.data.get(&feat.field) {
                    if let Some(s) = value_to_string(value) {
                        let cat_emb = self.embed_categorical(&s, feat);
                        for i in 0..cat_emb.len().min(self.embedding_dim - idx) {
                            embedding[idx + i] = cat_emb[i] * feat.weight;
                        }
                        idx += dim_per_feature;
                    }
                }
            }
        }

        self.normalize(&mut embedding);
        embedding
    }

    fn embed_auto(&self, event: &Event) -> Vec<f32> {
        let mut embedding = vec![0.0f32; self.embedding_dim];
        if self.embedding_dim == 0 { return embedding; }
        embedding[0] = self.hash_string(&event.event_type);

        for (field_name, value) in &event.data {
            let pos = ((self.hash_string(field_name).abs() * 1000.0) as usize) % self.embedding_dim;
            match value {
                Value::Int(i) => embedding[pos] += (*i as f32).clamp(-1e6, 1e6) * 0.001,
                Value::Float(f) => {
                    let clamped = (*f as f32).clamp(-1e6, 1e6);
                    if clamped.is_finite() { embedding[pos] += clamped * 0.01; }
                }
                Value::Str(s) => embedding[pos] += self.hash_string(s),
                Value::Bool(b) => embedding[pos] += if *b { 1.0 } else { -1.0 },
                _ => {}
            }
        }
        embedding
    }

    pub fn transform_numeric(&self, value: f64, transform: &NumericTransform) -> f64 {
        match transform {
            NumericTransform::Identity => value,
            NumericTransform::LogScale => (1.0 + value.abs()).ln() * value.signum(),
            NumericTransform::Normalize => 1.0 / (1.0 + (-value * 0.1).exp()),
            NumericTransform::ZScore { mean, std } => if *std > 0.0 { (value - mean) / std } else { 0.0 },
            NumericTransform::Cyclical { period } => ((value / period) * 2.0 * std::f64::consts::PI).sin(),
            NumericTransform::Bucketize { boundaries } => {
                boundaries.iter().filter(|&&b| value > b).count() as f64 / boundaries.len().max(1) as f64
            }
        }
    }

    pub fn embed_categorical(&self, value: &str, config: &CategoricalFeatureConfig) -> Vec<f32> {
        match &config.method {
            CategoricalMethod::OneHot { vocab } => {
                let mut emb = vec![0.0f32; vocab.len().max(config.dim)];
                if let Some(idx) = vocab.iter().position(|v| v == value) {
                    if idx < emb.len() { emb[idx] = 1.0; }
                }
                emb
            }
            CategoricalMethod::Hash => {
                let mut emb = vec![0.0f32; config.dim];
                let hash = self.hash_string(value);
                for i in 0..3 {
                    let pos = ((hash.abs() * (i + 1) as f32 * 7919.0) as usize) % config.dim;
                    emb[pos] = if i % 2 == 0 { 1.0 } else { -1.0 };
                }
                emb
            }
            CategoricalMethod::Lookup { embeddings } => {
                embeddings.get(value).cloned().unwrap_or_else(|| vec![0.0f32; config.dim])
            }
        }
    }

    fn hash_string(&self, s: &str) -> f32 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        s.hash(&mut hasher);
        ((hasher.finish() % 10000) as f32 / 5000.0) - 1.0
    }

    fn normalize(&self, embedding: &mut [f32]) {
        // Clamp any non-finite values first
        for e in embedding.iter_mut() {
            if !e.is_finite() { *e = 0.0; }
        }
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 1e-8 && norm.is_finite() {
            for e in embedding.iter_mut() { *e /= norm; }
        }
    }

    pub fn project(&self, embedding: &[f32], head: usize, proj_type: ProjectionType) -> Vec<f32> {
        let projection = match proj_type {
            ProjectionType::Query => &self.query_projections[head],
            ProjectionType::Key => &self.key_projections[head],
            ProjectionType::Value => &self.value_projections[head],
        };
        let head_dim = self.embedding_dim / self.num_heads;
        let mut result = vec![0.0f32; head_dim];
        for i in 0..head_dim {
            for j in 0..self.embedding_dim.min(embedding.len()) {
                result[i] += projection[i * self.embedding_dim + j] * embedding[j];
            }
        }
        result
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProjectionType { Query, Key, Value }

// ============================================================================
// EMBEDDING CACHE
// ============================================================================

pub struct EmbeddingCache {
    entries: HashMap<u64, CacheEntry>,
    access_order: Vec<u64>,
    config: CacheConfig,
    hits: u64,
    misses: u64,
}

struct CacheEntry {
    embedding: Vec<f32>,
    timestamp: Instant,
}

impl EmbeddingCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            entries: HashMap::with_capacity(config.max_size),
            access_order: Vec::with_capacity(config.max_size),
            config, hits: 0, misses: 0,
        }
    }

    pub fn get(&mut self, event_hash: u64) -> Option<Vec<f32>> {
        if !self.config.enabled { return None; }
        if let Some(entry) = self.entries.get(&event_hash) {
            if entry.timestamp.elapsed() > self.config.ttl {
                self.entries.remove(&event_hash);
                self.access_order.retain(|&h| h != event_hash);
                self.misses += 1;
                return None;
            }
            self.access_order.retain(|&h| h != event_hash);
            self.access_order.push(event_hash);
            self.hits += 1;
            Some(entry.embedding.clone())
        } else {
            self.misses += 1;
            None
        }
    }

    pub fn insert(&mut self, event_hash: u64, embedding: Vec<f32>) {
        if !self.config.enabled { return; }
        while self.entries.len() >= self.config.max_size && !self.access_order.is_empty() {
            let oldest = self.access_order.remove(0);
            self.entries.remove(&oldest);
        }
        self.entries.insert(event_hash, CacheEntry { embedding, timestamp: Instant::now() });
        self.access_order.push(event_hash);
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.entries.len(),
            capacity: self.config.max_size,
            hits: self.hits,
            misses: self.misses,
            hit_rate: if self.hits + self.misses > 0 { self.hits as f64 / (self.hits + self.misses) as f64 } else { 0.0 },
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.access_order.clear();
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
}

// ============================================================================
// ATTENTION ENGINE
// ============================================================================

#[derive(Debug, Clone)]
pub struct AttentionResult {
    pub scores: Vec<(String, f32)>,
    pub head_weights: Vec<f32>,
    pub context: Vec<f32>,
}

pub struct AttentionEngine {
    config: AttentionConfig,
    embedding_engine: EmbeddingEngine,
    cache: EmbeddingCache,
    history: Vec<(Event, Vec<f32>)>,
    computations: u64,
    total_events_processed: u64,
}

impl AttentionEngine {
    pub fn new(config: AttentionConfig) -> Self {
        let embedding_engine = EmbeddingEngine::new(config.embedding_config.clone(), config.embedding_dim, config.num_heads);
        let cache = EmbeddingCache::new(config.cache_config.clone());
        Self { config, embedding_engine, cache, history: Vec::new(), computations: 0, total_events_processed: 0 }
    }

    pub fn add_event(&mut self, event: Event) {
        let event_hash = self.hash_event(&event);
        let embedding = self.cache.get(event_hash).unwrap_or_else(|| {
            let emb = self.embedding_engine.embed(&event);
            self.cache.insert(event_hash, emb.clone());
            emb
        });
        self.history.push((event, embedding));
        self.total_events_processed += 1;
        while self.history.len() > self.config.max_history { self.history.remove(0); }
    }

    pub fn compute_attention(&mut self, current: &Event) -> AttentionResult {
        self.computations += 1;
        let event_hash = self.hash_event(current);
        let current_embedding = self.cache.get(event_hash).unwrap_or_else(|| {
            let emb = self.embedding_engine.embed(current);
            self.cache.insert(event_hash, emb.clone());
            emb
        });

        if self.history.is_empty() {
            return AttentionResult {
                scores: Vec::new(),
                head_weights: vec![0.0; self.config.num_heads],
                context: vec![0.0; self.config.embedding_dim],
            };
        }

        let head_dim = self.config.embedding_dim / self.config.num_heads;
        let mut all_scores = Vec::with_capacity(self.history.len());

        for (hist_event, hist_embedding) in &self.history {
            let mut total = 0.0f32;
            for head in 0..self.config.num_heads {
                let q = self.embedding_engine.project(&current_embedding, head, ProjectionType::Query);
                let k = self.embedding_engine.project(hist_embedding, head, ProjectionType::Key);
                total += self.dot_product(&q, &k) / (head_dim as f32).sqrt();
            }
            let avg = total / self.config.num_heads as f32;
            let event_id = format!("{}_{}", hist_event.event_type, hist_event.timestamp.timestamp_millis());
            all_scores.push((event_id, avg));
        }

        let context = self.compute_context(&all_scores);
        let filtered: Vec<_> = all_scores.into_iter().filter(|(_, s)| *s >= self.config.threshold).collect();

        AttentionResult {
            scores: filtered,
            head_weights: vec![1.0 / self.config.num_heads as f32; self.config.num_heads],
            context,
        }
    }

    fn compute_context(&self, scores: &[(String, f32)]) -> Vec<f32> {
        let mut context = vec![0.0f32; self.config.embedding_dim];
        if scores.is_empty() || self.history.is_empty() { return context; }
        
        let max_score = scores.iter().map(|(_, s)| *s).fold(f32::NEG_INFINITY, f32::max);
        let weights: Vec<f32> = scores.iter().map(|(_, s)| (s - max_score).exp()).collect();
        let sum: f32 = weights.iter().sum();
        if sum < 1e-8 { return context; }

        for (i, (_, emb)) in self.history.iter().enumerate() {
            if i >= weights.len() { break; }
            let w = weights[i] / sum;
            for (j, &e) in emb.iter().enumerate() {
                if j < context.len() { context[j] += w * e; }
            }
        }
        context
    }

    fn dot_product(&self, a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }

    fn hash_event(&self, event: &Event) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        event.event_type.hash(&mut hasher);
        let mut keys: Vec<_> = event.data.keys().collect();
        keys.sort();
        for key in keys {
            key.hash(&mut hasher);
            if let Some(v) = event.data.get(key) { format!("{:?}", v).hash(&mut hasher); }
        }
        hasher.finish()
    }

    pub fn attention_score(&self, e1: &Event, e2: &Event) -> f32 {
        let emb1 = self.embedding_engine.embed(e1);
        let emb2 = self.embedding_engine.embed(e2);
        let head_dim = self.config.embedding_dim / self.config.num_heads;
        let mut total = 0.0f32;
        for head in 0..self.config.num_heads {
            let q = self.embedding_engine.project(&emb1, head, ProjectionType::Query);
            let k = self.embedding_engine.project(&emb2, head, ProjectionType::Key);
            total += self.dot_product(&q, &k) / (head_dim as f32).sqrt();
        }
        total / self.config.num_heads as f32
    }

    pub fn clear_history(&mut self) { self.history.clear(); }

    pub fn stats(&self) -> AttentionStats {
        AttentionStats {
            history_size: self.history.len(),
            max_history: self.config.max_history,
            computations: self.computations,
            total_events: self.total_events_processed,
            cache_stats: self.cache.stats(),
        }
    }

    pub fn get_history(&self) -> Vec<&Event> { self.history.iter().map(|(e, _)| e).collect() }
}

#[derive(Debug, Clone)]
pub struct AttentionStats {
    pub history_size: usize,
    pub max_history: usize,
    pub computations: u64,
    pub total_events: u64,
    pub cache_stats: CacheStats,
}

// ============================================================================
// ATTENTION WINDOW
// ============================================================================

pub struct AttentionWindow {
    engine: AttentionEngine,
    duration: Duration,
    start_time: Option<Instant>,
}

impl AttentionWindow {
    pub fn new(config: AttentionConfig, duration: Duration) -> Self {
        Self { engine: AttentionEngine::new(config), duration, start_time: None }
    }

    pub fn process(&mut self, event: Event) -> AttentionResult {
        if self.start_time.is_none() { self.start_time = Some(Instant::now()); }
        if let Some(start) = self.start_time {
            if start.elapsed() > self.duration {
                self.engine.clear_history();
                self.start_time = Some(Instant::now());
            }
        }
        let result = self.engine.compute_attention(&event);
        self.engine.add_event(event);
        result
    }

    pub fn attention_score(&self, e1: &Event, e2: &Event) -> f32 { self.engine.attention_score(e1, e2) }
    pub fn history(&self) -> Vec<&Event> { self.engine.get_history() }
    pub fn stats(&self) -> AttentionStats { self.engine.stats() }
}

// ============================================================================
// HELPERS
// ============================================================================

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Str(s) => Some(s.clone()),
        Value::Int(i) => Some(i.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}
