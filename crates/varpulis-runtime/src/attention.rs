//! Attention Engine for Varpulis
//!
//! Implements deterministic attention mechanism for event correlation.
//! Unlike probabilistic LLMs, attention here is:
//! - Computed reproducibly (same inputs = same outputs)
//! - No generation, only correlation scoring
//! - Embeddings are rule-based or loaded from pre-trained models

#![allow(clippy::needless_range_loop)]
#![allow(clippy::unnecessary_to_owned)]

use crate::event::Event;
use hnsw_rs::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
#[allow(unused_imports)]
use std::sync::Arc;
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
    OneHot {
        vocab: Vec<String>,
    },
    Hash,
    Lookup {
        embeddings: HashMap<String, Vec<f32>>,
    },
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

        Self {
            config,
            query_projections,
            key_projections,
            value_projections,
            embedding_dim,
            num_heads,
        }
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
            let features_count =
                self.config.numeric_features.len() + self.config.categorical_features.len();
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
        if self.embedding_dim == 0 {
            return embedding;
        }
        embedding[0] = self.hash_string(&event.event_type);

        for (field_name, value) in &event.data {
            let pos = ((self.hash_string(field_name).abs() * 1000.0) as usize) % self.embedding_dim;
            match value {
                Value::Int(i) => embedding[pos] += (*i as f32).clamp(-1e6, 1e6) * 0.001,
                Value::Float(f) => {
                    let clamped = (*f as f32).clamp(-1e6, 1e6);
                    if clamped.is_finite() {
                        embedding[pos] += clamped * 0.01;
                    }
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
            NumericTransform::ZScore { mean, std } => {
                if *std > 0.0 {
                    (value - mean) / std
                } else {
                    0.0
                }
            }
            NumericTransform::Cyclical { period } => {
                ((value / period) * 2.0 * std::f64::consts::PI).sin()
            }
            NumericTransform::Bucketize { boundaries } => {
                boundaries.iter().filter(|&&b| value > b).count() as f64
                    / boundaries.len().max(1) as f64
            }
        }
    }

    pub fn embed_categorical(&self, value: &str, config: &CategoricalFeatureConfig) -> Vec<f32> {
        match &config.method {
            CategoricalMethod::OneHot { vocab } => {
                let mut emb = vec![0.0f32; vocab.len().max(config.dim)];
                if let Some(idx) = vocab.iter().position(|v| v == value) {
                    if idx < emb.len() {
                        emb[idx] = 1.0;
                    }
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
            CategoricalMethod::Lookup { embeddings } => embeddings
                .get(value)
                .cloned()
                .unwrap_or_else(|| vec![0.0f32; config.dim]),
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
            if !e.is_finite() {
                *e = 0.0;
            }
        }
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 1e-8 && norm.is_finite() {
            for e in embedding.iter_mut() {
                *e /= norm;
            }
        }
    }

    pub fn project(&self, embedding: &[f32], head: usize, proj_type: ProjectionType) -> Vec<f32> {
        let projection = match proj_type {
            ProjectionType::Query => &self.query_projections[head],
            ProjectionType::Key => &self.key_projections[head],
            ProjectionType::Value => &self.value_projections[head],
        };
        let head_dim = self.embedding_dim / self.num_heads;
        let emb_len = self.embedding_dim.min(embedding.len());
        let mut result = vec![0.0f32; head_dim];

        // SIMD-optimized matrix-vector multiplication with loop unrolling
        for i in 0..head_dim {
            let row_start = i * self.embedding_dim;
            let mut sum = 0.0f32;

            // Process 4 elements at a time
            let chunks = emb_len / 4;
            for c in 0..chunks {
                let base = c * 4;
                unsafe {
                    sum += *projection.get_unchecked(row_start + base)
                        * *embedding.get_unchecked(base);
                    sum += *projection.get_unchecked(row_start + base + 1)
                        * *embedding.get_unchecked(base + 1);
                    sum += *projection.get_unchecked(row_start + base + 2)
                        * *embedding.get_unchecked(base + 2);
                    sum += *projection.get_unchecked(row_start + base + 3)
                        * *embedding.get_unchecked(base + 3);
                }
            }

            // Handle remainder
            for j in (chunks * 4)..emb_len {
                unsafe {
                    sum += *projection.get_unchecked(row_start + j) * *embedding.get_unchecked(j);
                }
            }

            result[i] = sum;
        }
        result
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProjectionType {
    Query,
    Key,
    Value,
}

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
            config,
            hits: 0,
            misses: 0,
        }
    }

    pub fn get(&mut self, event_hash: u64) -> Option<Vec<f32>> {
        if !self.config.enabled {
            return None;
        }
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
        if !self.config.enabled {
            return;
        }
        while self.entries.len() >= self.config.max_size && !self.access_order.is_empty() {
            let oldest = self.access_order.remove(0);
            self.entries.remove(&oldest);
        }
        self.entries.insert(
            event_hash,
            CacheEntry {
                embedding,
                timestamp: Instant::now(),
            },
        );
        self.access_order.push(event_hash);
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.entries.len(),
            capacity: self.config.max_size,
            hits: self.hits,
            misses: self.misses,
            hit_rate: if self.hits + self.misses > 0 {
                self.hits as f64 / (self.hits + self.misses) as f64
            } else {
                0.0
            },
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
    // History with pre-computed K projections: (event, embedding, k_projections_per_head)
    history: Vec<(Event, Vec<f32>, Vec<Vec<f32>>)>,
    // HNSW index for O(log n) nearest neighbor search
    hnsw_index: Option<HnswIndex>,
    computations: u64,
    total_events_processed: u64,
    // Performance metrics
    total_compute_time_us: u64,
    max_compute_time_us: u64,
    total_ops: u64,
}

/// HNSW index wrapper for approximate nearest neighbor search
struct HnswIndex {
    hnsw: Hnsw<'static, f32, DistL2>,
    /// Threshold for using HNSW (only use when history > threshold)
    min_size: usize,
    /// Number of neighbors to retrieve
    ef_search: usize,
    /// Max neighbors per layer
    max_nb_connection: usize,
}

impl HnswIndex {
    fn new(_dim: usize, max_elements: usize) -> Self {
        let max_nb_connection = 16;
        let ef_construction = 200;
        let hnsw = Hnsw::new(
            max_nb_connection,
            max_elements,
            16, // max_layer
            ef_construction,
            DistL2,
        );
        Self {
            hnsw,
            min_size: 100, // Only use HNSW when history > 100
            ef_search: 30, // Retrieve top 30 neighbors (optimized from 50)
            max_nb_connection,
        }
    }

    fn insert(&mut self, embedding: &[f32], id: usize) {
        self.hnsw.insert((&embedding.to_vec(), id));
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<(usize, f32)> {
        let neighbors = self.hnsw.search(query, k, self.ef_search);
        neighbors
            .into_iter()
            .map(|n| (n.d_id, n.distance))
            .collect()
    }

    fn rebuild(&mut self, embeddings: &[(usize, Vec<f32>)]) {
        // Create new HNSW with current data
        let max_elements = embeddings.len().max(1000);
        self.hnsw = Hnsw::new(self.max_nb_connection, max_elements, 16, 200, DistL2);
        for (id, emb) in embeddings {
            self.hnsw.insert((&emb.clone(), *id));
        }
    }
}

impl AttentionEngine {
    pub fn new(config: AttentionConfig) -> Self {
        let embedding_engine = EmbeddingEngine::new(
            config.embedding_config.clone(),
            config.embedding_dim,
            config.num_heads,
        );
        let cache = EmbeddingCache::new(config.cache_config.clone());
        // Initialize HNSW index
        let hnsw_index = Some(HnswIndex::new(config.embedding_dim, config.max_history));
        Self {
            config,
            embedding_engine,
            cache,
            history: Vec::new(),
            hnsw_index,
            computations: 0,
            total_events_processed: 0,
            total_compute_time_us: 0,
            max_compute_time_us: 0,
            total_ops: 0,
        }
    }

    /// Create engine without HNSW (for comparison benchmarks)
    pub fn new_without_hnsw(config: AttentionConfig) -> Self {
        let embedding_engine = EmbeddingEngine::new(
            config.embedding_config.clone(),
            config.embedding_dim,
            config.num_heads,
        );
        let cache = EmbeddingCache::new(config.cache_config.clone());
        Self {
            config,
            embedding_engine,
            cache,
            history: Vec::new(),
            hnsw_index: None,
            computations: 0,
            total_events_processed: 0,
            total_compute_time_us: 0,
            max_compute_time_us: 0,
            total_ops: 0,
        }
    }

    pub fn add_event(&mut self, event: Event) {
        let event_hash = self.hash_event(&event);
        let embedding = self.cache.get(event_hash).unwrap_or_else(|| {
            let emb = self.embedding_engine.embed(&event);
            self.cache.insert(event_hash, emb.clone());
            emb
        });

        // Pre-compute K projections for all heads (Stage 2b optimization)
        let k_projections: Vec<Vec<f32>> = (0..self.config.num_heads)
            .map(|head| {
                self.embedding_engine
                    .project(&embedding, head, ProjectionType::Key)
            })
            .collect();

        // Add to HNSW index
        let idx = self.history.len();
        if let Some(ref mut hnsw) = self.hnsw_index {
            hnsw.insert(&embedding, idx);
        }

        self.history.push((event, embedding.clone(), k_projections));
        self.total_events_processed += 1;

        // Handle history overflow - need to rebuild HNSW index
        if self.history.len() > self.config.max_history {
            self.history.remove(0);
            // Rebuild HNSW with new indices
            if let Some(ref mut hnsw) = self.hnsw_index {
                let embeddings: Vec<(usize, Vec<f32>)> = self
                    .history
                    .iter()
                    .enumerate()
                    .map(|(i, (_, emb, _))| (i, emb.clone()))
                    .collect();
                hnsw.rebuild(&embeddings);
            }
        }
    }

    pub fn compute_attention(&mut self, current: &Event) -> AttentionResult {
        let start = Instant::now();
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
        let scale = 1.0 / (head_dim as f32).sqrt();

        // Pre-compute Q projections ONCE (was computed k times before!)
        let q_projections: Vec<Vec<f32>> = (0..self.config.num_heads)
            .map(|head| {
                self.embedding_engine
                    .project(&current_embedding, head, ProjectionType::Query)
            })
            .collect();

        // Use HNSW for large history, otherwise linear scan
        let candidates: Vec<usize> = if let Some(ref hnsw) = self.hnsw_index {
            if self.history.len() > hnsw.min_size {
                // HNSW search: O(log n) instead of O(n)
                let k = hnsw.ef_search.min(self.history.len());
                hnsw.search(&current_embedding, k)
                    .into_iter()
                    .map(|(idx, _)| idx)
                    .filter(|&idx| idx < self.history.len())
                    .collect()
            } else {
                (0..self.history.len()).collect()
            }
        } else {
            (0..self.history.len()).collect()
        };

        let mut all_scores = Vec::with_capacity(candidates.len());
        let ops_count = candidates.len();

        for idx in candidates {
            let (hist_event, _hist_embedding, k_projections) = &self.history[idx];
            let mut total = 0.0f32;
            for head in 0..self.config.num_heads {
                // Use pre-computed Q and K projections (Stage 1a + 2b)
                let q = &q_projections[head];
                let k = &k_projections[head];
                total += dot_product_simd(q, k) * scale;
            }
            let avg = total / self.config.num_heads as f32;
            let event_id = format!(
                "{}_{}",
                hist_event.event_type,
                hist_event.timestamp.timestamp_millis()
            );
            all_scores.push((event_id, avg));
        }

        let context = self.compute_context(&all_scores);
        let filtered: Vec<_> = all_scores
            .into_iter()
            .filter(|(_, s)| *s >= self.config.threshold)
            .collect();

        // Track performance metrics
        let elapsed_us = start.elapsed().as_micros() as u64;
        self.total_compute_time_us += elapsed_us;
        self.max_compute_time_us = self.max_compute_time_us.max(elapsed_us);
        // ops = candidates * num_heads * embedding_dim (projections + dot products)
        self.total_ops += (ops_count as u64)
            * (self.config.num_heads as u64)
            * (self.config.embedding_dim as u64)
            * 3;

        AttentionResult {
            scores: filtered,
            head_weights: vec![1.0 / self.config.num_heads as f32; self.config.num_heads],
            context,
        }
    }

    fn compute_context(&self, scores: &[(String, f32)]) -> Vec<f32> {
        let mut context = vec![0.0f32; self.config.embedding_dim];
        if scores.is_empty() || self.history.is_empty() {
            return context;
        }

        let max_score = scores
            .iter()
            .map(|(_, s)| *s)
            .fold(f32::NEG_INFINITY, f32::max);
        let weights: Vec<f32> = scores.iter().map(|(_, s)| (s - max_score).exp()).collect();
        let sum: f32 = weights.iter().sum();
        if sum < 1e-8 {
            return context;
        }

        for (i, (_, emb, _)) in self.history.iter().enumerate() {
            if i >= weights.len() {
                break;
            }
            let w = weights[i] / sum;
            for (j, &e) in emb.iter().enumerate() {
                if j < context.len() {
                    context[j] += w * e;
                }
            }
        }
        context
    }

    fn dot_product(&self, a: &[f32], b: &[f32]) -> f32 {
        dot_product_simd(a, b)
    }

    /// Batch compute attention scores for multiple events in parallel
    pub fn compute_attention_batch(&mut self, events: &[Event]) -> Vec<AttentionResult> {
        let start = Instant::now();

        // Pre-compute embeddings for all input events
        let embeddings: Vec<Vec<f32>> = events
            .iter()
            .map(|e| {
                let hash = self.hash_event(e);
                self.cache.get(hash).unwrap_or_else(|| {
                    let emb = self.embedding_engine.embed(e);
                    self.cache.insert(hash, emb.clone());
                    emb
                })
            })
            .collect();

        if self.history.is_empty() {
            return events
                .iter()
                .map(|_| AttentionResult {
                    scores: Vec::new(),
                    head_weights: vec![0.0; self.config.num_heads],
                    context: vec![0.0; self.config.embedding_dim],
                })
                .collect();
        }

        let head_dim = self.config.embedding_dim / self.config.num_heads;
        let num_heads = self.config.num_heads;
        let threshold = self.config.threshold;
        let embedding_dim = self.config.embedding_dim;

        // Parallel computation of attention for each event
        let results: Vec<AttentionResult> = embeddings
            .par_iter()
            .enumerate()
            .map(|(_, current_embedding)| {
                let mut all_scores = Vec::with_capacity(self.history.len());

                for (hist_event, _hist_embedding, k_projections) in &self.history {
                    let mut total = 0.0f32;
                    for head in 0..num_heads {
                        let q = self.embedding_engine.project(
                            current_embedding,
                            head,
                            ProjectionType::Query,
                        );
                        let k = &k_projections[head];
                        total += dot_product_simd(&q, k) / (head_dim as f32).sqrt();
                    }
                    let avg = total / num_heads as f32;
                    let event_id = format!(
                        "{}_{}",
                        hist_event.event_type,
                        hist_event.timestamp.timestamp_millis()
                    );
                    all_scores.push((event_id, avg));
                }

                let history_embs: Vec<(Event, Vec<f32>)> = self
                    .history
                    .iter()
                    .map(|(e, emb, _)| (e.clone(), emb.clone()))
                    .collect();
                let context = compute_context_internal(&all_scores, &history_embs, embedding_dim);
                let filtered: Vec<_> = all_scores
                    .into_iter()
                    .filter(|(_, s)| *s >= threshold)
                    .collect();

                AttentionResult {
                    scores: filtered,
                    head_weights: vec![1.0 / num_heads as f32; num_heads],
                    context,
                }
            })
            .collect();

        // Update metrics
        let elapsed_us = start.elapsed().as_micros() as u64;
        self.total_compute_time_us += elapsed_us;
        self.max_compute_time_us = self.max_compute_time_us.max(elapsed_us);
        self.computations += events.len() as u64;
        self.total_ops += (events.len() as u64)
            * (self.history.len() as u64)
            * (self.config.num_heads as u64)
            * (self.config.embedding_dim as u64)
            * 3;

        // Add events to history with pre-computed K projections
        for (event, embedding) in events.iter().zip(embeddings.into_iter()) {
            let k_projections: Vec<Vec<f32>> = (0..self.config.num_heads)
                .map(|head| {
                    self.embedding_engine
                        .project(&embedding, head, ProjectionType::Key)
                })
                .collect();
            self.history.push((event.clone(), embedding, k_projections));
            self.total_events_processed += 1;
        }
        while self.history.len() > self.config.max_history {
            self.history.remove(0);
        }

        results
    }

    fn hash_event(&self, event: &Event) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        event.event_type.hash(&mut hasher);
        let mut keys: Vec<_> = event.data.keys().collect();
        keys.sort();
        for key in keys {
            key.hash(&mut hasher);
            if let Some(v) = event.data.get(key) {
                format!("{:?}", v).hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    pub fn attention_score(&self, e1: &Event, e2: &Event) -> f32 {
        let emb1 = self.embedding_engine.embed(e1);
        let emb2 = self.embedding_engine.embed(e2);
        let head_dim = self.config.embedding_dim / self.config.num_heads;
        let mut total = 0.0f32;
        for head in 0..self.config.num_heads {
            let q = self
                .embedding_engine
                .project(&emb1, head, ProjectionType::Query);
            let k = self
                .embedding_engine
                .project(&emb2, head, ProjectionType::Key);
            total += self.dot_product(&q, &k) / (head_dim as f32).sqrt();
        }
        total / self.config.num_heads as f32
    }

    pub fn clear_history(&mut self) {
        self.history.clear();
    }

    pub fn stats(&self) -> AttentionStats {
        let avg_compute_us = if self.computations > 0 {
            self.total_compute_time_us as f64 / self.computations as f64
        } else {
            0.0
        };
        AttentionStats {
            history_size: self.history.len(),
            max_history: self.config.max_history,
            computations: self.computations,
            total_events: self.total_events_processed,
            cache_stats: self.cache.stats(),
            // Performance metrics
            avg_compute_time_us: avg_compute_us,
            max_compute_time_us: self.max_compute_time_us,
            total_ops: self.total_ops,
            ops_per_sec: if self.total_compute_time_us > 0 {
                (self.total_ops as f64 * 1_000_000.0) / self.total_compute_time_us as f64
            } else {
                0.0
            },
        }
    }

    pub fn get_history(&self) -> Vec<&Event> {
        self.history.iter().map(|(e, _, _)| e).collect()
    }
}

#[derive(Debug, Clone)]
pub struct AttentionStats {
    pub history_size: usize,
    pub max_history: usize,
    pub computations: u64,
    pub total_events: u64,
    pub cache_stats: CacheStats,
    // Performance metrics
    pub avg_compute_time_us: f64,
    pub max_compute_time_us: u64,
    pub total_ops: u64,
    pub ops_per_sec: f64,
}

impl AttentionStats {
    /// Log performance warning if thresholds exceeded
    pub fn check_performance(&self) -> Option<String> {
        let mut warnings = Vec::new();

        if self.history_size > 10_000 {
            warnings.push(format!(
                "⚠️ History size {} exceeds 10K - O(n²) will degrade performance",
                self.history_size
            ));
        }

        if self.avg_compute_time_us > 10_000.0 {
            warnings.push(format!(
                "⚠️ Avg compute time {:.0}μs exceeds 10ms - consider ANN indexing",
                self.avg_compute_time_us
            ));
        }

        if self.max_compute_time_us > 100_000 {
            warnings.push(format!(
                "⚠️ Max compute time {}μs exceeds 100ms - P99 latency issue",
                self.max_compute_time_us
            ));
        }

        if warnings.is_empty() {
            None
        } else {
            Some(warnings.join("\n"))
        }
    }

    /// Estimate max sustainable events/sec
    pub fn estimated_throughput(&self) -> f64 {
        if self.avg_compute_time_us > 0.0 {
            1_000_000.0 / self.avg_compute_time_us
        } else {
            f64::INFINITY
        }
    }
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
        Self {
            engine: AttentionEngine::new(config),
            duration,
            start_time: None,
        }
    }

    pub fn process(&mut self, event: Event) -> AttentionResult {
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }
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

    pub fn attention_score(&self, e1: &Event, e2: &Event) -> f32 {
        self.engine.attention_score(e1, e2)
    }
    pub fn history(&self) -> Vec<&Event> {
        self.engine.get_history()
    }
    pub fn stats(&self) -> AttentionStats {
        self.engine.stats()
    }
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

// ============================================================================
// SIMD-OPTIMIZED DOT PRODUCT
// ============================================================================

/// SIMD-optimized dot product using manual loop unrolling
/// Processes 8 elements at a time for better cache utilization
#[inline]
fn dot_product_simd(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len().min(b.len());
    let chunks = len / 8;
    let remainder = len % 8;

    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;
    let mut sum4 = 0.0f32;
    let mut sum5 = 0.0f32;
    let mut sum6 = 0.0f32;
    let mut sum7 = 0.0f32;

    // Process 8 elements at a time (loop unrolling)
    for i in 0..chunks {
        let base = i * 8;
        unsafe {
            sum0 += *a.get_unchecked(base) * *b.get_unchecked(base);
            sum1 += *a.get_unchecked(base + 1) * *b.get_unchecked(base + 1);
            sum2 += *a.get_unchecked(base + 2) * *b.get_unchecked(base + 2);
            sum3 += *a.get_unchecked(base + 3) * *b.get_unchecked(base + 3);
            sum4 += *a.get_unchecked(base + 4) * *b.get_unchecked(base + 4);
            sum5 += *a.get_unchecked(base + 5) * *b.get_unchecked(base + 5);
            sum6 += *a.get_unchecked(base + 6) * *b.get_unchecked(base + 6);
            sum7 += *a.get_unchecked(base + 7) * *b.get_unchecked(base + 7);
        }
    }

    // Handle remainder
    let base = chunks * 8;
    for i in 0..remainder {
        unsafe {
            sum0 += *a.get_unchecked(base + i) * *b.get_unchecked(base + i);
        }
    }

    sum0 + sum1 + sum2 + sum3 + sum4 + sum5 + sum6 + sum7
}

/// Compute context vector from scores (standalone function for parallel use)
fn compute_context_internal(
    scores: &[(String, f32)],
    history: &[(Event, Vec<f32>)],
    embedding_dim: usize,
) -> Vec<f32> {
    let mut context = vec![0.0f32; embedding_dim];
    if scores.is_empty() || history.is_empty() {
        return context;
    }

    let max_score = scores
        .iter()
        .map(|(_, s)| *s)
        .fold(f32::NEG_INFINITY, f32::max);
    let weights: Vec<f32> = scores.iter().map(|(_, s)| (s - max_score).exp()).collect();
    let sum: f32 = weights.iter().sum();
    if sum < 1e-8 {
        return context;
    }

    for (i, (_, emb)) in history.iter().enumerate() {
        if i >= weights.len() {
            break;
        }
        let w = weights[i] / sum;
        for (j, &e) in emb.iter().enumerate() {
            if j < context.len() {
                context[j] += w * e;
            }
        }
    }
    context
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod simd_tests {
    use super::*;

    #[test]
    fn test_dot_product_simd_basic() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![1.0, 2.0, 3.0, 4.0];
        let result = dot_product_simd(&a, &b);
        assert!((result - 30.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product_simd_large() {
        let a: Vec<f32> = (0..64).map(|i| i as f32).collect();
        let b: Vec<f32> = (0..64).map(|i| i as f32).collect();
        let expected: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let result = dot_product_simd(&a, &b);
        assert!((result - expected).abs() < 1e-3);
    }

    #[test]
    fn test_dot_product_simd_uneven() {
        let a: Vec<f32> = (0..17).map(|i| i as f32).collect();
        let b: Vec<f32> = (0..17).map(|i| i as f32).collect();
        let expected: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let result = dot_product_simd(&a, &b);
        assert!((result - expected).abs() < 1e-3);
    }
}
