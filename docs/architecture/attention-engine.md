# Attention Engine

## Principle

Unlike probabilistic LLMs, attention in Varpulis is **deterministic**:
- Attention scores are computed reproducibly
- No generation, only correlation
- Embeddings can be rule-based or learned offline

## Embeddings

### Rule-based (default)

```varpulis
embedding_config:
    type: "composite"
    
    numeric_features:
        - field: "price"
          transform: "log_scale"
          weight: 0.3
        - field: "volume"
          transform: "normalize"
          weight: 0.2
        - field: "timestamp"
          transform: "cyclical"
          weight: 0.1
    
    categorical_features:
        - field: "symbol"
          method: "hash"
          dim: 32
          weight: 0.2
        - field: "market"
          method: "one_hot"
          weight: 0.2
```

### Learned embeddings (advanced mode)

```varpulis
embedding_config:
    type: "learned"
    model_path: "models/event_embeddings.safetensors"
    
    # Model trained offline on historical data
    # generates deterministic embeddings
    freeze: true  # No fine-tuning in production
    
    # Fallback if model unavailable
    fallback: "rule_based"
```

## Deterministic attention computation

```rust
fn compute_attention(
    event_current: Event,
    event_history: &[Event],
    config: &AttentionConfig
) -> Vec<(EventId, f32)> {
    let emb_current = embedding_engine.embed(event_current);
    
    event_history
        .iter()
        .map(|e_hist| {
            let emb_hist = embedding_engine.embed(e_hist);
            
            // Deterministic multi-head attention
            let scores: Vec<f32> = (0..config.num_heads)
                .map(|head| {
                    let q = project(emb_current, head, "query");
                    let k = project(emb_hist, head, "key");
                    dot_product(q, k) / sqrt(dim)
                })
                .collect();
            
            // Head aggregation
            let final_score = scores.iter().sum::<f32>() / config.num_heads;
            
            (e_hist.id, final_score)
        })
        .collect()
}
```

## Optimizations

### Embedding cache

```rust
struct EmbeddingCache {
    cache: LruCache<EventHash, Vec<f32>>,
    ttl: Duration
}
```

### ANN Indexing (Approximate Nearest Neighbors)

```rust
struct AttentionIndex {
    // HNSW index for fast similar event search
    hnsw: HnswIndex<f32>,
    // Minimum similarity threshold
    threshold: f32
}
```

## Configuration

```varpulis
attention:
    enabled: true
    compute: "cpu"      # or "gpu" if available
    precision: "float32" # or "float16"
    batch_size: 1000    # for throughput mode
    num_heads: 4        # number of attention heads
    dim: 128            # embedding dimension
```
