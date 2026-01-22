# Attention Engine

## Principe

Contrairement aux LLMs probabilistes, l'attention dans Varpulis est **déterministe** :
- Les scores d'attention sont calculés de manière reproductible
- Pas de génération, seulement de la corrélation
- Les embeddings peuvent être rule-based ou appris offline

## Embeddings

### Rule-based (par défaut)

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

### Learned embeddings (mode avancé)

```varpulis
embedding_config:
    type: "learned"
    model_path: "models/event_embeddings.safetensors"
    
    # Le modèle a été entraîné offline sur des données historiques
    # et génère des embeddings déterministes
    freeze: true  # Pas de fine-tuning en production
    
    # Fallback si modèle non disponible
    fallback: "rule_based"
```

## Calcul d'attention déterministe

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
            
            // Multi-head attention déterministe
            let scores: Vec<f32> = (0..config.num_heads)
                .map(|head| {
                    let q = project(emb_current, head, "query");
                    let k = project(emb_hist, head, "key");
                    dot_product(q, k) / sqrt(dim)
                })
                .collect();
            
            // Agrégation des heads
            let final_score = scores.iter().sum::<f32>() / config.num_heads;
            
            (e_hist.id, final_score)
        })
        .collect()
}
```

## Optimisations

### Cache des embeddings

```rust
struct EmbeddingCache {
    cache: LruCache<EventHash, Vec<f32>>,
    ttl: Duration
}
```

### Indexation ANN (Approximate Nearest Neighbors)

```rust
struct AttentionIndex {
    // Index HNSW pour recherche rapide des événements similaires
    hnsw: HnswIndex<f32>,
    // Seuil de similarité minimum
    threshold: f32
}
```

## Configuration

```varpulis
attention:
    enabled: true
    compute: "cpu"      # ou "gpu" si disponible
    precision: "float32" # ou "float16"
    batch_size: 1000    # pour mode throughput
    num_heads: 4        # nombre de heads d'attention
    dim: 128            # dimension des embeddings
```
