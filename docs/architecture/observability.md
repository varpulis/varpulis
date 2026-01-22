# Observabilité

## Métriques automatiques

Pour chaque stream, Varpulis génère automatiquement :

```prometheus
# Throughput
varpulis_stream_events_total{stream="BuildingMetrics",status="success"}
varpulis_stream_events_total{stream="BuildingMetrics",status="error"}

# Latence
varpulis_stream_latency_seconds{stream="BuildingMetrics",quantile="0.5"}
varpulis_stream_latency_seconds{stream="BuildingMetrics",quantile="0.95"}
varpulis_stream_latency_seconds{stream="BuildingMetrics",quantile="0.99"}

# Backpressure
varpulis_stream_queue_size{stream="BuildingMetrics"}
varpulis_stream_queue_utilization{stream="BuildingMetrics"}

# Pattern matching
varpulis_pattern_matches_total{pattern="FraudDetection"}
varpulis_attention_computation_seconds{stream="FraudDetection"}
```

## Configuration métriques

```varpulis
observability:
    metrics:
        enabled: true
        endpoint: "0.0.0.0:9090"
        interval: 1s
        format: "prometheus"  # ou "openmetrics"
```

## Distributed Tracing

### Spans automatiques

Les spans sont automatiquement créés pour :
- Ingestion d'événements
- Embedding calculation
- Pattern matching
- Attention computation
- Aggregation
- Emission vers sinks

### Configuration OpenTelemetry

```varpulis
observability:
    tracing:
        enabled: true
        exporter: "otlp"
        endpoint: "localhost:4317"
        service_name: "varpulis-engine"
        sample_rate: 0.01  # 1% sampling
        
        # Propagation de contexte
        propagation: ["tracecontext", "baggage"]
```

## Logging

### Configuration

```varpulis
observability:
    logging:
        level: "info"  # trace, debug, info, warn, error
        format: "json" # ou "text"
        output: "stdout"  # ou fichier
```

### Structured logging

```json
{
  "timestamp": "2026-01-23T00:15:00Z",
  "level": "info",
  "stream": "FraudDetection",
  "event_id": "evt-12345",
  "message": "Pattern matched",
  "pattern": "suspicious_activity",
  "latency_ms": 2.3
}
```

## Instrumentation dans le code VarpulisQL

```varpulis
stream ProcessedTrades = RawTrades
    .tap(log: "trades.ingestion", sample: 0.1)  # Log 10%
    .validate(schema: TradeSchema)
    .tap(metrics:
        counter: "trades.validated"
        histogram: "trade.value"
    )
    .where(price > 0)
    .tap(trace:
        name: "positive_trades"
        when: debug_mode
        span_context: true
    )
    .emit()
```

## Dashboard recommandé

### Grafana panels suggérés

1. **Throughput**: Events/sec par stream
2. **Latency heatmap**: Distribution des latences
3. **Error rate**: Taux d'erreur par stream
4. **Queue depth**: Profondeur des queues (backpressure)
5. **Pattern matches**: Matches/sec par pattern
6. **Resource usage**: CPU, Memory, Disk I/O
