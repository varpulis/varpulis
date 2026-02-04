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

## Metrics Configuration

```varpulis
observability:
    metrics:
        enabled: true
        endpoint: "0.0.0.0:9090"
        interval: 1s
        format: "prometheus"  # ou "openmetrics"
```

## Distributed Tracing

### Automatic Spans

Spans are automatically created for:
- Event ingestion
- Embedding calculation
- Pattern matching
- Attention computation
- Aggregation
- Output routing via `.to()`

### OpenTelemetry Configuration

```varpulis
observability:
    tracing:
        enabled: true
        exporter: "otlp"
        endpoint: "localhost:4317"
        service_name: "varpulis-engine"
        sample_rate: 0.01  # 1% sampling
        
        # Context propagation
        propagation: ["tracecontext", "baggage"]
```

## Logging

### Configuration

```varpulis
observability:
    logging:
        level: "info"  # trace, debug, info, warn, error
        format: "json" # or "text"
        output: "stdout"  # or file
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

## Instrumentation in VarpulisQL Code

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

## Recommended Dashboard

### Suggested Grafana Panels

1. **Throughput**: Events/sec per stream
2. **Latency heatmap**: Latency distribution
3. **Error rate**: Error rate per stream
4. **Queue depth**: Queue depth (backpressure)
5. **Pattern matches**: Matches/sec per pattern
6. **Resource usage**: CPU, Memory, Disk I/O
