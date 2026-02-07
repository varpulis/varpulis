# Observability

## Prometheus Metrics

Varpulis exposes Prometheus metrics when started with `--metrics`:

```bash
varpulis server --metrics --metrics-port 9090
varpulis demo --metrics --metrics-port 9090
```

### Available Metrics

```prometheus
# Throughput
varpulis_events_processed_total
varpulis_alerts_generated_total

# Latency
varpulis_event_processing_duration_seconds

# State
varpulis_active_patterns
varpulis_window_events
```

### Prometheus Scrape Configuration

```yaml
scrape_configs:
  - job_name: 'varpulis'
    static_configs:
      - targets: ['varpulis:9090']
    scrape_interval: 15s
```

## In-Program Logging

Use `.log()` and `.print()` within VPL for debugging and operational logging:

```varpulis
stream Debug = SensorReading
    .where(value > 100)
    .log(level: "warn", message: "High value detected")
    .print("sensor={sensor_id} value={value}")
    .emit(
        alert_type: "HighValue",
        sensor_id: sensor_id,
        value: value
    )
```

## Runtime Log Configuration

Control logging levels via environment variable:

```bash
# General logging
RUST_LOG=info varpulis run --file rules.vpl

# Module-specific
RUST_LOG=varpulis_runtime::engine=debug,varpulis_cli=info varpulis run --file rules.vpl

# Full trace
RUST_LOG=trace varpulis run --file rules.vpl
```

### Structured JSON Logs

```json
{"timestamp":"2026-01-15T10:30:00Z","level":"INFO","message":"Event processed","event_type":"SensorReading","duration_ms":0.5}
```

## Recommended Dashboard

### Grafana Panels

1. **Throughput**: `rate(varpulis_events_processed_total[1m])`
2. **P99 Latency**: `histogram_quantile(0.99, varpulis_event_processing_duration_seconds_bucket)`
3. **Alert rate**: `rate(varpulis_alerts_generated_total[1m])`
4. **Active patterns**: `varpulis_active_patterns`

### Alerting Rules

```yaml
groups:
  - name: varpulis
    rules:
      - alert: HighLatency
        expr: histogram_quantile(0.99, varpulis_event_processing_duration_seconds_bucket) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Varpulis p99 latency above 100ms"

      - alert: LowThroughput
        expr: rate(varpulis_events_processed_total[5m]) < 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Varpulis throughput below 1000 events/sec"
```
