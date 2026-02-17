# Varpulis CEP Operations Runbook

## 1. Common Operations

### 1.1 Starting a Single-Node Server

```bash
# Minimal: binds to 127.0.0.1:9000
varpulis server --port 9000

# Production single-node: bind to all interfaces, enable metrics, set API key
varpulis server \
  --bind 0.0.0.0 \
  --port 9000 \
  --metrics \
  --metrics-port 9090 \
  --api-key "$VARPULIS_API_KEY" \
  --state-dir /var/lib/varpulis/state
```

Environment variables accepted: `VARPULIS_API_KEY`, `VARPULIS_CONFIG`, `VARPULIS_RATE_LIMIT`,
`VARPULIS_STATE_DIR`, `RUST_LOG`.

### 1.2 Starting a Cluster (Coordinator + Workers)

**Using Docker Compose (recommended for evaluation):**

```bash
docker compose -f deploy/docker/docker-compose.cluster.yml up -d
```

This starts: 1 coordinator (port 9100), 4 workers (ports 9000-9003), MQTT broker,
Prometheus, Grafana, and Web UI.

**Manual startup order:**

```bash
# 1. Start coordinator
varpulis coordinator \
  --bind 0.0.0.0 --port 9100 \
  --api-key "$VARPULIS_API_KEY" \
  --heartbeat-interval 5 \
  --heartbeat-timeout 15

# 2. Start workers (each on its own host or port)
varpulis server \
  --bind 0.0.0.0 --port 9000 \
  --api-key "$VARPULIS_WORKER_KEY" \
  --metrics \
  --coordinator http://coordinator-host:9100 \
  --worker-id worker-0 \
  --advertise-address http://worker-0-host:9000
```

Workers automatically register with the coordinator via heartbeats.

### 1.3 Starting a Raft HA Cluster

For coordinator high availability, use Raft consensus with 3+ coordinator nodes:

```bash
varpulis coordinator \
  --bind 0.0.0.0 --port 9100 \
  --api-key "$VARPULIS_API_KEY" \
  --raft \
  --raft-node-id 1 \
  --raft-peers "http://coord-1:9100,http://coord-2:9100,http://coord-3:9100" \
  --raft-data-dir /var/lib/varpulis/raft
```

### 1.4 Scaling Workers Up/Down

**Scaling up:** Start a new worker process with `--coordinator` pointing to the
coordinator. It auto-registers on first heartbeat.

**Scaling down:** Drain the worker first, then stop it:

```bash
# Drain: migrates pipelines off the worker
curl -X POST http://localhost:9100/api/v1/cluster/workers/worker-3/drain \
  -H "x-api-key: $VARPULIS_API_KEY"

# Verify pipelines migrated
curl http://localhost:9100/api/v1/cluster/workers \
  -H "x-api-key: $VARPULIS_API_KEY" | jq '.[] | select(.id=="worker-3")'

# Stop the worker process
docker stop varpulis-worker-3
```

**Auto-scaling:** The coordinator emits scaling recommendations when
`--scaling-min-workers` is set. Configure a webhook to receive them:

```bash
varpulis coordinator \
  --scaling-min-workers 2 \
  --scaling-max-workers 10 \
  --scaling-up-threshold 5.0 \
  --scaling-down-threshold 1.0 \
  --scaling-webhook-url http://orchestrator/scale
```

### 1.5 Rolling Restart Procedure

1. Identify workers: `GET /api/v1/cluster/workers`
2. For each worker (one at a time):
   a. Drain the worker: `POST /api/v1/cluster/workers/{id}/drain`
   b. Wait for pipeline migration to complete (check worker pipeline count = 0)
   c. Stop the worker process
   d. Upgrade binary / apply config changes
   e. Restart the worker with `--coordinator` flag
   f. Verify it re-registers: `GET /api/v1/cluster/workers`
3. For coordinator upgrades in Raft mode: restart non-leader nodes first, then
   the leader (Raft will elect a new leader during the brief downtime).

### 1.6 Configuration Hot Reload

Pipelines can be updated without full restart using the reload API:

```bash
curl -X POST http://localhost:9000/api/v1/pipelines/{pipeline_id}/reload \
  -H "x-api-key: $VARPULIS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"source": "stream Alert = TempReading as t .where(t.value > 100) .emit(alert: true)"}'
```

Configuration file formats supported: YAML (`.yaml`/`.yml`) and TOML (`.toml`).
Generate an example with `varpulis config-gen --format yaml`.

---

## 2. Incident Response

### 2.1 Leader Failover (Raft)

**What happens:** When the Raft leader becomes unavailable, remaining nodes hold
an election. A new leader is elected within the heartbeat timeout (default 15s).
All writes are blocked during the election; reads from the stale store may still
succeed.

**How to verify:**

```bash
# Check which node is the current leader
curl http://coord-1:9100/api/v1/cluster/raft/status \
  -H "x-api-key: $VARPULIS_API_KEY"

# Check from each coordinator node to find the leader
for port in 9100 9101 9102; do
  echo "Node on port $port:"
  curl -s http://localhost:$port/health | jq .
done
```

**Manual intervention:** If the cluster cannot elect a leader (majority lost):
- Restart failed coordinator nodes with `--raft-data-dir` pointing to persistent storage
- If data is lost, bootstrap a new cluster from the most recent Raft snapshot

### 2.2 Worker Loss

**Automatic behavior:** The coordinator detects worker loss via missed heartbeats
(default timeout: 15s). Pipelines on the lost worker are automatically reassigned
to healthy workers using the placement strategy (least-loaded or round-robin).

**Manual migration (if needed):**

```bash
# Check pipeline status
curl http://localhost:9100/api/v1/cluster/pipeline-groups \
  -H "x-api-key: $VARPULIS_API_KEY" | jq .

# Force reassignment by draining and removing the stale worker
curl -X DELETE http://localhost:9100/api/v1/cluster/workers/worker-2 \
  -H "x-api-key: $VARPULIS_API_KEY"
```

### 2.3 Connector Failure (Circuit Breaker + DLQ)

Sink connectors (Kafka, MQTT, HTTP, etc.) use a circuit breaker pattern:

- **Closed** (normal): events delivered to sink
- **Open** (failing): after 5 consecutive failures (default `failure_threshold`),
  events are rejected and routed to the Dead Letter Queue (DLQ)
- **HalfOpen** (probing): after `reset_timeout` (default 30s), one event is sent
  as a probe. Success closes the circuit; failure re-opens it.

**Inspect DLQ:**

```bash
# DLQ is a JSON-lines file at the configured state directory
cat /var/lib/varpulis/state/dlq.jsonl | jq .

# Count dead-lettered events
wc -l /var/lib/varpulis/state/dlq.jsonl

# Each line contains: timestamp, connector name, error message, original event
cat /var/lib/varpulis/state/dlq.jsonl | jq '{time: .timestamp, sink: .connector, err: .error}'
```

**Recovery:** Fix the downstream sink, then reprocess DLQ events by injecting them
back via the batch API.

### 2.4 Pipeline Stuck

If a pipeline stops producing output:

1. Check pipeline status via API
2. Checkpoint the current state
3. Inspect the checkpoint for stuck SASE runs
4. Restore from a known-good checkpoint if needed

```bash
# Check pipeline metrics
curl http://localhost:9000/api/v1/pipelines/{id}/metrics \
  -H "x-api-key: $VARPULIS_API_KEY"

# Force checkpoint
curl -X POST http://localhost:9000/api/v1/pipelines/{id}/checkpoint \
  -H "x-api-key: $VARPULIS_API_KEY"

# If stuck, restore from checkpoint
curl -X POST http://localhost:9000/api/v1/pipelines/{id}/restore \
  -H "x-api-key: $VARPULIS_API_KEY" \
  -H "Content-Type: application/json" \
  -d @checkpoint.json
```

---

## 3. Troubleshooting

### 3.1 High Latency

**Diagnostic steps:**

1. Check processing latency histogram:
   `varpulis_processing_latency_seconds` (Prometheus)
2. Identify which stream is slow:
   `varpulis_processing_latency_seconds{stream="AlertStream"}`
3. Common causes:
   - **SASE run explosion**: Kleene patterns (`+`) with broad predicates create
     exponential runs. Add tighter `.where()` conditions or reduce `.within()` windows.
   - **Connector backpressure**: Sink is slow. Check circuit breaker state and
     downstream service latency.
   - **Aggregation overhead**: Large trend aggregation windows. Consider Hamlet
     sharing for overlapping queries.

**Quick check:**

```bash
# Get per-pipeline metrics
curl http://localhost:9000/api/v1/pipelines/{id}/metrics \
  -H "x-api-key: $VARPULIS_API_KEY"

# Check Prometheus metrics directly
curl http://localhost:9090/metrics | grep varpulis_processing_latency
```

### 3.2 Memory Growth

**Common causes and fixes:**

| Cause | Symptom | Fix |
|-------|---------|-----|
| Active SASE runs | RSS grows over time | Reduce `.within()` window size |
| Kleene patterns | Memory spikes on bursty input | Add `.where()` filters early |
| Preloaded events | High initial RSS (~40 MB/100K events) | Expected for `--preload` mode |
| Large windows | Aggregation state accumulates | Tune window parameters |

**Diagnostic:**

```bash
# Check RSS
ps -o rss,vsz,comm -p $(pgrep varpulis) | awk '{print $1/1024 " MB RSS"}'

# Check active stream count (Prometheus)
curl http://localhost:9090/metrics | grep varpulis_active_streams
```

### 3.3 Stuck Pipelines

```bash
# List all pipelines and their status
curl http://localhost:9000/api/v1/pipelines \
  -H "x-api-key: $VARPULIS_API_KEY" | jq '.pipelines[] | {id, name, status}'

# Stream live logs (SSE)
curl -N http://localhost:9000/api/v1/pipelines/{id}/logs \
  -H "x-api-key: $VARPULIS_API_KEY"

# Force restart: delete and redeploy
curl -X DELETE http://localhost:9000/api/v1/pipelines/{id} \
  -H "x-api-key: $VARPULIS_API_KEY"

curl -X POST http://localhost:9000/api/v1/pipelines \
  -H "x-api-key: $VARPULIS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-pipeline", "source": "..."}'
```

---

## 4. Recovery Procedures

### 4.1 Checkpoint and Restore

```bash
# Create checkpoint (captures engine state, SASE runs, counters)
curl -X POST http://localhost:9000/api/v1/pipelines/{id}/checkpoint \
  -H "x-api-key: $VARPULIS_API_KEY" -o checkpoint.json

# Restore from checkpoint (after restart or migration)
curl -X POST http://localhost:9000/api/v1/pipelines/{id}/restore \
  -H "x-api-key: $VARPULIS_API_KEY" \
  -H "Content-Type: application/json" \
  -d @checkpoint.json
```

Auto-checkpointing is available for simulations:

```bash
varpulis simulate --program rules.vpl --events data.evt \
  --checkpoint-dir /var/lib/varpulis/checkpoints \
  --checkpoint-interval 60
```

### 4.2 Raft Recovery (Bootstrap from Snapshot)

If a majority of coordinator nodes are lost:

1. Stop all remaining coordinator processes
2. Identify the node with the most recent data in `--raft-data-dir`
3. Bootstrap a new single-node cluster from that data
4. Add new nodes one at a time to rebuild quorum

### 4.3 Data Migration Between Workers

Use the drain and deploy pattern:

```bash
# 1. Drain source worker
curl -X POST http://localhost:9100/api/v1/cluster/workers/worker-old/drain \
  -H "x-api-key: $VARPULIS_API_KEY"

# 2. Coordinator automatically reassigns pipelines to available workers

# 3. Verify new placement
curl http://localhost:9100/api/v1/cluster/pipeline-groups \
  -H "x-api-key: $VARPULIS_API_KEY" | jq '.[] | {name, workers: [.deployments[].worker_id]}'
```

---

## 5. Log Analysis

### 5.1 Key Log Patterns

Configure log level with `RUST_LOG` environment variable:

```bash
# Minimal production logging
RUST_LOG=info varpulis server ...

# Debug connector issues
RUST_LOG=info,varpulis_runtime::connector=debug varpulis server ...

# Trace SASE engine internals
RUST_LOG=info,varpulis_runtime::engine=trace varpulis server ...
```

**Patterns to watch for:**

| Log message | Severity | Action |
|-------------|----------|--------|
| `Heartbeat failed` | ERROR | Worker cannot reach coordinator. Check network. |
| `Circuit breaker opened` | WARN | Sink connector failing. Check downstream service. |
| `DLQ write` | WARN | Events being dead-lettered. Inspect DLQ file. |
| `Pipeline deployed` | INFO | Normal: pipeline started successfully. |
| `Worker registered` | INFO | Normal: worker joined the cluster. |
| `Raft leadership changed` | INFO | Leader election occurred. Verify cluster health. |
| `Migration started` | INFO | Pipeline moving between workers. Monitor completion. |

### 5.2 Structured Logging

Use JSON log format for production (compatible with log aggregators):

```yaml
# In config YAML
logging:
  level: info
  format: json
  timestamps: true
```

### 5.3 SSE Log Streaming

Stream live pipeline output events via Server-Sent Events:

```bash
# CLI
varpulis logs --server http://localhost:9000 --api-key "$KEY" --pipeline-id "$ID"

# Direct curl
curl -N http://localhost:9000/api/v1/pipelines/{id}/logs \
  -H "x-api-key: $VARPULIS_API_KEY"
```

### 5.4 Distributed Tracing (OpenTelemetry)

The runtime uses `tracing` spans throughout. To export to an OpenTelemetry
collector, configure the standard OTEL environment variables:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317 \
OTEL_SERVICE_NAME=varpulis-worker-0 \
varpulis server ...
```

---

## 6. Metrics Interpretation

### 6.1 Key Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `varpulis_events_total` | Counter | Total events received, by event type |
| `varpulis_events_processed` | Counter | Events processed, by stream name |
| `varpulis_output_events_total` | Counter | Output events emitted, by stream and type |
| `varpulis_processing_latency_seconds` | Histogram | Per-event processing latency, by stream |
| `varpulis_stream_queue_size` | Gauge | Current queue depth per stream |
| `varpulis_active_streams` | Gauge | Number of active stream definitions |

Histogram buckets for latency: 100us, 500us, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s.

### 6.2 Grafana Dashboards

Pre-built dashboards are provisioned automatically with Docker Compose:

- **Varpulis Overview** (`varpulis.json`): Single-node throughput, latency, active streams
- **Varpulis Cluster** (`varpulis-cluster.json`): Per-worker metrics, pipeline distribution

Access Grafana at `http://localhost:3000` (default credentials: admin / varpulis).

Prometheus scrapes workers on port 9090 (`/metrics`) and coordinator on port 9100
(`/metrics`), every 5 seconds.

### 6.3 Alert Response

Key thresholds to alert on:

| Condition | Query | Response |
|-----------|-------|----------|
| High latency (p99 > 100ms) | `histogram_quantile(0.99, rate(varpulis_processing_latency_seconds_bucket[5m])) > 0.1` | Check SASE run count, connector health |
| Event backlog growing | `rate(varpulis_events_total[5m]) > rate(varpulis_events_processed[5m])` | Scale workers or optimize queries |
| Worker down | Absent `up{job=~"varpulis-worker.*"}` | Check worker process, coordinator heartbeats |
| No output events | `rate(varpulis_output_events_total[10m]) == 0` | Verify input flow, check pipeline status |

---

## 7. API Quick Reference

All API calls require the `x-api-key` header. Base URL: `http://localhost:9000`
for single-node, `http://localhost:9100` for cluster coordinator.

### 7.1 Health Check

```bash
curl http://localhost:9000/health
```

### 7.2 Pipeline Management (Single-Node)

```bash
# Deploy pipeline
curl -X POST http://localhost:9000/api/v1/pipelines \
  -H "x-api-key: $KEY" -H "Content-Type: application/json" \
  -d '{"name": "fraud-detect", "source": "stream Alert = Login as a -> Purchase as b .within(5m) .where(a.user == b.user) .emit(user: a.user)"}'

# List pipelines
curl http://localhost:9000/api/v1/pipelines -H "x-api-key: $KEY"

# Get pipeline details
curl http://localhost:9000/api/v1/pipelines/{id} -H "x-api-key: $KEY"

# Delete pipeline
curl -X DELETE http://localhost:9000/api/v1/pipelines/{id} -H "x-api-key: $KEY"

# Reload pipeline source (hot update)
curl -X POST http://localhost:9000/api/v1/pipelines/{id}/reload \
  -H "x-api-key: $KEY" -H "Content-Type: application/json" \
  -d '{"source": "stream Alert = ..."}'

# Get pipeline metrics
curl http://localhost:9000/api/v1/pipelines/{id}/metrics -H "x-api-key: $KEY"
```

### 7.3 Event Injection

```bash
# Single event
curl -X POST http://localhost:9000/api/v1/pipelines/{id}/events \
  -H "x-api-key: $KEY" -H "Content-Type: application/json" \
  -d '{"event_type": "TempReading", "fields": {"sensor": "S1", "value": 95.5}}'

# Batch injection
curl -X POST http://localhost:9000/api/v1/pipelines/{id}/events-batch \
  -H "x-api-key: $KEY" -H "Content-Type: application/json" \
  -d '{"events": [{"event_type": "TempReading", "fields": {"value": 95.5}}, {"event_type": "TempReading", "fields": {"value": 101.2}}]}'
```

### 7.4 Checkpoint and Restore

```bash
# Checkpoint
curl -X POST http://localhost:9000/api/v1/pipelines/{id}/checkpoint \
  -H "x-api-key: $KEY" -o checkpoint.json

# Restore
curl -X POST http://localhost:9000/api/v1/pipelines/{id}/restore \
  -H "x-api-key: $KEY" -H "Content-Type: application/json" -d @checkpoint.json
```

### 7.5 Cluster Operations (Coordinator)

```bash
# List workers
curl http://localhost:9100/api/v1/cluster/workers -H "x-api-key: $KEY"

# Get worker details
curl http://localhost:9100/api/v1/cluster/workers/{id} -H "x-api-key: $KEY"

# Drain worker
curl -X POST http://localhost:9100/api/v1/cluster/workers/{id}/drain -H "x-api-key: $KEY"

# Remove worker
curl -X DELETE http://localhost:9100/api/v1/cluster/workers/{id} -H "x-api-key: $KEY"

# Deploy pipeline group
curl -X POST http://localhost:9100/api/v1/cluster/pipeline-groups \
  -H "x-api-key: $KEY" -H "Content-Type: application/json" \
  -d '{"name": "my-group", "pipelines": [...], "routes": [...]}'

# List pipeline groups
curl http://localhost:9100/api/v1/cluster/pipeline-groups -H "x-api-key: $KEY"

# Teardown pipeline group
curl -X DELETE http://localhost:9100/api/v1/cluster/pipeline-groups/{id} -H "x-api-key: $KEY"

# Cluster metrics
curl http://localhost:9100/api/v1/cluster/metrics -H "x-api-key: $KEY"

# Scaling recommendations
curl http://localhost:9100/api/v1/cluster/scaling -H "x-api-key: $KEY"

# Validate VPL source
curl -X POST http://localhost:9100/api/v1/cluster/validate \
  -H "x-api-key: $KEY" -H "Content-Type: application/json" \
  -d '{"source": "stream Test = A as a .where(a.x > 1) .emit(v: a.x)"}'
```

### 7.6 Usage and Tenant Management

```bash
# Usage stats
curl http://localhost:9000/api/v1/usage -H "x-api-key: $KEY"

# SSE log stream
curl -N http://localhost:9000/api/v1/pipelines/{id}/logs -H "x-api-key: $KEY"
```
