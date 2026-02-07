# Configuration Guide

Complete guide to configuring Varpulis for production deployments.

## Configuration Methods

Varpulis can be configured via:
1. **Command-line arguments** (highest priority)
2. **Configuration file** (YAML or TOML)
3. **Environment variables**
4. **VPL config blocks** (in-program configuration)

## Configuration File

### Loading a Configuration File

```bash
# Via command-line
varpulis --config /etc/varpulis/config.yaml run --file program.vpl

# Via environment variable
export VARPULIS_CONFIG=/etc/varpulis/config.yaml
varpulis run --file program.vpl
```

### Generating Example Configuration

```bash
# Generate YAML example
varpulis config-gen --format yaml --output config.yaml

# Generate TOML example
varpulis config-gen --format toml --output config.toml
```

---

## Configuration Reference

### YAML Format

```yaml
# Varpulis Configuration File

# Path to the VPL query file
query_file: /app/queries/rules.vpl

# Server settings
server:
  port: 9000              # WebSocket API port
  bind: "127.0.0.1"       # Bind address (use 0.0.0.0 for external access)
  metrics_enabled: true   # Enable Prometheus metrics
  metrics_port: 9090      # Metrics endpoint port
  workdir: /var/lib/varpulis  # Working directory for file operations

# Processing settings
processing:
  workers: 8              # Number of worker threads (default: CPU cores)
  partition_by: device_id # Field for event partitioning

# Simulation settings (for varpulis simulate)
simulation:
  immediate: true         # Skip timing delays
  preload: true           # Load events into memory
  verbose: false          # Show each event

# Kafka connector
kafka:
  bootstrap_servers: "kafka:9092"
  consumer_group: "varpulis-consumer"
  input_topic: "events"
  output_topic: "alerts"
  auto_commit: true
  auto_offset_reset: "latest"  # or "earliest"

# HTTP webhook input
http_webhook:
  enabled: true
  port: 8080
  bind: "0.0.0.0"
  api_key: "your-webhook-api-key"
  rate_limit: 1000        # Requests per second (0 = unlimited)
  max_batch_size: 100

# Logging settings
logging:
  level: info             # trace, debug, info, warn, error
  format: json            # text or json
  timestamps: true

# TLS settings (enables HTTPS/WSS)
tls:
  cert_file: /etc/ssl/certs/varpulis.crt
  key_file: /etc/ssl/private/varpulis.key

# Authentication
auth:
  api_key: "your-websocket-api-key"
```

### TOML Format

```toml
# Varpulis Configuration File

query_file = "/app/queries/rules.vpl"

[server]
port = 9000
bind = "127.0.0.1"
metrics_enabled = true
metrics_port = 9090
workdir = "/var/lib/varpulis"

[processing]
workers = 8
partition_by = "device_id"

[simulation]
immediate = true
preload = true
verbose = false

[kafka]
bootstrap_servers = "kafka:9092"
consumer_group = "varpulis-consumer"
input_topic = "events"
output_topic = "alerts"

[http_webhook]
enabled = true
port = 8080
bind = "0.0.0.0"
rate_limit = 1000

[logging]
level = "info"
format = "json"
timestamps = true

[tls]
cert_file = "/etc/ssl/certs/varpulis.crt"
key_file = "/etc/ssl/private/varpulis.key"

[auth]
api_key = "your-api-key"
```

---

## MQTT Setup

MQTT is configured within VPL files using the `connector` declaration syntax with `.from()` source binding.

### Connector Declaration

```vpl
connector MqttSensors = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "varpulis-engine"
)

# Bind events to the connector
stream Events = SensorReading
    .from(MqttSensors, topic: "events/#")
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `host` | required | MQTT broker hostname or IP |
| `port` | `1883` | MQTT broker port |
| `client_id` | `varpulis` | Unique client identifier |

Topics are specified per-stream in `.from()` calls, supporting `#` (multi-level) and `+` (single-level) wildcards.

### Deprecated: `config mqtt` Block

> **Deprecated**: The `config mqtt { }` block syntax is deprecated and will be removed in a future version. Use the `connector` declaration + `.from()` syntax shown above. See [Connectors](../language/connectors.md) for the migration guide.

### QoS Levels

| Level | Name | Description |
|-------|------|-------------|
| 0 | At most once | Fire and forget, may lose messages |
| 1 | At least once | Guaranteed delivery, may duplicate |
| 2 | Exactly once | Guaranteed exactly-once delivery |

**Recommendation:** Use QoS 1 for most use cases. QoS 2 has higher latency.

### Topic Wildcards

```vpl
// Single-level wildcard (+)
input_topic: "sensors/+/temperature"  // Matches sensors/1/temperature, sensors/2/temperature

// Multi-level wildcard (#)
input_topic: "sensors/#"              // Matches all topics under sensors/
input_topic: "building/floor1/#"      // All floor1 sensors
```

### Reconnection Behavior

Varpulis automatically handles MQTT reconnection:
- Initial connection retry with exponential backoff
- Automatic resubscription on reconnect
- Buffering of outgoing messages during disconnect (configurable)

---

## HTTP Webhooks

### Webhook Input Configuration

```yaml
http_webhook:
  enabled: true
  port: 8080
  bind: "0.0.0.0"
  api_key: "secret-key"
  rate_limit: 1000
  max_batch_size: 100
```

### Sending Events to Webhook

```bash
# Single event
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: secret-key" \
  -d '{"event_type": "SensorReading", "value": 42.5}'

# Batch of events
curl -X POST http://localhost:8080/events/batch \
  -H "Content-Type: application/json" \
  -H "X-API-Key: secret-key" \
  -d '[
    {"event_type": "SensorReading", "sensor_id": "S1", "value": 42.5},
    {"event_type": "SensorReading", "sensor_id": "S2", "value": 38.2}
  ]'
```

### Webhook Output (Outgoing)

Configure HTTP output in VPL using `.to()` to route events to a connector:

```vpl
connector AlertWebhook = http (
    url: "http://webhook.example.com/alerts",
    method: "POST"
)

stream Alerts from TemperatureReading
    .where(temperature > 100)
    .emit(alert_type: "HighTemp", message: "Temperature exceeded threshold")
    .to(AlertWebhook)
```

---

## Server Mode

### Basic Server Setup

```bash
varpulis server --port 9000 --bind 127.0.0.1
```

### Production Server with TLS, Auth, and Persistence

```bash
varpulis server \
    --port 9443 \
    --bind 0.0.0.0 \
    --api-key "$(cat /run/secrets/api-key)" \
    --tls-cert /etc/ssl/certs/server.crt \
    --tls-key /etc/ssl/private/server.key \
    --rate-limit 1000 \
    --workdir /var/lib/varpulis \
    --state-dir /var/lib/varpulis/state \
    --metrics \
    --metrics-port 9090
```

The `--state-dir` flag enables persistence of tenant and pipeline state across restarts. Without it, all state is in-memory only.

### Server Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/ws` | WebSocket | Yes | WebSocket API |
| `/health` | GET | No | Liveness probe |
| `/ready` | GET | No | Readiness probe |

### Health Checks (Kubernetes)

```yaml
# Kubernetes deployment excerpt
livenessProbe:
  httpGet:
    path: /health
    port: 9000
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /ready
    port: 9000
  initialDelaySeconds: 5
  periodSeconds: 5
```

### Rate Limiting

Configure rate limiting to protect against abuse:

```bash
# 100 requests/second per client
varpulis server --rate-limit 100
```

Rate limit headers in responses:
- `X-RateLimit-Limit`: Maximum requests per second
- `X-RateLimit-Remaining`: Remaining requests in window
- `X-RateLimit-Reset`: Time until limit resets

---

## Worker Threads and Partitioning

### Configuring Workers

```yaml
processing:
  workers: 8  # Number of parallel workers
```

Or via command line:
```bash
varpulis simulate -p rules.vpl -e events.evt --workers 8
```

### Partitioning Strategy

Events are partitioned by a key field to ensure:
- Events with the same key go to the same worker
- Pattern matching across related events is consistent
- State is isolated per partition

```yaml
processing:
  workers: 8
  partition_by: device_id  # Events grouped by device
```

### Choosing Partition Key

| Use Case | Partition Key |
|----------|---------------|
| IoT devices | `device_id` |
| Users | `user_id` |
| Sessions | `session_id` |
| Orders | `order_id` |
| Multi-tenant | `tenant_id` |

### Partition Cardinality

- **Low cardinality** (< workers): Some workers may be idle
- **High cardinality** (>> workers): Even distribution, good parallelism
- **Unbounded cardinality**: Watch memory usage (state per partition)

---

## TLS/WSS Configuration

### Generating Certificates

```bash
# Generate self-signed certificate (development only)
openssl req -x509 -nodes -days 365 \
    -newkey rsa:2048 \
    -keyout server.key \
    -out server.crt \
    -subj "/CN=localhost"

# For production, use Let's Encrypt or your CA
```

### Configuration

Command line:
```bash
varpulis server \
    --tls-cert /etc/ssl/certs/server.crt \
    --tls-key /etc/ssl/private/server.key
```

Config file:
```yaml
tls:
  cert_file: /etc/ssl/certs/server.crt
  key_file: /etc/ssl/private/server.key
```

Environment variables:
```bash
export VARPULIS_TLS_CERT=/etc/ssl/certs/server.crt
export VARPULIS_TLS_KEY=/etc/ssl/private/server.key
```

---

## Authentication

### API Key Authentication

Server:
```bash
varpulis server --api-key "your-secret-key"
# or
export VARPULIS_API_KEY="your-secret-key"
```

Client (WebSocket):
```javascript
const ws = new WebSocket('wss://server:9443/ws?api_key=your-secret-key');
// or use Authorization header
```

### Generating Secure Keys

```bash
# Generate a random 32-character API key
openssl rand -base64 32

# Or use a UUID
uuidgen
```

---

## Logging

### Configuration

```yaml
logging:
  level: info
  format: json
  timestamps: true
```

### Log Levels

| Level | Description |
|-------|-------------|
| `trace` | Very detailed debugging |
| `debug` | Debugging information |
| `info` | General operational info |
| `warn` | Warnings |
| `error` | Errors |

### Environment Variable Override

```bash
RUST_LOG=varpulis_runtime=debug,varpulis_cli=info varpulis run ...
```

### JSON Log Format

```json
{"timestamp":"2024-01-15T10:30:00Z","level":"INFO","message":"Event processed","event_type":"SensorReading","duration_ms":0.5}
```

---

## Prometheus Metrics

### Enabling Metrics

```bash
varpulis server --metrics --metrics-port 9090
varpulis demo --metrics --metrics-port 9090
```

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `varpulis_events_processed_total` | Counter | Total events processed |
| `varpulis_alerts_generated_total` | Counter | Total alerts generated |
| `varpulis_event_processing_duration_seconds` | Histogram | Event processing latency |
| `varpulis_active_patterns` | Gauge | Active pattern matches |
| `varpulis_window_events` | Gauge | Events in windows |

### Prometheus Scrape Config

```yaml
scrape_configs:
  - job_name: 'varpulis'
    static_configs:
      - targets: ['varpulis:9090']
    scrape_interval: 15s
```

---

## Docker Deployment

### Dockerfile

```dockerfile
FROM rust:1.75 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/varpulis /usr/local/bin/
COPY config.yaml /etc/varpulis/
COPY rules.vpl /app/
EXPOSE 9000 9090
CMD ["varpulis", "--config", "/etc/varpulis/config.yaml", "server"]
```

### Docker Compose

```yaml
version: '3.8'
services:
  varpulis:
    image: varpulis:latest
    ports:
      - "9000:9000"
      - "9090:9090"
    volumes:
      - ./config.yaml:/etc/varpulis/config.yaml:ro
      - ./rules.vpl:/app/rules.vpl:ro
    environment:
      - VARPULIS_API_KEY=${API_KEY}
      - RUST_LOG=info
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

---

## Environment Variables Summary

| Variable | Used By | Description |
|----------|---------|-------------|
| `VARPULIS_CONFIG` | Global | Path to config file |
| `VARPULIS_API_KEY` | Server | API authentication key |
| `VARPULIS_TLS_CERT` | Server | TLS certificate path |
| `VARPULIS_TLS_KEY` | Server | TLS private key path |
| `VARPULIS_RATE_LIMIT` | Server | Rate limit (requests/sec) |
| `RUST_LOG` | All | Logging level filter |

---

## Pipeline Management Commands

When running in server mode, use these CLI commands to manage pipelines remotely:

```bash
# Deploy a pipeline to a remote server
varpulis deploy --server http://localhost:9000 --api-key "key" \
    --file rules.vpl --name "my-pipeline"

# List deployed pipelines
varpulis pipelines --server http://localhost:9000 --api-key "key"

# Remove a pipeline
varpulis undeploy --server http://localhost:9000 --api-key "key" --id <pipeline-id>

# Check usage statistics
varpulis status --server http://localhost:9000 --api-key "key"
```

---

## See Also

- [CLI Reference](../reference/cli-reference.md) - All command options
- [Connectors](../language/connectors.md) - Connector syntax and configuration
- [State Management](../architecture/state-management.md) - Persistence and checkpointing
- [Troubleshooting](troubleshooting.md) - Common issues and solutions
- [Performance Tuning](performance-tuning.md) - Optimization guide
