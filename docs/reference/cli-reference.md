# Varpulis CLI Reference

Complete reference for all Varpulis command-line interface commands and options.

## Installation

```bash
# From source
cargo install --path crates/varpulis-cli

# Or build the workspace
cargo build --release
# Binary located at target/release/varpulis
```

## Global Options

| Option | Environment Variable | Description |
|--------|---------------------|-------------|
| `-c, --config <PATH>` | `VARPULIS_CONFIG` | Path to configuration file (YAML or TOML) |
| `-h, --help` | - | Print help information |
| `-V, --version` | - | Print version information |

---

## Commands

### `varpulis run`

Execute a VPL program with optional MQTT connectivity.

```bash
varpulis run --file program.vpl
varpulis run --code 'stream Readings = SensorReading'
```

**Options:**

| Option | Description |
|--------|-------------|
| `-f, --file <PATH>` | Path to the .vpl file to execute |
| `-c, --code <STRING>` | Inline VPL code to execute |

**Notes:**
- Either `--file` or `--code` must be provided
- If the program contains connector declarations with `.from()`, Varpulis will connect to the specified brokers
- Press Ctrl+C to stop execution

> **Note**: The `config mqtt { }` block syntax is deprecated. Use `connector` declarations with `.from()` instead. See [Connectors](../language/connectors.md).

**Example with MQTT:**
```vpl
connector MqttBroker = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "sensor-monitor"
)

event SensorReading:
    temperature: float

stream Readings = SensorReading
    .from(MqttBroker, topic: "sensors/#")
    .where(temperature > 100)
    .emit(alert_type: "HighTemp", temperature: temperature)
```

---

### `varpulis parse`

Parse a VPL file and display the Abstract Syntax Tree (AST).

```bash
varpulis parse program.vpl
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `<FILE>` | Path to the .vpl file to parse |

**Output:**
- On success: Prints the AST in Rust debug format
- On error: Displays parse error with location

---

### `varpulis check`

Validate the syntax of a VPL file without executing it.

```bash
varpulis check program.vpl
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `<FILE>` | Path to the .vpl file to check |

**Output:**
```
Syntax OK
   Statements: 12
```

**Error Output:**
```
Syntax error: unexpected token 'xyz' at line 5, column 10
   Hint: Did you mean 'where'?
   |
   | stream Readings xyz temperature > 100
   |                 ^
```

---

### `varpulis demo`

Run the built-in HVAC building monitoring demo.

```bash
varpulis demo --duration 120 --anomalies --metrics
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `-d, --duration <SECS>` | `60` | Duration to run the demo in seconds |
| `--anomalies` | disabled | Enable anomaly simulation |
| `--degradation` | disabled | Enable gradual degradation simulation |
| `--metrics` | disabled | Enable Prometheus metrics endpoint |
| `--metrics-port <PORT>` | `9090` | Port for Prometheus metrics |

**Output:**
- Real-time event processing statistics
- Alert generation when thresholds are exceeded
- Final summary with event count and throughput

---

### `varpulis server`

Start the Varpulis WebSocket API server for IDE integration and remote control.

```bash
varpulis server --port 9000 --api-key "secret" --metrics
```

**Options:**

| Option | Default | Environment Variable | Description |
|--------|---------|---------------------|-------------|
| `-p, --port <PORT>` | `9000` | - | WebSocket server port |
| `--bind <ADDR>` | `127.0.0.1` | - | Bind address (use 0.0.0.0 for external access) |
| `--api-key <KEY>` | none | `VARPULIS_API_KEY` | API key for authentication (disables auth if not set) |
| `--tls-cert <PATH>` | none | `VARPULIS_TLS_CERT` | TLS certificate file (PEM format) |
| `--tls-key <PATH>` | none | `VARPULIS_TLS_KEY` | TLS private key file (PEM format) |
| `--rate-limit <RPS>` | `0` | `VARPULIS_RATE_LIMIT` | Rate limit in requests/second per client (0 = disabled) |
| `--workdir <PATH>` | current dir | - | Working directory for file operations |
| `--metrics` | disabled | - | Enable Prometheus metrics endpoint |
| `--metrics-port <PORT>` | `9090` | - | Port for Prometheus metrics |

**Endpoints:**

| Endpoint | Description |
|----------|-------------|
| `ws://host:port/ws` | WebSocket API (or `wss://` with TLS) |
| `GET /health` | Liveness probe (always returns healthy) |
| `GET /ready` | Readiness probe (returns ready when engine is loaded) |

**REST API (Multi-tenant Pipeline Management):**

Authentication: `X-API-Key` header. When `--api-key` is set, a default tenant is auto-provisioned.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/pipelines` | Deploy a pipeline (`{"name": "...", "source": "..."}`) |
| `GET` | `/api/v1/pipelines` | List all pipelines for the tenant |
| `GET` | `/api/v1/pipelines/:id` | Get pipeline details |
| `DELETE` | `/api/v1/pipelines/:id` | Delete a pipeline |
| `POST` | `/api/v1/pipelines/:id/events` | Inject events (`{"event_type": "...", "fields": {...}}`) |
| `GET` | `/api/v1/pipelines/:id/metrics` | Pipeline metrics (events processed, alerts) |
| `POST` | `/api/v1/pipelines/:id/reload` | Hot reload with new source (`{"source": "..."}`) |
| `GET` | `/api/v1/usage` | Tenant usage stats and quota |

**WebSocket API Messages:**

```json
// Load a program
{"type": "load", "source": "stream X = Y"}

// Send an event
{"type": "event", "data": {"event_type": "Sensor", "value": 42}}

// Get status
{"type": "status"}
```

**Example with TLS:**
```bash
varpulis server \
    --port 9443 \
    --bind 0.0.0.0 \
    --api-key "$(cat /secrets/api-key)" \
    --tls-cert /certs/server.crt \
    --tls-key /certs/server.key \
    --rate-limit 100
```

---

### `varpulis simulate`

Play events from an event file through a VPL program.

```bash
varpulis simulate --program rules.vpl --events data.evt --immediate --workers 8
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `-p, --program <PATH>` | required | Path to the VPL program (.vpl) |
| `-e, --events <PATH>` | required | Path to the event file (.evt) |
| `--immediate` | disabled | Run without timing delays (batch mode) |
| `-v, --verbose` | disabled | Show each event as it's processed |
| `--preload` | disabled | Load all events into memory before processing |
| `-w, --workers <N>` | CPU cores | Number of worker threads for parallel processing |
| `--partition-by <FIELD>` | auto | Field to use for partitioning events |

**Event File Format (.evt):**
```
# Comments start with #
# Format: @<delay_ms> <event_type> { field: value, ... }

@0 TemperatureReading { sensor_id: "S1", value: 72.5 }
@100 TemperatureReading { sensor_id: "S2", value: 68.2 }
@200 HumidityReading { sensor_id: "S1", humidity: 45 }
```

**Processing Modes:**

| Mode | Flags | Description |
|------|-------|-------------|
| Timed | (none) | Events played at recorded timing intervals |
| Immediate | `--immediate` | Events processed as fast as possible |
| Preload | `--immediate --preload` | Load all events to memory, then process |
| Parallel | `--immediate --workers N` | Partition and process in parallel |

**Output:**
```
Varpulis Event Simulation
============================
Program: rules.vpl
Events:  data.evt
Mode:    immediate (parallel)
Workers: 8

Starting simulation...

Simulation Complete
======================
Duration:         1.234s
Events processed: 1000000
Workers used:     8
Alerts generated: 42
Event rate:       810,373.2 events/sec
```

---

### `varpulis config-gen`

Generate an example configuration file.

```bash
varpulis config-gen --format yaml --output config.yaml
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `-f, --format <FORMAT>` | `yaml` | Output format: `yaml` or `toml` |
| `-o, --output <PATH>` | stdout | Output file path |

**Example YAML output:**
```yaml
# Varpulis Configuration File
processing:
  workers: 4
  partition_by: "device_id"
  batch_size: 1000

mqtt:
  broker: "localhost"
  port: 1883
  input_topic: "events/#"
  output_topic: "alerts"
  qos: 1

metrics:
  enabled: true
  port: 9090
```

---

## Environment Variables

| Variable | Used By | Description |
|----------|---------|-------------|
| `VARPULIS_CONFIG` | Global | Path to default configuration file |
| `VARPULIS_API_KEY` | `server` | API key for WebSocket authentication |
| `VARPULIS_TLS_CERT` | `server` | Path to TLS certificate |
| `VARPULIS_TLS_KEY` | `server` | Path to TLS private key |
| `VARPULIS_RATE_LIMIT` | `server` | Default rate limit (requests/second) |
| `RUST_LOG` | All | Logging level (e.g., `info`, `debug`, `trace`) |

---

## Configuration File

Varpulis supports configuration files in YAML or TOML format. Use the global `--config` option or `VARPULIS_CONFIG` environment variable.

### YAML Example

```yaml
processing:
  workers: 8
  partition_by: "device_id"
  batch_size: 5000

mqtt:
  broker: "mqtt.example.com"
  port: 8883
  input_topic: "sensors/#"
  output_topic: "varpulis/alerts"
  client_id: "varpulis-prod"
  qos: 1
  tls: true

server:
  port: 9000
  bind: "0.0.0.0"
  api_key_file: "/secrets/api-key"

metrics:
  enabled: true
  port: 9090
```

### TOML Example

```toml
[processing]
workers = 8
partition_by = "device_id"
batch_size = 5000

[mqtt]
broker = "mqtt.example.com"
port = 8883
input_topic = "sensors/#"
output_topic = "varpulis/alerts"

[server]
port = 9000
bind = "0.0.0.0"

[metrics]
enabled = true
port = 9090
```

---

## Exit Codes

| Code | Description |
|------|-------------|
| `0` | Success |
| `1` | Syntax error or validation failure |
| `2` | File not found or I/O error |
| `3` | Configuration error |
| `4` | Runtime error (MQTT connection failed, etc.) |

---

## Examples

### Basic Validation Workflow

```bash
# Check syntax
varpulis check program.vpl

# View AST for debugging
varpulis parse program.vpl

# Simulate with sample events
varpulis simulate -p program.vpl -e test.evt --verbose
```

### Production Deployment

```bash
# Generate config template
varpulis config-gen -f yaml -o /etc/varpulis/config.yaml

# Run with MQTT in production
VARPULIS_CONFIG=/etc/varpulis/config.yaml \
RUST_LOG=info \
varpulis run --file /etc/varpulis/rules.vpl
```

### High-Performance Batch Processing

```bash
# Process 10M events with 16 workers
varpulis simulate \
    --program analytics.vpl \
    --events large_dataset.evt \
    --immediate \
    --preload \
    --workers 16 \
    --partition-by device_id
```

### Secure Server Deployment

```bash
# Production server with TLS and authentication
varpulis server \
    --port 9443 \
    --bind 0.0.0.0 \
    --api-key "$(cat /run/secrets/api-key)" \
    --tls-cert /etc/ssl/varpulis.crt \
    --tls-key /etc/ssl/varpulis.key \
    --rate-limit 1000 \
    --workdir /var/lib/varpulis \
    --metrics \
    --metrics-port 9090
```

---

### `varpulis coordinator`

Start the cluster coordinator (control plane for distributed execution).

```bash
varpulis coordinator --port 9100 --api-key admin
```

**Options:**

| Option | Default | Environment Variable | Description |
|--------|---------|---------------------|-------------|
| `-p, --port <PORT>` | `9100` | - | Coordinator port |
| `--bind <ADDR>` | `127.0.0.1` | - | Bind address |
| `--api-key <KEY>` | none | `VARPULIS_API_KEY` | API key for authentication |

**Endpoints:**

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Coordinator health check |
| `POST /api/v1/cluster/workers/register` | Worker registration |
| `POST /api/v1/cluster/workers/{id}/heartbeat` | Worker heartbeat |
| `GET /api/v1/cluster/workers` | List workers |
| `POST /api/v1/cluster/pipeline-groups` | Deploy pipeline group |
| `GET /api/v1/cluster/pipeline-groups` | List pipeline groups |
| `POST /api/v1/cluster/pipeline-groups/{id}/inject` | Inject event |
| `DELETE /api/v1/cluster/pipeline-groups/{id}` | Tear down group |
| `GET /api/v1/cluster/topology` | Full routing topology |

See [Cluster Architecture](../architecture/cluster.md) for full API documentation.

---

### `varpulis server` (Cluster Mode)

When `--coordinator` is provided, the server registers as a cluster worker:

```bash
varpulis server --port 9000 --api-key test \
    --coordinator http://localhost:9100 \
    --worker-id worker-0
```

**Additional Cluster Options:**

| Option | Default | Environment Variable | Description |
|--------|---------|---------------------|-------------|
| `--coordinator <URL>` | none | `VARPULIS_COORDINATOR` | Coordinator URL to register with |
| `--worker-id <ID>` | auto-generated | `VARPULIS_WORKER_ID` | Worker identifier |

The worker auto-registers, sends heartbeats every 5 seconds, and retries registration with exponential backoff if the coordinator is unavailable.

---

## Environment Variables

| Variable | Used By | Description |
|----------|---------|-------------|
| `VARPULIS_CONFIG` | Global | Path to default configuration file |
| `VARPULIS_API_KEY` | `server`, `coordinator` | API key for authentication |
| `VARPULIS_TLS_CERT` | `server` | Path to TLS certificate |
| `VARPULIS_TLS_KEY` | `server` | Path to TLS private key |
| `VARPULIS_RATE_LIMIT` | `server` | Default rate limit (requests/second) |
| `VARPULIS_COORDINATOR` | `server` | Coordinator URL for cluster registration |
| `VARPULIS_WORKER_ID` | `server` | Worker identifier in cluster mode |
| `RUST_LOG` | All | Logging level (e.g., `info`, `debug`, `trace`) |

---

## See Also

- [Getting Started Tutorial](../tutorials/getting-started.md)
- [VPL Language Tutorial](../tutorials/language-tutorial.md)
- [Cluster Tutorial](../tutorials/cluster-tutorial.md)
- [Configuration Guide](../guides/configuration.md)
- [Cluster Architecture](../architecture/cluster.md)
- [Troubleshooting Guide](../guides/troubleshooting.md)
