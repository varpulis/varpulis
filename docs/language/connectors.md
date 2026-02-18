# VPL Connectors

This document describes how to connect Varpulis to external systems for both event ingestion (sources) and output routing (sinks).

## Overview

| Connector | Input | Output | Status | Feature Flag |
|-----------|-------|--------|--------|--------------|
| **MQTT**  | Yes | Yes | Production | `mqtt` |
| **NATS**  | Yes | Yes | Production | `nats` |
| **HTTP**  | No | Yes | Output only (webhooks) | default |
| **Kafka** | Yes | Yes | Available | `kafka` |
| **Console** | No | Yes | Debug | default |

### Feature Flags

Connectors are compiled via Cargo feature flags:

```bash
# Build with MQTT only
cargo build --release --features mqtt

# Build with all connectors
cargo build --release --features all-connectors

# Docker build with Kafka support
docker build -f deploy/docker/Dockerfile \
  --build-arg FEATURES="mqtt,kafka" \
  -t varpulis/varpulis:latest .
```

Available features: `mqtt`, `kafka`, `nats`, `postgres`, `mysql`, `sqlite`, `database`, `redis`, `persistence`, `all-connectors`.

---

## Connector Declaration Syntax

Connectors are declared at the top of a VPL file using `connector Name = type (params)`:

```varpulis
connector MqttSensors = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "varpulis-app"
)

connector KafkaOutput = kafka (
    brokers: ["kafka:9092"],
    group_id: "varpulis-consumer"
)

connector AlertWebhook = http (
    url: "https://hooks.example.com/alerts"
)
```

### Source Binding with `.from()`

Bind a stream to ingest events from a connector:

```varpulis
stream Temperatures = TemperatureReading
    .from(MqttSensors, topic: "sensors/temperature/#")
```

### Sink Routing with `.to()`

Route a stream's output to a connector:

```varpulis
stream AlertsToKafka = AllAlerts
    .to(KafkaOutput)

stream CriticalToWebhook = CriticalAlerts
    .to(AlertWebhook)
```

---

## MQTT Connector

MQTT is the recommended connector for IoT and production deployments. It provides reliable message delivery with QoS support.

### Declaration

```varpulis
connector MqttSensors = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "varpulis-app"
)
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `host` | string | Yes | - | MQTT broker hostname or IP address |
| `port` | int | Yes | 1883 | MQTT broker port |
| `client_id` | string | Yes | - | Unique identifier for this client |

### Topic Wildcards

Topics are specified in `.from()` and support MQTT wildcards:

- `#` - Multi-level wildcard (matches any number of levels)
- `+` - Single-level wildcard (matches exactly one level)

**Examples:**
```
sensors/#            # Matches sensors/temp, sensors/humidity, sensors/zone1/temp
sensors/+            # Matches sensors/temp, sensors/humidity (but not sensors/zone1/temp)
sensors/+/temp       # Matches sensors/zone1/temp, sensors/zone2/temp
```

### Event Format

Events received from MQTT must be JSON with an `event_type` field (or `type` for short):

```json
{
  "type": "TemperatureReading",
  "sensor_id": "sensor-1",
  "zone": "lobby",
  "value": 23.5,
  "timestamp": 1706400000
}
```

### Output Format

Stream `.emit()` results are published as JSON:

```json
{
  "event_type": "HighTempAlert",
  "data": {
    "alert_type": "HIGH_TEMPERATURE",
    "zone": "lobby",
    "temperature": 45.2
  },
  "timestamp": "2026-02-04T10:30:00Z"
}
```

### Complete Example

```varpulis
# Connector declarations
connector MqttSensors = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "fraud-detector-prod"
)

connector KafkaAlerts = kafka (
    brokers: ["kafka:9092"],
    group_id: "fraud-alerts"
)

# Event definitions
event Login:
    user_id: str
    ip_address: str
    device: str

event Transaction:
    user_id: str
    amount: float
    status: str
    merchant: str

# Ingest from MQTT
stream Logins = Login
    .from(MqttSensors, topic: "transactions/login")

stream Transactions = Transaction
    .from(MqttSensors, topic: "transactions/payment")

# Pattern: Login followed by failed transaction within 10 minutes
stream SuspiciousActivity = Login as login
    -> Transaction where user_id == login.user_id and status == "failed" as tx
    .within(10m)
    .emit(
        alert_type: "LOGIN_THEN_FAILED_TX",
        user_id: login.user_id,
        login_ip: login.ip_address,
        failed_amount: tx.amount,
        merchant: tx.merchant,
        severity: if tx.amount > 1000 then "high" else "medium"
    )

# Route alerts to Kafka
stream AlertsOut = SuspiciousActivity
    .to(KafkaAlerts)
```

### Running with MQTT

```bash
# Basic execution (requires --features mqtt)
varpulis run --file fraud_detection.vpl

# With verbose logging
RUST_LOG=info varpulis run --file fraud_detection.vpl
```

### Deprecated: `config mqtt` Block

> **Deprecated**: The `config mqtt { }` block syntax is deprecated. Use the `connector` declaration + `.from()` syntax instead. The legacy syntax still works but will be removed in a future version.

```varpulis
# DEPRECATED - do not use
config mqtt {
    broker: "localhost",
    port: 1883,
    client_id: "my-app",
    input_topic: "events/#",
    output_topic: "alerts"
}

# USE THIS INSTEAD
connector MqttBroker = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "my-app"
)

stream Events = MyEvent
    .from(MqttBroker, topic: "events/#")
```

---

## Kafka Connector

Kafka provides high-throughput, durable event streaming. Requires the `kafka` feature flag.

### Declaration

```varpulis
connector KafkaBroker = kafka (
    brokers: ["broker1:9092", "broker2:9092"],
    group_id: "varpulis-consumer"
)
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `brokers` | array | Yes | - | List of Kafka broker addresses |
| `group_id` | string | Yes | - | Consumer group ID |

### Usage

```varpulis
# Ingest from Kafka
stream Events = SensorReading
    .from(KafkaBroker, topic: "sensor-events")

# Output to Kafka
stream AlertsOut = ProcessedAlerts
    .to(KafkaBroker)
```

### Building with Kafka

```bash
# Requires rdkafka (librdkafka)
cargo build --release --features mqtt,kafka
```

---

## NATS Connector

NATS provides lightweight, high-performance messaging. It uses a single multiplexed connection for both subscriptions and publishing. Requires the `nats` feature flag.

### Declaration

```varpulis
connector NatsMarket = nats (
    servers: "nats://localhost:4222",
    queue_group: "varpulis"
)
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `servers` | string | Yes | - | NATS server URL(s), e.g. `nats://host:4222` |
| `queue_group` | string | No | - | Queue group for load-balanced consumption |

### Subject Wildcards

NATS subjects use `.` as a separator with two wildcard tokens:

- `*` — Matches a single token: `trades.*` matches `trades.AAPL` but not `trades.us.AAPL`
- `>` — Matches one or more tokens (must be last): `trades.>` matches `trades.AAPL` and `trades.us.AAPL`

### Usage

```varpulis
# Ingest from NATS
stream Trades = Trade
    .from(NatsMarket, topic: "trades.>")

# Output to NATS
stream Alerts = HighValueTrades
    .to(NatsMarket, topic: "alerts.high-value")
```

### Building with NATS

```bash
# Build with NATS support
cargo build --release --features nats

# Build with multiple connectors
cargo build --release --features mqtt,nats,kafka
```

---

## HTTP Connector

The HTTP connector sends events to webhooks and REST APIs (output only).

### Declaration

```varpulis
connector AlertWebhook = http (
    url: "https://webhook.example.com/alerts"
)
```

### Usage

```varpulis
stream CriticalAlerts = AllAlerts
    .where(severity == "critical")
    .to(AlertWebhook)
```

### HTTP Source (Server Mode)

For HTTP input, use Varpulis in server mode with the REST API:

```bash
# Start the server
varpulis server --port 9000 --api-key "your-key" --metrics

# Inject events via HTTP POST
curl -X POST http://localhost:9000/api/v1/pipelines/<id>/events \
  -H "X-API-Key: your-key" \
  -H "Content-Type: application/json" \
  -d '{"event_type": "Login", "fields": {"user_id": "user123"}}'
```

---

## Console Connector

For debugging, stream output is printed to stdout when no `.to()` connector is specified:

```varpulis
stream DebugOutput = SomeStream
    .where(value > 100)
    .emit(debug_info: "High value detected", value: value)
```

---

## See Also

- [Syntax Reference](syntax.md) - Complete VPL syntax
- [Architecture](../architecture/system.md) - System architecture
- [Configuration Guide](../guides/configuration.md) - CLI and server configuration
