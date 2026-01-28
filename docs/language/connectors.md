# VarpulisQL Connectors

This document describes how to connect Varpulis to external systems for both event ingestion (sources) and result publishing (sinks).

## Overview

Varpulis supports multiple connector types:

| Connector | Input | Output | Status |
|-----------|-------|--------|--------|
| **MQTT**  | Yes | Yes | Production |
| **HTTP**  | No | Yes | Output only |
| **Kafka** | No | No | Stub (not implemented) |
| **Console** | No | Yes | Debug |

## MQTT Connector

MQTT is the recommended connector for production deployments. It provides reliable message delivery with QoS support.

### Configuration Block

Add a `config mqtt { }` block at the top of your VPL file:

```vpl
config mqtt {
    broker: "localhost",           # Required: MQTT broker hostname or IP
    port: 1883,                    # Required: MQTT broker port
    client_id: "varpulis-app",     # Required: Unique client ID
    input_topic: "events/#",       # Required: Topic pattern to subscribe
    output_topic: "alerts"         # Required: Topic for publishing results
}
```

### Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `broker` | string | Yes | - | MQTT broker hostname or IP address |
| `port` | int | Yes | 1883 | MQTT broker port |
| `client_id` | string | Yes | - | Unique identifier for this client |
| `input_topic` | string | Yes | - | Topic pattern for subscribing (supports `#` and `+` wildcards) |
| `output_topic` | string | Yes | - | Base topic for publishing stream results |

### Topic Wildcards

- `#` - Multi-level wildcard (matches any number of levels)
- `+` - Single-level wildcard (matches exactly one level)

**Examples:**
```
events/#        # Matches events/login, events/transaction, events/user/created
events/+        # Matches events/login, events/transaction (but not events/user/created)
sensors/+/temp  # Matches sensors/zone1/temp, sensors/zone2/temp
```

### Event Format

Events received from MQTT must be JSON with an `event_type` field (or `type` for short):

```json
{
  "type": "Login",
  "user_id": "user123",
  "ip_address": "192.168.1.1",
  "timestamp": 1706400000
}
```

Or with nested data:

```json
{
  "event_type": "TemperatureReading",
  "data": {
    "sensor_id": "sensor-1",
    "temperature": 23.5,
    "unit": "celsius"
  }
}
```

### Output Format

Stream `.emit()` results are published as JSON:

```json
{
  "event_type": "Alert",
  "data": {
    "alert_type": "high_temperature",
    "sensor_id": "sensor-1",
    "temperature": 45.2
  },
  "timestamp": "2025-01-28T10:30:00Z"
}
```

### Complete Example

```vpl
# File: fraud_detection.vpl
# Run: varpulis run fraud_detection.vpl

config mqtt {
    broker: "localhost",
    port: 1883,
    client_id: "fraud-detector-prod",
    input_topic: "transactions/#",
    output_topic: "alerts/fraud"
}

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
```

### Running with MQTT

```bash
# Basic execution
varpulis run fraud_detection.vpl

# With explicit MQTT broker (overrides config)
varpulis run fraud_detection.vpl --mqtt localhost:1883

# With verbose logging
RUST_LOG=info varpulis run fraud_detection.vpl
```

### Error Messages

If MQTT is not configured, Varpulis shows a helpful error:

```
⚠️  No 'config mqtt' block found in program.
   Add a config block to connect to MQTT:

   config mqtt {
       broker: "localhost",
       port: 1883,
       input_topic: "varpulis/events/#",
       output_topic: "varpulis/alerts"
   }
```

If MQTT feature is not compiled:

```
MQTT requires 'mqtt' feature. Build with: cargo build --features mqtt
```

## HTTP Connector

The HTTP connector allows sending events to webhooks and REST APIs.

### Output to HTTP

```vpl
stream Alerts = DetectedPatterns
    .emit(
        alert_id: uuid(),
        severity: "high"
    )
    .to("http://webhook.example.com/alerts")
```

### HTTP Source (Webhook Receiver)

```bash
# Start Varpulis HTTP server
varpulis server --port 9000

# Send events via HTTP POST
curl -X POST http://localhost:9000/events \
  -H "Content-Type: application/json" \
  -d '{"type": "Login", "user_id": "user123"}'
```

## Console Connector

For debugging, events can be printed to stdout:

```vpl
stream DebugOutput = SomeStream
    .emit(debug_info: "Processing event")
    .tap(log: "debug")
```

## See Also

- [Syntax Reference](syntax.md) - Complete VarpulisQL syntax
- [Demos README](../../demos/README.md) - Interactive demos with MQTT
- [Architecture](../architecture/system.md) - System architecture
