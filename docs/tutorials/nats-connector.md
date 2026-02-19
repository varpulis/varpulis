# NATS Connector Tutorial

This tutorial walks you through setting up Varpulis with NATS for event ingestion and output. You'll learn to declare NATS connectors, subscribe to subjects with wildcards, publish results, use queue groups for load balancing, and configure authentication.

## Prerequisites

- Varpulis built with the `nats` feature (`cargo build --release --features nats`)
- A running `nats-server` (we'll start one below)
- Basic VPL knowledge (see [Getting Started](getting-started.md))

---

## Part 1: Starting NATS

### Install and Run nats-server

```bash
# Docker (recommended)
docker run -d --name nats -p 4222:4222 -p 8222:8222 nats:latest

# Or install natively (macOS)
brew install nats-server
nats-server

# Or install natively (Linux)
# Download from https://github.com/nats-io/nats-server/releases
nats-server
```

Verify it's running:

```bash
curl http://localhost:8222/varz 2>/dev/null | head -5
# Should show server info
```

### Install NATS CLI (Optional)

The `nats` CLI tool is helpful for publishing test events and subscribing to subjects:

```bash
# macOS
brew install nats-io/nats-tools/nats

# Linux
curl -sf https://binaries.nats.dev/nats-io/natscli/nats@latest | sh

# Test connection
nats server ping
```

---

## Part 2: Your First NATS Source

### Write a Simple VPL Program

Create `nats_demo.vpl`:

```varpulis
connector NatsBroker = nats (
    servers: "nats://localhost:4222"
)

stream HighTemp = TemperatureReading
    .from(NatsBroker, topic: "sensors.temperature")
    .where(temperature > 50)
    .emit(
        alert: "High temperature detected",
        sensor_id: sensor_id,
        temperature: temperature
    )
```

### Run It

```bash
varpulis run --file nats_demo.vpl
```

### Publish Test Events

In another terminal, use the NATS CLI:

```bash
# Event that triggers the alert (temperature > 50)
nats pub sensors.temperature '{"type": "TemperatureReading", "sensor_id": "S1", "temperature": 72.5}'

# Event that doesn't trigger (temperature <= 50)
nats pub sensors.temperature '{"type": "TemperatureReading", "sensor_id": "S2", "temperature": 22.0}'
```

The first event matches the filter and produces output. The second is silently dropped.

---

## Part 3: Subject Wildcards

NATS subjects use `.` as a separator. Two wildcards are available:

| Wildcard | Scope | Example |
|----------|-------|---------|
| `*` | Single token | `sensors.*` matches `sensors.temp` but not `sensors.zone1.temp` |
| `>` | One or more tokens (must be last) | `sensors.>` matches `sensors.temp` and `sensors.zone1.temp` |

### Example: Multi-Level Subscription

```varpulis
connector NatsBroker = nats (
    servers: "nats://localhost:4222"
)

# Subscribe to all sensor readings across all zones
stream AllSensors = SensorReading
    .from(NatsBroker, topic: "sensors.>")
    .emit(sensor_id: sensor_id, value: value, zone: zone)

# Subscribe to temperature sensors only (any zone)
stream TempOnly = TemperatureReading
    .from(NatsBroker, topic: "sensors.*.temperature")
    .where(value > 100)
    .emit(alert: "overheating", sensor_id: sensor_id)
```

Publish events to different subjects:

```bash
nats pub sensors.zone1.temperature '{"type": "TemperatureReading", "sensor_id": "T1", "value": 105, "zone": "zone1"}'
nats pub sensors.zone2.pressure '{"type": "SensorReading", "sensor_id": "P1", "value": 250, "zone": "zone2"}'
```

The `sensors.>` subscription receives both events. The `sensors.*.temperature` subscription only receives the first.

### Event Type Inference

If a JSON payload doesn't include an `event_type` or `type` field, Varpulis uses the last segment of the NATS subject. For example, publishing to `sensors.temperature` with `{"sensor_id": "S1", "value": 72}` creates an event with `event_type = "temperature"`.

---

## Part 4: NATS Sink (Publishing Results)

### Output to NATS

Use `.to()` to publish stream results to a NATS subject:

```varpulis
connector NatsBroker = nats (
    servers: "nats://localhost:4222"
)

stream Alerts = TemperatureReading
    .from(NatsBroker, topic: "sensors.temperature")
    .where(temperature > 50)
    .emit(
        alert_type: "HIGH_TEMP",
        sensor_id: sensor_id,
        temperature: temperature
    )
    .to(NatsBroker, topic: "alerts.temperature")
```

Subscribe to the output subject to see results:

```bash
# In one terminal: listen for alerts
nats sub "alerts.>"

# In another terminal: publish a hot sensor reading
nats pub sensors.temperature '{"type": "TemperatureReading", "sensor_id": "S1", "temperature": 85.0}'
```

### Multiple Sinks

Route different alert severities to different subjects:

```varpulis
connector NatsBroker = nats (
    servers: "nats://localhost:4222"
)

stream Warnings = TemperatureReading
    .from(NatsBroker, topic: "sensors.temperature")
    .where(temperature > 50 and temperature <= 100)
    .emit(severity: "warning", sensor_id: sensor_id, temperature: temperature)
    .to(NatsBroker, topic: "alerts.warning")

stream Critical = TemperatureReading
    .from(NatsBroker, topic: "sensors.temperature")
    .where(temperature > 100)
    .emit(severity: "critical", sensor_id: sensor_id, temperature: temperature)
    .to(NatsBroker, topic: "alerts.critical")
```

---

## Part 5: Queue Groups (Load Balancing)

NATS queue groups distribute messages across subscribers. When multiple Varpulis instances share the same `queue_group`, each message is delivered to exactly one instance.

```varpulis
connector NatsBroker = nats (
    servers: "nats://localhost:4222",
    queue_group: "varpulis-workers"
)

stream Trades = TradeEvent
    .from(NatsBroker, topic: "trades.>")
    .where(amount > 10000)
    .emit(alert: "large trade", symbol: symbol, amount: amount)
```

Run multiple instances:

```bash
# Instance 1
varpulis run --file trades.vpl &

# Instance 2
varpulis run --file trades.vpl &
```

Messages published to `trades.>` are distributed evenly between the two instances. Each trade event is processed by exactly one instance, enabling horizontal scaling.

Without `queue_group`, every instance receives every message (fan-out pattern).

---

## Part 6: Authentication

### User/Password

Configure authentication in the NATS server:

```
# nats-server.conf
authorization {
  users = [
    {user: "varpulis", password: "secret-password"}
  ]
}
```

Start nats-server with the config:

```bash
nats-server -c nats-server.conf
```

In VPL, authentication is configured per connector. Currently, credentials are passed programmatically through the managed connector API when deploying in server mode. In VPL connector declarations, the `servers` URL is the primary configuration:

```varpulis
connector NatsBroker = nats (
    servers: "nats://localhost:4222"
)
```

For authenticated deployments, use the server mode REST API which passes credentials through the connector configuration.

### Token Authentication

Token authentication uses a single shared token:

```
# nats-server.conf
authorization {
  token: "my-secret-token"
}
```

---

## Part 7: Event Format

### Inbound Events (Source)

NATS events must be JSON. Two formats are supported:

**Flat format** (recommended):
```json
{
  "type": "TemperatureReading",
  "sensor_id": "S1",
  "temperature": 72.5,
  "zone": "lobby"
}
```

The `type` (or `event_type`) field determines which VPL stream processes the event. All other fields become event data accessible in `.where()` and `.emit()`.

**Nested format** (with `data` envelope):
```json
{
  "event_type": "TemperatureReading",
  "data": {
    "sensor_id": "S1",
    "temperature": 72.5,
    "zone": "lobby"
  }
}
```

When a `data` object is present, its fields are used as event data.

**Subject fallback**: If neither `type` nor `event_type` is present, the last `.`-delimited segment of the NATS subject is used. Publishing `{"value": 42}` to `sensors.temperature` creates an event with type `temperature`.

### Outbound Events (Sink)

Events published via `.to()` use this JSON format:

```json
{
  "event_type": "HighTempAlert",
  "data": {
    "alert_type": "HIGH_TEMP",
    "sensor_id": "S1",
    "temperature": 72.5
  },
  "timestamp": "2026-02-19T10:30:00Z"
}
```

---

## Part 8: Complete Example -- Market Data Pipeline

A realistic example: ingest market trades from NATS, detect large trades and rapid sequences, and publish alerts.

```varpulis
connector NatsMarket = nats (
    servers: "nats://localhost:4222",
    queue_group: "market-processor"
)

connector NatsAlerts = nats (
    servers: "nats://localhost:4222"
)

event Trade:
    symbol: str
    price: float
    volume: int
    exchange: str

event Quote:
    symbol: str
    bid: float
    ask: float

# Ingest trades from all exchanges
stream Trades = Trade
    .from(NatsMarket, topic: "market.trades.>")

# Ingest quotes
stream Quotes = Quote
    .from(NatsMarket, topic: "market.quotes.>")

# Detect large trades
stream LargeTrades = Trade
    .from(NatsMarket, topic: "market.trades.>")
    .where(volume > 10000)
    .emit(
        alert_type: "LARGE_TRADE",
        symbol: symbol,
        price: price,
        volume: volume,
        exchange: exchange
    )
    .to(NatsAlerts, topic: "alerts.large-trades")

# Detect rapid trade sequence: 3+ trades for the same symbol within 10 seconds
stream RapidTrading = Trade as t1
    -> Trade where symbol == t1.symbol as t2
    -> Trade where symbol == t1.symbol as t3
    .within(10s)
    .emit(
        alert_type: "RAPID_TRADING",
        symbol: t1.symbol,
        trade_count: 3,
        first_price: t1.price,
        last_price: t3.price,
        price_change: t3.price - t1.price
    )
    .to(NatsAlerts, topic: "alerts.rapid-trading")
```

### Run and Test

```bash
# Terminal 1: Start the pipeline
varpulis run --file market_pipeline.vpl

# Terminal 2: Subscribe to alerts
nats sub "alerts.>"

# Terminal 3: Publish test trades
nats pub market.trades.NYSE '{"type":"Trade","symbol":"AAPL","price":150.25,"volume":15000,"exchange":"NYSE"}'
nats pub market.trades.NYSE '{"type":"Trade","symbol":"AAPL","price":150.50,"volume":500,"exchange":"NYSE"}'
nats pub market.trades.NYSE '{"type":"Trade","symbol":"AAPL","price":151.00,"volume":200,"exchange":"NYSE"}'
```

The first trade triggers a `LARGE_TRADE` alert (volume > 10000). After the third trade, the sequence pattern triggers a `RAPID_TRADING` alert.

---

## NATS vs MQTT vs Kafka: When to Use What

| Feature | NATS | MQTT | Kafka |
|---------|------|------|-------|
| Latency | Sub-millisecond | Low (QoS 0) | Higher (batching) |
| Persistence | Optional (JetStream) | QoS 1/2 | Built-in |
| Throughput | Very high | Moderate | Very high |
| Queue groups | Native | Not standard | Consumer groups |
| Subject wildcards | `*` and `>` | `+` and `#` | None |
| Best for | Low-latency streaming, microservices | IoT, resource-constrained devices | Durable event logs, replay |

**Use NATS when** you need low-latency, lightweight messaging with flexible subject routing and built-in load balancing.

**Use MQTT when** you're working with IoT devices, need QoS guarantees, or have constrained network environments.

**Use Kafka when** you need durable event logs, replay capability, or very high-throughput batch processing.

---

## Troubleshooting

### Connection Refused

```
Error: ConnectionFailed: "connection refused"
```

Verify nats-server is running and accessible:

```bash
nats server ping
# or
curl http://localhost:8222/varz
```

### Feature Not Available

```
Error: NATS requires 'nats' feature. Build with: cargo build --features nats
```

Rebuild with the `nats` feature enabled:

```bash
cargo build --release --features nats
```

### No Events Received

1. Verify the subscription subject matches your publishing subject
2. Check wildcards: `sensors.*` won't match `sensors.zone1.temp` (use `sensors.>`)
3. Verify JSON format includes `type` or `event_type` field
4. Check NATS monitoring: `curl http://localhost:8222/subsz`

### Large Payloads Rejected

Varpulis enforces a maximum event payload size. Events exceeding the limit are logged as warnings and skipped. Check `RUST_LOG=debug` output for payload size warnings.

---

## Next Steps

- [Connectors Reference](../language/connectors.md) -- Complete NATS connector syntax
- [NATS Transport Architecture](../architecture/nats-transport.md) -- How NATS is used for cluster communication
- [Cluster Tutorial](cluster-tutorial.md) -- Distributed execution with coordinator and workers
- [Configuration Guide](../guides/configuration.md) -- NATS configuration options
