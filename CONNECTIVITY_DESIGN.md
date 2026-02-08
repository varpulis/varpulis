# VPL Connectivity Architecture Design

## Current Problems

1. **Unclear connection model**: How do sources/sinks relate to connectors?
2. **No support for multiple connectors**: Can't have 2 MQTT brokers or 3 Kafka clusters
3. **Inline vs config confusion**: When to use inline URIs vs config blocks?
4. **Stream declaration ambiguity**: `stream X = "mqtt://..."` mixes source with stream
5. **Emit vs Sink unclear**: What's the difference? When to use which?

---

## Proposed Architecture

### Core Concepts

```
┌─────────────────────────────────────────────────────────────────────┐
│                         VPL Program                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐           │
│  │ Connector   │     │ Connector   │     │ Connector   │           │
│  │ (MqttLocal) │     │ (KafkaProd) │     │ (HttpApi)   │           │
│  └──────┬──────┘     └──────┬──────┘     └──────┬──────┘           │
│         │                   │                   │                   │
│         │    ┌──────────────┼───────────────────┘                   │
│         │    │              │                                       │
│         ▼    ▼              ▼                                       │
│  ┌─────────────┐     ┌─────────────┐                               │
│  │ Stream      │     │ Stream      │                               │
│  │ Events.from │────▶│ (joined)    │                               │
│  │ (Connector) │     │             │                               │
│  └──────┬──────┘     └──────┬──────┘                               │
│         │                   │                                       │
│         ▼                   ▼                                       │
│  ┌─────────────┐     ┌─────────────┐                               │
│  │ Pattern     │     │ .emit(...)  │                               │
│  │ (sequence)  │     │             │                               │
│  └──────┬──────┘     └──────┬──────┘                               │
│         │                   │                                       │
│         └─────────┬─────────┘                                       │
│                   │                                                 │
│         ┌─────────┴─────────┐                                       │
│         ▼                   ▼                                       │
│  ┌─────────────┐     ┌─────────────┐                               │
│  │ Sink        │     │ Sink        │                               │
│  │ .to(Conn)   │     │ .to(Conn)   │                               │
│  └─────────────┘     └─────────────┘                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 1. Connector (Connection Definition)

**Purpose**: Define how to connect to an external system. Reusable by streams and sinks.

```vpl
# Connector with explicit configuration using parentheses
connector MqttLocal = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "varpulis-001"
)

connector KafkaProd = kafka (
    brokers: ["kafka-1:9092", "kafka-2:9092"],
    security_protocol: "SASL_SSL",
    sasl_mechanism: "PLAIN"
)

connector HttpApi = http (
    base_url: "https://api.example.com",
    auth: bearer("${API_TOKEN}"),
    timeout: 30s
)

# Multiple connectors of same type
connector MqttSensors = mqtt (host: "sensors.local", port: 1883)
connector MqttActuators = mqtt (host: "actuators.local", port: 1883)
```

### 2. Stream (Processing Pipeline)

**Purpose**: Read events from a connector and process them. No separate "source" declaration needed.

```vpl
# Stream reads EventType from Connector, then processes
stream FilteredSensors = SensorReading
    .from(MqttLocal, topic: "sensors/#", qos: 1)
    .where(value > threshold)
    .select(sensor_id, value, timestamp)

stream Trades = TradeEvent
    .from(KafkaProd, topic: "market.trades", offset: "earliest")
    .where(volume > 1000)

stream Webhooks = WebhookEvent
    .from(HttpApi, endpoint: "/webhooks", method: "POST")

# Stream from another stream (no .from needed - already connected)
stream Aggregated = FilteredSensors
    .partition_by(sensor_id)
    .window(tumbling: 5m)
    .aggregate(
        avg_value: avg(value),
        max_value: max(value),
        count: count()
    )

# Stream with join (multiple inputs)
stream Enriched = Trades
    .join(Quotes, on: symbol, within: 1s)
    .select(
        symbol,
        trade_price: Trades.price,
        quote_price: Quotes.price,
        spread: Trades.price - Quotes.price
    )

# Inline file source (for simple cases)
stream Logs = LogEvent.from(file("./events.jsonl"))
```

### 3. Pattern (SASE+ Matching)

**Purpose**: Detect temporal patterns in event streams.

```vpl
# Pattern on a stream
pattern FraudSequence on Transactions
    SEQ(
        Login as login where device == "unknown",
        Transaction+ as txs where amount > 500,
        NOT Logout
    )
    .within(30m)
    .partition_by(user_id)

# Pattern result is a stream - output type is automatically "FraudAlerts"
stream FraudAlerts = FraudSequence
    .emit(
        user_id: login.user_id,
        total_amount: sum(txs.amount),
        transaction_count: len(txs)
    )
```

### 4. Emit and To (Output Shaping & Routing)

**Purpose**:
- `.emit(...)` shapes output data (field mapping, transformation)
- `.to(Connector, ...)` routes to external system (optional)

**Key Design Decisions**:
1. Stream name IS the output event type (no redundancy)
2. Without `.to()`, events are emitted **internally** (available to other streams/patterns)
3. With `.to()`, events are also sent to external connector

```vpl
# Internal emit only - available to other streams/patterns
stream Alerts = SensorReading
    .from(MqttLocal, topic: "sensors/#")
    .where(value > critical_threshold)
    .emit(
        alert_id: uuid(),
        alert_type: "CRITICAL",
        sensor_id,
        value,
        timestamp: now()
    )
# "Alerts" events are available internally, no external output

# Another stream can consume the internal events
stream CriticalAlerts = Alerts
    .where(alert_type == "CRITICAL")

# With .to() - emit internally AND send to external connector
stream ExternalAlerts = SensorReading
    .from(MqttLocal, topic: "sensors/#")
    .where(value > threshold)
    .emit(
        alert_id: uuid(),
        severity: "warning"
    )
    .to(KafkaProd, topic: "alerts")  # Also sends to Kafka

# Explicit type override (rare case)
stream Pipeline = SensorReading
    .from(MqttLocal, topic: "sensors/#")
    .emit as Alert (           # Output type is "Alert", not "Pipeline"
        alert_id: uuid(),
        severity: "warning"
    )
    .to(HttpApi, endpoint: "/alerts")
```

**Summary**:
- `.emit(...)` = shape output fields (always happens)
- No `.to()` = internal only (other streams can `.from()` this stream)
- `.to(Conn)` = internal + external output

### 5. Sink (External Output Declaration)

**Purpose**: Alternative way to route stream data to connectors. Use when:
- Stream already defined without `.to()`
- Need multiple outputs from same stream
- Want to separate stream logic from output routing

```vpl
# Separate sink declaration (alternative to inline .to())
stream Alerts = SensorReading
    .from(MqttLocal, topic: "sensors/#")
    .where(value > threshold)
    .emit(alert_id: uuid(), value)
# No .to() - internal only

# Add external output via sink declaration
sink Alerts to KafkaProd (topic: "alerts.critical")

# Multiple sinks for same stream
sink Alerts to [
    KafkaProd (topic: "alerts"),
    MqttLocal (topic: "notifications/alerts")
]

# Sink with formatting options
sink Alerts to HttpApi (
    endpoint: "/alerts",
    method: "POST",
    format: json
)

# Built-in sinks (no connector needed)
sink Debug to console()
sink Metrics to tap(counter: "events_total", labels: [type, severity])
sink Logs to log(level: "info")
```

**Inline `.to()` vs Separate `sink`**:
```vpl
# Option A: Inline (compact, single output)
stream X = Event.from(Conn1).emit(...).to(Conn2, topic: "out")

# Option B: Separate (flexible, multiple outputs)
stream X = Event.from(Conn1).emit(...)
sink X to Conn2 (topic: "out1")
sink X to Conn3 (topic: "out2")
```

---

## Example: Complete Pipeline

```vpl
# 1. Define connectors (using parentheses for config)
connector MqttSensors = mqtt (
    host: "sensor-gateway.local",
    port: 1883
)

connector KafkaAnalytics = kafka (
    brokers: ["kafka-1:9092"],
    group_id: "analytics-pipeline"
)

connector HttpAlerts = http (
    base_url: "https://alert-service.local",
    auth: bearer("${ALERT_TOKEN}")
)

# 2. Define event types
event SensorReading {
    sensor_id: str,
    zone: str,
    value: float,
    unit: str,
    timestamp: timestamp
}

event Alert {
    alert_id: str,
    alert_type: str,
    severity: str,
    source: str,
    message: str,
    timestamp: timestamp
}

# 3. Process streams (EventType.from(Connector) syntax)
stream Sensors = SensorReading
    .from(MqttSensors, topic: "sensors/+/reading")

stream HighTempReadings = Sensors
    .where(unit == "celsius" and value > 28)
    .partition_by(zone)

stream ZoneAlerts = HighTempReadings
    .window(tumbling: 5m)
    .aggregate(
        zone,
        avg_temp: avg(value),
        max_temp: max(value),
        reading_count: count()
    )
    .where(avg_temp > 30)
    .emit as Alert (       # Explicit type: produces Alert events
        alert_id: uuid(),
        alert_type: "HIGH_TEMPERATURE",
        severity: if max_temp > 35 then "critical" else "warning",
        source: zone,
        message: format("Zone {} avg temp: {:.1f}°C", zone, avg_temp),
        timestamp: now()
    )

# 4. Pattern matching
pattern RapidTempRise on Sensors
    SEQ(
        SensorReading as r1,
        SensorReading+ as readings where sensor_id == r1.sensor_id
    )
    .within(2m)
    .where(last(readings).value - r1.value > 10)
    .partition_by(sensor_id)

stream TempSpikeAlerts = RapidTempRise
    .emit as Alert (       # Explicit type: produces Alert events
        alert_id: uuid(),
        alert_type: "RAPID_TEMPERATURE_RISE",
        severity: "warning",
        source: r1.sensor_id,
        message: format("Rapid rise from {} to {}", r1.value, last(readings).value),
        timestamp: now()
    )

# 5. Output to sinks (using parentheses for config)
sink ZoneAlerts to KafkaAnalytics (topic: "alerts.zone")

sink ZoneAlerts to HttpAlerts (
    endpoint: "/api/alerts",
    method: "POST"
)

sink TempSpikeAlerts to [
    KafkaAnalytics (topic: "alerts.sensor"),
    HttpAlerts (endpoint: "/api/alerts", method: "POST")
]

# Debug output
sink Sensors to console()
```

---

## Key Design Principles

### 1. Separation of Concerns
- **Connector** = HOW to connect (host, credentials, protocol settings)
- **Event** = WHAT data looks like (schema/structure)
- **Stream** = WHERE to read + HOW to process (`.from(Connector)` + operations)
- **Pattern** = WHAT to detect (temporal sequences)
- **Emit** = WHAT to output (field mapping, reshaping)
- **Sink** = WHERE to send (`.to(Connector)` with topic/endpoint)

### 2. Explicit Data Flow
- Data flows: `Connector ← Stream.from() → [Pattern →] .emit() → Sink.to() → Connector`
- No hidden connections or implicit behavior
- Visual editor can show this flow clearly

### 3. Reusability
- Connectors can be used by multiple streams and sinks
- Event types can be reused across streams
- Patterns can be applied to different streams

### 4. Composability
- Streams can be chained: `stream B = A.where(...)`
- Multiple streams can feed one stream: `merge(A, B, C)`
- One stream can feed multiple sinks

### 5. No Redundancy (DRY Principle)
- **Stream name = Output event type** by default
- `.emit(...)` only defines field mappings, not type
- Explicit `as Type` only when output type differs from stream name
- Example: `stream Alerts = ...emit(...)` produces `Alerts` events
- Override: `stream Pipeline = ...emit as Alert (...)` produces `Alert` events

### 6. Consistent Syntax
- Connectors use parentheses: `connector X = type (params...)`
- Stream input: `.from(Connector, params...)`
- Stream output: `.to(Connector, params...)` or separate `sink`
- All configuration uses same `key: value` format

---

## Implementation Plan

### Phase 1: Language Updates
1. Add `connector` keyword with parentheses syntax
2. Add `.from(Connector, ...)` method to event types
3. Add `.to(Connector, ...)` inline sink and `sink X to Connector (...)` syntax
4. Update parser and AST

### Phase 2: Runtime Updates
1. Connection pool management for connectors
2. Stream subscription via `.from()` handling
3. Sink publishing with batching/retry via `.to()`
4. Multi-sink support

### Phase 3: Flow Editor Updates
1. Connector nodes with visual type indicators
2. Stream nodes with `.from()` connection visualization
3. Sink nodes with `.to()` connection visualization
4. Clear data flow arrows

### Phase 4: Validation
1. Type checking between event types and stream operations
2. Connector compatibility validation
3. Cycle detection in stream dependencies

---

## Resolved Design Decisions

1. **Sources are integrated into streams** ✓
   ```vpl
   stream X = EventType.from(Connector, topic: "...")
       .where(...)
   ```

2. **Connector credentials via environment variables** ✓
   ```vpl
   connector Kafka = kafka (password: "${KAFKA_PASSWORD}")
   ```

3. **Sinks support both inline and separate declaration** ✓
   ```vpl
   # Inline
   stream X = ...emit(...).to(Connector, topic: "...")

   # Separate
   sink X to Connector (topic: "...")
   ```

4. **Stream branching works naturally** ✓
   ```vpl
   stream Base = Event.from(Conn, topic: "events")

   stream Critical = Base.where(severity == "critical")
   sink Critical to AlertsConn (topic: "critical")

   stream Metrics = Base.aggregate(...)
   sink Metrics to MetricsConn (topic: "metrics")
   ```
