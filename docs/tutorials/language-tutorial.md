# VarpulisQL Language Tutorial

A comprehensive guide to writing VarpulisQL programs, from basic event processing to advanced pattern matching.

## Table of Contents

1. [Part 1: Basics](#part-1-basics) - Events, streams, filters, emit
2. [Part 2: Windows and Aggregations](#part-2-windows-and-aggregations)
3. [Part 3: Sequence Patterns](#part-3-sequence-patterns)
4. [Part 4: SASE+ Advanced](#part-4-sase-advanced) - Kleene, negation, AND/OR
5. [Part 5: Attention Windows](#part-5-attention-windows)
6. [Part 6: Joins](#part-6-joins)

---

## Part 1: Basics

### Events

Events are the fundamental unit of data in Varpulis. Each event has:
- A **type** (string identifier)
- A **timestamp** (automatically assigned or from data)
- **Fields** (key-value data)

Events arrive from sources like MQTT, files, or the WebSocket API:

```json
{
  "event_type": "TemperatureReading",
  "timestamp": "2024-01-15T10:30:00Z",
  "sensor_id": "S1",
  "temperature": 72.5,
  "unit": "F"
}
```

### Defining Event Types (Optional)

You can explicitly define event schemas:

```vpl
event TemperatureReading:
    sensor_id: str
    temperature: float
    unit: str

event HumidityReading:
    sensor_id: str
    humidity: float
```

### Streams

Streams are continuous flows of events. Create streams with the `stream` keyword:

```vpl
// Basic stream: listen for all TemperatureReading events
stream Temperatures from TemperatureReading

// Stream with alias
stream T = Temperatures
```

### Filtering with `where`

Filter events based on conditions:

```vpl
// Single condition
stream HighTemps from TemperatureReading
    where temperature > 100

// Multiple conditions (AND)
stream CriticalTemps from TemperatureReading
    where temperature > 100 and sensor_id == "critical-zone"

// OR conditions
stream AlertZones from TemperatureReading
    where sensor_id == "zone-1" or sensor_id == "zone-2"

// Compound conditions
stream Filtered from TemperatureReading
    where (temperature > 90 and humidity > 80) or emergency == true
```

**Comparison operators**: `==`, `!=`, `<`, `<=`, `>`, `>=`

**Logical operators**: `and`, `or`, `not`

### Selecting Fields with `select`

Transform events by selecting specific fields or computing new ones:

```vpl
stream SimplifiedTemps from TemperatureReading
    select {
        sensor: sensor_id,
        temp: temperature,
        is_high: temperature > 80
    }

// Computed fields
stream EnhancedTemps from TemperatureReading
    select {
        sensor_id,
        temp_celsius: (temperature - 32) * 5 / 9,
        reading_time: timestamp
    }
```

### Emitting Alerts and Logs

Use `emit` to output data when conditions are met:

```vpl
// Emit an alert
stream TempAlerts from TemperatureReading
    where temperature > 100
    emit alert("HighTemperature", "Sensor {sensor_id} reading {temperature}°F")

// Emit to log
stream TempLog from TemperatureReading
    emit log("Received: {sensor_id} = {temperature}")

// Emit with severity
stream CriticalAlerts from TemperatureReading
    where temperature > 150
    emit alert("CriticalTemperature", "DANGER: {sensor_id} at {temperature}°F", severity: "critical")
```

### Variables and Constants

```vpl
// Immutable variable
let threshold = 100
let sensor_name = "main-sensor"

// Mutable variable
var counter = 0

// Constants (compile-time)
const MAX_TEMP = 200
const API_KEY = "secret123"

// Use in streams
stream Alerts from TemperatureReading
    where temperature > threshold
    emit alert("High", "Above threshold")
```

### Comments

```vpl
// Single-line comment

/*
   Multi-line
   comment
*/

# Hash-style comment (also single-line)
```

---

## Part 2: Windows and Aggregations

Windows collect events over time or count, enabling aggregate calculations.

### Tumbling Windows

Non-overlapping, fixed-duration windows:

```vpl
// 1-minute tumbling window
stream MinuteStats from TemperatureReading
    window tumbling 1m
    aggregate {
        avg_temp: avg(temperature),
        max_temp: max(temperature),
        count: count()
    }

// 5-second window
stream RapidStats from SensorReading
    window tumbling 5s
    aggregate { readings: count() }

// 1-hour window
stream HourlyReport from Transaction
    window tumbling 1h
    aggregate {
        total: sum(amount),
        avg_amount: avg(amount)
    }
```

**Duration units**: `s` (seconds), `m` (minutes), `h` (hours), `d` (days)

### Sliding Windows

Overlapping windows with a slide interval:

```vpl
// 5-minute window, slides every 1 minute
stream SlidingAvg from TemperatureReading
    window sliding 5m by 1m
    aggregate {
        rolling_avg: avg(temperature)
    }

// 10-second window, slides every 2 seconds
stream RecentTrend from SensorReading
    window sliding 10s by 2s
    aggregate {
        recent_max: max(value),
        recent_min: min(value)
    }
```

### Count-Based Windows

Windows based on event count:

```vpl
// Every 100 events
stream BatchStats from Transaction
    window count 100
    aggregate {
        batch_total: sum(amount),
        batch_avg: avg(amount)
    }

// Sliding count window: 50 events, slide by 10
stream RollingBatch from Reading
    window count 50 by 10
    aggregate { rolling_sum: sum(value) }
```

### Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `count()` | Number of events | `count()` |
| `sum(field)` | Sum of field values | `sum(amount)` |
| `avg(field)` | Average (SIMD-optimized) | `avg(temperature)` |
| `min(field)` | Minimum value (SIMD-optimized) | `min(price)` |
| `max(field)` | Maximum value (SIMD-optimized) | `max(price)` |
| `stddev(field)` | Standard deviation | `stddev(latency)` |
| `percentile(field, p)` | Percentile (0-100) | `percentile(latency, 95)` |
| `collect(field)` | Collect values into array | `collect(sensor_id)` |
| `first(field)` | First value in window | `first(timestamp)` |
| `last(field)` | Last value in window | `last(value)` |

### Partitioned Windows

Windows partitioned by a key:

```vpl
// Per-sensor statistics
stream PerSensorStats from TemperatureReading
    partition by sensor_id
    window tumbling 1m
    aggregate {
        sensor: sensor_id,
        avg_temp: avg(temperature),
        readings: count()
    }

// Per-customer totals
stream CustomerTotals from Transaction
    partition by customer_id
    window tumbling 1h
    aggregate {
        customer: customer_id,
        hourly_spend: sum(amount)
    }
```

### Filtering After Aggregation

Apply conditions to aggregated results:

```vpl
stream HighVolumeMinutes from Transaction
    window tumbling 1m
    aggregate {
        total: sum(amount),
        count: count()
    }
    where total > 10000 or count > 100
    emit alert("HighVolume", "Minute had {count} transactions totaling {total}")
```

---

## Part 3: Sequence Patterns

Sequence patterns detect events occurring in a specific order using the `->` operator.

### Basic Sequences

```vpl
// A followed by B
pattern LoginLogout = Login -> Logout

// A followed by B followed by C
pattern ThreeStep = Start -> Process -> Complete

// Use in stream
stream Sessions from *
    pattern Login -> Logout within 1h
    emit log("Session: {Login.user_id} logged in and out")
```

### Sequences with Conditions

```vpl
// Events must match conditions
pattern FailedLogin =
    LoginAttempt[status == "failed"] as first
    -> LoginAttempt[status == "failed" and user_id == first.user_id] as second
    -> LoginAttempt[status == "failed" and user_id == first.user_id] as third
    within 5m

stream BruteForceDetection from LoginAttempt
    pattern FailedLogin
    emit alert("BruteForce", "3 failed attempts for {first.user_id}")
```

### Referencing Previous Events

Use aliases to reference earlier events in the sequence:

```vpl
pattern PriceSpike =
    Trade as t1
    -> Trade[symbol == t1.symbol and price > t1.price * 1.1] as t2
    within 1m

stream Spikes from Trade
    pattern PriceSpike
    emit alert("PriceSpike", "{t1.symbol} jumped from {t1.price} to {t2.price}")
```

### Temporal Constraints

Constrain how quickly events must occur:

```vpl
// Must complete within 5 minutes
pattern QuickCheckout =
    CartAdd -> PaymentStart -> PaymentComplete
    within 5m

// Different timeouts per step
pattern SlowThenFast =
    SlowEvent
    -> FastEvent within 10s
    -> FinalEvent within 5s
```

---

## Part 4: SASE+ Advanced

SASE+ extends basic patterns with Kleene closures, negation, and logical operators.

### Kleene Plus (`+`) - One or More

```vpl
// One or more failed logins followed by success
pattern BruteForceSuccess =
    LoginFailed+ -> LoginSuccess
    within 10m

stream Attacks from *
    pattern BruteForceSuccess
    emit alert("BruteForce", "Multiple failures followed by success")
```

### Kleene Star (`*`) - Zero or More

```vpl
// Start, any number of middle events, then end
pattern FullSession =
    SessionStart -> Activity* -> SessionEnd
    within 1h

// Optional retries before success
pattern WithRetries =
    Request -> Retry* -> Success
    within 30s
```

### Negation (`NOT`) - Absence of Event

```vpl
// Payment started but not completed
pattern AbandonedPayment =
    PaymentStart -> NOT(PaymentComplete) within 5m

stream Abandoned from *
    pattern AbandonedPayment
    emit alert("Abandoned", "Payment started but not completed")

// Order without confirmation
pattern UnconfirmedOrder =
    OrderPlaced -> NOT(OrderConfirmed) within 1h
```

### AND - Both Events (Any Order)

```vpl
// Both A and B must occur (order doesn't matter)
pattern BothRequired =
    AND(DocumentUploaded, SignatureProvided)
    within 1h

stream Complete from *
    pattern BothRequired
    emit log("Both document and signature received")
```

### OR - Either Event

```vpl
// Either payment method
pattern PaymentReceived =
    OR(CreditCardPayment, BankTransfer)

stream Payments from *
    pattern PaymentReceived
    emit log("Payment received via {match.event_type}")
```

### Complex Combinations

```vpl
// (A followed by B) AND (C or D), all within 10 minutes
pattern ComplexFlow =
    (Start -> Middle) AND (OR(OptionA, OptionB))
    within 10m

// Multiple failures, then either success or lockout
pattern AuthResult =
    LoginFailed+ -> OR(LoginSuccess, AccountLocked)
    within 15m

// Order placed, items added, no cancellation, then shipped
pattern SuccessfulOrder =
    OrderPlaced
    -> ItemAdded+
    -> NOT(OrderCancelled)
    -> OrderShipped
    within 24h
```

### Partition-By for Patterns

Process patterns independently per partition key:

```vpl
// Per-user pattern matching
pattern UserFailures =
    LoginFailed+ -> LoginSuccess
    within 10m
    partition by user_id

stream UserAttacks from *
    pattern UserFailures
    emit alert("UserBruteForce", "User {user_id} had multiple failures")
```

---

## Part 5: Attention Windows

Attention windows use machine learning-inspired correlation to find related events.

### Basic Attention Window

```vpl
stream CorrelatedEvents from SensorReading
    attention_window {
        duration: 30s,
        heads: 4,
        embedding: "rule_based",
        threshold: 0.7
    }
    where attention_score > 0.85
    emit alert("HighCorrelation", "Events highly correlated")
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `duration` | `30s` | Window duration |
| `heads` | `4` | Number of attention heads |
| `embedding` | `rule_based` | Embedding type: `rule_based`, `learned`, `composite` |
| `threshold` | `0.0` | Minimum attention score |
| `max_history` | `1000` | Maximum events to retain |

### Feature Configuration

```vpl
stream FraudDetection from Transaction
    attention_window {
        duration: 1m,
        heads: 8,
        embedding: "composite",
        features: {
            numeric: [
                { field: "amount", transform: "log_scale", weight: 2.0 },
                { field: "hour", transform: "cyclical", period: 24 }
            ],
            categorical: [
                { field: "merchant_type", method: "hash", dim: 16 },
                { field: "country", method: "one_hot", vocab: ["US", "UK", "EU", "OTHER"] }
            ]
        }
    }
    where attention_score > 0.9
    emit alert("SuspiciousPattern", "Transaction pattern detected")
```

### Numeric Transforms

| Transform | Description |
|-----------|-------------|
| `identity` | No transformation |
| `log_scale` | Logarithmic scaling for large values |
| `normalize` | Sigmoid normalization to [0, 1] |
| `zscore` | Z-score standardization |
| `cyclical` | Cyclical encoding (for time-of-day, etc.) |
| `bucketize` | Bucket into discrete ranges |

### Accessing Attention Scores

```vpl
stream Correlations from Event
    attention_window { duration: 1m, heads: 4 }
    select {
        current_event: event_type,
        score: attention_score,
        related_events: attention_matches
    }
    where score > 0.8
    emit log("Found {related_events.len()} related events with score {score}")
```

---

## Part 6: Joins

Join multiple event streams based on conditions.

### Basic Join

```vpl
stream EnrichedOrders = join(
    stream Orders from OrderEvent,
    stream Customers from CustomerEvent
        on Orders.customer_id == Customers.id
)
.window(5m)
.select {
    order_id: Orders.id,
    customer_name: Customers.name,
    order_total: Orders.total
}
```

### Multi-Stream Join

```vpl
stream FullOrderDetails = join(
    stream Orders from OrderEvent,
    stream Customers from CustomerEvent
        on Orders.customer_id == Customers.id,
    stream Products from ProductEvent
        on Orders.product_id == Products.id,
    stream Inventory from InventoryEvent
        on Orders.product_id == Inventory.product_id
)
.window(10m)
.select {
    order_id: Orders.id,
    customer: Customers.name,
    product: Products.name,
    in_stock: Inventory.quantity > 0
}
```

### Join with Aggregation

```vpl
stream CustomerStats = join(
    stream Orders from OrderEvent,
    stream Customers from CustomerEvent
        on Orders.customer_id == Customers.id
)
.window(1h)
.aggregate {
    customer: Customers.name,
    order_count: count(),
    total_spent: sum(Orders.amount),
    avg_order: avg(Orders.amount)
}
```

### Merge Streams

Combine multiple streams of the same type:

```vpl
stream AllSensors = merge(
    stream Zone1 from SensorReading where zone == "1",
    stream Zone2 from SensorReading where zone == "2",
    stream Zone3 from SensorReading where zone == "3"
)
.window(1m)
.aggregate {
    total_sensors: count(),
    avg_value: avg(value)
}
```

---

## Best Practices

### 1. Start Simple

Begin with basic filters before adding windows and patterns:

```vpl
// Step 1: Basic filter
stream HighTemps from TemperatureReading
    where temperature > 100

// Step 2: Add window
stream HighTempMinutes from TemperatureReading
    where temperature > 100
    window tumbling 1m
    aggregate { count: count() }

// Step 3: Add alert
stream HighTempAlerts from TemperatureReading
    where temperature > 100
    window tumbling 1m
    aggregate { count: count() }
    where count > 5
    emit alert("SustainedHighTemp", "5+ high readings in 1 minute")
```

### 2. Use Partitioning for Scale

```vpl
// Process per-device independently
stream DeviceAlerts from SensorReading
    partition by device_id
    window tumbling 1m
    aggregate { avg_val: avg(value) }
    where avg_val > threshold
```

### 3. Set Appropriate Timeouts

```vpl
// Don't wait forever for patterns
pattern QuickMatch = A -> B -> C within 5m

// Different timeouts for different patterns
pattern SlowProcess = Start -> Middle within 1h -> End within 10m
```

### 4. Use Aliases for Clarity

```vpl
pattern ClearPattern =
    LoginFailed[user_id == "admin"] as failed_login
    -> LoginSuccess[user_id == failed_login.user_id] as success
    within 10m
```

### 5. Test with Simulation

```bash
# Always test with event files first
varpulis check program.vpl
varpulis simulate -p program.vpl -e test_events.evt --verbose
```

---

## Quick Reference

### Stream Operations

| Operation | Syntax | Description |
|-----------|--------|-------------|
| Filter | `where condition` | Filter events |
| Select | `select { fields }` | Transform/project fields |
| Window | `window tumbling Xm` | Time-based window |
| Window | `window count N` | Count-based window |
| Aggregate | `aggregate { funcs }` | Compute aggregations |
| Pattern | `pattern A -> B` | Sequence detection |
| Partition | `partition by field` | Process per-key |
| Emit | `emit alert(...)` | Output alert |
| Emit | `emit log(...)` | Output log message |

### Pattern Operators

| Operator | Meaning | Example |
|----------|---------|---------|
| `->` | Followed by | `A -> B -> C` |
| `+` | One or more | `A+` |
| `*` | Zero or more | `A*` |
| `NOT` | Absence | `NOT(A)` |
| `AND` | Both (any order) | `AND(A, B)` |
| `OR` | Either | `OR(A, B)` |
| `within` | Time constraint | `within 5m` |

### Duration Units

| Unit | Meaning | Example |
|------|---------|---------|
| `s` | Seconds | `30s` |
| `m` | Minutes | `5m` |
| `h` | Hours | `1h` |
| `d` | Days | `1d` |

---

## Next Steps

- [CLI Reference](../reference/cli-reference.md) - All command options
- [Windows & Aggregations Reference](../reference/windows-aggregations.md) - Detailed window documentation
- [SASE+ Pattern Guide](../guides/sase-patterns.md) - Advanced pattern matching
- [Configuration Guide](../guides/configuration.md) - MQTT, deployment, and more
