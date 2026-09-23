# VPL Language Tutorial

A comprehensive guide to writing VPL programs, from basic event processing to advanced pattern matching.

## Table of Contents

1. [Part 1: Basics](#part-1-basics) - Events, streams, filters, emit
2. [Part 2: Windows and Aggregations](#part-2-windows-and-aggregations)
3. [Part 3: Sequence Patterns](#part-3-sequence-patterns)
4. [Part 4: SASE+ Advanced](#part-4-sase-advanced) - Kleene, negation, AND/OR
5. [Part 5: Joins](#part-5-joins)
6. [Part 6: Contexts](#part-6-contexts) - Multi-threaded execution with CPU affinity

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
# Basic stream: listen for all TemperatureReading events
stream Temperatures = TemperatureReading

# Stream with alias
stream T = Temperatures
```

### Filtering with `.where()`

Filter events based on conditions:

```vpl
# Single condition
stream HighTemps = TemperatureReading
    .where(temperature > 100)

# Multiple conditions (AND)
stream CriticalTemps = TemperatureReading
    .where(temperature > 100 and sensor_id == "critical-zone")

# OR conditions
stream AlertZones = TemperatureReading
    .where(sensor_id == "zone-1" or sensor_id == "zone-2")

# Compound conditions
stream Filtered = TemperatureReading
    .where((temperature > 90 and humidity > 80) or emergency == true)
```

**Comparison operators**: `==`, `!=`, `<`, `<=`, `>`, `>=`

**Logical operators**: `and`, `or`, `not`

### Selecting Fields with `.select()`

Transform events by selecting specific fields or computing new ones:

```vpl
stream SimplifiedTemps = TemperatureReading
    .select(
        sensor: sensor_id,
        temp: temperature,
        is_high: temperature > 80
    )

# Computed fields
stream EnhancedTemps = TemperatureReading
    .select(
        sensor_id,
        temp_celsius: (temperature - 32) * 5 / 9,
        reading_time: timestamp
    )
```

### Emitting Alerts and Logs

Use `.emit()` and `.print()` to output data when conditions are met:

```vpl
# Emit an alert
stream TempAlerts = TemperatureReading
    .where(temperature > 100)
    .emit(alert_type: "HighTemperature", sensor_id: sensor_id, temperature: temperature)

# Print to log
stream TempLog = TemperatureReading
    .print("Received:", sensor_id, temperature)

# Emit with severity
stream CriticalAlerts = TemperatureReading
    .where(temperature > 150)
    .emit(alert_type: "CriticalTemperature", sensor_id: sensor_id, temperature: temperature, severity: "critical")
```

Neither `.emit()` nor `.print()` fills in `{sensor_id}` inside a string: the
braces come out as written. Emit values as fields, and give `.print()` the
values as arguments, as here.

### Variables and Constants

```vpl
# Immutable variable
let threshold = 100
let sensor_name = "main-sensor"

# Mutable variable
var counter = 0

# Constants (compile-time)
const MAX_TEMP = 200
const API_KEY = "secret123"

# Use in streams
stream Alerts = TemperatureReading
    .where(temperature > threshold)
    .emit(alert_type: "High", temperature: temperature)
```

A top-level `let` or `const` whose value is a literal is what a stream reads
under that name, even when an event carries a field called `threshold`. A
`var` is not visible to streams: its value can change, so
`.where(temperature > counter)` reads the event's `counter` field.

### Comments

```vpl
# Single-line comment

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
# 1-minute tumbling window
stream MinuteStats = TemperatureReading
    .window(1m)
    .aggregate(
        avg_temp: avg(temperature),
        max_temp: max(temperature),
        count: count()
    )

# 5-second window
stream RapidStats = SensorReading
    .window(5s)
    .aggregate(readings: count())

# 1-hour window
stream HourlyReport = Transaction
    .window(1h)
    .aggregate(
        total: sum(amount),
        avg_amount: avg(amount)
    )
```

**Duration units**: `s` (seconds), `m` (minutes), `h` (hours), `d` (days)

### Sliding Windows

Overlapping windows with a slide interval:

```vpl
# 5-minute window, slides every 1 minute
stream SlidingAvg = TemperatureReading
    .window(5m, sliding: 1m)
    .aggregate(
        rolling_avg: avg(temperature)
    )

# 10-second window, slides every 2 seconds
stream RecentTrend = SensorReading
    .window(10s, sliding: 2s)
    .aggregate(
        recent_max: max(value),
        recent_min: min(value)
    )
```

### Count-Based Windows

Windows based on event count:

```vpl
# Every 100 events
stream BatchStats = Transaction
    .window(100)
    .aggregate(
        batch_total: sum(amount),
        batch_avg: avg(amount)
    )

# Sliding count window: 50 events, slide by 10
stream RollingBatch = Reading
    .window(50, sliding: 10)
    .aggregate(rolling_sum: sum(value))
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
| `percentile(field, q)` | Percentile (0.0-1.0) | `percentile(latency, 0.95)` |
| `median(field)` | Median (50th percentile) | `median(price)` |
| `p50(field)` | 50th percentile shorthand | `p50(latency)` |
| `p95(field)` | 95th percentile shorthand | `p95(latency)` |
| `p99(field)` | 99th percentile shorthand | `p99(latency)` |
| `collect(field)` | Collect values into array | `collect(sensor_id)` |
| `first(field)` | First value in window | `first(timestamp)` |
| `last(field)` | Last value in window | `last(value)` |

### Partitioned Windows

Windows partitioned by a key:

```vpl
# Per-sensor statistics
stream PerSensorStats = TemperatureReading
    .partition_by(sensor_id)
    .window(1m)
    .aggregate(
        sensor: last(sensor_id),
        avg_temp: avg(temperature),
        readings: count()
    )

# Per-customer totals
stream CustomerTotals = Transaction
    .partition_by(customer_id)
    .window(1h)
    .aggregate(
        customer: last(customer_id),
        hourly_spend: sum(amount)
    )
```

### Percentile Aggregations

Compute percentiles for latency monitoring, SLA tracking, and distribution analysis:

```vpl
# Latency monitoring with percentile aggregations
stream LatencyStats = RequestEvent
    .window(1m)
    .aggregate(
        median_ms: median(latency_ms),
        p50: p50(latency_ms),
        p95: p95(latency_ms),
        p99: p99(latency_ms),
        p999: percentile(latency_ms, 0.999)
    )
```

The `percentile(field, q)` function takes a quantile between 0.0 and 1.0. The `median`, `p50`, `p95`, and `p99` functions are convenient shorthands.

### Filtering After Aggregation

Apply conditions to aggregated results:

```vpl
stream HighVolumeMinutes = Transaction
    .window(1m)
    .aggregate(
        total: sum(amount),
        count: count()
    )
    .having(total > 10000 or count > 100)
    .emit(alert_type: "HighVolume", transactions: count, total: total)
```

---

## Part 3: Sequence Patterns

Sequence patterns detect events occurring in a specific order using the `->` operator.

### Basic Sequences

```vpl
# A followed by B
pattern LoginLogout = Login -> Logout

# A followed by B followed by C
pattern ThreeStep = Start -> Process -> Complete

# Inline sequence in a stream
stream Sessions = Login as l -> Logout where user_id == l.user_id
    .within(1h)
    .emit(user_id: l.user_id, message: "Session: user logged in and out")
```

### Sequences with Conditions

```vpl
# Events must match conditions: `where` follows the event type
pattern FailedLogin =
    LoginAttempt where status == "failed" as first
    -> LoginAttempt where status == "failed" and user_id == first.user_id as second
    -> LoginAttempt where status == "failed" and user_id == first.user_id as third
    within 5m

stream BruteForceDetection = LoginAttempt where status == "failed" as first
    -> LoginAttempt where status == "failed" and user_id == first.user_id as second
    -> LoginAttempt where status == "failed" and user_id == first.user_id as third
    .within(5m)
    .emit(alert_type: "BruteForce", user_id: first.user_id, attempts: 3)
```

`.emit()` does not fill in `{first.user_id}` inside a string; emit the value as
a field of its own, as here.

### Referencing Previous Events

Use aliases to reference earlier events in the sequence:

```vpl
pattern PriceSpike =
    Trade as t1
    -> Trade where symbol == t1.symbol and price > t1.price * 1.1 as t2
    within 1m

stream Spikes = Trade as t1
    -> Trade where symbol == t1.symbol and price > t1.price * 1.1 as t2
    .within(1m)
    .emit(alert_type: "PriceSpike", symbol: t1.symbol, from_price: t1.price, to_price: t2.price)
```

AAPL at 100, 105, then 112 gives one spike, from 100 to 112: 112 is not 10%
above 105.

### Temporal Constraints

Constrain how quickly events must occur:

```vpl
# Must complete within 5 minutes
pattern QuickCheckout =
    CartAdd -> PaymentStart -> PaymentComplete
    within 5m
```

`within` bounds the whole sequence, counted from its first event. A deadline
per step is not available.

---

## Part 4: SASE+ Advanced

SASE+ extends basic patterns with Kleene closures and negation. VPL writes a
closure `all X` and an absence `-> NOT X`; there is no `X+`, `X*`, `AND(...)` or
`OR(...)` syntax, and the sections below show what to write instead.

### Kleene Plus (`all`) - One or More

```vpl
# One or more failed logins followed by success
pattern BruteForceSuccess =
    all LoginFailed as fails
    -> LoginSuccess as success
    within 10m
    partition by user_id

stream Attacks = BruteForceSuccess
    .longest()
    .emit(alert_type: "BruteForce", user: success.user_id, failures: count(fails))
```

Matches come when the success arrives, and none come if it never does. With
`.longest()` there is one per run, carrying the run's failures: each failure can
start a run, so two failures then a success give two alerts, with 2 and 1
failures. Under the default `.each()` the success brings one match per failure
in each run instead. Add `.stnm()` before `.longest()` for one alert per attack:
a failure that a run takes then opens no run of its own.

### Zero or More

There is no `X*`. A step skips the events before it, so `SessionStart ->
SessionEnd` matches a session whether or not activity came in between; add
`-> all Activity as acts` when you need that activity, and then at least one is
required.

```vpl
# Start then end, whatever came in between
pattern FullSession =
    SessionStart as start
    -> SessionEnd where user_id == start.user_id
    within 1h
```

### Negation (`NOT`) - Absence of Event

The negated step is `-> NOT X`, in a `pattern`, with its own `where`:

```vpl
# Payment started but not completed
pattern AbandonedPayment =
    PaymentStart as p
    -> NOT PaymentComplete where payment_id == p.payment_id
    within 5m
    partition by payment_id

stream Abandoned = AbandonedPayment
    .emit(alert_type: "Abandoned", payment_id: p.payment_id)

# Order without confirmation
pattern UnconfirmedOrder =
    OrderPlaced as o
    -> NOT OrderConfirmed where order_id == o.order_id
    within 1h
    partition by order_id
```

The alert comes out when a later event moves the stream's time past the
deadline, not on a wall clock; see
[Absence](../language/operators.md#absence-not-b).

### Both, in Any Order

There is no `AND(A, B)`: write one sequence per order.

```vpl
stream UploadThenSign = DocumentUploaded as d
    -> SignatureProvided where doc_id == d.doc_id as s
    .within(1h)
    .emit(doc_id: d.doc_id)

stream SignThenUpload = SignatureProvided as s
    -> DocumentUploaded where doc_id == s.doc_id as d
    .within(1h)
    .emit(doc_id: s.doc_id)
```

### Either Event

There is no `OR(A, B)`: merge the two event types into one stream.

```vpl
# Either payment method
stream Payments = merge(CreditCardPayment, BankTransfer)
    .emit(order_id: order_id, amount: amount)
```

### Combinations

A sequence that must not see an event in between uses `.not()`, which cancels a
run when that event arrives:

```vpl
# Order placed, items added, then shipped, with no cancellation in between
stream SuccessfulOrder = OrderPlaced as o
    -> all ItemAdded where order_id == o.order_id as items
    -> OrderShipped where order_id == o.order_id as shipped
    .within(24h)
    .not(OrderCancelled where order_id == o.order_id)
    .longest()
    .emit(order_id: o.order_id, items: count(items))
```

An order placed, given two items and shipped raises one alert with `items: 2`;
the same order cancelled before shipping raises none.

A step that accepts either of two outcomes is written once per outcome:

```vpl
# Multiple failures, then a success or a lockout
pattern FailedThenSuccess =
    all LoginFailed as fails
    -> LoginSuccess as outcome
    within 15m
    partition by user_id

pattern FailedThenLocked =
    all LoginFailed as fails
    -> AccountLocked as outcome
    within 15m
    partition by user_id
```

### Partition-By for Patterns

Process patterns independently per partition key:

```vpl
# Per-user pattern matching: Kleene+ uses `all` in arrow syntax
pattern UserFailures = LoginFailed as first
    -> all LoginFailed as fails
    -> LoginSuccess as success
    within 10m partition by user_id

stream UserAttacks = UserFailures
    .emit(alert_type: "UserBruteForce", user: first.user_id, failures: count(fails) + 1)
```

---

## Part 5: Joins

Join multiple event streams based on conditions.

> **Joins do not run in the engine as it ships.** They parse and pass
> `varpulis check`, but `varpulis simulate` and a Vejas detect unit run the
> engine's synchronous path, which refuses a join when it loads the program:
> `stream 'X' is a join, which the synchronous execution path does not
> implement`. No shipped command runs the asynchronous path since the platform
> was retired ([ADR-008](../adr/008-engine-only-platform-retired.md)). To correlate two event types today, use a
> sequence, which runs everywhere: `A as a -> B where key == a.key as b` with
> `.within(5m)`.

### Basic Join

```vpl
stream EnrichedOrders = join(
    stream Orders = OrderEvent,
    stream Customers = CustomerEvent.on(Orders.customer_id == Customers.id)
)
.window(5m)
.select(
    order_id: Orders.id,
    customer_name: Customers.name,
    order_total: Orders.total
)
```

### Multi-Stream Join

```vpl
stream FullOrderDetails = join(
    stream Orders = OrderEvent,
    stream Customers = CustomerEvent.on(Orders.customer_id == Customers.id),
    stream Products = ProductEvent.on(Orders.product_id == Products.id),
    stream Inventory = InventoryEvent.on(Orders.product_id == Inventory.product_id)
)
.window(10m)
.select(
    order_id: Orders.id,
    customer: Customers.name,
    product: Products.name,
    in_stock: Inventory.quantity > 0
)
```

### Join with Aggregation

```vpl
stream CustomerStats = join(
    stream Orders = OrderEvent,
    stream Customers = CustomerEvent.on(Orders.customer_id == Customers.id)
)
.window(1h)
.aggregate(
    customer: Customers.name,
    order_count: count(),
    total_spent: sum(Orders.amount),
    avg_order: avg(Orders.amount)
)
```

### Merge Streams

Combine multiple streams of the same type:

```vpl
stream AllSensors = merge(
    stream Zone1 = SensorReading .where(zone == "1"),
    stream Zone2 = SensorReading .where(zone == "2"),
    stream Zone3 = SensorReading .where(zone == "3")
)
.window(1m)
.aggregate(
    total_sensors: count(),
    avg_value: avg(value)
)
```

---

## Part 6: Contexts

Contexts let you run streams on dedicated OS threads for true multi-core parallelism.

> **Contexts have no effect in the engine as it ships.** `context`
> declarations and `.context(...)` parse and pass `varpulis check`, but the
> threads and channels behind them belong to the engine's asynchronous runtime,
> which neither `varpulis simulate` nor a Vejas detect unit includes (see
> [ADR-008](../adr/008-engine-only-platform-retired.md)). A program with
> contexts runs on one thread, exactly like the same program without them. In
> Vejas, work runs in parallel as separate detect units.

### Declaring Contexts

```vpl
# Declare named execution contexts
context ingestion
context analytics (cores: [2, 3])
context alerts (cores: [4])
```

Each context gets its own OS thread with a single-threaded Tokio runtime. The optional `cores` parameter pins the thread to specific CPU cores (Linux only).

### Assigning Streams

Use `.context()` to assign a stream to a context:

```vpl
context fast (cores: [0])
context slow (cores: [1])

stream RawFilter = SensorReading
    .context(fast)
    .where(value > 0)
    .emit(sensor_id: sensor_id, value: value)

stream HeavyAnalytics = SensorReading
    .context(slow)
    .window(5m)
    .aggregate(avg: avg(value), stddev: stddev(value))
```

### Cross-Context Communication

Send events from one context to another using `context:` in `.emit()`:

```vpl
context ingest (cores: [0])
context analyze (cores: [1])

# Filter in the ingest context, forward to analyze
stream Filtered = RawEvent
    .context(ingest)
    .where(priority > 5)
    .emit(context: analyze, data: data, priority: priority)

# Aggregate in the analyze context
stream Stats = Filtered
    .context(analyze)
    .window(1m)
    .aggregate(count: count(), avg_priority: avg(priority))
```

Cross-context events are delivered via bounded `mpsc` channels.

### Backward Compatibility

Programs without `context` declarations run exactly as before -- single-threaded with zero overhead. Contexts are purely opt-in.

For a complete tutorial with a multi-stage IoT pipeline, see the [Contexts Guide](../guides/contexts.md).

---

## Best Practices

### 1. Start Simple

Begin with basic filters before adding windows and patterns:

```vpl
# Step 1: Basic filter
stream HighTemps = TemperatureReading
    .where(temperature > 100)

# Step 2: Add window
stream HighTempMinutes = TemperatureReading
    .where(temperature > 100)
    .window(1m)
    .aggregate(count: count())

# Step 3: Add alert
stream HighTempAlerts = TemperatureReading
    .where(temperature > 100)
    .window(1m)
    .aggregate(count: count())
    .having(count > 5)
    .emit(alert_type: "SustainedHighTemp", message: "5+ high readings in 1 minute")
```

### 2. Use Partitioning for Scale

```vpl
# Process per-device independently
stream DeviceAlerts = SensorReading
    .partition_by(device_id)
    .window(1m)
    .aggregate(avg_val: avg(value))
    .having(avg_val > 100)
```

### 3. Set Appropriate Timeouts

```vpl
# Don't wait forever for patterns
pattern QuickMatch = A -> B -> C within 5m

# Different timeouts for different patterns (one `within` per pattern)
pattern SlowProcess = Start -> Middle -> End within 1h
```

### 4. Use Aliases for Clarity

```vpl
pattern ClearPattern =
    LoginFailed where user_id == "admin" as failed_login
    -> LoginSuccess where user_id == failed_login.user_id as success
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
| Filter | `.where(condition)` | Filter events |
| Select | `.select(field: expr, ...)` | Transform/project fields |
| Window (tumbling) | `.window(1m)` | Time-based tumbling window |
| Window (sliding) | `.window(5m, sliding: 1m)` | Time-based sliding window |
| Window (count) | `.window(100)` | Count-based window |
| Aggregate | `.aggregate(name: func(), ...)` | Compute aggregations |
| Pattern | `pattern A -> B` | Sequence detection (declaration) |
| Partition | `.partition_by(field)` | Process per-key |
| Context | `.context(name)` | Assign to execution context |
| Emit | `.emit(field: value, ...)` | Output alert/event |
| Print | `.print("message")` | Output log message |
| Having | `.having(condition)` | Post-aggregation filter |

### Pattern Operators

| Operator | Meaning | Example |
|----------|---------|---------|
| `->` | Followed by | `A -> B -> C` |
| `all` | One or more | `A -> all B as bs -> C` |
| `-> NOT` | Absence before the deadline (in a `pattern`) | `A as a -> NOT B where id == a.id within 1h` |
| `.not()` | Cancel the run if this event arrives | `.not(B where id == a.id)` |
| `where` | Condition on a step | `A where amount > 100 as a` |
| `within` | Time constraint for the whole sequence | `within 5m` |

There is no `A+`, `A*`, `AND(A, B)` or `OR(A, B)`; see
[SASE+ Advanced](#part-4-sase-advanced) for what to write instead.

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
