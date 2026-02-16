# VPL Syntax

## Comments

```varpulis
# Single line comment

/* 
   Multi-line
   comment
*/
```

## Variable Declaration

```varpulis
# Immutable (recommended)
let name = "value"
let count: int = 42

# Mutable
var counter = 0
counter := counter + 1

# Global constant
const MAX_RETRIES = 3
const API_URL = "https://api.example.com"
```

## Stream Declaration

### Simple Stream

```varpulis
# From an event source
stream Trades = TradeEvent

# With alias
stream T = Trades
```

### Stream with Filtering

```varpulis
stream HighValueTrades = Trades
    .where(price > 10000)

# Multiple conditions
stream FilteredTrades = Trades
    .where(price > 1000 and volume > 100)
    .where(exchange == "NYSE" or exchange == "NASDAQ")
```

### Stream with Projection

```varpulis
stream SimpleTrades = Trades
    .select(
        symbol,
        price,
        total: price * volume
    )
```

### Stream with Temporal Window

```varpulis
# Tumbling window (5 minutes)
stream WindowedTrades = Trades
    .window(5m)
    .aggregate(
        count: count(),
        avg_price: avg(price)
    )

# Sliding window (5 min, slide 1 min)
stream SlidingMetrics = Trades
    .window(5m, sliding: 1m)
    .aggregate(sum(volume))
```

## Multi-stream Aggregation

```varpulis
stream BuildingMetrics = merge(
    stream S1 = SensorEvent .where(sensor_id == "S1"),
    stream S2 = SensorEvent .where(sensor_id == "S2"),
    stream S3 = SensorEvent .where(sensor_id == "S3")
)
.window(1m, sliding: 10s)
.aggregate(
    avg_temp: avg(temperature),
    min_temp: min(temperature),
    max_temp: max(temperature),
    sensor_count: count(distinct(sensor_id))
)
```

## Joins

```varpulis
stream EnrichedOrders = join(
    stream Orders = OrderEvent,
    stream Customers = CustomerEvent
        on Orders.customer_id == Customers.id,
    stream Inventory = InventoryEvent
        on Orders.product_id == Inventory.product_id
)
.window(5m, policy: "watermark")
.emit(
    order_id: Orders.id,
    customer_name: Customers.name,
    stock: Inventory.quantity
)
```

## Contexts (Multi-Threaded Execution)

Contexts declare isolated execution domains, each running on a dedicated OS thread.

### Context Declaration

```varpulis
# Basic context
context ingestion

# With CPU affinity (Linux)
context analytics (cores: [2, 3])
context alerts (cores: [4])
```

### Assigning Streams to Contexts

```varpulis
stream FastFilter = RawEvents
    .context(ingestion)
    .where(value > 0)
    .emit(sensor_id: sensor_id, value: value)
```

### Cross-Context Emit

```varpulis
# Send events to a different context
stream Processed = RawEvents
    .context(ingestion)
    .where(priority > 5)
    .emit(context: analytics, data: data)
```

When no contexts are declared, the engine runs in single-threaded mode with zero overhead.

See the [Contexts Guide](../guides/contexts.md) for a full tutorial.

## Parallelization

Use contexts for parallel execution across CPU cores:

```varpulis
context fast_lane (cores: [0, 1])
context analytics (cores: [2, 3])

stream OrderProcessing = Orders
    .context(fast_lane)
    .partition_by(customer_id)
    .where(quantity > 0 and price > 0)
    .emit(order_id: id, customer: customer_id, total: price * quantity)
```

## Functions

```varpulis
fn calculate_total(price: float, quantity: int) -> float:
    return price * quantity

fn is_valid_order(order: OrderEvent) -> bool:
    return order.quantity > 0 and order.price > 0

# Inline function (lambda)
let double = x => x * 2
let add = (a, b) => a + b
```

## Control Structures

### Conditions

```varpulis
if price > 1000:
    category = "high"
elif price > 100:
    category = "medium"
else:
    category = "low"

# Ternary expression
let status = if active then "enabled" else "disabled"
```

### Loops

```varpulis
for item in items:
    process(item)

for i in 0..10:
    print(i)

while condition:
    do_something()
    if should_stop:
        break
```

## Connectors

### MQTT Connector

The MQTT connector allows Varpulis to receive events from and send alerts to an MQTT broker. MQTT support requires the `mqtt` feature flag.

```varpulis
# Connector declaration - place at the top of your VPL file
connector MqttBroker = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "varpulis-app"
)

# Bind a stream to receive events from the connector
stream Events = SensorReading
    .from(MqttBroker, topic: "events/#")
```

The MQTT connector automatically:
- Subscribes to the specified topic to receive events
- Publishes stream `.emit()` results via `.to(Connector)`
- Handles reconnection on connection loss

> **Note**: The `config mqtt { }` block syntax is deprecated. Use `connector` declarations with `.from()` and `.to()` instead. See [Connectors](connectors.md).

**Example with streams:**
```varpulis
connector MqttBroker = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "fraud-detector"
)

event Transaction:
    user_id: str
    amount: float
    status: str

# Alerts will be published to "alerts/fraud"
stream FraudAlert = Transaction
    .where(amount > 10000 and status == "pending")
    .emit(
        alert_type: "high_value_transaction",
        user_id: user_id,
        amount: amount
    )
```

### HTTP Connector

```varpulis
# HTTP webhook output — declare the connector first
connector AlertWebhook = http (
    url: "http://webhook.example.com/alerts",
    method: "POST"
)

stream Alerts = DetectedPatterns
    .emit(severity: "high")
    .to(AlertWebhook)
```

### Kafka Connector

Kafka is available with the `kafka` feature flag. Use connector declarations:

```varpulis
connector KafkaBroker = kafka (
    brokers: ["broker:9092"],
    group_id: "varpulis-consumer"
)

stream Output = Processed
    .emit(result: value)
    .to(KafkaBroker)
```

## Pattern Forecasting

Use `.forecast()` after a sequence pattern to predict completion probability:

```varpulis
# Zero-config — uses balanced defaults with adaptive warmup
stream SimpleForecast = EventA as a -> EventB as b
    .within(5m)
    .forecast()
    .where(forecast_confidence > 0.8)
    .emit(prob: forecast_probability)

# Fully configured
stream FraudForecast = Transaction as t1
    -> Transaction as t2 where t2.amount > t1.amount * 5
    -> Transaction as t3 where t3.location != t1.location
    .within(5m)
    .forecast(mode: "accurate", confidence: 0.7)
    .where(forecast_confidence > 0.8 and forecast_probability > 0.7)
    .emit(
        probability: forecast_probability,
        stability: forecast_confidence,
        expected_time: forecast_time,
        state: forecast_state,
        confidence_lower: forecast_lower,
        confidence_upper: forecast_upper
    )
```

### Forecast Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `mode` | `str` | `"balanced"` | Preset: `"fast"`, `"accurate"`, or `"balanced"` |
| `confidence` | `float` | 0.5 | Minimum probability threshold to emit forecast |
| `horizon` | `duration` | `within` duration | Forecast time window |
| `warmup` | `int` | 100 | Min events before forecasting starts |
| `max_depth` | `int` | 3 | PST context depth |
| `hawkes` | `bool` | `true` | Enable Hawkes intensity modulation |
| `conformal` | `bool` | `true` | Enable conformal prediction intervals |

Mode presets provide convenient defaults (explicit params override):

| Mode | `warmup` | `max_depth` | `hawkes` | `conformal` | Adaptive warmup |
|------|----------|-------------|----------|-------------|-----------------|
| `"fast"` | 50 | 3 | off | off | off |
| `"accurate"` | 200 | 5 | on | on | on |
| `"balanced"` | 100 | 3 | on | on | on |

### Forecast Built-in Variables

Available in `.where()` and `.emit()` after `.forecast()`:

| Variable | Type | Description |
|----------|------|-------------|
| `forecast_probability` | `float` | Pattern completion probability (0.0–1.0) |
| `forecast_confidence` | `float` | Prediction stability (0.0–1.0) — high = converged |
| `forecast_time` | `int` | Expected time to completion (nanoseconds) |
| `forecast_state` | `str` | Current NFA state label |
| `forecast_context_depth` | `int` | PST context depth used for prediction |
| `forecast_lower` | `float` | Lower bound of conformal prediction interval (0.0–1.0) |
| `forecast_upper` | `float` | Upper bound of conformal prediction interval (0.0–1.0) |

## Output Routing

Use `.to()` to route stream output to declared connectors:

```varpulis
connector AlertKafka = kafka (
    brokers: ["broker:9092"],
    topic: "alerts"
)

stream Alerts = DetectedPatterns
    .emit(
        alert_id: uuid(),
        severity: "high",
        timestamp: now()
    )
    .to(AlertKafka)
```
