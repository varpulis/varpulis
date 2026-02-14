# VPL Language Reference

VPL (Varpulis Processing Language) is a declarative stream processing language for complex event processing (CEP). Everything is a stream.

## Quick Start

```vpl
stream Trades = TradeEvent
stream HighValue = Trades .where(price > 10000)
```

## Stream Declaration

```vpl
stream Name = EventType
stream Filtered = Source .where(condition)
stream Projected = Source .select(field1, field2, computed: expr)
```

## Filtering (.where)

```vpl
stream Expensive = Trades .where(price > 10000)
stream Combined = Trades .where(price > 1000 and volume > 100)
```

## Projection (.select)

```vpl
stream Simple = Trades .select(symbol, price, total: price * volume)
```

## Windows and Aggregation

```vpl
# Tumbling window
stream Stats = Trades .window(5m) .aggregate(count: count(), avg_price: avg(price))

# Sliding window
stream Sliding = Trades .window(5m, sliding: 1m) .aggregate(sum(volume))
```

## Aggregation Functions

| Function | Description |
|----------|-------------|
| `count()` | Number of elements |
| `sum(expr)` | Sum of values |
| `avg(expr)` | Average |
| `min(expr)` | Minimum |
| `max(expr)` | Maximum |
| `first(expr)` | First in window |
| `last(expr)` | Last in window |
| `stddev(expr)` | Standard deviation |
| `count_distinct(expr)` | Distinct count |
| `ema(expr, period)` | Exponential moving average |

## Partitioning

```vpl
stream PerSymbol = Trades .partition_by(symbol) .window(1m) .aggregate(total: sum(volume))
```

## Joins

```vpl
stream Enriched = join(
    stream Orders = OrderEvent,
    stream Customers = CustomerEvent on Orders.customer_id == Customers.id
) .window(5m) .emit(order_id: Orders.id, name: Customers.name)
```

## Merge

```vpl
stream All = merge(
    stream S1 = SensorEvent .where(sensor_id == "S1"),
    stream S2 = SensorEvent .where(sensor_id == "S2")
) .window(1m) .aggregate(avg_temp: avg(temperature))
```

## Sequence Patterns (CEP)

```vpl
stream Suspicious = Login as login
    -> Transaction where user_id == login.user_id and status == "failed" as tx
    .within(10m)
    .emit(user_id: login.user_id, amount: tx.amount)
```

## Event Type Declaration

```vpl
event TradeEvent:
    symbol: str
    price: float
    volume: int
    timestamp: timestamp
    exchange: str?
```

## Connectors

### MQTT
```vpl
connector MqttBroker = mqtt(host: "localhost", port: 1883, client_id: "app")
stream Events = SensorReading .from(MqttBroker, topic: "events/#")
```

### Kafka
```vpl
connector KafkaBroker = kafka(brokers: ["broker:9092"], group_id: "consumer")
stream Input = EventType .from(KafkaBroker, topic: "events")
stream Output = Processed .to(KafkaBroker)
```

### HTTP (output only)
```vpl
connector Webhook = http(url: "https://hooks.example.com/alerts")
stream Alerts = Critical .to(Webhook)
```

## Output (.emit and .to)

```vpl
stream Alerts = Source
    .where(severity == "high")
    .emit(alert_type: "critical", value: value)
    .to(KafkaOutput)
```

## Pattern Forecasting (.forecast)

Predict whether a partially-matched sequence pattern will complete:

```vpl
stream Forecast = EventA as a
    -> EventB where value > a.value as b
    .within(5m)
    .forecast(confidence: 0.7, horizon: 2m, warmup: 500, max_depth: 5)
    .where(forecast_probability > 0.8)
    .emit(probability: forecast_probability, expected_time: forecast_time)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `confidence` | 0.5 | Min probability to emit forecast |
| `horizon` | within duration | Forecast time window |
| `warmup` | 100 | Events before forecasting starts |
| `max_depth` | 5 | PST context depth |

Built-in variables after `.forecast()`: `forecast_probability`, `forecast_time`, `forecast_state`, `forecast_context_depth`

## Contexts (Multi-Threading)

```vpl
context ingestion (cores: [0, 1])
context analytics (cores: [2, 3])

stream Fast = RawEvents .context(ingestion) .where(value > 0)
```

## Primitive Types

| Type | Examples |
|------|----------|
| `int` | `42`, `-100` |
| `float` | `3.14`, `1e10` |
| `bool` | `true`, `false` |
| `str` | `"hello"` |
| `timestamp` | `@2026-01-23T10:00:00Z` |
| `duration` | `5s`, `10m`, `1h`, `2d` |
| `null` | `null` |

## Duration Suffixes

`ms` (milliseconds), `s` (seconds), `m` (minutes), `h` (hours), `d` (days)

## Logical Operators

`and`, `or`, `not`, `in`, `not in`

## Comparison Operators

`==`, `!=`, `<`, `<=`, `>`, `>=`
