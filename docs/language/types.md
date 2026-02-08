# Type System

## Primitive Types

| Type | Description | Examples |
|------|-------------|----------|
| `int` | Signed 64-bit integer | `42`, `-100`, `0` |
| `float` | 64-bit floating point (IEEE 754) | `3.14`, `-0.5`, `1e10` |
| `bool` | Boolean | `true`, `false` |
| `str` | UTF-8 string | `"hello"`, `'world'` |
| `timestamp` | Point in time (ns precision) | `@2026-01-23T10:00:00Z` |
| `duration` | Time duration | `5s`, `10m`, `1h`, `2d` |
| `null` | Absence of value | `null` |

## Composite Types

### Arrays

```varpulis
let numbers: [int] = [1, 2, 3, 4, 5]
let names: [str] = ["alice", "bob"]
let empty: [float] = []
```

### Maps (dictionaries)

```varpulis
let config: {str: int} = {
    "timeout": 30,
    "retries": 3
}

let nested: {str: {str: int}} = {
    "server1": {"port": 8080, "threads": 4},
    "server2": {"port": 8081, "threads": 8}
}
```

### Tuples

```varpulis
let point: (float, float) = (3.14, 2.71)
let record: (str, int, bool) = ("alice", 30, true)
```

## Optional Types

```varpulis
let maybe_value: int? = null
let definitely: int = 42

# Coalesce operator
let result = maybe_value ?? 0  # 0 if null

# Safe access
let name = user?.profile?.name ?? "unknown"
```

## Event Types

### Declaration

```varpulis
event TradeEvent:
    symbol: str
    price: float
    volume: int
    timestamp: timestamp
    exchange: str?  # optional

event SensorReading:
    sensor_id: str
    value: float
    unit: str
    location: (float, float)  # lat, lon
```

### Event Inheritance

```varpulis
event BaseEvent:
    id: str
    timestamp: timestamp

event OrderEvent extends BaseEvent:
    customer_id: str
    items: [{product_id: str, quantity: int}]
    total: float
```

## Stream Types

```varpulis
# Explicitly typed stream
stream Trades: Stream<TradeEvent> = TradeEvent

# Type inference
stream HighValue = Trades.where(price > 1000)  # Stream<TradeEvent>

# Transformed stream
stream Summary = Trades
    .window(1m)
    .aggregate(
        avg_price: avg(price),
        total_volume: sum(volume)
    )  # Stream<{avg_price: float, total_volume: int}>
```

## Type Inference

The compiler automatically infers types when possible:

```varpulis
let x = 42          # int
let y = 3.14        # float
let z = "hello"     # str
let items = [1, 2]  # [int]

stream Result = Input
    .select(doubled: value * 2)  # return type inferred
```

## Type Conversions

```varpulis
# Explicit conversions
let i: int = int("42")
let f: float = float(42)
let s: str = str(3.14)

# Parse with error handling
let maybe_int: int? = try_int("not a number")  # null
```

## Type Aliases

```varpulis
type UserId = str
type Price = float
type Coordinates = (float, float)
type EventBatch = [TradeEvent]

event Order:
    user: UserId
    amount: Price
    location: Coordinates
```
