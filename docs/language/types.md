# Système de types

## Types primitifs

| Type | Description | Exemples |
|------|-------------|----------|
| `int` | Entier signé 64 bits | `42`, `-100`, `0` |
| `float` | Flottant 64 bits (IEEE 754) | `3.14`, `-0.5`, `1e10` |
| `bool` | Booléen | `true`, `false` |
| `str` | Chaîne de caractères UTF-8 | `"hello"`, `'world'` |
| `timestamp` | Point dans le temps (ns precision) | `@2026-01-23T10:00:00Z` |
| `duration` | Durée temporelle | `5s`, `10m`, `1h`, `2d` |
| `null` | Absence de valeur | `null` |

## Types composés

### Arrays

```varpulis
let numbers: [int] = [1, 2, 3, 4, 5]
let names: [str] = ["alice", "bob"]
let empty: [float] = []
```

### Maps (dictionnaires)

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

## Types optionnels

```varpulis
let maybe_value: int? = null
let definitely: int = 42

# Opérateur de coalescence
let result = maybe_value ?? 0  # 0 si null

# Accès sécurisé
let name = user?.profile?.name ?? "unknown"
```

## Types d'événements

### Déclaration

```varpulis
event TradeEvent:
    symbol: str
    price: float
    volume: int
    timestamp: timestamp
    exchange: str?  # optionnel

event SensorReading:
    sensor_id: str
    value: float
    unit: str
    location: (float, float)  # lat, lon
```

### Héritage d'événements

```varpulis
event BaseEvent:
    id: str
    timestamp: timestamp

event OrderEvent extends BaseEvent:
    customer_id: str
    items: [{product_id: str, quantity: int}]
    total: float
```

## Types de streams

```varpulis
# Stream typé explicitement
stream Trades: Stream<TradeEvent> from TradeEvent

# Inférence de type
stream HighValue = Trades.where(price > 1000)  # Stream<TradeEvent>

# Stream transformé
stream Summary = Trades
    .window(1m)
    .aggregate(
        avg_price: avg(price),
        total_volume: sum(volume)
    )  # Stream<{avg_price: float, total_volume: int}>
```

## Inférence de types

Le compilateur infère automatiquement les types quand possible :

```varpulis
let x = 42          # int
let y = 3.14        # float
let z = "hello"     # str
let items = [1, 2]  # [int]

stream Result = Input
    .map(e => e.value * 2)  # type de retour inféré
```

## Conversions de types

```varpulis
# Conversions explicites
let i: int = int("42")
let f: float = float(42)
let s: str = str(3.14)

# Parse avec gestion d'erreur
let maybe_int: int? = try_int("not a number")  # null
```

## Alias de types

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
