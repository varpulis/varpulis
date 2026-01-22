# Syntaxe VarpulisQL

## Commentaires

```varpulis
# Commentaire sur une ligne

/* 
   Commentaire
   multi-lignes
*/
```

## Déclaration de variables

```varpulis
# Immutable (recommandé)
let name = "value"
let count: int = 42

# Mutable
var counter = 0
counter += 1

# Constante globale
const MAX_RETRIES = 3
const API_URL = "https://api.example.com"
```

## Déclaration de streams

### Stream simple

```varpulis
# Depuis une source d'événements
stream Trades from TradeEvent

# Avec alias
stream T = Trades
```

### Stream avec filtrage

```varpulis
stream HighValueTrades = Trades
    .where(price > 10000)

# Conditions multiples
stream FilteredTrades = Trades
    .where(price > 1000 and volume > 100)
    .where(exchange == "NYSE" or exchange == "NASDAQ")
```

### Stream avec projection

```varpulis
stream SimpleTrades = Trades
    .select(
        symbol,
        price,
        total: price * volume
    )
```

### Stream avec fenêtre temporelle

```varpulis
# Fenêtre tumbling (5 minutes)
stream WindowedTrades = Trades
    .window(5m)
    .aggregate(
        count: count(),
        avg_price: avg(price)
    )

# Fenêtre sliding (5 min, slide 1 min)
stream SlidingMetrics = Trades
    .window(5m, sliding: 1m)
    .aggregate(sum(volume))
```

## Agrégation multi-streams

```varpulis
stream BuildingMetrics = merge(
    stream S1 from SensorEvent where sensor_id == "S1",
    stream S2 from SensorEvent where sensor_id == "S2",
    stream S3 from SensorEvent where sensor_id == "S3"
)
.window(1m, sliding: 10s)
.aggregate(
    avg_temp: avg(temperature),
    min_temp: min(temperature),
    max_temp: max(temperature),
    sensor_count: count(distinct(sensor_id))
)
```

## Jointures

```varpulis
stream EnrichedOrders = join(
    stream Orders from OrderEvent,
    stream Customers from CustomerEvent
        on Orders.customer_id == Customers.id,
    stream Inventory from InventoryEvent
        on Orders.product_id == Inventory.product_id
)
.window(5m, policy: "watermark")
.emit(
    order_id: Orders.id,
    customer_name: Customers.name,
    stock: Inventory.quantity
)
```

## Pattern matching avec attention

```varpulis
stream FraudDetection = Trades
    .attention_window(
        duration: 30s,
        heads: 4,
        embedding: "rule_based"
    )
    .pattern(
        suspicious: events =>
            let high_value = events.filter(e => e.amount > 10000)
            let correlations = high_value
                .map(e1 => 
                    high_value
                        .filter(e2 => e2.id != e1.id)
                        .map(e2 => (e1, e2, attention_score(e1, e2)))
                )
                .flatten()
                .filter((e1, e2, score) => score > 0.85)
            
            correlations.len() > 3
    )
    .emit(
        alert_type: "attention_pattern_fraud",
        events: events
    )
```

## Parallélisation

```varpulis
stream OrderProcessing = Orders
    .partition_by(customer_id)
    .concurrent(
        max_workers: 8,
        strategy: "consistent_hash",
        supervision:
            restart: "always"
            max_restarts: 3
            backoff: "exponential"
    )
    .process(order =>
        validate_order(order)?
        check_inventory(order)?
        calculate_shipping(order)?
        Ok(order)
    )
    .on_error((error, order) =>
        log_error(error)
        emit_to_dlq(order)
    )
    .collect()
```

## Fonctions

```varpulis
fn calculate_total(price: float, quantity: int) -> float:
    return price * quantity

fn is_valid_order(order: OrderEvent) -> bool:
    return order.quantity > 0 and order.price > 0

# Fonction inline (lambda)
let double = x => x * 2
let add = (a, b) => a + b
```

## Structures de contrôle

### Conditions

```varpulis
if price > 1000:
    category = "high"
elif price > 100:
    category = "medium"
else:
    category = "low"

# Expression ternaire
let status = if active then "enabled" else "disabled"
```

### Pattern matching

```varpulis
match event.type:
    "trade" => process_trade(event)
    "quote" => process_quote(event)
    "order" => process_order(event)
    _ => log_unknown(event)
```

### Boucles

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

## Configuration

```varpulis
config:
    mode: "low_latency"
    
    embedding:
        type: "rule_based"
        dim: 128
    
    attention:
        enabled: true
        compute: "cpu"
        num_heads: 4
    
    state:
        backend: "rocksdb"
        path: "/var/lib/varpulis/state"
    
    observability:
        metrics:
            enabled: true
            endpoint: "0.0.0.0:9090"
        tracing:
            enabled: true
            endpoint: "localhost:4317"
```

## Émission vers sinks

```varpulis
stream Alerts = DetectedPatterns
    .emit(
        alert_id: uuid(),
        severity: "high",
        timestamp: now()
    )
    .to("kafka://alerts-topic")

# Ou vers plusieurs destinations
stream Output = Processed
    .tap(log: "output", sample: 0.01)
    .emit()
    .to(["kafka://topic1", "http://webhook.example.com"])
```
