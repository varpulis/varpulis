# External Connector Enrichment

The `.enrich()` stream operation enriches streaming events with data from external connectors (HTTP APIs, SQL databases, Redis) using request-response lookups with built-in caching.

## Syntax

```varpulis
stream <Name> = <Source>
    .enrich(<Connector>, key: <expr>, fields: [<field1>, <field2>, ...], cache_ttl: <duration>, timeout: <duration>, fallback: <value>)
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `connector` | Yes | Connector name (first positional arg) |
| `key` | Yes | Expression used as lookup key |
| `fields` | Yes | List of fields to extract from the response |
| `cache_ttl` | No | TTL for caching results (e.g., `5m`, `1h`) |
| `timeout` | No | Max time to wait for response (default: `5s`) |
| `fallback` | No | Default value when lookup fails |

### Built-in Variables

After `.enrich()`, these variables are available in downstream `.where()` and `.emit()`:

| Variable | Type | Description |
|----------|------|-------------|
| `enrich_status` | str | `"ok"`, `"error"`, `"cached"`, or `"timeout"` |
| `enrich_latency_ms` | int | Lookup latency in milliseconds (0 for cache hits) |

## Compatible Connectors

`.enrich()` works with connectors that support request-response patterns:

| Connector Type | Lookup Method |
|----------------|---------------|
| `http` | GET with key as query parameter, parse JSON response |
| `database` | Parameterized SQL query (`$1` placeholder) |
| `redis` | `HGETALL key` (hash) or `GET key` (JSON string) |

Pub/sub connectors (`mqtt`, `kafka`) are **not** compatible with `.enrich()`.

## Examples

### HTTP API Enrichment

```varpulis
connector WeatherAPI = http(url: "https://api.weather.com/v1", method: "GET")

stream Enriched = Temperature as t
    .enrich(WeatherAPI, key: t.city, fields: [forecast, humidity], cache_ttl: 5m)
    .where(forecast == "rain" and temperature > 30)
    .emit(city: t.city, temp: t.temperature, forecast: forecast, humidity: humidity)
```

### SQL Database Enrichment

```varpulis
connector RefDataDB = database(url: "postgres://localhost/refdata", query: "SELECT * FROM products WHERE id = $1")

stream WithRefData = Order as o
    .enrich(RefDataDB, key: o.product_id, fields: [product_name, category], cache_ttl: 1h)
    .emit(order_id: o.id, product: product_name, category: category)
```

### Redis Enrichment

```varpulis
connector RedisCache = redis(url: "redis://localhost:6379")

stream WithRedis = Click as c
    .enrich(RedisCache, key: c.user_id, fields: [user_tier, preferences], cache_ttl: 10m)
    .where(user_tier == "premium")
    .emit(user: c.user_id, tier: user_tier)
```

### Fallback on Failure

```varpulis
connector API = http(url: "https://api.example.com/lookup")

stream Safe = Event as e
    .enrich(API, key: e.id, fields: [name], cache_ttl: 5m, timeout: 2s, fallback: "unknown")
    .emit(id: e.id, name: name, status: enrich_status)
```

When the lookup fails or times out, the fallback value `"unknown"` is injected for all fields, and the event continues through the pipeline instead of being dropped.

## Caching

Results are cached in a thread-safe in-memory cache:

- **TTL-based expiry**: Entries expire after `cache_ttl` duration
- **Max entries**: 100,000 entries; oldest ~10% evicted when full
- **Scope**: Cache is per-stream, shared across all events
- **Cache hits**: `enrich_status` is set to `"cached"` and `enrich_latency_ms` is `0`

## Behavior

1. The `key` expression is evaluated for each event
2. Cache is checked first — if hit, cached fields are injected
3. On cache miss, the external connector is queried with `timeout`
4. On success: fields are cached and injected into the event
5. On failure/timeout without `fallback`: event is skipped (not emitted)
6. On failure/timeout with `fallback`: fallback value injected for all fields

## Async-Only

`.enrich()` requires network I/O and only works in the async pipeline (like `.to()`). When `.enrich()` is present, the engine automatically uses the async processing path.
