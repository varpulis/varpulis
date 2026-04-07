# VPL by Example

Learn the **Varpulis Pattern Language (VPL)** through focused, runnable examples.
Each example demonstrates one concept with a `.vpl` program and matching `.evt` test data.

## Running Examples

```bash
varpulis simulate -p <example>.vpl -e <example>.evt -v -w 1
```

## Examples

### Basics
| # | File | Concept | Key Syntax |
|---|------|---------|------------|
| 01 | [hello_world](01_hello_world.vpl) | Simplest program: filter and emit | `stream`, `.where()`, `.emit()` |
| 02 | [filtering](02_filtering.vpl) | Comparison and logical operators | `and`, `or`, `!=`, chained `.where()` |
| 03 | [event_declarations](03_event_declarations.vpl) | Define event schemas with types | `event Name:` with `str`, `int`, `float`, `bool`, `timestamp` |
| 04 | [field_selection](04_field_selection.vpl) | Project, rename, and compute fields | `.select()`, `.emit()`, arithmetic, `if-then-else` |

### Windows
| # | File | Concept | Key Syntax |
|---|------|---------|------------|
| 05 | [tumbling_window](05_tumbling_window.vpl) | Fixed-size, non-overlapping time buckets | `.window(5s)` |
| 06 | [count_window](06_count_window.vpl) | Fixed event count triggers | `.window(3)` |
| 07 | [sliding_window](07_sliding_window.vpl) | Overlapping windows with slide step | `.window(10s, sliding: 5s)` |
| 08 | [session_window](08_session_window.vpl) | Activity-gap based windows | `.window(session: 5s)`, `.partition_by()` |

### Aggregations
| # | File | Concept | Key Syntax |
|---|------|---------|------------|
| 09 | [basic_aggregations](09_basic_aggregations.vpl) | count, sum, avg, min, max, first, last | `.aggregate()` with all built-in functions |
| 10 | [partitioned_aggregations](10_partitioned_aggregations.vpl) | Independent aggregation per key | `.partition_by(symbol)`, VWAP calculation |

### SASE+ Patterns (Complex Event Processing)
| # | File | Concept | Key Syntax |
|---|------|---------|------------|
| 11 | [simple_sequence](11_simple_sequence.vpl) | Two-event sequence (A -> B) | `->`, `as alias`, `where` correlation |
| 12 | [multi_step_sequence](12_multi_step_sequence.vpl) | Three+ event chains (A -> B -> C) | Chained `->` with cross-step references |
| 13 | [kleene_plus](13_kleene_plus.vpl) | One or more repeated events | `-> all EventType` |
| 14 | [negation](14_negation.vpl) | Detect event absence | `.not(EventType where condition)` |
| 15 | [temporal_constraints](15_temporal_constraints.vpl) | Time-bounded pattern matching | `.within(1h)` |
| 16 | [partition_by_patterns](16_partition_by_patterns.vpl) | Independent patterns per key | `.partition_by(user_id)` in patterns |
| 17 | [reusable_patterns](17_reusable_patterns.vpl) | Named pattern declarations | `pattern Name = A -> all B` |
| 18 | [match_all](18_match_all.vpl) | Capture all events in Kleene closure | `-> all ProcessingStep as steps` |

### Multi-Stream Operations
| # | File | Concept | Key Syntax |
|---|------|---------|------------|
| 19 | [join](19_join.vpl) | Correlate events from two streams | `join(A, B)`, `.on()`, `.window()` |
| 20 | [merge](20_merge.vpl) | Union multiple streams | `merge(StreamA, StreamB)` |

### Functions & Expressions
| # | File | Concept | Key Syntax |
|---|------|---------|------------|
| 21 | [functions](21_functions.vpl) | User-defined functions | `fn name(param: type) -> type:` |
| 22 | [conditional_expressions](22_conditional_expressions.vpl) | Inline if-then-else | `if cond then val else val` |

### Advanced Features
| # | File | Concept | Key Syntax |
|---|------|---------|------------|
| 23 | [forecasting](23_forecasting.vpl) | Predict pattern completion (PST) | `.forecast(confidence:, horizon:, warmup:)` |
| 24 | [trend_aggregation](24_trend_aggregation.vpl) | Hamlet engine multi-trend stats | `.trend_aggregate()`, `avg_trends()`, `count_trends()` |
| 25 | [watermarks](25_watermarks.vpl) | Out-of-order event handling | `.watermark(out_of_order: 5s)` |
| 26 | [connectors](26_connectors.vpl) | Source/sink configuration (reference) | `connector Name = mqtt(...)`, `.from()`, `.to()` |

### Monotonic Patterns & Arrow Syntax
| # | File | Concept | Key Syntax |
|---|------|---------|------------|
| 27 | [strictly_increasing](27_strictly_increasing.vpl) | Detect rising values | `.increasing(field)` |
| 28 | [strictly_decreasing](28_strictly_decreasing.vpl) | Detect falling values | `.decreasing(field)` |
| 29 | [arrow_patterns](29_seq_vs_arrow.vpl) | Named patterns vs inline streams | `pattern Name = A -> B within 5m` |

## VPL Quick Reference

### A Complete VPL Program

```vpl
event SensorReading:
    sensor_id: str
    temperature: float

# Filter events and emit output
stream HighTemp = SensorReading
    .where(temperature > 100)
    .emit(sensor: sensor_id, temp: temperature)

# Windowed aggregation
stream AvgTemp = SensorReading
    .partition_by(sensor_id)
    .window(5m)
    .aggregate(avg_temp: avg(temperature), readings: count())
    .emit(sensor: sensor_id, avg_temp: avg_temp)
```

### Detect Event Sequences

```vpl
# Login followed by suspicious purchase → alert
stream FraudAlert = Login as login
    -> PasswordChange where user_id == login.user_id as pwd
    -> Purchase where user_id == login.user_id and amount > 500 as purchase
    .within(30m)
    .emit(user: login.user_id, amount: purchase.amount)
```

### Event Declaration

```vpl
event Trade:
    symbol: str
    price: float
    quantity: int
    ts: timestamp
```

### Duration Units

`ms` (milliseconds), `s` (seconds), `m` (minutes), `h` (hours), `d` (days)

### Event File Format (.evt)

```
# Comments start with #
EventType { field: "value", number: 42, flag: true }

BATCH 1000
EventType { field: "later_event" }

@5s EventType { field: "at_5_seconds" }
```
