# VarpulisQL - Overview

## Design Philosophy

- **Everything is a Stream**: No artificial distinction between listeners/streams
- **Declarative**: User describes WHAT, not HOW
- **Composable**: Operations chain naturally
- **Type-safe**: Error detection at compile time
- **Observable**: Built-in metrics and traces
- **Pythonic**: Python-inspired syntax (optional indentation, clarity)

## Quick Example

```varpulis
# Simple stream definition
stream Trades from TradeEvent

# Filtering and aggregation
stream HighValueTrades = Trades
    .where(price > 10000)
    .window(5m)
    .aggregate(
        total: sum(price),
        count: count()
    )

# Pattern detection with attention
stream FraudAlert = Trades
    .attention_window(30s, heads: 4)
    .pattern(
        suspicious: events =>
            events.filter(e => e.amount > 10000).count() > 3
            and attention_score(events[0], events[-1]) > 0.85
    )
    .emit(
        alert_type: "fraud",
        severity: "high"
    )
```

## Differences from Python

| Aspect | Python | VarpulisQL |
|--------|--------|------------|
| **Paradigm** | Imperative/OO | Declarative/Stream |
| **Typing** | Dynamic | Static with inference |
| **Indentation** | Required | Optional (preferred) |
| **Async** | `async/await` | Implicit (everything is async) |
| **Null** | `None` | `null` with optional types |

## Key Concepts

### Streams
A stream is a potentially infinite sequence of typed events.

### Events
An event is an immutable record with a timestamp.

### Windows
Temporal windows to bound computations on streams.

### Patterns
Detection rules on event sequences.

### Aggregations
Reduction functions on windows (sum, avg, count, etc.).

### Contexts
Named execution domains that run on dedicated OS threads. Enable multi-core parallelism with CPU affinity and cross-context communication via bounded channels.

## See Also

- [Complete Syntax](syntax.md)
- [Type System](types.md)
- [Reserved Keywords](keywords.md)
- [Operators](operators.md)
- [Built-in Functions](builtins.md)
- [Formal Grammar](grammar.md)
