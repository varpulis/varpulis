# SASE+ Pattern Matching Guide

Advanced guide to SASE+ pattern matching in Varpulis for detecting complex event sequences.

## Overview

SASE+ (Sequence Algebra for Stream Events) is a pattern matching algorithm for Complex Event Processing. Varpulis implements SASE+ based on the SIGMOD 2006 paper "High-Performance Complex Event Processing over Streams" by Wu, Diao, and Rizvi.

### Key Features

- **NFA-based matching**: Efficient finite automaton execution
- **Kleene closures**: Match one or more (`+`) or zero or more (`*`) events
- **Negation**: Detect absence of events within time windows
- **Logical operators**: AND (any order), OR (either)
- **Partition-by optimization**: Independent matching per partition key
- **Temporal constraints**: Patterns must complete within time bounds

---

## Pattern Syntax

Varpulis uses arrow (`->`) syntax for both **named patterns** and **inline stream expressions**. Both compile to the same NFA engine.

### Named Patterns

Use `pattern Name = ...` to declare a reusable named pattern:

```vpl
pattern BruteForce = AuthEvent where status == "failed" as first
    -> all AuthEvent where status == "failed" as fails
    -> AuthEvent where status == "success" as success
    within 30m partition by source_ip
```

Within an arrow item the order is: `[all] [NOT] EventType [where filter] [as alias]`. Use `all` for Kleene-plus accumulation.

### Inline Stream Patterns

Use `->` inside **stream expressions** for a chainable style:

```vpl
stream FraudAlert = login as l
    -> transfer as t .within(5m)
    .where(l.user_id == t.user_id && t.amount > 5000)
    .emit(alert: "Suspicious transfer", user: l.user_id)
```

For Kleene-plus inside stream expressions, use the `all` keyword:

```vpl
stream BruteForce = LoginFailed as f
    -> all LoginFailed where user_id == f.user_id as fails
    -> LoginSuccess where user_id == f.user_id as success
    .within(30m)
    .partition_by(user_id)
    .emit(failures: count(fails) + 1)
```

### When to Use Which

| | Named pattern | Inline stream expression |
|---|---|---|
| Reuse across multiple streams | Yes | No |
| Chaining `.emit()`, `.forecast()` | Via separate `stream = Pattern.emit(...)` | Inline |
| Readability for complex logic | Good | Good for short pipelines |

---

## Pattern Types

### Sequence

Events must occur in the specified order.

```vpl
pattern ThreeStep = A -> B -> C within 5m

# Or equivalently in a stream expression:
stream ThreeStep = A -> B -> C .within(5m)
```

**NFA Structure:**
```
[Start] -> [Match A] -> [Match B] -> [Match C] -> [Accept]
```

### Kleene Plus (`+` / `all`)

One or more occurrences of an event type.

```vpl
# Named pattern syntax
pattern BruteForce = all LoginFailed as fails
    -> LoginSuccess as success
    within 10m partition by user_id

# Stream expression syntax (same `all` keyword)
stream BruteForce = LoginFailed as first
    -> all LoginFailed as fails
    -> LoginSuccess as success
    .within(10m)
    .partition_by(user_id)
```

**NFA Structure:**
```
[Start] -> [Match Event] -> [Accept]
              ^      |
              +------+  (self-loop)
```

**Implementation Notes:**
- Uses a stack to track Kleene state
- Each match pushes to the stack
- Non-greedy by default (first complete match)

### Kleene Star (`*`)

Zero or more occurrences.

```vpl
// Start, any events, then end
pattern Session = SessionStart -> Activity* -> SessionEnd

// Optional middleware
pattern Request = ClientRequest -> Middleware* -> ServerResponse
```

**Key Difference from `+`:**
- `A*` accepts immediately (zero matches allowed)
- `A+` requires at least one match

### Negation (`NOT`)

Detect the absence of an event within a time window.

```vpl
// Order not confirmed within 1 hour
pattern UnconfirmedOrder =
    OrderPlaced -> NOT(OrderConfirmed) within 1h

// Payment started but never completed
pattern AbandonedPayment =
    PaymentStart -> NOT(PaymentComplete) within 5m
```

**How Negation Works:**

1. The NFA enters a "negation state" after matching the preceding pattern
2. A timeout is set based on the `within` clause
3. If the negated event occurs before timeout, the pattern fails
4. If timeout expires without the event, the pattern succeeds

**Implementation:** See `NegationInfo` and `StateType::Negation` in `sase.rs:108`

### AND (Any Order)

Both patterns must match, but order doesn't matter.

```vpl
// Both documents required (any order)
pattern BothDocs =
    AND(DocumentA, DocumentB) within 1h

// Application complete when both submitted
pattern ApplicationComplete =
    AND(FormSubmitted, PaymentReceived) within 24h
```

**Implementation:**
- Creates an AND state that tracks which branches have matched
- Accepts when all branches are satisfied
- See `AndConfig` and `StateType::And` in `sase.rs`

### OR (Either)

Either pattern matches.

```vpl
// Accept either payment method
pattern PaymentReceived =
    OR(CreditCard, BankTransfer)

// Multiple termination conditions
pattern SessionEnd =
    OR(Logout, Timeout, ForceDisconnect)
```

---

## Predicates

### Field Comparisons

```vpl
pattern HighValue =
    Transaction[amount > 10000]

pattern SpecificUser =
    Login[user_id == "admin" and ip != "10.0.0.1"]
```

**Operators:** `==`, `!=`, `<`, `<=`, `>`, `>=`

### Reference Comparisons

Reference fields from earlier events using aliases:

```vpl
pattern SameUser =
    Login as login
    -> Activity[user_id == login.user_id] as activity
    -> Logout[user_id == login.user_id]
    within 1h
```

### Compound Predicates

```vpl
pattern Complex =
    Event[(value > 100 and status == "active") or priority == "high"]
```

---

## Temporal Constraints

### Pattern-Level Constraints

Apply to the entire pattern:

```vpl
pattern MustBeQuick =
    Start -> Middle -> End
    within 5m
```

### Per-Transition Constraints

Different timeouts for different steps:

```vpl
pattern VariableTiming =
    FastEvent
    -> SlowEvent within 1h
    -> FinalEvent within 5m
```

### Timeout Handling

When a pattern times out:
1. Partial matches are discarded
2. Resources are freed
3. No match is emitted

---

## Rising & Monotonic Patterns (v0.10.0)

Self-referencing Kleene predicates let you detect **strictly monotonic trends** — rising temperatures, escalating prices, increasing severity — where each event must compare against the previous captured event.

### Strictly Rising Values

```vpl
pattern StrictlyRising = SensorReading as first
    -> all SensorReading where temperature > rising.temperature as rising
    -> SensorReading where temperature < first.temperature as drop
    within 5m partition by sensor_id
```

**How it works:** The predicate `temperature > rising.temperature` compares each new event against the **last captured Kleene event** (not the first). The alias `rising` refers to itself — the engine:

1. **First Kleene event**: Skips the self-referencing predicate (no previous value exists), checks event type only
2. **Subsequent events**: Evaluates `temperature > rising.temperature` against the previously captured event
3. **Terminator**: The final `SensorReading where temperature < first.temperature` closes the pattern

### Strictly Decreasing Values

```vpl
pattern CoolingDown = SensorReading as first
    -> all SensorReading where temperature < cooling.temperature as cooling
    within 10m partition by sensor_id
```

### Escalating Severity

```vpl
pattern Escalation = Alert as first
    -> all Alert where severity > escalating.severity as escalating
    within 1h partition by host

stream EscalationAlert = Escalation
    .where(count(escalating) >= 3)
    .emit(
        host: first.host,
        initial_severity: first.severity,
        final_severity: escalating.severity,
        steps: count(escalating)
    )
```

### Aggregating Over Trends

When you need **statistics** (count, average) over rising trends rather than individual matches, use `.trend_aggregate()` with the Hamlet engine for O(n) performance:

```vpl
stream TrendStats = StockTick as first
    -> all StockTick where price > first.price as rising
    .within(60s)
    .partition_by(symbol)
    .trend_aggregate(count: count_trends())
    .emit(symbol: first.symbol, trends: count)
```

> **Note:** Self-referencing predicates (`rising.temperature`) compare against the **previous** Kleene event. Cross-referencing predicates (`first.temperature`) compare against a **fixed** earlier event.

---

## Partition Strategies

### Partition-By Attribute

Process patterns independently per key:

```vpl
pattern PerUser =
    Login+ -> Logout
    within 1h
    partition by user_id
```

**Benefits:**
- Parallel processing across partitions
- Memory isolation (one partition doesn't affect others)
- Natural grouping for user/device/session patterns

**Memory Impact:**
- Each partition maintains its own NFA state
- N partitions = N × base memory

### Without Partitioning

Global pattern matching across all events:

```vpl
pattern GlobalPattern =
    SystemAlert -> AdminResponse
    within 30m
```

---

## Event Selection Strategies

SASE+ supports different strategies for selecting events when multiple matches are possible.

### Skip-Till-Any-Match (Default)

Match as many patterns as possible, potentially with overlapping events.

```vpl
// Given events: A1, B1, A2, B2
// Pattern: A -> B
// Matches: (A1, B1), (A1, B2), (A2, B2)
```

### Skip-Till-Next-Match

Each event participates in at most one match.

```vpl
// Given events: A1, B1, A2, B2
// Pattern: A -> B
// Matches: (A1, B1), (A2, B2)
```

### Strict Contiguity

Events must be immediately adjacent (no skipping).

```vpl
// Given events: A1, C1, B1
// Pattern: A -> B (strict)
// Matches: none (C1 breaks contiguity)
```

---

## Debugging Patterns

### Verbose Output

Use `--verbose` with simulation to see pattern state:

```bash
varpulis simulate -p rules.vpl -e events.evt --verbose
```

### Pattern Tracing

Enable trace logging:

```bash
RUST_LOG=varpulis_runtime::sase=trace varpulis simulate ...
```

### Common Issues

#### 1. Pattern Never Matches

**Possible causes:**
- Event types don't match exactly (case-sensitive)
- Predicates are too restrictive
- Timeout is too short

**Debug steps:**
```vpl
// Remove predicates to test basic matching
pattern Debug1 = A -> B within 1h

// Add predicates back one at a time
pattern Debug2 = A[field > 0] -> B within 1h
```

#### 2. Too Many Matches

**Possible causes:**
- Missing predicates to constrain matches
- Skip-till-any-match creating overlapping matches
- Missing `partition by` causing cross-user matches

**Solution:**
```vpl
// Add partition to isolate matches
pattern Isolated =
    Login -> Action -> Logout
    within 1h
    partition by user_id
```

#### 3. Memory Growth

**Possible causes:**
- Unbounded Kleene closure (`+` or `*`)
- Too many partitions
- Timeout too long

**Solutions:**
```vpl
// Limit Kleene matches
pattern Limited =
    Event+ within 5m  // Natural bound via timeout

// Reduce partition cardinality
partition by category  // Use low-cardinality field
```

#### 4. Negation Not Triggering

**Possible causes:**
- Event type mismatch in NOT clause
- Timeout too short (event arrives just after)
- Negated event arriving before pattern start

**Debug:**
```vpl
// Ensure event types match exactly
pattern Debug =
    Start -> NOT(Exactly_This_Type) within 10m
```

---

## Performance Considerations

### NFA Complexity

| Pattern | States | Transitions |
|---------|--------|-------------|
| `A -> B` | 3 | 2 |
| `A -> B -> C` | 4 | 3 |
| `A+` | 2 | 2 (with self-loop) |
| `AND(A, B)` | 4 | 4 |
| `A -> B+ -> C` | 4 | 4 |

### Memory Usage

```
Memory = O(partitions × active_runs × events_per_run)
```

**Recommendations:**
- Use short timeouts to limit active runs
- Partition by low-cardinality fields
- Avoid unbounded Kleene without timeout

### Throughput

Typical throughput on modern hardware:
- Simple patterns: 500K+ events/sec
- Complex patterns with Kleene: 100K+ events/sec
- Patterns with ZDD optimization: 200K+ events/sec

---

## ZDD Optimization

Varpulis uses Zero-suppressed Decision Diagrams (ZDD) to represent Kleene capture combinations during SASE+ pattern matching. When a Kleene pattern like `A -> B+ -> C` matches many B events, ZDD compactly encodes all possible subsets — e.g., 100 matching B events produce ~100 ZDD nodes instead of 2^100 explicit combinations.

### Benefits

- Exponential compression of Kleene match combinations
- Efficient subset/superset operations for match enumeration
- Reduced memory for patterns with many Kleene captures

### When ZDD Helps

- Kleene+ patterns with many matching events
- Patterns with overlapping match candidates
- High-throughput streams where match combination count would explode

### Implementation

See `varpulis-zdd` crate for ZDD data structures and `sase.rs` for integration with the pattern matcher.

> **Note**: ZDD is used for **pattern matching** (Kleene captures). For **trend aggregation** over patterns, Varpulis uses the Hamlet engine — see [trend aggregation](../architecture/trend-aggregation.md).

---

## Examples

### Fraud Detection

```vpl
// Multiple small transactions followed by large withdrawal
pattern SmurfingPattern = all Transaction where amount < 1000 as small
    -> Transaction where amount > 9000 as large
    within 1h partition by account_id

stream FraudAlerts = SmurfingPattern
    .emit(
        alert: "Smurfing",
        account: large.account_id,
        num_small: count(small)
    )
```

### SLA Monitoring

```vpl
// Request without response within SLA
pattern SLABreach = Request as req
    -> NOT Response where request_id == req.id
    within 5s

stream SLAAlerts = SLABreach
    .emit(alert: "SLA breach", request_id: req.id)
```

### IoT Device Monitoring

```vpl
// Device going offline (no heartbeat within 1m)
pattern DeviceOffline = Heartbeat as last_beat
    -> NOT Heartbeat
    within 1m partition by device_id

stream OfflineAlerts = DeviceOffline
    .emit(alert: "Device offline", device: last_beat.device_id)
```

---

## Trend Aggregation Mode

For patterns where you need **statistics over trends** (COUNT, SUM, AVG) rather than individual matches, use `.trend_aggregate()` instead of the default detection mode:

```vpl
# Instead of detecting each rising price pattern individually...
# Count how many rising trends exist (without enumerating them)
stream TrendCount = StockTick as first
    -> all StockTick where price > first.price as rising
    .within(60s)
    .partition_by(symbol)
    .trend_aggregate(count: count_trends())
    .emit(symbol: first.symbol, trends: count)
```

This uses the **Hamlet engine** (SIGMOD 2021) for O(n) aggregation instead of explicit trend construction, which can be exponential. Multiple queries sharing Kleene sub-patterns are automatically optimized via shared aggregation.

See [Trend Aggregation Reference](../reference/trend-aggregation.md) and [Trend Aggregation Tutorial](../tutorials/trend-aggregation-tutorial.md) for details.

---

## Pattern Forecasting

Use `.forecast()` to predict whether a partially-matched pattern will complete, and when. This uses **Prediction Suffix Trees** (PST) combined with the SASE NFA to form a Pattern Markov Chain (PMC):

```vpl
stream FraudForecast = Transaction as t1
    -> Transaction as t2 where t2.amount > t1.amount * 5
    -> Transaction as t3 where t3.location != t1.location
    .within(5m)
    .forecast(confidence: 0.7, horizon: 2m, warmup: 500)
    .where(forecast_probability > 0.8)
    .emit(
        probability: forecast_probability,
        expected_time: forecast_time
    )
```

**Parameters**: `confidence` (min probability, default 0.5), `horizon` (forecast window), `warmup` (learning period, default 100), `max_depth` (PST depth, default 5)

**Built-in variables** (available after `.forecast()`): `forecast_probability`, `forecast_time`, `forecast_state`, `forecast_context_depth`

The PST learns online from the event stream — no pre-training required. See [Forecasting Tutorial](../tutorials/forecasting-tutorial.md) and [Forecasting Architecture](../architecture/forecasting.md) for details.

---

## See Also

- [Language Tutorial](../tutorials/language-tutorial.md) - VPL basics
- [Windows & Aggregations](../reference/windows-aggregations.md) - Windowed pattern matching
- [Trend Aggregation](../reference/trend-aggregation.md) - `.trend_aggregate()` reference
- [Forecasting Tutorial](../tutorials/forecasting-tutorial.md) - PST-based pattern forecasting
- [Forecasting Architecture](../architecture/forecasting.md) - PST/PMC design
- [Troubleshooting Guide](troubleshooting.md) - Pattern debugging tips
- [SIGMOD 2006 Paper](https://dl.acm.org/doi/10.1145/1142473.1142520) - Original SASE+ research
