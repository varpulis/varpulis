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

## Pattern Types

### Sequence (`->`)

Events must occur in the specified order.

```vpl
// A followed by B
pattern Simple = A -> B

// A followed by B followed by C
pattern ThreeStep = A -> B -> C

// With conditions
pattern Conditional =
    Login[status == "success"] -> Action -> Logout
```

**NFA Structure:**
```
[Start] -> [Match A] -> [Match B] -> [Match C] -> [Accept]
```

### Kleene Plus (`+`)

One or more occurrences of a pattern.

```vpl
// One or more failed logins
pattern RepeatedFails = LoginFailed+

// Failed logins followed by success
pattern BruteForce = LoginFailed+ -> LoginSuccess within 10m
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
pattern SmurfingPattern =
    Transaction[amount < 1000]+
    -> Transaction[amount > 9000]
    within 1h
    partition by account_id

stream FraudAlerts = Transaction
    pattern SmurfingPattern
    emit alert("Smurfing", "Account {account_id} suspicious pattern")
```

### Session Analysis

```vpl
// Complete user session
pattern UserSession =
    Login as start
    -> PageView*
    -> OR(Logout, SessionTimeout) as end
    within 24h
    partition by user_id

stream Sessions = *
    pattern UserSession
    emit log("Session: {start.user_id}, duration: {end.timestamp - start.timestamp}")
```

### SLA Monitoring

```vpl
// Request without response within SLA
pattern SLABreach =
    Request as req
    -> NOT(Response[request_id == req.id]) within 5s

stream SLAAlerts = *
    pattern SLABreach
    emit alert("SLABreach", "Request {req.id} not responded within SLA")
```

### IoT Device Monitoring

```vpl
// Device going offline
pattern DeviceOffline =
    Heartbeat as last_beat
    -> NOT(Heartbeat[device_id == last_beat.device_id]) within 1m
    partition by device_id

stream OfflineAlerts = Heartbeat
    pattern DeviceOffline
    emit alert("DeviceOffline", "Device {last_beat.device_id} stopped responding")
```

### Order Processing

```vpl
// Order completion flow
pattern OrderComplete =
    OrderPlaced as order
    -> PaymentReceived[order_id == order.id]
    -> AND(
        ItemsPicked[order_id == order.id],
        AddressVerified[order_id == order.id]
       )
    -> Shipped[order_id == order.id]
    within 7d
    partition by order_id

stream CompletedOrders = *
    pattern OrderComplete
    emit log("Order {order.id} completed successfully")
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

## See Also

- [Language Tutorial](../tutorials/language-tutorial.md) - VPL basics
- [Windows & Aggregations](../reference/windows-aggregations.md) - Windowed pattern matching
- [Trend Aggregation](../reference/trend-aggregation.md) - `.trend_aggregate()` reference
- [Troubleshooting Guide](troubleshooting.md) - Pattern debugging tips
- [SIGMOD 2006 Paper](https://dl.acm.org/doi/10.1145/1142473.1142520) - Original SASE+ research
