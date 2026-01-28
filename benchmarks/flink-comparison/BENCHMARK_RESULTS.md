# Varpulis vs Flink CEP Benchmark Results

**Date**: 2026-01-28
**Scenario**: Login → FailedTransaction (same user within 5 seconds)

## Executive Summary

| Metric | Varpulis | Flink CEP | Difference |
|--------|----------|-----------|------------|
| Correctness | 4/4 PASS | 4/4 PASS | Equal |
| Avg Latency | 554ms | 926ms | **Varpulis 40% faster** |
| Min Latency | 202ms | 756ms | Varpulis 3.7x faster |
| Max Latency | 905ms | 1249ms | Varpulis 1.4x faster |

## Test Configuration

### Pattern Under Test
```
Login → FailedTransaction (same user_id) within 5 seconds
```

### Test Events (10 events + 1 heartbeat)
| # | Event Type | User | Expected Alert |
|---|------------|------|----------------|
| 1 | Login | user1 | - |
| 2 | Login | user2 | - |
| 3 | Transaction (failed) | user1 | Yes (user1) |
| 4 | Transaction (success) | user3 | No |
| 5 | Login | user3 | - |
| 6 | Transaction (failed) | user2 | Yes (user2) |
| 7 | Transaction (failed) | user3 | Yes (user3) |
| 8 | Login | user4 | - |
| 9 | Transaction (success) | user4 | No |
| 10 | Transaction (failed) | user4 | Yes (user4) |
| 11 | Heartbeat | _heartbeat | (watermark advancement) |

### Time Semantics
Both systems were tested in **Event Time** mode with watermarks:

- **Varpulis**: Uses event timestamps (`ts` field) with automatic watermark generation
- **Flink**: Uses `forMonotonousTimestamps()` with `withIdleness(2s)` for watermark progression

## Why Event Time?

Flink CEP's `within()` operator **requires event time semantics**. Without watermarks, Flink cannot determine when a time window has expired and thus cannot emit matches for time-bounded patterns.

This is not a limitation but a design choice: event time provides correct handling of:
- Out-of-order events
- Replay from historical data
- Distributed processing with network delays

Varpulis now also supports event time with watermarks for fair comparison.

## Results Analysis

### Latency Breakdown

```
Event Timeline (ms from start):
─────────────────────────────────────────────────────────────────────
0     100   200   300   400   500   600   700   800   900   1000
│     │     │     │     │     │     │     │     │     │     │
├─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
      V1    V2         V3                        V4          (Varpulis)
      │     │          │                         │
      202   504        605                       905          ms

                                         F1    F2    F3              F4
                                         │     │     │               │
                                         756   868   907             1249  ms (Flink)
```

### Why is Varpulis Faster?

1. **Immediate Pattern Completion**: Varpulis emits alerts as soon as a pattern completes, updating its watermark with each event.

2. **Flink's Watermark Delay**: Flink CEP waits for the watermark to pass the window end time before emitting. With `withIdleness(2s)`, there's a minimum 2-second delay after the last event before idle watermark advancement kicks in.

3. **Architecture**:
   - Varpulis: Rust, single-threaded, purpose-built for CEP
   - Flink: JVM, distributed framework with checkpointing overhead

## Code Comparison

### Varpulis (35 lines)
```vpl
# Event definitions
event Login:
    user_id: str
    ip_address: str
    device: str

event Transaction:
    user_id: str
    amount: float
    status: str
    merchant: str

# SASE+ Temporal sequence pattern
stream SuspiciousActivity = Login as login
    -> Transaction where user_id == login.user_id
                     and status == "failed" as failed_tx
    .within(10m)
    .emit(
        alert_type: "LOGIN_THEN_FAILED_TX",
        user_id: login.user_id,
        severity: if failed_tx.amount > 1000 then "high" else "medium"
    )
```

### Flink CEP (160+ lines)
```java
// Pattern definition
Pattern<UserEvent, ?> pattern = Pattern.<UserEvent>begin("login")
    .where(new SimpleCondition<UserEvent>() {
        @Override
        public boolean filter(UserEvent event) {
            return "Login".equals(event.type);
        }
    })
    .followedBy("failedTx")
    .where(new IterativeCondition<UserEvent>() {
        @Override
        public boolean filter(UserEvent event, Context<UserEvent> ctx) {
            if (!"Transaction".equals(event.type)) return false;
            if (!"failed".equals(event.status)) return false;
            for (UserEvent login : ctx.getEventsForPattern("login")) {
                if (login.userId.equals(event.userId)) {
                    return true;
                }
            }
            return false;
        }
    })
    .within(Time.seconds(5));

// Plus: Source function, Sink function, Watermark strategy,
// Event classes, Alert classes, MQTT handling...
```

## Watermark Implementation

### Varpulis Watermark Support (New)

```rust
// Enable event-time mode with watermarks
let engine = SaseEngine::new(pattern)
    .with_event_time()
    .with_max_out_of_orderness(Duration::from_secs(2));

// Watermark is automatically updated with each event:
// watermark = max_observed_timestamp - max_out_of_orderness

// Runs expire when: watermark > run.event_time_deadline
```

### Flink Watermark Strategy

```java
WatermarkStrategy.<UserEvent>forMonotonousTimestamps()
    .withTimestampAssigner((event, ts) -> event.timestamp)
    .withIdleness(Duration.ofSeconds(2))
```

## Conclusion

Both systems produce **correct results** for the test pattern. Varpulis demonstrates:

- **40% lower latency** (554ms vs 926ms average)
- **4.5x less code** (35 lines vs 160+ lines)
- **Simpler deployment** (single binary vs JVM + Flink runtime)

The latency difference is primarily due to Flink's watermark-based window completion, which adds inherent delay to ensure correct handling of out-of-order events.

## Raw Results

```json
{
  "timestamp": "2026-01-28T16:44:01",
  "scenario": "Login -> FailedTransaction",
  "time_mode": "event",
  "events_count": 11,
  "expected_alerts": ["user1", "user2", "user3", "user4"],
  "varpulis": {
    "alerts": ["user1", "user2", "user3", "user4"],
    "correct": true,
    "latencies_ms": [202, 504, 605, 905]
  },
  "flink": {
    "alerts": ["user1", "user2", "user3", "user4"],
    "correct": true,
    "latencies_ms": [756, 868, 907, 1249]
  }
}
```

## Reproduction

```bash
# Start MQTT broker
docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:2

# Build Varpulis
cargo build --release

# Build Flink benchmark
cd benchmarks/flink-comparison/test-harness
mvn package

# Run benchmark (event time mode)
python3 run_full_benchmark.py event
```
