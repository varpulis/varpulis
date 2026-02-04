# Parallel Stream Processing with Contexts

This tutorial teaches you to use VarpulisQL contexts for parallel stream processing. You'll start with a single context and build up to multi-stage pipelines with session windows -- each step includes runnable code you can copy-paste and test.

## What You'll Learn

- How to declare and assign streams to contexts
- Running independent streams in parallel on separate OS threads
- Building cross-context pipelines with `.emit(context: ...)`
- Windowed aggregation across parallel contexts
- Session windows for inactivity-based grouping
- Production considerations: CPU affinity, backpressure, monitoring

## Prerequisites

- Varpulis built and on your `PATH` (see [Getting Started](getting-started.md))
- Basic VPL knowledge: streams, `where`, `window`, `aggregate` (see [Language Tutorial](language-tutorial.md))

---

## Part 1: Your First Context

Without contexts, all streams run on a single thread. That's fine for simple programs, but when you have a CPU-intensive analytics stream alongside a fast filtering stream, the slow one blocks the fast one.

Contexts solve this by giving each stream its own OS thread.

### The Program

Create a file called `first_context.vpl`:

```vpl
// first_context.vpl
// A single stream running in a named context

context sensors

stream HighReadings = SensorReading
    .context(sensors)
    .where(value > 50)
    .emit(sensor_id: sensor_id, value: value)
```

This declares one context called `sensors` and assigns the `HighReadings` stream to it. The stream filters sensor readings above 50 and emits the result.

### Test Data

Create `first_context.evt`:

```
# first_context.evt
@0 SensorReading { sensor_id: "S1", value: 72.5 }
@100 SensorReading { sensor_id: "S2", value: 31.0 }
@200 SensorReading { sensor_id: "S1", value: 88.3 }
@300 SensorReading { sensor_id: "S3", value: 45.0 }
@400 SensorReading { sensor_id: "S2", value: 91.7 }
```

### Run It

```bash
varpulis check first_context.vpl
varpulis simulate -p first_context.vpl -e first_context.evt --verbose
```

**Expected output:**

```
Program loaded: 1 streams, 1 contexts

Starting simulation...

  [  1] @     0ms SensorReading { ... }
  => HighReadings: { sensor_id: "S1", value: 72.5 }
  [  2] @   100ms SensorReading { ... }
  [  3] @   200ms SensorReading { ... }
  => HighReadings: { sensor_id: "S1", value: 88.3 }
  [  4] @   300ms SensorReading { ... }
  [  5] @   400ms SensorReading { ... }
  => HighReadings: { sensor_id: "S2", value: 91.7 }

Simulation Complete
======================
Events processed: 5
Contexts used:    1
```

**Key takeaway:** The program behaves identically to one without contexts, but the stream now runs on its own dedicated OS thread. This is the foundation for parallelism.

---

## Part 2: Parallel Contexts

Now let's run two independent streams in separate contexts. Each gets its own OS thread, so they execute in true parallelism -- no shared state, no locking.

### The Program

Create `parallel_contexts.vpl`:

```vpl
// parallel_contexts.vpl
// Two independent streams, each on its own thread

context temperature_ctx
context pressure_ctx

stream HighTemps = TemperatureReading
    .context(temperature_ctx)
    .where(temperature > 80)
    .emit(sensor_id: sensor_id, temperature: temperature)

stream HighPressure = PressureReading
    .context(pressure_ctx)
    .where(pressure > 100)
    .emit(sensor_id: sensor_id, pressure: pressure)
```

Two contexts, two streams, two OS threads. Temperature readings are processed on one thread while pressure readings are processed on another -- simultaneously.

### Test Data

Create `parallel_contexts.evt`:

```
# parallel_contexts.evt
# Mixed event types -- each goes to its own context
@0 TemperatureReading { sensor_id: "T1", temperature: 85.0 }
@50 PressureReading { sensor_id: "P1", pressure: 110.0 }
@100 TemperatureReading { sensor_id: "T2", temperature: 72.0 }
@150 PressureReading { sensor_id: "P2", pressure: 95.0 }
@200 TemperatureReading { sensor_id: "T1", temperature: 91.2 }
@250 PressureReading { sensor_id: "P1", pressure: 120.5 }
@300 TemperatureReading { sensor_id: "T3", temperature: 65.0 }
@350 PressureReading { sensor_id: "P3", pressure: 108.0 }
```

### Run It

```bash
varpulis simulate -p parallel_contexts.vpl -e parallel_contexts.evt --verbose
```

**Expected output:**

```
Program loaded: 2 streams, 2 contexts

Starting simulation...

  [  1] @     0ms TemperatureReading { ... }
  => HighTemps: { sensor_id: "T1", temperature: 85.0 }
  [  2] @    50ms PressureReading { ... }
  => HighPressure: { sensor_id: "P1", pressure: 110.0 }
  [  3] @   100ms TemperatureReading { ... }
  [  4] @   150ms PressureReading { ... }
  [  5] @   200ms TemperatureReading { ... }
  => HighTemps: { sensor_id: "T1", temperature: 91.2 }
  [  6] @   250ms PressureReading { ... }
  => HighPressure: { sensor_id: "P1", pressure: 120.5 }
  [  7] @   300ms TemperatureReading { ... }
  [  8] @   350ms PressureReading { ... }
  => HighPressure: { sensor_id: "P3", pressure: 108.0 }

Simulation Complete
======================
Events processed: 8
Contexts used:    2
```

**Key takeaway:** Each context runs on its own OS thread. Temperature and pressure processing happen in parallel with no shared state between them.

---

## Part 3: Cross-Context Pipelines

Real systems often need data to flow between stages: ingest, then compute, then alert. Contexts support this with `.emit(context: target, ...)`, which sends events from one context to another via bounded channels.

### The Program

Create `pipeline.vpl`:

```vpl
// pipeline.vpl
// 3-stage pipeline: ingest -> compute -> alert

context ingest
context compute
context alert

// Stage 1: Filter invalid readings, forward to compute
stream ValidReadings = SensorReading
    .context(ingest)
    .where(value > 0 and sensor_id != "")
    .emit(context: compute, sensor_id: sensor_id, value: value, zone: zone)

// Stage 2: Aggregate per zone, forward high averages to alert
stream ZoneStats = ValidReadings
    .context(compute)
    .partition_by(zone)
    .window(1m)
    .aggregate(
        zone: zone,
        avg_value: avg(value),
        max_value: max(value),
        reading_count: count()
    )
    .emit(context: alert, zone: zone, avg_value: avg_value, max_value: max_value)

// Stage 3: Generate alerts for overheating zones
stream OverheatAlerts = ZoneStats
    .context(alert)
    .where(max_value > 150)
    .emit(alert_type: "ZoneOverheat", zone: zone, max_value: max_value)
```

Data flows: `SensorReading` -> `ingest` context -> `compute` context -> `alert` context. Each stage is isolated on its own thread. The `.emit(context: ...)` syntax sends events across context boundaries via `mpsc` channels.

### Test Data

Create `pipeline.evt`:

```
# pipeline.evt
@0 SensorReading { sensor_id: "S1", value: 140.0, zone: "north" }
@100 SensorReading { sensor_id: "S2", value: 160.5, zone: "north" }
@200 SensorReading { sensor_id: "S3", value: 95.0, zone: "south" }
@300 SensorReading { sensor_id: "S4", value: 170.2, zone: "north" }
@400 SensorReading { sensor_id: "S5", value: 88.0, zone: "south" }
@500 SensorReading { sensor_id: "S6", value: -5.0, zone: "north" }
@600 SensorReading { sensor_id: "S7", value: 155.0, zone: "south" }
```

Notice the invalid reading at `@500` (`value: -5.0`) -- the ingest stage will filter it out before it reaches compute.

### Run It

```bash
varpulis simulate -p pipeline.vpl -e pipeline.evt --immediate --verbose
```

**Expected output:**

```
Program loaded: 3 streams, 3 contexts

Starting simulation...

  [  1] @     0ms SensorReading { ... }
  => ValidReadings -> compute: { sensor_id: "S1", value: 140.0, zone: "north" }
  [  2] @   100ms SensorReading { ... }
  => ValidReadings -> compute: { sensor_id: "S2", value: 160.5, zone: "north" }
  [  3] @   200ms SensorReading { ... }
  => ValidReadings -> compute: { sensor_id: "S3", value: 95.0, zone: "south" }
  [  4] @   300ms SensorReading { ... }
  => ValidReadings -> compute: { sensor_id: "S4", value: 170.2, zone: "north" }
  [  5] @   400ms SensorReading { ... }
  => ValidReadings -> compute: { sensor_id: "S5", value: 88.0, zone: "south" }
  [  6] @   500ms SensorReading { ... }
  (filtered: value <= 0)
  [  7] @   600ms SensorReading { ... }
  => ValidReadings -> compute: { sensor_id: "S7", value: 155.0, zone: "south" }

  [window] ZoneStats (north): { zone: "north", avg_value: 156.9, max_value: 170.2, reading_count: 3 }
  => ZoneStats -> alert: { zone: "north", avg_value: 156.9, max_value: 170.2 }
  [window] ZoneStats (south): { zone: "south", avg_value: 112.7, max_value: 155.0, reading_count: 3 }
  => ZoneStats -> alert: { zone: "south", avg_value: 112.7, max_value: 155.0 }

ALERT: ZoneOverheat
  zone: north
  max_value: 170.2
ALERT: ZoneOverheat
  zone: south
  max_value: 155.0

Simulation Complete
======================
Events processed: 7
Contexts used:    3
Cross-context events: 8
```

**Key takeaway:** Each pipeline stage is isolated in its own context. Data flows through bounded channels. The ingest stage filters bad data before it crosses a context boundary -- this is a best practice, since every cross-context emit has a small channel cost.

---

## Part 4: Windowed Aggregation in Parallel

Contexts shine when you have CPU-intensive workloads. Windowed aggregation with partitioning is a common example: each context handles a subset of the data independently.

### The Program

Create `parallel_windows.vpl`:

```vpl
// parallel_windows.vpl
// Two contexts doing independent windowed aggregation

context zone_a_ctx
context zone_b_ctx

// Zone A analytics on its own thread
stream ZoneAStats = SensorReading
    .context(zone_a_ctx)
    .where(zone == "A")
    .partition_by(sensor_id)
    .window(tumbling 30s)
    .aggregate(
        sensor_id: sensor_id,
        avg_value: avg(value),
        max_value: max(value),
        stddev_value: stddev(value),
        reading_count: count()
    )

// Zone B analytics on its own thread
stream ZoneBStats = SensorReading
    .context(zone_b_ctx)
    .where(zone == "B")
    .partition_by(sensor_id)
    .window(tumbling 30s)
    .aggregate(
        sensor_id: sensor_id,
        avg_value: avg(value),
        max_value: max(value),
        stddev_value: stddev(value),
        reading_count: count()
    )
```

Both zones compute the same heavy analytics (`avg`, `max`, `stddev`, `count`) but on separate threads. If zone A has a burst of data, it doesn't slow down zone B.

### Test Data

Create `parallel_windows.evt`:

```
# parallel_windows.evt
# Interleaved readings from two zones
@0 SensorReading { sensor_id: "A1", value: 72.0, zone: "A" }
@100 SensorReading { sensor_id: "B1", value: 65.0, zone: "B" }
@200 SensorReading { sensor_id: "A1", value: 75.5, zone: "A" }
@300 SensorReading { sensor_id: "B1", value: 68.0, zone: "B" }
@400 SensorReading { sensor_id: "A2", value: 80.0, zone: "A" }
@500 SensorReading { sensor_id: "B2", value: 71.0, zone: "B" }
@600 SensorReading { sensor_id: "A1", value: 78.0, zone: "A" }
@700 SensorReading { sensor_id: "B1", value: 67.5, zone: "B" }
@800 SensorReading { sensor_id: "A2", value: 82.5, zone: "A" }
@900 SensorReading { sensor_id: "B2", value: 73.0, zone: "B" }
```

### Run It

```bash
varpulis simulate -p parallel_windows.vpl -e parallel_windows.evt --immediate --verbose
```

**Expected output:**

```
Program loaded: 2 streams, 2 contexts

Starting simulation...

  [  1-10] 10 events dispatched to 2 contexts

  [window] ZoneAStats (A1): { sensor_id: "A1", avg_value: 75.2, max_value: 78.0, stddev_value: 2.5, reading_count: 3 }
  [window] ZoneAStats (A2): { sensor_id: "A2", avg_value: 81.3, max_value: 82.5, stddev_value: 1.3, reading_count: 2 }
  [window] ZoneBStats (B1): { sensor_id: "B1", avg_value: 66.8, max_value: 68.0, stddev_value: 1.3, reading_count: 3 }
  [window] ZoneBStats (B2): { sensor_id: "B2", avg_value: 72.0, max_value: 73.0, stddev_value: 1.0, reading_count: 2 }

Simulation Complete
======================
Events processed: 10
Contexts used:    2
```

### Without Contexts (Baseline)

For comparison, here's the same logic without contexts:

```vpl
// parallel_windows_single.vpl
// Same analytics, single-threaded

stream ZoneAStats = SensorReading
    .where(zone == "A")
    .partition_by(sensor_id)
    .window(tumbling 30s)
    .aggregate(
        sensor_id: sensor_id,
        avg_value: avg(value),
        max_value: max(value),
        stddev_value: stddev(value),
        reading_count: count()
    )

stream ZoneBStats = SensorReading
    .where(zone == "B")
    .partition_by(sensor_id)
    .window(tumbling 30s)
    .aggregate(
        sensor_id: sensor_id,
        avg_value: avg(value),
        max_value: max(value),
        stddev_value: stddev(value),
        reading_count: count()
    )
```

Same output, but both streams share one thread. Under high load, the context-based version will process events faster because zone A and zone B aggregation happen simultaneously.

**Key takeaway:** Contexts provide true parallelism for CPU-bound workloads. Partition your streams by workload domain (zone, region, customer) and assign each to its own context.

---

## Part 5: Session Windows in Contexts

Session windows group events by inactivity gaps rather than fixed time intervals. They're ideal for user session analytics: a session starts with the first event and ends when no events arrive for a specified duration.

The syntax is `.window(session: <gap>)`, where `<gap>` is the maximum inactivity period before the session closes.

### The Program

Create `session_contexts.vpl`:

```vpl
// session_contexts.vpl
// User session analytics with session windows in a dedicated context

context sessions_ctx

stream UserSessions = UserActivity
    .context(sessions_ctx)
    .partition_by(user_id)
    .window(session: 5s)
    .aggregate(
        user_id: user_id,
        event_count: count(),
        first_action: first(action),
        last_action: last(action)
    )
```

This creates session windows per user. If a user is inactive for more than 5 seconds, the session closes and emits a summary. Each user's sessions are tracked independently thanks to `partition_by`.

### Test Data

Create `session_contexts.evt`:

```
# session_contexts.evt
# Two users with different activity patterns
# User A: two sessions (gap at 15s)
# User B: one continuous session

@0 UserActivity { user_id: "alice", action: "login" }
@1000 UserActivity { user_id: "alice", action: "view_page" }
@2000 UserActivity { user_id: "bob", action: "login" }
@3000 UserActivity { user_id: "alice", action: "click" }
@4000 UserActivity { user_id: "bob", action: "view_page" }
@6000 UserActivity { user_id: "bob", action: "click" }
@8000 UserActivity { user_id: "bob", action: "purchase" }

# Alice is inactive for >5s -- her first session closes

@15000 UserActivity { user_id: "alice", action: "login" }
@16000 UserActivity { user_id: "alice", action: "view_page" }
@17000 UserActivity { user_id: "alice", action: "logout" }
```

Alice has two sessions: events at 0-3s, then a gap longer than 5s, then events at 15-17s. Bob has one continuous session from 2-8s.

### Run It

```bash
varpulis simulate -p session_contexts.vpl -e session_contexts.evt --immediate --verbose
```

**Expected output:**

```
Program loaded: 1 streams, 1 contexts

Starting simulation...

  [  1-10] 10 events dispatched to 1 context

  [session] UserSessions (alice): { user_id: "alice", event_count: 3, first_action: "login", last_action: "click" }
  [session] UserSessions (bob): { user_id: "bob", event_count: 4, first_action: "login", last_action: "purchase" }
  [session] UserSessions (alice): { user_id: "alice", event_count: 3, first_action: "login", last_action: "logout" }

Simulation Complete
======================
Events processed: 10
Contexts used:    1
Sessions closed:  3
```

Alice's first session (3 events) closes when the gap exceeds 5s. Bob's session (4 events) closes when no more events arrive. Alice's second session (3 events) closes at the end.

### How Sessions Close

Sessions close through two mechanisms:

1. **Event-driven gap detection**: When a new event arrives after a gap exceeding the configured duration, the current session closes immediately. This is the hot path -- it happens per-event with zero timer overhead.

2. **Periodic sweep**: If a stream goes idle (no events arrive), sessions would sit in memory forever with only gap detection. To prevent this, a background sweep runs every `gap` duration inside each context's event loop. It checks all session windows and closes any partition whose last event is older than the gap. In batch/simulation mode, the sweep runs once after all events are processed.

This means you don't need a "trailing event" to close the last session -- the sweep handles it automatically.

### Combining with Cross-Context Pipelines

Session windows pair well with cross-context pipelines. Here's a pattern where raw events are ingested in one context and session analytics run in another:

```vpl
// session_pipeline.vpl
context ingest
context sessions_ctx

stream ValidActivity = UserActivity
    .context(ingest)
    .where(user_id != "" and action != "")
    .emit(context: sessions_ctx, user_id: user_id, action: action)

stream UserSessions = ValidActivity
    .context(sessions_ctx)
    .partition_by(user_id)
    .window(session: 5s)
    .aggregate(
        user_id: user_id,
        event_count: count(),
        first_action: first(action),
        last_action: last(action)
    )
```

**Key takeaway:** Session windows with `.window(session: <gap>)` group events by inactivity. Combined with `partition_by` and contexts, you get per-entity session tracking running on a dedicated thread.

---

## Part 6: Going to Production

The examples above use simulation mode. When moving to production, there are three areas to consider.

### CPU Affinity

Pin contexts to specific CPU cores for predictable latency:

```vpl
context ingest (cores: [1])
context compute (cores: [2, 3])
context alert (cores: [4])
```

Guidelines:
- Avoid core 0 on Linux -- it handles system interrupts
- Keep cooperating contexts on the same NUMA node (check with `numactl --hardware`)
- Use `isolcpus` kernel parameter for lowest jitter
- On non-Linux platforms, the `cores` parameter is accepted but has no effect

### Channel Backpressure

Cross-context events are delivered via bounded `mpsc` channels (default capacity: 1000 events). When a consumer context is slower than a producer:

- The producer blocks until space is available
- This prevents unbounded memory growth
- But it can cause upstream contexts to stall

To handle this:
- Filter events before crossing context boundaries to reduce volume
- Use `--immediate` mode for batch processing to avoid timing delays
- Monitor channel utilization via Prometheus metrics

### Monitoring with Prometheus

Enable metrics to track per-context performance:

```bash
varpulis run --file pipeline.vpl --metrics --metrics-port 9090
```

Query context-specific metrics:

```bash
curl -s localhost:9090/metrics | grep varpulis_events_processed
```

Key metrics to watch:
- `varpulis_events_processed` -- events processed per context
- `varpulis_context_channel_utilization` -- how full the cross-context channels are
- `varpulis_window_flushes` -- window closure rate

---

## Quick Reference

| Syntax | Description |
|--------|-------------|
| `context name` | Declare a context |
| `context name (cores: [0, 1])` | Declare with CPU affinity |
| `.context(name)` | Assign a stream to a context |
| `.emit(context: target, ...)` | Forward events to another context |
| `.window(session: 5s)` | Session window with 5s inactivity gap |
| `.partition_by(field)` | Partition processing by key |

---

## Next Steps

- [Contexts Guide](../guides/contexts.md) -- Architecture deep-dive, internal design, and advanced best practices
- [Performance Tuning](../guides/performance-tuning.md) -- CPU pinning, NUMA optimization, and benchmarking
- [HVAC Building Example](../examples/hvac-building.md) -- Full IoT monitoring example using contexts
