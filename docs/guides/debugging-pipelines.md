# Debugging Pipelines

Varpulis provides several tools for understanding how events flow through your pipeline, diagnosing why events are filtered or missed, and iterating quickly on pipeline logic.

## Trace Mode

Trace mode shows the step-by-step path of each event through every stream and operator in your pipeline. It answers the question: "Why did this event produce (or not produce) output?"

### Running a Traced Simulation

```bash
varpulis simulate --trace -p pipeline.vpl -e events.evt -w 1
```

The `--trace` flag forces single-threaded execution (`-w 1` is implicit) so that trace output appears in event order.

### Reading Trace Output

For each event, trace mode prints:

1. An **EVENT** header with the event type and fields
2. For each stream that matches the event type:
   - A `->` line showing the stream name and match
   - One line per operator showing **PASS** (green) or **BLOCK** (red)
3. If the event produces output, a `<-` line showing the emitted fields

Example output for a temperature monitoring pipeline:

```
EVENT [1/5] TemperatureReading { sensor_id=sensor-1, temperature=72.5, unit=F }
  -> stream HighTempAlerts matched on TemperatureReading
     | Filter BLOCK

EVENT [2/5] TemperatureReading { sensor_id=sensor-2, temperature=68.0, unit=F }
  -> stream HighTempAlerts matched on TemperatureReading
     | Filter BLOCK

EVENT [3/5] TemperatureReading { sensor_id=sensor-1, temperature=105.2, unit=F }
  -> stream HighTempAlerts matched on TemperatureReading
     | Filter PASS
  <- HighTempAlerts emitted { alert_type=HighTemperature, sensor=sensor-1, temp=105.2 }
```

### What Each Line Means

| Prefix | Meaning |
|--------|---------|
| `EVENT [n/total]` | The nth event being processed, with its type and fields |
| `-> stream X matched on Y` | Stream X accepts events of type Y |
| `\| Filter PASS` | The `.where()` condition evaluated to true |
| `\| Filter BLOCK` | The `.where()` condition evaluated to false -- event stops here |
| `\| Emit PASS` | The `.emit()` operator produced output fields |
| `\| Window PASS` | The event entered a window |
| `\| Aggregate PASS` | A window aggregation produced a result |
| `pattern: N active run(s), M completed` | SASE pattern state: N partial matches in progress, M completed |
| `<- StreamName emitted { ... }` | The stream produced an output event with these fields |
| `-> no matching streams` | No stream accepts this event type |

### Debugging Common Issues

**Events show "no matching streams"**: The event type name does not match any stream's source. Check that `stream X = EventTypeName` matches the actual event type exactly (case-sensitive).

**Filter always BLOCKs**: The `.where()` condition never evaluates to true. Check field names and comparison values. A common mistake is comparing a string field without quotes: `.where(status == active)` should be `.where(status == "active")`.

**Pattern never completes**: Sequence patterns require events to arrive in order within the time window. Use trace mode to see `active run(s)` count -- if runs start but never complete, check that all required event types are present and arrive within `.within()`.

## Watch Mode

Watch mode automatically re-runs the simulation whenever you save changes to the VPL program or event file. This creates a tight edit-test feedback loop.

### Starting Watch Mode

```bash
varpulis simulate --watch -p pipeline.vpl -e events.evt
```

```
Varpulis Watch Mode
Watching: /home/user/project/pipeline.vpl
Watching: /home/user/project/events.evt
Press Ctrl+C to stop.

Varpulis Event Simulation
============================
Program: pipeline.vpl
Events:  events.evt
...

--- Watching for changes... ---
```

The simulation runs once immediately, then waits for file changes.

### Workflow

1. Open your VPL file in an editor (VS Code, vim, etc.)
2. Start watch mode in a terminal alongside the editor
3. Edit the VPL file and save
4. Watch mode detects the change, clears the terminal, and re-runs the simulation
5. See the new results immediately

### Parse Error Recovery

If you save a file with a syntax error, watch mode shows the error and keeps watching:

```
Parse error: unexpected token at line 5, column 10
   Hint: Expected 'where', 'select', or 'emit'

--- Watching for changes... ---
```

Fix the syntax error and save again -- watch mode picks up the corrected file automatically.

### Combining Watch with Trace

For maximum debugging feedback during development:

```bash
varpulis simulate --watch --trace -p pipeline.vpl -e events.evt
```

Every save triggers a full trace run, so you see the per-event PASS/BLOCK flow after each edit.

### Debouncing

Watch mode debounces file system events with a 300ms window. Rapid saves from editors that perform atomic writes (save to temp file, then rename) are coalesced into a single re-run.

## Interactive Debugging

The interactive shell provides the most granular debugging experience: inject events one at a time and see exactly what happens.

### Step-by-Step Event Injection

```bash
varpulis interactive
```

```
vpl> event LoginAttempt:
...>     username: str
...>     success: bool
...>     ip_address: str
...>

vpl> stream FailedLogins = LoginAttempt .where(success == false) .emit(user: username, ip: ip_address)
✓ 1 stream(s) loaded: FailedLogins
  added: FailedLogins

vpl> :trace on
✓ Trace enabled

vpl> LoginAttempt { username: "admin", success: true, ip_address: "10.0.0.1" }
  -> stream FailedLogins matched on LoginAttempt
     | Filter BLOCK

vpl> LoginAttempt { username: "admin", success: false, ip_address: "192.168.1.99" }
  -> stream FailedLogins matched on LoginAttempt
     | Filter PASS
  <- FailedLogins emitted { user=admin, ip=192.168.1.99 }
→ FailedLogins: {"user":"admin","ip":"192.168.1.99"}
```

### Iterating on Definitions

In the interactive shell, you can redefine streams without restarting:

```
vpl> stream FailedLogins = LoginAttempt .where(success == false && ip_address != "10.0.0.1") .emit(user: username, ip: ip_address)
✓ 1 stream(s) loaded: FailedLogins
  added: FailedLogins
```

Each new stream definition with the same name replaces the previous one. This lets you tweak conditions and immediately re-test with the same events.

### Using :reset

If you need to start completely fresh:

```
vpl> :reset
✓ Session reset -- all definitions and state cleared
```

This clears all event declarations, stream definitions, and engine state. Useful when you want to test a pattern from a clean slate (e.g., sequence patterns that depend on event ordering).

## Connector Discovery

Before building pipelines with external connectors, use the connector commands to discover what is available.

### Listing Available Connectors

```bash
varpulis connector list
```

```
┌────────────┬──────────────────────────────┬────────┬──────┬─────────┐
│ Type       │ Description                  │ Source │ Sink │ Managed │
├────────────┼──────────────────────────────┼────────┼──────┼─────────┤
│ kafka      │ Apache Kafka                 │ ✓      │ ✓    │         │
│ mqtt       │ MQTT v3.1.1/v5              │ ✓      │ ✓    │         │
│ nats       │ NATS JetStream              │ ✓      │ ✓    │         │
│ postgres   │ PostgreSQL CDC              │ ✓      │      │         │
│ http       │ HTTP/HTTPS webhooks         │        │ ✓    │         │
│ stdout     │ Standard output             │        │ ✓    │         │
└────────────┴──────────────────────────────┴────────┴──────┴─────────┘
```

### Getting Connector Details

```bash
varpulis connector info kafka
```

This shows the connector's configuration parameters, supported features, and example VPL syntax. Use this to verify connection strings and available options before writing your pipeline.

### Testing Connectivity

```bash
varpulis connector test kafka --url localhost:9092
```

Tests that the broker is reachable. Useful for diagnosing "no events arriving" issues -- verify the connector can reach the service before debugging the pipeline logic.

## Debugging Checklist

When a pipeline is not producing expected output, work through these steps:

1. **Validate syntax**: `varpulis check pipeline.vpl` -- catches parse errors before runtime.

2. **Run with trace**: `varpulis simulate --trace -p pipeline.vpl -e events.evt -w 1` -- shows exactly where events are blocked.

3. **Check event type names**: Ensure the event type in your `.evt` file matches the stream source exactly. `TemperatureReading` is not `temperatureReading`.

4. **Check field names**: Field references in `.where()` and `.emit()` must match the actual event field names.

5. **Check partition key**: When using multiple workers, events are hash-partitioned. Sequence patterns require all events for a given entity to land on the same worker. Use `--partition-by user_id` (or whichever field groups related events) or `-w 1` to rule out partitioning issues.

6. **Simplify**: Remove operators one at a time until output appears. Then add them back to find which operator is blocking.

7. **Use interactive mode**: For sequence patterns, inject events one at a time with `:trace on` to see pattern state transitions.

## Verbose Mode

For a lighter alternative to trace mode, use `--verbose` (`-v`) to see each output event as it is emitted without the per-operator detail:

```bash
varpulis simulate -p pipeline.vpl -e events.evt -v -w 1
```

```
OUTPUT EVENT: HighTempAlerts - {"alert_type": "HighTemperature", "sensor": "sensor-1", "temp": 105.2}
OUTPUT EVENT: HighTempAlerts - {"alert_type": "HighTemperature", "sensor": "sensor-1", "temp": 112.5}
```

This is useful when you know the pipeline logic is correct and just want to confirm which events produce output.

## See Also

- [Getting Started](../tutorials/getting-started.md) -- Installation and first pipeline
- [Interactive Shell Tutorial](../tutorials/interactive-shell-tutorial.md) -- Interactive shell in depth
- [SASE+ Pattern Guide](sase-patterns.md) -- Sequence pattern debugging specifics
- [Troubleshooting Guide](troubleshooting.md) -- Common error messages and fixes
- [Schema Inference Guide](schema-inference.md) -- Generate event declarations from sample data
