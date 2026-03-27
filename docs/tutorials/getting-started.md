# Getting Started with Varpulis

This tutorial will get you up and running with Varpulis in under 5 minutes. You'll learn to write, validate, and run your first VPL program.

## Prerequisites

- **Rust 1.93+**: Install via [rustup](https://rustup.rs/)
- **Git**: For cloning the repository
- **Optional**: MQTT broker (Mosquitto) for real-time streaming

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/varpulis/varpulis.git
cd varpulis

# Build the CLI
cargo build --release

# Add to PATH (optional)
export PATH="$PATH:$(pwd)/target/release"

# Verify installation
varpulis --version
# Output: varpulis 0.9.0
```

### Quick Build Check

```bash
# Run tests to verify everything works
cargo test --workspace
```

## Your First VPL Program

Let's create a simple temperature monitoring program.

### Step 1: Create the Program File

Create a file called `temperature_monitor.vpl`:

```vpl
// temperature_monitor.vpl
// Simple temperature monitoring with alerts

// Declare the event type we expect
event TemperatureReading:
    sensor_id: str
    temperature: float
    unit: str

// Create an alert stream for high temperatures
stream HighTempAlerts = TemperatureReading
    .where(temperature > 100)
    .emit(
        alert_type: "HighTemperature",
        sensor: sensor_id,
        temp: temperature
    )
```

### Step 2: Validate the Syntax

Before running, check that your program has valid syntax:

```bash
varpulis check temperature_monitor.vpl
```

**Expected output:**
```
Syntax OK
   Statements: 2
```

If there's an error, you'll see helpful diagnostics:

```
Syntax error: unexpected token at line 5, column 10
   Hint: Expected 'where', 'select', or 'emit'
   |
   | stream HighTempAlerts form TemperatureReading
   |                       ^^^^
```

### Step 3: View the AST (Optional)

To understand how Varpulis parses your program:

```bash
varpulis parse temperature_monitor.vpl
```

This shows the Abstract Syntax Tree, useful for debugging complex expressions.

### Step 4: Run a Simulation

Create a test event file called `test_events.evt`:

```
# test_events.evt
# Format: @delay_ms EventType { field: value, ... }

@0 TemperatureReading { sensor_id: "sensor-1", temperature: 72.5, unit: "F" }
@100 TemperatureReading { sensor_id: "sensor-2", temperature: 68.0, unit: "F" }
@200 TemperatureReading { sensor_id: "sensor-1", temperature: 105.2, unit: "F" }
@300 TemperatureReading { sensor_id: "sensor-3", temperature: 71.8, unit: "F" }
@400 TemperatureReading { sensor_id: "sensor-1", temperature: 112.5, unit: "F" }
```

Now simulate the events:

```bash
varpulis simulate \
    --program temperature_monitor.vpl \
    --events test_events.evt \
    --verbose
```

**Expected output:**
```
Varpulis Event Simulation
============================
Program: temperature_monitor.vpl
Events:  test_events.evt
Mode:    timed
Workers: 1

Program loaded: 2 streams

Starting simulation...

  [  1] @     0ms TemperatureReading { ... }
  [  2] @   100ms TemperatureReading { ... }
  [  3] @   200ms TemperatureReading { ... }
ALERT: HighTemperature - Temperature exceeded 100 degrees
   sensor_id: sensor-1
   temperature: 105.2
  [  4] @   300ms TemperatureReading { ... }
  [  5] @   400ms TemperatureReading { ... }
ALERT: HighTemperature - Temperature exceeded 100 degrees
   sensor_id: sensor-1
   temperature: 112.5

Simulation Complete
======================
Duration:         0.402s
Events processed: 5
Workers used:     1
Alerts generated: 2
Event rate:       12.4 events/sec
```

## Faster Processing with Parallel Workers

By default, `simulate` runs in fast mode -- it preloads all events into memory and processes them as fast as possible with no timing delays. To scale across multiple CPU cores:

```bash
varpulis simulate \
    --program temperature_monitor.vpl \
    --events test_events.evt \
    --workers 8 \
    --partition-by sensor_id
```

If you want to replay events with real-time timing delays (e.g., to observe how alerts trigger over time), use `--timed`:

```bash
varpulis simulate \
    --program temperature_monitor.vpl \
    --events test_events.evt \
    --timed
```

## Adding Aggregations

Let's enhance our program with windowed aggregations:

```vpl
// temperature_monitor_v2.vpl

// Raw temperature readings
stream Readings = TemperatureReading

// Calculate average temperature over 1-minute windows
stream AvgTemperature = TemperatureReading
    .window(1m)
    .aggregate(
        avg_temp: avg(temperature),
        max_temp: max(temperature),
        min_temp: min(temperature),
        reading_count: count()
    )
    .print("Minute summary: avg={avg_temp}, max={max_temp}, min={min_temp}")

// Alert on sustained high temperatures (average > 90 over window)
stream SustainedHighTemp = TemperatureReading
    .window(1m)
    .aggregate(avg_temp: avg(temperature))
    .where(avg_temp > 90)
    .emit(
        alert_type: "SustainedHighTemp",
        avg_temp: avg_temp,
        message: "Average temperature exceeds threshold"
    )
```

## Running the Built-in Demo

Varpulis includes a HVAC building monitoring demo:

```bash
# Run for 30 seconds with simulated anomalies
varpulis demo --duration 30 --anomalies

# With Prometheus metrics
varpulis demo --duration 60 --anomalies --metrics --metrics-port 9090
```

Then open http://localhost:9090/metrics to see real-time metrics.

## Interactive Mode (Recommended for Learning)

The fastest way to explore VPL is the interactive shell — type definitions and
events directly, no files needed:

```bash
varpulis interactive
```

```
Varpulis Interactive Shell v0.9.0
Type VPL declarations (event, stream, ...) or event literals to inject.
Type :help for commands, :quit to exit.

vpl> event TemperatureReading:
...>     sensor_id: str
...>     temperature: float
vpl>
✓ 0 stream(s) loaded

vpl> stream HighTemp = TemperatureReading .where(temperature > 100) .emit(sensor: sensor_id, temp: temperature)
✓ 1 stream(s) loaded: HighTemp
  added: HighTemp

vpl> TemperatureReading { sensor_id: "sensor-1", temperature: 72.5 }

vpl> TemperatureReading { sensor_id: "sensor-1", temperature: 105.2 }
→ HighTemp: {"sensor":"sensor-1","temp":105.2}

vpl> TemperatureReading { sensor_id: "sensor-1", temperature: 112.5 }
→ HighTemp: {"sensor":"sensor-1","temp":112.5}

vpl> :streams
  HighTemp (source: stream:TemperatureReading, 2 ops)

vpl> :quit
Bye!
```

**Key features:**
- Type `event` or `stream` declarations and they compile incrementally
- Multi-line blocks: type `event Foo:`, then indented fields, blank line to submit
- Type event literals (e.g., `Sensor { temp: 150 }`) to inject and see results
- Commands: `:streams`, `:topology`, `:trace on`, `:gen fraud`, `:help`

### TUI Mode

For a split-pane visual experience with topology, events, and metrics:

```bash
varpulis interactive --tui --file temperature_monitor.vpl --trace
```

This opens a full terminal UI with 4 panes:
- **Top-left**: Pipeline topology graph
- **Top-right**: Scrolling event stream with trace (PASS/BLOCK indicators)
- **Bottom-left**: VPL input / command area
- **Bottom-right**: Live metrics dashboard

Key bindings: `Tab` (switch pane), `Ctrl+G` (toggle datagen), `Ctrl+T` (trace), `Ctrl+Q` (quit).

## Pipeline Trace (Explain Mode)

To understand exactly how events flow through your pipeline, use `--trace`:

```bash
varpulis simulate --trace \
    -p temperature_monitor.vpl \
    -e test_events.evt \
    -w 1
```

```
EVENT [1/5] TemperatureReading { sensor_id="sensor-1", temperature=72.5, unit="F" }
  -> stream HighTempAlerts matched on TemperatureReading
     | Filter BLOCK

EVENT [3/5] TemperatureReading { sensor_id="sensor-1", temperature=105.2, unit="F" }
  -> stream HighTempAlerts matched on TemperatureReading
     | Filter PASS
  <- HighTempAlerts emitted { alert_type="HighTemperature", sensor="sensor-1", temp=105.2 }
```

Each event shows: which streams matched, which operators passed (green) or
blocked (red), and what output was emitted. Essential for debugging complex
pipelines.

## Schema Inference

Don't want to write event declarations manually? Use `varpulis infer` to
generate them from sample data:

```bash
varpulis infer --input test_events.evt
```

```
event TemperatureReading:
    sensor_id: str
    temperature: float
    unit: str
# Inferred 1 event type(s) from 5 event(s)
```

Copy the output into your VPL file — instant type declarations.

## Next Steps

Now that you have Varpulis running:

1. **Learn the Language**: Read the [VPL Language Tutorial](language-tutorial.md) for comprehensive coverage of streams, patterns, windows, and more.

2. **Explore Windows**: See the [Windows & Aggregations Reference](../reference/windows-aggregations.md) for tumbling, sliding, and count-based windows.

3. **Pattern Matching**: Learn about SASE+ patterns in the [Pattern Guide](../guides/sase-patterns.md) for detecting complex event sequences.

4. **Production Setup**: Review the [Configuration Guide](../guides/configuration.md) for MQTT integration, TLS, and deployment options.

## Quick Reference

| Command | Purpose |
|---------|---------|
| `varpulis interactive` | Interactive shell (type VPL + events live) |
| `varpulis interactive --tui` | Split-pane terminal UI |
| `varpulis interactive --json` | JSON-line protocol (for agents) |
| `varpulis check file.vpl` | Validate syntax |
| `varpulis simulate -p file.vpl -e events.evt` | Run simulation |
| `varpulis simulate --trace ...` | Explain mode (per-event flow) |
| `varpulis simulate --watch ...` | Auto-reload on file changes |
| `varpulis infer --input data.jsonl` | Infer event type declarations |
| `varpulis connector list` | Show available connectors |
| `varpulis run --file file.vpl` | Run with live connectors |
| `varpulis demo` | Built-in HVAC demo |
| `varpulis server` | Start WebSocket API |

## Troubleshooting

**"Parse error: unexpected token"**
- Check for typos in keywords (`where`, `emit`)
- Ensure strings are quoted: `"value"` not `value`
- Verify brackets match: `{ }` for blocks, `( )` for function calls

**"No events processed"**
- Check event file format matches expected event types
- Verify the stream declaration (`stream Name = EventType`) matches the event type exactly

**"Simulation runs slowly"**
- The default mode already runs as fast as possible (no timing delays, events preloaded)
- Use `--workers N` for parallel processing
- If using `--timed` or `--streaming`, switch to the default fast mode for maximum throughput

For more help, see the [Troubleshooting Guide](../guides/troubleshooting.md).
