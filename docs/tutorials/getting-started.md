# Getting Started with Varpulis

This tutorial will get you up and running with Varpulis in under 5 minutes. You'll learn to write, validate, and run your first VPL program.

## Prerequisites

- **Rust 1.75+**: Install via [rustup](https://rustup.rs/)
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
# Output: varpulis 0.1.0
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

// Define what events we're listening for
stream TemperatureReadings from TemperatureReading
    .where(temperature > 0)  // Basic validation

// Create an alert stream for high temperatures
stream HighTempAlerts from TemperatureReading
    .where(temperature > 100)
    .emit(
        alert_type: "HighTemperature",
        message: "Temperature exceeded 100 degrees"
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

## Faster Processing with Immediate Mode

For larger datasets, use immediate mode to skip timing delays:

```bash
varpulis simulate \
    --program temperature_monitor.vpl \
    --events test_events.evt \
    --immediate
```

For even faster processing with multiple CPU cores:

```bash
varpulis simulate \
    --program temperature_monitor.vpl \
    --events test_events.evt \
    --immediate \
    --workers 8 \
    --partition-by sensor_id
```

## Adding Aggregations

Let's enhance our program with windowed aggregations:

```vpl
// temperature_monitor_v2.vpl

// Raw temperature readings
stream Readings from TemperatureReading

// Calculate average temperature over 1-minute windows
stream AvgTemperature from TemperatureReading
    .window(1m)
    .aggregate(
        avg_temp: avg(temperature),
        max_temp: max(temperature),
        min_temp: min(temperature),
        reading_count: count()
    )
    .print("Minute summary: avg={avg_temp}, max={max_temp}, min={min_temp}")

// Alert on sustained high temperatures (average > 90 over window)
stream SustainedHighTemp from TemperatureReading
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

## Next Steps

Now that you have Varpulis running:

1. **Learn the Language**: Read the [VPL Language Tutorial](language-tutorial.md) for comprehensive coverage of streams, patterns, windows, and more.

2. **Explore Windows**: See the [Windows & Aggregations Reference](../reference/windows-aggregations.md) for tumbling, sliding, and count-based windows.

3. **Pattern Matching**: Learn about SASE+ patterns in the [Pattern Guide](../guides/sase-patterns.md) for detecting complex event sequences.

4. **Production Setup**: Review the [Configuration Guide](../guides/configuration.md) for MQTT integration, TLS, and deployment options.

## Quick Reference

| Command | Purpose |
|---------|---------|
| `varpulis check file.vpl` | Validate syntax |
| `varpulis parse file.vpl` | Show AST |
| `varpulis simulate -p file.vpl -e events.evt` | Run simulation |
| `varpulis simulate ... --immediate --workers 8` | Fast parallel mode |
| `varpulis run --file file.vpl` | Run with MQTT |
| `varpulis demo` | Built-in HVAC demo |
| `varpulis server` | Start WebSocket API |

## Troubleshooting

**"Parse error: unexpected token"**
- Check for typos in keywords (`from`, `where`, `emit`)
- Ensure strings are quoted: `"value"` not `value`
- Verify brackets match: `{ }` for blocks, `( )` for function calls

**"No events processed"**
- Check event file format matches expected event types
- Verify the `from` clause matches the event type exactly

**"Simulation runs slowly"**
- Use `--immediate` to skip timing delays
- Add `--preload` to load events into memory
- Use `--workers N` for parallel processing

For more help, see the [Troubleshooting Guide](../guides/troubleshooting.md).
