# Schema Inference Guide

The `varpulis infer` command reads sample event data and generates VPL `event` type declarations automatically. Instead of writing event declarations by hand, point the tool at your data and copy the output into your VPL program.

## Basic Usage

### From .evt Files

Given a file `events.evt`:

```
StockTick { symbol: "AAPL", price: 150.0, volume: 1000 }
StockTick { symbol: "GOOG", price: 2800.5, volume: 500 }
Alert { severity: "high", active: true }
```

Run inference:

```bash
varpulis infer --input events.evt
```

Output:

```
event Alert:
    active: bool
    severity: str

event StockTick:
    price: float
    symbol: str
    volume: int
# Inferred 2 event type(s) from 3 event(s)
```

The declarations are printed to stdout. The summary line goes to stderr so it does not interfere with piping.

### From JSONL Files

Given a file `data.jsonl`:

```json
{"event_type": "Metric", "data": {"value": 10, "host": "server1"}}
{"event_type": "Metric", "data": {"value": 20.5, "host": "server2"}}
{"event_type": "Alert", "data": {"message": "disk full", "critical": true}}
```

Run inference:

```bash
varpulis infer --input data.jsonl
```

Output:

```
event Alert:
    critical: bool
    message: str

event Metric:
    host: str
    value: float
# Inferred 2 event type(s) from 3 event(s)
```

## JSONL Format Detection

The inference engine auto-detects the format of each line. Lines starting with `{` are parsed as JSON; all others are parsed as .evt format. This means mixed files work, though in practice your data will be one format or the other.

Two JSON layouts are supported:

**Nested format** (fields in a `data` object):

```json
{"event_type": "Sensor", "data": {"temperature": 23.5, "zone": "A1"}}
```

**Flat format** (fields at the top level):

```json
{"event_type": "Sensor", "temperature": 23.5, "zone": "A1"}
```

In both cases, the `event_type` field is required and used as the event type name. The `timestamp` field is automatically excluded from the generated declaration.

## Type Promotion Rules

When the same field has different types across events, the inference engine applies promotion rules:

| Observed Types | Inferred VPL Type | Rationale |
|----------------|-------------------|-----------|
| `int` only | `int` | Consistent integer values |
| `float` only | `float` | Consistent floating-point values |
| `str` only | `str` | Consistent string values |
| `bool` only | `bool` | Consistent boolean values |
| `int` + `float` | `float` | Integer is promotable to float |
| `null` + any type | that type | Null is ignored for type decisions |
| `null` only | `str` | Default when only nulls are seen |
| any mix with `str` | `str` | String is the universal fallback |
| array values | `list` | Any array-typed value |

For example, if a `value` field is `10` in one event and `20.5` in another, the inferred type is `float` (int + float promotes to float).

## Controlling Sample Size

By default, the inference engine reads all lines in the file. For large files, limit the sample:

```bash
varpulis infer --input large_dataset.jsonl --sample-size 500
```

This reads only the first 500 parseable events (skipping comments and blank lines). The sample size affects type inference accuracy: a small sample may not see all type variants for a field.

## Writing Output to a File

Write the inferred declarations directly to a file:

```bash
varpulis infer --input data.jsonl --output schema.vpl
```

```
Inferred 3 event type(s) from 1000 event(s), written to schema.vpl
```

The `--output` flag writes the VPL declarations to the specified file instead of stdout.

## Piping into a Pipeline File

Append inferred declarations to an existing VPL file:

```bash
varpulis infer --input data.jsonl >> pipeline.vpl
```

Or create a new file with declarations at the top, then add your stream logic below:

```bash
varpulis infer --input data.jsonl > my_pipeline.vpl
```

Then edit `my_pipeline.vpl` to add stream definitions after the generated event declarations.

## Handling .evt Timing Prefixes

The .evt format supports `@Ns` timing prefixes for simulation. These are stripped automatically during inference:

```
@0 Sensor { temp: 72.5, zone: "A" }
@100 Sensor { temp: 68.0, zone: "B" }
@200 Alert { severity: "warning", source: "monitor" }
```

```bash
varpulis infer --input timed_events.evt
```

```
event Alert:
    severity: str
    source: str

event Sensor:
    temp: float
    zone: str
# Inferred 2 event type(s) from 3 event(s)
```

Comment lines (starting with `#` or `//`) and `BATCH` directives are also skipped.

## Practical Workflow

A typical workflow for starting a new pipeline from existing data:

1. **Collect sample data**: Export events from your Kafka topic, MQTT broker, or application logs as JSONL.

2. **Infer the schema**:
   ```bash
   varpulis infer --input sample.jsonl --output pipeline.vpl
   ```

3. **Add stream definitions**: Open `pipeline.vpl` and add your processing logic after the event declarations:
   ```vpl
   // (generated event declarations above)

   stream HighCPU = ServerMetric
       .where(cpu_percent > 90)
       .alert(webhook: "https://hooks.slack.com/...", message: "High CPU on {hostname}")
       .emit(host: hostname, cpu: cpu_percent)
   ```

4. **Validate**:
   ```bash
   varpulis check pipeline.vpl
   ```

5. **Test with the original data**:
   ```bash
   varpulis simulate -p pipeline.vpl -e sample.jsonl -v -w 1
   ```

## Field Ordering

Inferred event types and their fields are sorted alphabetically in the output. This makes the output deterministic regardless of the order events appear in the input file.

## See Also

- [Getting Started](../tutorials/getting-started.md) -- Installation and first pipeline
- [VPL Language Tutorial](../tutorials/language-tutorial.md) -- Event declarations and stream definitions
- [Interactive Shell Tutorial](../tutorials/interactive-shell-tutorial.md) -- Type declarations interactively
- [Debugging Pipelines](debugging-pipelines.md) -- Verify your pipeline with trace mode
