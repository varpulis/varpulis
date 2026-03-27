# Interactive Session Protocol Reference

The Varpulis interactive session uses a JSON-line protocol for communication between clients and the engine. Each message is a single JSON object on one line, terminated by a newline. This protocol is used by `varpulis interactive --json`, the MCP server, and any programmatic driver.

## Overview

The protocol follows a request-response model:

- **Commands** (client to session): JSON objects with a `"cmd"` tag field
- **Responses** (session to client): JSON objects with a `"type"` tag field

Commands are synchronous: each command returns zero or more responses immediately. Asynchronous output (events from generators or subscriptions) arrives on a broadcast channel accessible through the MCP tools or the TUI.

## Commands

All commands use `#[serde(tag = "cmd", rename_all = "snake_case")]` serialization. The `cmd` field determines the command type.

### load_vpl

Load a VPL program from an inline string. Replaces the current program entirely.

```json
{"cmd": "load_vpl", "vpl": "event Sensor:\n    temp: float\n\nstream Hot = Sensor .where(temp > 100) .emit(t: temp)"}
```

Response: `loaded`

### append_vpl

Append VPL declarations to the current program and recompile. New streams are added; existing streams with the same name are replaced.

```json
{"cmd": "append_vpl", "vpl": "stream Cold = Sensor .where(temp < 0) .emit(t: temp)"}
```

Response: `loaded`

### load_file

Load a VPL program from a file path on disk.

```json
{"cmd": "load_file", "path": "/home/user/pipeline.vpl"}
```

Response: `loaded` or `error`

### inject

Inject a single event into the engine.

```json
{"cmd": "inject", "event_type": "Sensor", "data": {"temp": 150, "zone": "A"}}
```

Response: zero or more `output` responses (one per matching stream that produces output), optionally followed by `trace` entries if tracing is enabled.

### inject_file

Inject all events from a file (.evt or JSONL format).

```json
{"cmd": "inject_file", "path": "/home/user/events.evt"}
```

Response: zero or more `output` responses, optionally followed by `trace` entries.

### generate

Start the built-in event generator with the specified schema.

```json
{"cmd": "generate", "schema": "fraud", "rate": 500, "duration": 30}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `schema` | string | Yes | -- | Generator schema: `"fraud"`, `"iot"`, or `"trading"` |
| `rate` | integer | No | 1000 | Events per second |
| `duration` | integer | No | null | Duration in seconds (null = run until stopped) |

Response: `generator_status`

### stop_generate

Stop the running event generator.

```json
{"cmd": "stop_generate"}
```

Response: `generator_status`

### subscribe

Subscribe to output events from a specific stream or all streams.

```json
{"cmd": "subscribe", "stream": "Alerts"}
```

```json
{"cmd": "subscribe", "stream": null}
```

Pass `null` or omit `stream` to subscribe to all streams.

### unsubscribe

Unsubscribe from a specific stream or all streams.

```json
{"cmd": "unsubscribe", "stream": "Alerts"}
```

### get_streams

List all loaded stream definitions.

```json
{"cmd": "get_streams"}
```

Response: `streams`

### get_metrics

Get engine metrics snapshot.

```json
{"cmd": "get_metrics"}
```

Response: `metrics`

### get_topology

Get the pipeline topology graph (nodes and edges).

```json
{"cmd": "get_topology"}
```

Response: `topology`

### set_trace

Enable or disable pipeline tracing. When enabled, each `inject` command returns additional `trace` responses showing per-operator PASS/BLOCK results.

```json
{"cmd": "set_trace", "enabled": true}
```

### quit

Terminate the session.

```json
{"cmd": "quit"}
```

Response: `bye`

## Responses

All responses use `#[serde(tag = "type", rename_all = "snake_case")]` serialization. The `type` field determines the response type.

### ready

Sent once when the session starts. Indicates the session is ready to accept commands.

```json
{"type": "ready", "version": "0.9.0"}
```

### loaded

A VPL program was loaded or updated.

```json
{
  "type": "loaded",
  "streams": ["HighTemp", "LowTemp"],
  "added": ["LowTemp"],
  "removed": [],
  "preserved": ["HighTemp"]
}
```

| Field | Description |
|-------|-------------|
| `streams` | All currently loaded stream names |
| `added` | Streams that are new in this load |
| `removed` | Streams that were removed by this load |
| `preserved` | Streams that existed before and still exist |

### output

An output event emitted by the pipeline.

```json
{
  "type": "output",
  "stream": "HighTemp",
  "event": {"temperature": 105.2, "zone": "A"},
  "timestamp": "2026-03-26T10:30:00Z"
}
```

### trace

Trace entries from the last operation (only when tracing is enabled).

```json
{
  "type": "trace",
  "entries": [
    {"kind": "stream_matched", "stream": "HighTemp", "detail": null},
    {"kind": "operator_result", "stream": "HighTemp", "detail": "Filter PASS"},
    {"kind": "event_emitted", "stream": "HighTemp", "detail": "temperature=105.2, zone=A"}
  ]
}
```

Each entry has:

| Field | Description |
|-------|-------------|
| `kind` | Entry type: `"stream_matched"`, `"operator_result"`, `"pattern_state"`, `"event_emitted"` |
| `stream` | Stream name (if applicable) |
| `detail` | Human-readable detail string (if applicable) |

### metrics

Engine metrics snapshot.

```json
{
  "type": "metrics",
  "events_processed": 1000,
  "output_events": 42,
  "streams_count": 3
}
```

### topology

Pipeline topology as a directed graph.

```json
{
  "type": "topology",
  "nodes": [
    {"id": "source_Sensor", "label": "Sensor", "node_type": "source", "config": {}, "position": null},
    {"id": "stream_HighTemp", "label": "HighTemp", "node_type": "stream", "config": {}, "position": null}
  ],
  "edges": [
    {"id": "e1", "source": "source_Sensor", "target": "stream_HighTemp"}
  ]
}
```

### streams

List of loaded stream definitions.

```json
{
  "type": "streams",
  "streams": [
    {"name": "HighTemp", "source": "event:Sensor", "ops_count": 2},
    {"name": "LowTemp", "source": "event:Sensor", "ops_count": 2}
  ]
}
```

### error

An error occurred processing a command.

```json
{"type": "error", "message": "Parse error: unexpected token at line 1, column 15"}
```

### generator_status

Status update from the event generator.

```json
{"type": "generator_status", "running": true, "generated": 5000}
```

### bye

Session is shutting down (response to `quit` command).

```json
{"type": "bye"}
```

## MCP Tool Mapping

The Varpulis MCP server exposes three tools that wrap the interactive session protocol:

| MCP Tool | Protocol Commands Used |
|----------|----------------------|
| `start_interactive_session` | Creates a new session, optionally sends `load_vpl` |
| `send_interactive_command` | Forwards any `SessionCommand` JSON to the session |
| `get_interactive_events` | Retrieves buffered `output` responses |

An AI agent workflow looks like:

1. Call `start_interactive_session` with optional VPL source. Receives a `session_id`.
2. Call `send_interactive_command` with the `session_id` and a command JSON (e.g., `{"cmd": "inject", ...}`).
3. Call `get_interactive_events` to retrieve output events that were produced.

Sessions expire after 10 minutes of inactivity.

## Agent Integration

### Piping Commands from a Script

The `--json` mode reads from stdin and writes to stdout, so it can be driven by any language:

```bash
#!/bin/bash
# drive_session.sh -- Example: load a pipeline, inject events, read output

{
  # Load the pipeline
  echo '{"cmd":"load_vpl","vpl":"event Tick:\\n    price: float\\n\\nstream High = Tick .where(price > 100) .emit(p: price)"}'

  # Inject events
  echo '{"cmd":"inject","event_type":"Tick","data":{"price":50}}'
  echo '{"cmd":"inject","event_type":"Tick","data":{"price":150}}'
  echo '{"cmd":"inject","event_type":"Tick","data":{"price":200}}'

  # Query state
  echo '{"cmd":"get_metrics"}'
  echo '{"cmd":"get_streams"}'

  # Quit
  echo '{"cmd":"quit"}'
} | varpulis interactive --json
```

Expected output (one JSON object per line):

```json
{"type":"ready","version":"0.9.0"}
{"type":"loaded","streams":["High"],"added":["High"],"removed":[],"preserved":[]}
{"type":"output","stream":"High","event":{"p":150},"timestamp":"2026-03-26T10:00:00Z"}
{"type":"output","stream":"High","event":{"p":200},"timestamp":"2026-03-26T10:00:00Z"}
{"type":"metrics","events_processed":3,"output_events":2,"streams_count":1}
{"type":"streams","streams":[{"name":"High","source":"event:Tick","ops_count":2}]}
{"type":"bye"}
```

### Python Example

```python
import subprocess
import json

proc = subprocess.Popen(
    ["varpulis", "interactive", "--json"],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    text=True,
)

def send(cmd):
    proc.stdin.write(json.dumps(cmd) + "\n")
    proc.stdin.flush()

def recv():
    line = proc.stdout.readline()
    return json.loads(line) if line else None

# Read the ready message
ready = recv()
assert ready["type"] == "ready"

# Load a pipeline
send({"cmd": "load_vpl", "vpl": "event Sensor:\n    temp: float\n\nstream Hot = Sensor .where(temp > 100) .emit(t: temp)"})
loaded = recv()
assert "Hot" in loaded["streams"]

# Inject an event that triggers output
send({"cmd": "inject", "event_type": "Sensor", "data": {"temp": 150}})
output = recv()
assert output["type"] == "output"
assert output["event"]["t"] == 150

# Clean up
send({"cmd": "quit"})
bye = recv()
assert bye["type"] == "bye"
proc.wait()
```

### Enabling Trace in JSON Mode

Start with `--trace` or send a `set_trace` command:

```bash
varpulis interactive --json --trace
```

Or dynamically:

```json
{"cmd": "set_trace", "enabled": true}
```

When tracing is enabled, `inject` commands produce additional `trace` response objects after any `output` responses. This allows programmatic analysis of per-operator behavior.

## Error Handling

All errors are returned as `error` responses -- the session never crashes or disconnects due to a bad command. Invalid JSON on stdin produces an error response and the session continues:

```json
{"type": "error", "message": "invalid JSON: expected value at line 1 column 1"}
```

Unknown command names also produce errors:

```json
{"type": "error", "message": "unknown variant `foo`, expected one of `load_vpl`, `append_vpl`, ..."}
```

## See Also

- [Interactive Shell Tutorial](../tutorials/interactive-shell-tutorial.md) -- Human-friendly shell usage
- [MCP Integration Reference](mcp-integration.md) -- AI agent integration via MCP tools
- [Getting Started](../tutorials/getting-started.md) -- Installation and first pipeline
- [Debugging Pipelines](../guides/debugging-pipelines.md) -- Trace mode and watch mode
