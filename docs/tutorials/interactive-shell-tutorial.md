# Interactive Shell Tutorial

The Varpulis interactive shell lets you build streaming pipelines incrementally, like a Python interpreter for event processing. Type event declarations, stream definitions, and event literals directly -- no files needed.

## Starting the Shell

```bash
varpulis interactive
```

```
Varpulis Interactive Shell v0.9.0
Type VPL declarations (event, stream, ...) or event literals to inject.
Type :help for commands, :quit to exit.

vpl>
```

The `vpl>` prompt indicates the shell is ready for input. There are three kinds of input:

1. **VPL declarations** (event types, stream definitions) -- these accumulate into your program
2. **Event literals** (e.g., `Sensor { temp: 150 }`) -- injected into the engine immediately
3. **Commands** (prefixed with `:`) -- control the shell itself

## Declaring Event Types

Event declarations use the same colon-block syntax as `.vpl` files. Type the header line ending with `:`, and the shell switches to continuation mode (`...>`) for the indented field lines. Submit the block by pressing Enter on a blank line.

```
vpl> event Sensor:
...>     temp: float
...>     zone: str
...>
```

The blank line signals end-of-block. The event type is now registered.

## Defining Streams

Stream definitions are typically single-line, so they compile immediately:

```
vpl> stream HighTemp = Sensor .where(temp > 100) .emit(temperature: temp, zone: zone)
✓ 1 stream(s) loaded: HighTemp
  added: HighTemp
```

The shell confirms which streams were loaded and which are new.

## Injecting Events

Type an event literal to inject it into the engine and see output:

```
vpl> Sensor { temp: 150, zone: "A" }
→ HighTemp: {"temperature":150,"zone":"A"}

vpl> Sensor { temp: 50, zone: "B" }

vpl>
```

The first event passes the `temp > 100` filter and produces output. The second is filtered out silently.

## Full Example Session

Here is a complete session building a fraud detection pipeline from scratch:

```
vpl> event Transaction:
...>     user_id: str
...>     amount: float
...>     merchant: str
...>     country: str
...>

vpl> stream LargeTransactions = Transaction .where(amount > 5000) .emit(user: user_id, amount: amount, merchant: merchant)
✓ 1 stream(s) loaded: LargeTransactions
  added: LargeTransactions

vpl> Transaction { user_id: "u001", amount: 250.0, merchant: "Coffee Shop", country: "US" }

vpl> Transaction { user_id: "u001", amount: 12500.0, merchant: "Luxury Watches", country: "CH" }
→ LargeTransactions: {"user":"u001","amount":12500,"merchant":"Luxury Watches"}

vpl> stream ForeignLarge = Transaction .where(amount > 5000 && country != "US") .emit(user: user_id, amount: amount, country: country)
✓ 2 stream(s) loaded: LargeTransactions, ForeignLarge
  added: ForeignLarge

vpl> Transaction { user_id: "u002", amount: 8000.0, merchant: "Electronics", country: "RU" }
→ LargeTransactions: {"user":"u002","amount":8000,"merchant":"Electronics"}
→ ForeignLarge: {"user":"u002","amount":8000,"country":"RU"}

vpl> :streams
  LargeTransactions (source: event:Transaction, 2 ops)
  ForeignLarge (source: event:Transaction, 2 ops)

vpl> :quit
Bye!
```

Key things to notice:

- Stream definitions accumulate -- adding `ForeignLarge` does not remove `LargeTransactions`
- A single injected event can trigger multiple streams
- The `:streams` command shows what is currently loaded

## Shell Commands

| Command | Description |
|---------|-------------|
| `:help` | Show available commands |
| `:load <file.vpl>` | Load a VPL file (replaces current program) |
| `:streams` | List loaded stream definitions |
| `:topology` | Show the pipeline graph structure |
| `:metrics` | Show engine metrics (events processed, outputs emitted) |
| `:trace on` | Enable per-event trace output (shows PASS/BLOCK for each operator) |
| `:trace off` | Disable trace output |
| `:gen fraud` | Start the built-in event generator (schemas: `fraud`, `iot`, `trading`) |
| `:stop` | Stop the event generator |
| `:reset` | Clear all definitions and engine state |
| `:quit` | Exit the shell |

## Trace Mode

Enable trace mode to see exactly how each event flows through the pipeline:

```
vpl> :trace on
✓ Trace enabled

vpl> Sensor { temp: 150, zone: "A" }
  -> stream HighTemp matched on Sensor
     | Filter PASS
  <- HighTemp emitted { temperature=150, zone=A }
→ HighTemp: {"temperature":150,"zone":"A"}

vpl> Sensor { temp: 50, zone: "B" }
  -> stream HighTemp matched on Sensor
     | Filter BLOCK
```

Each operator shows a green **PASS** or red **BLOCK** indicator. This is invaluable for understanding why events are or are not producing output.

## Loading a File at Start

To pre-load a VPL file when launching the shell:

```bash
varpulis interactive --file pipeline.vpl
```

```
✓ Loaded pipeline.vpl (3 streams)

Varpulis Interactive Shell v0.9.0
Type VPL declarations (event, stream, ...) or event literals to inject.
Type :help for commands, :quit to exit.

vpl>
```

The file is loaded before the prompt appears. You can then inject events or add more definitions on top of the loaded program.

## Combining with Data Generation

Start the shell with a built-in event generator to immediately see a pipeline in action:

```bash
varpulis interactive --file fraud_detection.vpl --generate fraud --rate 500
```

This loads the VPL file and starts generating 500 synthetic fraud-scenario events per second. Output events from matching streams appear in the terminal as they are emitted. Stop the generator with `:stop`, or adjust the schema with `:gen iot`.

## TUI Mode

For a split-pane visual experience:

```bash
varpulis interactive --tui
```

This opens a full terminal UI with four panes:

- **Top-left**: Pipeline topology graph (stream dependency visualization)
- **Top-right**: Scrolling event log with trace entries (PASS/BLOCK indicators)
- **Bottom-left**: VPL input and command area
- **Bottom-right**: Live metrics dashboard (events processed, outputs, stream count)

Key bindings:

| Key | Action |
|-----|--------|
| `Tab` | Switch active pane |
| `Ctrl+G` | Toggle datagen on/off |
| `Ctrl+T` | Toggle trace mode |
| `Ctrl+Q` | Quit the TUI |

Combine with `--file` and `--trace` for a pre-loaded, traced session:

```bash
varpulis interactive --tui --file temperature_monitor.vpl --trace
```

## JSON-Line Mode (for Agents)

For programmatic use by AI agents, scripts, or test harnesses:

```bash
varpulis interactive --json
```

In this mode, the shell reads JSON commands from stdin (one per line) and writes JSON responses to stdout (one per line). The protocol is documented in the [Interactive Session Protocol Reference](../reference/interactive-protocol.md).

Example session (input lines prefixed with `>`, output with `<`):

```
< {"type":"ready","version":"0.9.0"}
> {"cmd":"load_vpl","vpl":"event Sensor:\n    temp: float\n\nstream Hot = Sensor .where(temp > 100) .emit(t: temp)"}
< {"type":"loaded","streams":["Hot"],"added":["Hot"],"removed":[],"preserved":[]}
> {"cmd":"inject","event_type":"Sensor","data":{"temp":150}}
< {"type":"output","stream":"Hot","event":{"t":150},"timestamp":"2026-03-26T10:00:00Z"}
> {"cmd":"get_streams"}
< {"type":"streams","streams":[{"name":"Hot","source":"event:Sensor","ops_count":2}]}
> {"cmd":"quit"}
< {"type":"bye"}
```

This mode is used by the MCP server tools (`start_interactive_session`, `send_interactive_command`, `get_interactive_events`) to let AI agents drive interactive sessions programmatically.

## Tips

- **Iterative development**: Start with a simple `.where()` filter, inject test events, then layer on `.emit()`, sequence patterns, and aggregations one at a time.
- **Multi-line blocks**: Any line starting with a VPL keyword (`event`, `stream`, `connector`, `fn`, `context`) and ending with `:` starts multi-line accumulation. An empty line submits it.
- **History**: The shell saves command history to `~/.varpulis_history`. Use arrow keys to recall previous inputs.
- **Single-worker debugging**: When debugging sequence patterns, start the shell without `--generate` and inject events manually. This gives deterministic, one-at-a-time processing.
- **Reset without restart**: Use `:reset` to clear all state and start fresh without leaving the shell.

## See Also

- [Getting Started](getting-started.md) -- Installation and first VPL program
- [Debugging Pipelines](../guides/debugging-pipelines.md) -- Trace mode, watch mode, and connector discovery
- [Interactive Session Protocol Reference](../reference/interactive-protocol.md) -- JSON-line protocol specification
- [VPL Language Tutorial](language-tutorial.md) -- Comprehensive language guide
- [MCP Integration Reference](../reference/mcp-integration.md) -- AI agent integration via MCP tools
