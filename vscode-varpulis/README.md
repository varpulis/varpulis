# Varpulis VSCode Extension

VSCode extension for VPL language support with full integration to the Varpulis CEP engine.

## Features

### Language Support

- **Syntax Highlighting** - Full syntax highlighting for VPL (.vpl files)
- **Auto-completion** - Intelligent code completion for keywords, stream operations, and functions
- **Hover Documentation** - Inline documentation for keywords and functions
- **Error Diagnostics** - Real-time syntax validation

### Engine Integration

- **Start/Stop Engine** - Control the Varpulis engine directly from VSCode
- **Run Files** - Execute VPL files with one click (Ctrl+Shift+R)
- **Syntax Check** - Validate syntax without running (Ctrl+Shift+C)
- **Event Injection** - Inject test events for debugging

### Live Monitoring

- **Active Streams** - View all running streams with events/sec metrics
- **Recent Events** - Monitor events flowing through the engine
- **Alerts** - See alerts in real-time with notifications for critical alerts
- **Metrics** - View engine metrics (events processed, memory, CPU)

## Installation

### From VSIX

```bash
code --install-extension varpulis-0.1.0.vsix
```

### From Source

```bash
cd vscode-varpulis
npm install
npm run compile
# Press F5 in VSCode to launch Extension Development Host
```

## Usage

### Quick Start

1. Open a `.vpl` file
2. Start the engine: `Ctrl+Shift+P` → "Varpulis: Start Engine"
3. Run the file: `Ctrl+Shift+R`

### Commands

| Command | Shortcut | Description |
|---------|----------|-------------|
| Varpulis: Start Engine | - | Start the Varpulis engine |
| Varpulis: Stop Engine | - | Stop the engine |
| Varpulis: Run Current File | Ctrl+Shift+R | Load and run current file |
| Varpulis: Check Syntax | Ctrl+Shift+C | Validate syntax |
| Varpulis: Show Metrics | - | Open metrics dashboard |
| Varpulis: Inject Test Event | - | Inject a test event |

### Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `varpulis.enginePath` | `varpulis` | Path to CLI executable |
| `varpulis.enginePort` | `9000` | Engine WebSocket port |
| `varpulis.metricsPort` | `9090` | Prometheus metrics port |
| `varpulis.autoStart` | `false` | Auto-start engine on file open |
| `varpulis.liveReload` | `true` | Reload on file save |
| `varpulis.showInlineMetrics` | `true` | Show events/sec inline |

## Screenshots

### Syntax Highlighting

```varpulis
stream HighValueTrades = Trades
    .where(price > 10000)
    .window(5m)
    .aggregate(
        total: sum(price),
        count: count()
    )
    .emit(
        alert_type: "high_value",
        severity: "info"
    )
```

### Activity Bar

The Varpulis activity bar shows:
- **Active Streams** - Running streams with status
- **Recent Events** - Last 100 events
- **Alerts** - Generated alerts
- **Metrics** - Engine statistics

## Development

### Prerequisites

- Node.js 18+
- VSCode 1.85+
- Varpulis CLI installed

### Building

```bash
npm install
npm run compile
```

### Testing

```bash
npm run test
```

### Packaging

```bash
npm run package
```

## License

MIT

## Links

- [Varpulis Documentation](../docs/README.md)
- [VPL Syntax](../docs/language/syntax.md)
- [GitHub Repository](https://github.com/varpulis/varpulis)
