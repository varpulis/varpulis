# Varpulis MCP Integration Reference

The Varpulis CEP engine ships a built-in **Model Context Protocol (MCP) server** (`varpulis-mcp`) that exposes pipeline management, validation, metrics, and diagnostics to AI agents such as Claude Desktop, Cursor, and any MCP-compatible client.

---

## Overview

### What is MCP?

The [Model Context Protocol](https://modelcontextprotocol.io) is an open standard that lets AI assistants interact with external services in a structured, tool-based way. An MCP server advertises a set of **tools** (callable actions), **resources** (readable data), and **prompts** (guided workflows) that an AI client can use to accomplish tasks without requiring the user to copy-paste data manually.

### How Varpulis Uses MCP

`varpulis-mcp` acts as a bridge between AI agents and a running Varpulis cluster. The server connects to the coordinator API (default `http://localhost:9100`) and exposes:

- **7 tools** — validate VPL code, deploy pipelines, query metrics, investigate alerts, inject test events, and list ONNX models
- **3 resources** — live cluster status, live cluster metrics, and the embedded VPL language reference
- **3 prompts** — guided workflows for alert investigation, fraud detection pipeline generation, and stream optimization

All tool calls that require a live cluster return structured JSON. The `validate_vpl` tool works entirely offline using the embedded VPL parser, so AI-assisted authoring requires no running infrastructure.

### Architecture

```
AI Agent (Claude Desktop / Cursor / other)
    |
    | stdio (JSON-RPC over stdin/stdout)
    |
varpulis-mcp
    |
    | HTTP REST
    |
Varpulis Coordinator  (:9100)
    |
Varpulis Workers      (:9000+)
```

The MCP server communicates with AI clients over stdio (the `stdio` transport). All logging goes to stderr; stdout is reserved for the MCP protocol.

---

## Setup

### Prerequisites

- Rust toolchain (to build from source)
- A running Varpulis coordinator for cluster operations (not required for offline VPL validation)

### Binary Installation

**From source (recommended):**

```bash
cargo install --path crates/varpulis-mcp
# Binary installed to ~/.cargo/bin/varpulis-mcp
```

**From the workspace build:**

```bash
cargo build --release
# Binary at target/release/varpulis-mcp
```

**Verify the installation:**

```bash
varpulis-mcp --help
```

### Command-Line Options

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--coordinator-url <URL>` | `VARPULIS_COORDINATOR_URL` | `http://localhost:9100` | URL of the running Varpulis coordinator |
| `--api-key <KEY>` | `VARPULIS_API_KEY` | _(none)_ | API key for coordinator authentication |
| `--transport <MODE>` | `VARPULIS_MCP_TRANSPORT` | `stdio` | Transport mode (only `stdio` is supported) |

### Configuration: Claude Desktop

Add the following to your Claude Desktop configuration file.

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "varpulis": {
      "command": "varpulis-mcp",
      "args": [
        "--coordinator-url", "http://localhost:9100"
      ],
      "env": {
        "VARPULIS_API_KEY": "your-api-key-here"
      }
    }
  }
}
```

If the binary is not on your PATH, use its absolute path:

```json
{
  "mcpServers": {
    "varpulis": {
      "command": "/home/user/.cargo/bin/varpulis-mcp",
      "args": ["--coordinator-url", "http://my-cluster:9100"],
      "env": {
        "VARPULIS_API_KEY": "prod-key"
      }
    }
  }
}
```

### Configuration: VS Code (Cursor / GitHub Copilot)

Add to `.vscode/settings.json` or your global VS Code settings:

```json
{
  "mcp.servers": {
    "varpulis": {
      "command": "varpulis-mcp",
      "args": ["--coordinator-url", "http://localhost:9100"],
      "env": {
        "VARPULIS_API_KEY": ""
      }
    }
  }
}
```

### Configuration: Generic MCP Client

Any MCP client that supports child-process stdio transport can launch the server as:

```bash
varpulis-mcp \
  --coordinator-url http://localhost:9100 \
  --api-key "$VARPULIS_API_KEY"
```

The server speaks JSON-RPC 2.0 on stdin/stdout and writes diagnostic logs to stderr.

### Authentication

When the coordinator is configured with an API key, pass it via:

- The `--api-key` flag
- The `VARPULIS_API_KEY` environment variable

The MCP server forwards the key in the `x-api-key` HTTP header on every coordinator request.

---

## Available Tools

### `validate_vpl`

Validates VPL source code for syntax and semantic correctness. This tool runs the full parser and semantic validator locally and **does not require a running coordinator**. If a coordinator is reachable and the code has errors, the tool also consults the coordinator for connector-aware validation.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `source` | `string` | Yes | VPL source code to validate |

**Returns:** A text message indicating validity, or a list of diagnostics in `line:column: [severity] message (hint: ...)` format.

**Example request:**

```json
{
  "name": "validate_vpl",
  "arguments": {
    "source": "stream HighValue = TradeEvent .where(price > 10000)"
  }
}
```

**Example response (valid):**

```
VPL is valid. No errors or warnings.
```

**Example response (invalid):**

```
Parse error: --> 1:24
  = unexpected ')'
    ...
```

**Example response (semantic warning):**

```
VPL is valid with warnings:
1:8: [warning] Stream 'Prices' is declared but never used (hint: add .to() or remove the stream)
```

---

### `deploy_pipeline`

Validates and then deploys a VPL pipeline group to the cluster. A pipeline group is the unit of deployment: it contains one pipeline definition and optional routing rules. The tool validates the VPL locally before sending it to the coordinator — a parse failure is returned immediately without a network call.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | `string` | Yes | Name for the pipeline group (used as the identifier) |
| `source` | `string` | Yes | VPL source code for the pipeline |
| `worker_affinity` | `string` | No | Target a specific worker by ID (e.g., `"worker-1"`) |

**Returns:** JSON object with the group ID, deployment status, and worker placement details.

**Example request:**

```json
{
  "name": "deploy_pipeline",
  "arguments": {
    "name": "fraud-monitor",
    "source": "stream FraudAlert = Transaction .where(amount > 5000 and status == \"declined\")\n    .emit(alert: \"high-value-decline\", amount: amount)"
  }
}
```

**Example response:**

```json
{
  "id": "grp-abc123",
  "name": "fraud-monitor",
  "status": "running",
  "placements": [
    {
      "name": "fraud-monitor",
      "worker_id": "worker-0",
      "status": "running"
    }
  ]
}
```

---

### `list_pipelines`

Lists all deployed pipeline groups with their current status, worker assignments, and event counts. Optionally filters by status.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `status` | `string` | No | Filter by pipeline status: `"running"`, `"deploying"`, or `"failed"` |

**Returns:** JSON object with a `pipeline_groups` array and a `cluster_summary` section. When a filter is applied, includes a `filtered_total` count.

**Example request (all pipelines):**

```json
{
  "name": "list_pipelines",
  "arguments": {}
}
```

**Example request (only failed):**

```json
{
  "name": "list_pipelines",
  "arguments": {
    "status": "failed"
  }
}
```

**Example response:**

```json
{
  "pipeline_groups": [
    {
      "id": "grp-abc123",
      "name": "fraud-monitor",
      "status": "running",
      "placements": [
        { "name": "fraud-monitor", "worker_id": "worker-0", "status": "running" }
      ]
    }
  ],
  "cluster_summary": {
    "total_workers": 3,
    "healthy_workers": 3,
    "total_pipeline_groups": 1,
    "events_per_second": 42300
  }
}
```

---

### `query_metrics`

Queries live cluster metrics including throughput, latency, worker health, and connector status. Optionally restricts metrics to a specific pipeline group and can include raw Prometheus exposition format.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `pipeline_group` | `string` | No | Filter metrics to a specific pipeline group name or ID |
| `include_prometheus` | `boolean` | No | If `true`, includes raw Prometheus metrics text in the response |

**Returns:** JSON object with `metrics` and `summary` sections. When `pipeline_group` is set, also includes a `filtered_pipelines` array.

**Example request:**

```json
{
  "name": "query_metrics",
  "arguments": {
    "pipeline_group": "fraud-monitor",
    "include_prometheus": false
  }
}
```

**Example response:**

```json
{
  "metrics": {
    "workers": [
      {
        "id": "worker-0",
        "events_per_second": 41200,
        "pipeline_count": 1,
        "memory_mb": 48
      }
    ],
    "pipelines": [
      {
        "group_id": "grp-abc123",
        "name": "fraud-monitor",
        "events_in": 41200,
        "events_out": 12,
        "latency_p99_ms": 1.4
      }
    ]
  },
  "summary": {
    "total_workers": 3,
    "healthy_workers": 3,
    "events_per_second": 42300
  },
  "filtered_pipelines": [
    {
      "group_id": "grp-abc123",
      "name": "fraud-monitor",
      "events_in": 41200,
      "events_out": 12,
      "latency_p99_ms": 1.4
    }
  ]
}
```

---

### `explain_alert`

Investigates an alert or anomaly by gathering comprehensive diagnostic context in parallel: pipeline group status, live metrics, worker health, cluster topology, and scaling recommendations. Performs heuristic analysis to identify likely root causes.

Internally this tool makes five concurrent API calls to minimise response latency.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `pipeline_group` | `string` | Yes | Pipeline group name or ID to investigate |
| `alert_description` | `string` | No | Free-text description of the alert or anomaly |

**Returns:** A structured Markdown report containing pipeline status, worker health table, full metrics JSON, scaling recommendation, topology map, and a list of heuristically identified root causes.

**Example request:**

```json
{
  "name": "explain_alert",
  "arguments": {
    "pipeline_group": "fraud-monitor",
    "alert_description": "No alerts emitted for the last 15 minutes despite high transaction volume"
  }
}
```

**Example response (abbreviated):**

```markdown
# Alert Investigation: fraud-monitor

**Alert**: No alerts emitted for the last 15 minutes despite high transaction volume

## Pipeline Group

- **Status**: partially_running
- **ID**: grp-abc123
- **Name**: fraud-monitor
- **Pipelines**: 1
  - fraud-monitor on worker-1 (failed)

## Worker Health

- [WARN] worker-1 (disconnected) — 0 pipelines, 0 events
- [OK] worker-0 (ready) — 2 pipelines, 8400000 events
- [OK] worker-2 (ready) — 1 pipelines, 2100000 events

## Possible Causes

- Pipeline group is not fully running — check worker logs for deploy errors.
- One or more workers are unhealthy — may cause event processing failures.
```

---

### `search_events`

Injects a test event directly into a deployed pipeline and observes the output. This is the primary tool for testing whether a filter or pattern correctly responds to specific input data.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `pipeline_group` | `string` | Yes | Pipeline group name or ID to inject the event into |
| `event_type` | `string` | Yes | Event type name (must match an event type declared in the pipeline's VPL, e.g., `"TradeEvent"`) |
| `fields` | `object` | Yes | Event fields as a JSON object with string keys and any-type values |

**Returns:** JSON object with the pipeline's response, including any output events generated by the injection.

**Example request:**

```json
{
  "name": "search_events",
  "arguments": {
    "pipeline_group": "fraud-monitor",
    "event_type": "Transaction",
    "fields": {
      "user_id": "u-999",
      "amount": 7500.00,
      "status": "declined",
      "merchant": "ATM-1234"
    }
  }
}
```

**Example response:**

```json
{
  "injected": true,
  "output_events": [
    {
      "stream": "FraudAlert",
      "alert": "high-value-decline",
      "amount": 7500.0,
      "timestamp": "2026-02-17T14:32:01.123Z"
    }
  ]
}
```

---

### `list_models`

Lists all ONNX models registered in the Varpulis model registry, with their input/output tensor specifications and metadata. Useful when writing VPL pipelines that use `.model()` for inline ML inference.

**Parameters:** None (pass an empty object `{}`).

**Returns:** JSON array of registered models.

**Example request:**

```json
{
  "name": "list_models",
  "arguments": {}
}
```

**Example response:**

```json
{
  "models": [
    {
      "name": "fraud-scorer",
      "version": "2",
      "inputs": [
        { "name": "amount", "dtype": "float32", "shape": [1] },
        { "name": "merchant_risk", "dtype": "float32", "shape": [1] }
      ],
      "outputs": [
        { "name": "fraud_score", "dtype": "float32", "shape": [1] }
      ],
      "registered_at": "2026-02-10T09:15:00Z"
    }
  ]
}
```

---

## Available Resources

Resources are readable data that AI agents can pull into context without making a tool call.

### `varpulis://docs/vpl-reference`

**Type:** `text/markdown`

The complete VPL language reference embedded in the MCP server binary. Contains syntax for streams, filters, windows, aggregations, joins, sequence patterns, connectors, `.forecast()`, contexts, and all built-in operators. Always available offline.

### `varpulis://cluster/status`

**Type:** `application/json`

Live cluster status: worker list with health states, pipeline group summaries, and aggregate health counts. Fetched from the coordinator on every read.

### `varpulis://cluster/metrics`

**Type:** `application/json`

Live cluster metrics combined with the current scaling recommendation: throughput per worker, per-pipeline event counts and latency, and the coordinator's recommended scale-up/scale-down action.

---

## Available Prompts

Prompts are pre-built guided workflows that the AI agent uses as a starting point for multi-step tasks. Select them from your MCP client's prompt picker.

### `investigate_alert`

Guides an AI agent through a structured root-cause analysis for an alert or anomaly.

| Argument | Required | Description |
|----------|----------|-------------|
| `alert` | Yes | Description of the alert or anomaly |
| `pipeline_group` | No | Pipeline group name or ID (auto-detected if omitted) |

The prompt instructs the agent to call `list_pipelines`, `query_metrics`, `explain_alert`, and optionally `search_events`, then produce a structured root-cause report with remediation steps.

### `create_fraud_detection`

Generates a VPL fraud detection pipeline from a brief specification, validates it, and deploys it.

| Argument | Required | Description |
|----------|----------|-------------|
| `event_type` | Yes | Name of the event type to monitor (e.g., `Transaction`) |
| `fields` | Yes | Comma-separated field names (e.g., `user_id, amount, status`) |
| `time_window` | No | Time window for pattern detection (default: `10m`) |

The prompt produces a starting template with a basic filter, windowed aggregation, sequence pattern, and alert output, then calls `validate_vpl` and `deploy_pipeline`.

### `optimize_stream`

Analyzes a deployed pipeline's performance and produces an optimized rewrite.

| Argument | Required | Description |
|----------|----------|-------------|
| `pipeline_group` | Yes | Pipeline group name or ID to optimize |
| `goal` | No | Optimization target: `throughput`, `latency`, or `memory` (default: `throughput`) |

The prompt collects live metrics and topology, applies goal-specific optimization heuristics (filter pushdown, partitioning, window tuning, `.trend_aggregate()` replacement), then emits an optimized VPL and validates it.

---

## Common AI-Assisted Workflows

### Writing VPL Pipelines with AI Assistance

The most effective approach is iterative: write a skeleton, validate, refine.

**Suggested conversation flow:**

1. Ask the AI to read the VPL reference resource (`varpulis://docs/vpl-reference`) to load syntax knowledge into context.
2. Describe your event types and detection goal in natural language.
3. The AI drafts VPL and calls `validate_vpl` — syntax and semantic errors are returned inline.
4. The AI fixes any errors and re-validates until clean.
5. When valid, the AI calls `deploy_pipeline` with your chosen group name.

**Example prompt:**

> "I have a `LoginEvent` with fields `user_id: str`, `ip_address: str`, and `success: bool`. Write a VPL pipeline that detects when the same user fails to log in 5 or more times within 2 minutes, then succeeds. Validate and deploy it."

The AI will draft something like:

```vpl
event LoginEvent:
    user_id: str
    ip_address: str
    success: bool

stream FailedLogin = LoginEvent .where(not success)
stream SuccessfulLogin = LoginEvent .where(success)

stream BruteForceAlert = FailedLogin as first_fail
    -> FailedLogin where user_id == first_fail.user_id as _
    -> FailedLogin where user_id == first_fail.user_id as _
    -> FailedLogin where user_id == first_fail.user_id as _
    -> SuccessfulLogin where user_id == first_fail.user_id as login
    .within(2m)
    .emit(
        alert: "brute_force_then_login",
        user_id: first_fail.user_id,
        ip: login.ip_address
    )
```

Then call `validate_vpl` and, if clean, `deploy_pipeline`.

---

### Debugging Pattern Matching Issues

When a pipeline is deployed but not producing expected output:

1. Use `list_pipelines` to confirm the group is `"running"` (not `"failed"` or `"deploying"`).
2. Use `search_events` to inject a synthetic event that should trigger the pattern.
3. Inspect the response: if no output events appear, the filter conditions or pattern bindings may not match.
4. Ask the AI to explain the pattern logic step by step and suggest field name or type mismatches.
5. Use `query_metrics` to compare `events_in` vs `events_out` — a high ratio indicates events are being dropped by `.where()` filters.

**Example prompt for debugging:**

> "My `BruteForceAlert` stream has processed 50,000 events but emitted zero alerts. Use `search_events` to inject a sequence of 5 failed logins followed by a success for user 'u-42', and tell me why no alert is generated."

---

### Optimizing Pipeline Performance

Use the `optimize_stream` prompt for a guided analysis, or drive the investigation manually:

1. Call `query_metrics` with `include_prometheus: true` to get raw counters.
2. Look for pipelines where `latency_p99_ms` is high or `events_in` far exceeds `events_out` after a filter.
3. Ask the AI to suggest filter pushdown — move `.where()` clauses earlier in the chain.
4. For Kleene patterns (`+` / `*`) with high event volume, ask the AI whether `.trend_aggregate()` can replace the sequence.
5. Check whether `.partition_by()` is missing — unpartitioned aggregations run single-threaded.
6. Call `validate_vpl` on the proposed rewrite, then `deploy_pipeline` with the updated source.

**Key optimization techniques:**

| Symptom | Technique |
|---------|-----------|
| High latency in windowed aggregations | Reduce window size or use sliding windows with larger intervals |
| Exponential match growth in Kleene patterns | Replace with `.trend_aggregate()` |
| Single-threaded aggregation bottleneck | Add `.partition_by(key_field)` |
| High memory from long-lived state | Reduce `.within()` durations on sequence patterns |
| Low throughput on multi-query workloads | Enable Hamlet sharing via overlapping Kleene patterns |

---

### Understanding Existing Pipelines

When inheriting or reviewing an existing pipeline:

1. Call `list_pipelines` to get the deployed source code.
2. Ask the AI to explain what each stream does in plain English.
3. Use `query_metrics` to see which streams are high-volume vs. rarely triggered.
4. Ask the AI to identify any redundant computations (e.g., the same `.where()` applied to multiple derived streams that could share a base stream).
5. Use the `investigate_alert` prompt if there is an active anomaly to resolve.

---

## Troubleshooting

### MCP server fails to start

**Symptom:** Claude Desktop shows the server as disconnected immediately.

**Causes and fixes:**

- The binary is not on the PATH specified in the MCP config. Use the absolute path to `varpulis-mcp`.
- The binary was built for a different architecture. Rebuild with `cargo build --release` on the target machine.
- Enable debug logging by setting `RUST_LOG=varpulis_mcp=debug` in the `env` block of your MCP config. Logs appear in the MCP client's server log view (stderr).

---

### "Cannot reach coordinator" errors from cluster tools

**Symptom:** `validate_vpl` works but `deploy_pipeline`, `list_pipelines`, and other cluster tools return `"Cannot reach coordinator at http://localhost:9100"`.

**Causes and fixes:**

- The Varpulis coordinator is not running. Start it with `varpulis coordinator` or check your deployment.
- The coordinator is on a different host or port. Update `--coordinator-url` in the MCP config.
- A firewall or VPN is blocking the connection. Verify with `curl http://localhost:9100/api/v1/cluster/summary`.

---

### "Authentication failed (HTTP 401)"

**Symptom:** All coordinator API calls return an authentication error.

**Causes and fixes:**

- The coordinator requires an API key but none was provided. Set `VARPULIS_API_KEY` in the MCP config's `env` block.
- The API key is correct but has been rotated. Update the value in the config.

---

### `validate_vpl` reports errors for valid-looking VPL

**Symptom:** VPL that looks correct is rejected with parse or semantic errors.

**Common causes:**

- **Field type mismatches**: VPL is strictly typed. `amount > 5000` fails if `amount` is declared as `str`. Ensure field types in `event` declarations match usage.
- **Undefined stream references**: A stream used in `.join()` or `.merge()` must be declared in the same file.
- **Connector not declared**: If `.from()` or `.to()` references a connector name, that connector must be declared with `connector Name = mqtt(...)` or similar.
- **Missing `as` alias in sequences**: Every event in a sequence pattern that is referenced in `.where()` or `.emit()` must have an `as alias` binding.

**Debugging approach:** Ask the AI to call `validate_vpl` with a minimal reproduction and iterate from there.

---

### `search_events` returns no output events

**Symptom:** Injecting a test event produces `"output_events": []`.

**Causes and fixes:**

- The injected `event_type` does not match the source event type in the stream declaration (case-sensitive).
- The `.where()` filter condition does not match the injected field values. Check operator precedence and type coercion.
- For sequence patterns, a single injection is not enough — the pattern requires multiple events in order within the time window. Inject the full sequence.
- The pipeline is in `"failed"` state. Check with `list_pipelines` and redeploy if needed.

---

### High latency or slow responses from `explain_alert`

**Symptom:** `explain_alert` takes several seconds to respond.

**Explanation:** The tool makes five concurrent API calls to the coordinator. If any coordinator endpoint is slow (e.g., topology computation on a large cluster), the overall latency is bounded by the slowest call. This is expected behavior and not a bug in the MCP server. The coordinator-side timeout is 30 seconds.

---

## See Also

- [VPL Language Syntax](../language/syntax.md) — Full grammar and operator reference
- [VPL Built-in Functions](../language/builtins.md) — Aggregation functions and forecast variables
- [SASE Pattern Guide](../guides/sase-patterns.md) — Sequence pattern authoring guide
- [Trend Aggregation Reference](./trend-aggregation.md) — `.trend_aggregate()` for Kleene patterns
- [Pattern Forecasting](../architecture/forecasting.md) — PST-based `.forecast()` architecture
- [CLI Reference](./cli-reference.md) — Local `varpulis` command reference
- [Cluster Tutorial](../tutorials/cluster-tutorial.md) — Setting up a multi-worker cluster
- [Troubleshooting Guide](../guides/troubleshooting.md) — General debugging techniques
