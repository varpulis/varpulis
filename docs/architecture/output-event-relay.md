# Output Event Relay Architecture

## Overview

Varpulis uses a multi-hop relay chain to deliver output events from worker engines to WebSocket clients. This is necessary because in a cluster deployment, workers process events but clients connect to the coordinator.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Worker Process                                                         │
│                                                                         │
│  Engine ──(mpsc)──▶ forward_output_events_to_websocket()                │
│                         │                                               │
│                    broadcast_tx (10K buffer)                             │
│                    ┌────┴────────────┐                                  │
│                    ▼                 ▼                                   │
│            WS clients          forward_output_events_to_coordinator()   │
│          (standalone)               │                                   │
│                              HTTP POST (batched, retries)               │
│                              x-api-key auth                             │
└─────────────────────────────┼───────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Coordinator Process                                                    │
│                                                                         │
│  /api/v1/internal/output-events ──▶ coord_broadcast_tx (1K buffer)     │
│                                          │                              │
│                                          ▼                              │
│                                    WS clients                           │
│                               (handle_coordinator_connection)           │
└─────────────────────────────────────────────────────────────────────────┘
```

## Event Paths

### Path A: Standalone Worker (no coordinator)

```
Engine → mpsc(1000) → forward_output_events_to_websocket → broadcast(10K) → WS clients
```

Events flow directly from the engine's output channel to connected WebSocket clients. No coordinator relay involved.

### Path B: Cluster Worker → Coordinator

```
Engine → mpsc(1000) → forward_output_events_to_websocket → broadcast(10K)
                                                              ├─→ WS clients (worker-local)
                                                              └─→ forward_output_events_to_coordinator
                                                                       │ (batch: 50 events / 200ms)
                                                                       │ (retry: 3 attempts, exp backoff)
                                                                       ▼
                                                              POST /api/v1/internal/output-events
                                                                       │ (auth: x-api-key)
                                                                       ▼
                                                              coord_broadcast(1K) → WS clients
```

Workers batch events (up to 50 events or 200ms deadline) and POST them to the coordinator's internal endpoint. The coordinator broadcasts them to its WebSocket clients.

### Path C: Connector-sourced Events (tenant drain task)

```
Connector (MQTT/Kafka) → Engine → mpsc → drain task → log_broadcast
                                                         └─→ ws_broadcast → (same as Path B)
```

For connector-sourced pipelines deployed via the REST API, a drain task continuously reads the engine's output channel and forwards events to both the per-pipeline log broadcast and the global WebSocket broadcast channel.

## Buffer Sizes and Rationale

| Buffer | Size | Location | Rationale |
|--------|------|----------|-----------|
| `mpsc` output channel | 1,000 | Engine → forwarder | Backpressure from engine to forwarder |
| Worker `broadcast` | 10,000 | Forwarder → WS + coordinator relay | 100ms slack at 100K events/sec (~2MB memory) |
| Coordinator `broadcast` | 1,000 | Internal endpoint → WS clients | Lower rate (batched input), fewer subscribers |
| Per-pipeline `log_broadcast` | 256 | Drain task → SSE subscribers | Per-pipeline, low subscriber count |

## Failure Modes and Recovery

### Coordinator Down

1. Worker's `forward_output_events_to_coordinator` retries each batch 3 times with exponential backoff (100ms, 200ms, 400ms)
2. After all retries fail, the batch is dropped and `events_dropped` incremented
3. After 5 consecutive batch failures, the forwarder enters **cooldown mode**: probes coordinator `/health` every 5 seconds
4. When health check passes, normal forwarding resumes

During cooldown, events are still delivered to worker-local WS clients. Only the coordinator relay is paused.

### Broadcast Buffer Full

If a WS client is too slow to consume events, the broadcast channel's lagged error fires. The forwarder logs a warning and continues. The slow client misses events but other clients are unaffected.

### No WebSocket Subscribers

When no WS clients are connected, `broadcast_tx.send()` returns `Err`. This is counted as `events_dropped` and logged at `debug` level. This is normal operation — events are only "dropped" in the sense that nobody is listening.

## Metrics

Available on the worker's `/health` endpoint under the `relay` key:

| Metric | Type | Description |
|--------|------|-------------|
| `relay_events_forwarded` | Counter | Events successfully sent to broadcast or coordinator |
| `relay_events_dropped` | Counter | Events dropped (no subscribers or coordinator unreachable after retries) |
| `relay_forwarding_errors` | Counter | Individual HTTP request failures (each retry attempt counts) |
| `relay_coordinator_healthy` | Boolean | Whether the coordinator is reachable |

## Authentication

The internal `/api/v1/internal/output-events` endpoint requires the `x-api-key` header. Workers already include this header in their POST requests. The coordinator validates the key against its RBAC configuration (same key used for worker registration).

## Testing

### Unit Tests

Run relay-specific unit tests:
```bash
cargo test -p varpulis-cli relay
```

### Local Cluster E2E

Run the local cluster test harness (no Docker required):
```bash
./tests/local-cluster/run.sh
```

This starts a coordinator + 2 workers as native processes, deploys a pipeline, injects events, and verifies the relay chain works end-to-end.

### Verifying Relay Health

```bash
# Check worker relay metrics
curl -s http://localhost:9000/health | jq .relay

# Check coordinator WebSocket relay
websocat ws://localhost:9100/ws
```
