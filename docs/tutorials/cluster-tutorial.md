# Distributed Execution with Cluster Mode

This tutorial teaches you to scale Varpulis horizontally by running multiple worker processes coordinated by a central control plane. You'll start with a single worker, add a coordinator, deploy pipeline groups across workers, route events, and monitor health -- each step includes commands you can copy-paste and run.

## What You'll Learn

- The difference between single-process and distributed execution
- How to start a coordinator and register workers
- Deploying pipeline groups across multiple workers
- Event routing with wildcard pattern matching
- Health monitoring via the heartbeat protocol
- Tearing down pipeline groups and deregistering workers
- Production deployment with Docker Compose and Helm
- A complete distributed Mandelbrot demo across 4 workers

## Prerequisites

- Varpulis built and on your `PATH` (see [Getting Started](getting-started.md))
- Basic VPL knowledge: streams, `.emit()`, contexts (see [Contexts Tutorial](contexts-tutorial.md))
- `curl` and `jq` for API calls (optional but helpful)

---

## Part 1: Understanding the Architecture

### Single-Process Mode (What You Already Know)

In single-process mode, a `varpulis server` instance runs one or more pipelines, each with its own engine and context orchestrator:

```
varpulis server --port 9000 --api-key test

POST /api/v1/pipelines  →  Pipeline A (4 contexts)
POST /api/v1/pipelines  →  Pipeline B (2 contexts)
```

This works well until you need more CPU cores than one machine has, or you want isolation between workloads.

### Distributed Mode (What This Tutorial Teaches)

In distributed mode, a **coordinator** manages multiple **workers**. Each worker is a standard `varpulis server` process that registers with the coordinator. The coordinator decides where to deploy pipelines and how to route events:

```
                  ┌──────────────────────┐
                  │    Coordinator       │
                  │  (port 9100)         │
                  │                      │
                  │  - Worker registry   │
                  │  - Pipeline groups   │
                  │  - Event routing     │
                  │  - Health monitor    │
                  └──────┬───────────────┘
                         │ REST + heartbeat
           ┌─────────────┼─────────────┐
           │             │             │
     ┌─────┴──┐   ┌──────┴──┐   ┌─────┴──┐
     │Worker 0│   │Worker 1 │   │Worker 2│
     │:9000   │   │:9001    │   │:9002   │
     └────────┘   └─────────┘   └────────┘
```

Key principles:

1. **Workers are standard `varpulis server` processes** -- everything you already know still works
2. **The coordinator manages deployment and routing** -- it does not process events itself
3. **Each worker runs one or more pipelines** with their own engines and contexts
4. **The coordinator routes events** to the correct worker based on pattern-matching rules

**Key takeaway:** Distributed mode is an additive layer. Existing single-process programs run unchanged. The coordinator simply orchestrates multiple servers.

---

## Part 2: Starting a Coordinator

The coordinator is a lightweight control plane. It has no pipelines and processes no events -- it only manages workers and routing.

### Start It

Open a terminal and start the coordinator:

```bash
varpulis coordinator --port 9100 --api-key admin
```

**Expected output:**

```
Varpulis Coordinator
=======================
API:       http://127.0.0.1:9100/api/v1/cluster/
Auth:      enabled (API key required)
```

### Verify It's Running

```bash
curl -s http://localhost:9100/health | jq .
```

```json
{
  "role": "coordinator",
  "status": "healthy",
  "version": "0.1.0"
}
```

The coordinator is ready. It has zero workers and zero pipeline groups -- just an empty control plane waiting for workers to register.

### What the Coordinator Provides

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `POST /api/v1/cluster/workers/register` | Worker registration |
| `GET /api/v1/cluster/workers` | List workers |
| `POST /api/v1/cluster/pipeline-groups` | Deploy pipeline group |
| `POST /api/v1/cluster/pipeline-groups/{id}/inject` | Route an event |
| `GET /api/v1/cluster/topology` | Full routing topology |

All endpoints require the `x-api-key` header when `--api-key` is set.

**Key takeaway:** The coordinator is stateless and lightweight. It holds worker metadata and routing rules in memory. You can restart it and workers will re-register automatically.

---

## Part 3: Registering Workers

Workers are standard `varpulis server` processes with two extra flags: `--coordinator` and `--worker-id`.

### Start Two Workers

Open two more terminals.

**Terminal 2 -- Worker 0:**

```bash
varpulis server --port 9000 --api-key test \
    --coordinator http://localhost:9100 \
    --worker-id worker-0
```

**Terminal 3 -- Worker 1:**

```bash
varpulis server --port 9001 --api-key test \
    --coordinator http://localhost:9100 \
    --worker-id worker-1
```

Each worker starts as a normal server, then spawns a background task that:

1. Sends `POST /api/v1/cluster/workers/register` to the coordinator
2. Starts sending heartbeats every 5 seconds
3. Retries with exponential backoff if the coordinator is unavailable

You'll see logs like:

```
INFO varpulis: Registering with coordinator at http://localhost:9100 as worker 'worker-0'
INFO varpulis: Server listening on 127.0.0.1:9000
INFO varpulis_cluster: Registered with coordinator as 'worker-0'
```

### Verify Registration

Back in the first terminal (or any terminal):

```bash
curl -s http://localhost:9100/api/v1/cluster/workers \
    -H "x-api-key: admin" | jq .
```

```json
{
  "workers": [
    {
      "id": "worker-0",
      "address": "http://127.0.0.1:9000",
      "status": "ready",
      "pipelines_running": 0,
      "max_pipelines": 100,
      "assigned_pipelines": []
    },
    {
      "id": "worker-1",
      "address": "http://127.0.0.1:9001",
      "status": "ready",
      "pipelines_running": 0,
      "max_pipelines": 100,
      "assigned_pipelines": []
    }
  ],
  "total": 2
}
```

Both workers are registered and `ready` for pipeline deployment.

### What Happens If the Coordinator Goes Down?

Workers continue running and processing events normally. Heartbeats fail silently (logged as warnings), and when the coordinator comes back, workers re-register automatically.

The coordinator is not in the data path -- it only handles deployment and routing API calls. If the coordinator is down, existing pipelines keep running, but you can't deploy new ones or inject events through the coordinator.

**Key takeaway:** Worker registration is automatic and resilient. Start workers with `--coordinator` and they handle the rest. The `--worker-id` flag gives you a stable name; if omitted, a UUID is auto-generated.

---

## Part 4: Deploying a Pipeline Group

A **pipeline group** is a collection of related pipelines deployed together with routing rules. The coordinator places each pipeline on a worker and records where it lives.

### Write Two Simple Pipelines

For this tutorial, we'll deploy two simple pipelines that echo events:

- **Pipeline A**: processes `EventA` types, emits `{result: "from-a"}`
- **Pipeline B**: processes `EventB` types, emits `{result: "from-b"}`

### Deploy the Group

```bash
curl -s -X POST http://localhost:9100/api/v1/cluster/pipeline-groups \
    -H "x-api-key: admin" \
    -H "Content-Type: application/json" \
    -d '{
        "name": "my-first-group",
        "pipelines": [
            {
                "name": "pipeline-a",
                "source": "stream A = EventA .emit(result: \"from-a\")",
                "worker_affinity": "worker-0"
            },
            {
                "name": "pipeline-b",
                "source": "stream B = EventB .emit(result: \"from-b\")",
                "worker_affinity": "worker-1"
            }
        ],
        "routes": [
            {
                "from_pipeline": "_external",
                "to_pipeline": "pipeline-a",
                "event_types": ["EventA*"]
            },
            {
                "from_pipeline": "_external",
                "to_pipeline": "pipeline-b",
                "event_types": ["EventB*"]
            }
        ]
    }' | jq .
```

**Expected output:**

```json
{
  "id": "6b19c256-a766-40f9-bb19-5b9f61d9fa09",
  "name": "my-first-group",
  "status": "running",
  "pipeline_count": 2,
  "placements": [
    {
      "pipeline_name": "pipeline-a",
      "worker_id": "worker-0",
      "worker_address": "http://127.0.0.1:9000",
      "pipeline_id": "91f08c4a-...",
      "status": "Running"
    },
    {
      "pipeline_name": "pipeline-b",
      "worker_id": "worker-1",
      "worker_address": "http://127.0.0.1:9001",
      "pipeline_id": "52d0451e-...",
      "status": "Running"
    }
  ]
}
```

Save the group ID -- you'll need it for event injection:

```bash
GROUP_ID="6b19c256-a766-40f9-bb19-5b9f61d9fa09"  # use your actual ID
```

### What Happened Behind the Scenes

The coordinator:

1. **Parsed** the pipeline group spec (2 pipelines, 2 routes)
2. **Placed** pipeline-a on worker-0 (respecting `worker_affinity`)
3. **Placed** pipeline-b on worker-1 (respecting `worker_affinity`)
4. **Deployed** each pipeline by calling `POST /api/v1/pipelines` on the worker's existing REST API
5. **Recorded** the deployment: which pipeline runs where, with which pipeline ID

The workers don't know they're part of a cluster -- they just received a normal pipeline deploy request through their REST API. The coordinator is the only component that knows the full topology.

### Pipeline Group Spec Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Human-readable group name |
| `pipelines[].name` | Yes | Pipeline identifier within the group |
| `pipelines[].source` | Yes | VPL source code |
| `pipelines[].worker_affinity` | No | Preferred worker (falls back to round-robin) |
| `routes[].from_pipeline` | Yes | Source (`_external` for injected events) |
| `routes[].to_pipeline` | Yes | Destination pipeline name |
| `routes[].event_types` | Yes | Event type patterns to match |

**Key takeaway:** Pipeline groups let you deploy and manage related pipelines as a unit. The coordinator handles placement and records the topology. Workers receive normal pipeline deploy requests -- no special handling needed.

---

## Part 5: Routing Events

Now that pipelines are deployed, you can inject events through the coordinator. It matches the event type against routing rules and forwards the event to the correct worker.

### Inject an EventA

```bash
curl -s -X POST \
    "http://localhost:9100/api/v1/cluster/pipeline-groups/$GROUP_ID/inject" \
    -H "x-api-key: admin" \
    -H "Content-Type: application/json" \
    -d '{"event_type": "EventA", "fields": {"x": 42}}' | jq .
```

**Expected output:**

```json
{
  "routed_to": "pipeline-a",
  "worker_id": "worker-0",
  "worker_response": {
    "accepted": true,
    "output_events": [
      {
        "event_type": "A",
        "result": "from-a"
      }
    ]
  }
}
```

The coordinator matched `EventA` against the route `EventA*` and forwarded it to pipeline-a on worker-0.

### Inject an EventB

```bash
curl -s -X POST \
    "http://localhost:9100/api/v1/cluster/pipeline-groups/$GROUP_ID/inject" \
    -H "x-api-key: admin" \
    -H "Content-Type: application/json" \
    -d '{"event_type": "EventB_foo", "fields": {"y": 99}}' | jq .
```

```json
{
  "routed_to": "pipeline-b",
  "worker_id": "worker-1",
  "worker_response": {
    "accepted": true,
    "output_events": [
      {
        "event_type": "B",
        "result": "from-b"
      }
    ]
  }
}
```

Notice that `EventB_foo` matched the pattern `EventB*` (trailing wildcard). The coordinator sent it to pipeline-b on worker-1.

### How Pattern Matching Works

Routing rules use simple pattern matching with an optional trailing `*` wildcard:

| Pattern | Matches |
|---------|---------|
| `EventA*` | `EventA`, `EventA_foo`, `EventABC`, ... |
| `SensorReading` | Only `SensorReading` (exact match) |
| `*` | Everything |
| `ComputeTile0*` | `ComputeTile00`, `ComputeTile01`, `ComputeTile0Foo`, ... |

Routes are evaluated in order. The first matching route wins. If no route matches, the event goes to the first pipeline in the group.

### View the Topology

```bash
curl -s http://localhost:9100/api/v1/cluster/topology \
    -H "x-api-key: admin" | jq .
```

```json
{
  "groups": [
    {
      "group_id": "6b19c256-...",
      "group_name": "my-first-group",
      "pipelines": [
        {
          "name": "pipeline-a",
          "worker_id": "worker-0",
          "worker_address": "http://127.0.0.1:9000"
        },
        {
          "name": "pipeline-b",
          "worker_id": "worker-1",
          "worker_address": "http://127.0.0.1:9001"
        }
      ],
      "routes": [
        {
          "from_pipeline": "_external",
          "to_pipeline": "pipeline-a",
          "event_types": ["EventA*"]
        },
        {
          "from_pipeline": "_external",
          "to_pipeline": "pipeline-b",
          "event_types": ["EventB*"]
        }
      ]
    }
  ]
}
```

**Key takeaway:** The coordinator is a smart router. Events arrive at one endpoint, get matched against patterns, and are forwarded to the correct worker. The response includes the worker's output, so the caller gets immediate feedback.

---

## Part 6: Health Monitoring

The coordinator tracks worker health via heartbeats. Workers send a heartbeat every 5 seconds. If a worker misses 3 consecutive heartbeats (15 seconds), the coordinator marks it unhealthy.

### Check Worker Health

```bash
curl -s http://localhost:9100/api/v1/cluster/workers/worker-0 \
    -H "x-api-key: admin" | jq .
```

```json
{
  "id": "worker-0",
  "address": "http://127.0.0.1:9000",
  "status": "ready",
  "pipelines_running": 1,
  "max_pipelines": 100,
  "assigned_pipelines": ["pipeline-a"]
}
```

### Worker States

```
Registering → Ready → Busy
                ↓         ↓
            Unhealthy ← (timeout)
                ↓
            Draining
```

| State | Description |
|-------|-------------|
| `Registering` | Worker is connecting (transient) |
| `Ready` | Available for pipeline deployment |
| `Busy` | At maximum pipeline capacity |
| `Unhealthy` | No heartbeat for 15+ seconds |
| `Draining` | Shutting down gracefully |

### Simulating a Failure

If you kill worker-1 (Ctrl+C in terminal 3), within 15 seconds the coordinator will mark it unhealthy:

```bash
# After ~15 seconds with worker-1 stopped:
curl -s http://localhost:9100/api/v1/cluster/workers \
    -H "x-api-key: admin" | jq '.workers[] | {id, status}'
```

```json
{"id": "worker-0", "status": "ready"}
{"id": "worker-1", "status": "unhealthy"}
```

### Auto-Recovery

If you restart worker-1, it re-registers and the coordinator marks it `ready` again as soon as a heartbeat arrives:

```bash
# Restart worker-1
varpulis server --port 9001 --api-key test \
    --coordinator http://localhost:9100 \
    --worker-id worker-1
```

```
INFO varpulis_cluster: Registered with coordinator as 'worker-1'
```

**Key takeaway:** Health monitoring is automatic. Workers send heartbeats, the coordinator sweeps every 5 seconds, and stale workers are marked unhealthy. Recovery is also automatic -- a heartbeat from an unhealthy worker restores it to `ready`.

---

## Part 7: Teardown and Cleanup

### Tear Down a Pipeline Group

Tearing down a group deletes all deployed pipelines from their workers:

```bash
curl -s -X DELETE \
    "http://localhost:9100/api/v1/cluster/pipeline-groups/$GROUP_ID" \
    -H "x-api-key: admin" | jq .
```

```json
{
  "torn_down": true
}
```

The coordinator calls `DELETE /api/v1/pipelines/{id}` on each worker to remove the pipeline. Worker assigned_pipelines lists are updated accordingly.

### Deregister a Worker

```bash
curl -s -X DELETE \
    http://localhost:9100/api/v1/cluster/workers/worker-1 \
    -H "x-api-key: admin" | jq .
```

```json
{
  "deleted": true
}
```

The worker process keeps running but is no longer known to the coordinator. Its heartbeats will be rejected with 404.

**Key takeaway:** Teardown is clean. The coordinator tracks all deployments and can remove them from workers. Deregistering a worker removes it from the registry but doesn't stop the process.

---

## Part 8: Placement Strategies

When you don't specify `worker_affinity`, the coordinator uses a placement strategy to decide which worker runs each pipeline.

### Round-Robin (Default)

Distributes pipelines evenly across available workers in order:

```json
{
    "pipelines": [
        {"name": "p1", "source": "..."},
        {"name": "p2", "source": "..."},
        {"name": "p3", "source": "..."},
        {"name": "p4", "source": "..."}
    ]
}
```

With 2 workers: p1 → worker-0, p2 → worker-1, p3 → worker-0, p4 → worker-1.

### Worker Affinity

Pin a pipeline to a specific worker with `worker_affinity`:

```json
{
    "name": "critical-pipeline",
    "source": "...",
    "worker_affinity": "worker-0"
}
```

If worker-0 is unavailable, the coordinator falls back to round-robin placement instead of failing.

### Least-Loaded

Available programmatically. Picks the worker with the fewest running pipelines. Useful when pipelines have uneven resource requirements.

**Key takeaway:** Placement is flexible. Use `worker_affinity` for pinning, or let round-robin distribute evenly. The coordinator always checks that the selected worker is available before deploying.

---

## Part 9: Complete Example -- Distributed IoT Monitoring

Let's put it all together with a realistic scenario: an IoT system with temperature and pressure sensors, where each sensor type is processed by a different worker.

### Step 1: Start the Cluster

```bash
# Terminal 1: Coordinator
varpulis coordinator --port 9100 --api-key admin

# Terminal 2: Worker for temperature
varpulis server --port 9000 --api-key test \
    --coordinator http://localhost:9100 --worker-id temp-worker

# Terminal 3: Worker for pressure
varpulis server --port 9001 --api-key test \
    --coordinator http://localhost:9100 --worker-id pressure-worker
```

### Step 2: Deploy the Pipeline Group

```bash
curl -s -X POST http://localhost:9100/api/v1/cluster/pipeline-groups \
    -H "x-api-key: admin" \
    -H "Content-Type: application/json" \
    -d '{
        "name": "iot-monitoring",
        "pipelines": [
            {
                "name": "temp-alerts",
                "source": "stream TempAlerts = TemperatureReading .where(temperature > 100) .emit(alert_type: \"HIGH_TEMP\", sensor_id: sensor_id, temperature: temperature)",
                "worker_affinity": "temp-worker"
            },
            {
                "name": "pressure-alerts",
                "source": "stream PressureAlerts = PressureReading .where(pressure > 200) .emit(alert_type: \"HIGH_PRESSURE\", sensor_id: sensor_id, pressure: pressure)",
                "worker_affinity": "pressure-worker"
            }
        ],
        "routes": [
            {"from_pipeline": "_external", "to_pipeline": "temp-alerts", "event_types": ["Temperature*"]},
            {"from_pipeline": "_external", "to_pipeline": "pressure-alerts", "event_types": ["Pressure*"]}
        ]
    }' | jq .
```

Save the returned group ID.

### Step 3: Inject Events

```bash
GROUP_ID="<your-group-id>"

# Temperature event → routed to temp-worker
curl -s -X POST \
    "http://localhost:9100/api/v1/cluster/pipeline-groups/$GROUP_ID/inject" \
    -H "x-api-key: admin" \
    -H "Content-Type: application/json" \
    -d '{"event_type": "TemperatureReading", "fields": {"sensor_id": "T1", "temperature": 105.5}}' | jq .

# Pressure event → routed to pressure-worker
curl -s -X POST \
    "http://localhost:9100/api/v1/cluster/pipeline-groups/$GROUP_ID/inject" \
    -H "x-api-key: admin" \
    -H "Content-Type: application/json" \
    -d '{"event_type": "PressureReading", "fields": {"sensor_id": "P1", "pressure": 250.0}}' | jq .
```

Both events trigger alerts because they exceed the thresholds:

```json
{
  "routed_to": "temp-alerts",
  "worker_id": "temp-worker",
  "worker_response": {
    "accepted": true,
    "output_events": [{"event_type": "TempAlerts", "alert_type": "HIGH_TEMP", "sensor_id": "T1", "temperature": 105.5}]
  }
}
```

### Step 4: Check Topology

```bash
curl -s http://localhost:9100/api/v1/cluster/topology \
    -H "x-api-key: admin" | jq '.groups[0].pipelines[] | {name, worker_id}'
```

```json
{"name": "temp-alerts", "worker_id": "temp-worker"}
{"name": "pressure-alerts", "worker_id": "pressure-worker"}
```

---

## Part 10: Distributed Mandelbrot Demo

The distributed Mandelbrot demo is a real-world example that computes a 1000x1000 Mandelbrot set image across 4 worker processes. Each worker handles one row of the 4x4 tile grid (4 tiles of 250x250 pixels each).

### Architecture

```
Coordinator (port 9100)
    ├── Worker 0 (port 9000): row0.vpl → tiles (0,0)-(3,0)
    ├── Worker 1 (port 9001): row1.vpl → tiles (0,1)-(3,1)
    ├── Worker 2 (port 9002): row2.vpl → tiles (0,2)-(3,2)
    └── Worker 3 (port 9003): row3.vpl → tiles (0,3)-(3,3)

Event routing:
    ComputeTile0* → row0 (worker-0)
    ComputeTile1* → row1 (worker-1)
    ComputeTile2* → row2 (worker-2)
    ComputeTile3* → row3 (worker-3)
```

Each worker's VPL program uses 4 contexts (one per tile in its row) and the `compute_tile` function with nested loops and `emit Pixel(...)` to generate 62,500 pixels per tile.

Here's a snippet from `mandelbrot_worker_0.vpl` (the full file is in `examples/mandelbrot/distributed/`):

```vpl
# Worker 0 handles row 0 (top row): tiles (0,0) through (3,0)

connector MqttOut = mqtt (host: "localhost", port: 1883, client_id: "mandelbrot-w0")

context t00
context t01
context t02
context t03

fn mandelbrot(cx: float, cy: float, max_iter: int) -> int:
    var zr = 0.0
    var zi = 0.0
    var i = 0
    while i < max_iter:
        let r2 = zr * zr
        let i2 = zi * zi
        if r2 + i2 > 4.0:
            return i
        zi := 2.0 * zr * zi + cy
        zr := r2 - i2 + cx
        i := i + 1
    return max_iter

fn compute_tile(x_off: int, y_off: int, size: int, max_iter: int):
    for px in 0..size:
        for py in 0..size:
            let cx = -2.0 + (x_off + px) * 3.0 / 1000.0
            let cy = -1.5 + (y_off + py) * 3.0 / 1000.0
            let iters = mandelbrot(cx, cy, max_iter)
            emit Pixel(x: x_off + px, y: y_off + py, iterations: iters, diverged: iters < max_iter)

stream Tile00 = ComputeTile00
    .context(t00)
    .process(compute_tile(0, 0, 250, 256))
    .to(MqttOut, topic: "mandelbrot/pixels")

stream Tile01 = ComputeTile01
    .context(t01)
    .process(compute_tile(250, 0, 250, 256))
    .to(MqttOut, topic: "mandelbrot/pixels")

# ... Tile02 and Tile03 similarly
```

### Run It

Prerequisites: MQTT broker running and binary built.

```bash
# Start MQTT broker
docker run -d -p 1883:1883 eclipse-mosquitto

# Build
cargo build --release

# Run the deployment script
./examples/mandelbrot/distributed/deploy.sh
```

The script:
1. Starts the coordinator on port 9100
2. Starts 4 workers on ports 9000-9003
3. Waits for registration
4. Deploys a pipeline group with 4 pipelines and wildcard routing
5. Injects 16 `ComputeTile{row}{col}` events in parallel
6. Prints the topology

### Benchmark Single vs Distributed

```bash
python3 examples/mandelbrot/distributed/bench.py
```

This A/B test compares:
- **Baseline**: 1 server, 1 pipeline, 16 contexts (all tiles on one process)
- **Distributed**: 1 coordinator + 4 workers, 4 pipelines, 4 contexts each

---

## Part 11: Production Deployment

### Docker Compose

The simplest way to run a cluster in production:

```bash
docker compose -f deploy/docker/docker-compose.cluster.yml up -d
```

This starts:
- 1 coordinator (port 9100)
- 4 workers (ports 9000-9003)
- 1 MQTT broker (port 1883)
- Prometheus (port 9091)
- Grafana (port 3000)

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `VARPULIS_API_KEY` | `admin` | Coordinator API key |
| `VARPULIS_WORKER_KEY` | `test` | Worker API key |
| `RUST_LOG` | `info` | Log level |
| `GRAFANA_USER` | `admin` | Grafana admin user |
| `GRAFANA_PASSWORD` | `varpulis` | Grafana admin password |

Deploy a pipeline group:

```bash
curl -X POST http://localhost:9100/api/v1/cluster/pipeline-groups \
    -H "x-api-key: admin" \
    -H "Content-Type: application/json" \
    -d '{ ... }'
```

### Kubernetes / Helm

For Kubernetes deployments:

```bash
helm install my-cluster deploy/helm/varpulis-cluster/ \
    --set coordinator.apiKey=my-secret \
    --set workers.replicas=4 \
    --set workers.apiKey=worker-secret
```

The Helm chart creates:
- **Coordinator Deployment** with liveness/readiness probes
- **Worker StatefulSet** -- each pod auto-registers with `--worker-id` set to the pod name
- **Headless Service** for worker-to-worker communication
- **Kubernetes Secret** for API keys
- **Optional MQTT** broker (set `mqtt.enabled: true`)

Scale workers:

```bash
kubectl scale statefulset my-cluster-worker --replicas=8
```

New workers auto-register with the coordinator. No pipeline re-deployment needed -- the coordinator will use them for future deployments.

### Production Checklist

| Item | How |
|------|-----|
| Authentication | Set `--api-key` on coordinator and workers |
| TLS | Use `--tls-cert`/`--tls-key` on workers (coordinator uses plain HTTP internally) |
| Monitoring | Enable `--metrics` on workers, scrape with Prometheus |
| Health probes | `GET /health` on coordinator, `GET /health` + `GET /ready` on workers |
| Persistence | Use `--state-dir` on workers for pipeline state persistence |
| Log level | Set `RUST_LOG=info` (or `debug` for troubleshooting) |

---

## Quick Reference

### CLI Commands

| Command | Description |
|---------|-------------|
| `varpulis coordinator --port 9100 --api-key KEY` | Start coordinator |
| `varpulis server --port 9000 --api-key KEY --coordinator URL --worker-id ID` | Start worker |

### Coordinator API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/cluster/workers/register` | Register worker |
| `POST` | `/api/v1/cluster/workers/{id}/heartbeat` | Worker heartbeat |
| `GET` | `/api/v1/cluster/workers` | List workers |
| `GET` | `/api/v1/cluster/workers/{id}` | Worker details |
| `DELETE` | `/api/v1/cluster/workers/{id}` | Deregister worker |
| `POST` | `/api/v1/cluster/pipeline-groups` | Deploy group |
| `GET` | `/api/v1/cluster/pipeline-groups` | List groups |
| `GET` | `/api/v1/cluster/pipeline-groups/{id}` | Group details |
| `DELETE` | `/api/v1/cluster/pipeline-groups/{id}` | Tear down group |
| `POST` | `/api/v1/cluster/pipeline-groups/{id}/inject` | Route event |
| `GET` | `/api/v1/cluster/topology` | Full topology |

### Event Type Pattern Matching

| Pattern | Matches |
|---------|---------|
| `Exact` | Only `Exact` |
| `Prefix*` | `Prefix`, `PrefixFoo`, `PrefixBar`, ... |
| `*` | Everything |

### Timeouts

| Setting | Value |
|---------|-------|
| Heartbeat interval | 5 seconds |
| Unhealthy threshold | 15 seconds (3 missed heartbeats) |
| HTTP client timeout | 10 seconds |
| Registration backoff | 1-30 seconds (exponential) |

---

## Next Steps

- [Cluster Architecture](../architecture/cluster.md) -- Deep-dive into the coordinator, routing, placement, and crate structure
- [CLI Reference](../reference/cli-reference.md) -- Full `varpulis coordinator` and `varpulis server` options
- [Contexts Tutorial](contexts-tutorial.md) -- Single-process parallelism with contexts (used within each worker)
- [Checkpointing Tutorial](checkpointing-tutorial.md) -- State persistence for worker pipelines
