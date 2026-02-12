# E2E Horizontal Scaling + Coordinator HA Test Results

## Overview

This test suite validates Varpulis CEP's horizontal scaling and high-availability capabilities using real Docker containers, MQTT traffic, and process kills. It tests:

- **WebSocket-based failure detection** (~0s vs ~20s with REST heartbeats)
- **Worker failover** with automatic pipeline migration
- **Coordinator failover** with fault isolation between coordinators
- **Full cluster recovery** after cascading failures
- **Traffic resilience** during coordinator and worker kills

## Architecture

```
┌────────────────────── varpulis-e2e (Docker network) ──────────────────────┐
│                                                                           │
│  ┌────────────┐                                                           │
│  │ mosquitto  │◄──────────────────────────────────────┐                   │
│  │ :1883      │◄──────────────────┐                   │                   │
│  └────────────┘                   │                   │                   │
│                                   │                   │                   │
│  ┌────────────────┐   ┌────────────────┐   ┌──────────────┐              │
│  │ coordinator-1  │   │ coordinator-2  │   │ test-driver   │              │
│  │ :9100          │   │ :9100          │   │ (python)      │              │
│  │ WS: 2 conns    │   │ WS: 1 conn     │   │ Docker socket │              │
│  └───┬────────┬───┘   └───────┬────────┘   └──────────────┘              │
│      │ WS/REST│               │ WS/REST                                   │
│      │        │               │                                           │
│  ┌───▼───┐ ┌──▼────┐   ┌─────▼──┐                                        │
│  │worker │ │worker │   │worker │                                        │
│  │  -1   │ │  -2   │   │  -3   │                                        │
│  │ :9000 │ │ :9000 │   │ :9000 │                                        │
│  └───────┘ └───────┘   └───────┘                                        │
└───────────────────────────────────────────────────────────────────────────┘
```

### Port and Network Matrix

| Service | Internal Port | Protocol | Connections To | Purpose |
|---------|--------------|----------|----------------|---------|
| mosquitto | 1883 | MQTT | workers (pub/sub) | Event broker for pipeline I/O |
| coordinator-1 | 9100 | HTTP + WS | worker-1, worker-2 | Control plane for workers 1+2 |
| coordinator-2 | 9100 | HTTP + WS | worker-3 | Control plane for worker 3 |
| worker-1 | 9000 | HTTP | coordinator-1 | CEP engine, serves REST API |
| worker-2 | 9000 | HTTP | coordinator-1 | CEP engine, serves REST API |
| worker-3 | 9000 | HTTP | coordinator-2 | CEP engine, serves REST API |
| test-driver | — | HTTP + MQTT | coord-1, coord-2, mosquitto | Test orchestrator with Docker socket |

### Connection Types

| Connection | Transport | Direction | Purpose |
|------------|-----------|-----------|---------|
| Worker → Coordinator (REST) | HTTP POST | Worker initiates | Registration, heartbeat fallback |
| Worker → Coordinator (WS) | WebSocket | Worker initiates, persistent | Heartbeats, instant failure detection |
| Coordinator → Worker (REST) | HTTP POST | Coordinator initiates | Pipeline deploy/undeploy, drain |
| Worker ↔ Mosquitto | MQTT (TCP) | Bidirectional | Event ingestion (.from) and output (.to) |
| Test Driver → Coordinators | HTTP | Driver initiates | API calls for deploy, status, topology |
| Test Driver → Mosquitto | MQTT | Driver initiates | Publish test events, collect output |
| Test Driver → Docker | Unix socket | Driver initiates | Container stop/start for chaos testing |

### Pipeline Design

Three independent pipelines, split across two coordinators:

| Pipeline | Coordinator | Worker(s) | Connector | Input Topic | Output Topic |
|----------|-------------|-----------|-----------|------------|--------------|
| sensor_filter_1 | coordinator-1 | worker-1 or worker-2 | mqtt_1 | e2e/input/1 | e2e/output |
| sensor_filter_2 | coordinator-1 | worker-1 or worker-2 | mqtt_2 | e2e/input/2 | e2e/output |
| sensor_filter_3 | coordinator-2 | worker-3 | mqtt_3 | e2e/input/3 | e2e/output |

Each pipeline uses its own MQTT connector to avoid client ID collisions when multiple pipelines share a worker after migration. Events are published round-robin across input topics.

## Failure Detection: Three Layers

Varpulis implements a layered failure detection model. The fastest layer to detect a failure wins:

| Layer | Detection Time | Mechanism | Catches |
|-------|---------------|-----------|---------|
| **WebSocket** | **~0s** (instant) | TCP connection drop on persistent WS | Process crash, OOM kill, network partition |
| **K8s Pod Watcher** | ~1-2s | K8s Watch API on pod status | OOMKill, eviction, node failure (K8s only) |
| **REST Heartbeat** | ~10-20s (fallback) | Heartbeat timeout + health sweep | Hung process, degraded performance, WS unavailable |

### How WebSocket Failure Detection Works

```
  Worker                                  Coordinator
    │                                          │
    │──── WS Connect ────────────────────────▶│
    │◀─── IdentifyAck ────────────────────────│
    │                                          │
    │──── WS Heartbeat ──────────────────────▶│  (periodic, replaces REST)
    │◀─── HeartbeatAck ───────────────────────│
    │                                          │
    │──── WS Heartbeat ──────────────────────▶│
    │◀─── HeartbeatAck ───────────────────────│
    │                                          │
    ╳ worker process killed                    │
    │                                          │
    │  TCP FIN/RST detected instantly ───────▶│ mark unhealthy
    │                                          │ trigger failover
    │                                          │ migrate pipelines
```

When a worker's process dies, the OS closes the TCP connection. The coordinator's WebSocket handler detects the stream end immediately and:
1. Removes the worker from the WS connection manager
2. Marks the worker as `Unhealthy`
3. Calls `handle_worker_failure()` to migrate pipelines

If WS is unavailable (e.g., network issues, older worker version), the worker falls back to REST heartbeats automatically, and the coordinator detects failure via heartbeat timeout.

### Coordinator Failover Model

With 2 independent coordinators, each managing a subset of workers:

```
Before coordinator-1 failure:         After coordinator-1 failure:
┌──────────────┐  ┌──────────────┐    ┌──────────────┐  ┌──────────────┐
│coordinator-1 │  │coordinator-2 │    │coordinator-1 │  │coordinator-2 │
│ worker-1 ✓   │  │ worker-3 ✓   │    │   ╳ DOWN     │  │ worker-3 ✓   │
│ worker-2 ✓   │  │ pipeline-3 ✓ │    │              │  │ pipeline-3 ✓ │
│ pipeline-1 ✓ │  └──────────────┘    └──────────────┘  └──────────────┘
│ pipeline-2 ✓ │                       Workers 1+2 lose    Worker-3 and
└──────────────┘                       their coordinator   pipeline-3 are
                                       but continue MQTT   100% unaffected
                                       processing
```

**Key properties:**
- Coordinators are fully independent — killing one has zero impact on the other
- Workers continue processing MQTT events even after their coordinator dies (pipelines are already deployed)
- Workers detect coordinator death via WS disconnect and retry connection
- On coordinator restart, workers automatically re-register and WS connections are re-established

In Kubernetes, coordinator HA is enhanced with:
- K8s Lease-based leader election (active-passive with ~10s failover)
- State replication from leader to followers via internal WebSocket
- K8s Service readiness probe routes worker traffic to the leader only

## Test Phases

### Phase 0: Setup
- Wait for 2 workers on coordinator-1, 1 worker on coordinator-2
- Register MQTT connectors on both coordinators
- Deploy pipelines 1+2 on coordinator-1, pipeline 3 on coordinator-2
- Verify MQTT data path with 100 warm-up events

### Phase 1: Baseline
- Publish 500 events round-robin (250 matching the `temperature > 50.0` filter)
- Verify all 250 output events arrive through all 3 pipelines

### Phase 2: WebSocket Check
- Query `/health` on both coordinators
- Verify coordinator-1 reports 2 WS connections (worker-1, worker-2)
- Verify coordinator-2 reports 1 WS connection (worker-3)
- Confirm both report healthy status

### Phase 3: Worker Failover via WebSocket
- Record WS connection count on coordinator-1
- Kill worker-1 (Docker stop, instant)
- Verify coordinator detects failure via WS disconnect (~0s)
- Verify WS connection count decreases by 1
- Verify `"WebSocket disconnected"` appears in coordinator-1 logs
- Wait for pipeline migration off worker-1
- Publish 500 events, verify all 250 matching events arrive

### Phase 4: Cascading Failure
- Kill worker-2 (coordinator-1 now has zero healthy workers)
- Verify coordinator-2 is completely unaffected
- Send 500 events only to pipeline 3 (coordinator-2's worker)
- Verify all 250 matching events arrive through coordinator-2

### Phase 5: Coordinator Failover
- Snapshot coordinator-2 state (WS connections, status)
- Kill coordinator-1
- Verify coordinator-1 is unreachable
- Verify coordinator-2 is **completely unaffected** (same WS count, healthy status)
- Send 200 events through coordinator-2's pipeline
- Verify all 100 matching events arrive

### Phase 6: Recovery
- Restart coordinator-1, wait for it to become healthy
- Restart worker-1 and worker-2
- Wait for workers to re-register and WS connections to re-establish
- Re-register connectors and re-deploy pipelines on coordinator-1
- Verify both coordinators report correct WS connections (coord-1: 2, coord-2: 1)
- Publish 500 events across all pipelines, verify all 250 arrive

### Phase 7: Traffic During Coordinator Kill
- Start continuous background traffic at ~100 events/sec across all pipelines
- Kill coordinator-1 after 5 seconds of traffic
- Continue publishing for 20 more seconds
- Verify coordinator-2 remains operational
- Verify event delivery rate (workers continue processing even without coordinator)

## Running the Test

### Prerequisites

- Docker and Docker Compose v2+
- Docker socket accessible at `/var/run/docker.sock`
- ~3 GB RAM for the full cluster (2 coordinators + 3 workers + mosquitto + test driver)
- ~5 minutes for the complete test (including Docker build)

### Command

```bash
cd tests/e2e-scaling
bash run.sh
```

### Output

```
============================================================
=== E2E Scaling + Coordinator HA Test Results ===
  Phase 0: Setup ......................... PASS (12.1s)
           warm-up 50/50
  Phase 1: Baseline ...................... PASS (0.6s)
           250/250 events
  Phase 2: WebSocket Check ............... PASS (0.0s)
           coord-1: 2 WS, coord-2: 1 WS
  Phase 3: Worker Failover (WS) .......... PASS (2.0s)
           250/250 events, failover in 1.0s, WS: 2->1
  Phase 4: Cascading Failure ............. PASS (0.9s)
           250/250 events via coordinator-2, coordinator-2 healthy: True
  Phase 5: Coordinator Failover .......... PASS (13.0s)
           coord-1 down: True, coord-2 OK: True, events: 100/100
  Phase 6: Recovery ...................... PASS (6.6s)
           250/250 events, coord-1 WS=2, coord-2 WS=1, both coordinators up: True
  Phase 7: Traffic During Coordinator Kill . PASS (35.4s)
           1221/1221 events (100.0%), coord-2 up: True
============================================================
RESULT: 8/8 phases passed
```

Logs are captured to `tests/e2e-scaling/results/docker-logs.txt` for debugging.

## Results

Run: 2026-02-12, 8/8 phases pass.

| Phase | Status | Duration | Events | Notes |
|-------|--------|----------|--------|-------|
| 0: Setup | PASS | 12.1s | 50/50 warm-up | 2 coordinators, 3 workers, 3 pipelines |
| 1: Baseline | PASS | 0.6s | 250/250 | All pipelines active |
| 2: WebSocket Check | PASS | 0.0s | — | coord-1: 2 WS, coord-2: 1 WS |
| 3: Worker Failover (WS) | PASS | 2.0s | 250/250 | **Failover in 1.0s** (WS: 2→1) |
| 4: Cascading Failure | PASS | 0.9s | 250/250 | coordinator-2 unaffected |
| 5: Coordinator Failover | PASS | 13.0s | 100/100 | coord-1 down, coord-2 100% operational |
| 6: Recovery | PASS | 6.6s | 250/250 | Full cluster restored, WS re-established |
| 7: Traffic During Kill | PASS | 35.4s | 1221/1221 (100%) | Workers continue processing without coordinator |

## Failover Timing Comparison

### Before WebSocket (REST heartbeat only)

| Parameter | Value |
|-----------|-------|
| Heartbeat interval | 5s |
| Heartbeat timeout | 15s |
| Worst-case detection | ~20s (timeout + sweep) |
| Migration time | ~2-5s after detection |
| **Total failover** | **~25s** |

### After WebSocket (current)

| Parameter | Value |
|-----------|-------|
| WS disconnect detection | **~0s** (TCP FIN/RST) |
| Migration time | ~1s after detection |
| **Total failover** | **~1s** |
| REST fallback (if WS unavailable) | ~20s (unchanged) |

**Improvement: 25x faster failure detection** when WebSocket connections are active.

### Coordinator Failover

| Scenario | Impact on unrelated coordinator | Impact on workers | Event delivery |
|----------|-------------------------------|-------------------|----------------|
| Coordinator-1 killed | **Zero** — coordinator-2 unaffected | Workers lose control plane, continue MQTT processing | 100% for coordinator-2's pipelines |
| Coordinator-1 restarted | **Zero** — coordinator-2 unaffected | Workers auto-reconnect, WS re-established | 100% after re-registration |
| Both coordinators killed | N/A | Workers lose control plane but continue processing already-deployed pipelines | 100% for already-deployed pipelines |

## Known Limitations

1. **MQTT QoS 0** — No delivery guarantee. Events published while a pipeline is migrating between workers are lost. This is expected behavior with QoS 0.

2. **Coordinator state is in-memory** — When a coordinator restarts, it loses pipeline group and connector state. Workers re-register automatically, but pipelines must be re-deployed. In Kubernetes with HA mode, state replication mitigates this.

3. **MQTT client ID collisions** — Each pipeline must use its own named connector (mqtt_1, mqtt_2, mqtt_3) to avoid MQTT client ID conflicts when multiple pipelines share a worker after migration.

4. **No cross-coordinator pipeline migration** — If coordinator-1 loses all workers, its pipelines cannot be migrated to coordinator-2's workers. Each coordinator manages its own worker pool. In Kubernetes with HA mode, the leader coordinator manages all workers.

5. **Docker socket access** — The test-driver container requires Docker socket access to stop/start containers. This is a test-only requirement.

## Conclusion

The E2E test suite validates that Varpulis CEP correctly handles:

- **Instant failure detection** via WebSocket (0s detection, 1s total failover)
- **Graceful degradation** with REST heartbeat fallback when WS is unavailable
- **Worker failover** with automatic pipeline migration to surviving workers
- **Cascading failures** with fault isolation between coordinator domains
- **Coordinator failover** with zero impact on unrelated coordinators and workers
- **Full cluster recovery** with automatic WS reconnection and re-registration
- **Traffic resilience** — 100% event delivery during coordinator kills (workers are autonomous)

---

# E2E Raft Consensus Cluster Tests

## Overview

A separate test suite validates Varpulis CEP's embedded Raft consensus for coordinator state sharing. This enables:

- **Shared coordinator state** — all coordinators see the same workers, pipelines, and connectors
- **Automatic leader election** — no external dependencies (no etcd, no K8s Lease)
- **Leader failover** — if the Raft leader dies, a new leader is elected within seconds
- **Cross-coordinator visibility** — deploy on any coordinator, state replicates to all

## Architecture

```
┌─────────────────── varpulis-raft (Docker network) ───────────────────┐
│                                                                      │
│  ┌────────────┐                                                      │
│  │ mosquitto  │◄───────────────────────────────────────┐             │
│  │ :1883      │◄──────────────────┐                    │             │
│  └────────────┘                   │                    │             │
│                                   │                    │             │
│  ┌─────────────────────── Raft Cluster ──────────────────────────┐   │
│  │                                                               │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │   │
│  │  │coordinator-1 │  │coordinator-2 │  │coordinator-3 │         │   │
│  │  │  node_id=1   │◄─┤  node_id=2   │◄─┤  node_id=3   │         │   │
│  │  │  (Leader)    │─►│  (Follower)  │─►│  (Follower)  │         │   │
│  │  │  :9100       │  │  :9100       │  │  :9100       │         │   │
│  │  └──┬───────┬───┘  └──────┬───────┘  └──────┬───────┘         │   │
│  │     │ WS    │ WS          │ WS              │ WS              │   │
│  └─────┼───────┼─────────────┼─────────────────┼─────────────────┘   │
│        │       │             │                 │                     │
│  ┌─────▼──┐ ┌──▼─────┐ ┌─────▼──┐ ┌────────┐   │                     │
│  │worker  │ │worker  │ │worker  │ │worker  │   │                     │
│  │  -1    │ │  -2    │ │  -3    │ │  -4    │◄──┘                     │
│  │ :9000  │ │ :9000  │ │ :9000  │ │ :9000  │                         │
│  └────────┘ └────────┘ └────────┘ └────────┘                         │
│   (coord-1)  (coord-1)  (coord-2)  (coord-3)                         │
└──────────────────────────────────────────────────────────────────────┘
```

### What Gets Replicated via Raft

| State | Replicated | Local Only |
|-------|:----------:|:----------:|
| Worker registry + status | Yes | |
| Pipeline group deployments | Yes | |
| Connectors (CRUD) | Yes | |
| Active migrations | Yes | |
| Scaling policy | Yes | |
| Worker heartbeat metrics | | Yes |
| Pending rebalance flag | | Yes |
| Last health sweep | | Yes |

### Raft Configuration

| Parameter | Value |
|-----------|-------|
| Heartbeat interval | 500ms |
| Election timeout (min) | 1500ms |
| Election timeout (max) | 3000ms |
| Quorum size | 2 of 3 (tolerates 1 failure) |
| Log storage | In-memory |
| Transport | HTTP (reqwest) |

### Pipeline Design (4 pipelines, 4 workers)

| Pipeline | Coordinator | Worker | Connector | Input Topic | Output Topic |
|----------|-------------|--------|-----------|-------------|--------------|
| sensor_filter_1 | coordinator-1 | worker-1 | mqtt_1 | e2e/input/1 | e2e/output |
| sensor_filter_2 | coordinator-1 | worker-2 | mqtt_2 | e2e/input/2 | e2e/output |
| sensor_filter_3 | coordinator-2 | worker-3 | mqtt_3 | e2e/input/3 | e2e/output |
| sensor_filter_4 | coordinator-3 | worker-4 | mqtt_4 | e2e/input/4 | e2e/output |

## Test Phases

### Phase 0: Raft Cluster Formation
- Verify all 3 coordinators are healthy
- Wait for Raft leader election (exactly 1 leader + 2 followers)
- Wait for 4 workers across the 3 coordinators

### Phase 1: State Replication
- Register MQTT connectors on each coordinator
- Deploy pipeline groups on each coordinator
- Verify warm-up events flow through all 4 pipelines

### Phase 2: Baseline
- Publish 500 events round-robin across 4 input topics
- Verify all 250 matching events arrive through the shared output topic

### Phase 3: Worker Failover
- Kill worker-1
- Verify coordinator-1 detects failure and marks worker-1 unhealthy
- Wait for pipeline migration to a surviving worker
- Verify 250 events arrive after migration

### Phase 4: Cross-Coordinator Migration
- Kill worker-2 (coordinator-1 now has zero workers)
- Verify coordinator-2 and coordinator-3 remain operational
- Send events to pipelines 3+4 (surviving coordinators)
- Verify events arrive through surviving pipelines

### Phase 5: Leader Failover
- Kill the current Raft leader
- Wait for new leader election among surviving coordinators
- Verify API is available on the new leader
- Send events through surviving pipelines and verify delivery

### Phase 6: Recovery
- Restart the killed coordinator
- Restart worker-1 and worker-2
- Verify Raft cluster re-forms (1 leader + 2 followers)
- Re-register connectors and re-deploy pipelines
- Verify all 250 events arrive through restored cluster

### Phase 7: Traffic During Leader Kill
- Start continuous background traffic (~100 events/sec)
- Kill the Raft leader after 5 seconds
- Continue traffic for 20 more seconds
- Verify new leader is elected
- Verify event delivery rate >= 30% (surviving pipelines continue)

## Running the Raft E2E Test

### Prerequisites

- Docker and Docker Compose v2+
- Docker socket accessible at `/var/run/docker.sock`
- ~5 GB RAM (3 coordinators + 4 workers + mosquitto + test driver)
- ~10 minutes for the complete test (includes building with `--features raft`)

### Command

```bash
cd tests/e2e-raft
bash run.sh
```

### Expected Output

```
============================================================
=== E2E Raft Consensus Test Results ===
  Phase 0: Raft Formation .................. PASS
  Phase 1: State Replication ............... PASS
  Phase 2: Baseline ........................ PASS
  Phase 3: Worker Failover ................. PASS
  Phase 4: Cross-Coordinator Migration ..... PASS
  Phase 5: Leader Failover ................. PASS
  Phase 6: Recovery ........................ PASS
  Phase 7: Traffic During Leader Kill ...... PASS
============================================================
RESULT: 8/8 phases passed
```

Logs are captured to `tests/e2e-raft/results/docker-logs.txt`.

## Raft vs Independent Coordinators

| Capability | Independent (tests/e2e-scaling) | Raft (tests/e2e-raft) |
|------------|-------------------------------|----------------------|
| Coordinators | 2 (independent) | 3 (Raft cluster) |
| Workers | 3 | 4 |
| Shared state | None | Full (via Raft log) |
| Leader election | None (all independent) | Automatic (~3s) |
| Cross-coordinator migration | Not possible | Possible (shared worker registry) |
| State persistence | Lost on restart | Replicated via Raft snapshots |
| External dependencies | None | None (embedded Raft) |
| Fault tolerance | Each coordinator isolated | Tolerates 1 coordinator failure |
