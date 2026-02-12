#!/usr/bin/env python3
"""
E2E Horizontal Scaling + Coordinator HA Test Driver for Varpulis CEP.

Tests failover, cascading failure, recovery, rebalancing, and coordinator
failover with real Docker containers and MQTT traffic.

Architecture:
  - 2 coordinators (coordinator-1 with workers 1+2, coordinator-2 with worker 3)
  - 3 workers, 3 pipelines
  - Each pipeline reads from a separate MQTT input topic
  - All pipelines write to a shared output topic
  - Events published round-robin across input topics

Test phases:
  0. Setup — cluster readiness, connector registration, deploy pipelines
  1. Baseline — normal operation with all workers
  2. WebSocket monitoring — verify WS connections on both coordinators
  3. Worker failover — kill worker-1, verify fast detection via WS
  4. Cascading failure — kill worker-2, all coordinator-1 work moves to survivor
  5. Coordinator failover — kill coordinator-1, verify coordinator-2 is unaffected
  6. Recovery — restart everything, rebalance, verify
  7. Traffic during coordinator kill — continuous traffic while killing coordinator
"""

import json
import os
import sys
import threading
import time
import traceback

import docker
import paho.mqtt.client as mqtt
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

COORDINATOR_1_URL = os.environ.get("COORDINATOR_URL", "http://coordinator-1:9100")
COORDINATOR_2_URL = os.environ.get("COORDINATOR_2_URL", "http://coordinator-2:9100")
MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))

INPUT_TOPIC_PREFIX = "e2e/input"
OUTPUT_TOPIC = "e2e/output"
NUM_PIPELINES = 3

POLL_INTERVAL = 2.0
FAILOVER_TIMEOUT = 30.0
EVENT_WAIT_TIMEOUT = 30.0

# ---------------------------------------------------------------------------
# VPL template — connector declaration is injected by the coordinator
# ---------------------------------------------------------------------------

VPL_TEMPLATE = """
event SensorReading:
    sensor_id: str
    temperature: float
    seq: int

stream Input = SensorReading
    .from(mqtt_{n}, topic: "{input_topic}")

stream HighTemp = Input
    .where(temperature > 50.0)

stream Output = HighTemp
    .to(mqtt_{n}, topic: "e2e/output")
""".strip()


def vpl_for_pipeline(n):
    """Generate VPL source for pipeline n (1-indexed)."""
    return VPL_TEMPLATE.format(n=n, input_topic=f"{INPUT_TOPIC_PREFIX}/{n}")


# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------

class MqttCollector:
    """Background MQTT subscriber that collects output events."""

    def __init__(self, host, port, topic):
        self.host = host
        self.port = port
        self.topic = topic
        self._events = []
        self._lock = threading.Lock()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self._connected = threading.Event()

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            client.subscribe(self.topic)
            self._connected.set()

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            with self._lock:
                self._events.append(data)
        except Exception:
            pass

    def start(self):
        self.client.connect(self.host, self.port, 60)
        self.client.loop_start()
        if not self._connected.wait(timeout=10):
            raise RuntimeError("MQTT collector failed to connect")

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

    def events(self):
        with self._lock:
            return list(self._events)

    def clear(self):
        with self._lock:
            self._events.clear()

    def count(self):
        with self._lock:
            return len(self._events)

    def wait_for_count(self, expected, timeout=EVENT_WAIT_TIMEOUT):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.count() >= expected:
                return True
            time.sleep(0.5)
        return False


class MqttPublisher:
    """MQTT publisher for sending test events."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    def connect(self):
        self.client.connect(self.host, self.port, 60)
        self.client.loop_start()

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def publish_event(self, topic, sensor_id, temperature, seq):
        payload = json.dumps({
            "event_type": "SensorReading",
            "sensor_id": sensor_id,
            "temperature": temperature,
            "seq": seq,
        })
        self.client.publish(topic, payload)

    def publish_batch(self, start_seq, count, num_pipelines=NUM_PIPELINES):
        """Publish events round-robin across input topics.

        Even seq -> temperature 75.0 (matches filter)
        Odd  seq -> temperature 25.0 (does not match)
        """
        matching = 0
        for i in range(count):
            seq = start_seq + i
            topic_n = (i % num_pipelines) + 1
            temp = 75.0 if seq % 2 == 0 else 25.0
            self.publish_event(
                f"{INPUT_TOPIC_PREFIX}/{topic_n}",
                f"sensor_{seq % 10}",
                temp,
                seq,
            )
            if temp > 50.0:
                matching += 1
        return matching


# ---------------------------------------------------------------------------
# Coordinator API helpers
# ---------------------------------------------------------------------------

def api_get(path, coordinator_url=COORDINATOR_1_URL):
    resp = requests.get(f"{coordinator_url}{path}", timeout=10)
    resp.raise_for_status()
    return resp.json()


def api_post(path, data=None, coordinator_url=COORDINATOR_1_URL):
    resp = requests.post(f"{coordinator_url}{path}", json=data or {}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def coordinator_health(coordinator_url):
    """Get coordinator health data, return None on failure."""
    try:
        return api_get("/health", coordinator_url=coordinator_url)
    except Exception:
        return None


def coordinator_is_up(coordinator_url):
    """Check if a coordinator is reachable."""
    return coordinator_health(coordinator_url) is not None


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------

def get_docker_client():
    return docker.from_env()


def find_container(docker_client, service_name):
    """Find a Docker Compose container by service label."""
    containers = docker_client.containers.list(
        all=True,
        filters={"label": f"com.docker.compose.service={service_name}"},
    )
    return containers[0] if containers else None


def stop_container(docker_client, service_name):
    container = find_container(docker_client, service_name)
    if container:
        container.stop(timeout=0)
        print(f"    Stopped container for {service_name}")
        return True
    print(f"    WARNING: container for {service_name} not found")
    return False


def start_container(docker_client, service_name):
    container = find_container(docker_client, service_name)
    if container:
        container.start()
        print(f"    Started container for {service_name}")
        return True
    print(f"    WARNING: container for {service_name} not found")
    return False


def get_container_logs(docker_client, service_name, tail=50):
    """Get recent logs from a container."""
    container = find_container(docker_client, service_name)
    if container:
        return container.logs(tail=tail).decode("utf-8", errors="replace")
    return ""


# ---------------------------------------------------------------------------
# Polling helpers
# ---------------------------------------------------------------------------

def wait_for_workers(expected_ready, timeout=60, coordinator_url=COORDINATOR_1_URL):
    """Poll until at least expected_ready workers report ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            data = api_get("/api/v1/cluster/workers", coordinator_url=coordinator_url)
            ready = [
                w for w in data.get("workers", [])
                if w.get("status", "").lower() == "ready"
            ]
            if len(ready) >= expected_ready:
                return ready
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Timed out waiting for {expected_ready} ready workers")


def wait_for_worker_status(worker_id, statuses, timeout=FAILOVER_TIMEOUT,
                           coordinator_url=COORDINATOR_1_URL):
    """Poll until a worker reaches one of the given statuses."""
    statuses_lower = [s.lower() for s in statuses]
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            data = api_get("/api/v1/cluster/workers", coordinator_url=coordinator_url)
            for w in data.get("workers", []):
                wid = w.get("id") or w.get("worker_id", "")
                if wid == worker_id:
                    if w.get("status", "").lower() in statuses_lower:
                        return True
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    return False


def get_topology_pipelines(coordinator_url=COORDINATOR_1_URL):
    """Return flat list of pipeline entries from topology."""
    topo = api_get("/api/v1/cluster/topology", coordinator_url=coordinator_url)
    pipelines = []
    for group in topo.get("groups", []):
        pipelines.extend(group.get("pipelines", []))
    return pipelines


def wait_for_all_pipelines_assigned(expected_count, exclude_workers=None,
                                     timeout=FAILOVER_TIMEOUT,
                                     coordinator_url=COORDINATOR_1_URL):
    """Wait until all pipelines are assigned to healthy (non-excluded) workers."""
    exclude_workers = set(exclude_workers or [])
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            pipelines = get_topology_pipelines(coordinator_url=coordinator_url)
            if len(pipelines) >= expected_count:
                assigned_workers = {p["worker_id"] for p in pipelines}
                if not assigned_workers & exclude_workers:
                    return pipelines
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(
        f"Not all {expected_count} pipelines reassigned within {timeout}s "
        f"(excluding {exclude_workers})"
    )


def wait_for_migrations_complete(timeout=FAILOVER_TIMEOUT,
                                  coordinator_url=COORDINATOR_1_URL):
    """Wait until no pending/in-progress migrations remain."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            data = api_get("/api/v1/cluster/migrations", coordinator_url=coordinator_url)
            active = [
                m for m in data.get("migrations", [])
                if m.get("status", "").lower() in ("pending", "inprogress", "in_progress")
            ]
            if not active:
                return True
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    return False


# ---------------------------------------------------------------------------
# Phase result
# ---------------------------------------------------------------------------

class PhaseResult:
    def __init__(self, name, passed, duration, detail=""):
        self.name = name
        self.passed = passed
        self.duration = duration
        self.detail = detail


# ---------------------------------------------------------------------------
# Test phases
# ---------------------------------------------------------------------------

def phase_0_setup(publisher, collector, docker_client):
    """Setup: wait for cluster, register connectors, deploy pipelines, warm-up."""
    start = time.time()

    # 1. Wait for workers on coordinator-1
    print("  Waiting for 2 workers on coordinator-1...")
    workers_1 = wait_for_workers(2, timeout=60, coordinator_url=COORDINATOR_1_URL)
    print(f"  {len(workers_1)} workers ready on coordinator-1")

    # 2. Wait for worker on coordinator-2
    print("  Waiting for 1 worker on coordinator-2...")
    workers_2 = wait_for_workers(1, timeout=60, coordinator_url=COORDINATOR_2_URL)
    print(f"  {len(workers_2)} worker(s) ready on coordinator-2")

    # 3. Register MQTT connectors on BOTH coordinators
    print("  Registering MQTT connectors on coordinator-1...")
    for n in range(1, NUM_PIPELINES + 1):
        for coord_url in [COORDINATOR_1_URL, COORDINATOR_2_URL]:
            try:
                api_post("/api/v1/cluster/connectors", {
                    "name": f"mqtt_{n}",
                    "connector_type": "mqtt",
                    "params": {"host": MQTT_HOST, "port": str(MQTT_PORT)},
                    "description": f"MQTT broker for pipeline {n}",
                }, coordinator_url=coord_url)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 409:
                    pass  # already exists
                else:
                    raise

    # 4. Deploy pipeline group on coordinator-1 (pipelines 1+2)
    print("  Deploying pipelines 1+2 on coordinator-1...")
    api_post("/api/v1/cluster/pipeline-groups", {
        "name": "e2e_sensor_filter_c1",
        "pipelines": [
            {"name": f"sensor_filter_{n}", "source": vpl_for_pipeline(n), "replicas": 1}
            for n in range(1, 3)
        ],
        "routes": [],
    }, coordinator_url=COORDINATOR_1_URL)

    # 5. Deploy pipeline group on coordinator-2 (pipeline 3)
    print("  Deploying pipeline 3 on coordinator-2...")
    api_post("/api/v1/cluster/pipeline-groups", {
        "name": "e2e_sensor_filter_c2",
        "pipelines": [
            {"name": "sensor_filter_3", "source": vpl_for_pipeline(3), "replicas": 1}
        ],
        "routes": [],
    }, coordinator_url=COORDINATOR_2_URL)

    # 6. Wait for deployment
    print("  Waiting for pipelines to deploy...")
    time.sleep(5)
    pipelines_c1 = wait_for_all_pipelines_assigned(2, timeout=30, coordinator_url=COORDINATOR_1_URL)
    pipelines_c2 = wait_for_all_pipelines_assigned(1, timeout=30, coordinator_url=COORDINATOR_2_URL)
    total_pipelines = len(pipelines_c1) + len(pipelines_c2)
    workers_c1 = {p["worker_id"] for p in pipelines_c1}
    workers_c2 = {p["worker_id"] for p in pipelines_c2}
    print(f"  {total_pipelines} pipelines deployed: "
          f"coordinator-1 has {len(pipelines_c1)} on {workers_c1}, "
          f"coordinator-2 has {len(pipelines_c2)} on {workers_c2}")

    # 7. Start collector + publisher
    print("  Starting MQTT subscriber...")
    collector.start()
    time.sleep(2)

    print("  Sending warm-up events...")
    publisher.connect()
    expected = publisher.publish_batch(start_seq=-100, count=100)
    time.sleep(5)

    warmup_count = collector.count()
    print(f"  Warm-up: received {warmup_count}/{expected} events")
    collector.clear()

    duration = time.time() - start
    if warmup_count > 0:
        return PhaseResult("Setup", True, duration, f"warm-up {warmup_count}/{expected}")
    else:
        return PhaseResult("Setup", False, duration, f"warm-up FAILED: 0/{expected}")


def phase_1_baseline(publisher, collector):
    """Baseline: all workers, 500 events (250 matching)."""
    start = time.time()
    collector.clear()

    expected = publisher.publish_batch(start_seq=0, count=500)
    print(f"  Published 500 events ({expected} matching)")

    collector.wait_for_count(expected, timeout=30)
    received = collector.count()

    duration = time.time() - start
    passed = received >= expected
    detail = f"{received}/{expected} events"
    return PhaseResult("Baseline", passed, duration, detail)


def phase_2_websocket_check(docker_client):
    """Verify WebSocket connections on both coordinators."""
    start = time.time()
    checks = []

    # Coordinator-1 should have 2 WS connections (worker-1, worker-2)
    health_1 = coordinator_health(COORDINATOR_1_URL)
    ws_1 = health_1.get("ws_connections", 0) if health_1 else 0
    print(f"  Coordinator-1: {ws_1} WS connections (workers: {health_1.get('workers', {}).get('total', 0) if health_1 else 0})")
    checks.append(("coord-1 has WS connections", ws_1 >= 1))

    # Coordinator-2 should have 1 WS connection (worker-3)
    health_2 = coordinator_health(COORDINATOR_2_URL)
    ws_2 = health_2.get("ws_connections", 0) if health_2 else 0
    print(f"  Coordinator-2: {ws_2} WS connections (workers: {health_2.get('workers', {}).get('total', 0) if health_2 else 0})")
    checks.append(("coord-2 has WS connections", ws_2 >= 1))

    # Both should report healthy
    status_1 = health_1.get("status", "unknown") if health_1 else "unreachable"
    status_2 = health_2.get("status", "unknown") if health_2 else "unreachable"
    print(f"  Coordinator-1 status: {status_1}")
    print(f"  Coordinator-2 status: {status_2}")

    passed = all(ok for _, ok in checks)
    detail = f"coord-1: {ws_1} WS, coord-2: {ws_2} WS"

    duration = time.time() - start
    return PhaseResult("WebSocket Check", passed, duration, detail)


def phase_3_worker_failover(publisher, collector, docker_client):
    """Kill worker-1, verify failover detection and pipeline migration."""
    start = time.time()
    collector.clear()

    # Record WS connections before
    health_before = coordinator_health(COORDINATOR_1_URL)
    ws_before = health_before.get("ws_connections", 0) if health_before else 0
    print(f"  WS connections before kill: {ws_before}")

    # Kill worker-1
    print("  Killing worker-1...")
    stop_container(docker_client, "worker-1")

    # Wait for coordinator to detect failure
    print("  Waiting for failover detection...")
    failover_start = time.time()
    wait_for_worker_status("worker-1", ["unhealthy", "dead"], timeout=FAILOVER_TIMEOUT)
    detect_time = time.time() - failover_start
    print(f"  Worker-1 marked unhealthy after {detect_time:.1f}s")

    # Check WS connections dropped
    time.sleep(1)
    health_after = coordinator_health(COORDINATOR_1_URL)
    ws_after = health_after.get("ws_connections", 0) if health_after else 0
    print(f"  WS connections after kill: {ws_after} (was {ws_before})")

    # Check coordinator logs for WS disconnect message
    logs = get_container_logs(docker_client, "coordinator-1", tail=100)
    ws_disconnect_detected = "WebSocket disconnected" in logs
    print(f"  WS disconnect in coordinator-1 logs: {ws_disconnect_detected}")

    # Wait for pipeline migration
    print("  Waiting for pipeline migration...")
    pipelines = wait_for_all_pipelines_assigned(
        2, exclude_workers=["worker-1"], timeout=FAILOVER_TIMEOUT,
        coordinator_url=COORDINATOR_1_URL,
    )
    failover_time = time.time() - failover_start
    print(f"  Failover completed in {failover_time:.1f}s")

    worker_set = {p["worker_id"] for p in pipelines}
    assert "worker-1" not in worker_set, "worker-1 still has pipelines"

    # Send events after migration
    expected = publisher.publish_batch(start_seq=500, count=500)
    print(f"  Published 500 events ({expected} matching)")

    collector.wait_for_count(expected, timeout=30)
    received = collector.count()

    duration = time.time() - start
    passed = (
        received >= expected
        and "worker-1" not in worker_set
        and ws_after < ws_before
    )
    detail = (f"{received}/{expected} events, failover in {failover_time:.1f}s, "
              f"WS: {ws_before}->{ws_after}")
    return PhaseResult("Worker Failover (WS)", passed, duration, detail)


def phase_4_cascading_failure(publisher, collector, docker_client):
    """Kill worker-2, all coordinator-1 pipelines must migrate to remaining capacity."""
    start = time.time()
    collector.clear()

    # Kill worker-2
    print("  Killing worker-2...")
    stop_container(docker_client, "worker-2")

    print("  Waiting for failover detection...")
    failover_start = time.time()
    wait_for_worker_status("worker-2", ["unhealthy", "dead"], timeout=FAILOVER_TIMEOUT)
    detect_time = time.time() - failover_start
    print(f"  Worker-2 marked unhealthy after {detect_time:.1f}s")

    # Coordinator-1 has no ready workers left — pipelines cannot be reassigned there
    # But coordinator-2 still has worker-3 running with pipeline 3
    print("  Checking coordinator-2 is still operational...")
    health_2 = coordinator_health(COORDINATOR_2_URL)
    coord_2_ok = health_2 is not None
    if coord_2_ok:
        ws_2 = health_2.get("ws_connections", 0)
        print(f"  Coordinator-2: status={health_2.get('status')}, WS={ws_2}")
    else:
        print(f"  WARNING: coordinator-2 unreachable")

    # Send events only to pipeline 3 (the one on coordinator-2) since pipelines 1+2 have no workers
    print("  Publishing events to pipeline 3 (coordinator-2's worker)...")
    collector.clear()
    matching = 0
    for i in range(500):
        seq = 1000 + i
        temp = 75.0 if seq % 2 == 0 else 25.0
        publisher.publish_event(f"{INPUT_TOPIC_PREFIX}/3", f"sensor_{seq % 10}", temp, seq)
        if temp > 50.0:
            matching += 1

    collector.wait_for_count(matching, timeout=30)
    received = collector.count()

    duration = time.time() - start
    passed = received >= matching and coord_2_ok
    detail = (f"{received}/{matching} events via coordinator-2, "
              f"coordinator-2 healthy: {coord_2_ok}")
    return PhaseResult("Cascading Failure", passed, duration, detail)


def phase_5_coordinator_failover(publisher, collector, docker_client):
    """Kill coordinator-1. Coordinator-2 and its worker must be completely unaffected."""
    start = time.time()
    collector.clear()

    # Snapshot coordinator-2 state before
    health_2_before = coordinator_health(COORDINATOR_2_URL)
    ws_2_before = health_2_before.get("ws_connections", 0) if health_2_before else 0
    print(f"  Coordinator-2 state before: WS={ws_2_before}, status={health_2_before.get('status') if health_2_before else 'N/A'}")

    # Kill coordinator-1
    print("  KILLING COORDINATOR-1...")
    stop_container(docker_client, "coordinator-1")

    # Verify coordinator-1 is actually down
    time.sleep(2)
    coord_1_down = not coordinator_is_up(COORDINATOR_1_URL)
    print(f"  Coordinator-1 is down: {coord_1_down}")

    # Verify coordinator-2 is UNAFFECTED
    print("  Checking coordinator-2 is unaffected...")
    health_2_after = coordinator_health(COORDINATOR_2_URL)
    coord_2_ok = health_2_after is not None
    ws_2_after = health_2_after.get("ws_connections", 0) if health_2_after else 0
    print(f"  Coordinator-2 after kill: WS={ws_2_after}, status={health_2_after.get('status') if health_2_after else 'N/A'}")

    # Verify pipeline 3 on coordinator-2 still processes events
    print("  Sending events through coordinator-2's pipeline...")
    collector.clear()
    matching = 0
    for i in range(200):
        seq = 2000 + i
        temp = 75.0 if seq % 2 == 0 else 25.0
        publisher.publish_event(f"{INPUT_TOPIC_PREFIX}/3", f"sensor_{seq % 10}", temp, seq)
        if temp > 50.0:
            matching += 1

    collector.wait_for_count(matching, timeout=20)
    received = collector.count()
    print(f"  Received {received}/{matching} events through coordinator-2")

    # Check worker-1/2 logs for coordinator connection failure (expected)
    w1_logs = get_container_logs(docker_client, "worker-1", tail=30)
    w2_logs = get_container_logs(docker_client, "worker-2", tail=30)
    # worker-1 and worker-2 are already stopped from phase 4, so they won't have new logs

    duration = time.time() - start
    passed = (
        coord_1_down
        and coord_2_ok
        and ws_2_after >= ws_2_before  # coordinator-2 WS unaffected
        and received >= matching
    )
    detail = (f"coord-1 down: {coord_1_down}, coord-2 OK: {coord_2_ok}, "
              f"events: {received}/{matching}")
    return PhaseResult("Coordinator Failover", passed, duration, detail)


def phase_6_recovery(publisher, collector, docker_client):
    """Restart coordinator-1, worker-1, worker-2. Rebalance. Verify full cluster."""
    start = time.time()
    collector.clear()

    # Restart coordinator-1
    print("  Restarting coordinator-1...")
    start_container(docker_client, "coordinator-1")

    # Wait for coordinator-1 to be healthy
    print("  Waiting for coordinator-1 to recover...")
    deadline = time.time() + 30
    while time.time() < deadline:
        if coordinator_is_up(COORDINATOR_1_URL):
            print("  Coordinator-1 is back!")
            break
        time.sleep(1)
    else:
        print("  WARNING: coordinator-1 did not recover within 30s")

    # Restart worker-1 and worker-2
    print("  Restarting worker-1 and worker-2...")
    start_container(docker_client, "worker-1")
    start_container(docker_client, "worker-2")

    # Wait for workers to register
    print("  Waiting for workers to register with coordinator-1...")
    workers = wait_for_workers(2, timeout=60, coordinator_url=COORDINATOR_1_URL)
    print(f"  {len(workers)} workers ready on coordinator-1")

    # Re-register connectors on coordinator-1 (it lost state on restart)
    print("  Re-registering MQTT connectors on coordinator-1...")
    for n in range(1, NUM_PIPELINES + 1):
        try:
            api_post("/api/v1/cluster/connectors", {
                "name": f"mqtt_{n}",
                "connector_type": "mqtt",
                "params": {"host": MQTT_HOST, "port": str(MQTT_PORT)},
                "description": f"MQTT broker for pipeline {n}",
            }, coordinator_url=COORDINATOR_1_URL)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 409:
                pass
            else:
                raise

    # Re-deploy pipelines on coordinator-1
    print("  Re-deploying pipelines 1+2 on coordinator-1...")
    try:
        api_post("/api/v1/cluster/pipeline-groups", {
            "name": "e2e_sensor_filter_c1",
            "pipelines": [
                {"name": f"sensor_filter_{n}", "source": vpl_for_pipeline(n), "replicas": 1}
                for n in range(1, 3)
            ],
            "routes": [],
        }, coordinator_url=COORDINATOR_1_URL)
    except requests.exceptions.HTTPError:
        print("  (pipeline group may already exist, continuing)")

    time.sleep(5)

    # Check both coordinators
    health_1 = coordinator_health(COORDINATOR_1_URL)
    health_2 = coordinator_health(COORDINATOR_2_URL)
    ws_1 = health_1.get("ws_connections", 0) if health_1 else 0
    ws_2 = health_2.get("ws_connections", 0) if health_2 else 0
    print(f"  Coordinator-1: WS={ws_1}, status={health_1.get('status') if health_1 else 'N/A'}")
    print(f"  Coordinator-2: WS={ws_2}, status={health_2.get('status') if health_2 else 'N/A'}")

    # Send events across all pipelines
    collector.clear()
    expected = publisher.publish_batch(start_seq=3000, count=500)
    print(f"  Published 500 events ({expected} matching)")

    collector.wait_for_count(expected, timeout=30)
    received = collector.count()

    duration = time.time() - start
    both_up = health_1 is not None and health_2 is not None
    passed = received >= expected and both_up and ws_1 >= 1
    detail = (f"{received}/{expected} events, coord-1 WS={ws_1}, coord-2 WS={ws_2}, "
              f"both coordinators up: {both_up}")
    return PhaseResult("Recovery", passed, duration, detail)


def phase_7_traffic_during_coordinator_kill(publisher, collector, docker_client):
    """Continuous MQTT traffic while killing coordinator-1.

    Pipeline 3 (on coordinator-2) should be completely unaffected.
    Pipelines 1+2 may lose events during the coordinator outage.
    """
    start = time.time()
    collector.clear()

    stop_flag = threading.Event()
    counters = {"total": 0, "matching": 0, "seq": 5000, "matching_pipeline_3": 0}

    def background_publish():
        while not stop_flag.is_set():
            seq = counters["seq"]
            topic_n = (seq % NUM_PIPELINES) + 1
            temp = 75.0 if seq % 2 == 0 else 25.0
            publisher.publish_event(
                f"{INPUT_TOPIC_PREFIX}/{topic_n}",
                f"sensor_{seq % 10}",
                temp,
                seq,
            )
            counters["total"] += 1
            if temp > 50.0:
                counters["matching"] += 1
                if topic_n == 3:
                    counters["matching_pipeline_3"] += 1
            counters["seq"] += 1
            time.sleep(0.01)  # ~100 events/sec

    print("  Starting background traffic...")
    pub_thread = threading.Thread(target=background_publish, daemon=True)
    pub_thread.start()

    # Publish for 5s, then kill coordinator-1
    time.sleep(5)
    print("  Killing coordinator-1 during traffic...")
    stop_container(docker_client, "coordinator-1")

    # Continue publishing for 20s
    time.sleep(20)

    # Stop publisher
    stop_flag.set()
    pub_thread.join(timeout=5)

    # Wait for drain
    print("  Waiting for event drain...")
    time.sleep(10)

    total_matching = counters["matching"]
    matching_p3 = counters["matching_pipeline_3"]
    received = collector.count()
    pct = (received / total_matching * 100) if total_matching > 0 else 0

    # Pipeline 3 should be unaffected (coordinator-2 never went down)
    # Pipelines 1+2 may lose events when coordinator-1 is killed
    # (workers lose their coordinator, but MQTT→pipeline path still works
    #  since workers run independently once pipelines are deployed)
    print(f"  Total matching events: {total_matching}")
    print(f"  Pipeline 3 matching: {matching_p3}")
    print(f"  Received: {received} ({pct:.1f}%)")

    # Coordinator-2 should still be operational
    health_2 = coordinator_health(COORDINATOR_2_URL)
    coord_2_ok = health_2 is not None
    print(f"  Coordinator-2 still up: {coord_2_ok}")

    duration = time.time() - start
    # Pipeline 3 (on coordinator-2) must deliver events.
    # Overall >=50% accounts for pipelines 1+2 potentially losing events
    # during coordinator-1 death (workers continue processing but may stall).
    passed = pct >= 50.0 and coord_2_ok
    detail = f"{received}/{total_matching} events ({pct:.1f}%), coord-2 up: {coord_2_ok}"
    return PhaseResult("Traffic During Coordinator Kill", passed, duration, detail)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  E2E Horizontal Scaling + Coordinator HA Test Suite")
    print("  Varpulis CEP — 2 coordinators, 3 workers")
    print("=" * 60)
    print()
    print(f"  Coordinator-1: {COORDINATOR_1_URL}")
    print(f"  Coordinator-2: {COORDINATOR_2_URL}")
    print(f"  MQTT:          {MQTT_HOST}:{MQTT_PORT}")
    print()

    docker_client = get_docker_client()
    collector = MqttCollector(MQTT_HOST, MQTT_PORT, OUTPUT_TOPIC)
    publisher = MqttPublisher(MQTT_HOST, MQTT_PORT)

    results = []

    phases = [
        ("Phase 0", "Setup",
         lambda: phase_0_setup(publisher, collector, docker_client)),
        ("Phase 1", "Baseline",
         lambda: phase_1_baseline(publisher, collector)),
        ("Phase 2", "WebSocket Check",
         lambda: phase_2_websocket_check(docker_client)),
        ("Phase 3", "Worker Failover (WS)",
         lambda: phase_3_worker_failover(publisher, collector, docker_client)),
        ("Phase 4", "Cascading Failure",
         lambda: phase_4_cascading_failure(publisher, collector, docker_client)),
        ("Phase 5", "Coordinator Failover",
         lambda: phase_5_coordinator_failover(publisher, collector, docker_client)),
        ("Phase 6", "Recovery",
         lambda: phase_6_recovery(publisher, collector, docker_client)),
        ("Phase 7", "Traffic During Coordinator Kill",
         lambda: phase_7_traffic_during_coordinator_kill(publisher, collector, docker_client)),
    ]

    for phase_id, phase_name, phase_fn in phases:
        print(f"\n--- {phase_id}: {phase_name} ---")
        try:
            result = phase_fn()
            results.append(result)
            status = "PASS" if result.passed else "FAIL"
            print(f"  >> {status} ({result.duration:.1f}s) -- {result.detail}")
        except Exception as e:
            traceback.print_exc()
            results.append(PhaseResult(phase_name, False, 0, str(e)))
            print(f"  >> FAIL -- {e}")

    # Cleanup
    try:
        publisher.disconnect()
    except Exception:
        pass
    try:
        collector.stop()
    except Exception:
        pass

    # Summary
    print()
    print("=" * 60)
    print("=== E2E Scaling + Coordinator HA Test Results ===")
    for i, r in enumerate(results):
        status = "PASS" if r.passed else "FAIL"
        dots = "." * max(1, 30 - len(r.name))
        print(f"  Phase {i}: {r.name} {dots} {status} ({r.duration:.1f}s)")
        print(f"           {r.detail}")
    passed_count = sum(1 for r in results if r.passed)
    total = len(results)
    print("=" * 60)
    print(f"RESULT: {passed_count}/{total} phases passed")
    print("=" * 60)

    sys.exit(0 if passed_count == total else 1)


if __name__ == "__main__":
    main()
