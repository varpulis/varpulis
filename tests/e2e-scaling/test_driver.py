#!/usr/bin/env python3
"""
E2E Horizontal Scaling + Raft Coordinator HA Test Driver for Varpulis CEP.

Tests failover, cascading failure, recovery, rebalancing, and Raft coordinator
failover with real Docker containers and MQTT traffic.

Architecture:
  - 3 Raft coordinators (coordinator-1=node1, coordinator-2=node2, coordinator-3=node3)
  - 4 workers: workers 1+2 -> coordinator-1, worker-3 -> coordinator-2, worker-4 -> coordinator-3
  - 3 pipelines (across input topics 1-3)
  - Each pipeline reads from a separate MQTT input topic
  - All pipelines write to a shared output topic
  - Events published round-robin across input topics

Test phases:
  0. Setup — Raft leader elected, cluster ready, connectors + pipelines deployed
  1. Baseline — normal operation with all workers
  2. WebSocket monitoring — verify WS connections on all 3 coordinators
  3. Worker failover — kill worker-1, verify fast detection via WS
  4. Cascading failure — kill worker-2, all coordinator-1 work moves to survivors
  5. Coordinator failover — kill Raft leader, verify new leader elected, API available
  6. Recovery — restart everything, rebalance, verify
  7. Traffic during coordinator kill — continuous traffic while killing Raft leader
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
COORDINATOR_3_URL = os.environ.get("COORDINATOR_3_URL", "http://coordinator-3:9100")

COORDINATOR_URLS = [COORDINATOR_1_URL, COORDINATOR_2_URL, COORDINATOR_3_URL]

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))

INPUT_TOPIC_PREFIX = "e2e/input"
OUTPUT_TOPIC = "e2e/output"
NUM_PIPELINES = 3

POLL_INTERVAL = 2.0
FAILOVER_TIMEOUT = 45.0
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
# Coordinator + Raft API helpers
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


def raft_metrics(coordinator_url):
    """Get Raft metrics from a coordinator. Returns None on failure."""
    try:
        return api_get("/raft/metrics", coordinator_url=coordinator_url)
    except Exception:
        return None


def find_raft_leader():
    """Find the current Raft leader across all coordinators.

    Returns (leader_node_id, leader_url) or (None, None).
    """
    for url in COORDINATOR_URLS:
        m = raft_metrics(url)
        if m and m.get("current_leader") not in (None, "null"):
            leader_id = int(m["current_leader"])
            if 1 <= leader_id <= len(COORDINATOR_URLS):
                return leader_id, COORDINATOR_URLS[leader_id - 1]
    return None, None


def wait_for_raft_leader(timeout=30, exclude_node=None):
    """Wait until a Raft leader is elected. Returns (leader_id, leader_url)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        leader_id, leader_url = find_raft_leader()
        if leader_id is not None:
            if exclude_node is None or leader_id != exclude_node:
                return leader_id, leader_url
        time.sleep(1)
    raise TimeoutError(f"No Raft leader elected within {timeout}s")


def get_raft_role(coordinator_url):
    """Get Raft role: 'Leader', 'Follower', 'Candidate', or 'Unknown'."""
    m = raft_metrics(coordinator_url)
    if m:
        return m.get("state", "Unknown")
    return "Unreachable"


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
    """Setup: Raft leader elected, cluster ready, connectors + pipelines deployed."""
    start = time.time()

    # 1. Check all 3 coordinators are healthy
    print("  Checking all 3 coordinators are healthy...")
    all_healthy = True
    for i, url in enumerate(COORDINATOR_URLS, 1):
        health = coordinator_health(url)
        if health:
            print(f"    Coordinator-{i}: status={health.get('status')}, "
                  f"workers={health.get('workers', {}).get('total', 0)}")
        else:
            print(f"    Coordinator-{i}: UNREACHABLE")
            all_healthy = False

    # 2. Wait for Raft leader election
    print("  Waiting for Raft leader election...")
    leader_id, leader_url = wait_for_raft_leader(timeout=30)
    print(f"  Raft leader: node {leader_id} ({leader_url})")

    # 3. Check Raft roles
    for i, url in enumerate(COORDINATOR_URLS, 1):
        role = get_raft_role(url)
        print(f"    Coordinator-{i} Raft role: {role}")

    # 4. Wait for all 4 workers to be visible (Raft-replicated to all nodes)
    print("  Waiting for all 4 workers via Raft sync...")
    all_workers = wait_for_workers(4, timeout=60, coordinator_url=leader_url)
    print(f"    {len(all_workers)} workers ready on leader")
    # Also verify visibility on followers
    for i, url in enumerate(COORDINATOR_URLS, 1):
        try:
            data = api_get("/api/v1/cluster/workers", coordinator_url=url)
            count = len([w for w in data.get("workers", [])
                        if w.get("status", "").lower() == "ready"])
            print(f"    Coordinator-{i} sees {count} workers")
        except Exception:
            print(f"    Coordinator-{i}: unreachable")

    # 5. Register MQTT connectors on the Raft leader
    print(f"  Registering MQTT connectors on leader ({leader_url})...")
    for n in range(1, NUM_PIPELINES + 1):
        try:
            api_post("/api/v1/cluster/connectors", {
                "name": f"mqtt_{n}",
                "connector_type": "mqtt",
                "params": {"host": MQTT_HOST, "port": str(MQTT_PORT)},
                "description": f"MQTT broker for pipeline {n}",
            }, coordinator_url=leader_url)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 409:
                pass  # already exists
            else:
                raise

    # 6. Deploy all pipeline groups on the Raft leader
    print(f"  Deploying pipelines 1+2 on leader...")
    api_post("/api/v1/cluster/pipeline-groups", {
        "name": "e2e_sensor_filter_c1",
        "pipelines": [
            {"name": f"sensor_filter_{n}", "source": vpl_for_pipeline(n), "replicas": 1}
            for n in range(1, 3)
        ],
        "routes": [],
    }, coordinator_url=leader_url)

    print(f"  Deploying pipeline 3 on leader...")
    api_post("/api/v1/cluster/pipeline-groups", {
        "name": "e2e_sensor_filter_c2",
        "pipelines": [
            {"name": "sensor_filter_3", "source": vpl_for_pipeline(3), "replicas": 1}
        ],
        "routes": [],
    }, coordinator_url=leader_url)

    # 7. Wait for deployment (check on leader)
    print("  Waiting for pipelines to deploy...")
    time.sleep(5)
    all_pipelines = wait_for_all_pipelines_assigned(3, timeout=30, coordinator_url=leader_url)
    assigned_workers = {p["worker_id"] for p in all_pipelines}
    print(f"  {len(all_pipelines)} pipelines deployed on workers: {assigned_workers}")

    # 8. Start collector + publisher
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
        return PhaseResult("Setup", True, duration,
                           f"leader=node {leader_id}, warm-up {warmup_count}/{expected}")
    else:
        return PhaseResult("Setup", False, duration,
                           f"warm-up FAILED: 0/{expected}")


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
    """Verify WebSocket connections on all 3 coordinators."""
    start = time.time()
    checks = []

    # Coordinator-1 should have 2 WS connections (worker-1, worker-2)
    health_1 = coordinator_health(COORDINATOR_1_URL)
    ws_1 = health_1.get("ws_connections", 0) if health_1 else 0
    print(f"  Coordinator-1: {ws_1} WS connections "
          f"(workers: {health_1.get('workers', {}).get('total', 0) if health_1 else 0})")
    checks.append(("coord-1 has WS connections", ws_1 >= 1))

    # Coordinator-2 should have 1 WS connection (worker-3)
    health_2 = coordinator_health(COORDINATOR_2_URL)
    ws_2 = health_2.get("ws_connections", 0) if health_2 else 0
    print(f"  Coordinator-2: {ws_2} WS connections "
          f"(workers: {health_2.get('workers', {}).get('total', 0) if health_2 else 0})")
    checks.append(("coord-2 has WS connections", ws_2 >= 1))

    # Coordinator-3 should have 1 WS connection (worker-4)
    health_3 = coordinator_health(COORDINATOR_3_URL)
    ws_3 = health_3.get("ws_connections", 0) if health_3 else 0
    print(f"  Coordinator-3: {ws_3} WS connections "
          f"(workers: {health_3.get('workers', {}).get('total', 0) if health_3 else 0})")
    checks.append(("coord-3 has WS connections", ws_3 >= 1))

    # All should report healthy
    for i, url in enumerate(COORDINATOR_URLS, 1):
        h = coordinator_health(url)
        status = h.get("status", "unknown") if h else "unreachable"
        role = get_raft_role(url)
        print(f"  Coordinator-{i} status: {status}, Raft role: {role}")

    passed = all(ok for _, ok in checks)
    detail = f"coord-1: {ws_1} WS, coord-2: {ws_2} WS, coord-3: {ws_3} WS"

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

    # Coordinator-1 has no ready workers left
    # Coordinator-2 has worker-3 with pipeline 3
    # Coordinator-3 has worker-4 (idle)
    print("  Checking coordinator-2 and coordinator-3 are still operational...")
    health_2 = coordinator_health(COORDINATOR_2_URL)
    health_3 = coordinator_health(COORDINATOR_3_URL)
    coord_2_ok = health_2 is not None
    coord_3_ok = health_3 is not None
    if coord_2_ok:
        ws_2 = health_2.get("ws_connections", 0)
        print(f"  Coordinator-2: status={health_2.get('status')}, WS={ws_2}")
    else:
        print(f"  WARNING: coordinator-2 unreachable")
    if coord_3_ok:
        ws_3 = health_3.get("ws_connections", 0)
        print(f"  Coordinator-3: status={health_3.get('status')}, WS={ws_3}")
    else:
        print(f"  WARNING: coordinator-3 unreachable")

    # Send events only to pipeline 3 (on coordinator-2's worker)
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
    passed = received >= matching and coord_2_ok and coord_3_ok
    detail = (f"{received}/{matching} events via coordinator-2, "
              f"coordinator-2 healthy: {coord_2_ok}, coordinator-3 healthy: {coord_3_ok}")
    return PhaseResult("Cascading Failure", passed, duration, detail)


def phase_5_coordinator_failover(publisher, collector, docker_client):
    """Kill Raft leader. Verify new leader elected and surviving coordinators unaffected."""
    start = time.time()
    collector.clear()

    # Find current Raft leader
    old_leader_id, old_leader_url = find_raft_leader()
    print(f"  Current Raft leader: node {old_leader_id} ({old_leader_url})")

    # Snapshot surviving coordinators' state before
    surviving_urls = [(i, url) for i, url in enumerate(COORDINATOR_URLS, 1)
                      if i != old_leader_id]
    for i, url in surviving_urls:
        h = coordinator_health(url)
        ws = h.get("ws_connections", 0) if h else 0
        print(f"  Coordinator-{i} before: WS={ws}, status={h.get('status') if h else 'N/A'}")

    # Kill the Raft leader
    leader_service = f"coordinator-{old_leader_id}"
    print(f"  KILLING {leader_service} (Raft leader)...")
    stop_container(docker_client, leader_service)

    # Verify leader is actually down
    time.sleep(2)
    coord_leader_down = not coordinator_is_up(old_leader_url)
    print(f"  {leader_service} is down: {coord_leader_down}")

    # Wait for new Raft leader election
    print("  Waiting for new Raft leader election...")
    election_start = time.time()
    try:
        new_leader_id, new_leader_url = wait_for_raft_leader(
            timeout=30, exclude_node=old_leader_id)
        election_time = time.time() - election_start
        print(f"  New leader: node {new_leader_id} ({new_leader_url}) "
              f"elected in {election_time:.1f}s")
    except TimeoutError:
        new_leader_id, new_leader_url = None, None
        election_time = time.time() - election_start
        print(f"  TIMEOUT: No new leader after {election_time:.1f}s")

    # Check surviving coordinators' Raft roles
    for i, url in surviving_urls:
        role = get_raft_role(url)
        print(f"  Coordinator-{i} role: {role}")

    # Verify API is available on surviving coordinators
    api_available = False
    for i, url in surviving_urls:
        h = coordinator_health(url)
        if h is not None:
            api_available = True
            print(f"  API available on coordinator-{i}: status={h.get('status')}")

    # Verify pipeline 3 on coordinator-2 still processes events
    # (worker-3 is connected to coordinator-2 which is still alive)
    print("  Sending events through surviving pipeline...")
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
    print(f"  Received {received}/{matching} events through surviving pipeline")

    duration = time.time() - start
    passed = (
        coord_leader_down
        and new_leader_id is not None
        and new_leader_id != old_leader_id
        and api_available
        and received >= matching
    )
    detail = (f"old leader: node {old_leader_id}, new leader: node {new_leader_id}, "
              f"election: {election_time:.1f}s, events: {received}/{matching}")
    return PhaseResult("Coordinator Failover (Raft)", passed, duration, detail)


def phase_6_recovery(publisher, collector, docker_client):
    """Restart killed coordinator and workers. Verify full cluster recovery."""
    start = time.time()
    collector.clear()

    # Find which coordinator was killed
    killed_coord = None
    for i, url in enumerate(COORDINATOR_URLS, 1):
        if not coordinator_is_up(url):
            killed_coord = i
            break

    # Restart the killed coordinator
    if killed_coord:
        print(f"  Restarting coordinator-{killed_coord}...")
        start_container(docker_client, f"coordinator-{killed_coord}")

        # Wait for coordinator to recover
        deadline = time.time() + 30
        while time.time() < deadline:
            if coordinator_is_up(COORDINATOR_URLS[killed_coord - 1]):
                print(f"  Coordinator-{killed_coord} is back!")
                break
            time.sleep(1)
        else:
            print(f"  WARNING: coordinator-{killed_coord} did not recover within 30s")

    # Find current Raft leader for write operations
    print("  Finding Raft leader for writes...")
    leader_id, leader_url = wait_for_raft_leader(timeout=30)
    has_leader = leader_id is not None
    print(f"  Raft leader: node {leader_id} ({leader_url})")

    # Check Raft roles after recovery
    print("  Raft roles after recovery:")
    for i, url in enumerate(COORDINATOR_URLS, 1):
        role = get_raft_role(url)
        print(f"    Coordinator-{i}: {role}")

    # Restart worker-1 and worker-2
    print("  Restarting worker-1 and worker-2...")
    start_container(docker_client, "worker-1")
    start_container(docker_client, "worker-2")

    # Wait for workers to register (all 4 should be visible on leader via Raft)
    print("  Waiting for workers to register (Raft-replicated)...")
    write_url = leader_url or COORDINATOR_1_URL
    workers = wait_for_workers(4, timeout=60, coordinator_url=write_url)
    print(f"  {len(workers)} workers ready")

    # Re-register connectors on the leader
    print(f"  Re-registering MQTT connectors on leader ({write_url})...")
    for n in range(1, NUM_PIPELINES + 1):
        try:
            api_post("/api/v1/cluster/connectors", {
                "name": f"mqtt_{n}",
                "connector_type": "mqtt",
                "params": {"host": MQTT_HOST, "port": str(MQTT_PORT)},
                "description": f"MQTT broker for pipeline {n}",
            }, coordinator_url=write_url)
        except requests.exceptions.HTTPError:
            pass  # already exists

    # Re-deploy pipelines on the leader
    print(f"  Re-deploying pipelines 1+2 on leader ({write_url})...")
    try:
        api_post("/api/v1/cluster/pipeline-groups", {
            "name": "e2e_sensor_filter_c1",
            "pipelines": [
                {"name": f"sensor_filter_{n}", "source": vpl_for_pipeline(n), "replicas": 1}
                for n in range(1, 3)
            ],
            "routes": [],
        }, coordinator_url=write_url)
    except requests.exceptions.HTTPError:
        print("  (pipeline group may already exist, continuing)")

    time.sleep(5)

    # Check all coordinators
    all_up = all(coordinator_is_up(url) for url in COORDINATOR_URLS)
    for i, url in enumerate(COORDINATOR_URLS, 1):
        h = coordinator_health(url)
        ws = h.get("ws_connections", 0) if h else 0
        print(f"  Coordinator-{i}: WS={ws}, status={h.get('status') if h else 'N/A'}")

    # Send events across all pipelines
    collector.clear()
    expected = publisher.publish_batch(start_seq=3000, count=500)
    print(f"  Published 500 events ({expected} matching)")

    collector.wait_for_count(expected, timeout=30)
    received = collector.count()

    duration = time.time() - start
    passed = received >= expected and all_up and has_leader
    detail = (f"{received}/{expected} events, all 3 coordinators up: {all_up}, "
              f"Raft leader: node {leader_id}")
    return PhaseResult("Recovery", passed, duration, detail)


def phase_7_traffic_during_coordinator_kill(publisher, collector, docker_client):
    """Continuous MQTT traffic while killing Raft leader.

    Pipelines on surviving coordinators should be completely unaffected.
    Pipelines on the killed coordinator may lose events during the outage.
    """
    start = time.time()
    collector.clear()

    leader_id, leader_url = find_raft_leader()
    print(f"  Current Raft leader: node {leader_id}")

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

    # Publish for 5s, then kill the Raft leader
    time.sleep(5)
    print(f"  Killing coordinator-{leader_id} (Raft leader) during traffic...")
    stop_container(docker_client, f"coordinator-{leader_id}")

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

    print(f"  Total matching events: {total_matching}")
    print(f"  Pipeline 3 matching: {matching_p3}")
    print(f"  Received: {received} ({pct:.1f}%)")

    # Check if new leader was elected
    new_leader_id, _ = find_raft_leader()
    has_new_leader = new_leader_id is not None and new_leader_id != leader_id
    print(f"  New leader after kill: node {new_leader_id}")

    # Surviving coordinators should still be operational
    surviving_ok = 0
    for i, url in enumerate(COORDINATOR_URLS, 1):
        if i == leader_id:
            continue
        if coordinator_is_up(url):
            surviving_ok += 1
            print(f"  Coordinator-{i} still up: True")

    duration = time.time() - start
    # Accept >= 50% delivery (pipelines on surviving coordinators continue)
    passed = pct >= 50.0 and has_new_leader and surviving_ok >= 2
    detail = (f"{received}/{total_matching} events ({pct:.1f}%), "
              f"new leader: node {new_leader_id}, "
              f"{surviving_ok} surviving coordinators OK")
    return PhaseResult("Traffic During Coordinator Kill", passed, duration, detail)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  E2E Horizontal Scaling + Raft Coordinator HA Test Suite")
    print("  Varpulis CEP — 3 Raft coordinators, 4 workers")
    print("=" * 60)
    print()
    print(f"  Coordinator-1: {COORDINATOR_1_URL}")
    print(f"  Coordinator-2: {COORDINATOR_2_URL}")
    print(f"  Coordinator-3: {COORDINATOR_3_URL}")
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
        ("Phase 5", "Coordinator Failover (Raft)",
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
    print("=== E2E Scaling + Raft Coordinator HA Test Results ===")
    for i, r in enumerate(results):
        status = "PASS" if r.passed else "FAIL"
        dots = "." * max(1, 35 - len(r.name))
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
