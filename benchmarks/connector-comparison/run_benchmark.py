#!/usr/bin/env python3
"""
Connector-Based Benchmark: Varpulis vs Apama (MQTT + Kafka)

Measures end-to-end throughput through real connectors:
  Producer -> Broker (MQTT/Kafka) -> Engine (Varpulis/Apama) -> Broker -> Consumer

Methodology:
  - Events published as JSON to broker topics
  - Engine reads from input topic(s), processes, writes to output topic
  - Consumer counts output events and measures elapsed time
  - Throughput = N_input / (end_time - start_time)
  - Multiple runs, median throughput reported

Dependencies:
  pip install paho-mqtt>=2.0 confluent-kafka>=2.3
"""

import subprocess
import signal
import time
import json
import math
import os
import argparse
import statistics
import threading
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Dict

try:
    import paho.mqtt.client as mqtt_client
    HAS_MQTT = True
except ImportError:
    HAS_MQTT = False

try:
    from confluent_kafka import Producer as KafkaProducer, Consumer as KafkaConsumer
    from confluent_kafka import KafkaError
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

BENCH_DIR = Path(__file__).parent
PROJECT_ROOT = BENCH_DIR.parent.parent

MQTT_HOST = "localhost"
MQTT_PORT = 1884
KAFKA_BOOTSTRAP = "localhost:9092"

APAMA_PORT = 15903
APAMA_CONTAINER = "bench-apama"

# 06_multi_sensor: Apama stream query produces 0 outputs (join/having conditions don't trigger)
UNSUPPORTED_APAMA = {"06_multi_sensor"}

# Apama community image doesn't include Kafka connectivity plugin
APAMA_UNSUPPORTED_CONNECTORS = {"kafka"}

# Scenario 04 uses SASE+ pattern which doesn't route to sinks in `run` mode yet
UNSUPPORTED_VARPULIS = set()  # all scenarios supported

SCENARIOS = [
    "01_filter",
    "02_aggregation",
    "03_temporal",
    "04_kleene",
    "05_ema_crossover",
    "06_multi_sensor",
    "07_sequence",
]


# Apama event field order per event type (must match .mon definitions)
APAMA_EVENT_FIELDS = {
    "StockTick": ["symbol", "price", "volume"],
    "Trade": ["symbol", "price", "volume"],
    "Login": ["user_id", "ip", "device"],
    "Transaction": ["user_id", "amount", "ip", "merchant"],
    "TemperatureReading": ["sensor_id", "location", "value"],
    "PressureReading": ["sensor_id", "location", "value"],
    "A": ["id"],
    "B": ["id"],
}


def to_apama_event_string(evt: dict) -> str:
    """Convert a JSON-style event dict to Apama event string format.

    Example: {"event_type": "StockTick", "symbol": "AAPL", "price": 150.5, "volume": 1000}
    becomes: StockTick("AAPL",150.5,1000)
    """
    et = evt["event_type"]
    fields = APAMA_EVENT_FIELDS.get(et)
    if not fields:
        # Fallback: use all fields except event_type, as-is
        fields = [k for k in evt if k != "event_type"]

    parts = []
    for f in fields:
        v = evt.get(f, "")
        if isinstance(v, str):
            parts.append(f'"{v}"')
        elif isinstance(v, float):
            parts.append(str(v))
        elif isinstance(v, int):
            parts.append(str(v))
        else:
            parts.append(str(v))

    return f'{et}({",".join(parts)})'


# ---------------------------------------------------------------------------
# Memory tracking
# ---------------------------------------------------------------------------

class MemoryTracker:
    """Track peak RSS memory of a process or Docker container."""

    def __init__(self, mode: str, target, interval: float = 0.2):
        """
        mode: "process" (target=PID, reads /proc) or "container" (target=name, docker stats)
        """
        self.mode = mode
        self.target = target
        self.interval = interval if mode == "process" else 1.0
        self.samples: List[float] = []  # RSS in MB
        self.running = False
        self.thread = None

    def _read_process_rss(self) -> Optional[float]:
        """Read VmRSS from /proc/PID/status. Returns MB."""
        try:
            with open(f"/proc/{self.target}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        kb = int(line.split()[1])
                        return kb / 1024.0
        except (FileNotFoundError, ProcessLookupError, ValueError, IndexError):
            pass
        return None

    def _read_container_rss(self) -> Optional[float]:
        """Read memory usage via docker stats. Returns MB."""
        try:
            result = subprocess.run(
                ["docker", "stats", "--no-stream", "--format",
                 "{{.MemUsage}}", self.target],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                # "125.3MiB / 7.765GiB" or "1.234GiB / 7.765GiB"
                usage = result.stdout.strip().split("/")[0].strip()
                if "GiB" in usage:
                    return float(usage.replace("GiB", "").strip()) * 1024
                elif "MiB" in usage:
                    return float(usage.replace("MiB", "").strip())
                elif "KiB" in usage:
                    return float(usage.replace("KiB", "").strip()) / 1024
        except Exception:
            pass
        return None

    def _sample_loop(self):
        while self.running:
            if self.mode == "process":
                rss = self._read_process_rss()
            else:
                rss = self._read_container_rss()
            if rss is not None:
                self.samples.append(rss)
            time.sleep(self.interval)

    def start(self):
        self.running = True
        self.samples = []
        self.thread = threading.Thread(target=self._sample_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=3)

    def get_peak_mb(self) -> float:
        return max(self.samples) if self.samples else 0.0

    def get_avg_mb(self) -> float:
        return statistics.mean(self.samples) if self.samples else 0.0


@dataclass
class BenchmarkResult:
    scenario: str
    engine: str
    connector: str
    events: int
    elapsed_ms: float
    throughput: float
    output_count: int
    error: Optional[str] = None
    runs: List[float] = field(default_factory=list)
    peak_rss_mb: float = 0.0


# ---------------------------------------------------------------------------
# Scenario configuration: topics and event types
# ---------------------------------------------------------------------------

def get_scenario_config(scenario: str, connector: str) -> dict:
    """Return topic names and event routing for a scenario."""
    sep = "/" if connector == "mqtt" else "-"

    if scenario == "01_filter":
        return {
            "input_topics": {
                "StockTick": f"bench{sep}01{sep}input",
            },
            "output_topic": f"bench{sep}01{sep}output",
        }
    elif scenario == "02_aggregation":
        return {
            "input_topics": {
                "Trade": f"bench{sep}02{sep}input",
            },
            "output_topic": f"bench{sep}02{sep}output",
        }
    elif scenario == "03_temporal":
        return {
            "input_topics": {
                "Login": f"bench{sep}03{sep}Login",
                "Transaction": f"bench{sep}03{sep}Transaction",
            },
            "output_topic": f"bench{sep}03{sep}output",
        }
    elif scenario == "04_kleene":
        return {
            "input_topics": {
                "StockTick": f"bench{sep}04{sep}input",
            },
            "output_topic": f"bench{sep}04{sep}output",
        }
    elif scenario == "05_ema_crossover":
        return {
            "input_topics": {
                "StockTick": f"bench{sep}05{sep}input",
            },
            "output_topic": f"bench{sep}05{sep}output",
        }
    elif scenario == "06_multi_sensor":
        return {
            "input_topics": {
                "TemperatureReading": f"bench{sep}06{sep}TemperatureReading",
                "PressureReading": f"bench{sep}06{sep}PressureReading",
            },
            "output_topic": f"bench{sep}06{sep}output",
        }
    elif scenario == "07_sequence":
        return {
            "input_topics": {
                "A": f"bench{sep}07{sep}A",
                "B": f"bench{sep}07{sep}B",
            },
            "output_topic": f"bench{sep}07{sep}output",
        }
    else:
        raise ValueError(f"Unknown scenario: {scenario}")


# ---------------------------------------------------------------------------
# Event generation
# ---------------------------------------------------------------------------

def generate_events(scenario: str, count: int) -> List[Dict]:
    """Generate events as JSON-serializable dicts with event_type field."""
    events = []

    if scenario == "01_filter":
        for i in range(count):
            events.append({
                "event_type": "StockTick",
                "symbol": ["AAPL", "GOOG", "MSFT"][i % 3],
                "price": 40.0 + (i % 100),  # 40-139, ~90% pass price>50
                "volume": 1000 + (i % 5000),
            })

    elif scenario == "02_aggregation":
        for i in range(count):
            events.append({
                "event_type": "Trade",
                "symbol": "ACME",
                "price": 100.0 + (i % 50) * 0.1,
                "volume": 100.0 + (i % 900),
            })

    elif scenario == "03_temporal":
        # Generate Login/Transaction pairs that will trigger fraud alerts
        # Every 3 events: Login, then 2 Transactions (from different IP, amount > 5000)
        for i in range(count):
            if i % 3 == 0:
                events.append({
                    "event_type": "Login",
                    "user_id": f"user_{i % 100}",
                    "ip": f"192.168.1.{i % 255}",
                    "device": "mobile",
                })
            else:
                events.append({
                    "event_type": "Transaction",
                    "user_id": f"user_{((i // 3) * 3) % 100}",
                    "amount": 6000.0 + (i % 4000),  # always > 5000
                    "ip": f"10.0.0.{i % 255}",  # always different subnet
                    "merchant": "Store",
                })

    elif scenario == "04_kleene":
        # Generate clear rising sequences: 4 rises then a break
        for i in range(count):
            symbol = ["AAPL", "GOOG"][i % 2]
            seq_pos = (i // 2) % 5  # 0-4 within each symbol's sequence
            base = 100.0 + ((i // 10) % 100) * 10
            if seq_pos < 4:
                price = base + seq_pos * 2.0  # rising
            else:
                price = base - 5.0  # break
            events.append({
                "event_type": "StockTick",
                "symbol": symbol,
                "price": price,
                "volume": 1000,
            })

    elif scenario == "05_ema_crossover":
        # Use wider sine to force EMA crossovers
        for i in range(count):
            trend = math.sin(i / 50) * 20  # wider amplitude
            events.append({
                "event_type": "StockTick",
                "symbol": "AAPL",
                "price": 100.0 + trend + (i % 5) * 0.1,
                "volume": 1000,
            })

    elif scenario == "06_multi_sensor":
        # Inject correlated high-variance bursts per zone
        for i in range(count):
            zone = f"zone_{i % 5}"
            if i % 2 == 0:
                # Temperature: inject spikes every 20 events for variance
                spike = 25.0 if (i % 20 == 0) else 0.0
                events.append({
                    "event_type": "TemperatureReading",
                    "sensor_id": f"temp_{i % 10}",
                    "location": zone,
                    "value": 20.0 + (i % 10) + spike,
                })
            else:
                # Pressure: inject spikes every 20 events for variance
                spike = 50.0 if (i % 20 == 1) else 0.0
                events.append({
                    "event_type": "PressureReading",
                    "sensor_id": f"press_{i % 10}",
                    "location": zone,
                    "value": 1000.0 + (i % 50) + spike,
                })

    elif scenario == "07_sequence":
        # A/B pairs with matching ids
        for i in range(count // 2):
            events.append({
                "event_type": "A",
                "id": i,
            })
            events.append({
                "event_type": "B",
                "id": i,
            })

    return events


# ---------------------------------------------------------------------------
# MQTT publisher / subscriber
# ---------------------------------------------------------------------------

class MQTTPublisher:
    def __init__(self, host: str = MQTT_HOST, port: int = MQTT_PORT):
        self.client = mqtt_client.Client(
            mqtt_client.CallbackAPIVersion.VERSION2,
            client_id="bench-producer",
        )
        self.client.connect(host, port, keepalive=60)
        self.client.loop_start()

    def publish_events(self, events: List[Dict], config: dict, engine: str = "varpulis"):
        """Publish events to MQTT topics.

        Uses QoS 0 for speed. Broker must be configured with
        max_queued_messages 0 to prevent message loss.
        """
        topics = config["input_topics"]
        for evt in events:
            et = evt["event_type"]
            topic = topics[et]
            if engine == "apama":
                payload = to_apama_event_string(evt)
            else:
                payload = json.dumps(evt)
            self.client.publish(topic, payload, qos=0)

    def close(self):
        self.client.loop_stop()
        self.client.disconnect()


class MQTTConsumer:
    def __init__(self, topic: str, host: str = MQTT_HOST, port: int = MQTT_PORT):
        self.topic = topic
        self.count = 0
        self.lock = threading.Lock()
        self.last_received = time.perf_counter()
        self.running = False

        self.client = mqtt_client.Client(
            mqtt_client.CallbackAPIVersion.VERSION2,
            client_id=f"bench-consumer-{topic.replace('/', '-')}",
        )
        self.client.on_message = self._on_message
        self.client.connect(host, port, keepalive=60)
        self.client.subscribe(topic, qos=0)

    def _on_message(self, client, userdata, msg):
        with self.lock:
            self.count += 1
            self.last_received = time.perf_counter()

    def start(self):
        self.running = True
        self.client.loop_start()

    def reset(self):
        with self.lock:
            self.count = 0
            self.last_received = time.perf_counter()

    def get_count(self) -> int:
        with self.lock:
            return self.count

    def seconds_since_last(self) -> float:
        with self.lock:
            return time.perf_counter() - self.last_received

    def close(self):
        self.running = False
        self.client.loop_stop()
        self.client.disconnect()


# ---------------------------------------------------------------------------
# Kafka publisher / subscriber
# ---------------------------------------------------------------------------

class KafkaPublisherWrapper:
    def __init__(self, bootstrap: str = KAFKA_BOOTSTRAP):
        self.producer = KafkaProducer({
            "bootstrap.servers": bootstrap,
            "queue.buffering.max.messages": 500000,
            "queue.buffering.max.ms": 5,
            "batch.num.messages": 10000,
        })

    def publish_events(self, events: List[Dict], config: dict, engine: str = "varpulis"):
        """Publish events to appropriate Kafka topics."""
        topics = config["input_topics"]
        for i, evt in enumerate(events):
            et = evt["event_type"]
            topic = topics[et]
            if engine == "apama":
                payload = to_apama_event_string(evt).encode("utf-8")
            else:
                payload = json.dumps(evt).encode("utf-8")
            self.producer.produce(topic, payload)
            if i % 10000 == 0:
                self.producer.flush()
        self.producer.flush()

    def close(self):
        self.producer.flush(timeout=10)


class KafkaConsumerWrapper:
    def __init__(self, topic: str, bootstrap: str = KAFKA_BOOTSTRAP):
        self.topic = topic
        self.count = 0
        self.lock = threading.Lock()
        self.last_received = time.perf_counter()
        self.running = False
        self.thread = None

        self.consumer = KafkaConsumer({
            "bootstrap.servers": bootstrap,
            "group.id": f"bench-consumer-{topic}-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        self.consumer.subscribe([topic])

    def _poll_loop(self):
        while self.running:
            msg = self.consumer.poll(timeout=0.1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                continue
            with self.lock:
                self.count += 1
                self.last_received = time.perf_counter()

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.thread.start()

    def reset(self):
        with self.lock:
            self.count = 0
            self.last_received = time.perf_counter()

    def get_count(self) -> int:
        with self.lock:
            return self.count

    def seconds_since_last(self) -> float:
        with self.lock:
            return time.perf_counter() - self.last_received

    def close(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        self.consumer.close()


# ---------------------------------------------------------------------------
# Engine management
# ---------------------------------------------------------------------------

def start_varpulis(scenario: str, connector: str) -> Optional[subprocess.Popen]:
    """Start Varpulis in `run` mode with the appropriate VPL file."""
    vpl_file = BENCH_DIR / "varpulis" / connector / f"{scenario}.vpl"
    if not vpl_file.exists():
        return None

    varpulis_bin = PROJECT_ROOT / "target" / "release" / "varpulis"
    if not varpulis_bin.exists():
        return None

    proc = subprocess.Popen(
        [str(varpulis_bin), "run", "--file", str(vpl_file)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        cwd=PROJECT_ROOT,
    )
    return proc


def _drain_stderr(proc: subprocess.Popen):
    """Drain stderr in background to prevent pipe buffer deadlock."""
    try:
        while True:
            line = proc.stderr.readline()
            if not line:
                break
    except Exception:
        pass


def wait_varpulis_ready(proc: subprocess.Popen, timeout: float = 15.0) -> bool:
    """Wait for Varpulis to indicate it's ready (listening on connectors)."""
    import select
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if proc.poll() is not None:
            return False
        # Check stderr for ready messages (tracing output goes to stderr)
        if select.select([proc.stderr], [], [], 0.5)[0]:
            line = proc.stderr.readline().decode("utf-8", errors="replace")
            if "Listening" in line or "Connected" in line or "subscribed" in line:
                # Start draining stderr to prevent pipe deadlock
                threading.Thread(
                    target=_drain_stderr, args=(proc,), daemon=True
                ).start()
                return True
            if "running" in line.lower():
                threading.Thread(
                    target=_drain_stderr, args=(proc,), daemon=True
                ).start()
                return True
        time.sleep(0.1)
    # If we timed out but process is still alive, assume it's ready
    if proc.poll() is None:
        threading.Thread(
            target=_drain_stderr, args=(proc,), daemon=True
        ).start()
        return True
    return False


def stop_varpulis(proc: subprocess.Popen):
    """Gracefully stop Varpulis."""
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def apama_available() -> bool:
    """Check if Apama container is running."""
    result = subprocess.run(
        ["docker", "ps", "-q", "-f", f"name={APAMA_CONTAINER}"],
        capture_output=True, text=True,
    )
    return bool(result.stdout.strip())


_apama_connectivity_initialized = False

def apama_init_connectivity() -> Optional[str]:
    """Initialize Apama connectivity plugins (call once before any scenarios).

    Injects ConnectivityPluginsControl, ConnectivityPlugins, and
    AutomaticOnApplicationInitialized monitors, then sends the
    ApplicationInitialized event to activate connectivity transports.
    Idempotent — skips if already initialized.
    """
    global _apama_connectivity_initialized
    if _apama_connectivity_initialized:
        return None

    apama_home = "/opt/cumulocity/Apama/monitors"
    init_monitors = [
        f"{apama_home}/ConnectivityPluginsControl.mon",
        f"{apama_home}/ConnectivityPlugins.mon",
        f"{apama_home}/AutomaticOnApplicationInitialized.mon",
    ]
    result = subprocess.run(
        ["docker", "exec", APAMA_CONTAINER,
         "engine_inject", "-p", str(APAMA_PORT)] + init_monitors,
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        # Check if it's just "already exists" — that's OK
        if "already exists" not in result.stderr and "already" not in result.stderr.lower():
            return f"Connectivity init failed: {result.stderr.strip()[:200]}"

    # Send the ApplicationInitialized event to start transports
    result = subprocess.run(
        ["docker", "exec", APAMA_CONTAINER,
         "engine_send", "-p", str(APAMA_PORT),
         f"{apama_home}/AutomaticOnApplicationInitialized.evt"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return f"Connectivity init event failed: {result.stderr.strip()[:200]}"

    _apama_connectivity_initialized = True
    time.sleep(0.5)
    return None


def apama_inject_monitor(scenario: str, connector: str) -> Optional[str]:
    """Inject a scenario monitor into the Apama correlator. Returns error or None."""
    mon_file = BENCH_DIR / "apama" / "monitors" / f"{scenario}_{connector}.mon"
    if not mon_file.exists():
        return f"Monitor file not found: {mon_file}"

    # Delete the monitor first in case it's already loaded from a previous run
    apama_delete_monitor(scenario, connector)

    # Copy into container
    container_path = f"/tmp/{scenario}_{connector}.mon"
    subprocess.run(
        ["docker", "cp", str(mon_file), f"{APAMA_CONTAINER}:{container_path}"],
        capture_output=True,
    )

    result = subprocess.run(
        ["docker", "exec", APAMA_CONTAINER,
         "engine_inject", "-p", str(APAMA_PORT), container_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return f"engine_inject failed: {result.stderr.strip()[:200]}"
    return None


def apama_delete_monitor(scenario: str, connector: str):
    """Delete an injected monitor from the Apama correlator."""
    # Monitor names follow the pattern: ScenarioNameConnector
    # e.g., FilterBenchmarkMQTT, FraudDetectionKafka
    monitor_names = {
        "01_filter": "FilterBenchmark",
        "02_aggregation": "VWAPBenchmark",
        "03_temporal": "FraudDetection",
        "04_kleene": "RisingSequenceDetector",
        "05_ema_crossover": "EMACrossover",
        "06_multi_sensor": "MultiSensorCorrelation",
        "07_sequence": "SequenceDetection",
    }
    base = monitor_names.get(scenario, "")
    suffix = connector.upper()  # MQTT or KAFKA
    name = f"{base}{suffix}"

    subprocess.run(
        ["docker", "exec", APAMA_CONTAINER,
         "engine_delete", "-p", str(APAMA_PORT), name],
        capture_output=True, text=True,
    )


# ---------------------------------------------------------------------------
# Single benchmark run
# ---------------------------------------------------------------------------

def run_single(
    scenario: str,
    engine: str,
    connector: str,
    events: List[Dict],
    warmup_count: int = 1000,
) -> BenchmarkResult:
    """Run a single benchmark iteration."""
    config = get_scenario_config(scenario, connector)
    n_events = len(events)

    # Check for unsupported scenarios
    if engine == "apama" and scenario in UNSUPPORTED_APAMA:
        return BenchmarkResult(
            scenario, engine, connector, n_events, 0, 0, 0,
            error="Unsupported EPL stream query syntax",
        )
    if engine == "apama" and connector in APAMA_UNSUPPORTED_CONNECTORS:
        return BenchmarkResult(
            scenario, engine, connector, n_events, 0, 0, 0,
            error="Apama community image lacks Kafka connectivity plugin",
        )
    if engine == "varpulis" and scenario in UNSUPPORTED_VARPULIS:
        return BenchmarkResult(
            scenario, engine, connector, n_events, 0, 0, 0,
            error="SASE+ patterns not yet supported in connector mode",
        )

    # Create publisher and consumer
    publisher = None
    consumer = None
    varpulis_proc = None

    try:
        # Set up consumer on output topic
        if connector == "mqtt":
            if not HAS_MQTT:
                return BenchmarkResult(
                    scenario, engine, connector, n_events, 0, 0, 0,
                    error="paho-mqtt not installed",
                )
            publisher = MQTTPublisher()
            consumer = MQTTConsumer(config["output_topic"])
        elif connector == "kafka":
            if not HAS_KAFKA:
                return BenchmarkResult(
                    scenario, engine, connector, n_events, 0, 0, 0,
                    error="confluent-kafka not installed",
                )
            publisher = KafkaPublisherWrapper()
            consumer = KafkaConsumerWrapper(config["output_topic"])
        else:
            return BenchmarkResult(
                scenario, engine, connector, n_events, 0, 0, 0,
                error=f"Unknown connector: {connector}",
            )

        consumer.start()

        # Start engine
        if engine == "varpulis":
            varpulis_proc = start_varpulis(scenario, connector)
            if varpulis_proc is None:
                return BenchmarkResult(
                    scenario, engine, connector, n_events, 0, 0, 0,
                    error="Failed to start Varpulis (binary or VPL not found)",
                )
            if not wait_varpulis_ready(varpulis_proc):
                stop_varpulis(varpulis_proc)
                stderr = ""
                try:
                    stderr = varpulis_proc.stderr.read().decode("utf-8", errors="replace")[:300]
                except Exception:
                    pass
                return BenchmarkResult(
                    scenario, engine, connector, n_events, 0, 0, 0,
                    error=f"Varpulis failed to start: {stderr}",
                )
        elif engine == "apama":
            if not apama_available():
                return BenchmarkResult(
                    scenario, engine, connector, n_events, 0, 0, 0,
                    error="Apama container not running",
                )
            err = apama_inject_monitor(scenario, connector)
            if err:
                return BenchmarkResult(
                    scenario, engine, connector, n_events, 0, 0, 0,
                    error=err,
                )
            time.sleep(1)  # Let monitor settle

        # Start memory tracker
        mem_tracker = None
        if engine == "varpulis" and varpulis_proc:
            mem_tracker = MemoryTracker("process", varpulis_proc.pid)
        elif engine == "apama":
            mem_tracker = MemoryTracker("container", APAMA_CONTAINER)
        if mem_tracker:
            mem_tracker.start()

        # Warmup phase
        if warmup_count > 0:
            warmup_events = events[:warmup_count]
            publisher.publish_events(warmup_events, config, engine)
            time.sleep(0.5)
            consumer.reset()

        # Timed phase
        start_time = time.perf_counter()
        publisher.publish_events(events, config, engine)
        publish_done = time.perf_counter()

        # Wait for outputs to drain: idle timeout after publishing completes
        idle_timeout = 2.0
        hard_timeout = 60.0
        hard_deadline = publish_done + hard_timeout

        while True:
            now = time.perf_counter()
            if now >= hard_deadline:
                break
            if consumer.get_count() > 0 and consumer.seconds_since_last() > idle_timeout:
                break
            time.sleep(0.05)

        end_time = time.perf_counter()
        elapsed = end_time - start_time
        output_count = consumer.get_count()
        throughput = n_events / elapsed if elapsed > 0 else 0

        # Collect memory stats
        peak_rss = 0.0
        if mem_tracker:
            mem_tracker.stop()
            peak_rss = mem_tracker.get_peak_mb()

        return BenchmarkResult(
            scenario, engine, connector, n_events,
            elapsed * 1000, throughput, output_count,
            peak_rss_mb=peak_rss,
        )

    except Exception as e:
        if mem_tracker:
            mem_tracker.stop()
        return BenchmarkResult(
            scenario, engine, connector, n_events, 0, 0, 0,
            error=str(e),
        )

    finally:
        # Cleanup
        if varpulis_proc:
            stop_varpulis(varpulis_proc)
        if engine == "apama":
            apama_delete_monitor(scenario, connector)
        if consumer:
            consumer.close()
        if publisher:
            publisher.close()


# ---------------------------------------------------------------------------
# Multi-run benchmark
# ---------------------------------------------------------------------------

def run_benchmark(
    scenario: str,
    engine: str,
    connector: str,
    events: List[Dict],
    runs: int,
) -> BenchmarkResult:
    """Run a benchmark multiple times and report median."""
    throughputs = []
    peak_rss_values = []
    last_output_count = 0
    last_error = None

    for run_idx in range(runs):
        result = run_single(scenario, engine, connector, events)
        if result.error:
            last_error = result.error
            continue
        throughputs.append(result.throughput)
        if result.peak_rss_mb > 0:
            peak_rss_values.append(result.peak_rss_mb)
        last_output_count = result.output_count

        # Brief pause between runs
        if run_idx < runs - 1:
            time.sleep(1)

    if not throughputs:
        return BenchmarkResult(
            scenario, engine, connector, len(events), 0, 0, 0,
            error=last_error or "No successful runs",
        )

    median_throughput = statistics.median(throughputs)
    median_elapsed = (len(events) / median_throughput * 1000) if median_throughput > 0 else 0
    median_peak_rss = statistics.median(peak_rss_values) if peak_rss_values else 0.0

    return BenchmarkResult(
        scenario, engine, connector, len(events),
        median_elapsed, median_throughput, last_output_count,
        runs=throughputs, peak_rss_mb=median_peak_rss,
    )


# ---------------------------------------------------------------------------
# Results display
# ---------------------------------------------------------------------------

def format_throughput(t: float) -> str:
    if t >= 1_000_000:
        return f"{t/1_000_000:.1f}M/s"
    elif t >= 1_000:
        return f"{t/1_000:.1f}K/s"
    else:
        return f"{t:.0f}/s"


def format_memory(mb: float) -> str:
    if mb <= 0:
        return "—"
    elif mb >= 1024:
        return f"{mb/1024:.1f} GB"
    elif mb >= 1:
        return f"{mb:.1f} MB"
    else:
        return f"{mb*1024:.0f} KB"


def print_results(results: List[BenchmarkResult], num_runs: int):
    """Print benchmark results with comparison tables."""
    # Group by (scenario, connector)
    groups = {}
    for r in results:
        key = (r.scenario, r.connector)
        groups.setdefault(key, {})[r.engine] = r

    print("\n" + "=" * 120)
    print(f"CONNECTOR BENCHMARK RESULTS  (median of {num_runs} runs)")
    print("=" * 120)

    header = (
        f"{'Scenario':<20} {'Connector':<8} {'Engine':<10} "
        f"{'Events':>8} {'Time (ms)':>10} {'Throughput':>14} "
        f"{'Outputs':>8} {'Peak RSS':>10}"
    )
    print(f"\n{header}")
    print("-" * 120)

    for key in sorted(groups.keys()):
        scenario, connector = key
        group = groups[key]
        for engine in ["varpulis", "apama"]:
            if engine not in group:
                continue
            r = group[engine]
            if r.error:
                print(f"{r.scenario:<20} {r.connector:<8} {r.engine:<10} ERROR: {r.error}")
            else:
                print(
                    f"{r.scenario:<20} {r.connector:<8} {r.engine:<10} "
                    f"{r.events:>8,} {r.elapsed_ms:>10.1f} "
                    f"{format_throughput(r.throughput):>14} {r.output_count:>8} "
                    f"{format_memory(r.peak_rss_mb):>10}"
                )

        # Comparison
        if "varpulis" in group and "apama" in group:
            v = group["varpulis"]
            a = group["apama"]
            if v.throughput > 0 and a.throughput > 0 and not v.error and not a.error:
                ratio = v.throughput / a.throughput
                winner = "Varpulis" if ratio > 1 else "Apama"
                factor = max(ratio, 1 / ratio)
                mem_note = ""
                if v.peak_rss_mb > 0 and a.peak_rss_mb > 0:
                    mem_ratio = a.peak_rss_mb / v.peak_rss_mb
                    mem_winner = "Varpulis" if mem_ratio > 1 else "Apama"
                    mem_note = f"  |  RAM: {mem_winner} {max(mem_ratio, 1/mem_ratio):.1f}x lighter"
                print(f"{'':>28} {'->':>10} {winner} {factor:.1f}x faster{mem_note}")
        print()

    # Summary comparison tables (one per connector)
    for conn in ["mqtt", "kafka"]:
        conn_groups = {k: v for k, v in groups.items() if k[1] == conn}
        has_both = any(
            "varpulis" in g and "apama" in g
            for g in conn_groups.values()
        )
        if not has_both:
            continue

        print("=" * 100)
        print(f"COMPARISON SUMMARY — {conn.upper()}")
        print("=" * 100)
        print(
            f"\n{'Scenario':<20} {'Apama':>14} {'Varpulis':>14} "
            f"{'Winner':>16} {'Apama RSS':>12} {'V. RSS':>10} {'RSS Winner':>14}"
        )
        print("-" * 100)

        for key in sorted(conn_groups.keys()):
            scenario, _ = key
            group = conn_groups[key]
            if "varpulis" not in group or "apama" not in group:
                continue
            v = group["varpulis"]
            a = group["apama"]
            if v.error or a.error:
                err_engine = "varpulis" if v.error else "apama"
                print(f"{scenario:<20} {'error':>14} {'error':>14} {err_engine + ' error':>16}")
                continue
            ratio = v.throughput / a.throughput if a.throughput > 0 else 0
            if ratio > 1:
                winner = f"V {ratio:.1f}x"
            elif ratio > 0:
                winner = f"A {1/ratio:.1f}x"
            else:
                winner = "N/A"
            # Memory comparison
            if v.peak_rss_mb > 0 and a.peak_rss_mb > 0:
                mem_ratio = a.peak_rss_mb / v.peak_rss_mb
                if mem_ratio > 1:
                    rss_winner = f"V {mem_ratio:.1f}x"
                else:
                    rss_winner = f"A {1/mem_ratio:.1f}x"
            else:
                rss_winner = "—"
            print(
                f"{scenario:<20} "
                f"{format_throughput(a.throughput):>14} "
                f"{format_throughput(v.throughput):>14} "
                f"{winner:>16} "
                f"{format_memory(a.peak_rss_mb):>12} "
                f"{format_memory(v.peak_rss_mb):>10} "
                f"{rss_winner:>14}"
            )
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Connector-Based Benchmark: Varpulis vs Apama (MQTT + Kafka)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 run_benchmark.py --events 100000 --runs 5 --connector both --engine both
  python3 run_benchmark.py --events 1000 --runs 1 --connector mqtt             # smoke test
  python3 run_benchmark.py -s 01_filter --connector kafka --engine varpulis
  python3 run_benchmark.py -n 100000 -c mqtt -e both -r 3
        """,
    )
    parser.add_argument(
        "--scenario", "-s",
        help="Run specific scenario (e.g., 01_filter). Default: all",
    )
    parser.add_argument(
        "--events", "-n", type=int, default=100000,
        help="Number of events (default: 100000)",
    )
    parser.add_argument(
        "--engine", "-e",
        choices=["varpulis", "apama", "both"], default="both",
        help="Which engine to benchmark (default: both)",
    )
    parser.add_argument(
        "--connector", "-c",
        choices=["mqtt", "kafka", "both"], default="both",
        help="Which connector to benchmark (default: both)",
    )
    parser.add_argument(
        "--runs", "-r", type=int, default=3,
        help="Number of runs per combination (default: 3)",
    )
    args = parser.parse_args()

    # Resolve scenarios and connectors
    scenarios = [args.scenario] if args.scenario else SCENARIOS
    connectors = ["mqtt", "kafka"] if args.connector == "both" else [args.connector]
    engines = ["varpulis", "apama"] if args.engine == "both" else [args.engine]

    # Check dependencies
    if "mqtt" in connectors and not HAS_MQTT:
        print("ERROR: paho-mqtt not installed. Run: pip install paho-mqtt>=2.0")
        return
    if "kafka" in connectors and not HAS_KAFKA:
        print("ERROR: confluent-kafka not installed. Run: pip install confluent-kafka>=2.3")
        return

    print("=" * 110)
    print("Connector-Based Benchmark: Varpulis vs Apama")
    print("=" * 110)
    print(f"Scenarios:   {len(scenarios)}")
    print(f"Connectors:  {', '.join(connectors)}")
    print(f"Engines:     {', '.join(engines)}")
    print(f"Events:      {args.events:,}")
    print(f"Runs:        {args.runs} (median reported)")
    print()
    print("Architecture:")
    print("  [Producer] -> [Broker] -> [Engine] -> [Broker] -> [Consumer]")
    print()

    # Check prerequisites
    varpulis_bin = PROJECT_ROOT / "target" / "release" / "varpulis"
    if "varpulis" in engines and not varpulis_bin.exists():
        print("Building Varpulis release binary...")
        subprocess.run(
            ["cargo", "build", "--release", "-p", "varpulis-cli", "--features", "kafka"],
            cwd=PROJECT_ROOT,
        )

    if "apama" in engines:
        if apama_available():
            print(f"Apama container '{APAMA_CONTAINER}' is running on port {APAMA_PORT}")
            err = apama_init_connectivity()
            if err:
                print(f"WARN: {err}")
            else:
                print("Apama connectivity initialized (onApplicationInitialized sent)")
        else:
            print(f"WARN: Apama container '{APAMA_CONTAINER}' not running.")
            if args.engine == "apama":
                print("Start it with: cd benchmarks/connector-comparison && docker compose up -d")
                return

    results = []

    for scenario in scenarios:
        for connector in connectors:
            print(f"\n>>> {scenario} / {connector}")
            events = generate_events(scenario, args.events)
            print(f"    Generated {len(events):,} events")

            for engine in engines:
                label = f"{engine}/{connector}"
                print(f"    Running {label} ({args.runs} runs)...", end="", flush=True)

                result = run_benchmark(scenario, engine, connector, events, args.runs)
                results.append(result)

                if result.error:
                    print(f" ERROR: {result.error}")
                else:
                    mem_str = f", RSS: {format_memory(result.peak_rss_mb)}" if result.peak_rss_mb > 0 else ""
                    print(
                        f" {format_throughput(result.throughput)} "
                        f"(outputs: {result.output_count}{mem_str})"
                    )

    print_results(results, args.runs)

    # Save results
    results_file = BENCH_DIR / "results" / f"connector_benchmark_{int(time.time())}.json"
    results_file.parent.mkdir(exist_ok=True)
    with open(results_file, "w") as f:
        json.dump([{
            "scenario": r.scenario,
            "engine": r.engine,
            "connector": r.connector,
            "events": r.events,
            "elapsed_ms": r.elapsed_ms,
            "throughput": r.throughput,
            "output_count": r.output_count,
            "peak_rss_mb": round(r.peak_rss_mb, 1),
            "error": r.error,
            "all_runs_throughput": r.runs,
        } for r in results], f, indent=2)
    print(f"\nResults saved to: {results_file}")


if __name__ == "__main__":
    main()
