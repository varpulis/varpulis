#!/usr/bin/env python3
"""
Multi-Scenario Benchmark: Varpulis vs Flink CEP
Runs all 4 comparable scenarios and collects metrics.

Usage:
    python run_multi_scenario_benchmark.py [processing|event] [scenario_num]

    processing: Use processing time (wall-clock) - fair comparison, low latency
    event: Use event time with watermarks - proper out-of-order handling
    scenario_num: 1, 2, 3, 4 or 'all' (default: all)

Examples:
    python run_multi_scenario_benchmark.py              # All scenarios, processing time
    python run_multi_scenario_benchmark.py event        # All scenarios, event time
    python run_multi_scenario_benchmark.py processing 2 # Only scenario 2
"""

import json
import time
import subprocess
import sys
import os
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
import paho.mqtt.client as mqtt

# Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
INPUT_TOPIC_PREFIX = "benchmark/input/"
VARPULIS_OUTPUT = "benchmark/output/varpulis"
FLINK_OUTPUT = "benchmark/output/flink"

# Arguments
TIME_MODE = sys.argv[1] if len(sys.argv) > 1 else "processing"
SPECIFIC_SCENARIO = sys.argv[2] if len(sys.argv) > 2 else "all"

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCENARIOS_DIR = os.path.join(BASE_DIR, "..")
VARPULIS_BIN = "/home/cpo/cep/target/release/varpulis"
FLINK_JAR = os.path.join(BASE_DIR, "target/flink-varpulis-comparison-1.0-SNAPSHOT.jar")
RESULTS_DIR = os.path.join(SCENARIOS_DIR, "results")


@dataclass
class ScenarioConfig:
    number: int
    name: str
    description: str
    vpl_file: str
    flink_class: str
    wait_time: int  # seconds
    expected_alerts: List[str]


SCENARIOS = {
    1: ScenarioConfig(
        number=1,
        name="Aggregation",
        description="PageView aggregation by category (5m window, 30s slide)",
        vpl_file="scenario1-aggregation/varpulis.vpl",
        flink_class="Scenario1FlinkMqtt",
        wait_time=35,
        expected_alerts=["news", "tech"]
    ),
    2: ScenarioConfig(
        number=2,
        name="Sequence Pattern",
        description="Login -> FailedTransaction (same user, within 10m)",
        vpl_file="scenario2-sequence/varpulis.vpl",
        flink_class="Scenario2FlinkMqtt",
        wait_time=10,
        expected_alerts=["user1", "user2", "user3"]
    ),
    3: ScenarioConfig(
        number=3,
        name="Fraud Detection",
        description="Suspicious -> 3 small tx -> large withdrawal",
        vpl_file="scenario3-fraud/varpulis.vpl",
        flink_class="Scenario3FlinkMqtt",
        wait_time=30,
        expected_alerts=["user1"]
    ),
    4: ScenarioConfig(
        number=4,
        name="Stream Join",
        description="Arbitrage detection (>1% spread between markets)",
        vpl_file="scenario4-join/varpulis.vpl",
        flink_class="Scenario4FlinkMqtt",
        wait_time=5,
        expected_alerts=["AAPL", "MSFT", "TSLA"]
    )
}


def generate_scenario1_events(pub: mqtt.Client):
    """Generate PageView events for aggregation scenario."""
    base_ts = int(time.time() * 1000)
    events = [
        {"type": "PageView", "user_id": "user1", "page": "/home", "category": "news", "duration_ms": 1500, "ts": base_ts},
        {"type": "PageView", "user_id": "user2", "page": "/sports", "category": "news", "duration_ms": 2000, "ts": base_ts + 1000},
        {"type": "PageView", "user_id": "user3", "page": "/tech", "category": "tech", "duration_ms": 3000, "ts": base_ts + 2000},
        {"type": "PageView", "user_id": "user1", "page": "/weather", "category": "news", "duration_ms": 1000, "ts": base_ts + 3000},
        {"type": "PageView", "user_id": "user4", "page": "/gadgets", "category": "tech", "duration_ms": 2500, "ts": base_ts + 4000},
        {"type": "PageView", "user_id": "user2", "page": "/reviews", "category": "tech", "duration_ms": 4000, "ts": base_ts + 5000},
        {"type": "PageView", "user_id": "user5", "page": "/politics", "category": "news", "duration_ms": 1800, "ts": base_ts + 6000},
        # Heartbeat to force Flink watermark progression (jump 5+ minutes for window close)
        {"type": "PageView", "user_id": "_heartbeat", "page": "/system", "category": "system", "duration_ms": 0, "ts": base_ts + 310000},
    ]
    for e in events:
        pub.publish(f"{INPUT_TOPIC_PREFIX}PageView", json.dumps(e))
        time.sleep(0.1)
    return len(events)


def generate_scenario2_events(pub: mqtt.Client):
    """Generate Login/Transaction events for sequence pattern."""
    base_ts = int(time.time() * 1000)
    events = [
        {"type": "Login", "user_id": "user1", "ip_address": "192.168.1.1", "device": "mobile", "ts": base_ts},
        {"type": "Login", "user_id": "user2", "ip_address": "192.168.1.2", "device": "desktop", "ts": base_ts + 1000},
        {"type": "Transaction", "user_id": "user1", "amount": 500.0, "status": "failed", "merchant": "store_a", "ts": base_ts + 2000},
        {"type": "Transaction", "user_id": "user3", "amount": 100.0, "status": "success", "merchant": "store_b", "ts": base_ts + 3000},
        {"type": "Login", "user_id": "user3", "ip_address": "192.168.1.3", "device": "tablet", "ts": base_ts + 4000},
        {"type": "Transaction", "user_id": "user2", "amount": 1500.0, "status": "failed", "merchant": "store_c", "ts": base_ts + 5000},
        {"type": "Transaction", "user_id": "user3", "amount": 200.0, "status": "failed", "merchant": "store_d", "ts": base_ts + 6000},
        # Heartbeat to force Flink watermark progression (event-time mode)
        {"type": "Login", "user_id": "_heartbeat", "ip_address": "0.0.0.0", "device": "system", "ts": base_ts + 15000},
    ]
    for e in events:
        topic = f"{INPUT_TOPIC_PREFIX}{e['type']}"
        pub.publish(topic, json.dumps(e))
        time.sleep(0.1)
    return len(events)


def generate_scenario3_events(pub: mqtt.Client):
    """Generate Transaction events for fraud pattern detection."""
    base_ts = int(time.time() * 1000)
    events = [
        # User1: suspicious transaction (high risk)
        {"user_id": "user1", "amount": 6000.0, "type": "transfer", "merchant": "foreign_bank", "location": "unknown", "risk_score": 0.85, "ts": base_ts},
        # User1: 3 small purchases
        {"user_id": "user1", "amount": 25.0, "type": "purchase", "merchant": "coffee_shop", "location": "city_a", "risk_score": 0.1, "ts": base_ts + 5000},
        {"user_id": "user1", "amount": 50.0, "type": "purchase", "merchant": "gas_station", "location": "city_a", "risk_score": 0.1, "ts": base_ts + 10000},
        {"user_id": "user1", "amount": 35.0, "type": "purchase", "merchant": "grocery", "location": "city_a", "risk_score": 0.1, "ts": base_ts + 15000},
        # User1: large withdrawal
        {"user_id": "user1", "amount": 5000.0, "type": "withdrawal", "merchant": "atm", "location": "city_b", "risk_score": 0.5, "ts": base_ts + 20000},
        # User2: normal activity
        {"user_id": "user2", "amount": 200.0, "type": "purchase", "merchant": "store", "location": "city_a", "risk_score": 0.2, "ts": base_ts + 3000},
        # Heartbeat to force Flink watermark progression
        {"user_id": "_heartbeat", "amount": 0.0, "type": "heartbeat", "merchant": "system", "location": "system", "risk_score": 0.0, "ts": base_ts + 60000},
    ]
    for e in events:
        pub.publish(f"{INPUT_TOPIC_PREFIX}Transaction", json.dumps(e))
        time.sleep(0.1)
    return len(events)


def generate_scenario4_events(pub: mqtt.Client):
    """Generate MarketTick events for arbitrage detection."""
    base_ts = int(time.time() * 1000)
    events = [
        # AAPL with >1% spread
        ({"symbol": "AAPL", "price": 150.00, "volume": 1000, "exchange": "NYSE", "ts": base_ts}, "MarketATick"),
        ({"symbol": "AAPL", "price": 152.50, "volume": 800, "exchange": "NASDAQ", "ts": base_ts + 100}, "MarketBTick"),
        # GOOG with small spread (no alert)
        ({"symbol": "GOOG", "price": 2800.00, "volume": 500, "exchange": "NYSE", "ts": base_ts + 200}, "MarketATick"),
        ({"symbol": "GOOG", "price": 2805.00, "volume": 600, "exchange": "NASDAQ", "ts": base_ts + 300}, "MarketBTick"),
        # MSFT with >1% spread
        ({"symbol": "MSFT", "price": 380.00, "volume": 1200, "exchange": "NYSE", "ts": base_ts + 400}, "MarketATick"),
        ({"symbol": "MSFT", "price": 386.00, "volume": 1000, "exchange": "NASDAQ", "ts": base_ts + 500}, "MarketBTick"),
        # TSLA with >1% spread
        ({"symbol": "TSLA", "price": 250.00, "volume": 2000, "exchange": "NYSE", "ts": base_ts + 600}, "MarketATick"),
        ({"symbol": "TSLA", "price": 255.00, "volume": 1800, "exchange": "NASDAQ", "ts": base_ts + 700}, "MarketBTick"),
        # Heartbeat to force Flink watermark progression
        ({"symbol": "_heartbeat", "price": 0.0, "volume": 0, "exchange": "system", "ts": base_ts + 5000}, "MarketATick"),
    ]
    for e, event_type in events:
        pub.publish(f"{INPUT_TOPIC_PREFIX}{event_type}", json.dumps(e))
        time.sleep(0.05)
    return len(events)


GENERATORS = {
    1: generate_scenario1_events,
    2: generate_scenario2_events,
    3: generate_scenario3_events,
    4: generate_scenario4_events,
}


class ResultCollector:
    def __init__(self, scenario_num: int):
        self.scenario_num = scenario_num
        self.varpulis_alerts: List[Dict] = []
        self.flink_alerts: List[Dict] = []
        self.start_time: float = 0

    def on_message(self, client, userdata, msg):
        recv_time = time.time() * 1000
        try:
            payload = json.loads(msg.payload.decode())
            # Varpulis may wrap data
            data = payload.get("data", payload)

            if VARPULIS_OUTPUT in msg.topic:
                self.varpulis_alerts.append({
                    "data": data,
                    "latency_ms": recv_time - self.start_time
                })
                print(f"  [VARPULIS] Alert received")
            elif FLINK_OUTPUT in msg.topic:
                self.flink_alerts.append({
                    "data": data,
                    "latency_ms": recv_time - self.start_time
                })
                print(f"  [FLINK] Alert received")
        except Exception as e:
            print(f"  Error parsing: {e}")


def check_prerequisites():
    """Check that all prerequisites are met."""
    print("\n" + "=" * 70)
    print(" Checking Prerequisites")
    print("=" * 70)

    # Check MQTT
    try:
        test_client = mqtt.Client(client_id="prereq_test", protocol=mqtt.MQTTv311)
        test_client.connect(MQTT_BROKER, MQTT_PORT)
        test_client.disconnect()
        print(f"[OK] MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
    except Exception as e:
        print(f"[ERROR] MQTT not available: {e}")
        print("Start with: docker-compose up -d")
        return False

    # Check Varpulis
    if not os.path.isfile(VARPULIS_BIN):
        print(f"[ERROR] Varpulis not found: {VARPULIS_BIN}")
        print("Build with: cd /home/cpo/cep && cargo build --release")
        return False
    print(f"[OK] Varpulis binary")

    # Check/build Flink JAR
    if not os.path.isfile(FLINK_JAR):
        print(f"[WARN] Flink JAR not found, building...")
        result = subprocess.run(
            ["mvn", "package", "-DskipTests", "-q"],
            cwd=BASE_DIR,
            capture_output=True
        )
        if result.returncode != 0:
            print(f"[ERROR] Failed to build Flink JAR")
            return False
    print(f"[OK] Flink JAR")

    os.makedirs(RESULTS_DIR, exist_ok=True)
    return True


def run_scenario(scenario_num: int) -> Dict[str, Any]:
    """Run a single scenario and return results."""
    config = SCENARIOS[scenario_num]

    print("\n" + "=" * 70)
    print(f" Scenario {config.number}: {config.name}")
    print(f" {config.description}")
    print("=" * 70)

    collector = ResultCollector(scenario_num)

    # Setup MQTT subscriber
    sub = mqtt.Client(client_id=f"bench_sub_{scenario_num}", protocol=mqtt.MQTTv311)
    sub.on_message = collector.on_message
    sub.connect(MQTT_BROKER, MQTT_PORT)
    sub.subscribe(f"{VARPULIS_OUTPUT}/#")
    sub.subscribe(FLINK_OUTPUT)
    sub.loop_start()

    # Setup MQTT publisher
    pub = mqtt.Client(client_id=f"bench_pub_{scenario_num}", protocol=mqtt.MQTTv311)
    pub.connect(MQTT_BROKER, MQTT_PORT)

    # File paths
    vpl_path = os.path.join(SCENARIOS_DIR, config.vpl_file)

    # Start Varpulis
    print(f"\n[*] Starting Varpulis...")
    varpulis_proc = subprocess.Popen(
        [VARPULIS_BIN, "run", "-f", vpl_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    # Start Flink
    print(f"[*] Starting Flink ({TIME_MODE} time)...")
    flink_proc = subprocess.Popen(
        ["java", "-cp", FLINK_JAR, f"com.benchmark.flink.{config.flink_class}", TIME_MODE],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    # Wait for initialization
    print("[*] Waiting for systems to initialize (5s)...")
    time.sleep(5)

    # Check processes
    if varpulis_proc.poll() is not None:
        print("[ERROR] Varpulis failed to start")
        out = varpulis_proc.stdout.read().decode()[:500]
        print(out)
        flink_proc.terminate()
        return {"error": "Varpulis failed"}

    if flink_proc.poll() is not None:
        print("[ERROR] Flink failed to start")
        out = flink_proc.stdout.read().decode()[:500]
        print(out)
        varpulis_proc.terminate()
        return {"error": "Flink failed"}

    # Generate events
    print(f"\n[*] Generating test events...")
    collector.start_time = time.time() * 1000
    events_count = GENERATORS[scenario_num](pub)
    print(f"[*] Published {events_count} events")

    # Wait for processing
    print(f"\n[*] Waiting for results ({config.wait_time}s)...")
    time.sleep(config.wait_time)

    # Cleanup
    sub.loop_stop()
    sub.disconnect()
    pub.disconnect()
    varpulis_proc.terminate()
    flink_proc.terminate()

    try:
        varpulis_proc.wait(timeout=3)
        flink_proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        varpulis_proc.kill()
        flink_proc.kill()

    # Compile results
    result = {
        "scenario": config.number,
        "name": config.name,
        "time_mode": TIME_MODE,
        "events_count": events_count,
        "expected_alerts": config.expected_alerts,
        "varpulis": {
            "alert_count": len(collector.varpulis_alerts),
            "alerts": collector.varpulis_alerts,
            "latencies_ms": [a["latency_ms"] for a in collector.varpulis_alerts]
        },
        "flink": {
            "alert_count": len(collector.flink_alerts),
            "alerts": collector.flink_alerts,
            "latencies_ms": [a["latency_ms"] for a in collector.flink_alerts]
        }
    }

    # Print summary
    print("\n" + "-" * 50)
    print(f"Results for Scenario {config.number}:")
    print(f"  Varpulis alerts: {len(collector.varpulis_alerts)}")
    print(f"  Flink alerts:    {len(collector.flink_alerts)}")

    if collector.varpulis_alerts:
        lats = result["varpulis"]["latencies_ms"]
        print(f"  Varpulis latency: min={min(lats):.0f}ms, max={max(lats):.0f}ms, avg={sum(lats)/len(lats):.0f}ms")

    if collector.flink_alerts:
        lats = result["flink"]["latencies_ms"]
        print(f"  Flink latency:    min={min(lats):.0f}ms, max={max(lats):.0f}ms, avg={sum(lats)/len(lats):.0f}ms")

    return result


def main():
    print("=" * 70)
    print(" MULTI-SCENARIO BENCHMARK: Varpulis vs Flink CEP")
    print(f" Time Mode: {TIME_MODE.upper()}")
    print(f" Scenarios: {SPECIFIC_SCENARIO}")
    print("=" * 70)

    if not check_prerequisites():
        sys.exit(1)

    results = {
        "timestamp": datetime.now().isoformat(),
        "time_mode": TIME_MODE,
        "scenarios": {}
    }

    # Determine which scenarios to run
    if SPECIFIC_SCENARIO == "all":
        scenarios_to_run = [1, 2, 3, 4]
    else:
        try:
            scenarios_to_run = [int(SPECIFIC_SCENARIO)]
        except ValueError:
            print(f"[ERROR] Invalid scenario: {SPECIFIC_SCENARIO}")
            sys.exit(1)

    # Run scenarios
    for scenario_num in scenarios_to_run:
        if scenario_num not in SCENARIOS:
            print(f"[WARN] Unknown scenario {scenario_num}, skipping")
            continue

        try:
            result = run_scenario(scenario_num)
            results["scenarios"][scenario_num] = result
        except Exception as e:
            print(f"[ERROR] Scenario {scenario_num} failed: {e}")
            results["scenarios"][scenario_num] = {"error": str(e)}

        # Small pause between scenarios
        time.sleep(2)

    # Save results
    results_file = os.path.join(
        RESULTS_DIR,
        f"multi_scenario_{TIME_MODE}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)

    # Print final summary
    print("\n" + "=" * 70)
    print(" FINAL SUMMARY")
    print("=" * 70)

    for num, res in results["scenarios"].items():
        if "error" in res:
            print(f"\nScenario {num}: ERROR - {res['error']}")
        else:
            v_count = res["varpulis"]["alert_count"]
            f_count = res["flink"]["alert_count"]
            print(f"\nScenario {num} ({res['name']}):")
            print(f"  Varpulis: {v_count} alerts")
            print(f"  Flink:    {f_count} alerts")

            if res["varpulis"]["latencies_ms"] and res["flink"]["latencies_ms"]:
                v_avg = sum(res["varpulis"]["latencies_ms"]) / len(res["varpulis"]["latencies_ms"])
                f_avg = sum(res["flink"]["latencies_ms"]) / len(res["flink"]["latencies_ms"])
                diff = f_avg - v_avg
                if diff > 0:
                    print(f"  => Varpulis {diff:.0f}ms faster on average")
                else:
                    print(f"  => Flink {-diff:.0f}ms faster on average")

    print(f"\nResults saved: {results_file}")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
