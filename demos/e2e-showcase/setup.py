#!/usr/bin/env python3
"""
E2E Showcase Setup Script

Creates cluster connectors and deploys the financial markets pipeline
via the Varpulis coordinator REST API.
"""

import argparse
import json
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests")
    sys.exit(1)


def wait_for_coordinator(base_url: str, timeout: int = 30) -> bool:
    """Wait for coordinator to be ready."""
    print(f"Waiting for coordinator at {base_url}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{base_url}/api/v1/cluster/workers", timeout=2)
            if resp.status_code in (200, 401):
                print("  Coordinator is ready!")
                return True
        except requests.ConnectionError:
            pass
        time.sleep(1)
    print("  ERROR: Coordinator did not start in time")
    return False


def create_connector(base_url: str, api_key: str, connector: dict) -> bool:
    """Create a cluster connector."""
    name = connector["name"]
    resp = requests.post(
        f"{base_url}/api/v1/cluster/connectors",
        headers={"x-api-key": api_key, "Content-Type": "application/json"},
        json=connector,
    )
    if resp.status_code == 201:
        print(f"  Created connector: {name}")
        return True
    elif resp.status_code == 400 and "already exists" in resp.text:
        # Update instead
        resp = requests.put(
            f"{base_url}/api/v1/cluster/connectors/{name}",
            headers={"x-api-key": api_key, "Content-Type": "application/json"},
            json=connector,
        )
        if resp.status_code == 200:
            print(f"  Updated connector: {name}")
            return True
    print(f"  ERROR creating {name}: {resp.status_code} {resp.text}")
    return False


def wait_for_workers(base_url: str, api_key: str, min_workers: int = 1, timeout: int = 30) -> bool:
    """Wait for at least min_workers to register."""
    print(f"Waiting for {min_workers} worker(s)...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(
                f"{base_url}/api/v1/cluster/workers",
                headers={"x-api-key": api_key},
            )
            if resp.status_code == 200:
                workers = resp.json().get("workers", [])
                ready = [w for w in workers if w.get("status") == "ready"]
                if len(ready) >= min_workers:
                    print(f"  {len(ready)} worker(s) ready")
                    return True
        except requests.ConnectionError:
            pass
        time.sleep(1)
    print("  ERROR: Not enough workers registered")
    return False


def deploy_pipeline(base_url: str, api_key: str, name: str, source: str) -> bool:
    """Deploy a pipeline group."""
    spec = {
        "name": name,
        "pipelines": [{"name": name, "source": source}],
    }
    resp = requests.post(
        f"{base_url}/api/v1/cluster/pipeline-groups",
        headers={"x-api-key": api_key, "Content-Type": "application/json"},
        json=spec,
    )
    if resp.status_code == 201:
        group = resp.json()
        print(f"  Pipeline deployed: {group.get('name')} (id: {group.get('id')})")
        return True
    print(f"  ERROR deploying pipeline: {resp.status_code} {resp.text}")
    return False


def main():
    parser = argparse.ArgumentParser(description="E2E Showcase Setup")
    parser.add_argument("--coordinator", default="http://localhost:9100", help="Coordinator URL")
    parser.add_argument("--api-key", default="dev-key", help="Coordinator API key")
    parser.add_argument("--mqtt-host", default="localhost", help="MQTT broker host")
    parser.add_argument("--mqtt-port", default="11883", help="MQTT broker port")
    parser.add_argument("--kafka-brokers", default="localhost:19092", help="Kafka broker(s)")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("  Varpulis E2E Showcase Setup")
    print(f"{'='*60}")
    print(f"  Coordinator: {args.coordinator}")
    print(f"  MQTT: {args.mqtt_host}:{args.mqtt_port}")
    print(f"  Kafka: {args.kafka_brokers}")
    print(f"{'='*60}\n")

    # 1. Wait for coordinator
    if not wait_for_coordinator(args.coordinator):
        sys.exit(1)

    # 2. Create connectors
    print("\nCreating connectors...")
    connectors = [
        {
            "name": "mqtt_market",
            "connector_type": "mqtt",
            "params": {"host": args.mqtt_host, "port": args.mqtt_port},
            "description": "Market data MQTT broker",
        },
        {
            "name": "kafka_signals",
            "connector_type": "kafka",
            "params": {"brokers": args.kafka_brokers},
            "description": "Trading signals Kafka cluster",
        },
    ]
    for conn in connectors:
        if not create_connector(args.coordinator, args.api_key, conn):
            sys.exit(1)

    # 3. Wait for workers
    print("\nWaiting for workers...")
    if not wait_for_workers(args.coordinator, args.api_key, min_workers=1):
        print("WARNING: No workers available. Deploy will fail.")
        print("Start workers with: varpulis server --coordinator", args.coordinator)
        sys.exit(1)

    # 4. Deploy pipeline
    print("\nDeploying pipeline...")
    vpl_path = Path(__file__).parent / "pipeline.vpl"
    source = vpl_path.read_text()
    if not deploy_pipeline(args.coordinator, args.api_key, "financial-cep", source):
        sys.exit(1)

    print(f"\n{'='*60}")
    print("  Setup complete!")
    print(f"{'='*60}")
    print("  Next steps:")
    print(f"    1. Start generator:  python generator.py --rate 50")
    print(f"    2. Start consumer:   python consumer.py")
    print(f"    3. Open dashboard:   http://localhost:5173")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
