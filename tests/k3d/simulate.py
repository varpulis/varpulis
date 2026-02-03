#!/usr/bin/env python3
"""Varpulis k3d simulation script.

Creates a tenant, deploys a pipeline, and injects events with randomized
temperature values to exercise the full stack and generate Prometheus metrics.
"""

import argparse
import random
import signal
import sys
import time

import requests


def main():
    parser = argparse.ArgumentParser(description="Simulate events for Varpulis k3d cluster")
    parser.add_argument("--base-url", default="http://localhost:9000", help="Varpulis API base URL")
    parser.add_argument("--admin-key", default="k3d-test-admin-key", help="Admin API key")
    parser.add_argument("--events", type=int, default=50, help="Number of events to inject")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between events")
    args = parser.parse_args()

    # Graceful shutdown on Ctrl+C
    shutdown = False

    def handle_signal(sig, frame):
        nonlocal shutdown
        print("\nShutting down gracefully...")
        shutdown = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    base = args.base_url.rstrip("/")

    # Step 1: Create tenant
    print(f"[1] Creating tenant...")
    resp = requests.post(
        f"{base}/api/v1/tenants",
        headers={"Content-Type": "application/json", "x-admin-key": args.admin_key},
        json={"name": "sim-tenant", "quota_tier": "free"},
    )
    if resp.status_code not in (200, 201):
        print(f"    Failed to create tenant: {resp.status_code} {resp.text}")
        sys.exit(1)
    tenant = resp.json()
    tenant_id = tenant["id"]
    api_key = tenant["api_key"]
    print(f"    Tenant created: id={tenant_id}")

    # Step 2: Deploy pipeline
    print(f"[2] Deploying pipeline...")
    resp = requests.post(
        f"{base}/api/v1/pipelines",
        headers={"Content-Type": "application/json", "x-api-key": api_key},
        json={
            "name": "sim-pipeline",
            "source": "stream Alerts = SensorReading .where(temperature > 100)",
        },
    )
    if resp.status_code not in (200, 201):
        print(f"    Failed to deploy pipeline: {resp.status_code} {resp.text}")
        sys.exit(1)
    pipeline = resp.json()
    pipeline_id = pipeline["id"]
    print(f"    Pipeline deployed: id={pipeline_id}")

    # Step 3: Inject events
    print(f"[3] Injecting {args.events} events (interval={args.interval}s)...")
    alerts_triggered = 0
    events_sent = 0

    for i in range(args.events):
        if shutdown:
            break

        # Randomize temperature: ~30% chance of exceeding 100 threshold
        temperature = random.uniform(50.0, 160.0)
        is_alert = temperature > 100.0

        resp = requests.post(
            f"{base}/api/v1/pipelines/{pipeline_id}/events",
            headers={"Content-Type": "application/json", "x-api-key": api_key},
            json={
                "event_type": "SensorReading",
                "fields": {
                    "temperature": round(temperature, 2),
                    "sensor_id": f"sim-sensor-{random.randint(1, 5)}",
                },
            },
        )

        events_sent += 1
        result = resp.json() if resp.status_code == 200 else {}
        accepted = result.get("accepted", False)

        if is_alert:
            alerts_triggered += 1

        status = "ALERT" if is_alert else "ok"
        print(
            f"    [{events_sent}/{args.events}] temp={temperature:.1f} "
            f"status={status} accepted={accepted}"
        )

        if i < args.events - 1 and not shutdown:
            time.sleep(args.interval)

    print(f"\nDone. Sent {events_sent} events, {alerts_triggered} above threshold.")

    # Cleanup: delete pipeline and tenant
    print(f"[4] Cleaning up...")
    requests.delete(
        f"{base}/api/v1/pipelines/{pipeline_id}",
        headers={"x-api-key": api_key},
    )
    requests.delete(
        f"{base}/api/v1/tenants/{tenant_id}",
        headers={"x-admin-key": args.admin_key},
    )
    print("    Cleanup complete.")


if __name__ == "__main__":
    main()
