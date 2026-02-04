#!/usr/bin/env python3
"""Integration test for the README HVAC example pipeline.

Tests the end-to-end flow:
1. Create a tenant via admin API
2. Deploy the README example pipeline
3. Inject events and verify output_events in responses
4. Clean up (delete pipeline and tenant)

Usage:
    python3 test_readme_example.py [--base-url http://localhost:9000] [--admin-key my-admin-key]
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.error


def api_request(base_url, method, path, headers=None, body=None):
    """Make an HTTP request and return (status_code, parsed_json_body)."""
    url = f"{base_url}{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body_text)
        except json.JSONDecodeError:
            return e.code, {"raw": body_text}


PIPELINE_SOURCE = r"""
event TemperatureReading:
    sensor_id: str
    zone: str
    value: float

stream HighTempAlert = TemperatureReading
    .where(value > 28)
    .emit(
        alert_type: "HIGH_TEMPERATURE",
        zone: zone,
        temperature: value
    )
"""


def main():
    parser = argparse.ArgumentParser(description="Integration test for README example")
    parser.add_argument("--base-url", default="http://localhost:9000", help="Varpulis server URL")
    parser.add_argument("--admin-key", default="test-admin-key", help="Admin API key")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    admin_key = args.admin_key
    passed = 0
    failed = 0
    tenant_id = None
    api_key = None
    pipeline_id = None

    def check(name, condition, detail=""):
        nonlocal passed, failed
        if condition:
            passed += 1
            print(f"  PASS: {name}")
        else:
            failed += 1
            print(f"  FAIL: {name}" + (f" -- {detail}" if detail else ""))

    # -------------------------------------------------------------------------
    # Step 0: Wait for server readiness
    # -------------------------------------------------------------------------
    print("\n[Step 0] Waiting for server readiness...")
    for attempt in range(10):
        try:
            status, _ = api_request(base_url, "GET", "/ready")
            if status == 200:
                print(f"  Server ready after {attempt + 1} attempt(s)")
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        print("  FAIL: Server not ready after 10 seconds")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # Step 1: Create tenant
    # -------------------------------------------------------------------------
    print("\n[Step 1] Creating tenant...")
    status, body = api_request(
        base_url, "POST", "/api/v1/tenants",
        headers={"X-Admin-Key": admin_key},
        body={"name": "Integration Test Tenant"},
    )
    check("Create tenant returns 201", status == 201, f"got {status}: {body}")
    if status == 201:
        tenant_id = body.get("id")
        api_key = body.get("api_key")
        check("Tenant has id", tenant_id is not None)
        check("Tenant has api_key", api_key is not None)
    else:
        print("  Cannot continue without tenant. Exiting.")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # Step 2: Deploy pipeline
    # -------------------------------------------------------------------------
    print("\n[Step 2] Deploying pipeline...")
    status, body = api_request(
        base_url, "POST", "/api/v1/pipelines",
        headers={"X-API-Key": api_key},
        body={"name": "readme-hvac-test", "source": PIPELINE_SOURCE},
    )
    check("Deploy pipeline returns 201", status == 201, f"got {status}: {body}")
    if status == 201:
        pipeline_id = body.get("id")
        check("Pipeline has id", pipeline_id is not None)
        check("Pipeline status is running", body.get("status") == "running", f"got {body.get('status')}")
    else:
        print("  Cannot continue without pipeline. Exiting.")
        cleanup(base_url, admin_key, api_key, tenant_id, pipeline_id)
        sys.exit(1)

    # -------------------------------------------------------------------------
    # Step 3: Inject normal temperature (should NOT trigger alert)
    # -------------------------------------------------------------------------
    print("\n[Step 3] Injecting normal temperature event (value=22)...")
    status, body = api_request(
        base_url, "POST", f"/api/v1/pipelines/{pipeline_id}/events",
        headers={"X-API-Key": api_key},
        body={"event_type": "TemperatureReading", "fields": {
            "sensor_id": "sensor-1", "zone": "lobby", "value": 22.0,
        }},
    )
    check("Inject returns 200", status == 200, f"got {status}: {body}")
    check("Response accepted=true", body.get("accepted") is True)
    output_events = body.get("output_events", [])
    check("No output events for normal temp", len(output_events) == 0,
          f"got {len(output_events)} events: {output_events}")

    # -------------------------------------------------------------------------
    # Step 4: Inject high temperature (should trigger HighTempAlert)
    # -------------------------------------------------------------------------
    print("\n[Step 4] Injecting high temperature event (value=35)...")
    status, body = api_request(
        base_url, "POST", f"/api/v1/pipelines/{pipeline_id}/events",
        headers={"X-API-Key": api_key},
        body={"event_type": "TemperatureReading", "fields": {
            "sensor_id": "sensor-1", "zone": "server-room", "value": 35.0,
        }},
    )
    check("Inject returns 200", status == 200, f"got {status}: {body}")
    check("Response accepted=true", body.get("accepted") is True)
    output_events = body.get("output_events", [])
    check("Got output event(s)", len(output_events) > 0,
          f"expected output events but got {len(output_events)}")
    if output_events:
        evt = output_events[0]
        check("Output event_type is HighTempAlert", evt.get("event_type") == "HighTempAlert",
              f"got {evt.get('event_type')}")
        check("Output has alert_type=HIGH_TEMPERATURE",
              evt.get("alert_type") == "HIGH_TEMPERATURE",
              f"got {evt.get('alert_type')}")
        check("Output has zone=server-room",
              evt.get("zone") == "server-room",
              f"got {evt.get('zone')}")
        check("Output has temperature=35",
              evt.get("temperature") == 35.0,
              f"got {evt.get('temperature')}")

    # -------------------------------------------------------------------------
    # Step 5: Check pipeline metrics
    # -------------------------------------------------------------------------
    print("\n[Step 5] Checking pipeline metrics...")
    status, body = api_request(
        base_url, "GET", f"/api/v1/pipelines/{pipeline_id}/metrics",
        headers={"X-API-Key": api_key},
    )
    check("Metrics returns 200", status == 200, f"got {status}: {body}")
    if status == 200:
        check("events_processed >= 2", body.get("events_processed", 0) >= 2,
              f"got {body.get('events_processed')}")
        check("output_events_emitted >= 1", body.get("output_events_emitted", 0) >= 1,
              f"got {body.get('output_events_emitted')}")

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------
    cleanup(base_url, admin_key, api_key, tenant_id, pipeline_id)

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed")
    print(f"{'='*60}")
    sys.exit(0 if failed == 0 else 1)


def cleanup(base_url, admin_key, api_key, tenant_id, pipeline_id):
    """Clean up test resources."""
    print("\n[Cleanup] Removing test resources...")
    if pipeline_id and api_key:
        try:
            status, _ = api_request(
                base_url, "DELETE", f"/api/v1/pipelines/{pipeline_id}",
                headers={"X-API-Key": api_key},
            )
            print(f"  Deleted pipeline: {status}")
        except Exception as e:
            print(f"  Failed to delete pipeline: {e}")

    if tenant_id and admin_key:
        try:
            status, _ = api_request(
                base_url, "DELETE", f"/api/v1/tenants/{tenant_id}",
                headers={"X-Admin-Key": admin_key},
            )
            print(f"  Deleted tenant: {status}")
        except Exception as e:
            print(f"  Failed to delete tenant: {e}")


if __name__ == "__main__":
    main()
