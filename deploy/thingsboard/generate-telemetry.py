#!/usr/bin/env python3
"""
ThingsBoard + Varpulis CEP Demo — Device Telemetry Simulator

Simulates IoT devices publishing telemetry to ThingsBoard CE via MQTT.
ThingsBoard's rule chain then forwards the data to Varpulis for CEP processing.

Usage:
    pip install paho-mqtt
    python generate-telemetry.py

    # Or specify ThingsBoard host:
    python generate-telemetry.py --host thingsboard --port 1883

    # Faster rate for load testing:
    python generate-telemetry.py --rate 100

The script creates devices in 3 zones with realistic sensor data and
periodic anomalies to trigger Varpulis CEP patterns.
"""

import argparse
import json
import math
import random
import sys
import time

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Install paho-mqtt: pip install paho-mqtt")
    sys.exit(1)

# ── Device configuration ─────────────────────────────────────────

DEVICES = [
    # (device_name, access_token, zone, base_temp, base_humidity, base_pressure)
    ("sensor-zone-a-01", "TOKEN_A01", "zone-a", 22.0, 55.0, 1013.0),
    ("sensor-zone-a-02", "TOKEN_A02", "zone-a", 23.0, 57.0, 1013.5),
    ("sensor-zone-b-01", "TOKEN_B01", "zone-b", 20.0, 60.0, 1012.0),
    ("sensor-zone-b-02", "TOKEN_B02", "zone-b", 21.0, 62.0, 1012.5),
    ("sensor-zone-c-01", "TOKEN_C01", "zone-c", 25.0, 50.0, 1014.0),
    ("sensor-zone-c-02", "TOKEN_C02", "zone-c", 24.0, 52.0, 1014.5),
]

# ── Anomaly injection ────────────────────────────────────────────

ANOMALY_SCENARIOS = [
    # Each scenario triggers different Varpulis CEP patterns:
    {
        "name": "high_temperature",
        "description": "Exceeds 85C threshold → HighTemperature alert",
        "probability": 0.03,
        "apply": lambda d: {"temperature": random.uniform(86.0, 105.0)},
    },
    {
        "name": "temp_spike",
        "description": "Sudden 20C+ jump → TempSpike SASE+ pattern",
        "probability": 0.02,
        "apply": lambda d: {"temperature": d["base_temp"] + random.uniform(20.0, 35.0)},
    },
    {
        "name": "high_humidity",
        "description": "Exceeds 95% → HighHumidity alert",
        "probability": 0.02,
        "apply": lambda d: {"humidity": random.uniform(95.5, 99.0)},
    },
    {
        "name": "equipment_stress_step1",
        "description": "High humidity (>85%) — first step of 3-stage stress sequence",
        "probability": 0.04,
        "apply": lambda d: {"humidity": random.uniform(86.0, 95.0)},
    },
    {
        "name": "equipment_stress_step2",
        "description": "High temp (>70C) — second step of stress sequence",
        "probability": 0.03,
        "apply": lambda d: {"temperature": random.uniform(71.0, 85.0)},
    },
    {
        "name": "equipment_stress_step3",
        "description": "High pressure (>1050) — third step of stress sequence",
        "probability": 0.02,
        "apply": lambda d: {"pressure": random.uniform(1051.0, 1080.0)},
    },
    {
        "name": "cascade_trigger",
        "description": "Device overheats (>80C) — triggers cross-device cascade check",
        "probability": 0.02,
        "apply": lambda d: {"temperature": random.uniform(81.0, 95.0)},
    },
]


def generate_reading(device_config, tick):
    """Generate a single telemetry reading with optional anomaly injection."""
    name, token, zone, base_temp, base_hum, base_pres = device_config

    # Normal values with sinusoidal drift and noise
    temp = base_temp + 2.0 * math.sin(tick / 60.0) + random.gauss(0, 0.5)
    humidity = base_hum + 3.0 * math.sin(tick / 90.0) + random.gauss(0, 1.0)
    pressure = base_pres + 1.0 * math.sin(tick / 120.0) + random.gauss(0, 0.2)
    power_kw = random.uniform(0.5, 3.0)

    payload = {
        "temperature": round(temp, 2),
        "humidity": round(max(0, min(100, humidity)), 2),
        "pressure": round(pressure, 2),
        "power_kw": round(power_kw, 2),
    }

    # Inject anomalies
    device_info = {"base_temp": base_temp}
    anomaly = None
    for scenario in ANOMALY_SCENARIOS:
        if random.random() < scenario["probability"]:
            overrides = scenario["apply"](device_info)
            payload.update({k: round(v, 2) for k, v in overrides.items()})
            anomaly = scenario["name"]
            break

    return token, payload, anomaly


def main():
    parser = argparse.ArgumentParser(description="ThingsBoard telemetry simulator")
    parser.add_argument("--host", default="localhost", help="ThingsBoard MQTT host")
    parser.add_argument("--port", type=int, default=11883, help="ThingsBoard MQTT port")
    parser.add_argument("--rate", type=float, default=1.0, help="Readings per second per device")
    parser.add_argument("--duration", type=int, default=0, help="Duration in seconds (0=infinite)")
    args = parser.parse_args()

    interval = 1.0 / args.rate

    # Create one MQTT client per device (each uses its own access token)
    clients = []
    for name, token, zone, *_ in DEVICES:
        client = mqtt.Client(client_id=f"sim-{name}", protocol=mqtt.MQTTv311)
        client.username_pw_set(token)
        try:
            client.connect(args.host, args.port, 60)
            client.loop_start()
            clients.append((name, client))
            print(f"  Connected: {name} (token: {token})")
        except Exception as e:
            print(f"  FAILED to connect {name}: {e}")

    if not clients:
        print("\nNo devices connected. Is ThingsBoard running?")
        print("  docker compose up -d thingsboard")
        print("  # Wait ~60s for initialization, then retry")
        sys.exit(1)

    print(f"\nStreaming telemetry at {args.rate} readings/sec/device...")
    print("Press Ctrl+C to stop.\n")

    tick = 0
    start = time.time()
    anomaly_count = 0
    total_messages = 0

    try:
        while True:
            for i, device_config in enumerate(DEVICES):
                name = device_config[0]
                token, payload, anomaly = generate_reading(device_config, tick)

                # Find matching client
                for client_name, client in clients:
                    if client_name == name:
                        # ThingsBoard device MQTT API
                        client.publish("v1/devices/me/telemetry", json.dumps(payload))
                        total_messages += 1
                        if anomaly:
                            anomaly_count += 1
                            print(f"  [{name}] ANOMALY ({anomaly}): {payload}")
                        break

            tick += 1
            elapsed = time.time() - start

            # Progress update every 30 seconds
            if tick % max(1, int(30 * args.rate)) == 0:
                rate = total_messages / elapsed if elapsed > 0 else 0
                print(f"  [{int(elapsed)}s] {total_messages} messages sent "
                      f"({rate:.1f} msg/s), {anomaly_count} anomalies injected")

            if args.duration > 0 and elapsed >= args.duration:
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n\nStopped after {total_messages} messages, {anomaly_count} anomalies.")

    finally:
        for name, client in clients:
            client.loop_stop()
            client.disconnect()


if __name__ == "__main__":
    main()
