#!/usr/bin/env python3
"""Generate batch event payloads for Varpulis HTTP injection benchmark."""
import json
import random
import sys

count = int(sys.argv[1]) if len(sys.argv) > 1 else 100
devs = [
    ("sensor-a1", "zone-a"), ("sensor-a2", "zone-a"),
    ("sensor-b1", "zone-b"), ("sensor-b2", "zone-b"),
    ("sensor-c1", "zone-c"), ("sensor-c2", "zone-c"),
]
events = []
for i in range(count):
    d, z = devs[i % len(devs)]
    events.append({"event_type": "TbTelemetry", "fields": {
        "deviceName": d, "deviceType": "IoT Sensor", "zone": z,
        "temperature": round(22 + random.gauss(0, 15), 2),
        "humidity": round(55 + random.gauss(0, 10), 2),
        "pressure": round(1013 + random.gauss(0, 5), 2),
        "power_kw": round(random.uniform(0.5, 3), 2),
    }})
print(json.dumps(events))
