#!/usr/bin/env python3
"""
Shared event generator for Proton vs Varpulis benchmarks.

Produces TWO files per scenario from the same underlying data:
  - <scenario>.flat.jsonl  — flat JSONL for Proton's JSONEachRow ingestion
  - <scenario>.varp.jsonl  — Varpulis nested format ({"event_type": ..., "data": {...}})

The data content is identical; only the serialization layout differs. Both
engines pay their own parse cost, which is correct (parsing is part of the
engine's job).

Usage:
    python3 generate_events.py <scenario> <count> <output_dir>
"""
import json
import sys
from pathlib import Path


def gen_filter(count):
    base_ms = 1_775_990_400_000  # 2026-04-07T10:00:00Z
    for i in range(count):
        yield {
            "event_type": "Tick",
            "ts": base_ms + i,
            "symbol": ["AAPL", "GOOG", "MSFT", "TSLA"][i % 4],
            "price": 40.0 + (i % 100),  # 40-139, 90% pass price>50
            "volume": 1000 + (i % 5000),
        }


def gen_aggregation(count):
    """Tumbling 1s window per device — 100 devices, 10ms gap between events."""
    base_ms = 1_775_990_400_000
    for i in range(count):
        yield {
            "event_type": "Reading",
            "ts": base_ms + i * 10,
            "device_id": f"dev_{i % 100}",
            "temperature": 20.0 + (i % 30) * 0.5,
        }


def gen_join(count):
    """Trade-Quote interleaved."""
    base_ms = 1_775_990_400_000
    for i in range(count):
        symbol = ["AAPL", "GOOG", "MSFT"][i % 3]
        if i % 2 == 0:
            yield {
                "event_type": "Trade",
                "ts": base_ms + i,
                "symbol": symbol,
                "price": 100.0 + (i % 50),
                "volume": 100 + (i % 900),
            }
        else:
            yield {
                "event_type": "Quote",
                "ts": base_ms + i,
                "symbol": symbol,
                "bid": 99.5 + (i % 50),
                "ask": 100.5 + (i % 50),
            }


def gen_pipeline(count):
    """Multi-stage: filter then window-agg then having clause."""
    base_ms = 1_775_990_400_000
    for i in range(count):
        yield {
            "event_type": "Reading",
            "ts": base_ms + i * 5,
            "device_id": f"dev_{i % 50}",
            "temperature": 20.0 + (i % 60),  # 20-79, ~50% > 50
            "humidity": 30.0 + (i % 70),
        }


SCENARIOS = {
    "01_filter": gen_filter,
    "02_aggregation": gen_aggregation,
    "03_join": gen_join,
    "04_pipeline": gen_pipeline,
}


def write_files(scenario, count, out_dir):
    """Write two files (same data, different layouts):
      - <scenario>.flat.jsonl  for Proton/Arroyo (flat fields, ts as int64 millis)
      - <scenario>.varp.jsonl  for Varpulis (nested under "data", with @timestamp
                                            in RFC3339 so Varpulis preserves event-time)
    """
    from datetime import datetime, timezone
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    flat_path = out_dir / f"{scenario}.flat.jsonl"
    varp_path = out_dir / f"{scenario}.varp.jsonl"

    with open(flat_path, "w") as flat, open(varp_path, "w") as varp:
        for evt in SCENARIOS[scenario](count):
            event_type = evt["event_type"]
            payload = {k: v for k, v in evt.items() if k != "event_type"}
            # Flat format for Proton/Arroyo: payload only (Proton infers type
            # from the target stream's CREATE STREAM schema)
            flat.write(json.dumps(payload) + "\n")
            # Varpulis nested format. Add @timestamp (RFC3339) so Varpulis's
            # event_file parser preserves event-time instead of stamping with
            # wall-clock — required for time-based windows to advance.
            ts_ms = payload.get("ts", 0)
            if ts_ms:
                rfc3339 = (
                    datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
                # Set @timestamp at the top level so the parser picks it up,
                # AND keep it inside data for VPL field access if needed.
                varp.write(
                    json.dumps(
                        {
                            "@timestamp": rfc3339,
                            "event_type": event_type,
                            "data": payload,
                        }
                    )
                    + "\n"
                )
            else:
                varp.write(
                    json.dumps({"event_type": event_type, "data": payload}) + "\n"
                )
    return flat_path, varp_path


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <scenario> <count> <output_dir>", file=sys.stderr)
        print(f"Scenarios: {', '.join(SCENARIOS.keys())}", file=sys.stderr)
        sys.exit(1)
    scenario, count, out_dir = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    if scenario not in SCENARIOS:
        print(f"Unknown scenario: {scenario}", file=sys.stderr)
        sys.exit(1)
    flat_path, varp_path = write_files(scenario, count, out_dir)
    print(f"Generated {count} events for {scenario}")
    print(f"  Proton/flat: {flat_path}")
    print(f"  Varpulis:    {varp_path}")


if __name__ == "__main__":
    main()
