# Varpulis vs Arroyo — benchmark suite

Reproducible head-to-head benchmark of Varpulis and Arroyo on identical
workloads. The numbers cited in [`docs/comparisons/varpulis-vs-arroyo.md`](../../docs/comparisons/varpulis-vs-arroyo.md)
come from this suite.

## Methodology

- **100,000 events per scenario**, **5 runs each**, median throughput reported.
- Both engines see the same input data with the same field values.
- Arroyo runs in its production-recommended **Kafka source** path (events
  pre-loaded into a Redpanda topic before timing starts).
- Varpulis runs in its native **file-mode JSONL ingestion** path via
  `varpulis simulate` (the numbers come from the parallel
  [`proton-comparison`](../proton-comparison/) suite, which uses the
  same VPL programs).
- **End-to-end timing**: from "engine starts processing" to "output topic
  high-watermark reaches expected count" (Arroyo) or "engine reports
  Duration" (Varpulis).
- Output count is verified for correctness across both engines.

### Why different input paths?

Each engine is measured at its native fast path:

- Arroyo's primary input is Kafka. The filesystem source connector exists
  but the v0.15.0 SQL parser does not accept the `compression_format` /
  `regex_pattern` options the connector schema declares as required.
- Varpulis's primary input for batch benchmarks is file-mode JSONL via
  `simulate`, which bypasses any network/broker overhead.

A like-for-like Kafka comparison requires rebuilding `varpulis-cli` with
`--features kafka` (architectural prediction: Varpulis on Kafka would land
130-160k events/sec for filter, vs Arroyo's measured 86k).

## Layout

```
benchmarks/arroyo-comparison/
├── README.md              # this file
├── docker/
│   └── docker-compose.yml # Postgres + Redpanda + Arroyo
├── run_benchmark.py       # benchmark runner (REST API + rpk)
├── scenarios/
│   └── 01_filter/
│       ├── varpulis.vpl
│       └── arroyo.sql
├── data/                  # regenerable JSONL files (gitignored)
└── results/               # measured results (per-scenario JSON)
```

## Running it

Prerequisites:
- Docker
- Python 3.10+
- Varpulis release build (file-mode comparison only — no kafka feature
  required for the current methodology)

```bash
# Boot the Arroyo stack (Postgres + Redpanda + Arroyo)
cd benchmarks/arroyo-comparison/docker
docker compose up -d

# Wait for arroyo to be healthy
docker ps --filter name=bench-arroyo

# Run scenario 1
cd ..
python3 run_benchmark.py --scenario 01_filter --events 100000 --runs 5

# Stop the stack
cd docker && docker compose down
```

## Latest results (April 2026)

| Scenario | Varpulis | Arroyo | V/A ratio | Output |
|---|---|---|---|---|
| 01 Filter (price > 50) | 174,751 eps (file) | 86,398 eps (Kafka) | **2.02×** | 89,000 ✓ |

Both engines deliver identical output counts, verifying correctness.

## What's not yet measured

- **Scenario 2 (tumbling 1s aggregation)**: Arroyo's `WATERMARK FOR` clause
  requires a `TIMESTAMP` column, and our generator emits `ts` as `BIGINT`
  (Unix millis) for Proton compatibility. A future revision will add a
  parallel `event_time` ISO string field. The Arroyo SQL is written
  (`scenarios/02_aggregation/arroyo.sql`); only the data generator update
  is needed.
- **Like-for-like Kafka comparison**: rebuild `varpulis-cli` with
  `--features kafka` and run both engines on the same Redpanda topic.
- **Native pattern detection workloads** (sequence, Kleene closure,
  forecasting): Arroyo has no native implementation, so the only
  comparison would be Varpulis's NFA vs a hand-coded Rust UDAF — that's
  not engine-vs-engine, it's "Varpulis vs UDAF effort".
