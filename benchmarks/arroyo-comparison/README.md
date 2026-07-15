# Varpulis vs Arroyo — benchmark suite

Reproducible head-to-head benchmark of Varpulis and Arroyo on identical
workloads. The numbers cited in [`docs/comparisons/varpulis-vs-arroyo.md`](../../docs/comparisons/varpulis-vs-arroyo.md)
come from this suite.

## Methodology

- **100,000 events per scenario**, **5 runs each**, median throughput reported.
- **Like-for-like Kafka path**: both engines consume the same pre-loaded
  Redpanda topic (same events, same offsets) and produce to a Kafka sink
  topic whose high-watermark is polled for completion.
- **Timing starts at engine readiness**, symmetrically for both engines:
  - Arroyo: pipeline reports `state=Running` (SQL compilation and job
    deployment excluded).
  - Varpulis: the run loop prints its `Listening for events` marker
    (VPL parse and process startup — ~113 ms — excluded).
  Both engines pay their Kafka client connect and consumer-group join
  *inside* the timed window.
- **Timing ends** when the output topic high-watermark reaches the
  expected count.
- Output count and record content are verified for correctness across
  both engines (same records, same order).
- Varpulis needs `cargo build --release -p varpulis-cli --features kafka`.

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

## Latest results (July 2026, like-for-like Kafka)

| Scenario | Varpulis | Arroyo | V/A ratio | Output |
|---|---|---|---|---|
| 01 Filter (price > 50) | 160,615 eps | 99,016 eps | **1.62×** | 89,000 ✓ |

Both engines deliver identical output counts *and identical record
content in identical order*, verifying correctness. Varpulis's output
records additionally carry `event_type` and the event-time `timestamp`
(more bytes per record than Arroyo's).

History: the 2026-04 baseline measured Varpulis at 88.5k eps (0.91× vs
Arroyo) on this path. The 2026-07 hot-path work (streaming JSON→Event
decoder with key interning, run-grouped batch dispatch, batched Kafka
sink enqueue, emit-key interning, discard-mode output channel, mimalloc)
plus readiness-based timing brought it to 160.6k eps. See
`results/01_filter.json` for the engine-gain vs methodology split.

## What's not yet measured

- **Scenario 2 (tumbling 1s aggregation)**: Arroyo's `WATERMARK FOR` clause
  requires a `TIMESTAMP` column, and our generator emits `ts` as `BIGINT`
  (Unix millis) for Proton compatibility. A future revision will add a
  parallel `event_time` ISO string field. The Arroyo SQL is written
  (`scenarios/02_aggregation/arroyo.sql`); only the data generator update
  is needed.
- **Native pattern detection workloads** (sequence, Kleene closure,
  forecasting): Arroyo has no native implementation, so the only
  comparison would be Varpulis's NFA vs a hand-coded Rust UDAF — that's
  not engine-vs-engine, it's "Varpulis vs UDAF effort".
