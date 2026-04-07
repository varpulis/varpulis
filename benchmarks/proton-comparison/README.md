# Varpulis vs Timeplus Proton — benchmark suite

Reproducible head-to-head benchmark of Varpulis and Timeplus Proton on identical
workloads. The numbers cited in [`docs/comparisons/varpulis-vs-proton.md`](../../docs/comparisons/varpulis-vs-proton.md)
come from this suite.

## Methodology

- **100,000 events per scenario**, **5 runs each**, median throughput reported.
- Both engines see **identical event payloads** (same field names, same values,
  same timestamps). The two formats — `*.flat.jsonl` for Proton and
  `*.varp.jsonl` for Varpulis — are produced from the same generator function.
- Output count is verified for correctness across both engines (the runner
  asserts both produce the expected number of output events).
- **End-to-end timing**: from "submit input" to "all output events visible".
  - Varpulis: time the engine's internal `Duration:` line (the engine's own
    measured event-time after preload).
  - Proton: wall-clock time from `INSERT FORMAT JSONEachRow` start to the
    point where the destination stream's history table reaches the expected
    output count (covers ingestion + materialised view propagation).
- **Memory**: peak RSS during the run.
  - Varpulis: `/proc/{pid}/status` polling.
  - Proton: `docker stats` polling.

### What's in scope

- Single-node, single-core workloads. No clustering.
- File-mode ingestion (no Kafka, no MQTT). MQTT was deliberately excluded
  because the broker caps throughput at ~6K events/sec (we proved this in the
  apama benchmark) and would just be measuring the broker.
- Cold-start excluded. Both engines are already booted and ready before
  the timing window opens.

### What's not yet measured

- Stream-stream joins (scenario 3): the join semantics across the two engines
  are not yet apples-to-apples.
- Multi-stage pipelines (scenario 4): planned, not yet wired up.
- Native pattern detection workloads (sequence, Kleene closure, forecasting):
  Proton has no native implementation, so the comparison would be Varpulis's
  NFA vs a hand-coded JavaScript UDAF — not engine-vs-engine.

## Layout

```
benchmarks/proton-comparison/
├── README.md              # this file
├── docker/
│   ├── docker-compose.yml # Proton + Redpanda stack
│   └── ...
├── generate_events.py     # event generator (produces .flat + .varp variants)
├── run_benchmark.py       # benchmark runner (subprocess + memory tracker)
├── scenarios/
│   ├── 01_filter/
│   │   ├── varpulis.vpl
│   │   └── proton.sql
│   └── 02_aggregation/
│       ├── varpulis.vpl
│       └── proton.sql
├── data/                  # regenerable JSONL files (gitignored)
└── results/               # measured results (per-scenario JSON)
```

## Running it

Prerequisites:
- Docker
- Python 3.10+
- Varpulis release build at `target/release/varpulis` — run `cargo build --release -p varpulis-cli` from the project root

```bash
# Boot the Proton stack
cd benchmarks/proton-comparison/docker
docker compose up -d

# Wait for proton to be healthy
docker ps --filter name=bench-proton

# Run all scenarios
cd ..
python3 run_benchmark.py --scenario all --events 100000 --runs 5

# Or one scenario
python3 run_benchmark.py --scenario 01_filter --events 100000 --runs 5

# Stop the stack
cd docker && docker compose down
```

## Latest results (April 2026)

Hardware: Ryzen 9 7950X / 32 GB DDR5 / NVMe SSD.
Varpulis: v0.10.x release build.
Proton: v3.0.19 in Docker.

| Scenario | Varpulis throughput | Proton throughput | V/P ratio | Varpulis RSS | Proton RSS |
|---|---|---|---|---|---|
| 01 Filter (price > 50) | 174,751 eps | 41,353 eps | **4.23×** | 101 MB | 305 MB |
| 02 Tumbling 1s agg per device | 126,415 eps | 39,241 eps | **3.22×** | 114 MB | 367 MB |

Both engines deliver identical output counts on both scenarios, verifying
correctness as a precondition for the throughput comparison.

## Bugs found while building this benchmark

Two real Varpulis bugs were uncovered and fixed during the benchmark setup
(see the commit history):

1. **Output channel silently dropped events under backpressure.** When
   running `varpulis simulate ... | jq` (or any other slow consumer), the
   output channel's `try_send` would silently drop events when full, only
   logging a `warn!`. The fix replaces `try_send` with a `try_send + yield_now`
   retry loop so events are NEVER dropped — backpressure is applied
   cooperatively. Regression test: `tests/output_backpressure_tests.rs`.

2. **Native JSONL ignored top-level `@timestamp`.** The Varpulis native
   format `{"event_type":"X","data":{...}}` did not call `apply_json_timestamp`,
   so any top-level `@timestamp` field was silently ignored. This meant
   replaying historical data through `varpulis simulate` would assign every
   event the wall-clock time and time-based windows would never advance.
   The fix calls the timestamp parser in the native path. Regression test:
   `tests/native_jsonl_timestamp_tests.rs`.

This is exactly what benchmarks are for.
