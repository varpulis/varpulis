# Kafka Exactly-Once Crash Recovery Test

Verifies that the Varpulis 2PC checkpoint barrier delivers exactly-once
semantics by killing the process at different points during the checkpoint
cycle and checking for duplicates or gaps on restart.

## Prerequisites

1. Docker (for Redpanda)
2. Varpulis binary built with Kafka support:
   ```
   cargo build --release --features kafka -p varpulis-cli
   ```
3. Python 3.8+

## Running

```bash
# Start Redpanda
docker compose -f tests/kafka_exactly_once/docker-compose.yml up -d

# Wait for Redpanda to be healthy
docker compose -f tests/kafka_exactly_once/docker-compose.yml ps

# Run all scenarios (10,000 events each)
python3 tests/kafka_exactly_once/test_harness.py

# Run a specific scenario with custom event count
python3 tests/kafka_exactly_once/test_harness.py --scenario a --events 5000

# Use a custom binary path
python3 tests/kafka_exactly_once/test_harness.py --varpulis-bin ./target/debug/varpulis
```

## Test Scenarios

### A: Kill during processing
Starts varpulis, waits until ~50% of events appear in the output topic,
then sends SIGKILL. Restarts and waits for completion. Verifies no
duplicates or gaps.

### B: Kill after short run
Starts varpulis, waits 1 second, then SIGKILL. Tests that very early
kills (before any 2PC checkpoint completes) correctly replay all events.

### C: Kill and restart 3x
Kills at ~20%, ~50%, ~80% progress with restarts in between. The most
aggressive scenario -- exercises multiple crash/recovery cycles.

## How it works

The VPL pipeline (`scenario_eo.vpl`) is a simple passthrough that reads
sequentially-numbered JSON events from `test-eo-input` and writes them
transactionally to `test-eo-output` using `exactly_once: true`.

The 2PC barrier in `run.rs` fires every ~2 seconds:
1. Engine state snapshot
2. `prepare_commit` -- flush Kafka producer (data invisible)
3. `commit` -- `commit_transaction()` (data visible)
4. Commit consumer group offsets

On SIGKILL between steps 2 and 3, the uncommitted transaction is aborted
by Kafka (transaction timeout). On restart, the consumer group resumes
from the last committed offset, replaying only the events that were not
yet committed to the output.

Verification reads the output topic and checks:
- Total count matches expected (no gaps)
- No duplicate event IDs

## Cleanup

```bash
docker compose -f tests/kafka_exactly_once/docker-compose.yml down -v
```
