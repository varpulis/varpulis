# Kafka source batching — known performance limitation

## Status

**Open** — tracked for the next streaming-performance milestone.

## Problem

When `varpulis run` consumes events from a Kafka source and writes to a
Kafka sink, sustained throughput is currently bounded at roughly **150-700
events/sec** even on a single 100k-event topic and a trivial filter pipeline.
For comparison:

- `rpk topic consume` (raw librdkafka baseline): ~56,000 events/sec
- Arroyo on the same Redpanda topic + same filter: ~86,000 events/sec
- Varpulis on the same engine pipeline via **file mode** (`varpulis simulate`):
  ~174,000 events/sec

So the engine itself can sustain >170k eps, but the Kafka source path
loses 200-1000x of that throughput somewhere between librdkafka and
`engine.process(event).await`. **A streaming engine bottlenecked at <1k eps
on its primary streaming source is functionally useless.**

This was uncovered while writing `benchmarks/arroyo-comparison/` and
trying to produce an apples-to-apples Kafka-vs-Kafka measurement
between Varpulis and Arroyo.

## Bugs already fixed in the same investigation

While writing the benchmark, the connector path exposed **six** independent
bugs that have been fixed in the same commit series:

1. **`send_output_shared` silently dropped events** under stdout pipe
   backpressure (`try_send` + warn-and-drop). Replaced with cooperative
   `try_send + yield_now` retry loop. Regression test:
   `tests/output_backpressure_tests.rs`.
2. **Native JSONL parser ignored top-level `@timestamp`** in the Varpulis
   format `{"event_type":"X","data":{...}}` — only the Sysmon and generic
   flat paths called `apply_json_timestamp`. Time-based windows replaying
   historical JSONL never advanced. Regression test:
   `tests/native_jsonl_timestamp_tests.rs`.
3. **`brokers` array silently dropped** in `engine/sink_factory.rs` —
   `ConfigValue::Array(_) => continue` skipped the entire array. The
   validator demanded an array but the converter threw it away. Now joins
   array elements with commas (the standard `bootstrap.servers` format).
4. **`auto_offset_reset` was not honored** at the connector level —
   hardcoded to `latest` regardless of user config. Added the param to
   `KAFKA_PARAMS` validator schema and the runtime now reads it from
   `params` or `self.config.properties`.
5. **Producer client config leaked VPL-only properties to librdkafka** —
   `ensure_producer` forwarded `auto_offset_reset`, `group_id`, etc.
   directly to librdkafka, which rejected the unknown property names.
   Both consumer and producer config builders now share an
   `is_vpl_only_property` filter.
6. **`varpulis run` had no `--quiet` flag** — every output event was
   printed via `println!` with Debug formatting + global stdout lock,
   serialising the entire pipeline at the print-rate. Added `--quiet`
   to match `varpulis simulate`.

## Remaining bottleneck

Even with all six fixes above applied, end-to-end throughput is still
~150-700 eps. After tightening the consumer loop further (removing the
`tokio::time::timeout(100ms)` per-event timer registration, removing the
per-event `commit_message`), no significant improvement was observed.

The bottleneck appears to be **per-event async dispatch overhead** in the
`run.rs` main loop:

```rust
loop {
    tokio::select! {
        Some(event) = event_rx.recv() => {
            if event_rx.is_empty() {
                // Single event — fast path
                engine.process(event).await
            } else {
                // Multiple events buffered — drain and batch
                let mut batch = vec![event];
                while let Ok(extra) = event_rx.try_recv() {
                    batch.push(extra);
                }
                engine.process_batch(batch).await
            }
        }
    }
}
```

The `process_batch` path is much faster per event than `process` (per-event
async setup, tracing span creation, watermark check, etc.). But the
batching only kicks in when the consumer produces events faster than the
engine drains them — and right now the consumer feeds events one-at-a-time
via `tx.send(event).await`, which immediately wakes the run-loop, which
processes the single event via the slow path.

The result: a ping-pong between the consumer task and the run-loop where
each event takes a full async wake-up cycle, dominating the actual
filter+emit work.

## Architectural fix (todo)

**Option A — Consumer-side batching.**

Change the connector→run-loop channel from `Sender<Event>` to
`Sender<Vec<Event>>`. The Kafka source consumer accumulates events in
a small buffer (e.g., 256 events or 5ms, whichever comes first) and
sends them as a batch. The run-loop calls `engine.process_batch_shared`
on each batch.

Tradeoffs:
- **Pros**: Eliminates the per-event ping-pong. Each `recv().await` cycle
  amortizes over many events. Batches of 256 events at 1ms processing
  cost = ~250k eps ceiling.
- **Cons**: Adds up to ~5ms latency per batch. Requires changes to all
  source connectors (not just kafka), or a wrapper that adapts
  single-event sources to the batch contract.
- **Compatibility**: this is a public-API-breaking change to the
  `ManagedConnector` trait. Needs a versioning story.

**Option B — Adaptive batching in the run-loop.**

Keep the per-event channel but make the run-loop drain *aggressively*
before processing. Currently it does a single `try_recv` loop. Better:
add a small `tokio::time::sleep(Duration::from_micros(50)).await` before
processing to let the consumer fill the channel, then drain everything.

Tradeoffs:
- **Pros**: No API change. Can land in a single PR.
- **Cons**: Adds ~50µs latency per batch. Less efficient than option A
  (still has the per-event consumer task wake-up).

**Option C — Drop the run-loop and let the engine pull from sources.**

Inverted control flow: instead of the consumer pushing events into a
channel and the run-loop pulling, the engine itself drives source
polling. This is closer to what Flink and Arroyo do.

Tradeoffs:
- **Pros**: Architecturally cleaner, eliminates the channel entirely.
- **Cons**: Largest change. Requires re-thinking how multi-source
  pipelines coordinate.

## Recommended next step

Start with **Option A** (consumer-side batching) because:
1. It localises the change to the connector layer plus a small run-loop
   adaptation.
2. The performance ceiling matches what `process_batch_shared` already
   delivers (174k eps file mode → ~150k eps Kafka mode is a realistic target).
3. It does not require re-architecting the engine's event-processing
   contract.

The `ManagedConnector` trait API change is the only invasive part.
Bumping the trait to `Sender<Vec<Event>>` is a breaking change for
out-of-tree connectors, but the in-tree connectors are all maintained
in this repo and can be migrated atomically.

## Reproduction

```bash
cd benchmarks/arroyo-comparison
docker compose -f docker/docker-compose.yml up -d

# Pre-load 100k events into Redpanda
python3 ../proton-comparison/generate_events.py 01_filter 100000 data/
cat data/01_filter.flat.jsonl | docker exec -i bench-arroyo-redpanda \
    rpk topic produce scenario-01-filter --brokers redpanda:9092

# Run Varpulis (built with kafka feature)
cargo build --release -p varpulis-cli --features 'varpulis-runtime/kafka'
./target/release/varpulis run --file scenarios/01_filter/varpulis.vpl --quiet
# Watch output topic high-watermark
docker exec bench-arroyo-redpanda rpk topic describe -p scenario-01-filter-vpl-out
```

You should see the output high-watermark climb at ~150-700 events/sec
on a Ryzen 9 7950X. After the architectural fix, expect 50-150k eps.
