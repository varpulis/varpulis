# Kafka source batching — known performance limitation

## Status

**Resolved (2026-04-08)** — see the "Fix shipped" section at the bottom. End-to-end throughput on the `scenario-01-filter` Kafka→Kafka pipeline is now ~80k input eps / 72k output eps, up from ~160 eps. This matches the order of magnitude of Arroyo (86k eps) on the same scenario and is on the right side of "production-usable".

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

After the fix shipped 2026-04-08, you should see the output
high-watermark reach 89000 within ~1.2 s — i.e. ~72k output eps ≈
~80k input eps (the filter passes ~89% of the generated data).

## Fix shipped — 2026-04-08

Two root causes were identified by narrow integration tests
(`crates/varpulis-connector-kafka/tests/sink_throughput_smoke.rs`),
not by guessing:

### Cause 1: legacy `KafkaSink::send` blocked per event

The engine's `.to(ConnectorOut)` path routed through the engine's
default sink factory, which built the legacy
`KafkaSink` (`crates/varpulis-connector-kafka/src/lib.rs`), whose
`SinkConnector::send` implementation did:

```rust
self.producer
    .send(record, Duration::ZERO)
    .await
    .map_err(|(e, _)| ConnectorError::SendFailed(e.to_string()))?;
```

`FutureProducer::send(...).await` **blocks until the broker ACKs** the
record. With `linger.ms = 5` and `acks = all`, each event costs one
broker round-trip (~5–7 ms ≈ 150–200 eps). For a filter+emit+sink
pipeline processing one event at a time, every single emitted record
paid that round-trip. The `[to-op] sink.send_batch(1) took 6.xxx ms`
diagnostic proved this.

**Fix:** switch `KafkaSink::send` to `producer.send_result(record)`
(non-blocking enqueue, fire-and-forget). Delivery errors after enqueue
are reported by the producer's background thread. The transactional
path still uses `.send(...).await` + `commit_transaction` for
exactly-once semantics. This matches what Arroyo's Kafka sink does.

### Cause 2: `run.rs` only injected managed sinks for `.from()` sources

`varpulis-cli/src/commands/run.rs` iterated `engine.source_bindings()`
and, for each `.from()` binding, created a shared managed sink. But a
pipeline like `stream S = Tick.from(In).where(...).to(Out)` has two
connectors (`In` and `Out`) — `In` is the only source binding, so
`Out` was NEVER considered by the sink-injection loop. The engine's
default path built a `KafkaSink` wrapped in `SinkConnectorAdapter`
instead, which hit Cause 1.

**Fix:** iterate `engine.connector_configs().keys()` and inject
managed sinks for every connector that has sink operations, not just
source ones.

### Cause 3 (also fixed, smaller effect): per-event async dispatch

The original `ManagedConnector::start_source` trait used
`Sender<Event>`. The consumer task pushed events one at a time, the
run-loop woke on each one, and the per-event tokio wake-up cost was
significant. Switched to `Sender<Vec<Event>>` and batched up to 256
events or 5 ms in the Kafka consumer. MQTT/NATS connectors wrap each
incoming message in a single-element `Vec`. `run.rs`'s main loop now
calls `engine.process_batch(batch)` directly on each received
`Vec<Event>` and additionally coalesces contiguous batches via
`try_recv`.

### Regression test

`crates/varpulis-connector-kafka/tests/sink_throughput_smoke.rs`
contains three `#[ignore]`-by-default throughput asserts:

1. `KafkaSharedSink::send` (managed path) must sustain ≥ 10k eps.
2. `KafkaSharedSink::send_batch(256)` must sustain ≥ 10k eps.
3. Legacy `KafkaSink::send` must sustain ≥ 10k eps.

Run them against a live Redpanda on `localhost:29092` with:

```bash
docker compose -f benchmarks/arroyo-comparison/docker/docker-compose.yml up -d
cargo test -p varpulis-connector-kafka --test sink_throughput_smoke \
    --release -- --ignored --nocapture
```

Current numbers on a Ryzen 9 7950X are ~140k–220k eps per test.
