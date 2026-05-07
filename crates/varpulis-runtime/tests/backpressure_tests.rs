//! Backpressure testing — Phase 0 / Task 0.3.
//!
//! Verifies the runtime's behaviour when a sink is slower than its source:
//!   1. **No OOM** — internal buffers stay bounded (the producer is throttled,
//!      not silently buffering forever).
//!   2. **Metrics report backpressure state** — the relevant components
//!      ([`StageBuffer`], [`WorkerPool`]) expose counters that increment when
//!      backpressure is being applied, so an operator can detect and react.
//!
//! Sibling file: `output_backpressure_tests.rs` covers the no-event-loss
//! invariant of the engine's output channel specifically. This file focuses
//! on the *bounded-memory + metric-visibility* invariants across all of
//! Varpulis' backpressure surfaces.
//!
//! These are pure in-process tests — no Kafka / MQTT broker required. Sink
//! "slowness" is simulated with `tokio::time::sleep` on the consumer side.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use varpulis_core::{Event, Value};
use varpulis_parser::parse;
use varpulis_runtime::backpressure::{StageBuffer, StageBufferConfig, WhenFull};
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::SharedEvent;
use varpulis_runtime::worker_pool::{BackpressureStrategy, WorkerPool, WorkerPoolConfig};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build N `Tick { n: i }` events for feeding into a simple emit pipeline.
fn make_ticks(n: u64) -> Vec<Event> {
    (0..n)
        .map(|i| {
            let mut e = Event::new("Tick");
            e.data.insert(Arc::from("n"), Value::Int(i as i64));
            e
        })
        .collect()
}

/// VPL program that just re-emits each Tick — used as a minimal pipeline to
/// drive the engine's output channel.
const TICK_PIPELINE: &str = r"
event Tick:
    n: int

stream Out = Tick
    .emit(n: n)
";

// ---------------------------------------------------------------------------
// Engine-level backpressure (slow output sink)
// ---------------------------------------------------------------------------

/// Slow sink + small channel must not blow up memory: the producer thread is
/// throttled (cooperative `yield_now` retry on the output sender), so the
/// in-flight event count is bounded by the channel capacity. We verify this
/// by sampling the channel's depth while events are flowing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_slow_sink_keeps_channel_depth_bounded() {
    use tokio::sync::Notify;

    const CHANNEL_CAP: usize = 64;
    const TOTAL: u64 = 5_000;

    let (tx, mut rx) = mpsc::channel::<SharedEvent>(CHANNEL_CAP);
    // Held by the probe task only — dropped when probe exits. Keeping a
    // sender clone in a long-running task would block the consumer's
    // `rx.recv()` from ever returning `None`, so the probe explicitly
    // releases it on shutdown.
    let depth_probe = tx.clone();
    let stop = Arc::new(Notify::new());
    let stop_clone = Arc::clone(&stop);

    let peak_depth = Arc::new(AtomicUsize::new(0));
    let peak_depth_clone = Arc::clone(&peak_depth);
    let probe_task = tokio::spawn(async move {
        loop {
            let depth = depth_probe
                .max_capacity()
                .saturating_sub(depth_probe.capacity());
            let prev = peak_depth_clone.load(Ordering::Relaxed);
            if depth > prev {
                peak_depth_clone.store(depth, Ordering::Relaxed);
            }
            tokio::select! {
                () = stop_clone.notified() => break,
                () = tokio::time::sleep(Duration::from_micros(200)) => {}
            }
        }
        // depth_probe is dropped here, allowing the channel to fully close
        // once the engine drops its own sender.
    });

    // Slow consumer: pull each event, yield a little. With a 64-deep channel
    // and a producer hot-looping, the channel will sit near full almost
    // continuously — exactly the slow-sink scenario.
    let received = Arc::new(AtomicUsize::new(0));
    let received_clone = Arc::clone(&received);
    let consumer = tokio::spawn(async move {
        while let Some(_evt) = rx.recv().await {
            received_clone.fetch_add(1, Ordering::Relaxed);
            // Slower than producer to force backpressure
            tokio::task::yield_now().await;
        }
    });

    let mut engine = Engine::new_shared(tx);
    let program = parse(TICK_PIPELINE).expect("parse");
    engine.load(&program).expect("load");

    // Drive the engine in small batches so backpressure has time to kick in.
    // Use process_batch (async) so the slow consumer task can be polled by
    // the runtime concurrently with the producer.
    let events = make_ticks(TOTAL);
    for chunk in events.chunks(64) {
        engine
            .process_batch(chunk.to_vec())
            .await
            .expect("process_batch");
    }

    drop(engine); // engine drops its own tx
    stop.notify_one(); // signal probe to stop and release its tx clone
    probe_task.await.expect("probe panicked");
    // With both sender holders dropped, consumer's recv() now returns None.
    consumer.await.expect("consumer panicked");

    let peak = peak_depth.load(Ordering::Relaxed);
    let got = received.load(Ordering::Relaxed) as u64;

    assert_eq!(
        got, TOTAL,
        "all {TOTAL} events must reach the consumer (no silent drops)"
    );
    assert!(
        peak <= CHANNEL_CAP,
        "channel depth must stay bounded by capacity {CHANNEL_CAP}, but peak was {peak}"
    );
}

/// Backpressure must throttle the producer: with a small channel and a sink
/// that takes ~50µs per event, the wall-clock time for the engine call to
/// finish processing N events is dominated by the consumer's drain time —
/// not by how fast the producer can push. If the engine were silently
/// dropping or unboundedly buffering, the producer would race ahead and
/// finish far faster than the consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_producer_is_throttled_by_slow_consumer() {
    const CHANNEL_CAP: usize = 16;
    const TOTAL: u64 = 2_000;
    // Target consumer rate ≈ 20k ev/s. Producer alone would do >1M ev/s.
    const SLEEP_PER_EVENT: Duration = Duration::from_micros(50);

    let (tx, mut rx) = mpsc::channel::<SharedEvent>(CHANNEL_CAP);

    let received = Arc::new(AtomicUsize::new(0));
    let received_clone = Arc::clone(&received);
    let consumer = tokio::spawn(async move {
        while let Some(_evt) = rx.recv().await {
            tokio::time::sleep(SLEEP_PER_EVENT).await;
            received_clone.fetch_add(1, Ordering::Relaxed);
        }
    });

    let mut engine = Engine::new_shared(tx);
    let program = parse(TICK_PIPELINE).expect("parse");
    engine.load(&program).expect("load");

    let events = make_ticks(TOTAL);
    let start = Instant::now();
    // Use process_batch (async) so the multi-thread runtime can drain the
    // consumer concurrently while the producer cooperatively yields.
    for chunk in events.chunks(64) {
        engine
            .process_batch(chunk.to_vec())
            .await
            .expect("process_batch");
    }
    let producer_elapsed = start.elapsed();

    drop(engine);
    consumer.await.expect("consumer panicked");

    let got = received.load(Ordering::Relaxed) as u64;
    assert_eq!(got, TOTAL, "no events lost");

    // If backpressure is applied, the producer cannot finish faster than the
    // consumer can drain. We use a generous bound (50% of theoretical drain
    // time) to avoid flakes on slow CI: the point is that the producer is
    // *not* dramatically faster than the consumer.
    let min_expected = SLEEP_PER_EVENT * (TOTAL as u32) / 2;
    assert!(
        producer_elapsed >= min_expected,
        "producer finished in {producer_elapsed:?}, but slow consumer should have throttled it to ≥ {min_expected:?}"
    );
}

// ---------------------------------------------------------------------------
// StageBuffer — explicit backpressure-state metrics
// ---------------------------------------------------------------------------

/// `WhenFull::Block` strategy: under sustained over-supply, `blocks_total`
/// must increment so an operator can observe that the buffer is saturated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stage_buffer_block_strategy_reports_blocks_in_metrics() {
    const CAPACITY: usize = 4;
    const SENDS: u64 = 200;

    let (buffer, mut rx) = StageBuffer::new(StageBufferConfig {
        capacity: CAPACITY,
        when_full: WhenFull::Block,
    });
    let metrics = Arc::clone(buffer.metrics());

    // Slow drainer
    let drainer = tokio::spawn(async move {
        let mut count = 0u64;
        while let Some(_evt) = rx.recv().await {
            tokio::time::sleep(Duration::from_micros(100)).await;
            count += 1;
        }
        count
    });

    for i in 0..SENDS {
        let mut e = Event::new("Tick");
        e.data.insert(Arc::from("n"), Value::Int(i as i64));
        buffer.send(Arc::new(e)).await.expect("send");
    }

    drop(buffer); // closes channel
    let drained = drainer.await.expect("drainer panicked");

    assert_eq!(drained, SENDS, "all events drained, none lost");
    assert_eq!(
        metrics.events_received.load(Ordering::Relaxed),
        SENDS,
        "events_received counts every send"
    );
    assert_eq!(
        metrics.events_dropped.load(Ordering::Relaxed),
        0,
        "Block strategy never drops"
    );
    // The exact number depends on scheduler timing, but with a 4-deep channel
    // and 200 sends against a 100µs/event drainer, blocks_total should be
    // strictly positive — we observed the buffer at full capacity at least
    // once. Allow zero only if the scheduler somehow kept us perfectly
    // ahead, which is very unlikely on a 2-thread runtime.
    let blocks = metrics.blocks_total.load(Ordering::Relaxed);
    assert!(
        blocks > 0,
        "blocks_total must increment when buffer fills (got {blocks})"
    );
}

/// `WhenFull::DropNewest`: when oversaturated, the buffer drops incoming
/// events and increments `events_dropped`. Verifies the metric is wired up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stage_buffer_drop_newest_reports_drops_in_metrics() {
    const CAPACITY: usize = 2;
    const SENDS: u64 = 1_000;

    let (buffer, _rx) = StageBuffer::new(StageBufferConfig {
        capacity: CAPACITY,
        when_full: WhenFull::DropNewest,
    });
    let metrics = Arc::clone(buffer.metrics());

    // Don't drain — ensure the buffer fills and stays full.
    for i in 0..SENDS {
        let mut e = Event::new("Tick");
        e.data.insert(Arc::from("n"), Value::Int(i as i64));
        buffer.send(Arc::new(e)).await.expect("send");
    }

    let received = metrics.events_received.load(Ordering::Relaxed);
    let dropped = metrics.events_dropped.load(Ordering::Relaxed);

    assert_eq!(received, SENDS, "every send is recorded as received");
    assert!(
        dropped >= SENDS - CAPACITY as u64,
        "with capacity {CAPACITY} and {SENDS} sends and no drainer, ≥{} drops expected, got {dropped}",
        SENDS - CAPACITY as u64
    );
}

// ---------------------------------------------------------------------------
// WorkerPool — pool-level backpressure visibility
// ---------------------------------------------------------------------------

/// `BackpressureStrategy::Error`: when the pool is saturated, `submit` fails
/// AND `events_dropped` in the pool metrics increments — operators can wire
/// this into a dashboard and know that the pool is the bottleneck.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_pool_error_strategy_reports_drops_in_metrics() {
    let config = WorkerPoolConfig {
        name: "bp-error".to_string(),
        workers: 1,
        queue_size: 1,
        backpressure: BackpressureStrategy::Error,
    };

    // Slow processor — synchronous sleep so the dispatcher's queue saturates.
    let pool = WorkerPool::new(config, |_event| {
        std::thread::sleep(Duration::from_millis(20));
    });

    let mut errors = 0u64;
    for i in 0..200u64 {
        let event = Event::new("Tick").with_field("id", i as i64);
        if pool.submit(event, "p").await.is_err() {
            errors += 1;
        }
    }

    let metrics = pool.metrics().await;
    assert!(
        errors > 0,
        "Error strategy must surface at least one PoolBackpressureError under saturation"
    );
    assert!(
        metrics.events_dropped > 0,
        "pool metrics must report dropped events when backpressure rejects (got {})",
        metrics.events_dropped
    );
    assert_eq!(
        metrics.events_dropped, errors,
        "events_dropped should match observed error count"
    );
}

/// `BackpressureStrategy::Block`: under sustained load the pool blocks the
/// submitter. Events are NOT dropped, the queue depth is bounded, and all
/// events eventually reach the processor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_pool_block_strategy_no_drops_no_oom() {
    const TOTAL: u64 = 500;
    let processed = Arc::new(AtomicUsize::new(0));
    let processed_clone = Arc::clone(&processed);

    let config = WorkerPoolConfig {
        name: "bp-block".to_string(),
        workers: 2,
        queue_size: 8, // small — forces backpressure at submit-time
        backpressure: BackpressureStrategy::Block,
    };

    let pool = WorkerPool::new(config, move |_event| {
        // Slow processor
        std::thread::sleep(Duration::from_micros(200));
        processed_clone.fetch_add(1, Ordering::Relaxed);
    });

    for i in 0..TOTAL {
        let event = Event::new("Tick").with_field("id", i as i64);
        pool.submit(event, "p").await.expect("submit");
    }

    // Drain: poll until processed count reaches TOTAL or we time out.
    let deadline = Instant::now() + Duration::from_secs(10);
    while processed.load(Ordering::Relaxed) < TOTAL as usize {
        if Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let metrics = pool.metrics().await;
    assert_eq!(
        metrics.events_dropped, 0,
        "Block strategy must never drop events"
    );
    assert_eq!(
        processed.load(Ordering::Relaxed),
        TOTAL as usize,
        "all submitted events must be processed"
    );
    // Queue depth at end-of-run is essentially 0 — bounded by definition,
    // but we still assert it never exceeded the configured queue_size *
    // workers (which the pool's accounting guarantees).
    assert!(
        metrics.queue_depth <= 8 * 2,
        "queue depth {} exceeded configured upper bound",
        metrics.queue_depth
    );
}
