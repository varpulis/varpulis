//! In-process CPU profile of the async process_batch path.
//! Uses the pprof crate for timer-signal-based sampling (no perf needed).
//! Unix-only (pprof depends on POSIX signal handling).
//! Run: cargo test -p varpulis-runtime --features arrow --test profile_batch_dispatch --release -- --nocapture
#![cfg(unix)]

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

fn make_events(n: usize, n_devices: usize) -> Vec<Event> {
    let base_ms = 1_775_990_400_000i64;
    (0..n)
        .map(|i| {
            let dev = i % n_devices;
            let ts_ms = base_ms + (i as i64) * 10;
            let ts = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts_ms).unwrap();
            Event::new("Reading")
                .with_timestamp(ts)
                .with_field("ts", Value::Int(ts_ms))
                .with_field("device_id", Value::Str(format!("dev_{dev}").into()))
                .with_field("temperature", Value::Float(20.0 + (i % 30) as f64 * 0.5))
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn profile_process_batch() {
    let source = r"
        event Reading:
            ts: int
            device_id: str
            temperature: float

        stream DeviceAgg = Reading
            .partition_by(device_id)
            .window(1s)
            .aggregate(
                s: sum(temperature),
                a: avg(temperature),
                mn: min(temperature),
                mx: max(temperature)
            )
            .emit(device_id: device_id, s: s, a: a, mn: mn, mx: mx)
    ";
    let program = parse(source).expect("parse");
    let (output_tx, mut output_rx) = mpsc::channel::<Event>(10_000);
    let mut engine = Engine::builder().output(output_tx).build();
    engine.load(&program).expect("load");

    let drain = tokio::spawn(async move {
        let mut c = 0u64;
        while output_rx.recv().await.is_some() {
            c += 1;
        }
        c
    });

    let all_events = make_events(100_000, 100);

    // Profile only the processing loop
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();

    let start = std::time::Instant::now();
    for chunk in all_events.chunks(256) {
        engine.process_batch(chunk.to_vec()).await.expect("ok");
    }
    let elapsed = start.elapsed();

    if let Ok(report) = guard.report().build() {
        let mut file = std::fs::File::create("/tmp/varpulis_pprof.svg").unwrap();
        report.flamegraph(&mut file).unwrap();
        eprintln!("Flamegraph written to /tmp/varpulis_pprof.svg");
    }

    drop(engine);
    let drained = drain.await.unwrap();
    eprintln!(
        "Processed 100k events in {:?} ({:.0} eps), drained {} output",
        elapsed,
        100_000.0 / elapsed.as_secs_f64(),
        drained
    );
}
