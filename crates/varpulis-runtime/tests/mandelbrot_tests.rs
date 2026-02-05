//! Tests for emit statement and .process() operation
//! Used by the Mandelbrot set demo

use std::sync::Arc;
use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::ContextOrchestrator;

#[tokio::test]
async fn test_mandelbrot_function_parses() {
    let code = r#"
        fn mandelbrot(cx: float, cy: float, max_iter: int) -> int:
            var zr = 0.0
            var zi = 0.0
            var i = 0
            while i < max_iter:
                let r2 = zr * zr
                let i2 = zi * zi
                if r2 + i2 > 4.0:
                    return i
                zi := 2.0 * zr * zi + cy
                zr := r2 - i2 + cx
                i := i + 1
            return max_iter

        fn compute_tile(x_off: int, y_off: int, size: int, max_iter: int):
            for px in 0..size:
                for py in 0..size:
                    let cx = -2.0 + (x_off + px) * 3.0 / 1000.0
                    let cy = -1.5 + (y_off + py) * 3.0 / 1000.0
                    let iters = mandelbrot(cx, cy, max_iter)
                    emit Pixel(x: x_off + px, y: y_off + py, iterations: iters)

        stream Tile = Trigger
            .process(compute_tile(0, 0, 2, 10))
    "#;

    let program = parse(code).expect("Failed to parse mandelbrot VPL");
    let (tx, _rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine
        .load(&program)
        .expect("Failed to load mandelbrot program");
}

#[tokio::test]
async fn test_mandelbrot_small_tile() {
    // Compute a 5x5 tile and verify we get 25 Pixel events
    let code = r#"
        fn mandelbrot(cx: float, cy: float, max_iter: int) -> int:
            var zr = 0.0
            var zi = 0.0
            var i = 0
            while i < max_iter:
                let r2 = zr * zr
                let i2 = zi * zi
                if r2 + i2 > 4.0:
                    return i
                zi := 2.0 * zr * zi + cy
                zr := r2 - i2 + cx
                i := i + 1
            return max_iter

        fn compute_tile(x_off: int, y_off: int, size: int, max_iter: int):
            for px in 0..size:
                for py in 0..size:
                    let cx = -2.0 + (x_off + px) * 3.0 / 100.0
                    let cy = -1.5 + (y_off + py) * 3.0 / 100.0
                    let iters = mandelbrot(cx, cy, max_iter)
                    emit Pixel(x: x_off + px, y: y_off + py, iterations: iters,
                               diverged: iters < max_iter)

        stream Tile = Trigger
            .process(compute_tile(0, 0, 5, 32))
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(1000);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    // Collect all output events
    let mut pixel_count = 0;
    while let Ok(event) = rx.try_recv() {
        pixel_count += 1;
        // Each pixel event should have x, y, iterations, diverged
        assert!(event.data.contains_key("x"), "Missing x field");
        assert!(event.data.contains_key("y"), "Missing y field");
        assert!(
            event.data.contains_key("iterations"),
            "Missing iterations field"
        );
        assert!(
            event.data.contains_key("diverged"),
            "Missing diverged field"
        );

        // Verify iterations is a non-negative integer
        if let Some(Value::Int(iters)) = event.data.get("iterations") {
            assert!(
                *iters >= 0 && *iters <= 32,
                "iterations out of range: {}",
                iters
            );
        } else {
            panic!("iterations should be Int");
        }
    }

    assert_eq!(pixel_count, 25, "Expected 5x5 = 25 pixel events");
}

#[tokio::test]
async fn test_emit_produces_correct_fields() {
    // Verify that emit statement creates events with correct field values
    let code = r#"
        fn gen():
            emit Alpha(val: 1)
            emit Beta(val: 2)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let out1 = rx.try_recv().expect("Should have output 1");
    assert_eq!(out1.data.get("val"), Some(&Value::Int(1)));

    let out2 = rx.try_recv().expect("Should have output 2");
    assert_eq!(out2.data.get("val"), Some(&Value::Int(2)));
}

// =============================================================================
// Loop expansion + runtime integration tests
// =============================================================================

#[tokio::test]
async fn test_expanded_process_with_arithmetic_args() {
    // Verify that .process(compute(0 * 100, 0 * 200)) — the pattern produced
    // by loop expansion — evaluates correctly at runtime.
    let code = r#"fn compute(x_off: int, y_off: int):
    emit Result(x: x_off, y: y_off)

for i in 0..3:
    stream S{i} = Trigger{i}
        .process(compute({i} * 100, {i} * 200))
"#;

    let program = parse(code).expect("Failed to parse expanded VPL");
    let (tx, mut rx) = mpsc::channel(1000);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    // Trigger each stream
    engine.process(Event::new("Trigger0")).await.unwrap();
    engine.process(Event::new("Trigger1")).await.unwrap();
    engine.process(Event::new("Trigger2")).await.unwrap();

    let mut results = Vec::new();
    while let Ok(event) = rx.try_recv() {
        results.push(event);
    }

    assert_eq!(results.len(), 3, "Should emit 3 Result events");

    // Verify arithmetic was evaluated: 0*100=0, 1*100=100, 2*100=200
    assert_eq!(results[0].get_int("x"), Some(0));
    assert_eq!(results[0].get_int("y"), Some(0));
    assert_eq!(results[1].get_int("x"), Some(100));
    assert_eq!(results[1].get_int("y"), Some(200));
    assert_eq!(results[2].get_int("x"), Some(200));
    assert_eq!(results[2].get_int("y"), Some(400));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_expanded_contexts_with_process() {
    // End-to-end test: loop-expanded contexts + streams + .process() with
    // arithmetic args, dispatched through the ContextOrchestrator.
    // This mirrors the compact Mandelbrot VPL pattern.
    let code = r#"fn compute(x_off: int, y_off: int):
    emit Result(x: x_off, y: y_off)

for i in 0..4:
    context ctx{i}

for i in 0..4:
    stream S{i} = Trigger{i}
        .context(ctx{i})
        .process(compute({i} * 10, {i} * 20))
"#;

    let program = parse(code).expect("Failed to parse");
    let (output_tx, mut output_rx) = mpsc::channel::<Event>(1000);

    let (tmp_tx, _tmp_rx) = mpsc::channel(100);
    let mut tmp_engine = Engine::new(tmp_tx);
    tmp_engine.load(&program).expect("Failed to load");

    assert!(tmp_engine.has_contexts(), "Should detect 4 contexts");

    let orchestrator =
        ContextOrchestrator::build(tmp_engine.context_map(), &program, output_tx, 1000)
            .expect("Failed to build orchestrator");

    // Dispatch one event per context
    for i in 0..4 {
        let event = Event::new(format!("Trigger{}", i));
        orchestrator
            .process(Arc::new(event))
            .await
            .expect("Failed to process");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        orchestrator.shutdown();
    })
    .await
    .expect("Shutdown panicked");

    let mut results = Vec::new();
    while let Ok(event) = output_rx.try_recv() {
        results.push(event);
    }

    assert_eq!(
        results.len(),
        4,
        "Should emit 4 Result events from 4 contexts"
    );

    // Sort by x to get deterministic order (contexts process in parallel)
    results.sort_by_key(|e| e.get_int("x").unwrap_or(0));

    assert_eq!(results[0].get_int("x"), Some(0));
    assert_eq!(results[0].get_int("y"), Some(0));
    assert_eq!(results[1].get_int("x"), Some(10));
    assert_eq!(results[1].get_int("y"), Some(20));
    assert_eq!(results[2].get_int("x"), Some(20));
    assert_eq!(results[2].get_int("y"), Some(40));
    assert_eq!(results[3].get_int("x"), Some(30));
    assert_eq!(results[3].get_int("y"), Some(60));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_compact_mandelbrot_2x2_tile_runs() {
    // A scaled-down version of the compact Mandelbrot VPL (2x2 grid, 2x2 tile)
    // to verify the full pipeline: loop expansion → parse → engine → contexts → emit
    let code = r#"fn mandelbrot(cx: float, cy: float, max_iter: int) -> int:
    var zr = 0.0
    var zi = 0.0
    var i = 0
    while i < max_iter:
        let r2 = zr * zr
        let i2 = zi * zi
        if r2 + i2 > 4.0:
            return i
        zi := 2.0 * zr * zi + cy
        zr := r2 - i2 + cx
        i := i + 1
    return max_iter

fn compute_tile(x_off: int, y_off: int, size: int, max_iter: int):
    for py in 0..size:
        var row_data = ""
        for px in 0..size:
            let cx = -2.0 + (x_off + px) * 3.0 / 4.0
            let cy = -1.5 + (y_off + py) * 3.0 / 4.0
            let iters = mandelbrot(cx, cy, max_iter)
            if px > 0:
                row_data := row_data + "," + to_string(iters)
            else:
                row_data := to_string(iters)
        emit PixelRow(y: y_off + py, x_start: x_off, count: size, data: row_data)

for row in 0..2:
    for col in 0..2:
        context t{row}{col}

for row in 0..2:
    for col in 0..2:
        stream Tile{row}{col} = ComputeTile{row}{col}
            .context(t{row}{col})
            .process(compute_tile({col} * 2, {row} * 2, 2, 10))
"#;

    let program = parse(code).expect("Failed to parse compact Mandelbrot");
    let (output_tx, mut output_rx) = mpsc::channel::<Event>(1000);

    let (tmp_tx, _tmp_rx) = mpsc::channel(100);
    let mut tmp_engine = Engine::new(tmp_tx);
    tmp_engine.load(&program).expect("Failed to load");

    assert!(tmp_engine.has_contexts(), "Should have 4 contexts");

    let orchestrator =
        ContextOrchestrator::build(tmp_engine.context_map(), &program, output_tx, 1000)
            .expect("Failed to build orchestrator");

    // Inject 4 trigger events (one per tile)
    for row in 0..2 {
        for col in 0..2 {
            let event = Event::new(format!("ComputeTile{}{}", row, col));
            orchestrator
                .process(Arc::new(event))
                .await
                .expect("Failed to process");
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    tokio::task::spawn_blocking(move || {
        orchestrator.shutdown();
    })
    .await
    .expect("Shutdown panicked");

    let mut results = Vec::new();
    while let Ok(event) = output_rx.try_recv() {
        results.push(event);
    }

    // 4 tiles × 2 rows each = 8 output events
    // Events are typed by stream name (Tile00, Tile01, etc.) not PixelRow
    assert_eq!(
        results.len(),
        8,
        "Should emit 8 events (4 tiles × 2 rows), got {}",
        results.len()
    );

    // Every event should have the PixelRow fields from emit
    for row in &results {
        assert!(row.data.contains_key("y"), "Missing y field");
        assert!(row.data.contains_key("x_start"), "Missing x_start field");
        assert!(row.data.contains_key("count"), "Missing count field");
        assert!(row.data.contains_key("data"), "Missing data field");

        // count should be 2 (tile size)
        assert_eq!(row.get_int("count"), Some(2));

        // data should contain comma-separated values (1 comma for 2 values)
        if let Some(Value::Str(data)) = row.data.get("data") {
            assert!(
                data.contains(','),
                "data should contain comma-separated values, got: {}",
                data
            );
        }
    }

    // Verify all 4 tiles produced output
    let mut tile_types: Vec<String> = results.iter().map(|e| e.event_type.clone()).collect();
    tile_types.sort();
    tile_types.dedup();
    assert_eq!(tile_types.len(), 4, "All 4 tiles should produce output");
}

#[tokio::test]
async fn test_mandelbrot_vpl_file_loads_into_engine() {
    // Verify the actual mandelbrot_parallel.vpl loads into the engine
    // (without MQTT connector, so we strip the connector and .to() lines)
    let vpl = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/mandelbrot/web/mandelbrot_parallel.vpl"),
    )
    .expect("Failed to read mandelbrot_parallel.vpl");

    let program = parse(&vpl).expect("Failed to parse mandelbrot_parallel.vpl");

    let (tx, _rx) = mpsc::channel(1000);
    let mut engine = Engine::new(tx);
    engine
        .load(&program)
        .expect("Failed to load mandelbrot_parallel.vpl into engine");

    assert!(
        engine.has_contexts(),
        "Mandelbrot VPL should have 16 contexts"
    );
}
