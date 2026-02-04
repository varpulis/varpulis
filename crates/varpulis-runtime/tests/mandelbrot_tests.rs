//! Tests for emit statement and .process() operation
//! Used by the Mandelbrot set demo

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

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
