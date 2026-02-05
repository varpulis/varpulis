//! Tests for imperative language features:
//! for loops, while loops, if/elif/else, return, and combined patterns.

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

// =============================================================================
// For loops
// =============================================================================

#[tokio::test]
async fn test_for_loop_range() {
    let code = r#"
        fn gen():
            for i in 0..5:
                emit R(val: i)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let mut vals = Vec::new();
    while let Ok(event) = rx.try_recv() {
        vals.push(event.get_int("val").expect("Missing val"));
    }

    assert_eq!(vals, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn test_for_loop_accumulator() {
    let code = r#"
        fn gen():
            var sum = 0
            for i in 1..=4:
                sum := sum + i
            emit R(total: sum)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let event = rx.try_recv().expect("Should have output");
    assert_eq!(event.get_int("total"), Some(10));
}

#[tokio::test]
async fn test_for_loop_break() {
    let code = r#"
        fn gen():
            for i in 0..10:
                if i == 3:
                    break
                emit R(val: i)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let mut vals = Vec::new();
    while let Ok(event) = rx.try_recv() {
        vals.push(event.get_int("val").expect("Missing val"));
    }

    assert_eq!(vals, vec![0, 1, 2]);
}

#[tokio::test]
async fn test_for_loop_continue() {
    let code = r#"
        fn gen():
            for i in 0..6:
                if i % 2 == 0:
                    continue
                emit R(val: i)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let mut vals = Vec::new();
    while let Ok(event) = rx.try_recv() {
        vals.push(event.get_int("val").expect("Missing val"));
    }

    assert_eq!(vals, vec![1, 3, 5]);
}

#[tokio::test]
async fn test_nested_for_loops() {
    let code = r#"
        fn gen():
            for row in 0..3:
                for col in 0..3:
                    emit R(row: row, col: col)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let mut results = Vec::new();
    while let Ok(event) = rx.try_recv() {
        let r = event.get_int("row").expect("Missing row");
        let c = event.get_int("col").expect("Missing col");
        results.push((r, c));
    }

    assert_eq!(results.len(), 9);
    assert_eq!(results[0], (0, 0));
    assert_eq!(results[4], (1, 1));
    assert_eq!(results[8], (2, 2));
}

// =============================================================================
// While loops
// =============================================================================

#[tokio::test]
async fn test_while_loop_basic() {
    let code = r#"
        fn gen():
            var i = 0
            while i < 5:
                emit R(val: i)
                i := i + 1

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let mut vals = Vec::new();
    while let Ok(event) = rx.try_recv() {
        vals.push(event.get_int("val").expect("Missing val"));
    }

    assert_eq!(vals, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn test_while_loop_break() {
    let code = r#"
        fn gen():
            var sum = 0
            var i = 1
            while i < 100:
                sum := sum + i
                if sum > 10:
                    break
                i := i + 1
            emit R(sum: sum, i: i)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let event = rx.try_recv().expect("Should have output");
    let sum = event.get_int("sum").expect("Missing sum");
    assert!(sum > 10, "sum should exceed 10, got {}", sum);
}

#[tokio::test]
async fn test_while_loop_continue() {
    let code = r#"
        fn gen():
            var i = 0
            while i < 10:
                i := i + 1
                if i % 3 == 0:
                    continue
                emit R(val: i)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let mut vals = Vec::new();
    while let Ok(event) = rx.try_recv() {
        vals.push(event.get_int("val").expect("Missing val"));
    }

    // 1..10 excluding multiples of 3 (3, 6, 9)
    assert_eq!(vals, vec![1, 2, 4, 5, 7, 8, 10]);
}

// =============================================================================
// If/elif/else
// =============================================================================

#[tokio::test]
async fn test_if_basic() {
    let code = r#"
        fn classify(n: int):
            if n > 0:
                emit R(label: "positive")
            else:
                emit R(label: "non_positive")

        fn gen():
            classify(5)
            classify(-3)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let e1 = rx.try_recv().expect("Should have output 1");
    assert_eq!(e1.data.get("label"), Some(&Value::Str("positive".into())));

    let e2 = rx.try_recv().expect("Should have output 2");
    assert_eq!(
        e2.data.get("label"),
        Some(&Value::Str("non_positive".into()))
    );
}

#[tokio::test]
async fn test_if_elif_else_chain() {
    let code = r#"
        fn classify(n: int):
            if n > 100:
                emit R(bucket: "high")
            elif n > 50:
                emit R(bucket: "mid")
            else:
                emit R(bucket: "low")

        fn gen():
            classify(150)
            classify(75)
            classify(10)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let e1 = rx.try_recv().expect("Should have output 1");
    assert_eq!(e1.data.get("bucket"), Some(&Value::Str("high".into())));

    let e2 = rx.try_recv().expect("Should have output 2");
    assert_eq!(e2.data.get("bucket"), Some(&Value::Str("mid".into())));

    let e3 = rx.try_recv().expect("Should have output 3");
    assert_eq!(e3.data.get("bucket"), Some(&Value::Str("low".into())));
}

#[tokio::test]
async fn test_if_nested() {
    let code = r#"
        fn classify(x: int, y: int):
            if x > 0:
                if y > 0:
                    emit R(quadrant: "I")
                else:
                    emit R(quadrant: "IV")
            else:
                if y > 0:
                    emit R(quadrant: "II")
                else:
                    emit R(quadrant: "III")

        fn gen():
            classify(1, 1)
            classify(-1, 1)
            classify(-1, -1)
            classify(1, -1)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let e1 = rx.try_recv().expect("Q1");
    assert_eq!(e1.data.get("quadrant"), Some(&Value::Str("I".into())));

    let e2 = rx.try_recv().expect("Q2");
    assert_eq!(e2.data.get("quadrant"), Some(&Value::Str("II".into())));

    let e3 = rx.try_recv().expect("Q3");
    assert_eq!(e3.data.get("quadrant"), Some(&Value::Str("III".into())));

    let e4 = rx.try_recv().expect("Q4");
    assert_eq!(e4.data.get("quadrant"), Some(&Value::Str("IV".into())));
}

// =============================================================================
// Return
// =============================================================================

#[tokio::test]
async fn test_return_value() {
    let code = r#"
        fn square(n: int) -> int:
            return n * n

        fn gen():
            let result = square(7)
            emit R(val: result)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let event = rx.try_recv().expect("Should have output");
    assert_eq!(event.get_int("val"), Some(49));
}

#[tokio::test]
async fn test_return_early_exit() {
    let code = r#"
        fn first_positive(a: int, b: int, c: int) -> int:
            if a > 0:
                return a
            if b > 0:
                return b
            if c > 0:
                return c
            return -1

        fn gen():
            let r = first_positive(-5, 3, 7)
            emit R(val: r)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let event = rx.try_recv().expect("Should have output");
    assert_eq!(event.get_int("val"), Some(3));
}

#[tokio::test]
async fn test_return_void() {
    let code = r#"
        fn maybe_emit(n: int):
            if n < 0:
                return
            emit R(val: n)

        fn gen():
            maybe_emit(-1)
            maybe_emit(42)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let mut vals = Vec::new();
    while let Ok(event) = rx.try_recv() {
        vals.push(event.get_int("val").expect("Missing val"));
    }

    // Only the second call (42) should emit
    assert_eq!(vals, vec![42]);
}

// =============================================================================
// Combined
// =============================================================================

#[tokio::test]
async fn test_fibonacci_iterative() {
    let code = r#"
        fn fib(n: int) -> int:
            var a = 0
            var b = 1
            var i = 0
            while i < n:
                let tmp = b
                b := a + b
                a := tmp
                i := i + 1
            return a

        fn gen():
            let result = fib(10)
            emit R(val: result)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let event = rx.try_recv().expect("Should have output");
    assert_eq!(event.get_int("val"), Some(55));
}

#[tokio::test]
async fn test_loop_with_conditional_emit() {
    let code = r#"
        fn gen():
            for i in 0..10:
                if i % 2 == 0:
                    emit Even(val: i)
                else:
                    emit Odd(val: i)

        stream S = Trigger
            .process(gen())
    "#;

    let program = parse(code).expect("Failed to parse");
    let (tx, mut rx) = mpsc::channel(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("Failed to load");

    engine.process(Event::new("Trigger")).await.unwrap();

    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }

    assert_eq!(events.len(), 10);

    // Even values: 0, 2, 4, 6, 8
    let evens: Vec<i64> = events
        .iter()
        .filter(|e| e.event_type.contains("Even") || e.event_type == "S")
        .filter_map(|e| e.get_int("val"))
        .filter(|v| v % 2 == 0)
        .collect();
    assert_eq!(evens.len(), 5);

    // Odd values: 1, 3, 5, 7, 9
    let odds: Vec<i64> = events
        .iter()
        .filter_map(|e| e.get_int("val"))
        .filter(|v| v % 2 != 0)
        .collect();
    assert_eq!(odds.len(), 5);
}
