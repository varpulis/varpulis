//! Benchmarks for imperative programming features in VPL
//!
//! Tests performance of:
//! - For/while loops
//! - Array/map operations
//! - Built-in functions
//! - User-defined functions

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use varpulis_core::Program;
use varpulis_parser::parse;
use varpulis_runtime::{Engine, Event};

fn parse_program(source: &str) -> Program {
    parse(source).expect("Failed to parse")
}

/// Benchmark for loop performance
fn bench_for_loop(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("for_loop");

    for size in [10, 100, 1000].iter() {
        let source = format!(
            r#"
            fn sum_range(n: int) -> int:
                let total = 0
                for i in range(n):
                    total := total + i
                return total

            stream Test = Input
                .emit(result: sum_range({}))
        "#,
            size
        );

        let program = parse_program(&source);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let (tx, mut rx) = mpsc::channel(100);
                    let mut engine = Engine::new(tx);
                    engine.load(&program).unwrap();
                    engine.process(Event::new("Input")).await.unwrap();
                    let _ = rx.try_recv();
                })
            })
        });
    }

    group.finish();
}

/// Benchmark while loop performance
fn bench_while_loop(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("while_loop");

    for iterations in [10, 100, 1000].iter() {
        let source = format!(
            r#"
            fn count_to(limit: int) -> int:
                let i = 0
                while i < limit:
                    i := i + 1
                return i

            stream Test = Input
                .emit(result: count_to({}))
        "#,
            iterations
        );

        let program = parse_program(&source);

        group.bench_with_input(
            BenchmarkId::from_parameter(iterations),
            iterations,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let (tx, mut rx) = mpsc::channel(100);
                        let mut engine = Engine::new(tx);
                        engine.load(&program).unwrap();
                        engine.process(Event::new("Input")).await.unwrap();
                        let _ = rx.try_recv();
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark array operations
fn bench_array_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("array_ops");

    // Array sum
    let source = r#"
        fn array_sum() -> float:
            let arr = range(100)
            return sum(arr)

        stream Test = Input
            .emit(result: array_sum())
    "#;

    let program = parse_program(source);

    group.bench_function("sum_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    // Array sort
    let source = r#"
        fn array_sort() -> int:
            let arr = [9, 3, 7, 1, 5, 8, 2, 6, 4, 0]
            let sorted = sort(arr)
            return first(sorted)

        stream Test = Input
            .emit(result: array_sort())
    "#;

    let program = parse_program(source);

    group.bench_function("sort_10", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    // Array contains
    let source = r#"
        fn array_contains() -> bool:
            let arr = range(100)
            return contains(arr, 50)

        stream Test = Input
            .emit(result: array_contains())
    "#;

    let program = parse_program(source);

    group.bench_function("contains_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    group.finish();
}

/// Benchmark map operations
fn bench_map_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("map_ops");

    // Map access
    let source = r#"
        fn map_access() -> int:
            let m = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
            return m["c"]

        stream Test = Input
            .emit(result: map_access())
    "#;

    let program = parse_program(source);

    group.bench_function("access", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    // Map keys
    let source = r#"
        fn map_keys() -> int:
            let m = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
            return len(keys(m))

        stream Test = Input
            .emit(result: map_keys())
    "#;

    let program = parse_program(source);

    group.bench_function("keys", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    group.finish();
}

/// Benchmark built-in math functions
fn bench_math_functions(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("math_funcs");

    // Basic math
    let source = r#"
        fn math_ops() -> float:
            let a = abs(-42)
            let b = sqrt(16.0)
            let c = floor(3.7)
            let d = ceil(3.2)
            let e = round(3.5)
            return to_float(a) + b + to_float(c) + to_float(d) + to_float(e)

        stream Test = Input
            .emit(result: math_ops())
    "#;

    let program = parse_program(source);

    group.bench_function("basic_math", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    // Trigonometry
    let source = r#"
        fn trig_ops() -> float:
            let a = sin(1.0)
            let b = cos(1.0)
            let c = tan(1.0)
            return a + b + c

        stream Test = Input
            .emit(result: trig_ops())
    "#;

    let program = parse_program(source);

    group.bench_function("trig", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    group.finish();
}

/// Benchmark string functions
fn bench_string_functions(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("string_funcs");

    // String operations
    let source = r#"
        fn string_ops() -> str:
            let s = "  Hello, World!  "
            let trimmed = trim(s)
            let lower = lower(trimmed)
            let upper = upper(trimmed)
            return lower

        stream Test = Input
            .emit(result: string_ops())
    "#;

    let program = parse_program(source);

    group.bench_function("basic_string", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    // Split and join
    let source = r#"
        fn split_join() -> str:
            let s = "a,b,c,d,e"
            let parts = split(s, ",")
            return join(parts, "-")

        stream Test = Input
            .emit(result: split_join())
    "#;

    let program = parse_program(source);

    group.bench_function("split_join", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    group.finish();
}

/// Benchmark user-defined function calls
fn bench_user_functions(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("user_funcs");

    // Simple function
    let source = r#"
        fn double(x: int) -> int:
            return x * 2

        stream Test = Input
            .emit(result: double(value))
    "#;

    let program = parse_program(source);

    group.bench_function("simple_call", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine
                    .process(Event::new("Input").with_field("value", 42i64))
                    .await
                    .unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    // Recursive function (factorial)
    let source = r#"
        fn factorial(n: int) -> int:
            if n <= 1:
                return 1
            return n * factorial(n - 1)

        stream Test = Input
            .emit(result: factorial(10))
    "#;

    let program = parse_program(source);

    group.bench_function("recursive_10", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    // Complex function with multiple statements
    let source = r#"
        fn complex_calc(x: int, y: int) -> int:
            let result = 0
            if x > y:
                result := x - y
            else:
                result := y - x
            for i in range(result):
                result := result + i
            return result

        stream Test = Input
            .emit(result: complex_calc(20, 10))
    "#;

    let program = parse_program(source);

    group.bench_function("complex", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    group.finish();
}

/// Benchmark if/else conditionals
fn bench_conditionals(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("conditionals");

    // Simple if/else
    let source = r#"
        fn classify(x: int) -> str:
            if x > 100:
                return "high"
            elif x > 50:
                return "medium"
            else:
                return "low"

        stream Test = Input
            .emit(result: classify(value))
    "#;

    let program = parse_program(source);

    group.bench_function("simple_if", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine
                    .process(Event::new("Input").with_field("value", 75i64))
                    .await
                    .unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    // Nested if
    let source = r#"
        fn nested_check(a: int, b: int, c: int) -> str:
            if a > 0:
                if b > 0:
                    if c > 0:
                        return "all_positive"
                    else:
                        return "c_negative"
                else:
                    return "b_negative"
            else:
                return "a_negative"

        stream Test = Input
            .emit(result: nested_check(1, 2, 3))
    "#;

    let program = parse_program(source);

    group.bench_function("nested_if", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = mpsc::channel(100);
                let mut engine = Engine::new(tx);
                engine.load(&program).unwrap();
                engine.process(Event::new("Input")).await.unwrap();
                let _ = rx.try_recv();
            })
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_for_loop,
    bench_while_loop,
    bench_array_operations,
    bench_map_operations,
    bench_math_functions,
    bench_string_functions,
    bench_user_functions,
    bench_conditionals,
);

criterion_main!(benches);
