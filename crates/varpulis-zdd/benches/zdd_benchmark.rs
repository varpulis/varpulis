//! Benchmarks comparing ZDD implementations and naive approach
//!
//! Run with: cargo bench -p varpulis-zdd

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::{BTreeSet, HashSet};
use varpulis_zdd::{Zdd, ZddArena};

// ============================================================================
// NAIVE IMPLEMENTATION (for comparison baseline)
// ============================================================================

/// Naive implementation using HashSet<BTreeSet<u32>>
fn product_with_optional_naive(sets: &HashSet<BTreeSet<u32>>, var: u32) -> HashSet<BTreeSet<u32>> {
    let mut result = sets.clone();
    for s in sets {
        let mut extended = s.clone();
        extended.insert(var);
        result.insert(extended);
    }
    result
}

// ============================================================================
// BENCHMARK: ZDD vs Naive for product_with_optional
// ============================================================================

fn bench_product_with_optional(c: &mut Criterion) {
    let mut group = c.benchmark_group("product_with_optional");

    for n in [5, 10, 12, 15, 18, 20] {
        // Naive implementation - limited to smaller sizes (exponential memory)
        if n <= 15 {
            group.bench_with_input(BenchmarkId::new("naive", n), &n, |b, &n| {
                b.iter(|| {
                    let mut sets: HashSet<BTreeSet<u32>> = HashSet::new();
                    sets.insert(BTreeSet::new());
                    for i in 0..n {
                        sets = product_with_optional_naive(&sets, i);
                    }
                    black_box(sets.len())
                });
            });
        }

        // Old ZDD (table-per-zdd with cloning)
        group.bench_with_input(BenchmarkId::new("zdd_old", n), &n, |b, &n| {
            b.iter(|| {
                let mut zdd = Zdd::base();
                for i in 0..n {
                    zdd = zdd.product_with_optional(i);
                }
                black_box(zdd.count())
            });
        });

        // New ZddArena (shared table, no cloning)
        group.bench_with_input(BenchmarkId::new("zdd_arena", n), &n, |b, &n| {
            b.iter(|| {
                let mut arena = ZddArena::new();
                let mut handle = arena.base();
                for i in 0..n {
                    handle = arena.product_with_optional(handle, i);
                }
                black_box(arena.count(handle))
            });
        });
    }

    group.finish();
}

// ============================================================================
// BENCHMARK: ZddArena vs Old Zdd for repeated operations
// ============================================================================

fn bench_repeated_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("repeated_ops");

    for n in [10, 15, 20] {
        // Old Zdd: each operation clones and remaps table
        group.bench_with_input(BenchmarkId::new("zdd_old_unions", n), &n, |b, &n| {
            b.iter(|| {
                // Build several ZDDs then union them
                let mut zdds: Vec<Zdd> = Vec::new();
                for base in 0..5 {
                    let mut zdd = Zdd::base();
                    for i in 0..n {
                        zdd = zdd.product_with_optional(base * n + i);
                    }
                    zdds.push(zdd);
                }
                // Union all
                let mut result = zdds[0].clone();
                for zdd in &zdds[1..] {
                    result = result.union(zdd);
                }
                black_box(result.count())
            });
        });

        // New ZddArena: shared table, cached operations
        group.bench_with_input(BenchmarkId::new("zdd_arena_unions", n), &n, |b, &n| {
            b.iter(|| {
                let mut arena = ZddArena::new();
                let mut handles: Vec<_> = Vec::new();
                for base in 0..5u32 {
                    let mut h = arena.base();
                    for i in 0..n {
                        h = arena.product_with_optional(h, base * n + i);
                    }
                    handles.push(h);
                }
                // Union all
                let mut result = handles[0];
                for h in &handles[1..] {
                    result = arena.union(result, *h);
                }
                black_box(arena.count(result))
            });
        });
    }

    group.finish();
}

// ============================================================================
// BENCHMARK: Count operation (with/without cache)
// ============================================================================

fn bench_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("count");

    for n in [10, 15, 20, 25] {
        // Old Zdd: count creates new HashMap each time
        let mut zdd = Zdd::base();
        for i in 0..n {
            zdd = zdd.product_with_optional(i);
        }

        group.bench_with_input(BenchmarkId::new("zdd_old", n), &zdd, |b, zdd| {
            b.iter(|| black_box(zdd.count()));
        });

        // ZddArena: builds and counts fresh each time (still faster due to arena)
        group.bench_with_input(BenchmarkId::new("zdd_arena", n), &n, |b, &n| {
            b.iter(|| {
                let mut arena = ZddArena::new();
                let mut handle = arena.base();
                for i in 0..n {
                    handle = arena.product_with_optional(handle, i);
                }
                black_box(arena.count(handle))
            });
        });
    }

    group.finish();
}

// ============================================================================
// BENCHMARK: Contains operation
// ============================================================================

fn bench_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains");

    // Build ZDDs with 20 variables
    let mut zdd = Zdd::base();
    for i in 0..20 {
        zdd = zdd.product_with_optional(i);
    }

    let mut arena = ZddArena::new();
    let mut handle = arena.base();
    for i in 0..20 {
        handle = arena.product_with_optional(handle, i);
    }

    // Test different sizes of query sets
    for size in [1, 5, 10, 15, 20] {
        let query: Vec<u32> = (0..size).collect();

        group.bench_with_input(BenchmarkId::new("zdd_old", size), &query, |b, query| {
            b.iter(|| black_box(zdd.contains(query)));
        });

        group.bench_with_input(BenchmarkId::new("zdd_arena", size), &query, |b, query| {
            b.iter(|| black_box(arena.contains(handle, query)));
        });
    }

    group.finish();
}

// ============================================================================
// BENCHMARK: Iterator performance
// ============================================================================

fn bench_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_first_100");

    for n in [10, 15, 20] {
        // Old Zdd iterator (uses path.clone())
        let mut zdd = Zdd::base();
        for i in 0..n {
            zdd = zdd.product_with_optional(i);
        }

        group.bench_with_input(BenchmarkId::new("zdd_old", n), &zdd, |b, zdd| {
            b.iter(|| {
                let first_100: Vec<_> = zdd.iter().take(100).collect();
                black_box(first_100.len())
            });
        });

        // ZddArena iterator (uses push/pop pattern)
        group.bench_with_input(BenchmarkId::new("zdd_arena", n), &n, |b, &n| {
            // Build once outside the iter
            let mut arena = ZddArena::new();
            let mut handle = arena.base();
            for i in 0..n {
                handle = arena.product_with_optional(handle, i);
            }

            b.iter(|| {
                let first_100: Vec<_> = arena.iter(handle).take(100).collect();
                black_box(first_100.len())
            });
        });
    }

    group.finish();
}

// ============================================================================
// BENCHMARK: Node efficiency (compression ratio)
// ============================================================================

fn bench_node_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_efficiency");

    // Measure how few nodes represent exponential combinations
    for n in [10, 15, 20, 25, 30] {
        group.throughput(Throughput::Elements(1 << n)); // 2^n combinations

        group.bench_with_input(BenchmarkId::new("zdd_arena", n), &n, |b, &n| {
            b.iter(|| {
                let mut arena = ZddArena::new();
                let mut handle = arena.base();
                for i in 0..n {
                    handle = arena.product_with_optional(handle, i);
                }
                // Return (count, nodes) to show efficiency
                let count = arena.count(handle);
                let nodes = arena.node_count();
                black_box((count, nodes))
            });
        });
    }

    group.finish();
}

// ============================================================================
// BENCHMARK: Large scale (stress test)
// ============================================================================

fn bench_large_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale");
    group.sample_size(10); // Fewer samples for expensive benchmarks

    // Test with 30+ variables (billions of combinations)
    for n in [25, 30, 35] {
        group.bench_with_input(BenchmarkId::new("zdd_arena", n), &n, |b, &n| {
            b.iter(|| {
                let mut arena = ZddArena::new();
                let mut handle = arena.base();
                for i in 0..n {
                    handle = arena.product_with_optional(handle, i);
                }
                black_box((arena.count(handle), arena.node_count()))
            });
        });
    }

    group.finish();
}

// ============================================================================
// BENCHMARK: Simulated CEP Kleene pattern
// ============================================================================

fn bench_cep_kleene_simulation(c: &mut Criterion) {
    let mut group = c.benchmark_group("cep_kleene");

    // Simulate processing N Kleene events and then enumerating first M matches
    for (events, enumerate) in [(20, 10), (50, 100), (100, 100)] {
        let label = format!("{}events_{}enum", events, enumerate);

        group.bench_function(&label, |b| {
            b.iter(|| {
                let mut arena = ZddArena::new();
                let mut handle = arena.base();

                // Process events (Kleene accumulation)
                for i in 0..events {
                    handle = arena.product_with_optional(handle, i);
                }

                // Enumerate first N matches
                let matches: Vec<_> = arena.iter(handle).take(enumerate).collect();
                black_box((arena.count(handle), matches.len()))
            });
        });
    }

    group.finish();
}

// ============================================================================
// BENCHMARK: Memory pressure (GC)
// ============================================================================

fn bench_gc(c: &mut Criterion) {
    let mut group = c.benchmark_group("gc");

    group.bench_function("gc_after_many_ops", |b| {
        b.iter(|| {
            let mut arena = ZddArena::new();

            // Create many ZDDs
            let mut handles = Vec::new();
            for base in 0..10u32 {
                let mut h = arena.base();
                for i in 0..15 {
                    h = arena.product_with_optional(h, base * 100 + i);
                }
                handles.push(h);
            }

            // Do many unions to fill caches
            for i in 0..handles.len() - 1 {
                let _ = arena.union(handles[i], handles[i + 1]);
            }

            // GC keeping only first handle
            let (stats, _new_handles) = arena.gc(&[handles[0]]);
            black_box(stats)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_product_with_optional,
    bench_repeated_operations,
    bench_count,
    bench_contains,
    bench_iter,
    bench_node_efficiency,
    bench_large_scale,
    bench_cep_kleene_simulation,
    bench_gc,
);
criterion_main!(benches);
