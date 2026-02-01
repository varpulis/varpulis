//! Benchmarks comparing ZDD vs naive implementation

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::{BTreeSet, HashSet};
use varpulis_zdd::Zdd;

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

fn bench_product_with_optional(c: &mut Criterion) {
    let mut group = c.benchmark_group("product_with_optional");

    for n in [5, 10, 15, 18] {
        // Naive implementation - limited to smaller sizes
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

        // ZDD implementation
        group.bench_with_input(BenchmarkId::new("zdd", n), &n, |b, &n| {
            b.iter(|| {
                let mut zdd = Zdd::base();
                for i in 0..n {
                    zdd = zdd.product_with_optional(i);
                }
                black_box(zdd.count())
            });
        });
    }

    group.finish();
}

fn bench_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("count");

    for n in [10, 15, 20, 25] {
        // Build ZDD first
        let mut zdd = Zdd::base();
        for i in 0..n {
            zdd = zdd.product_with_optional(i);
        }

        group.bench_with_input(BenchmarkId::new("zdd", n), &zdd, |b, zdd| {
            b.iter(|| black_box(zdd.count()));
        });
    }

    group.finish();
}

fn bench_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains");

    // Build a ZDD with 20 variables
    let mut zdd = Zdd::base();
    for i in 0..20 {
        zdd = zdd.product_with_optional(i);
    }

    // Test different sizes of query sets
    for size in [1, 5, 10, 15, 20] {
        let query: Vec<u32> = (0..size).collect();
        group.bench_with_input(BenchmarkId::new("zdd", size), &query, |b, query| {
            b.iter(|| black_box(zdd.contains(query)));
        });
    }

    group.finish();
}

fn bench_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_first_100");

    for n in [10, 15, 20] {
        let mut zdd = Zdd::base();
        for i in 0..n {
            zdd = zdd.product_with_optional(i);
        }

        group.bench_with_input(BenchmarkId::new("zdd", n), &zdd, |b, zdd| {
            b.iter(|| {
                let first_100: Vec<_> = zdd.iter().take(100).collect();
                black_box(first_100.len())
            });
        });
    }

    group.finish();
}

fn bench_union(c: &mut Criterion) {
    let mut group = c.benchmark_group("union");

    for n in [5, 10, 15] {
        let mut zdd_a = Zdd::base();
        let mut zdd_b = Zdd::base();

        for i in 0..n {
            zdd_a = zdd_a.product_with_optional(i);
        }
        for i in n..(2 * n) {
            zdd_b = zdd_b.product_with_optional(i);
        }

        group.bench_with_input(
            BenchmarkId::new("zdd", n),
            &(zdd_a.clone(), zdd_b.clone()),
            |b, (a, b_zdd)| {
                b.iter(|| black_box(a.union(b_zdd)));
            },
        );
    }

    group.finish();
}

fn bench_node_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_efficiency");

    // Measure how few nodes represent exponential combinations
    for n in [10, 15, 20, 25, 30] {
        group.bench_with_input(BenchmarkId::new("build_and_count", n), &n, |b, &n| {
            b.iter(|| {
                let mut zdd = Zdd::base();
                for i in 0..n {
                    zdd = zdd.product_with_optional(i);
                }
                // Return (count, nodes) to show efficiency
                black_box((zdd.count(), zdd.node_count()))
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_product_with_optional,
    bench_count,
    bench_contains,
    bench_iter,
    bench_union,
    bench_node_efficiency,
);
criterion_main!(benches);
