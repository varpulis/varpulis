# Performance Baselines

Benchmark baselines are tracked automatically via the `benchmark-action/github-action-benchmark` GitHub Action.

## How It Works

1. **On push to `main`**: Benchmarks run and results are stored in the `gh-pages` branch under `dev/bench/`
2. **On pull requests**: Benchmarks run and are compared against the latest `main` baseline
3. **Regression detection**: If any benchmark regresses by >10%, the PR gets a warning comment
4. **PR blocking**: PRs with >10% regression will fail the benchmark check

## Benchmark Suites

| Suite | File | Focus |
|-------|------|-------|
| `pattern_benchmark` | `benches/pattern_benchmark.rs` | SASE+ pattern matching throughput |
| `imperative_benchmark` | `benches/imperative_benchmark.rs` | Imperative block processing |
| `kleene_benchmark` | `benches/kleene_benchmark.rs` | Kleene closure performance |
| `comparison_benchmark` | `benches/comparison_benchmark.rs` | Cross-engine comparison |
| `context_benchmark` | `benches/context_benchmark.rs` | Context/enrichment joins |
| `hamlet_zdd_benchmark` | `benches/hamlet_zdd_benchmark.rs` | Hamlet vs ZDD trend aggregation |
| `pst_forecast_benchmark` | `benches/pst_forecast_benchmark.rs` | PST prediction and PMC forecast |

## Reference Baselines (v0.3.0)

| Benchmark | Throughput/Latency | Hardware |
|-----------|-------------------|----------|
| Simple filter (100K events) | 234K events/sec | GitHub Actions runner |
| SASE sequence (100K events) | 256K events/sec | GitHub Actions runner |
| Kleene+ matching | 97K events/sec | GitHub Actions runner |
| PST prediction (depth 5) | 51 ns | GitHub Actions runner |
| PST full distribution | 105 ns | GitHub Actions runner |
| Hamlet single query | 6.9M events/sec | GitHub Actions runner |
| Hamlet 10 queries | 2.1M events/sec | GitHub Actions runner |
| PMC forecast (1 active run) | 93K events/sec | GitHub Actions runner |

> Note: CI runners have variable performance. Baselines are stored per-commit
> to account for hardware differences. The 10% threshold accommodates normal variance.

## Updating Baselines

Baselines are updated automatically on every push to `main`. To manually reset:

```bash
# Delete the gh-pages benchmark data
git checkout gh-pages
rm -rf dev/bench/
git commit -am "Reset benchmark baselines"
git push origin gh-pages
git checkout main
```

## Running Locally

```bash
# Run all benchmarks
cargo bench -p varpulis-runtime

# Run a specific suite
cargo bench -p varpulis-runtime --bench pattern_benchmark

# Compare against a previous run (Criterion built-in)
cargo bench -p varpulis-runtime -- --baseline main
```
