# Varpulis vs Apama Performance Analysis

> **For comprehensive benchmark results**, see [`benchmarks-apama-comparison.md`](benchmarks-apama-comparison.md) —
> 7 scenarios, MQTT connector + CLI ramdisk modes, memory + throughput comparisons.

## Current Benchmark Results (2026-02-09)

Measured across 7 CEP scenarios with 100K events, median of 3 runs:

| Mode | Varpulis | Apama | Varpulis Memory | Apama Memory |
|------|----------|-------|-----------------|--------------|
| MQTT Connector | 6–10K/s | 6K/s | 10–57 MB | 85–153 MB |
| CLI Ramdisk | 234–335K/s | 195–221K/s | 36–66 MB | 166–190 MB |

Varpulis wins 4 of 5 CPU-bound scenarios (1.2–1.3x) and uses 2x–16x less memory in every scenario.

### Optimization History

**PERF-04 (2026-01-31)**: Changed `event_sources: HashMap<String, Vec<String>>` to `Arc<[String]>`.
Arc clone is O(1) atomic increment vs O(n) deep copy. Improvement: ~4-6%.

**PERF-05 (2026-02-07)**: Sync processing path, zero-clone preload, batch size increase (1K→10K),
zero-alloc field splitting. Improvement: ~40% (140K→234K evt/s on filter benchmark).

## Optimization Analysis

### Resolved Bottlenecks

The following bottlenecks from the January 2026 analysis have been fixed:

1. **Stream name Vec cloning** — Resolved: Changed to `Arc<[String]>` (PERF-04)
2. **Event file parsing** — Resolved: Zero-alloc `split_fields()` returns `Vec<&str>` (PERF-05)
3. **Batch processing** — Resolved: Batch size 10K in preload mode, sync path (PERF-05)
4. **Event cloning** — Resolved: Zero-clone preload with `into_iter()` + `drain()` (PERF-05)

### Remaining Optimization Opportunities

1. **Object pooling for events**: Pre-allocated pools for high-throughput scenarios
2. **SmallVec for small collections**: Avoid heap for collections < 8 items
3. **SIMD-accelerated field parsing**: Use `memchr` for delimiter scanning

## Test Coverage Status

Current test coverage (COV-01/COV-02 - COMPLETE):

**Total: 752+ tests passing across all crates**

| Module | Tests | Status |
|--------|-------|--------|
| varpulis-cli | 114 tests | ✅ |
| varpulis-core | 57 tests | ✅ |
| varpulis-parser | 77 tests | ✅ |
| varpulis-runtime | 358 tests | ✅ |
| integration tests | 73 tests | ✅ |
| e2e tests | 49 tests | ✅ |

### Runtime Module Coverage

| Module | Tests | Coverage |
|--------|-------|----------|
| engine/mod.rs | ~40 tests | ✅ Comprehensive |
| event.rs | 17 tests | ✅ Good |
| event_file.rs | 26 tests | ✅ Good |
| simulator.rs | 12 tests | ✅ Good |
| sase.rs | ~25 tests | ✅ Good |
| window.rs | ~20 tests | ✅ Good |
| aggregation.rs | ~15 tests | ✅ Good |
| pattern.rs | 40+ tests | ✅ Comprehensive |
| attention.rs | 35+ tests | ✅ Comprehensive |

### Pattern Tests Coverage (40+ tests)
- Event matching with filters (eq, neq, gt, ge, lt, le)
- Logical operators (and, or, xor, not)
- Sequence patterns (followed_by, within)
- PatternContext merging
- Engine state management

### Attention Tests Coverage (35+ tests)
- Numeric transforms (identity, log, normalize, zscore, cyclical, bucketize)
- Categorical embeddings (hash, onehot, lookup)
- SIMD dot product optimization
- Cache operations
- History management
