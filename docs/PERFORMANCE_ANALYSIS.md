# Varpulis vs Apama Performance Analysis

## Benchmark Results (100K events)

### After PERF-04 Optimization (2026-01-31)

| Engine | Throughput | Notes |
|--------|------------|-------|
| Varpulis (before PERF-04) | ~140K evt/s | Rust native |
| Varpulis (after PERF-04) | **~146K evt/s** | +4% improvement |
| Apama | ~163K evt/s | C++ native |

**PERF-04 Results**: Changed `event_sources: HashMap<String, Vec<String>>` to `Arc<[String]>`.
Arc clone is O(1) atomic increment vs O(n) deep copy. Improvement: ~4-6%.

## Analysis: Why Apama Scales Better

### 1. Memory Allocation Patterns

**Varpulis bottlenecks identified:**

- **Stream name Vec cloning** (`engine/mod.rs:1180`):
  ```rust
  let stream_names: Vec<String> = self.event_sources.get(...).cloned().unwrap_or_default();
  ```
  - Clones a Vec<String> for **every event** processed
  - At 100K events, this creates 100K+ temporary Vec allocations

- **Event cloning for joins** (`engine/mod.rs:1279`):
  ```rust
  join_buffer.add_event(&source_name, (*event).clone())
  ```
  - Full deep clone of Event including IndexMap

- **SequenceContext allocation** (`engine/mod.rs:1226`):
  ```rust
  let ctx = SequenceContext::new();
  ```
  - Creates a new context object per merge source check

- **Event file parsing** (`event_file.rs:201-244`):
  ```rust
  fn split_fields(content: &str) -> Vec<String>
  ```
  - Character-by-character iteration with String allocation per field

### 2. Apama Advantages

1. **Batch processing**: Apama's correlator processes events in batches, amortizing per-event overhead
2. **Pre-compiled EPL**: Event patterns are compiled to efficient bytecode
3. **Memory pools**: Apama uses pre-allocated memory pools for events
4. **No Arc overhead**: Native C++ avoids atomic reference counting

### 3. Proposed Optimizations

#### High Impact

1. **Cache stream_names lookup**:
   ```rust
   // Instead of cloning per event, use Arc<[String]>
   event_sources: HashMap<String, Arc<[String]>>
   ```

2. **Object pooling for events**:
   ```rust
   use crossbeam::queue::ArrayQueue;
   static EVENT_POOL: ArrayQueue<Event> = ArrayQueue::new(1024);
   ```

3. **Batch processing mode**:
   - Process events in batches of 100-1000
   - Single allocation per batch, not per event

#### Medium Impact

4. **Pre-allocate SequenceContext**:
   - Use thread-local static context or pool

5. **Optimize event file parsing**:
   - Use memchr for fast field splitting
   - Zero-copy parsing with &str references

6. **SmallVec for small collections**:
   ```rust
   use smallvec::SmallVec;
   // Avoid heap for collections < 8 items
   ```

#### Low Impact

7. **Reduce tracing overhead**:
   - Use conditional compilation for debug traces
   - Batch log writes

### 4. Performance Scaling Characteristics

```
Events    Varpulis   Apama     Ratio
10K       57K/s      24K/s     V 2.3x faster
100K      140K/s     163K/s    A 1.16x faster
```

**Varpulis excels at**: Low latency, small batches (< 50K events)
- Lower per-event startup overhead
- Faster JIT warming for short runs

**Apama excels at**: High throughput, large batches (> 50K events)
- Better amortized overhead at scale
- More aggressive memory pooling

### 5. Next Steps

1. Profile with `perf record` to identify exact hotspots
2. Implement Arc<[String]> caching for stream_names
3. Add batch processing mode to CLI
4. Benchmark after optimizations

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
