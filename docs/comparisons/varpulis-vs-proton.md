---
title: Varpulis vs Timeplus Proton
description: Detailed comparison of Varpulis and Timeplus Proton for real-time stream processing. Both are single-binary, no-JVM streaming engines — but Proton is streaming ClickHouse SQL and Varpulis is pattern detection. Code examples, feature matrix, and guidance.
head:
  - - meta
    - name: keywords
      content: timeplus proton alternative, proton stream processing, varpulis vs proton, streaming engine comparison, pattern detection sql, no jvm streaming, single binary streaming, clickhouse stream processing, asof join streaming, match_recognize alternative
---

# Varpulis vs Timeplus Proton

Timeplus Proton and Varpulis are the two most architecturally similar open-source stream processors in 2026: both ship as a **single static binary**, both have **no JVM**, both can be run on a laptop, both connect natively to Kafka and MQTT. They are not competitors so much as **complementary tools that picked different problems to solve well**.

Proton is what you get when you take ClickHouse and add streaming primitives — a vectorised columnar SQL engine for real-time analytics, ETL, and incremental materialised views. Varpulis is what you get when you take a pattern-matching engine and wrap it in a declarative DSL — a stream processor for sequence detection, behavioural rules, and forecasting.

This page is honest about both. Proton's join ergonomics and SQL coverage are excellent; Varpulis's pattern language is in a different category. Picking between them is mostly about **what kind of question you are asking of your event stream**.

## At a Glance

| Dimension | Varpulis | Timeplus Proton |
|-----------|----------|-----------------|
| **Primary focus** | Pattern detection and forecasting | Streaming SQL analytics |
| **Language** | VPL (declarative DSL with SASE+ patterns) | Streaming SQL (ClickHouse dialect) |
| **Runtime** | Native Rust binary | Native C++ binary |
| **Foundation** | Custom engine + SASE+ NFA | Fork of ClickHouse + streaming layer |
| **Deployment** | Single binary, optional cluster | Single binary (OSS), Enterprise cluster |
| **Pattern matching** | Native (Kleene `+/*`, negation, sequences, partition_by) | None — write a JS/Python UDAF |
| **Forecasting** | `.forecast()` built-in (PST + Hawkes) | None |
| **OSS clustering** | Yes (Coordinator + Workers) | No — Enterprise only |
| **External dependency** | None | None |
| **License** | Open source (MIT/Apache-2.0) | Apache-2.0 |

Both engines are actively maintained. Proton released v3.0.19 in March 2026 with a roughly two-week release cadence. Varpulis is on its own active release schedule with no JVM, no Postgres, and no external metadata store.

## Code Comparison

Both engines are at their best when used for the workloads they were built for. The interesting question is what each one looks like outside its sweet spot.

### Workload 1: Tumbling 1-minute aggregation per device (Proton's home turf)

A device emits temperature readings to a Kafka topic. Compute sum/avg/min/max per device per minute, write to another Kafka topic. This is exactly the kind of workload Proton was designed for, and the SQL is beautifully concise.

**Timeplus Proton — streaming SQL**

```sql
CREATE EXTERNAL STREAM devices_in (
    device_id string,
    temperature float,
    ts datetime64(3)
) SETTINGS type='kafka', brokers='kafka:9092', topic='devices';

CREATE EXTERNAL STREAM devices_out (
    window_start datetime64(3),
    device_id string,
    s float, a float, mn float, mx float
) SETTINGS type='kafka', brokers='kafka:9092', topic='devices_agg';

CREATE MATERIALIZED VIEW mv_dev_agg INTO devices_out AS
SELECT window_start, device_id,
       sum(temperature) AS s, avg(temperature) AS a,
       min(temperature) AS mn, max(temperature) AS mx
FROM tumble(devices_in, ts, 1m)
GROUP BY window_start, device_id;
```

**Varpulis — VPL**

```vpl
event Reading:
    device_id: str
    temperature: float

connector KafkaIn = kafka(brokers: "kafka:9092", topic: "devices")
connector KafkaOut = kafka(brokers: "kafka:9092", topic: "devices_agg")

stream DeviceAgg = Reading
    .from(KafkaIn)
    .partition_by(device_id)
    .window(tumbling: 1m)
    .aggregate(
        s: sum(temperature),
        a: avg(temperature),
        mn: min(temperature),
        mx: max(temperature)
    )
    .to(KafkaOut)
```

Both are about the same length and equally readable. **Proton's syntax is a hair tighter** here because materialised views and external streams are first-class. If the rest of your team already speaks SQL, Proton wins this round on familiarity alone. Varpulis's `partition_by` + chained operators read more like a pipeline, which some teams find clearer for debugging.

For pure SQL analytics like this, **either choice is good**. Pick by team preference.

### Workload 2: Trade enriched with the latest Quote within 5 seconds

This is the classic ASOF / range join: for each Trade, find the most-recent Quote for the same symbol that arrived within 5 seconds.

**Timeplus Proton — has native `date_diff_within` and ASOF**

```sql
SELECT
    t.symbol,
    t.price AS trade_price,
    q.bid,
    q.ask,
    t.ts
FROM trades AS t
INNER JOIN quotes AS q
  ON t.symbol = q.symbol
  AND date_diff_within(5s, q.ts, t.ts);
```

Or with `ASOF JOIN`:

```sql
SELECT t.symbol, t.price, q.bid, q.ask
FROM trades AS t
ASOF LEFT JOIN quotes AS q
  ON t.symbol = q.symbol AND t.ts >= q.ts;
```

**Varpulis — left join with temporal correlation**

```vpl
event Trade:
    symbol: str
    price: float

event Quote:
    symbol: str
    bid: float
    ask: float

stream EnrichedTrade = Trade as t
    .left_join(Quote as q, on: t.symbol == q.symbol, within: 5s)
    .emit(symbol: t.symbol, trade_price: t.price, bid: q.bid, ask: q.ask)
```

This is the one workload where **Proton has the ergonomic edge**. Its `ASOF JOIN`, `date_diff_within`, and `LATEST JOIN` primitives — inherited from the ClickHouse heritage — are purpose-built for "enrich A with most-recent-B" patterns. Varpulis can do the join, but the temporal-window-style left join is one operator where Proton's SQL is flat-out cleaner.

If "ASOF join" is a weekly query for your team, that's a real reason to keep Proton in the toolbox.

### Workload 3: Login → Password Change → 3+ Transfers → Logout, within 5 minutes, total > $10K

This is where the two engines diverge sharply. The workload requires:

- **Ordered sequence** (login then password change then transfers then logout, in that order)
- **Kleene closure** (3 or more transfers)
- **Aggregation over the matched run** (sum of transfer amounts)
- **Per-user partitioning**
- **Temporal window** (5 minutes from start to logout)

**Varpulis — VPL**

```vpl
event Login:
    user_id: str
    ip: str

event PasswordChange:
    user_id: str

event Transfer:
    user_id: str
    amount: float

event Logout:
    user_id: str

stream SuspiciousSession = Login as login
    -> PasswordChange where user_id == login.user_id
    -> all Transfer where user_id == login.user_id as txs
    -> Logout where user_id == login.user_id
    .within(5m)
    .partition_by(login.user_id)
    .trend_aggregate(
        total: sum_trends(txs.amount),
        count: count_events(txs)
    )
    .where(count >= 3 and total > 10000)
    .emit(
        user_id: login.user_id,
        total: total,
        transfer_count: count
    )
```

**Twenty-eight lines.** The `->` operator expresses the ordered sequence, `all Transfer` is a Kleene plus that captures every matching transfer (not just one), `partition_by` keeps state isolated per user, `within(5m)` bounds the entire pattern, and `trend_aggregate` runs the sum across all matched transfers in O(n) using Hamlet shared-state.

**Timeplus Proton — JavaScript UDAF (the official recipe)**

Proton has no `MATCH_RECOGNIZE`, no sequence operator, no pattern DSL, and no Kleene closure. The [official Timeplus blog post](https://www.timeplus.com/post/cep-with-streaming-sql-udf) on "Complex Event Processing Made Easy with Streaming SQL + UDF" recommends building a finite-state machine inside a JavaScript UDAF. The implementation looks like this:

```sql
CREATE OR REPLACE AGGREGATE FUNCTION suspicious_session
    (ts datetime64(3), user_id string, event string, amount float)
RETURNS string LANGUAGE JAVASCRIPT AS $${
  has_customized_emit: true,
  initialize: function () {
    this.users = {};
    this.hits = [];
  },
  process: function (Ts, U, E, A) {
    for (let i = 0; i < Ts.length; i++) {
      const u = U[i];
      const s = this.users[u] || {state: 0, transfers: 0, total: 0, start: null};

      if (s.state === 0 && E[i] === 'login') {
        s.state = 1;
        s.start = Ts[i];
      } else if (s.state === 1 && E[i] === 'passwd_change') {
        s.state = 2;
      } else if (s.state === 2 && E[i] === 'transfer') {
        s.transfers++;
        s.total += A[i];
        if (s.transfers >= 3) s.state = 3;
      } else if (s.state === 3 && E[i] === 'logout') {
        if (Ts[i] - s.start <= 300000 && s.total > 10000) {
          this.hits.push({user: u, total: s.total, transfers: s.transfers});
        }
        delete this.users[u];
      }

      // Window expiry
      if (s.start !== null && Ts[i] - s.start > 300000) {
        delete this.users[u];
        continue;
      }
      this.users[u] = s;
    }
  },
  finalize: function () { return JSON.stringify(this.hits); }
}$$;

SELECT suspicious_session(ts, user_id, event, amount) FROM auth_events;
```

You write the state machine, the partitioning, the window management, the aggregation, and the cleanup yourself. **About 45 lines of JavaScript**, executed by V8 single-threaded per aggregation group. Add a second pattern (say, the same chain but with a card-not-present transfer) and you write a second UDAF — there's no rule library, no precondition sharing, no multi-pattern optimisation.

This is not a Proton flaw — Proton was never built to be a CEP engine. It's the correct comparison: **for pattern detection, Varpulis has a DSL and Proton has an escape hatch.** Choose accordingly.

### Workload 4: MITRE ATT&CK kill chain (cmd.exe → powershell with parent_pid match → network connect to 445/139, within 10 min, partition by host)

This is the security analogue of Workload 3, with the extra twist of **cross-event field correlation** (the powershell event's `parent_pid` must equal the cmd.exe event's `pid`).

**Varpulis — VPL**

```vpl
pattern PsExecKillChain =
    ProcessCreate where image contains "cmd.exe" as cmd ->
    ProcessCreate where image contains "powershell.exe"
                  and parent_pid == cmd.pid as ps ->
    NetworkConnect where dest_port in [445, 139] and host == cmd.host as net
    within 10m
    partition by host

stream APT = use pattern PsExecKillChain
    .emit(
        alert_type: "PSEXEC_KILL_CHAIN",
        host: cmd.host,
        cmd_pid: cmd.pid,
        ps_pid: ps.pid,
        target_port: net.dest_port,
        technique: "T1021.002"
    )
```

The pattern reads top to bottom: cmd.exe spawns powershell whose `parent_pid` matches the captured cmd's `pid`, followed by a network connection to SMB ports on the same host, all within 10 minutes. `partition by host` keeps per-host state isolated.

**Timeplus Proton — same JavaScript UDAF approach as Workload 3**

Per-host hashmap of recent cmd.exe PIDs, lookup on each powershell event, lookup on each network connect, hand-rolled window cleanup. Easily 60-100 lines of JS. Same shape as Workload 3, just a different state machine. Proton's blog post is honest about this: SQL was not built for this and they recommend hand-coded FSMs.

For security teams writing detection rules, **the line-count and maintenance gap matters**. Sigma rules, MITRE ATT&CK techniques, fraud playbooks — these are pattern catalogs that grow over time. Maintaining 200 of them in VPL is a different proposition from maintaining 200 of them as JavaScript UDAFs in materialised views.

## Architecture Differences

### Varpulis

- **Single Rust binary, ~15 MB.** No JVM, no Postgres, no external metadata store. Run from a laptop or as a container, scale out via the built-in Coordinator/Workers cluster mode.
- **SASE+ NFA pattern engine.** Sequences, Kleene closures, negation, partition_by, and temporal windows are first-class — no UDFs required. Multi-query optimisation via Hamlet graphlet sharing (SIGMOD 2021) when running many concurrent patterns.
- **Forecasting built in.** `.forecast()` uses Probabilistic Suffix Trees + Hawkes process intensity to predict pattern completion before the final event arrives. Unique among open-source streaming engines.
- **Connectors as crates.** Build a minimal binary with `--features mqtt,kafka` or include all 20+ connectors in the default release.
- **State backends**: in-memory, RocksDB, S3 (object-storage checkpoints with optional zstd compression).
- **Async checkpoint barriers** (Chandy-Lamport), exactly-once sink delivery (Kafka 2PC), dynamic rescaling.

### Timeplus Proton

- **Single C++ binary, ~500 MB.** Built on top of a fork of ClickHouse, inheriting its vectorised columnar execution and 1000+ scalar/aggregate SQL functions. No JVM, no ZooKeeper.
- **ClickHouse SQL with streaming extensions.** `tumble`, `hop`, `session`, watermarks, and the same SQL surface ClickHouse users already know. The `table(stream_name)` function lets you query the historical buffer of any stream as a regular ClickHouse table.
- **Excellent join ergonomics.** `ASOF JOIN`, `LATEST JOIN`, range joins via `date_diff_within`, lookup joins against MySQL/Postgres dictionaries.
- **No native pattern detection.** No `MATCH_RECOGNIZE`, no sequence operator. Hand-rolled FSMs in JavaScript or Python UDAFs are the official recipe.
- **OSS clustering is not supported.** Multi-node deployment with the NativeLog Raft-based distributed WAL is reserved for Timeplus Enterprise.
- **State storage via the underlying MergeTree engine** — no separate RocksDB or S3 checkpoint layer in OSS.

## Feature Comparison

| Feature | Varpulis | Timeplus Proton |
|---------|----------|-----------------|
| **Sequence detection (`A → B → C`)** | Native operator | JS/Python UDAF |
| **Kleene closure (`+/*`)** | Native operator | JS/Python UDAF |
| **Negation (`not B between A and C`)** | Native operator | UDAF or self-anti-join |
| **Pattern forecasting** | `.forecast()` built-in | Not available |
| **Cross-event correlation (`B.x == A.y`)** | Native (`as` aliases) | UDAF or self-join |
| **Per-key pattern partitioning** | `partition_by` operator | UDAF or `SHUFFLE BY` |
| **Multi-query optimisation** | Hamlet graphlet sharing | None |
| **Tumbling / hopping / session windows** | Yes | Yes |
| **ASOF join** | Via left join + within | **Native `ASOF JOIN`** |
| **Range / temporal join** | Via `within` clause | **Native `date_diff_within`** |
| **Lookup / enrichment joins** | `.enrich()` operator | Dictionary joins |
| **SQL surface** | VPL is purpose-built; SQL is not the goal | **1000+ ClickHouse functions** |
| **Stateful UDFs** | VPL functions, ONNX scoring, WASM UDFs | **JS, Python, SQL, HTTP UDFs** |
| **Watermarks** | Yes | Yes |
| **Exactly-once sink delivery** | Kafka 2PC | Not documented in OSS |
| **OSS clustering** | Yes | **Enterprise only** |
| **Hot historical replay** | Per stream | **`table(stream)` is first-class** |
| **Vendor independence** | Single-vendor open source | OSS + Timeplus Enterprise/Cloud |

## Performance

We ran our own head-to-head benchmark of both engines on **identical workloads** with **identical event payloads** (100,000 events per scenario, 5 runs each, median reported). The benchmark suite is reproducible from the [varpulis repository](https://github.com/varpulis/varpulis) under `benchmarks/proton-comparison/` and the methodology is documented inline.

**Test setup**:
- Hardware: Ryzen 9 7950X / 32 GB DDR5 / NVMe SSD
- Varpulis: v0.10.x release build, single core, file-based input via `varpulis simulate --workers 1 --quiet`
- Proton: v3.0.19 in Docker, single container, INSERT FROM JSONEachRow over `docker exec` stdin, output measured via materialized-view propagation to a destination stream
- Both engines see exactly the same event payloads with the same field values
- Memory: peak resident-set-size during the run (Varpulis via `/proc/{pid}/status`, Proton via `docker stats`)
- Output count is verified for correctness across both engines

### Scenario 1 — Filter (price > 50)

```vpl
# Varpulis
stream Filtered = Tick
    .where(price > 50.0)
    .emit(symbol: symbol, price: price, volume: volume)
```

```sql
-- Proton
CREATE MATERIALIZED VIEW mv_filter INTO ticks_filtered AS
SELECT symbol, price, volume FROM ticks WHERE price > 50.0;
```

| Engine | Throughput | Peak RSS | Output |
|---|---|---|---|
| **Varpulis** | **174,139 events/sec** | 96 MB | 89,000 ✓ |
| Proton | 41,612 events/sec | 350 MB | 89,000 ✓ |
| **Varpulis advantage** | **4.18×** | **3.6× less memory** | identical correctness |

### Scenario 2 — Tumbling 1-second windowed aggregation per device (100 partitions)

```vpl
# Varpulis
stream DeviceAgg = Reading
    .partition_by(device_id)
    .window(1s)
    .aggregate(
        s: sum(temperature), a: avg(temperature),
        mn: min(temperature), mx: max(temperature)
    )
    .emit(device_id: device_id, s: s, a: a, mn: mn, mx: mx)
```

```sql
-- Proton
CREATE MATERIALIZED VIEW mv_agg INTO device_agg AS
SELECT window_start AS win_start, device_id,
       sum(temperature) AS s, avg(temperature) AS a,
       min(temperature) AS mn, max(temperature) AS mx
FROM tumble(readings, to_datetime64(ts/1000.0, 3), 1s)
GROUP BY window_start, device_id;
```

| Engine | Throughput | Peak RSS | Output |
|---|---|---|---|
| **Varpulis** | **124,626 events/sec** | 118 MB | 99,900 ✓ |
| Proton | 40,135 events/sec | 348 MB | 99,900 ✓ |
| **Varpulis advantage** | **3.11×** | **2.95× less memory** | identical correctness |

### What the numbers tell us

Both engines deliver correct results — the output counts are identical across the two systems, which is the necessary precondition for any throughput comparison to be meaningful.

On these two scenarios, **Varpulis is 3-4× faster than Proton with ~3× less memory**. The gap is wider than the architectural difference between the two engines would predict, and the most likely explanations are:

1. **Proton's INSERT + materialized view path has more layers** — incoming events go through INSERT parsing → MergeTree write → MV trigger → vectorised computation → output stream write. Varpulis runs the entire pipeline as one in-process loop with no intermediate persistence.
2. **Proton's container adds overhead** — `docker exec` stdin piping adds latency that the native Varpulis binary doesn't pay. A bare-metal Proton install would close some of this gap.
3. **The benchmark's wait-for-completion polling** is identical for both engines, so it does not advantage one over the other, but it does add a small uniform noise floor (≤5ms) to both numbers.

**What we did NOT measure**:

- **Stream-stream joins** (Scenario 3 in the suite): the join semantics across the two engines are not yet apples-to-apples. Proton has native `ASOF JOIN` and `date_diff_within` (mentioned earlier as Proton's ergonomic edge); Varpulis's `join()` requires explicit upstream stream definitions. Comparing throughput before normalising the semantics would be misleading.
- **Multi-stage pipelines** (filter → window → having → emit): planned, not yet wired up.
- **Native pattern detection workloads** (sequence, Kleene closure, forecasting): Proton has no native implementation, so the only comparison would be Varpulis's NFA vs a hand-coded JavaScript UDAF — which is not an engine-vs-engine measurement, it's "Varpulis vs whoever wrote the UDAF".

If you want to reproduce these numbers, the benchmark scripts are at [`benchmarks/proton-comparison/`](https://github.com/varpulis/varpulis/tree/main/benchmarks/proton-comparison) — `python3 run_benchmark.py --scenario all` runs everything.

### Published vendor numbers (for context)

- **Timeplus Proton**: claims "up to 90M events/sec" on a single MacBook Pro M2 Max in their README. No event size, schema, or query complexity disclosed — this is a single-node microbenchmark, not a sustained-load test.
- **Varpulis**: 1.5M events/sec for SASE+ pattern matching on a single core, 410K events/sec for a full filter+aggregate+emit pipeline, 950K events/sec for 50 concurrent Hamlet-shared patterns, 51 ns per PST single-symbol forecast prediction. Measured on a Ryzen 9 7950X with full methodology in [docs/PERFORMANCE_ANALYSIS.md](../PERFORMANCE_ANALYSIS.md).

For workloads only one engine can express — sequence detection, Kleene closure, forecasting — comparing throughput is meaningless because the JavaScript-UDAF version is in a different operational class than a native NFA.

## When to Use Timeplus Proton

- You want **streaming SQL with the full ClickHouse function library** — string functions, JSON, math, statistics, geo, regex.
- Your queries are mostly **windowed aggregations, group-bys, and incremental materialised views** over Kafka or MQTT topics.
- You need **ASOF / range joins** for "enrich A with most-recent B" workloads (financial market data, sensor enrichment, telemetry correlation).
- Your team already runs ClickHouse and you want a streaming companion that speaks the same SQL.
- You want to **query historical and live data with the same SQL** (Proton's `table(stream)` makes this seamless).
- You are building an **observability or analytics product** where streaming SQL is the user interface, not pattern detection.

## When to Use Varpulis

- You need **sequence detection** — patterns where order matters (`A → B → C`).
- You need **Kleene closure** with exhaustive matching — capture *every* matching event in a window, not just the longest run.
- You need **per-key partitioning of patterns** with shared state across the matched events.
- You need **forecasting** — predict that a pattern is about to complete before the final event arrives.
- You need **multi-query optimisation** when running many concurrent patterns over the same stream (Hamlet graphlet sharing).
- You are doing **detection engineering** — Sigma rules, MITRE ATT&CK kill chains, fraud playbooks, behavioural rules — and you want a DSL designed for that.
- You want **OSS clustering and dynamic rescaling without paying for an Enterprise tier**.

## Using Both Together

These engines are **complementary, not competing**. A common architecture splits responsibilities by query type:

```
┌─────────┐    ┌──────────────┐    ┌──────────┐
│  Kafka  │───▶│   Varpulis   │───▶│  alerts  │
│ events  │    │  (patterns,  │    │   topic  │
│         │    │  forecasts)  │    └──────────┘
│         │    └──────────────┘
│         │
│         │    ┌──────────────┐    ┌──────────┐
│         │───▶│    Proton    │───▶│ metrics  │
│         │    │ (analytics,  │    │   topic  │
└─────────┘    │ aggregates)  │    └──────────┘
               └──────────────┘
```

Run Varpulis for behavioural detection and forecasting; run Proton for analytical SQL over the same Kafka topic. Both subscribe independently, both write to their own output topics, neither has any dependency on the other. Operators get a metrics dashboard from Proton's materialised views; security gets pattern alerts from Varpulis. **One Kafka topic, two stream processors, two different jobs.**

If your team has the operational budget for a single streaming engine and the workload is mostly aggregation, Proton is the right choice. If pattern detection is on the roadmap or already a pain, Varpulis is the only open-source engine that handles it without UDF gymnastics.

## Summary

Proton is the strongest pure SQL streaming engine in the no-JVM tier today. Varpulis is the strongest pattern-matching engine in the no-JVM tier today. They occupy different niches in the same architectural neighbourhood, and the right answer depends on the question you are asking of your event stream — not on which one is "better".

If you came looking for "how do I do MATCH_RECOGNIZE in Proton", the honest answer is: **you don't, you write a UDAF, or you use Varpulis**. If you came looking for "how do I run materialised views over a Kafka topic with proper ASOF joins", the honest answer is: **Proton was built for that**.
