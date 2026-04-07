---
title: Varpulis vs Arroyo
description: Detailed comparison of Varpulis and Arroyo for stream processing. Both are Rust-native streaming engines, but Arroyo is now part of Cloudflare Pipelines while Varpulis remains an independent open-source project with native pattern detection. Code examples, feature matrix, post-acquisition guidance.
head:
  - - meta
    - name: keywords
      content: arroyo alternative, arroyo stream processing, varpulis vs arroyo, self-hosted streaming engine, rust streaming, sql stream processing, cloudflare pipelines alternative, datafusion streaming, match_recognize alternative
---

# Varpulis vs Arroyo

Arroyo and Varpulis are the two best-known **Rust-native open-source stream processors** in 2026. Both ship as a single binary, both compile to native code with no JVM, both handle Kafka and MQTT natively. They also differ in two important ways: their query model (Arroyo is streaming SQL on Apache Arrow + DataFusion; Varpulis is a pattern-matching DSL with forecasting), and their **maintenance posture** (Arroyo was acquired by Cloudflare in April 2025 and now powers Cloudflare Pipelines; Varpulis is an independent open-source project).

This page is honest about both. Arroyo's SQL engine is excellent, and the team that built it is now building one of the cloud's most ambitious data platforms. Varpulis is the right choice when you need pattern detection that Arroyo doesn't have, when you need a project whose roadmap is governed entirely in the open, or when you want a streaming engine without an external Postgres dependency.

## At a Glance

| Dimension | Varpulis | Arroyo |
|-----------|----------|--------|
| **Primary focus** | Pattern detection and forecasting | Streaming SQL ETL and analytics |
| **Language** | VPL (declarative DSL with SASE+ patterns) | Streaming SQL (DataFusion dialect) |
| **Runtime** | Native Rust binary | Native Rust binary |
| **Foundation** | Custom engine + SASE+ NFA | Apache Arrow + Apache DataFusion |
| **Pattern matching** | Native (Kleene `+/*`, negation, sequences, partition_by) | None — not in docs or roadmap |
| **Forecasting** | `.forecast()` built-in (PST + Hawkes) | None |
| **Deployment** | Single binary, optional cluster | Single binary, optional cluster |
| **External dependency** | None | **Postgres required** for control plane |
| **OSS clustering** | Yes (Coordinator + Workers) | Yes |
| **Maintenance posture** | Independent open source | Acquired by Cloudflare; engine still OSS |
| **Latest release** | Active release schedule | v0.15.0 (Dec 2025) |
| **License** | MIT/Apache-2.0 | Apache-2.0 OR MIT |

## The Cloudflare Acquisition Matters

Before the technical comparison, this needs to be addressed honestly because it affects every "should I pick Arroyo?" decision in 2026.

In April 2025 [Cloudflare acquired the Arroyo team](https://blog.cloudflare.com/cloudflare-acquires-arroyo-pipelines-streaming-ingestion-beta/) to power **Cloudflare Pipelines** — a serverless streaming-ingestion service that takes HTTP/Worker events, runs SQL transforms, and lands them in R2 as Iceberg, Parquet, or JSON. The Arroyo team committed in their [farewell blog post](https://www.arroyo.dev/blog/arroyo-is-joining-cloudflare/) that the engine would "remain fully open-source and self-hostable" and that they would "continue contributing fixes and features to Arroyo open source."

A year later, here's what the data shows:

- **The OSS engine is alive but slow.** The last tagged release is **v0.15.0 from December 2025** — over four months at the time of writing. Master branch sees commits, but recent commit messages ("Introduce worker job controller mode", "Refactor out common RPC components", "Iceberg error mapping") read like infrastructure plumbing for the Cloudflare integration rather than end-user feature work.
- **Cloudflare Pipelines is in open beta** and currently does **stateless transformations only** — no aggregations, joins, or windows yet. Cloudflare's announcement says aggregations and incrementally-updated materialized views are "planned to follow in the first half of 2026."
- **The new-feature engine for the Arroyo team is now Cloudflare's.** A user picking Arroyo OSS in 2026 needs to understand that the maintainers' day job is building a managed service on top of it. Whether community velocity on the OSS engine will hold up long term is the unanswered question.

This is **not** an Arroyo-is-dying take. The engine is solid, the codebase is high quality, the Apache 2.0 / MIT licensing protects you against rugpulls, and Cloudflare is a credible long-term steward. It is, however, a different deal than picking an independent open-source project with no commercial overlay.

If you specifically want the OSS streaming engine without depending on a vendor's roadmap, **Varpulis is the alternative**. If you want a hosted managed service with serverless ingestion and don't want to run a worker pool, **Cloudflare Pipelines (when it adds windowing) is the right choice**.

## Code Comparison

### Workload 1: Tumbling 1-minute aggregation per device (Arroyo's home turf)

A device emits temperature readings to a Kafka topic. Compute sum/avg/min/max per device per minute, write to another Kafka topic.

**Arroyo — streaming SQL**

```sql
CREATE TABLE devices_in (
    device_id TEXT,
    temperature DOUBLE,
    ts TIMESTAMP,
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    type = 'source',
    bootstrap_servers = 'kafka:9092',
    topic = 'devices',
    format = 'json'
);

CREATE TABLE devices_out (
    window_start TIMESTAMP, device_id TEXT,
    s DOUBLE, a DOUBLE, mn DOUBLE, mx DOUBLE
) WITH (
    connector = 'kafka',
    type = 'sink',
    bootstrap_servers = 'kafka:9092',
    topic = 'devices_agg',
    format = 'json'
);

INSERT INTO devices_out
SELECT
    window.start AS window_start,
    device_id,
    sum(temperature),
    avg(temperature),
    min(temperature),
    max(temperature)
FROM (
    SELECT tumble(interval '1 minute') AS window, device_id, temperature
    FROM devices_in
)
GROUP BY window, device_id;
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

Both are clean. **Arroyo's WATERMARK declaration is a real feature** — first-class watermarks in DDL are useful when your event-time and processing-time skew matters. Varpulis's chained-operator pipeline is shorter and easier to scan, but doesn't expose watermark configuration in this example (it's available via `.watermark(delay: 5s)` if needed).

For pure SQL analytics, **either choice is good**. Pick by whether your team prefers SQL (Arroyo) or a method-chained DSL (Varpulis).

### Workload 2: Stream-stream join with windowed correlation

For each Trade, find matching Quotes within the same 5-second window.

**Arroyo — windowed join (windows must be identical on both sides)**

```sql
SELECT
    t.symbol,
    t.price AS trade_price,
    q.bid,
    q.ask
FROM (
    SELECT tumble(interval '5 seconds') AS w, symbol, price, ts
    FROM trades
) t
INNER JOIN (
    SELECT tumble(interval '5 seconds') AS w, symbol, bid, ask
    FROM quotes
) q
ON t.w = q.w AND t.symbol = q.symbol;
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

Both engines handle this, but **Arroyo's documented restriction is that windowed joins require identical window definitions on both sides** — you cannot directly express "the most-recent quote up to and including this trade's timestamp" with a windowed join. The closest workaround is an updating join with a TTL, which produces a changelog stream and has different semantics. Varpulis's `left_join ... within` matches each Trade with the Quote that fell inside the lookback window without forcing both streams into the same tumble.

If you need true ASOF / range-join semantics, **Arroyo's SQL surface is the limiting factor here** — it's a known DataFusion-streaming gap.

### Workload 3: Login → Password Change → 3+ Transfers → Logout, within 5 minutes, total > $10K

The same fraud-detection workload from the Proton comparison. It needs ordered sequence, Kleene closure, per-user partition, temporal window, and aggregation over the matched run.

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

**Arroyo — there is no SQL way to do this**

Arroyo has **no `MATCH_RECOGNIZE`, no sequence operator, no pattern DSL, no Kleene closure**. It is not in the docs and not in the roadmap. The Arroyo SQL reference covers SELECT, DDL, Windows, Joins, Updating Tables, scalar/aggregate/window functions — and that's it.

You have two options:

1. **Write a Rust UDAF.** Arroyo's UDF story is genuinely strong — it supports first-class Rust UDFs (sync and async) compiled as plugins via the `arroyo_udf_plugin` crate. You would write a stateful aggregator that maintains a per-user FSM, tracks events through the four-state sequence (login → password change → transfers → logout), and emits when a complete match satisfies the constraints. Probably 80-150 lines of Rust, but no documented tutorial covers this exact pattern in the Arroyo docs. You're on your own.

2. **Approximate with an updating join + count filter.** You can express *"these four event types happened for this user within a 5-minute window"* using updating tables, but **you cannot express the ordering** (login *before* password change *before* transfers *before* logout). Without ordering, the query is wrong — it will fire on any user who happens to have all four event types in the window, regardless of sequence. For real fraud detection, this is a non-starter.

Honest verdict: Arroyo can do the data-flow plumbing for this workload (windowing, partitioning, joining), but it cannot express the *pattern* without dropping into Rust. **For sequence detection, Varpulis is in a different category, not just a different syntax.**

### Workload 4: MITRE ATT&CK kill chain (cmd.exe → powershell with parent_pid match → network connect within 10 min, partition by host)

The security-flavored version of Workload 3, with the extra wrinkle of cross-event field correlation (powershell's `parent_pid` must equal the captured cmd.exe's `pid`).

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

**Arroyo — same Rust UDAF problem as Workload 3, plus a self-join headache**

The cross-event correlation (`parent_pid == cmd.pid`) is theoretically expressible as a three-way windowed self-join over the same event stream, but:

- All three sub-queries must wrap the source in the same tumble (loses correctness at window boundaries)
- Or you use updating joins (correct but unbounded state until TTL, and you still cannot enforce ordering)
- A correct implementation requires a Rust UDAF

For a security team maintaining a catalog of MITRE ATT&CK detection rules, **the practical difference is that Varpulis lets you write the rule and Arroyo makes you write a Rust plugin per rule**. For one or two rules, the Rust path is fine. For 200 rules, it's an operational dead end.

## Architecture Differences

### Varpulis

- **Single Rust binary, ~15 MB.** No JVM, no Postgres, no external metadata store. Run from a laptop or as a container, scale out via the built-in Coordinator/Workers cluster mode.
- **SASE+ NFA pattern engine.** Sequences, Kleene closures, negation, partition_by, and temporal windows are first-class. Multi-query optimisation via Hamlet graphlet sharing (SIGMOD 2021) when running many concurrent patterns.
- **Forecasting built in.** `.forecast()` uses Probabilistic Suffix Trees + Hawkes process intensity to predict pattern completion before the final event arrives.
- **Hybrid row/columnar execution.** Apache Arrow for vectorised aggregation paths, row-oriented for SASE+ pattern matching. The engine picks per operator based on batch size.
- **State backends**: in-memory, RocksDB, S3 (object-storage checkpoints with optional zstd compression).
- **Async checkpoint barriers** (Chandy-Lamport), exactly-once sink delivery (Kafka 2PC), dynamic rescaling.
- **Independent open source** with no managed-service vendor overlay.

### Arroyo

- **Single Rust binary** that can run all roles (controller, API server, compiler, worker) for development or split into separate processes for distributed production. ~85% Rust.
- **Apache Arrow + Apache DataFusion foundation** since the v0.10 rebuild (March 2024). Self-reported 3× throughput improvement, 20× faster pipeline startup, and 11× smaller Docker image vs the pre-v0.10 custom engine.
- **First-class Rust UDFs** — sync and async, compiled as plugins. This is genuinely unique among streaming engines and is Arroyo's strongest UDF story.
- **Watermarks declared in CREATE TABLE DDL** — `WATERMARK FOR col AS col - INTERVAL ...`. Cleaner event-time handling than most streaming SQL engines.
- **State backends**: S3, Cloudflare R2, local filesystem. Checkpoints are written incrementally via a Chandy-Lamport-style barrier protocol.
- **Requires an external Postgres** for control-plane metadata. There's no embedded option in the OSS engine — you must run a Postgres alongside.
- **Cloudflare-owned**: the team that develops the engine works at Cloudflare on Cloudflare Pipelines. The OSS engine is still released under MIT/Apache-2.0 and can be self-hosted, but feature velocity has decelerated since the acquisition.

## Feature Comparison

| Feature | Varpulis | Arroyo |
|---------|----------|--------|
| **Sequence detection (`A → B → C`)** | Native operator | Rust UDAF |
| **Kleene closure (`+/*`)** | Native operator | Rust UDAF |
| **Negation (`not B between A and C`)** | Native operator | Rust UDAF |
| **Pattern forecasting** | `.forecast()` built-in | Not available |
| **Cross-event correlation (`B.x == A.y`)** | Native (`as` aliases) | Multi-stage SQL or UDAF |
| **Per-key pattern partitioning** | `partition_by` operator | Rust UDAF |
| **Multi-query optimisation** | Hamlet graphlet sharing | None |
| **Tumbling / hopping / session windows** | Yes | Yes |
| **First-class watermark DDL** | Via `.watermark()` operator | **Yes (`WATERMARK FOR ...`)** |
| **Stream-stream windowed joins** | Yes | Yes (identical windows required) |
| **ASOF / range joins** | Via `left_join ... within` | Not available (use updating joins) |
| **Updating joins (changelog)** | Via `.merge()` | **First-class** |
| **Lookup / enrichment joins** | `.enrich()` operator | Redis lookup join |
| **Rust UDFs** | WASM-sandboxed UDFs | **Native Rust UDFs (sync + async)** |
| **Python UDFs** | Coming | Yes |
| **Exactly-once sink delivery** | Kafka 2PC | Yes (Kafka) |
| **State backend** | Memory / RocksDB / S3 | S3 / R2 / local |
| **External Postgres dependency** | None | **Required for control plane** |
| **Iceberg sink** | Via S3 connector | **Yes (v0.15.0, with R2 Data Catalog)** |
| **OSS clustering** | Yes | Yes |
| **Vendor independence** | Independent open source | **Cloudflare-owned (engine still OSS)** |

## Performance

We ran a head-to-head benchmark of Arroyo and Varpulis on Scenario 1 (filter
events where price > 50). The benchmark suite is reproducible from the
[varpulis repository](https://github.com/varpulis/varpulis) under
[`benchmarks/arroyo-comparison/`](https://github.com/varpulis/varpulis/tree/main/benchmarks/arroyo-comparison).

**Test setup**:
- Hardware: Ryzen 9 7950X / 32 GB DDR5 / NVMe SSD
- Arroyo: v0.15.0 in Docker, single-node `cluster` mode, Postgres + Redpanda
  alongside, Kafka source + Kafka sink, **5 runs, median reported**
- Varpulis: v0.10.x release build, single core, file-mode JSONL ingestion
  (its native fast path), **5 runs, median reported** (numbers from the
  [Proton comparison page](varpulis-vs-proton.md))
- Both engines see the same 100,000 events with the same field values.
- Output count is verified for correctness across both engines (89,000).

**Methodology note** — different input paths: Arroyo runs on its
production-recommended Kafka source (events pre-loaded into a Redpanda topic)
because that's the most documented and stable input path in v0.15.0. Varpulis
runs on file-mode JSONL ingestion via `varpulis simulate`, which is its
native high-throughput path. **Each engine is measured at its native fast
path.** A like-for-like Kafka comparison would require rebuilding Varpulis
with `--features kafka` and is on the follow-up list.

### Scenario 1 — Filter (price > 50)

```sql
-- Arroyo
CREATE TABLE ticks (ts BIGINT, symbol TEXT, price DOUBLE, volume BIGINT)
WITH (connector='kafka', type='source', bootstrap_servers='redpanda:9092',
      topic='scenario-01-filter', format='json', 'source.offset'='earliest');

CREATE TABLE ticks_filtered (symbol TEXT, price DOUBLE, volume BIGINT)
WITH (connector='kafka', type='sink', bootstrap_servers='redpanda:9092',
      topic='scenario-01-filter-out', format='json');

INSERT INTO ticks_filtered
SELECT symbol, price, volume FROM ticks WHERE price > 50.0;
```

```vpl
# Varpulis
stream Filtered = Tick
    .where(price > 50.0)
    .emit(symbol: symbol, price: price, volume: volume)
```

| Engine | Throughput | Output | Input path |
|---|---|---|---|
| **Varpulis** | **174,751 events/sec** | 89,000 ✓ | File (JSONL via `simulate`) |
| Arroyo | 86,398 events/sec | 89,000 ✓ | Kafka (Redpanda topic via Kafka source) |

Both engines deliver identical 89,000 output events, verifying correctness.
Varpulis is roughly **2× faster** on its native input path than Arroyo on its
native input path. The architectural reasons:

1. **Varpulis pays no broker round-trip.** File-mode reads JSONL directly from
   the local filesystem, no network/Kafka deserialisation overhead. The
   Arroyo path goes through Redpanda's Kafka protocol on every event.
2. **Arroyo runs in Docker with its full cluster topology** (controller +
   worker + Postgres) — non-trivial baseline overhead even at small
   parallelism. Varpulis runs as a single process with a shared in-process
   pipeline.
3. **The Arroyo job has to compile and start before timing begins**, but
   we exclude the compile time. The 1.16s wall time is purely the data
   processing pass through the running pipeline.

A direct Varpulis-on-Kafka comparison (rebuilding `varpulis-cli` with
`--features kafka` and using the same Redpanda topic) would close some of
this gap because Varpulis would also pay the Kafka deserialisation cost.
Architecturally we expect Varpulis-on-Kafka to land in the **130-160k
events/sec** range based on its file-mode-vs-Kafka delta in the apama
benchmark, leaving roughly a 1.5-2× margin over Arroyo.

### What we did NOT yet measure

- **Scenario 2 (tumbling 1-second windowed aggregation)**: Arroyo's `WATERMARK
  FOR` clause requires a `TIMESTAMP` column, and our benchmark generator emits
  `ts` as `BIGINT` (Unix millis) for compatibility with Proton's JSONEachRow.
  A future revision will add a parallel `event_time` ISO string field so the
  same generator drives both engines without schema changes. The infrastructure
  in `benchmarks/arroyo-comparison/` has the SQL written; only the data
  generator update is needed.
- **Like-for-like Kafka comparison** (both engines on Kafka): requires
  rebuilding Varpulis with `--features kafka`. Pending.
- **Native pattern detection workloads** (sequence, Kleene closure,
  forecasting): Arroyo has no native implementation, so the only comparison
  would be Varpulis's NFA vs a hand-coded Rust UDAF — that's not engine-vs-
  engine, it's "Varpulis vs whoever wrote the UDAF".

If you want to reproduce these numbers, the benchmark scripts are at
[`benchmarks/arroyo-comparison/`](https://github.com/varpulis/varpulis/tree/main/benchmarks/arroyo-comparison)
— `python3 run_benchmark.py --scenario 01_filter` runs the Arroyo path.

### Published vendor numbers (for context)

Arroyo's most-cited benchmark is the [10× faster than Flink on sliding windows](https://www.arroyo.dev/blog/how-arroyo-beats-flink-at-sliding-windows/) blog post. The architectural argument is sound: Flink naïvely updates `width/slide` windows per event (720 updates for a 1h/5s sliding window), while Arroyo's `WindowState` uses incremental bin aggregation whose memory cost scales with non-empty bins. Directionally plausible, but the post does not include hardware specs, dataset descriptions, or reproducible benchmark code — it's a marketing technical post, not a published benchmark.

Varpulis adopted the **same incremental binned-window approach** in the recent enhancement work (see the binned sliding window in `crates/varpulis-runtime/src/window.rs` — the compiler auto-selects it whenever `window_size / slide_interval >= 10`). For wide-window-small-slide workloads, Varpulis and Arroyo are now in the same algorithmic family — both should be substantially faster than Flink and roughly comparable to each other.

Published Varpulis numbers:
- 1.5M events/sec for SASE+ pattern matching on a single core
- 410K events/sec for a full filter+aggregate+emit pipeline
- 950K events/sec for 50 concurrent Hamlet-shared patterns
- 51 ns per PST single-symbol forecast prediction
- ~50 MB memory baseline

Both engines are vectorised native Rust binaries with similar baseline overheads. **Expect them to be in the same order of magnitude on workloads both can express**, with Arroyo holding an edge on column-heavy DataFusion compute (it inherits the full DataFusion vectorised kernel library) and Varpulis holding an edge on pattern-matching workloads where it isn't paying the FSM-in-UDF tax.

For workloads only one engine can express — sequence detection, Kleene closure, forecasting — comparing throughput is meaningless because the Rust-UDAF version is in a different operational class than a native NFA.

## When to Use Arroyo

- You want **streaming SQL with first-class DataFusion vectorised execution** and the full DataFusion scalar/aggregate function library.
- You need **Rust UDFs (sync and async)** as a primary extension mechanism — Arroyo's UDF story is genuinely strong here.
- You're building **streaming ETL into Apache Iceberg** (especially on Cloudflare R2) — the v0.15.0 Iceberg sink is well-integrated.
- You're already a Cloudflare customer and you want a path to **Cloudflare Pipelines** as your eventual managed offering.
- Your workloads are **windowed aggregations, group-bys, and updating joins** — the bread and butter of streaming SQL.
- You're comfortable running an **external Postgres** alongside the engine for control-plane state.
- You want the **"WATERMARK FOR ..."** DDL ergonomics for explicit event-time configuration.

## When to Use Varpulis

- You need **sequence detection** — patterns where order matters (`A → B → C`).
- You need **Kleene closure** with exhaustive matching — capture *every* matching event in a window, not just the longest run.
- You need **per-key partitioning of patterns** with shared state across the matched events.
- You need **forecasting** — predict that a pattern is about to complete before the final event arrives.
- You need **multi-query optimisation** when running many concurrent patterns over the same stream (Hamlet graphlet sharing).
- You are doing **detection engineering** — Sigma rules, MITRE ATT&CK kill chains, fraud playbooks, behavioural rules — and you want a DSL designed for that.
- You want a streaming engine **without an external Postgres dependency** — single binary, embedded state, no extra infra.
- You want a streaming engine whose **roadmap is governed entirely in the open** with no commercial managed-service overlay.

## Using Both Together

Both engines are MIT/Apache-2.0 licensed and run as single Rust binaries — there's nothing stopping you from running them side by side on the same Kafka topic. A common architecture splits responsibilities by query type:

```
┌─────────┐    ┌──────────────┐    ┌──────────┐
│  Kafka  │───▶│   Varpulis   │───▶│  alerts  │
│ events  │    │  (patterns,  │    │   topic  │
│         │    │  forecasts)  │    └──────────┘
│         │    └──────────────┘
│         │
│         │    ┌──────────────┐    ┌──────────┐
│         │───▶│    Arroyo    │───▶│  iceberg │
│         │    │ (SQL ETL,    │    │   on R2  │
└─────────┘    │  aggregates) │    └──────────┘
               └──────────────┘
```

Run Varpulis for behavioural detection and forecasting; run Arroyo for SQL ETL into Iceberg on R2. Both subscribe independently, both have their own state, neither has any dependency on the other. **One Kafka topic, two stream processors, two different jobs.**

If your team has the operational budget for a single streaming engine and the workload is mostly aggregation/ETL, Arroyo is a strong choice (with the caveat that you should be comfortable with the Cloudflare integration story). If pattern detection is on the roadmap or already a pain, Varpulis is the only open-source engine in the Rust-native single-binary tier that handles it without UDF gymnastics.

## Summary

Arroyo and Varpulis sit in adjacent niches: same language (Rust), same deployment shape (single binary), same target audience (teams who don't want a JVM cluster). They differ on what they make easy. Arroyo is the strongest open-source streaming-SQL engine in the no-JVM tier, with excellent Rust UDFs and a real Cloudflare-backed future. Varpulis is the strongest pattern-matching engine in the no-JVM tier, with native sequence detection, Kleene closures, forecasting, and multi-query optimisation that no SQL engine offers.

If you came looking for "how do I do MATCH_RECOGNIZE in Arroyo", the honest answer is: **you write a Rust UDAF, or you use Varpulis**. If you came looking for "how do I run streaming SQL ETL into Iceberg with first-class Rust UDFs", the honest answer is: **Arroyo was built for that, and Cloudflare is now backing it**.

Both projects are MIT/Apache-2.0 — the choice is reversible.
