# Why Are We Still Writing Callback Hell for Event Processing in 2026?

Complex Event Processing has a dirty secret: detecting a simple sequence like "transaction opens, processes through multiple steps, then closes" requires absurd amounts of boilerplate in most engines. Let's compare.

## The problem: match `A → B+ → C` and compute aggregates over the run

A common pattern in transaction monitoring: detect a transaction that opens, goes through one or more processing steps, and closes — then compute statistics across those steps.

Simple enough, right?

## Apama (MonitorScript)

The classic approach uses nested `on` listeners in a monitor:

```
monitor TransactionMonitor {
    action onload() {
        on all TransactionOpen() as open {
            sequence<ProcessingStep> steps := new sequence<ProcessingStep>;

            on all ProcessingStep(tx_id=open.tx_id) as step
                and not TransactionClose(tx_id=open.tx_id) {
                steps.append(step);
            }

            on TransactionClose(tx_id=open.tx_id) as close {
                float totalDuration := 0.0;
                integer maxErrors := 0;
                float totalThroughput := 0.0;

                ProcessingStep s;
                for s in steps {
                    totalDuration := totalDuration + s.duration;
                    if s.error_count > maxErrors {
                        maxErrors := s.error_count;
                    }
                    totalThroughput := totalThroughput + s.throughput;
                }

                float avgDuration := totalDuration / steps.size().toFloat();

                emit TransactionSummary(
                    open.tx_id, avgDuration, maxErrors,
                    totalThroughput, steps.size()
                );
            }
        }
    }
}
```

Three nested `on` blocks. Manual accumulation. Manual aggregation. And if you forget the `and not` guard on the middle listener, you get duplicate matches.

Apama does have a more declarative paradigm — stream queries with `from` clauses support windowed aggregation, joins, and partitioning. But stream queries operate on individual streams with retention windows. They have no sequence operator and no Kleene closure. You simply cannot express "A then B+ then C" in a `from` query. For sequential patterns with accumulation, the nested `on` listener approach above is what you're stuck with.

## Esper (EPL)

```sql
@Name('TransactionStats')
insert into TransactionSummary
select
    open.tx_id as tx_id,
    avg(steps.duration) as avg_duration,
    max(steps.error_count) as max_error_count,
    sum(steps.throughput) as total_throughput,
    count(steps) as step_count
from pattern [
    every open=TransactionOpen
    -> steps=ProcessingStep(tx_id=open.tx_id) until TransactionClose(tx_id=open.tx_id)
]
group by open.tx_id;
```

Better. Esper's `until` operator handles the Kleene-like repetition. The collected events are materialized as indexed properties — `steps[0]`, `steps[1]`, etc. — and aggregation functions operate over that array. The `every` keyword placement matters more than you'd think: move it inside the pattern and the semantics change silently.

## Flink CEP

```java
Pattern<Event, ?> pattern = Pattern.<Event>begin("open")
    .subtype(TransactionOpen.class)
    .followedBy("steps")
    .subtype(ProcessingStep.class)
    .oneOrMore()
    .greedy()
    .until(new SimpleCondition<Event>() {
        public boolean filter(Event e) {
            return e instanceof TransactionClose;
        }
    })
    .followedBy("close")
    .subtype(TransactionClose.class);

CEP.pattern(stream, pattern)
    .select(new PatternSelectFunction<Event, TransactionSummary>() {
        public TransactionSummary select(Map<String, List<Event>> pattern) {
            TransactionOpen open = (TransactionOpen) pattern.get("open").get(0);
            List<Event> steps = pattern.get("steps");

            double avgDur = steps.stream()
                .mapToDouble(e -> ((ProcessingStep) e).getDuration())
                .average().orElse(0);
            int maxErr = steps.stream()
                .mapToInt(e -> ((ProcessingStep) e).getErrorCount())
                .max().orElse(0);
            double totalTput = steps.stream()
                .mapToDouble(e -> ((ProcessingStep) e).getThroughput())
                .sum();

            return new TransactionSummary(
                open.getTxId(), avgDur, maxErr, totalTput, steps.size()
            );
        }
    });
```

The fluent API is verbose but readable. The real cost is the `PatternSelectFunction`: you're back to manual aggregation over a `Map<String, List<Event>>` with type casting everywhere. And this doesn't even include the `KeyedStream` partitioning boilerplate.

## Varpulis

```python
stream TransactionStats = TransactionOpen as open
    -> all ProcessingStep as steps
    -> TransactionClose as close
    .within(30m)
    .partition_by(tx_id)
    .trend_aggregate(
        avg_dur:    avg_trends(steps.duration),
        max_errors: max_trends(steps.error_count),
        total_tput: sum_trends(steps.throughput),
        step_count: count_events(steps)
    )
    .emit(
        event_type: "TransactionSummary",
        tx_id: open.tx_id,
        avg_duration: avg_dur,
        max_error_count: max_errors,
        total_throughput: total_tput,
        steps: step_count
    )
```

The pattern is the query. `->` is sequence. `all` is the Kleene closure — one or more repetitions of `ProcessingStep`. The VPL compiler translates this into a SASE+ automaton with Kleene closure states, and `.trend_aggregate()` computes statistics directly over all matching trends using the Hamlet algorithm — O(n) per event instead of enumerating every possible subsequence.

No callbacks. No manual accumulation. No type casting. The engine handles partitioning, windowing, and aggregation as part of the pattern itself.

## What about just detection?

Sometimes you don't need aggregates — you just want to catch slow steps inside a transaction. Here's the same pattern in Varpulis, this time as a simple SASE+ detection query:

```python
stream SlowTransactionStep = TransactionOpen as open
    -> all ProcessingStep where duration > 5000.0 as slow_step
    .within(30m)
    .partition_by(tx_id)
    .emit(
        event_type: "SlowStepAlert",
        tx_id: open.tx_id,
        step_name: slow_step.step_name,
        duration: slow_step.duration
    )
```

Two lines of pattern plus a window constraint, and you get an alert the instant a slow step occurs — no need to wait for the transaction to close. Try that in a `PatternSelectFunction`.

## The 2^n problem that nobody talks about

There's a deeper issue with Kleene closures that syntax alone doesn't solve. When you match `A → B+ → C` and 10 events match `B`, there are 2^10 - 1 = 1,023 possible trend combinations. With 20 events, that's over a million. With 30, over a billion.

This isn't a theoretical concern — it's an operational one, though it manifests differently in each engine. Esper's `until` operator collects matched events into an in-memory array. The array itself is O(n), but for long-running patterns with hundreds of intermediate steps, the accumulation keeps growing until the terminator arrives or the window expires — and there's no backpressure mechanism to limit it. Flink CEP buffers matched events in its `SharedBuffer` keyed state backend. The buffer stores individual events with versioned links for concurrent partial matches. Under high cardinality — many concurrent transactions with many steps each — state size grows with every new partial match, leading to checkpoint failures, back-pressure cascades, and ultimately OOM kills. Apama's approach of manually collecting events into a `sequence<>` has the same linear accumulation problem, and since the correlator processes events single-threaded within each context partition, one runaway pattern can starve other monitors.

The exponential blowup hits when you need *aggregates* over Kleene matches. If you want "average duration across all possible subsequences of B events," you're asking about 2^n combinations. Most engines don't even attempt this — they either aggregate over the flat event list (ignoring subsequence semantics) or force you to enumerate matches yourself. Most production deployments work around it by keeping Kleene windows short, limiting the number of intermediate events, or simply avoiding Kleene closures altogether. That's not a solution — that's giving up on the expressiveness that CEP was supposed to provide.

Varpulis tackles this at the engine level with two complementary approaches, chosen automatically at compile time based on the query:

**Detection mode (SASE+ with ZDD)** — When you need the actual matches, like in the `SlowTransactionStep` example, the engine uses Zero-suppressed Decision Diagrams (ZDD) to represent all 2^n match combinations as a compact boolean function. The full match space is encoded in O(poly(n)) nodes through structure sharing, rather than allocating memory for each combination individually.

**Aggregation mode (Hamlet)** — When you only need statistics over the matches, like in `TransactionStats`, the engine doesn't enumerate trends at all. Hamlet computes aggregates — count, sum, avg, max, min — directly in O(n) per event using propagation coefficients. It answers "what's the average duration across all possible trends?" without ever building the match set.

The engine picks the right strategy automatically. If your query has `.trend_aggregate()`, it uses Hamlet. If not, it uses SASE+ with ZDD. Same `-> all` Kleene pattern, different execution path — zero configuration from the developer.

| Mode | Engine | What it does | Complexity |
|------|--------|-------------|-----------|
| Detection | SASE+ with ZDD | Represents all Kleene matches compactly | 2^n matches in O(poly(n)) memory |
| Aggregation | Hamlet | Computes aggregates without enumerating | O(n) per event — skips the match set entirely |

This is the kind of optimization that's invisible in the query language but makes the difference between "works on a demo" and "works on a million events per second." A transaction with 50 processing steps produces 2^50 — over a quadrillion — possible trends. Hamlet computes the aggregate in 50 operations.

## Beyond detection: forecasting

Varpulis also supports something none of the engines above offer natively — pattern forecasting. By appending `.forecast()` to a sequence pattern, the engine builds a Prediction Suffix Tree (PST) online and predicts the probability and expected time of pattern completion *before* the full sequence has been observed:

```python
stream FraudForecast = Login as login
    -> all Transaction where amount > 10000 as tx
    -> Withdrawal as withdrawal
    .within(1h)
    .forecast(confidence: 0.7, horizon: 5m, warmup: 500)
    .where(forecast_probability > 0.85)
    .emit(
        event_type: "FraudWarning",
        user_id: login.user_id,
        probability: forecast_probability,
        expected_time: forecast_time
    )
```

The PST learns transition patterns incrementally — no pre-training, no batch step. After the warmup period, it starts emitting `ForecastResult` events with probability estimates and expected completion times that flow through the normal `.where()` / `.emit()` pipeline.

## Why this matters

CEP is not a new problem. SASE was published in 2006. Kleene closures over event streams have been well-understood for almost two decades. Yet most production engines still force developers to express these patterns through nested callbacks, manual state management, or verbose APIs that obscure the intent.

Worse, most engines split event processing into two disconnected worlds: stream queries for windowed aggregation, and pattern listeners for sequence detection. In Apama, you can't use a Kleene closure in a `from` query. In Flink, stream processing and CEP are separate APIs. This means developers end up stitching two paradigms together for problems that are fundamentally one thing.

Varpulis doesn't make that distinction. Sequential patterns, Kleene closures, trend aggregations, joins (`join`, `left_join`, `right_join`, `full_join`, `merge`) — it's all the same `stream` construct. The pattern is the query. The engine picks the execution strategy.

Varpulis is open-source, built in Rust, and early-stage — but the pattern language is stable.

**GitHub**: [github.com/varpulis/varpulis](https://github.com/varpulis/varpulis)

---

*Feedback, stars, and issues welcome.*
