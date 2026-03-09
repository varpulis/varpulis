import type { BlogPost } from '@/types/blog'

export const whyCallbackHell: BlogPost = {
  slug: 'why-callback-hell-for-cep',
  title: 'Why Are We Still Writing Callback Hell for Event Processing in 2026?',
  date: '2026-03-01',
  author: 'Cyril Poder',
  summary: 'Complex Event Processing has a dirty secret: detecting a simple sequence pattern requires absurd amounts of boilerplate in most engines. We compare Apama, Esper, Flink CEP, and Varpulis.',
  tags: ['engineering', 'comparison', 'patterns'],
  body: `
<p>Complex Event Processing has a dirty secret: detecting a simple sequence like "transaction opens, processes through multiple steps, then closes" requires absurd amounts of boilerplate in most engines. Let's compare.</p>

<h2>The problem: match A &rarr; B+ &rarr; C and compute aggregates over the run</h2>

<p>A common pattern in transaction monitoring: detect a transaction that opens, goes through one or more processing steps, and closes &mdash; then compute statistics across those steps.</p>

<h2>Apama (MonitorScript)</h2>

<p>The classic approach uses nested <code>on</code> listeners in a monitor:</p>

<pre><code>monitor TransactionMonitor {
    action onload() {
        on all TransactionOpen() as open {
            sequence&lt;ProcessingStep&gt; steps := new sequence&lt;ProcessingStep&gt;;

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
}</code></pre>

<p>Three nested <code>on</code> blocks. Manual accumulation. Manual aggregation. And if you forget the <code>and not</code> guard on the middle listener, you get duplicate matches.</p>

<h2>Esper (EPL)</h2>

<pre><code>@Name('TransactionStats')
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
group by open.tx_id;</code></pre>

<p>Better. Esper's <code>until</code> operator handles Kleene-like repetition. The <code>every</code> keyword placement matters more than you'd think: move it inside the pattern and the semantics change silently.</p>

<h2>Flink CEP</h2>

<pre><code>Pattern&lt;Event, ?&gt; pattern = Pattern.&lt;Event&gt;begin("open")
    .subtype(TransactionOpen.class)
    .followedBy("steps")
    .subtype(ProcessingStep.class)
    .oneOrMore()
    .greedy()
    .until(new SimpleCondition&lt;Event&gt;() {
        public boolean filter(Event e) {
            return e instanceof TransactionClose;
        }
    })
    .followedBy("close")
    .subtype(TransactionClose.class);

CEP.pattern(stream, pattern)
    .select(new PatternSelectFunction&lt;Event, TransactionSummary&gt;() {
        public TransactionSummary select(Map&lt;String, List&lt;Event&gt;&gt; pattern) {
            TransactionOpen open = (TransactionOpen) pattern.get("open").get(0);
            List&lt;Event&gt; steps = pattern.get("steps");
            // ... manual aggregation with type casting
        }
    });</code></pre>

<p>The fluent API is verbose but readable. The real cost is the <code>PatternSelectFunction</code>: you're back to manual aggregation over a <code>Map&lt;String, List&lt;Event&gt;&gt;</code> with type casting everywhere.</p>

<h2>Varpulis</h2>

<pre><code>stream TransactionStats = TransactionOpen as open
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
    )</code></pre>

<p>The pattern <em>is</em> the query. <code>-></code> is sequence. <code>all</code> is the Kleene closure. The VPL compiler translates this into a SASE+ automaton, and <code>.trend_aggregate()</code> computes statistics using the Hamlet algorithm &mdash; O(n) per event instead of enumerating every possible subsequence.</p>

<p>No callbacks. No manual accumulation. No type casting.</p>

<h2>The 2<sup>n</sup> problem that nobody talks about</h2>

<p>When you match <code>A &rarr; B+ &rarr; C</code> and 10 events match B, there are 2<sup>10</sup> - 1 = 1,023 possible trend combinations. With 20 events, over a million. With 30, over a billion.</p>

<p>Varpulis tackles this at the engine level with two complementary approaches, chosen automatically at compile time:</p>

<ul>
<li><strong>Detection mode (SASE+ with ZDD)</strong> &mdash; Represents all 2<sup>n</sup> match combinations as a compact boolean function in O(poly(n)) nodes.</li>
<li><strong>Aggregation mode (Hamlet)</strong> &mdash; Computes aggregates directly in O(n) per event using propagation coefficients, without ever building the match set.</li>
</ul>

<p>A transaction with 50 processing steps produces 2<sup>50</sup> &mdash; over a quadrillion &mdash; possible trends. Hamlet computes the aggregate in 50 operations.</p>

<h2>Beyond detection: forecasting</h2>

<p>Varpulis also supports pattern forecasting. By appending <code>.forecast()</code> to a sequence pattern, the engine builds a Prediction Suffix Tree online and predicts the probability and expected time of pattern completion <em>before</em> the full sequence has been observed.</p>

<pre><code>stream FraudForecast = Login as login
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
    )</code></pre>

<p>The PST learns transition patterns incrementally &mdash; no pre-training, no batch step. After the warmup period, it starts emitting probability estimates and expected completion times that flow through the normal <code>.where()</code> / <code>.emit()</code> pipeline.</p>

<h2>Why this matters</h2>

<p>CEP is not a new problem. SASE was published in 2006. Yet most production engines still force developers to express patterns through nested callbacks, manual state management, or verbose APIs that obscure the intent.</p>

<p>Varpulis is open-source, built in Rust, and early-stage &mdash; but the pattern language is stable.</p>

<p><strong>GitHub</strong>: <a href="https://github.com/varpulis/varpulis">github.com/varpulis/varpulis</a></p>
`,
}
