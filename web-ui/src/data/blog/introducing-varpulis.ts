import type { BlogPost } from '@/types/blog'

export const introducingVarpulis: BlogPost = {
  slug: 'introducing-varpulis',
  title: 'Introducing Varpulis: Pattern Detection for Event Streams in 3 Lines',
  date: '2026-02-02',
  author: 'Cyril Poder',
  summary: 'Varpulis is a new Complex Event Processing engine built in Rust with its own domain language (VPL). Define sequence patterns, detect fraud, monitor IoT, and forecast events — all declaratively.',
  tags: ['release', 'announcement'],
  body: `
<p>Today we're releasing <strong>Varpulis</strong>, a Complex Event Processing engine built from the ground up in Rust. It ships with its own domain language &mdash; VPL (Varpulis Pattern Language) &mdash; designed to make pattern detection over event streams as concise as possible.</p>

<h2>The problem with existing CEP engines</h2>

<p>Current CEP solutions fall into two camps: SQL-like stream processors that are great at windowed aggregation but terrible at sequence detection, and pattern-matching engines with verbose APIs that bury the intent under boilerplate. You end up writing 50+ lines of Java to detect what should be a 3-line pattern.</p>

<h2>VPL: the pattern <em>is</em> the query</h2>

<pre><code>connector = kafka(brokers: "localhost:9092", topic: "events")

stream FraudAlert = login as l -> transfer as t .within(5m)
    .where(l.user_id == t.user_id and t.amount > 5000)
    .emit(alert: "Suspicious transfer", user: l.user_id, amount: t.amount)</code></pre>

<p>That's it. Three lines. A connector, a sequence pattern with a time window, a filter, and an output projection. The engine compiles this into a SASE+ automaton that processes events at 250K+ events/sec on a single core.</p>

<h2>What's inside</h2>

<ul>
<li><strong>SASE+ pattern matching</strong> &mdash; Sequence detection with Kleene closures, negation, and temporal constraints</li>
<li><strong>14 connectors</strong> &mdash; Kafka, MQTT, NATS, Redis, PostgreSQL CDC, PostgreSQL, MySQL, SQLite, AWS Kinesis, AWS S3, Pulsar, Elasticsearch, HTTP/Webhooks, and Console</li>
<li><strong>Hamlet trend aggregation</strong> &mdash; Compute aggregates over 2<sup>n</sup> Kleene match combinations in O(n) per event</li>
<li><strong>PST forecasting</strong> &mdash; Predict pattern completions before they happen, with sub-microsecond latency</li>
<li><strong>Distributed cluster mode</strong> &mdash; Coordinator + workers with automatic partitioning and Raft consensus</li>
<li><strong>Full LSP</strong> &mdash; VS Code extension with completions, hover docs, diagnostics, and semantic tokens</li>
<li><strong>Web dashboard</strong> &mdash; Deploy, monitor, and manage pipelines through a modern UI</li>
</ul>

<h2>Performance</h2>

<p>We benchmarked against Apama (the incumbent CEP engine from Software AG) on identical workloads:</p>

<table>
<thead><tr><th>Scenario</th><th>Varpulis</th><th>Apama</th><th>RAM (V / A)</th></tr></thead>
<tbody>
<tr><td>Filter</td><td>234 K/s</td><td>199 K/s</td><td>54 / 166 MB</td></tr>
<tr><td>Sequence</td><td>256 K/s</td><td>221 K/s</td><td>36 / 185 MB</td></tr>
<tr><td>Kleene*</td><td>97 K/s</td><td>195 K/s</td><td>58 / 190 MB</td></tr>
</tbody>
</table>

<p><small>*Kleene: Apama reports 2x throughput but detects 5x fewer matches (20K vs 99.6K). Normalized by matches/sec: Varpulis 2.5x more efficient.</small></p>

<p>Memory usage is consistently 3-16x lower thanks to Rust's zero-cost abstractions and Varpulis's zero-copy event parsing.</p>

<h2>Get started</h2>

<pre><code>cargo install varpulis-cli</code></pre>

<p>Or try it in the <a href="/playground">interactive playground</a> &mdash; no installation needed.</p>

<p><strong>GitHub</strong>: <a href="https://github.com/varpulis/varpulis">github.com/varpulis/varpulis</a></p>
`,
}
