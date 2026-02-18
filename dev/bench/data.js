window.BENCHMARK_DATA = {
  "lastUpdate": 1771407249246,
  "repoUrl": "https://github.com/varpulis/varpulis",
  "entries": {
    "Varpulis Performance": [
      {
        "commit": {
          "author": {
            "email": "cyril.poderà@gmail.com",
            "name": "cpoder"
          },
          "committer": {
            "email": "cyril.poderà@gmail.com",
            "name": "cpoder"
          },
          "distinct": true,
          "id": "fd85da1bd82c0341f0e71f1ed7cc57230c244cdd",
          "message": "fix(fuzz): fix string slice panic and duration overflow, add bench push perms\n\n- event_file: require len >= 2 before slicing quoted strings (single `\"`\n  caused `s[1..0]` panic)\n- helpers: use saturating_mul in parse_duration to prevent integer\n  overflow panic on adversarial inputs like \"999999999d\"\n- bench.yml: add `permissions: contents: write` so benchmark-action can\n  push results to gh-pages\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-18T10:27:18+01:00",
          "tree_id": "5dc369eb916322c90023814a6b39b90cd473a8a9",
          "url": "https://github.com/varpulis/varpulis/commit/fd85da1bd82c0341f0e71f1ed7cc57230c244cdd"
        },
        "date": 1771407247773,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 28288,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 279330,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 2844300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 43707,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 482960,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2446800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 27029,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 346440,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1750300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 1955000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3649900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1200700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1476100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 193510000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1525100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 2868700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 14435000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 28601000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 27857000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19603000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}