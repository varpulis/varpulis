window.BENCHMARK_DATA = {
  "lastUpdate": 1771796842787,
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
      },
      {
        "commit": {
          "author": {
            "email": "cyril.poder@gmail.com",
            "name": "Cyril PODER",
            "username": "cpoder"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f70104504f61d42ee96a56407f6f430b71bd0773",
          "message": "Merge pull request #2 from varpulis/feat/nats-connector\n\nfeat(connector): add NATS data connector",
          "timestamp": "2026-02-18T19:08:21+01:00",
          "tree_id": "b63a0c14cfcbaa944c1570557f31dc55da869e24",
          "url": "https://github.com/varpulis/varpulis/commit/f70104504f61d42ee96a56407f6f430b71bd0773"
        },
        "date": 1771438506109,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 28220,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 280920,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 2817100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 44000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 491990,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2420600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 27552,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 351780,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1769500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 1938900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3605900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1176800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1480800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 192950000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1489500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 2805700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 14355000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 28584000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 28609000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19396000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "9cf2ec3f3efff9c3c4942ed45f21658adbc7d9f6",
          "message": "fix(parser): harden parser against fuzz-discovered edge cases\n\n- Return Result from parse_duration() to reject overflow instead of\n  silently saturating (e.g. 999999999999d)\n- Add O(n) nesting depth pre-scan (max 64 levels) before pest parsing\n  to prevent stack overflow on deeply nested inputs\n- Extend fuzz CI runs from 5 to 30 minutes with corpus caching\n- Ignore fuzz crash artifacts in .gitignore\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-18T21:12:07+01:00",
          "tree_id": "9d1fc0823e135e8c9005eb04c3623aba45d8d883",
          "url": "https://github.com/varpulis/varpulis/commit/9cf2ec3f3efff9c3c4942ed45f21658adbc7d9f6"
        },
        "date": 1771445902790,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 32874,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 329670,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3463300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 54009,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 576180,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2937400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 31629,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 389840,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1986500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2335600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4791900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1453000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1792900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 217300000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1782600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3673100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 18932000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 37895000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 36907000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 24916000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "ea234c2ad6afcc46efdf91c4498095e128de20c0",
          "message": "test(nats): add E2E integration tests with real nats-server\n\nAdd 13 end-to-end tests against a real NATS server covering the full\nNATS connector and cluster transport stack. Add nats-e2e CI job with\na nats:latest service container, and add nats to the feature-flags matrix.\n\nRuntime tests (7): source receive, sink publish, roundtrip, JSON parsing\nvariants (flat/nested), subject-based event_type fallback, managed\nconnector, and queue group load balancing.\n\nCluster tests (6): request/reply roundtrip, publish/subscribe, worker\nregistration, heartbeat, deploy command, and inject command — all\nexercising the real coordinator and worker NATS handlers.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-18T23:15:53+01:00",
          "tree_id": "4bb145c48a1d615e0f4c5088233fec8c579cf111",
          "url": "https://github.com/varpulis/varpulis/commit/ea234c2ad6afcc46efdf91c4498095e128de20c0"
        },
        "date": 1771453332998,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 28855,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 286930,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 2880400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 44931,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 496260,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2463900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 27344,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 346110,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1747100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2009500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3672300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1317000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1486100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 191100000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1578900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 2801000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 14037000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 28143000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 28887000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19774000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "898ae47f8603b71153179ca02423e85fc1f1a695",
          "message": "feat(kafka): concurrent batch delivery for 10x+ sink throughput\n\nAdd send_batch() to KafkaSinkImpl using producer.send_result() to enqueue\nall events synchronously, then await all DeliveryFutures at once via\njoin_all. New BatchKafkaSinkAdapter routes non-transactional Kafka sinks\nthrough this path. Also adds VPL parameter name mapping (batch_size →\nbatch.size, linger_ms → linger.ms, etc.).\n\nCloses #4.\n\nAlso removes obsolete docs: KANBAN.md (completed task board),\nevent-listeners.md (superseded by SASE+), HOT_RELOAD_AND_PARALLELISM.md\n(outdated design proposal).\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-19T12:20:37+01:00",
          "tree_id": "d6dabda055f0b83a1afaa016f34b56676ad477e0",
          "url": "https://github.com/varpulis/varpulis/commit/898ae47f8603b71153179ca02423e85fc1f1a695"
        },
        "date": 1771500401185,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 28622,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 280430,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 2791700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 44506,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 492120,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2441800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 27600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 347380,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1764300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 1974800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3587100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1178700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1453900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 192330000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1495700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3033100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 14934000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 29706000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 28555000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19289000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "8627bdb1a7cec5dd28131c08665647d34ee3b51d",
          "message": "ci: add multi-worker NATS E2E test step\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-19T12:24:38+01:00",
          "tree_id": "88dbad81ef62508001842d8d034c410f204575d1",
          "url": "https://github.com/varpulis/varpulis/commit/8627bdb1a7cec5dd28131c08665647d34ee3b51d"
        },
        "date": 1771500639083,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 28323,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 282830,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 2824300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 43971,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 489690,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2453500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 27008,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 346970,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1740400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 1939900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3626200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1196200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1469000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 194530000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1522700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 2899600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 14501000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 28994000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 28226000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19372000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "e2532f95315a6d17b20761a06fdc4b897a59d276",
          "message": "fix(ci): add missing NATS E2E test file and resolve npm audit vulnerabilities\n\n- Commit nats_multi_worker_e2e.rs (was untracked, referenced by CI)\n- Upgrade vue-tsc ^2.2.0 → ^3.2.4 (drops vulnerable minimatch via picomatch)\n- Add npm overrides for minimatch ^10.2.1 to fix remaining transitive vuln\n  from @vue/test-utils → js-beautify → editorconfig → minimatch\n\nnpm audit: 0 vulnerabilities\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-19T12:37:09+01:00",
          "tree_id": "241a9110a58649a1524bb10cce19106563780114",
          "url": "https://github.com/varpulis/varpulis/commit/e2532f95315a6d17b20761a06fdc4b897a59d276"
        },
        "date": 1771501398295,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 28270,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 282930,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 2819900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 44960,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 492750,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2436500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 27396,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 343620,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1752400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 1969600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3635400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1179800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1455800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 196090000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1506200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 2894500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 14377000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 28792000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 28445000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19195000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "eea24dc49209e8db24168e699e4f2699ea9a09d2",
          "message": "perf(sase): optimize Kleene closure hot paths for 15-40% throughput improvement\n\nSeven targeted optimizations to the SASE+ Kleene pattern matching engine:\n\n1. Skip ZDD when no deferred predicate — add extend_simple() that bypasses\n   arena.product_with_optional() when postponed_predicate is None (common case)\n2. Eliminate per-push Instant::now() — capture once per event in process loop,\n   pass through advance_run_shared via push_at()\n3. Pre-compute has_epsilon_to_accept on State — avoid per-event iteration over\n   epsilon_transitions in Kleene self-loop and transition-entering paths\n4. Avoid alias key re-allocation in Kleene self-loop — push_at_kleene() uses\n   get_mut to update existing captured entry without re-inserting key\n5. Throttle cleanup_timeouts() — skip if <100ms elapsed (ProcessingTime only;\n   EventTime always runs to handle watermark jumps)\n6. Skip empty check_global_negations — early return when no negations configured\n7. Avoid partition_by.clone() per event — borrow with as_ref() instead\n\nBenchmarks (criterion):\n- nested_kleene_5k: -17.8% time (p=0.00)\n- kleene_plus/sase/5000: -5.4% time (p=0.00)\n- Pathological workload (A/B): +41% throughput (443 → 627 events/sec)\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-19T15:33:49+01:00",
          "tree_id": "5b2b33dc7c2a09e6c907aa1fa54b344ab17aba1b",
          "url": "https://github.com/varpulis/varpulis/commit/eea24dc49209e8db24168e699e4f2699ea9a09d2"
        },
        "date": 1771511987409,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 34031,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 328390,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3277600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40454,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 438700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2211700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32313,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 393990,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1987200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2104700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3954400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1485900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1659100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 114380000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1748000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3385600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16875000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 34374000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32918000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19405000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "ce324865f26d48b64588ebddfc94ddeeece2620a",
          "message": "feat(web-ui): add pipeline monitoring dashboard with live event stream\n\nCloses #5. Adds a new Monitoring view with:\n\n- Pipeline status table: per-pipeline events in/out, throughput (evt/s),\n  connector health chips (NATS/MQTT/Kafka status at a glance)\n- Connector health panel: detailed table showing connector name, type,\n  pipeline, worker, connection status, message count, last message time,\n  and error messages\n- Live event stream: WebSocket-based real-time feed of matched events\n  with pause/resume/clear controls (up to 200 events in buffer)\n- Pipeline detail dialog: click any pipeline for metrics breakdown\n  including selectivity ratio and per-connector health details\n- Summary cards: pipeline count, worker health, connector status, live\n  event count with start/stop toggle\n- Throughput chart: integrated existing ThroughputChart component\n\nAlso:\n- Extended PipelineWorkerMetrics type with connector_health array\n  (connector_name, connector_type, connected, messages_received,\n  seconds_since_last_message, last_error) matching backend API\n- Added /monitoring route and navigation item\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-19T19:49:18+01:00",
          "tree_id": "459c17dcef7a5c65027ec0d723d77df006c51ad7",
          "url": "https://github.com/varpulis/varpulis/commit/ce324865f26d48b64588ebddfc94ddeeece2620a"
        },
        "date": 1771527336035,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33550,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 330170,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3318900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40495,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 454940,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2280700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32459,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 390360,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1967500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2088800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3832900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1475200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1695700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 108580000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1737900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3406100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 17128000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 34102000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33356000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19224000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "8b69a3a6ea54dcc6b3b91dd95bf629530e053f37",
          "message": "perf(engine): reduce allocations in pipeline hot paths for 10-25% throughput gain\n\nCache Arc<str> stream name on StreamDefinition to eliminate repeated\nString→Arc<str> conversions. Use Arc::try_unwrap to avoid deep Event\nclones when refcount is 1 (common in filter/where pipelines). Remove\nVec<Event> clone in pattern evaluator by accepting &[SharedEvent].\nAdd Vec::with_capacity at 6 allocation sites. Avoid per-event String\nclone in Log op.\n\nCloses #9\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-20T00:20:52+01:00",
          "tree_id": "6272883b3824dce3a2e3a7f871eae7449ac35fcb",
          "url": "https://github.com/varpulis/varpulis/commit/8b69a3a6ea54dcc6b3b91dd95bf629530e053f37"
        },
        "date": 1771543601458,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33159,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 325080,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3295400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39989,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 440060,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2186900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32329,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 385510,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1941500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2098100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3845300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1460700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1657100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 108560000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1749200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3286000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16302000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32896000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32644000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 18891000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "59c48934b4a119d240e4899e33d99ba82353cff8",
          "message": "feat(lsp): implement go-to-definition and find-references\n\nCloses #13 — adds navigation module with symbol table lookup for\ngo-to-definition (streams, events, connectors, functions, variables,\ntypes, patterns, contexts) and whole-word reference search. Exposes\nvalidate_with_symbols() API from core. 18 tests covering both features\nincluding edge cases (empty docs, parse errors, unknown symbols).\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-20T14:40:08+01:00",
          "tree_id": "7581134d315da58bb4a992b3484ce267d47429aa",
          "url": "https://github.com/varpulis/varpulis/commit/59c48934b4a119d240e4899e33d99ba82353cff8"
        },
        "date": 1771595255360,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 36667,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 374920,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3783300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 48832,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 512159,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2577200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 36863,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 417930,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2133200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2465000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4790900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1702000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1975500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 146960000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 2007299,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3834500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 19885000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 40778000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 39695000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 23885000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "3ecb371d562a29b290570c882b0745d10f87bc1a",
          "message": "fix(ci): resolve clippy and compile errors across feature flags\n\n- Add cfg guards for stub-only imports (S3Sink, ElasticsearchSink, KinesisSink, RedisSink)\n- Fix elasticsearch bulk body to use JsonBody wrapper (Body trait)\n- Fix unnecessary to_string/to_owned clippy warnings in kinesis connector\n- Fix redundant pattern matching in database connector\n- Remove unused tracing::error import in redis connector\n- Remove unused afterEach import in useWebSocket test\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-20T15:03:14+01:00",
          "tree_id": "f490d07eaa725435b8e8f28a812536b8dc081153",
          "url": "https://github.com/varpulis/varpulis/commit/3ecb371d562a29b290570c882b0745d10f87bc1a"
        },
        "date": 1771596559309,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33512,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 331190,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3320100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40230,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 450450,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2416700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32305,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 383580,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2009300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2125300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3797200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1461000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1667300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 107160000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1720200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3323900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16552000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33046000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32863000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19488000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "6aa35f11c61f970300506d15657d783b5b89bf9c",
          "message": "fix(ci): format fixes, elasticsearch useless conversion, redis test cfg guard\n\n- Fix cargo fmt diff in kinesis.rs and elasticsearch.rs\n- Remove useless .into() on String in elasticsearch ApiKey credential\n- Guard redis stub tests with #[cfg(not(feature = \"redis\"))] since\n  RedisSink::new is async when the real feature is enabled\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-20T15:31:06+01:00",
          "tree_id": "779b9f3f699d53bd748c7cc6f957d7125451af1b",
          "url": "https://github.com/varpulis/varpulis/commit/6aa35f11c61f970300506d15657d783b5b89bf9c"
        },
        "date": 1771598214888,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33425,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 330760,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3298900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39744,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 434520,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2202000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32749,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 387750,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1994200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2077900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3899800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1457100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1685300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 110990000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1750600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3244100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16360000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32637999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33155000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19560000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "cb06aa9181507fcfc72c0142b17cf31a95af4ed2",
          "message": "fix(parser): prevent exponential backtracking on deeply nested brackets\n\nFuzzer discovered inputs with unmatched `[` brackets that cause pest's\nPEG parser to hang via exponential backtracking through ambiguous\narray_literal/index_access/slice_access rules.\n\nThree bugs in check_nesting_depth pre-scan:\n- Skipped single-quoted strings (`'`), but VPL has no such syntax —\n  this hid brackets from the depth count\n- Skipped `//` line comments, but VPL uses `#` for line comments\n- MAX_NESTING_DEPTH of 64 was too high — 28 unmatched brackets already\n  cause O(2^n) backtracking timeout\n\nFixes: lower limit to 24, correct comment syntax to `#`, remove\nsingle-quote handling. Adds 5 regression tests.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-22T21:40:04+01:00",
          "tree_id": "23b0f37eae1c5703b0fec09cf364447ca9c90763",
          "url": "https://github.com/varpulis/varpulis/commit/cb06aa9181507fcfc72c0142b17cf31a95af4ed2"
        },
        "date": 1771793147834,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 32913,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 328850,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3225400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39622,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 437440,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2150200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32540,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 383180,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2004900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2110600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3876100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1457700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1666000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 113590000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1728000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3286800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16836000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32793000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32673000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 18856000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "78c69b60f04b38e4900185c1a556d5c442327c68",
          "message": "chore(fuzz): add seed corpus files for parser and runtime fuzzers\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-22T21:52:25+01:00",
          "tree_id": "b36b82ab5d1fa4e5aba0d64aa0246624a2e011f9",
          "url": "https://github.com/varpulis/varpulis/commit/78c69b60f04b38e4900185c1a556d5c442327c68"
        },
        "date": 1771793891361,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 32969,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 327350,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3245800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40307,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 445410,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2221400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32091,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 385980,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1938000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2125300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3898700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1456100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1679100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 107860000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1724000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3417100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 17019000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 34255000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32786000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19148000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "e91ad7f1129d5a9a4a24193110b461b92ad6abf2",
          "message": "docs: replace ASCII diagrams with SVG and fix architecture accuracy\n\nConvert 25 ASCII box-drawing diagrams across 13 markdown files to\nstandalone SVG files with consistent styling. Fix system.md to remove\nincorrect Avro/Protobuf reference, add PST Forecast/LSP/MCP sections,\nand clarify the ZDD crate vs zdd_unified module distinction.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-22T22:32:50+01:00",
          "tree_id": "58a8b124ca3327969857a8c2f95d6c8c74301270",
          "url": "https://github.com/varpulis/varpulis/commit/e91ad7f1129d5a9a4a24193110b461b92ad6abf2"
        },
        "date": 1771796318441,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33462,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 331110,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3276700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40057,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 441910,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2188600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33720,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 384530,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1958700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2101800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3847300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1445400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1667100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 105750000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1705400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3275500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16244000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32589000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32750000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19329000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "b0a9c8345068eaf1550f20a74ed131f3065537dc",
          "message": "fix(docs): rewrite processing-flow-pipeline SVG split box for visibility\n\nThe Aggregation label was hidden behind overlapping rectangles in the\nsplit Aggregation/Forecast box. Rewritten with a clean outer container,\ntwo distinct fill regions, and a single divider line.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-22T22:40:24+01:00",
          "tree_id": "9415ffa7cd00e7da85261f822ff97b6e3b230474",
          "url": "https://github.com/varpulis/varpulis/commit/b0a9c8345068eaf1550f20a74ed131f3065537dc"
        },
        "date": 1771796771236,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33918,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 331880,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3301800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39851,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 432610,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2180200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32060,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 384130,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1956100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2135800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3904900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1496900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1716800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 107710000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1751300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3215400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16181999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32707000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33455000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19325000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "0138a308eee6cb91cf01f7d8fd1e044ae4824ab6",
          "message": "fix(docs): correct bidirectional arrow in nats-overview-dual-role SVG\n\nThe Workers→Coordinator arrow used arrowhead-left with orient=\"auto\",\nwhich double-reversed the direction. Use the standard arrowhead marker\nand let orient=\"auto\" handle direction from the line coordinates.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-22T22:41:18+01:00",
          "tree_id": "d69708b4a7c72586239ca08a67f97b47a944f573",
          "url": "https://github.com/varpulis/varpulis/commit/0138a308eee6cb91cf01f7d8fd1e044ae4824ab6"
        },
        "date": 1771796841908,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33974,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 327940,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3267600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39263,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 438020,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2186400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 31836,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 383110,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1937900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2143000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3959200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1468100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1661900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 111270000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1735000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3308700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16498999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33275000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 34800000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19236000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}